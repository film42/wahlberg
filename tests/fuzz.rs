//! Randomized consistency fuzzer.
//!
//! Generates random op histories over a tiny key space (lots of collisions,
//! lots of ts ties), lays them out as WAL files, and checks invariants against
//! an order-independent oracle that implements the README spec.
//!
//! Runs as part of `cargo test` (~4s, random seed each run). For a long soak:
//!
//!     FUZZ_ITERS=50000 cargo test --release --test fuzz -- --nocapture
//!
//! To replay a failure, use the seed from its message:
//!
//!     FUZZ_SEED=<seed> cargo test --test fuzz -- --nocapture
//!
//! Env knobs:
//!   FUZZ_ITERS=N            iterations (default 400)
//!   FUZZ_SEED=N             base seed (default: random per run)
//!   FUZZ_UNPURGE=0          never generate `_purge=false`
//!   FUZZ_AUTORESTORE=0      oracle ignores the README auto-restore rule
//!   FUZZ_SYSTEM_VALUES=0    only generate `true` for `_deleted`/`_purge`
//!
//! Checks:
//!   order       two readers, same files, different file order → same state
//!   oracle      reader state == spec oracle
//!   compaction  random partial compactions → fresh reader == oracle
//!   incremental reader that synced before compaction == fresh reader after
//!   arrival     a file is only partially visible during compaction, then
//!               fully arrives → fresh reader == oracle (no lost writes)

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use common::{EntityKey, Rng, View, diff, env_flag, env_u64, random_seed, store_view};
use serde_json::Value;
use ulid::Ulid;
use walburg::eavc::{self, Op, OpType};
use walburg::store::Store;
use walburg::wal;

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Cfg {
    unpurge: bool,
    autorestore: bool,
    system_values: bool,
}

// ---------------------------------------------------------------------------
// case generation
// ---------------------------------------------------------------------------

const TABLES: &[&str] = &["t0", "t1"];
const IDS: &[&str] = &["e0", "e1", "e2"];
const FIELDS: &[&str] = &["a", "b", "_deleted", "_purge"];

const BASE_MS: u64 = 1_800_000_000_000;

/// A case is a list of files; each file is a list of ops (one "flush").
type Case = Vec<Vec<Op>>;

fn entities() -> Vec<EntityKey> {
    TABLES
        .iter()
        .flat_map(|t| IDS.iter().map(move |i| (t.to_string(), i.to_string())))
        .collect()
}

fn oracle(case: &Case, cfg: Cfg) -> View {
    common::oracle(case.iter().flatten(), &entities(), cfg.autorestore)
}

fn gen_case(rng: &mut Rng, cfg: Cfg) -> Case {
    let n_files = 1 + rng.below(8);
    let mut files = Vec::new();
    for _ in 0..n_files {
        // A file holds 1..3 transactions; each shares one tx and an entity.
        // tx times span only 6ms, so same-ms ties are common.
        let mut ops = Vec::new();
        for _ in 0..1 + rng.below(3) {
            let tbl = TABLES[rng.below(TABLES.len())];
            let id = IDS[rng.below(IDS.len())];
            let tx = Ulid::from_parts(BASE_MS + rng.below(6) as u64, rng.next() as u128);
            let mut fields = Vec::new();
            for _ in 0..1 + rng.below(3) {
                let field = FIELDS[rng.below(FIELDS.len())];
                let value = if field.starts_with('_') {
                    let allow_false = cfg.system_values && (field != "_purge" || cfg.unpurge);
                    Value::Bool(!allow_false || rng.chance(70))
                } else {
                    Value::String(format!("v{}", rng.below(4)))
                };
                fields.push((field, value));
            }
            ops.extend(eavc::transaction(
                tbl,
                id,
                OpType::Update,
                &fields,
                tx,
                "fuzz",
            ));
        }
        files.push(ops);
    }
    files
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

fn fresh_view(dir: &Path) -> View {
    let mut s = Store::open(dir, "reader", "reader");
    s.sync().expect("sync failed");
    store_view(&s, &entities())
}

/// Writes each file with a rank-based name so directory order == `order`.
fn lay_out(dir: &Path, case: &Case, order: &[usize]) -> Vec<PathBuf> {
    fs::create_dir_all(dir).unwrap();
    let mut paths = vec![PathBuf::new(); case.len()];
    for (rank, &fi) in order.iter().enumerate() {
        let p = wal::write_wal(dir, &case[fi], "s", "u").unwrap();
        let dest = dir.join(format!("{:08}_f{}.wal", rank, fi));
        fs::rename(&p, &dest).unwrap();
        paths[fi] = dest;
    }
    paths
}

/// Compacts a random subset of the files in `dir` (a partial compaction, as
/// a compactor with a stale directory listing would do).
fn partial_compact(dir: &Path, rng: &mut Rng) {
    let files = wal::list_wal_files(dir).unwrap();
    if files.is_empty() {
        return;
    }
    let side = dir.join("side");
    fs::create_dir_all(&side).unwrap();
    for f in &files {
        if rng.chance(60) {
            fs::rename(f, side.join(f.file_name().unwrap())).unwrap();
        }
    }
    let _ = wal::compact(&side, "cmp");
    for f in fs::read_dir(&side).unwrap() {
        let f = f.unwrap().path();
        fs::rename(&f, dir.join(f.file_name().unwrap())).unwrap();
    }
    fs::remove_dir(&side).unwrap();
}

/// Runs every check on a case. Returns (check name, detail) for each failure.
fn run_case(case: &Case, plan_seed: u64, cfg: Cfg) -> Vec<(&'static str, String)> {
    let mut fails = Vec::new();
    let mut rng = Rng(plan_seed);
    let expected = oracle(case, cfg);
    let root = tempfile::tempdir().unwrap();

    // order + oracle
    let mut order: Vec<usize> = (0..case.len()).collect();
    lay_out(&root.path().join("fwd"), case, &order);
    order.reverse();
    lay_out(&root.path().join("rev"), case, &order);
    rng.shuffle(&mut order);
    lay_out(&root.path().join("shuf"), case, &order);

    let fwd = fresh_view(&root.path().join("fwd"));
    let rev = fresh_view(&root.path().join("rev"));
    let shuf = fresh_view(&root.path().join("shuf"));
    if fwd != rev || fwd != shuf {
        fails.push((
            "order",
            format!(
                "fwd vs rev:\n{}fwd vs shuf:\n{}",
                diff(&fwd, &rev),
                diff(&fwd, &shuf)
            ),
        ));
    }
    if fwd != expected {
        fails.push(("oracle", diff(&expected, &fwd)));
    }

    // compaction + incremental
    let cdir = root.path().join("shuf");
    let mut early = Store::open(&cdir, "early", "early");
    early.sync().unwrap();
    for _ in 0..1 + rng.below(3) {
        partial_compact(&cdir, &mut rng);
    }
    let after = fresh_view(&cdir);
    let after_oracle_on_disk = {
        // What the files on disk now say, per the oracle — isolates
        // "compactor lost data" from "reader materializes wrong".
        let mut on_disk: Case = Vec::new();
        for f in wal::list_wal_files(&cdir).unwrap() {
            if let Ok((_, ops)) = wal::read_wal_file(&f) {
                on_disk.push(ops);
            }
        }
        oracle(&on_disk, cfg)
    };
    if after_oracle_on_disk != expected {
        fails.push(("compaction", diff(&expected, &after_oracle_on_disk)));
    }
    early.sync().unwrap();
    let inc = store_view(&early, &entities());
    if inc != after {
        fails.push(("incremental", diff(&after, &inc)));
    }

    // arrival: file partially visible while a compactor runs, then lands fully.
    let adir = root.path().join("arr");
    let order: Vec<usize> = (0..case.len()).collect();
    let paths = lay_out(&adir, case, &order);
    let victim = &paths[rng.below(paths.len())];
    let full = fs::read(victim).unwrap();
    fs::write(victim, &full[..full.len() / 2]).unwrap();
    partial_compact(&adir, &mut rng);
    // If the compactor deleted it, that delete propagates (sync folder / SMB):
    // the rest of the bytes never land.
    if victim.exists() {
        fs::write(victim, &full).unwrap();
    }
    let arrived = fresh_view(&adir);
    if arrived != expected {
        fails.push(("arrival", diff(&expected, &arrived)));
    }

    fails
}

fn case_ops(case: &Case) -> usize {
    case.iter().map(|f| f.len()).sum()
}

/// Greedy shrink: drop ops/files while the named check still fails.
fn shrink(mut case: Case, plan_seed: u64, cfg: Cfg, check: &str) -> Case {
    let still_fails = |c: &Case| run_case(c, plan_seed, cfg).iter().any(|(n, _)| *n == check);
    loop {
        let mut progressed = false;
        for fi in 0..case.len() {
            let mut c = case.clone();
            c.remove(fi);
            if !c.is_empty() && still_fails(&c) {
                case = c;
                progressed = true;
                break;
            }
            for oi in 0..case[fi].len() {
                let mut c = case.clone();
                c[fi].remove(oi);
                if c[fi].is_empty() {
                    continue;
                }
                if still_fails(&c) {
                    case = c;
                    progressed = true;
                    break;
                }
            }
            if progressed {
                break;
            }
        }
        if !progressed {
            return case;
        }
    }
}

fn print_case(case: &Case) {
    for (fi, f) in case.iter().enumerate() {
        println!("    file {fi}:");
        for o in f {
            println!(
                "      {}/{}.{} = {}  @+{}ms  tx=..{}",
                o.tbl,
                o.id,
                o.field,
                o.value,
                o.tx.timestamp_ms() - BASE_MS,
                &o.tx.to_string()[20..],
            );
        }
    }
}

#[test]
fn fuzz_consistency() {
    wal::set_fsync(false);
    let iters = env_u64("FUZZ_ITERS", 400);
    let seed = env_u64("FUZZ_SEED", random_seed());
    let cfg = Cfg {
        unpurge: env_flag("FUZZ_UNPURGE", true),
        autorestore: env_flag("FUZZ_AUTORESTORE", true),
        system_values: env_flag("FUZZ_SYSTEM_VALUES", true),
    };
    println!(
        "fuzz: iters={iters} seed={seed} unpurge={} autorestore={} system_values={}",
        cfg.unpurge, cfg.autorestore, cfg.system_values
    );

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reported: BTreeSet<&str> = BTreeSet::new();

    for it in 0..iters {
        let case_seed = seed.wrapping_mul(1_000_003).wrapping_add(it);
        let mut rng = Rng(case_seed);
        let case = gen_case(&mut rng, cfg);
        let plan_seed = rng.next();

        for (check, detail) in run_case(&case, plan_seed, cfg) {
            *counts.entry(check).or_default() += 1;
            if reported.insert(check) {
                println!(
                    "\n[FAIL {check}] iter={it} case_seed={case_seed} ({} ops)",
                    case_ops(&case)
                );
                print!("{detail}");
                let small = shrink(case.clone(), plan_seed, cfg, check);
                println!("  shrunk to {} ops:", case_ops(&small));
                print_case(&small);
                if let Some((_, d)) = run_case(&small, plan_seed, cfg)
                    .into_iter()
                    .find(|(n, _)| *n == check)
                {
                    println!("  expected vs actual:\n{d}");
                }
            }
        }
        if (it + 1) % 250 == 0 {
            println!("... {} iters, failures so far: {:?}", it + 1, counts);
        }
    }

    println!(
        "\nfuzz done: {iters} iters, failures by check: {:?}",
        counts
    );
    assert!(
        counts.is_empty(),
        "consistency violations: {:?}; replay with FUZZ_SEED={seed}",
        counts
    );
}
