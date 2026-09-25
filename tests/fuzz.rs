//! Randomized consistency fuzzer.
//!
//! Generates random op histories over a tiny key space (lots of collisions,
//! lots of ts ties), lays them out as WAL files, and checks invariants against
//! an order-independent oracle that implements the README spec.
//!
//!     cargo test --release --test fuzz -- --ignored --nocapture
//!
//! Env knobs:
//!   FUZZ_ITERS=N            iterations (default 2000)
//!   FUZZ_SEED=N             base seed (default 1)
//!   FUZZ_UNPURGE=0          never generate `_purge=false`
//!   FUZZ_AUTORESTORE=1      oracle applies the README auto-restore rule
//!                           (off by default: not implemented yet)
//!   FUZZ_SYSTEM_VALUES=0    only generate `true` for `_deleted`/`_purge`
//!
//! Checks:
//!   order       two readers, same files, different file order → same state
//!   oracle      reader state == spec oracle
//!   compaction  random partial compactions → fresh reader == oracle
//!   incremental reader that synced before compaction == fresh reader after
//!   arrival     a file is only partially visible during compaction, then
//!               fully arrives → fresh reader == oracle (no lost writes)

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use ulid::Ulid;
use wahlberg::eavc::{Op, OpType};
use wahlberg::store::Store;
use wahlberg::wal;

// ---------------------------------------------------------------------------
// rng
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i + 1);
            v.swap(i, j);
        }
    }
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Cfg {
    unpurge: bool,
    autorestore: bool,
    system_values: bool,
}

fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name).map(|v| v != "0").unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// case generation
// ---------------------------------------------------------------------------

const TABLES: &[&str] = &["t0", "t1"];
const IDS: &[&str] = &["e0", "e1", "e2"];
const FIELDS: &[&str] = &["a", "b", "_deleted", "_purge"];

fn base_ts(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(1_800_000_000_000 + ms).unwrap()
}

/// A case is a list of files; each file is a list of ops (one "flush").
type Case = Vec<Vec<Op>>;

fn gen_case(rng: &mut Rng, cfg: Cfg) -> Case {
    let n_files = 1 + rng.below(8);
    let mut files = Vec::new();
    for _ in 0..n_files {
        // A file holds 1..3 transactions; each tx shares a ts and an entity.
        let mut ops = Vec::new();
        for _ in 0..1 + rng.below(3) {
            let tbl = TABLES[rng.below(TABLES.len())];
            let id = IDS[rng.below(IDS.len())];
            let t = base_ts(rng.below(6) as i64);
            for _ in 0..1 + rng.below(3) {
                let field = FIELDS[rng.below(FIELDS.len())];
                let value = if field.starts_with('_') {
                    let allow_false = cfg.system_values && (field != "_purge" || cfg.unpurge);
                    Value::Bool(!allow_false || rng.chance(70))
                } else {
                    Value::String(format!("v{}", rng.below(4)))
                };
                ops.push(Op {
                    op_id: Ulid::new(),
                    tbl: tbl.into(),
                    id: id.into(),
                    op: OpType::Update,
                    field: field.into(),
                    value,
                    ts: t,
                    user: "fuzz".into(),
                });
            }
        }
        files.push(ops);
    }
    files
}

// ---------------------------------------------------------------------------
// oracle: order-independent spec model
// ---------------------------------------------------------------------------

type EntityKey = (String, String);
/// None = invisible (never existed or purged). Some((deleted, fields)).
type View = BTreeMap<EntityKey, Option<(bool, BTreeMap<String, String>)>>;

fn oracle(case: &Case, cfg: Cfg) -> View {
    let mut winners: HashMap<(String, String, String), &Op> = HashMap::new();
    for op in case.iter().flatten() {
        let k = (op.tbl.clone(), op.id.clone(), op.field.clone());
        let replace = winners.get(&k).map_or(true, |e| {
            op.ts > e.ts || (op.ts == e.ts && op.op_id > e.op_id)
        });
        if replace {
            winners.insert(k, op);
        }
    }

    let mut view = View::new();
    for t in TABLES {
        for i in IDS {
            let get = |f: &str| winners.get(&(t.to_string(), i.to_string(), f.to_string()));
            let exists = FIELDS.iter().any(|f| get(f).is_some());
            // Purge is irreversible: any `_purge=true` ever written wins.
            let purged = case.iter().flatten().any(|o| {
                o.tbl == *t && o.id == *i && o.field == "_purge" && o.value == Value::Bool(true)
            });
            if !exists || purged {
                view.insert((t.to_string(), i.to_string()), None);
                continue;
            }
            let mut fields = BTreeMap::new();
            let mut newest_field_ts = None;
            for f in FIELDS.iter().filter(|f| !f.starts_with('_')) {
                if let Some(o) = get(f) {
                    fields.insert(f.to_string(), o.value.to_string());
                    newest_field_ts = newest_field_ts.max(Some(o.ts));
                }
            }
            let deleted = match get("_deleted") {
                Some(d) if d.value == Value::Bool(true) => {
                    !(cfg.autorestore && newest_field_ts.is_some_and(|t| t > d.ts))
                }
                _ => false,
            };
            view.insert((t.to_string(), i.to_string()), Some((deleted, fields)));
        }
    }
    view
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

fn store_view(store: &Store) -> View {
    let mut view = View::new();
    for t in TABLES {
        for i in IDS {
            let v = store.get_including_deleted(t, i).map(|e| {
                let fields = e
                    .fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect();
                (e.deleted, fields)
            });
            view.insert((t.to_string(), i.to_string()), v);
        }
    }
    view
}

fn fresh_view(dir: &Path) -> View {
    let mut s = Store::open(dir, "reader", "reader");
    s.sync().expect("sync failed");
    store_view(&s)
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

fn diff(a: &View, b: &View) -> String {
    let mut out = String::new();
    for (k, va) in a {
        let vb = &b[k];
        if va != vb {
            out.push_str(&format!("    {}/{}: {:?}  vs  {:?}\n", k.0, k.1, va, vb));
        }
    }
    out
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
    let inc = store_view(&early);
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
                "      {}/{}.{} = {}  @ts+{}ms  op={}",
                o.tbl,
                o.id,
                o.field,
                o.value,
                o.ts.timestamp_millis() - 1_800_000_000_000,
                &o.op_id.to_string()[20..],
            );
        }
    }
}

#[test]
#[ignore = "fuzzer; run explicitly"]
fn fuzz_consistency() {
    wal::set_fsync(false);
    let iters = env_u64("FUZZ_ITERS", 2000);
    let seed = env_u64("FUZZ_SEED", 1);
    let cfg = Cfg {
        unpurge: env_flag("FUZZ_UNPURGE", true),
        autorestore: env_flag("FUZZ_AUTORESTORE", false),
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
    assert!(counts.is_empty(), "consistency violations: {:?}", counts);
}
