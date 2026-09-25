//! Audit scenarios: each test asserts the behavior the README promises (or
//! that a hostile shared filesystem requires). Every one of these failed
//! against the original implementation.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tempfile::TempDir;
use ulid::Ulid;
use walburg::eavc::{self, Op, OpType};
use walburg::store::Store;
use walburg::wal;

fn tmp() -> TempDir {
    tempfile::tempdir().unwrap()
}

/// A fresh tx at a fixed base time + `ms` (random tiebreak bits).
fn tx(ms: u64) -> Ulid {
    Ulid::from_parts(1_800_000_000_000 + ms, Ulid::new().random())
}

fn op_at(tbl: &str, id: &str, field: &str, value: Value, tx: Ulid) -> Op {
    Op {
        tx,
        tbl: tbl.into(),
        id: id.into(),
        op: OpType::Update,
        field: field.into(),
        value,
        user: "u".into(),
    }
}

fn s(v: &str) -> Value {
    Value::String(v.into())
}

/// Writes a WAL file and renames it so it sorts at position `rank` (lower = earlier).
fn write_ranked(dir: &Path, rank: u32, ops: &[Op]) -> PathBuf {
    let p = wal::write_wal(dir, ops, "s", "u").unwrap();
    let dest = dir.join(format!("{:08}_s.wal", rank));
    fs::rename(&p, &dest).unwrap();
    dest
}

fn wal_count(dir: &Path) -> usize {
    wal::list_wal_files(dir).unwrap().len()
}

// ---------------------------------------------------------------------------
// 1. Flush failure silently drops the outbox.
// ---------------------------------------------------------------------------

#[test]
fn flush_failure_keeps_pending_ops() {
    let root = tmp();
    let wal_dir = root.path().join("share");
    // Simulate the network share being unavailable: a *file* sits where the dir should be.
    fs::write(&wal_dir, b"not a dir").unwrap();

    let mut store = Store::open(&wal_dir, "s", "u");
    store.create("t", "1", &[("name", s("Alice"))]).unwrap();
    assert!(
        store.flush().is_err(),
        "flush should fail while share is down"
    );

    // Share comes back.
    fs::remove_file(&wal_dir).unwrap();
    assert_eq!(
        store.pending_ops(),
        1,
        "failed flush must not discard pending ops"
    );
    assert!(store.flush().unwrap().is_some());
}

// ---------------------------------------------------------------------------
// 2. A non-"corrupt" IO error mid-batch discards ops from files already
//    marked processed. On SMB the common trigger is NotFound (stale directory
//    listing / concurrent compactor). Here we use a directory named *.wal,
//    which follows the same code path deterministically.
// ---------------------------------------------------------------------------

#[test]
fn io_error_mid_sync_does_not_lose_earlier_files() {
    let dir = tmp();
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), tx(0))]);
    fs::create_dir(dir.path().join("00000002_s.wal")).unwrap();

    let mut store = Store::open(dir.path(), "r", "r");
    let _ = store.sync(); // errors on the directory

    fs::remove_dir(dir.path().join("00000002_s.wal")).unwrap();
    let _ = store.sync();
    assert!(
        store.get("t", "1").is_some(),
        "ops from 00000001 were consumed-and-dropped; this session can never see them"
    );
}

// ---------------------------------------------------------------------------
// 3. One file with invalid UTF-8 wedges sync and compaction for everyone.
// ---------------------------------------------------------------------------

#[test]
fn invalid_utf8_file_does_not_wedge_sync_or_compaction() {
    let dir = tmp();
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), tx(0))]);
    fs::write(
        dir.path().join("00000002_x.wal"),
        b"\xff\xfe garbage from a bad disk\n",
    )
    .unwrap();
    write_ranked(dir.path(), 3, &[op_at("t", "2", "name", s("Bob"), tx(1))]);

    let mut store = Store::open(dir.path(), "r", "r");
    assert!(
        store.sync().is_ok(),
        "one bad file should be skipped, not fail the sync"
    );
    assert!(store.get("t", "2").is_some());
    assert!(
        wal::compact(dir.path(), "c").is_ok(),
        "compaction wedged by one bad file"
    );
}

// ---------------------------------------------------------------------------
// 4. Compaction deletes files it could not parse. On a sync-folder or a
//    flaky SMB share, "unparseable" often means "not fully arrived yet".
// ---------------------------------------------------------------------------

#[test]
fn compaction_never_deletes_unreadable_files() {
    let dir = tmp();
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), tx(0))]);

    // A second writer's file that is only partially visible (truncated).
    let partial = write_ranked(
        dir.path(),
        2,
        &[
            op_at("t", "2", "name", s("Bob"), tx(1)),
            op_at("t", "2", "email", s("bob@x"), tx(1)),
        ],
    );
    let full = fs::read_to_string(&partial).unwrap();
    let cut = full.rfind("{\"tx\"").unwrap();
    fs::write(&partial, &full[..cut]).unwrap();

    wal::compact(dir.path(), "c").unwrap();
    assert!(
        partial.exists(),
        "compactor deleted a file it could not read"
    );
}

#[test]
fn compaction_of_only_corrupt_files_deletes_nothing() {
    let dir = tmp();
    // What NTFS/ext4 commonly leave behind after a crash with no fsync.
    fs::write(dir.path().join("00000001_s.compact.wal"), vec![0u8; 4096]).unwrap();
    wal::compact(dir.path(), "c").unwrap();
    assert_eq!(wal_count(dir.path()), 1);
}

// ---------------------------------------------------------------------------
// 5. A compactor must not rewrite files of another version: parsing them
//    with this build's schema silently drops or misreads fields.
// ---------------------------------------------------------------------------

#[test]
fn compaction_leaves_other_version_files_alone() {
    let dir = tmp();
    let id = Ulid::new();
    // A future v4 op with a field this build doesn't know.
    let v4 = format!(
        "{{\"v\":4,\"t\":\"f\",\"n\":1,\"lo\":\"{id}\",\"hi\":\"{id}\"}}\n\
         {{\"sid\":\"new\",\"user\":\"u\",\"at\":\"2027-01-01T00:00:00Z\"}}\n\
         {{\"tx\":\"{id}\",\"tbl\":\"t\",\"id\":\"1\",\"op\":\"U\",\"field\":\"name\",\
         \"value\":\"\\\"Alice\\\"\",\"user\":\"u\",\"sig\":\"abc\"}}\n"
    );
    // An abandoned v2 op (separate opId + ts).
    let v2 = format!(
        "{{\"v\":2,\"t\":\"f\",\"n\":1,\"lo\":\"{id}\",\"hi\":\"{id}\"}}\n\
         {{\"sid\":\"old\",\"user\":\"u\",\"at\":\"2026-01-01T00:00:00Z\"}}\n\
         {{\"opId\":\"{id}\",\"tbl\":\"t\",\"id\":\"1\",\"op\":\"U\",\"field\":\"name\",\
         \"value\":\"\\\"Alice\\\"\",\"ts\":\"2026-01-01T00:00:00.000Z\",\"user\":\"u\"}}\n"
    );
    let v4_path = dir.path().join("00000001_new.wal");
    let v2_path = dir.path().join("00000002_old.wal");
    fs::write(&v4_path, v4).unwrap();
    fs::write(&v2_path, v2).unwrap();
    write_ranked(dir.path(), 3, &[op_at("t", "2", "name", s("Bob"), tx(0))]);

    wal::compact(dir.path(), "c").unwrap();
    assert!(v4_path.exists(), "compactor consumed a newer-version file");
    assert!(v2_path.exists(), "compactor consumed an older-version file");

    let mut r = Store::open(dir.path(), "r", "r");
    r.sync().unwrap();
    assert!(
        r.get("t", "1").is_none(),
        "other-version ops must not be read"
    );
    assert!(r.get("t", "2").is_some());
}

// ---------------------------------------------------------------------------
// 6. Purge is order-dependent in Store::apply_op, so replicas diverge.
// ---------------------------------------------------------------------------

#[test]
fn purge_converges_regardless_of_file_order() {
    let name = op_at("t", "1", "name", s("Alice"), tx(0));
    let purge = op_at("t", "1", "_purge", Value::Bool(true), tx(1));
    // Anyone can write this through Store::update — fields are freeform.
    // Purge is irreversible, so this must be ignored.
    let unpurge = op_at("t", "1", "_purge", Value::Bool(false), tx(2));

    let a = tmp();
    write_ranked(a.path(), 1, &[name.clone()]);
    write_ranked(a.path(), 2, &[purge.clone()]);
    write_ranked(a.path(), 3, &[unpurge.clone()]);

    let b = tmp();
    write_ranked(b.path(), 1, &[unpurge]);
    write_ranked(b.path(), 2, &[purge]);
    write_ranked(b.path(), 3, &[name]);

    let mut ra = Store::open(a.path(), "a", "a");
    ra.sync().unwrap();
    let mut rb = Store::open(b.path(), "b", "b");
    rb.sync().unwrap();

    let fa = ra.get_including_deleted("t", "1").map(|e| e.fields);
    let fb = rb.get_including_deleted("t", "1").map(|e| e.fields);
    assert_eq!(fa, fb, "same ops, different order, different state");
    assert_eq!(fa, None, "purge is irreversible");
}

// ---------------------------------------------------------------------------
// 7. README's auto-restore rule is not implemented.
// ---------------------------------------------------------------------------

#[test]
fn auto_restore_when_field_is_newer_than_delete() {
    let dir = tmp();
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), tx(0))]);
    write_ranked(
        dir.path(),
        2,
        &[op_at("t", "1", "_deleted", Value::Bool(true), tx(1))],
    );
    // Offline user edits after the delete (by tx).
    write_ranked(
        dir.path(),
        3,
        &[op_at("t", "1", "name", s("Alicia"), tx(2))],
    );

    let mut r = Store::open(dir.path(), "r", "r");
    r.sync().unwrap();
    assert!(r.get("t", "1").is_some(), "spec says entity is alive");
}

// ---------------------------------------------------------------------------
// 8. Transactions must not tear when they land in the same millisecond. With a
//    random tiebreak per op, two such transactions interleaved field-by-field;
//    now every op in a transaction shares one tx.
// ---------------------------------------------------------------------------

#[test]
fn same_ms_transactions_do_not_tear() {
    let mut torn = 0;
    for _ in 0..200 {
        let tx1 = eavc::transaction(
            "t",
            "1",
            OpType::Update,
            &[("a", s("x1")), ("b", s("x1"))],
            tx(0),
            "u1",
        );
        let tx2 = eavc::transaction(
            "t",
            "1",
            OpType::Update,
            &[("a", s("x2")), ("b", s("x2"))],
            tx(0),
            "u2",
        );
        let dir = tmp();
        write_ranked(dir.path(), 1, &tx1);
        write_ranked(dir.path(), 2, &tx2);
        let mut r = Store::open(dir.path(), "r", "r");
        r.sync().unwrap();
        let e = r.get("t", "1").unwrap();
        if e.fields["a"] != e.fields["b"] {
            torn += 1;
        }
    }
    assert_eq!(
        torn, 0,
        "{torn}/200 runs produced a mix of both transactions"
    );
}

// ---------------------------------------------------------------------------
// 9. `tx` on the wire: fixed-width Crockford base32, so string order ==
//    ULID order == time order. Case must not matter.
// ---------------------------------------------------------------------------

#[test]
fn wire_tx_sorts_lexicographically_and_ignores_case() {
    let earlier = op_at("t", "1", "f", Value::Null, tx(0));
    let later = op_at("t", "1", "f", Value::Null, tx(500));
    let ja = serde_json::to_value(&earlier).unwrap();
    let jb = serde_json::to_value(&later).unwrap();
    let (sa, sb) = (ja["tx"].as_str().unwrap(), jb["tx"].as_str().unwrap());
    assert_eq!(sa.len(), 26);
    assert!(sa < sb, "string order must match time order");

    let mut lower = ja.clone();
    lower["tx"] = Value::String(sa.to_lowercase());
    let parsed: Op = serde_json::from_value(lower).unwrap();
    assert_eq!(parsed.tx, earlier.tx, "a lowercase tx must compare equal");
}

#[test]
fn ts_is_derived_from_tx() {
    let op = op_at("t", "1", "f", Value::Null, tx(176));
    assert_eq!(op.ts().timestamp_millis(), 1_800_000_000_176);
    let wire = serde_json::to_value(&op).unwrap();
    assert!(wire.get("ts").is_none() && wire.get("opId").is_none());
}

// ---------------------------------------------------------------------------
// 9b. Timing: a session's own writes stay in order (monotonic ULIDs per
//     session), and clock skew between machines is an accepted v3 limitation.
// ---------------------------------------------------------------------------

#[test]
fn same_millisecond_writes_by_one_session_stay_in_order() {
    let dir = tmp();
    let mut st = Store::open(dir.path(), "me", "me");
    // Every transaction lands in the same millisecond.
    let mut r = 0u128;
    st.set_tx_source(move || {
        r = r
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        Ulid::from_parts(1_800_000_000_000, r)
    });
    for i in 0..200 {
        st.update("t", "1", &[("n", Value::from(i))]).unwrap();
        assert_eq!(
            st.get("t", "1").unwrap().fields["n"],
            i,
            "write {i} lost to an earlier one"
        );
    }
}

#[test]
fn skewed_clock_wins_conflicts_documented_v3_limitation() {
    // Not a bug, a documented limitation (README: Design Assumptions): a
    // machine whose clock runs ahead wins, even against an edit made after
    // seeing its write. HLC would fix this; it's Future Work.
    let dir = tmp();
    let future = Ulid::from_parts(Ulid::new().timestamp_ms() + 3_600_000, Ulid::new().random());
    write_ranked(
        dir.path(),
        1,
        &[op_at("t", "1", "name", s("Skewed"), future)],
    );

    let mut st = Store::open(dir.path(), "me", "me");
    st.sync().unwrap();
    st.update("t", "1", &[("name", s("Mine"))]).unwrap();
    assert_eq!(st.get("t", "1").unwrap().fields["name"], "Skewed");
}

#[test]
fn rapid_local_writes_never_tie() {
    let dir = tmp();
    let mut st = Store::open(dir.path(), "me", "me");
    for i in 0..500 {
        st.update("t", "1", &[("n", Value::from(i))]).unwrap();
        assert_eq!(
            st.get("t", "1").unwrap().fields["n"],
            i,
            "write {i} lost to an earlier one"
        );
    }
}

// ---------------------------------------------------------------------------
// 10. Exporter never removes purged entities from SQLite.
// ---------------------------------------------------------------------------

#[test]
fn exporter_honors_purge() {
    use std::process::Command;
    let build = Command::new("cargo")
        .args(["build", "--example", "wal-exporter"])
        .output()
        .unwrap();
    assert!(build.status.success());
    let mut bin = std::env::current_exe().unwrap();
    bin.pop();
    bin.pop();
    bin.push("examples");
    bin.push(format!("wal-exporter{}", std::env::consts::EXE_SUFFIX));

    let wal_dir = tmp();
    let db_dir = tmp();
    let db = db_dir.path().join("out.db");
    let run = || {
        let o = Command::new(&bin)
            .arg("--wal-dir")
            .arg(wal_dir.path())
            .arg("--output-db")
            .arg(&db)
            .output()
            .unwrap();
        assert!(o.status.success());
    };

    let mut st = Store::open(wal_dir.path(), "s", "u");
    st.create("t", "1", &[("ssn", s("123-45-6789"))]).unwrap();
    st.flush().unwrap();
    run();
    st.purge("t", "1").unwrap();
    st.flush().unwrap();
    run();

    let conn = rusqlite::Connection::open(&db).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM fields WHERE id = '1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(n, 0, "purged field data still present in export");
}

// ---------------------------------------------------------------------------
// 11. Two different transactions sharing a tx (a buggy or foreign writer)
//     must still resolve identically on every replica.
// ---------------------------------------------------------------------------

#[test]
fn exact_tx_ties_resolve_the_same_everywhere() {
    // A buggy writer reuses a tx. Replicas must still agree.
    let t = tx(0);
    let a = op_at("t", "1", "name", s("Alice"), t);
    let b = op_at("t", "1", "name", s("Bob"), t);
    let mut seen = Vec::new();
    for order in [[&a, &b], [&b, &a]] {
        let dir = tmp();
        write_ranked(dir.path(), 1, &[order[0].clone()]);
        write_ranked(dir.path(), 2, &[order[1].clone()]);
        let mut r = Store::open(dir.path(), "r", "r");
        r.sync().unwrap();
        seen.push(r.get("t", "1").unwrap().fields["name"].clone());
    }
    assert_eq!(seen[0], seen[1]);
}
