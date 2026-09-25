//! Audit scenarios: each test asserts the behavior the README promises (or
//! that a hostile shared filesystem requires). Tests marked `#[ignore]`
//! still FAIL and document a known open issue. Run them with:
//!
//!     cargo test --test audit -- --ignored

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use tempfile::TempDir;
use ulid::Ulid;
use wahlberg::eavc::{Op, OpType};
use wahlberg::store::Store;
use wahlberg::wal;

fn tmp() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn ts(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(1_800_000_000_000 + ms).unwrap()
}

fn op_at(tbl: &str, id: &str, field: &str, value: Value, t: DateTime<Utc>) -> Op {
    Op {
        op_id: Ulid::new(),
        tbl: tbl.into(),
        id: id.into(),
        op: OpType::Update,
        field: field.into(),
        value,
        ts: t,
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
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), ts(0))]);
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
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), ts(0))]);
    fs::write(
        dir.path().join("00000002_x.wal"),
        b"\xff\xfe garbage from a bad disk\n",
    )
    .unwrap();
    write_ranked(dir.path(), 3, &[op_at("t", "2", "name", s("Bob"), ts(1))]);

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
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), ts(0))]);

    // A second writer's file that is only partially visible (truncated).
    let partial = write_ranked(
        dir.path(),
        2,
        &[
            op_at("t", "2", "name", s("Bob"), ts(1)),
            op_at("t", "2", "email", s("bob@x"), ts(1)),
        ],
    );
    let full = fs::read_to_string(&partial).unwrap();
    let cut = full.rfind("{\"opId\"").unwrap();
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
// 5. An older compactor rewrites newer-version files, stripping fields it
//    doesn't know about (serde ignores unknown fields) and re-labelling v2.
// ---------------------------------------------------------------------------

#[test]
fn compaction_leaves_newer_version_files_alone() {
    let dir = tmp();
    let id = Ulid::new();
    let v3 = format!(
        "{{\"v\":3,\"t\":\"f\",\"n\":1,\"lo\":\"{id}\",\"hi\":\"{id}\"}}\n\
         {{\"sid\":\"new\",\"user\":\"u\",\"at\":\"2027-01-01T00:00:00Z\"}}\n\
         {{\"opId\":\"{id}\",\"tbl\":\"t\",\"id\":\"1\",\"op\":\"U\",\"field\":\"name\",\
         \"value\":\"\\\"Alice\\\"\",\"ts\":\"2027-01-01T00:00:00Z\",\"user\":\"u\",\
         \"hlc\":\"0001-0003-node\"}}\n"
    );
    let v3_path = dir.path().join("00000001_new.wal");
    fs::write(&v3_path, v3).unwrap();
    write_ranked(dir.path(), 2, &[op_at("t", "2", "name", s("Bob"), ts(0))]);

    wal::compact(dir.path(), "old").unwrap();
    assert!(v3_path.exists(), "v2 compactor consumed a v3 file");
}

// ---------------------------------------------------------------------------
// 6. Purge is order-dependent in Store::apply_op, so replicas diverge.
// ---------------------------------------------------------------------------

#[test]
fn purge_converges_regardless_of_file_order() {
    let name = op_at("t", "1", "name", s("Alice"), ts(0));
    let purge = op_at("t", "1", "_purge", Value::Bool(true), ts(1));
    // Anyone can write this through Store::update — fields are freeform.
    // Purge is irreversible, so this must be ignored.
    let unpurge = op_at("t", "1", "_purge", Value::Bool(false), ts(2));

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
#[ignore = "BUG: materialize_entity ignores the auto-restore rule from the README"]
fn auto_restore_when_field_is_newer_than_delete() {
    let dir = tmp();
    write_ranked(dir.path(), 1, &[op_at("t", "1", "name", s("Alice"), ts(0))]);
    write_ranked(
        dir.path(),
        2,
        &[op_at("t", "1", "_deleted", Value::Bool(true), ts(1))],
    );
    // Offline user edits after the delete (by ts).
    write_ranked(
        dir.path(),
        3,
        &[op_at("t", "1", "name", s("Alicia"), ts(2))],
    );

    let mut r = Store::open(dir.path(), "r", "r");
    r.sync().unwrap();
    assert!(r.get("t", "1").is_some(), "spec says entity is alive");
}

// ---------------------------------------------------------------------------
// 8. Transactions tear on ts ties: the tiebreak is per-op random ULID, so
//    two transactions with the same ts interleave field-by-field.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "BUG/DESIGN: per-op ULID tiebreak lets same-ts transactions interleave"]
fn same_ts_transactions_do_not_tear() {
    let t = ts(0);
    let mut torn = 0;
    for _ in 0..200 {
        let tx1 = [
            op_at("t", "1", "a", s("x1"), t),
            op_at("t", "1", "b", s("x1"), t),
        ];
        let tx2 = [
            op_at("t", "1", "a", s("x2"), t),
            op_at("t", "1", "b", s("x2"), t),
        ];
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
// 9. Wire `ts` is variable-width, so string comparison (which the README's
//    "other language" pseudocode uses) disagrees with time order.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "PROTOCOL: ts serialization is variable precision; lexicographic order != time order"]
fn wire_ts_sorts_lexicographically() {
    let earlier = Utc.timestamp_opt(1_800_000_000, 0).unwrap(); // whole second
    let later = Utc.timestamp_opt(1_800_000_000, 500_000_000).unwrap(); // +500ms
    let a = op_at("t", "1", "f", Value::Null, earlier);
    let b = op_at("t", "1", "f", Value::Null, later);
    let ja: Value = serde_json::to_value(&a).unwrap();
    let jb: Value = serde_json::to_value(&b).unwrap();
    let (sa, sb) = (ja["ts"].as_str().unwrap(), jb["ts"].as_str().unwrap());
    println!("earlier={sa} later={sb}");
    assert!(
        sa < sb,
        "a JS reader comparing strings would pick the wrong winner"
    );
}

// ---------------------------------------------------------------------------
// 10. Exporter never removes purged entities from SQLite.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "BUG: wal-exporter upserts only; purged entities persist in the SQLite output"]
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
    bin.push("wal-exporter");

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
