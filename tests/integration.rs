use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use serde_json::Value;
use tempfile::TempDir;
use wahlberg::eavc::{Op, OpType};
use wahlberg::store::Store;
use wahlberg::wal;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn tmp() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn make_op(tbl: &str, id: &str, field: &str, val: &str) -> Op {
    Op::new(
        tbl,
        id,
        OpType::Create,
        field,
        Value::String(val.into()),
        "test",
    )
}

// ===========================================================================
// WAL layer tests
// ===========================================================================

#[test]
fn wal_roundtrip_single_file() {
    let dir = tmp();
    let ops = vec![
        make_op("t", "1", "a", "hello"),
        make_op("t", "1", "b", "world"),
        make_op("t", "2", "a", "foo"),
    ];

    let path = wal::write_wal(dir.path(), &ops, "s1", "test").unwrap();
    let (header, read_ops) = wal::read_wal_file(&path).unwrap();

    assert_eq!(header.v, 2);
    assert_eq!(header.t, "f");
    assert_eq!(header.n, 3);
    assert_eq!(read_ops.len(), 3);
    assert_eq!(read_ops[0], ops[0]);
    assert_eq!(read_ops[1], ops[1]);
    assert_eq!(read_ops[2], ops[2]);
}

#[test]
fn wal_multiple_files_reader() {
    let dir = tmp();

    wal::write_wal(dir.path(), &[make_op("t", "1", "a", "v1")], "s1", "t").unwrap();
    wal::write_wal(dir.path(), &[make_op("t", "2", "a", "v2")], "s1", "t").unwrap();
    wal::write_wal(dir.path(), &[make_op("t", "3", "a", "v3")], "s1", "t").unwrap();

    let mut reader = wal::WalReader::new(dir.path());

    let ops = reader.consume().unwrap();
    assert_eq!(ops.len(), 3);

    // Idempotent — second consume sees nothing new.
    let ops2 = reader.consume().unwrap();
    assert!(ops2.is_empty());

    // Write one more, should pick it up.
    wal::write_wal(dir.path(), &[make_op("t", "4", "a", "v4")], "s1", "t").unwrap();
    let ops3 = reader.consume().unwrap();
    assert_eq!(ops3.len(), 1);
}

#[test]
fn wal_empty_dir_is_fine() {
    let dir = tmp();
    let mut reader = wal::WalReader::new(dir.path());
    let ops = reader.consume().unwrap();
    assert!(ops.is_empty());
}

#[test]
fn wal_nonexistent_dir_is_fine() {
    let dir = tmp();
    let ghost = dir.path().join("does-not-exist");
    let mut reader = wal::WalReader::new(&ghost);
    let ops = reader.consume().unwrap();
    assert!(ops.is_empty());
}

#[test]
fn wal_rejects_empty_ops() {
    let dir = tmp();
    assert!(wal::write_wal(dir.path(), &[], "s1", "t").is_err());
}

#[test]
fn wal_skips_corrupt_files() {
    let dir = tmp();
    // Write a good file.
    wal::write_wal(dir.path(), &[make_op("t", "1", "a", "good")], "s1", "t").unwrap();

    // Write a corrupt file that looks like a WAL file by name.
    let corrupt_path = dir.path().join("00000000000000000001_bad.wal");
    std::fs::write(&corrupt_path, "not valid json\n").unwrap();

    // Write another good file.
    wal::write_wal(dir.path(), &[make_op("t", "2", "a", "also-good")], "s1", "t").unwrap();

    let mut reader = wal::WalReader::new(dir.path());
    let ops = reader.consume().unwrap();
    // Should get ops from both good files, skipping the corrupt one.
    assert_eq!(ops.len(), 2);
}

#[test]
fn wal_tmp_files_ignored() {
    let dir = tmp();
    // Simulate a leftover tmp file.
    std::fs::write(dir.path().join(".incomplete.wal.tmp"), "garbage").unwrap();
    wal::write_wal(dir.path(), &[make_op("t", "1", "a", "v")], "s1", "t").unwrap();

    let mut reader = wal::WalReader::new(dir.path());
    let ops = reader.consume().unwrap();
    assert_eq!(ops.len(), 1);
}

// ===========================================================================
// Compaction tests
// ===========================================================================

#[test]
fn compact_merges_files() {
    let dir = tmp();

    // Write 5 fragment files with overlapping keys.
    for i in 0..5 {
        let val = format!("v{}", i);
        wal::write_wal(dir.path(), &[make_op("t", "1", "name", &val)], "s1", "t").unwrap();
    }

    let files_before = wal::list_wal_files(dir.path()).unwrap();
    assert_eq!(files_before.len(), 5);

    let compact_path = wal::compact(dir.path(), "compactor").unwrap().unwrap();
    assert!(compact_path.exists());

    // Fragments should be gone, only compact file remains.
    let files_after = wal::list_wal_files(dir.path()).unwrap();
    assert_eq!(files_after.len(), 1);

    let (header, ops) = wal::read_wal_file(&compact_path).unwrap();
    assert_eq!(header.t, "c");
    // LWW merge: only 1 winning op for (t, 1, name).
    assert_eq!(ops.len(), 1);
}

#[test]
fn compact_preserves_distinct_fields() {
    let dir = tmp();

    wal::write_wal(dir.path(), &[make_op("t", "1", "name", "Alice")], "s1", "t").unwrap();
    wal::write_wal(dir.path(), &[make_op("t", "1", "email", "a@b.c")], "s1", "t").unwrap();
    wal::write_wal(dir.path(), &[make_op("t", "2", "name", "Bob")], "s1", "t").unwrap();

    wal::compact(dir.path(), "compactor").unwrap();

    let files = wal::list_wal_files(dir.path()).unwrap();
    assert_eq!(files.len(), 1);

    let (_header, ops) = wal::read_wal_file(&files[0]).unwrap();
    // 3 distinct (table, id, field) tuples.
    assert_eq!(ops.len(), 3);
}

#[test]
fn compact_empty_dir() {
    let dir = tmp();
    let result = wal::compact(dir.path(), "compactor").unwrap();
    assert!(result.is_none());
}

#[test]
fn store_reads_compacted_files() {
    let dir = tmp();

    // Write several fragments.
    {
        let mut store = Store::open(dir.path(), "w", "author");
        store
            .create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
            .unwrap();
        store.flush().unwrap();
        store
            .update("contacts", "c-1", &[("name", Value::String("Alicia".into()))])
            .unwrap();
        store.flush().unwrap();
        store
            .create("contacts", "c-2", &[("name", Value::String("Bob".into()))])
            .unwrap();
        store.flush().unwrap();
    }

    // Compact.
    wal::compact(dir.path(), "compactor").unwrap();

    // Fresh reader should see the final state.
    let mut reader = Store::open(dir.path(), "r", "reader");
    reader.sync().unwrap();

    let c1 = reader.get("contacts", "c-1").unwrap();
    assert_eq!(c1.fields["name"], "Alicia");

    let c2 = reader.get("contacts", "c-2").unwrap();
    assert_eq!(c2.fields["name"], "Bob");
}

// ===========================================================================
// Store layer tests
// ===========================================================================

#[test]
fn store_create_get_update_delete_lifecycle() {
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "garrett");

    // Create
    s.create(
        "people",
        "p-1",
        &[
            ("name", Value::String("Garrett".into())),
            ("age", Value::Number(30.into())),
        ],
    )
    .unwrap();

    let e = s.get("people", "p-1").unwrap();
    assert_eq!(e.fields["name"], "Garrett");
    assert_eq!(e.fields["age"], 30);
    assert!(!e.deleted);

    // Update
    s.update("people", "p-1", &[("age", Value::Number(31.into()))])
        .unwrap();
    let e = s.get("people", "p-1").unwrap();
    assert_eq!(e.fields["age"], 31);
    assert_eq!(e.fields["name"], "Garrett"); // unchanged

    // Delete
    s.delete("people", "p-1").unwrap();
    assert!(s.get("people", "p-1").is_none());
    assert!(s.get_including_deleted("people", "p-1").unwrap().deleted);
}

#[test]
fn store_list_filters_deleted() {
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "a");

    for i in 0..10 {
        let id = format!("e-{}", i);
        s.create("items", &id, &[("val", Value::Number(i.into()))])
            .unwrap();
    }

    // Delete half.
    for i in (0..10).step_by(2) {
        s.delete("items", &format!("e-{}", i)).unwrap();
    }

    assert_eq!(s.list("items").len(), 5);
    assert_eq!(s.list_all("items").len(), 10);
}

#[test]
fn store_multi_table() {
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "a");

    s.create("dogs", "d-1", &[("name", Value::String("Rex".into()))])
        .unwrap();
    s.create("cats", "c-1", &[("name", Value::String("Whiskers".into()))])
        .unwrap();
    s.create("birds", "b-1", &[("name", Value::String("Tweety".into()))])
        .unwrap();

    let tables = s.tables();
    assert_eq!(tables, vec!["birds", "cats", "dogs"]);
    assert_eq!(s.list("dogs").len(), 1);
    assert_eq!(s.list("cats").len(), 1);
    assert_eq!(s.list("birds").len(), 1);
    assert!(s.list("fish").is_empty());
}

#[test]
fn store_flush_empty_is_none() {
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "a");
    assert!(s.flush().unwrap().is_none());
}

#[test]
fn store_replication_two_writers() {
    let dir = tmp();

    // Writer A
    {
        let mut a = Store::open(dir.path(), "a", "alice");
        a.create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
            .unwrap();
        a.create("contacts", "c-2", &[("name", Value::String("Bob".into()))])
            .unwrap();
        a.flush().unwrap();
    }

    // Writer B
    {
        let mut b = Store::open(dir.path(), "b", "bob");
        b.create("contacts", "c-3", &[("name", Value::String("Charlie".into()))])
            .unwrap();
        b.update(
            "contacts",
            "c-1",
            &[("email", Value::String("alice@x.com".into()))],
        )
        .unwrap();
        b.flush().unwrap();
    }

    // Reader sees everything.
    let mut r = Store::open(dir.path(), "r", "reader");
    r.sync().unwrap();

    assert_eq!(r.list("contacts").len(), 3);
    let c1 = r.get("contacts", "c-1").unwrap();
    assert_eq!(c1.fields["name"], "Alice");
    assert_eq!(c1.fields["email"], "alice@x.com");
}

#[test]
fn store_lww_latest_timestamp_wins() {
    let dir = tmp();

    {
        let mut w1 = Store::open(dir.path(), "w1", "early");
        w1.create("t", "1", &[("x", Value::String("first".into()))])
            .unwrap();
        w1.flush().unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(2));

    {
        let mut w2 = Store::open(dir.path(), "w2", "late");
        w2.create("t", "1", &[("x", Value::String("second".into()))])
            .unwrap();
        w2.flush().unwrap();
    }

    let mut r = Store::open(dir.path(), "r", "r");
    r.sync().unwrap();
    assert_eq!(r.get("t", "1").unwrap().fields["x"], "second");
}

#[test]
fn store_lww_same_timestamp_higher_ulid_wins() {
    use chrono::Utc;
    use ulid::Ulid;

    let dir = tmp();
    let now = Utc::now();

    let op_low = Op {
        op_id: Ulid::from_parts(now.timestamp_millis() as u64, 0),
        tbl: "t".into(),
        id: "1".into(),
        op: OpType::Update,
        field: "x".into(),
        value: Value::String("loser".into()),
        ts: now,
        user: "a".into(),
    };

    let op_high = Op {
        op_id: Ulid::from_parts(now.timestamp_millis() as u64, u128::MAX),
        tbl: "t".into(),
        id: "1".into(),
        op: OpType::Update,
        field: "x".into(),
        value: Value::String("winner".into()),
        ts: now,
        user: "b".into(),
    };

    // Write low first, then high.
    wal::write_wal(dir.path(), &[op_low.clone()], "s1", "a").unwrap();
    wal::write_wal(dir.path(), &[op_high.clone()], "s2", "b").unwrap();

    let mut r1 = Store::open(dir.path(), "r1", "r");
    r1.sync().unwrap();
    assert_eq!(r1.get("t", "1").unwrap().fields["x"], "winner");

    // Now verify order-independence: write high first, then low.
    let dir2 = tmp();
    wal::write_wal(dir2.path(), &[op_high], "s2", "b").unwrap();
    wal::write_wal(dir2.path(), &[op_low], "s1", "a").unwrap();

    let mut r2 = Store::open(dir2.path(), "r2", "r");
    r2.sync().unwrap();
    assert_eq!(r2.get("t", "1").unwrap().fields["x"], "winner");
}

// ===========================================================================
// Purge tests
// ===========================================================================

#[test]
fn purge_removes_entity_from_store() {
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "a");

    s.create("contacts", "c-1", &[
        ("name", Value::String("Alice".into())),
        ("email", Value::String("a@b.c".into())),
    ]).unwrap();
    assert!(s.get("contacts", "c-1").is_some());

    s.purge("contacts", "c-1").unwrap();

    // Gone from all views.
    assert!(s.get("contacts", "c-1").is_none());
    assert!(s.get_including_deleted("contacts", "c-1").is_none());
    assert!(s.list("contacts").is_empty());
    assert!(s.list_all("contacts").is_empty());
}

#[test]
fn purge_replicates_via_wal() {
    let dir = tmp();

    // Writer creates and purges.
    {
        let mut w = Store::open(dir.path(), "w", "writer");
        w.create("contacts", "c-1", &[("name", Value::String("Alice".into()))]).unwrap();
        w.create("contacts", "c-2", &[("name", Value::String("Bob".into()))]).unwrap();
        w.purge("contacts", "c-1").unwrap();
        w.flush().unwrap();
    }

    // Reader syncs — should see c-2 but not c-1.
    let mut r = Store::open(dir.path(), "r", "reader");
    r.sync().unwrap();

    assert!(r.get("contacts", "c-1").is_none());
    assert!(r.get("contacts", "c-2").is_some());
    assert_eq!(r.list("contacts").len(), 1);
}

#[test]
fn purge_is_stronger_than_delete() {
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "a");

    s.create("t", "1", &[("x", Value::String("val".into()))]).unwrap();
    s.delete("t", "1").unwrap();

    // Soft-deleted: visible via get_including_deleted.
    assert!(s.get("t", "1").is_none());
    assert!(s.get_including_deleted("t", "1").is_some());

    s.purge("t", "1").unwrap();

    // Purged: gone from everywhere.
    assert!(s.get("t", "1").is_none());
    assert!(s.get_including_deleted("t", "1").is_none());
}

#[test]
fn purge_survives_new_field_writes() {
    // Once purged, even newer field writes should not resurrect the entity
    // in the store, because apply_op won't insert fields for a purged entity.
    // (The _purge tombstone has the latest ts, so it wins LWW on the _purge field.)
    let dir = tmp();
    let mut s = Store::open(dir.path(), "s", "a");

    s.create("t", "1", &[("x", Value::String("old".into()))]).unwrap();
    s.purge("t", "1").unwrap();

    // A later write to a different field — the purge tombstone already
    // stripped the entity. The new field gets inserted into `current`
    // but `_purge` is still true, so get() returns None.
    s.update("t", "1", &[("y", Value::String("new".into()))]).unwrap();

    assert!(s.get("t", "1").is_none());
}

#[test]
fn compaction_strips_purged_tuples() {
    let dir = tmp();

    // Write several fields for an entity across multiple WAL files.
    {
        let mut w = Store::open(dir.path(), "w", "a");
        w.create("contacts", "c-1", &[
            ("name", Value::String("Alice".into())),
            ("email", Value::String("a@b.c".into())),
            ("phone", Value::String("555".into())),
        ]).unwrap();
        w.flush().unwrap();

        w.create("contacts", "c-2", &[("name", Value::String("Bob".into()))]).unwrap();
        w.flush().unwrap();

        // Purge c-1.
        w.purge("contacts", "c-1").unwrap();
        w.flush().unwrap();
    }

    // Compact — should strip all c-1 field tuples, keep only the _purge tombstone.
    wal::compact(dir.path(), "compactor").unwrap();

    let files = wal::list_wal_files(dir.path()).unwrap();
    assert_eq!(files.len(), 1);

    let (_header, ops) = wal::read_wal_file(&files[0]).unwrap();

    // Should have: c-1/_purge + c-2/name = 2 ops.
    let c1_ops: Vec<_> = ops.iter().filter(|o| o.id == "c-1").collect();
    assert_eq!(c1_ops.len(), 1, "only the _purge tombstone should remain for c-1");
    assert_eq!(c1_ops[0].field, "_purge");

    let c2_ops: Vec<_> = ops.iter().filter(|o| o.id == "c-2").collect();
    assert_eq!(c2_ops.len(), 1);
    assert_eq!(c2_ops[0].field, "name");
}

#[test]
fn compaction_purge_propagates_over_multiple_passes() {
    let dir = tmp();

    // Pass 1: create entity with many fields.
    {
        let mut w = Store::open(dir.path(), "w", "a");
        for i in 0..20 {
            let field = format!("f{}", i);
            w.update("t", "1", &[(&field, Value::Number(i.into()))]).unwrap();
            // Flush each one separately so they're in different WAL files.
            w.flush().unwrap();
        }
    }

    // First compaction: merges all 20 fields into one compact file.
    wal::compact(dir.path(), "c").unwrap();

    // Now write the purge as a new fragment.
    {
        let mut w = Store::open(dir.path(), "w2", "a");
        w.purge("t", "1").unwrap();
        w.flush().unwrap();
    }

    // Second compaction: should merge the compact + purge fragment,
    // strip all field tuples, keep only _purge.
    wal::compact(dir.path(), "c").unwrap();

    let files = wal::list_wal_files(dir.path()).unwrap();
    assert_eq!(files.len(), 1);

    let (_header, ops) = wal::read_wal_file(&files[0]).unwrap();
    assert_eq!(ops.len(), 1, "only the _purge tombstone should survive");
    assert_eq!(ops[0].field, "_purge");
    assert_eq!(ops[0].id, "1");

    // Store built from this should see nothing.
    let mut s = Store::open(dir.path(), "verify", "v");
    s.sync().unwrap();
    assert!(s.get("t", "1").is_none());
    assert!(s.get_including_deleted("t", "1").is_none());
}

// ===========================================================================
// Concurrent stress test
// ===========================================================================

/// 10 writer threads each create entities and write WAL files concurrently.
/// Several compactor threads run compaction passes during writes.
/// After all threads finish, a fresh Store syncs from the WAL dir and we
/// verify every expected entity/field is present with correct LWW values.
#[test]
fn concurrent_writers_and_compactors_converge() {
    let dir = tmp();
    let wal_dir = dir.path().to_path_buf();

    std::fs::create_dir_all(&wal_dir).unwrap();

    let num_writers: usize = 10;
    let ops_per_writer: usize = 50;
    let num_compactors: usize = 3;

    let contended_counter = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for writer_id in 0..num_writers {
        let wal_dir = wal_dir.clone();
        let counter = contended_counter.clone();

        handles.push(thread::spawn(move || {
            let session = format!("writer-{}", writer_id);
            let user = format!("user-{}", writer_id);
            let mut store = Store::open(&wal_dir, &session, &user);

            for i in 0..ops_per_writer {
                let entity_id = format!("w{}-e{}", writer_id, i);
                store
                    .create(
                        "items",
                        &entity_id,
                        &[
                            ("writer", Value::Number(writer_id.into())),
                            ("seq", Value::Number(i.into())),
                        ],
                    )
                    .unwrap();

                let tick = counter.fetch_add(1, Ordering::SeqCst);
                store
                    .update(
                        "shared",
                        "contended",
                        &[("counter", Value::Number(tick.into()))],
                    )
                    .unwrap();

                if i % 5 == 4 {
                    store.flush().unwrap();
                }
            }

            store.flush().unwrap();
        }));
    }

    for compactor_id in 0..num_compactors {
        let wal_dir = wal_dir.clone();

        handles.push(thread::spawn(move || {
            let session = format!("compactor-{}", compactor_id);
            for _ in 0..20 {
                let _ = wal::compact(&wal_dir, &session);
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let _ = wal::compact(&wal_dir, "final");

    let mut verifier = Store::open(&wal_dir, "verifier", "verifier");
    verifier.sync().unwrap();

    let items = verifier.list("items");
    let expected_items = num_writers * ops_per_writer;
    assert_eq!(
        items.len(),
        expected_items,
        "expected {} items but found {}",
        expected_items,
        items.len()
    );

    let mut seen: HashMap<(u64, u64), bool> = HashMap::new();
    for entity in &items {
        let writer = entity.fields["writer"].as_u64().unwrap();
        let seq = entity.fields["seq"].as_u64().unwrap();
        assert!(
            !seen.contains_key(&(writer, seq)),
            "duplicate entity for writer={} seq={}",
            writer,
            seq,
        );
        seen.insert((writer, seq), true);
    }
    assert_eq!(seen.len(), expected_items);

    let contended = verifier.get("shared", "contended").unwrap();
    let final_counter = contended.fields["counter"].as_u64().unwrap();
    let max_counter = contended_counter.load(Ordering::SeqCst);
    assert!(
        final_counter < max_counter,
        "counter {} should be less than max {}",
        final_counter,
        max_counter,
    );

    // Determinism: second reader converges to identical state.
    let mut verifier2 = Store::open(&wal_dir, "verifier2", "verifier2");
    verifier2.sync().unwrap();

    let items2 = verifier2.list("items");
    assert_eq!(items.len(), items2.len());

    for e1 in &items {
        let e2 = verifier2.get(&e1.table, &e1.id).unwrap();
        assert_eq!(e1.fields, e2.fields, "divergence on entity {}", e1.id);
    }

    let contended2 = verifier2.get("shared", "contended").unwrap();
    assert_eq!(
        contended.fields["counter"], contended2.fields["counter"],
        "contended entity diverged between two readers"
    );
}

#[test]
fn concurrent_writers_different_fields_no_data_loss() {
    let dir = tmp();
    let wal_dir = dir.path().to_path_buf();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let num_writers: usize = 10;

    let mut handles = Vec::new();
    for writer_id in 0..num_writers {
        let wal_dir = wal_dir.clone();
        handles.push(thread::spawn(move || {
            let session = format!("w-{}", writer_id);
            let field = format!("field_{}", writer_id);
            let mut store = Store::open(&wal_dir, &session, &session);

            store
                .create(
                    "shared",
                    "entity-1",
                    &[(&field, Value::String(format!("val-{}", writer_id)))],
                )
                .unwrap();
            store.flush().unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut reader = Store::open(&wal_dir, "reader", "reader");
    reader.sync().unwrap();

    let entity = reader.get("shared", "entity-1").unwrap();
    for writer_id in 0..num_writers {
        let field = format!("field_{}", writer_id);
        assert_eq!(
            entity.fields[&field],
            Value::String(format!("val-{}", writer_id)),
            "missing or wrong value for {}",
            field,
        );
    }
}

#[test]
fn compaction_during_writes_preserves_consistency() {
    let dir = tmp();
    let wal_dir = dir.path().to_path_buf();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let num_writers: usize = 5;
    let ops_per_writer: usize = 40;

    let mut handles = Vec::new();

    for wid in 0..num_writers {
        let wal_dir = wal_dir.clone();
        handles.push(thread::spawn(move || {
            let session = format!("w{}", wid);
            let mut store = Store::open(&wal_dir, &session, &session);

            for i in 0..ops_per_writer {
                let eid = format!("w{}-{}", wid, i);
                store
                    .create("data", &eid, &[("n", Value::Number(i.into()))])
                    .unwrap();

                if i % 3 == 0 {
                    store.flush().unwrap();
                }
            }
            store.flush().unwrap();
        }));
    }

    for cid in 0..4 {
        let wal_dir = wal_dir.clone();
        handles.push(thread::spawn(move || {
            let session = format!("c{}", cid);
            for _ in 0..30 {
                let _ = wal::compact(&wal_dir, &session);
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let _ = wal::compact(&wal_dir, "final");

    let mut store = Store::open(&wal_dir, "verify", "verify");
    store.sync().unwrap();

    let entities = store.list("data");
    let expected = num_writers * ops_per_writer;
    assert_eq!(
        entities.len(),
        expected,
        "expected {} entities, got {} (data loss during compaction)",
        expected,
        entities.len(),
    );
}
