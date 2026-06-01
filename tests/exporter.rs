use std::process::Command;

use serde_json::Value;
use wahlberg::store::Store;

fn exporter_bin() -> std::path::PathBuf {
    // Build path to the example binary in the same target dir as tests.
    let mut path = std::env::current_exe().unwrap();
    // tests live in target/debug/deps/exporter-HASH, go up to target/debug/
    path.pop();
    path.pop();
    path.push("examples");
    path.push("wal-exporter");
    path
}

#[test]
fn exporter_roundtrip() {
    // Build the example first.
    let build = Command::new("cargo")
        .args(["build", "--example", "wal-exporter"])
        .output()
        .unwrap();
    assert!(build.status.success(), "failed to build wal-exporter");

    let wal_dir = tempfile::tempdir().unwrap();
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("out.db");

    // Seed the WAL dir with some data.
    let mut store = Store::open(wal_dir.path(), "seeder", "test-author");
    store
        .create(
            "contacts",
            "c-1",
            &[
                ("name", Value::String("Alice".into())),
                ("email", Value::String("alice@example.com".into())),
            ],
        )
        .unwrap();
    store
        .create(
            "contacts",
            "c-2",
            &[("name", Value::String("Bob".into()))],
        )
        .unwrap();
    store
        .create(
            "tasks",
            "t-1",
            &[("title", Value::String("Ship it".into()))],
        )
        .unwrap();
    store.delete("contacts", "c-2").unwrap();
    store.flush().unwrap();

    // Run the exporter binary.
    let bin = exporter_bin();
    let output = Command::new(&bin)
        .arg("--wal-dir")
        .arg(wal_dir.path())
        .arg("--output-db")
        .arg(&db_path)
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "exporter failed:\nstdout: {}\nstderr: {}",
        stdout,
        stderr,
    );
    assert!(stdout.contains("exported"), "unexpected output: {}", stdout);

    // Verify the database contents.
    let db = rusqlite::Connection::open(&db_path).unwrap();

    // Check entities table.
    let entity_count: i64 = db
        .query_row("SELECT count(*) FROM entities", [], |r| r.get(0))
        .unwrap();
    assert_eq!(entity_count, 3, "expected 3 entities (including deleted)");

    // c-2 should be marked deleted.
    let deleted: bool = db
        .query_row(
            "SELECT deleted FROM entities WHERE tbl = 'contacts' AND id = 'c-2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(deleted);

    // Check fields.
    let alice_name: String = db
        .query_row(
            "SELECT value FROM fields WHERE tbl = 'contacts' AND id = 'c-1' AND attribute = 'name'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(alice_name, "\"Alice\"");

    let task_title: String = db
        .query_row(
            "SELECT value FROM fields WHERE tbl = 'tasks' AND id = 't-1' AND attribute = 'title'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(task_title, "\"Ship it\"");

    // Run again — should be idempotent (upsert, not duplicate).
    let output2 = Command::new(&bin)
        .arg("--wal-dir")
        .arg(wal_dir.path())
        .arg("--output-db")
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(output2.status.success());

    let entity_count2: i64 = db
        .query_row("SELECT count(*) FROM entities", [], |r| r.get(0))
        .unwrap();
    assert_eq!(entity_count2, 3, "idempotent re-run should not duplicate");
}
