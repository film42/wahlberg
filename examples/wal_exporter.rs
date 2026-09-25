use std::path::PathBuf;

use clap::Parser;
use rusqlite::{Connection, params};
use walburg::store::Store;

#[derive(Parser)]
#[command(name = "wal-exporter", about = "Sync WAL ops into a SQLite database")]
struct Args {
    /// Path to the WAL directory (the dir containing .wal files)
    #[arg(long)]
    wal_dir: PathBuf,

    /// Path to the output SQLite database (created if missing)
    #[arg(long)]
    output_db: PathBuf,
}

fn main() {
    let args = Args::parse();

    if !args.wal_dir.exists() {
        eprintln!(
            "error: WAL directory does not exist: {}",
            args.wal_dir.display()
        );
        std::process::exit(1);
    }

    let db = Connection::open(&args.output_db).unwrap_or_else(|e| {
        eprintln!("error: failed to open database: {}", e);
        std::process::exit(1);
    });

    create_schema(&db);

    let mut store = Store::open(&args.wal_dir, "wal-exporter", "wal-exporter");
    let synced = store.sync().unwrap_or_else(|e| {
        eprintln!("error: failed to sync WAL: {}", e);
        std::process::exit(1);
    });

    println!("synced {} ops from WAL", synced);

    let tables = store.tables();
    let mut total_entities = 0;
    let mut total_fields = 0;

    let tx = db.unchecked_transaction().unwrap();

    // The output mirrors materialized state exactly, so rebuild it. Anything
    // purged (or otherwise gone) since the last export disappears; with
    // secure_delete on, its bytes are overwritten rather than left in free pages.
    tx.execute_batch("DELETE FROM fields; DELETE FROM entities;")
        .unwrap();

    for table in &tables {
        let entities = store.list_all(table);
        for entity in &entities {
            tx.execute(
                "INSERT INTO entities (tbl, id, deleted) VALUES (?1, ?2, ?3)",
                params![table, entity.id, entity.deleted],
            )
            .unwrap();

            for (attr, value) in &entity.fields {
                let value_str = serde_json::to_string(value).unwrap();
                tx.execute(
                    "INSERT INTO fields (tbl, id, attribute, value) VALUES (?1, ?2, ?3, ?4)",
                    params![table, entity.id, attr, value_str],
                )
                .unwrap();
                total_fields += 1;
            }

            total_entities += 1;
        }
    }

    tx.commit().unwrap();

    println!(
        "exported {} entities ({} fields) across {} tables to {}",
        total_entities,
        total_fields,
        tables.len(),
        args.output_db.display(),
    );
}

fn create_schema(db: &Connection) {
    db.execute_batch(
        "PRAGMA secure_delete = ON;

        CREATE TABLE IF NOT EXISTS entities (
            tbl      TEXT NOT NULL,
            id       TEXT NOT NULL,
            deleted  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tbl, id)
        );

        CREATE TABLE IF NOT EXISTS fields (
            tbl       TEXT NOT NULL,
            id        TEXT NOT NULL,
            attribute TEXT NOT NULL,
            value     TEXT,
            PRIMARY KEY (tbl, id, attribute),
            FOREIGN KEY (tbl, id) REFERENCES entities(tbl, id)
        );

        CREATE INDEX IF NOT EXISTS idx_fields_entity ON fields(tbl, id);
        CREATE INDEX IF NOT EXISTS idx_entities_tbl ON entities(tbl);",
    )
    .unwrap();
}
