use std::collections::HashMap;
use std::path::PathBuf;

use chrono::Utc;
use serde_json::Value;
use ulid::Ulid;

use crate::eavc::{Fact, Op, OpType};
use crate::wal::{self, WalError, WalReader};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("entity not found: {table}/{id}")]
    NotFound { table: String, id: String },
}

/// Key for the materialized current-state map: (table, entity_id, field).
type FieldKey = (String, String, String);

/// A materialized entity — a bag of field→value pairs.
#[derive(Debug, Clone)]
pub struct Entity {
    pub table: String,
    pub id: String,
    pub fields: HashMap<String, Value>,
    pub deleted: bool,
    pub purged: bool,
}

/// The core store. Holds materialized state from EAVC ops and manages
/// a WAL directory for persistence and replication.
pub struct Store {
    wal_dir: PathBuf,
    session_id: String,
    user: String,
    reader: WalReader,
    /// Current materialized state: (tbl, id, field) → Fact
    current: HashMap<FieldKey, Fact>,
    /// Ops waiting to be flushed to disk.
    outbox: Vec<Op>,
}

impl Store {
    /// Open or create a store backed by the given WAL directory.
    pub fn open(wal_dir: impl Into<PathBuf>, session_id: &str, user: &str) -> Self {
        let wal_dir = wal_dir.into();
        let reader = WalReader::new(&wal_dir);
        Self {
            wal_dir,
            session_id: session_id.into(),
            user: user.into(),
            reader,
            current: HashMap::new(),
            outbox: Vec::new(),
        }
    }

    /// Ingest ops from all unprocessed WAL files, updating materialized state.
    pub fn sync(&mut self) -> Result<usize, StoreError> {
        let ops = self.reader.consume()?;
        let count = ops.len();
        for op in ops {
            self.apply_op(&op);
        }
        Ok(count)
    }

    /// Create a new entity with the given fields.
    pub fn create(
        &mut self,
        tbl: &str,
        id: &str,
        fields: &[(&str, Value)],
    ) -> Result<(), StoreError> {
        let now = Utc::now();
        for (field, value) in fields {
            let op = Op {
                op_id: Ulid::new(),
                tbl: tbl.into(),
                id: id.into(),
                op: OpType::Create,
                field: field.to_string(),
                value: value.clone(),
                ts: now,
                user: self.user.clone(),
            };
            self.apply_op(&op);
            self.outbox.push(op);
        }
        Ok(())
    }

    /// Update fields on an existing entity.
    pub fn update(
        &mut self,
        tbl: &str,
        id: &str,
        fields: &[(&str, Value)],
    ) -> Result<(), StoreError> {
        let now = Utc::now();
        for (field, value) in fields {
            let op = Op {
                op_id: Ulid::new(),
                tbl: tbl.into(),
                id: id.into(),
                op: OpType::Update,
                field: field.to_string(),
                value: value.clone(),
                ts: now,
                user: self.user.clone(),
            };
            self.apply_op(&op);
            self.outbox.push(op);
        }
        Ok(())
    }

    /// Soft-delete an entity. Fields are preserved; entity can be
    /// auto-restored if a newer field write arrives.
    pub fn delete(&mut self, tbl: &str, id: &str) -> Result<(), StoreError> {
        let op = Op {
            op_id: Ulid::new(),
            tbl: tbl.into(),
            id: id.into(),
            op: OpType::Delete,
            field: "_deleted".into(),
            value: Value::Bool(true),
            ts: Utc::now(),
            user: self.user.clone(),
        };
        self.apply_op(&op);
        self.outbox.push(op);
        Ok(())
    }

    /// Purge an entity. Drops all field tuples from local state immediately.
    /// The `_purge` tombstone is retained and replicated so other readers
    /// drop their copies too. Compaction will strip the field tuples from
    /// WAL files over time.
    pub fn purge(&mut self, tbl: &str, id: &str) -> Result<(), StoreError> {
        let op = Op {
            op_id: Ulid::new(),
            tbl: tbl.into(),
            id: id.into(),
            op: OpType::Delete,
            field: "_purge".into(),
            value: Value::Bool(true),
            ts: Utc::now(),
            user: self.user.clone(),
        };
        self.apply_op(&op);
        self.outbox.push(op);
        Ok(())
    }

    /// Flush pending ops to a WAL file on disk. Returns the file path, or
    /// None if there was nothing to flush.
    pub fn flush(&mut self) -> Result<Option<PathBuf>, StoreError> {
        if self.outbox.is_empty() {
            return Ok(None);
        }
        let ops: Vec<Op> = self.outbox.drain(..).collect();
        let path = wal::write_wal(&self.wal_dir, &ops, &self.session_id, &self.user)?;
        Ok(Some(path))
    }

    /// Get a single materialized entity. Returns None if it doesn't exist,
    /// is deleted, or is purged.
    pub fn get(&self, tbl: &str, id: &str) -> Option<Entity> {
        self.materialize_entity(tbl, id)
            .filter(|e| !e.deleted && !e.purged)
    }

    /// Get a single entity even if soft-deleted. Purged entities are gone.
    pub fn get_including_deleted(&self, tbl: &str, id: &str) -> Option<Entity> {
        self.materialize_entity(tbl, id).filter(|e| !e.purged)
    }

    /// List all non-deleted entities in a table.
    pub fn list(&self, tbl: &str) -> Vec<Entity> {
        self.list_entity_ids(tbl)
            .into_iter()
            .filter_map(|id| self.get(tbl, &id))
            .collect()
    }

    /// List all entities in a table including soft-deleted but not purged.
    pub fn list_all(&self, tbl: &str) -> Vec<Entity> {
        self.list_entity_ids(tbl)
            .into_iter()
            .filter_map(|id| self.get_including_deleted(tbl, &id))
            .collect()
    }

    /// Returns the list of known table names.
    pub fn tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = self
            .current
            .keys()
            .map(|(t, _, _)| t.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        tables.sort();
        tables
    }

    /// Number of pending ops not yet flushed to disk.
    pub fn pending_ops(&self) -> usize {
        self.outbox.len()
    }

    // --- internals ---

    fn apply_op(&mut self, op: &Op) {
        let key = (op.tbl.clone(), op.id.clone(), op.field.clone());

        let dominated = self
            .current
            .get(&key)
            .map(|existing| existing.is_superseded_by(op))
            .unwrap_or(true);

        if dominated {
            self.current.insert(
                key,
                Fact {
                    value: op.value.clone(),
                    ts: op.ts,
                    op_id: op.op_id,
                    user: op.user.clone(),
                },
            );

            // Purge: strip all non-tombstone tuples for this entity.
            if op.field == "_purge" && op.value == Value::Bool(true) {
                self.current.retain(|(t, eid, field), _| {
                    !(t == &op.tbl && eid == &op.id && field != "_purge")
                });
            }
        }
    }

    fn materialize_entity(&self, tbl: &str, id: &str) -> Option<Entity> {
        let mut fields = HashMap::new();
        let mut found = false;
        let mut deleted = false;
        let mut purged = false;

        for ((t, eid, field), fact) in &self.current {
            if t == tbl && eid == id {
                found = true;
                if field == "_purge" && fact.value == Value::Bool(true) {
                    purged = true;
                } else if field == "_deleted" && fact.value == Value::Bool(true) {
                    deleted = true;
                } else {
                    fields.insert(field.clone(), fact.value.clone());
                }
            }
        }

        if !found {
            return None;
        }

        Some(Entity {
            table: tbl.into(),
            id: id.into(),
            fields,
            deleted,
            purged,
        })
    }

    fn list_entity_ids(&self, tbl: &str) -> Vec<String> {
        let mut ids: Vec<String> = self
            .current
            .keys()
            .filter(|(t, _, _)| t == tbl)
            .map(|(_, id, _)| id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        ids.sort();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), "test-session", "tester");
        (dir, store)
    }

    #[test]
    fn create_and_get() {
        let (_dir, mut store) = temp_store();
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

        let entity = store.get("contacts", "c-1").unwrap();
        assert_eq!(entity.fields["name"], "Alice");
        assert_eq!(entity.fields["email"], "alice@example.com");
        assert!(!entity.deleted);
    }

    #[test]
    fn update_entity() {
        let (_dir, mut store) = temp_store();
        store
            .create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
            .unwrap();
        store
            .update("contacts", "c-1", &[("name", Value::String("Alicia".into()))])
            .unwrap();

        let entity = store.get("contacts", "c-1").unwrap();
        assert_eq!(entity.fields["name"], "Alicia");
    }

    #[test]
    fn soft_delete() {
        let (_dir, mut store) = temp_store();
        store
            .create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
            .unwrap();
        store.delete("contacts", "c-1").unwrap();

        assert!(store.get("contacts", "c-1").is_none());
        let deleted = store.get_including_deleted("contacts", "c-1").unwrap();
        assert!(deleted.deleted);
    }

    #[test]
    fn list_entities() {
        let (_dir, mut store) = temp_store();
        store
            .create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
            .unwrap();
        store
            .create("contacts", "c-2", &[("name", Value::String("Bob".into()))])
            .unwrap();
        store
            .create("contacts", "c-3", &[("name", Value::String("Charlie".into()))])
            .unwrap();
        store.delete("contacts", "c-2").unwrap();

        let list = store.list("contacts");
        assert_eq!(list.len(), 2);

        let all = store.list_all("contacts");
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn flush_and_sync() {
        let dir = tempfile::tempdir().unwrap();

        // Writer session: create some entities and flush.
        {
            let mut writer = Store::open(dir.path(), "writer", "garrett");
            writer
                .create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
                .unwrap();
            writer
                .create("contacts", "c-2", &[("name", Value::String("Bob".into()))])
                .unwrap();
            assert_eq!(writer.pending_ops(), 2);
            writer.flush().unwrap();
            assert_eq!(writer.pending_ops(), 0);
        }

        // Reader session: open a fresh store pointing at same WAL dir.
        {
            let mut reader = Store::open(dir.path(), "reader", "other");
            let synced = reader.sync().unwrap();
            assert_eq!(synced, 2);

            let alice = reader.get("contacts", "c-1").unwrap();
            assert_eq!(alice.fields["name"], "Alice");

            let bob = reader.get("contacts", "c-2").unwrap();
            assert_eq!(bob.fields["name"], "Bob");
        }
    }

    #[test]
    fn lww_conflict_resolution() {
        let dir = tempfile::tempdir().unwrap();

        // Two writers both set the same field.
        {
            let mut w1 = Store::open(dir.path(), "w1", "alice");
            w1.create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
                .unwrap();
            w1.flush().unwrap();
        }

        // Small delay so timestamp differs.
        std::thread::sleep(std::time::Duration::from_millis(2));

        {
            let mut w2 = Store::open(dir.path(), "w2", "bob");
            w2.create("contacts", "c-1", &[("name", Value::String("Bob".into()))])
                .unwrap();
            w2.flush().unwrap();
        }

        // Reader should see Bob's write (later timestamp wins).
        {
            let mut reader = Store::open(dir.path(), "reader", "reader");
            reader.sync().unwrap();
            let entity = reader.get("contacts", "c-1").unwrap();
            assert_eq!(entity.fields["name"], "Bob");
        }
    }

    #[test]
    fn tables_listing() {
        let (_dir, mut store) = temp_store();
        store
            .create("contacts", "c-1", &[("name", Value::String("Alice".into()))])
            .unwrap();
        store
            .create("tasks", "t-1", &[("title", Value::String("Do thing".into()))])
            .unwrap();

        let tables = store.tables();
        assert_eq!(tables, vec!["contacts", "tasks"]);
    }
}
