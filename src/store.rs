use std::collections::HashMap;
use std::path::PathBuf;

use serde_json::Value;
use ulid::Ulid;

use crate::eavc::{self, Op, OpType};
use crate::merge::{DELETED, MergeState, PURGE};
use crate::wal::{self, WalError, WalReader};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("entity not found: {table}/{id}")]
    NotFound { table: String, id: String },
}

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
    /// Current materialized state: (tbl, id, field) → winning op
    current: MergeState,
    /// Ops waiting to be flushed to disk.
    outbox: Vec<Op>,
    /// tx of the last transaction this session wrote.
    last_tx: Option<Ulid>,
    /// Where fresh tx values come from. `None` = `Ulid::new()`.
    tx_source: Option<TxSource>,
}

/// Supplies fresh ULIDs for new transactions. See [`Store::set_tx_source`].
pub type TxSource = Box<dyn FnMut() -> Ulid + Send>;

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
            current: MergeState::default(),
            outbox: Vec::new(),
            last_tx: None,
            tx_source: None,
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
        self.write(tbl, id, OpType::Create, fields);
        Ok(())
    }

    /// Update fields on an existing entity.
    pub fn update(
        &mut self,
        tbl: &str,
        id: &str,
        fields: &[(&str, Value)],
    ) -> Result<(), StoreError> {
        self.write(tbl, id, OpType::Update, fields);
        Ok(())
    }

    /// Soft-delete an entity. Fields are preserved; entity can be
    /// auto-restored if a newer field write arrives.
    pub fn delete(&mut self, tbl: &str, id: &str) -> Result<(), StoreError> {
        self.write(tbl, id, OpType::Delete, &[(DELETED, Value::Bool(true))]);
        Ok(())
    }

    /// Purge an entity. Drops all field tuples from local state immediately.
    /// The `_purge` tombstone is retained and replicated so other readers
    /// drop their copies too. Compaction will strip the field tuples from
    /// WAL files over time.
    pub fn purge(&mut self, tbl: &str, id: &str) -> Result<(), StoreError> {
        self.write(tbl, id, OpType::Delete, &[(PURGE, Value::Bool(true))]);
        Ok(())
    }

    /// Flush pending ops to a WAL file on disk. Returns the file path, or
    /// None if there was nothing to flush.
    pub fn flush(&mut self) -> Result<Option<PathBuf>, StoreError> {
        if self.outbox.is_empty() {
            return Ok(None);
        }
        // Only clear the outbox once the file is verified on disk, so a
        // dropped share doesn't silently discard local edits.
        let path = wal::write_wal(&self.wal_dir, &self.outbox, &self.session_id, &self.user)?;
        self.outbox.clear();
        Ok(Some(path))
    }

    /// Get a single materialized entity. Returns None if it doesn't exist,
    /// is deleted, or is purged.
    pub fn get(&self, tbl: &str, id: &str) -> Option<Entity> {
        self.materialize_entity(tbl, id)
            .filter(|e| !e.deleted && !e.purged)
    }

    /// True if the entity has been purged. Purged ids can never be written again.
    pub fn is_purged(&self, tbl: &str, id: &str) -> bool {
        self.current.is_purged(tbl, id)
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
            .entity_keys()
            .map(|(t, _)| t.to_string())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        tables.sort();
        tables
    }

    /// Replaces the source of fresh tx values (default: `Ulid::new()`, i.e.
    /// the wall clock plus randomness). For deterministic simulation and
    /// tests, e.g. a seeded or deliberately skewed clock. The "strictly
    /// greater than what this session has seen" rule still applies on top.
    pub fn set_tx_source(&mut self, source: impl FnMut() -> Ulid + Send + 'static) {
        self.tx_source = Some(Box::new(source));
    }

    /// Number of pending ops not yet flushed to disk.
    pub fn pending_ops(&self) -> usize {
        self.outbox.len()
    }

    // --- internals ---

    /// Records one transaction: applies it locally and queues it for flush.
    fn write(&mut self, tbl: &str, id: &str, op: OpType, fields: &[(&str, Value)]) {
        let tx = self.next_tx();
        for op in eavc::transaction(tbl, id, op, fields, tx, &self.user) {
            self.apply_op(&op);
            self.outbox.push(op);
        }
    }

    /// Picks the tx for a new transaction: standard monotonic ULID
    /// generation, per session. Each new millisecond starts from a fresh
    /// random ULID; another transaction in the same millisecond (or after the
    /// clock stepped back) is this session's own previous tx + 1, so its
    /// writes always sort in the order it made them. Other writers' values
    /// are never used, so there's no cross-session coupling (and clock skew
    /// between machines is an accepted v3 limitation).
    fn next_tx(&mut self) -> Ulid {
        let fresh = self.tx_source.as_mut().map_or_else(Ulid::new, |f| f());
        let tx = match self.last_tx {
            Some(last) if fresh.timestamp_ms() <= last.timestamp_ms() => last
                .increment()
                .unwrap_or_else(|| Ulid::from_parts(last.timestamp_ms() + 1, fresh.random())),
            _ => fresh,
        };
        self.last_tx = Some(tx);
        tx
    }

    fn apply_op(&mut self, op: &Op) {
        self.current.apply(op.clone());
    }

    fn materialize_entity(&self, tbl: &str, id: &str) -> Option<Entity> {
        let facts = self.current.entity(tbl, id)?;
        let mut fields = HashMap::new();
        let mut deleted_at = None;
        let mut newest_field = None;
        let mut purged = false;

        for (field, op) in facts {
            if field == PURGE {
                purged |= op.value == Value::Bool(true);
            } else if field == DELETED {
                if op.value == Value::Bool(true) {
                    deleted_at = Some(op.tx);
                }
            } else if !field.starts_with('_') {
                fields.insert(field.clone(), op.value.clone());
                newest_field = newest_field.max(Some(op.tx));
            }
        }

        // Auto-restore: a field written after the delete means someone was
        // still editing, so the entity is alive.
        let deleted = match (deleted_at, newest_field) {
            (Some(d), Some(f)) => f <= d,
            (Some(_), None) => true,
            (None, _) => false,
        };

        Some(Entity {
            table: tbl.into(),
            id: id.into(),
            fields,
            deleted,
            purged,
        })
    }

    pub(crate) fn list_entity_ids(&self, tbl: &str) -> Vec<String> {
        let mut ids: Vec<String> = self
            .current
            .entity_keys()
            .filter(|(t, _)| *t == tbl)
            .map(|(_, id)| id.to_string())
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
            .create(
                "contacts",
                "c-1",
                &[("name", Value::String("Alice".into()))],
            )
            .unwrap();
        store
            .update(
                "contacts",
                "c-1",
                &[("name", Value::String("Alicia".into()))],
            )
            .unwrap();

        let entity = store.get("contacts", "c-1").unwrap();
        assert_eq!(entity.fields["name"], "Alicia");
    }

    #[test]
    fn soft_delete() {
        let (_dir, mut store) = temp_store();
        store
            .create(
                "contacts",
                "c-1",
                &[("name", Value::String("Alice".into()))],
            )
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
            .create(
                "contacts",
                "c-1",
                &[("name", Value::String("Alice".into()))],
            )
            .unwrap();
        store
            .create("contacts", "c-2", &[("name", Value::String("Bob".into()))])
            .unwrap();
        store
            .create(
                "contacts",
                "c-3",
                &[("name", Value::String("Charlie".into()))],
            )
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
                .create(
                    "contacts",
                    "c-1",
                    &[("name", Value::String("Alice".into()))],
                )
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
            w1.create(
                "contacts",
                "c-1",
                &[("name", Value::String("Alice".into()))],
            )
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
            .create(
                "contacts",
                "c-1",
                &[("name", Value::String("Alice".into()))],
            )
            .unwrap();
        store
            .create(
                "tasks",
                "t-1",
                &[("title", Value::String("Do thing".into()))],
            )
            .unwrap();

        let tables = store.tables();
        assert_eq!(tables, vec!["contacts", "tasks"]);
    }
}
