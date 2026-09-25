//! Typed records on top of the field-level store.
//!
//! ```
//! use serde::{Deserialize, Serialize};
//! use wahlberg::{Record, store::Store};
//!
//! #[derive(Serialize, Deserialize, Record)]
//! #[record(table = "users")]
//! struct User { id: String, name: String, email: String }
//!
//! # let dir = tempfile::tempdir().unwrap();
//! let mut store = Store::open(dir.path(), "session", "me");
//! let mut users = store.table::<User>();
//! users.insert(&User { id: "u-1".into(), name: "Alice".into(), email: "a@x.com".into() })?;
//! users.update("u-1", |u| u.email = "new@x.com".into())?; // writes only `email`
//! # Ok::<(), wahlberg::RecordError>(())
//! ```
//!
//! A record maps to an entity: each key of the struct's serde JSON object is
//! one field. The id field names the entity and is not stored as a field.

use std::collections::BTreeSet;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::store::{Store, StoreError};

#[doc(hidden)]
pub mod __private {
    //! Paths for `#[derive(Record)]`'s generated code.
    pub use serde::Serialize;
    pub use serde::de::DeserializeOwned;
}

/// A struct that can be stored as an entity. Usually derived with
/// `#[derive(Record)]`, but small enough to implement by hand.
pub trait Record: Serialize + DeserializeOwned {
    /// Table the records live in.
    const TABLE: &'static str;
    /// Key of the id field in the serialized object (after serde renames).
    const ID_FIELD: &'static str;
    /// The entity id.
    fn id(&self) -> &str;
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{table}/{id} not found")]
    NotFound { table: &'static str, id: String },
    #[error("{table}/{id} already exists")]
    AlreadyExists { table: &'static str, id: String },
    #[error("{table}/{id} is purged and can never be written again")]
    Purged { table: &'static str, id: String },
    #[error("update changed the id of {table}/{id}; ids are immutable")]
    IdChanged { table: &'static str, id: String },
    #[error("{table}/{id}: field `{field}` starts with `_`, which is reserved for system fields")]
    ReservedField {
        table: &'static str,
        id: String,
        field: String,
    },
    #[error("{table}/{id}: {source}")]
    Serde {
        table: &'static str,
        id: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("{table}/{id}: record must serialize to a JSON object with a string id")]
    BadShape { table: &'static str, id: String },
}

/// A typed view of one table. Get one with [`Store::table`].
pub struct Table<'a, T> {
    store: &'a mut Store,
    _record: PhantomData<fn() -> T>,
}

impl Store {
    /// A typed handle for reading and writing `T` records.
    pub fn table<T: Record>(&mut self) -> Table<'_, T> {
        Table {
            store: self,
            _record: PhantomData,
        }
    }
}

impl<T: Record> Table<'_, T> {
    /// Writes a new record (every field). Fails if the id is already live or
    /// was ever purged. Inserting over a soft-deleted id brings it back.
    pub fn insert(&mut self, record: &T) -> Result<(), RecordError> {
        let id = record.id().to_string();
        if self.store.is_purged(T::TABLE, &id) {
            return Err(RecordError::Purged {
                table: T::TABLE,
                id,
            });
        }
        if self.store.get(T::TABLE, &id).is_some() {
            return Err(RecordError::AlreadyExists {
                table: T::TABLE,
                id,
            });
        }
        let fields = to_fields(record, &id)?;
        let fields: Vec<(&str, Value)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        self.store.create(T::TABLE, &id, &fields)?;
        Ok(())
    }

    /// Reads a live record. `Ok(None)` if it doesn't exist or is deleted;
    /// `Err` if the stored fields don't fit `T`.
    pub fn get(&self, id: &str) -> Result<Option<T>, RecordError> {
        let Some(entity) = self.store.get(T::TABLE, id) else {
            return Ok(None);
        };
        let mut object: Map<String, Value> = entity.fields.into_iter().collect();
        object.insert(T::ID_FIELD.to_string(), Value::String(id.to_string()));
        serde_json::from_value(Value::Object(object))
            .map(Some)
            .map_err(|source| RecordError::Serde {
                table: T::TABLE,
                id: id.to_string(),
                source,
            })
    }

    /// Ids of all live records, sorted.
    pub fn ids(&self) -> Vec<String> {
        self.store
            .list_entity_ids(T::TABLE)
            .into_iter()
            .filter(|id| self.store.get(T::TABLE, id).is_some())
            .collect()
    }

    /// All live records, sorted by id. Fails on the first record that
    /// doesn't fit `T`; use [`Table::ids`] + [`Table::get`] to handle those
    /// one by one.
    pub fn list(&self) -> Result<Vec<T>, RecordError> {
        let mut out = Vec::new();
        for id in self.ids() {
            if let Some(record) = self.get(&id)? {
                out.push(record);
            }
        }
        Ok(out)
    }

    /// Loads the record, applies `change`, and writes only the fields whose
    /// values changed, so concurrent edits to other fields are never
    /// overwritten. Returns the updated record.
    pub fn update(&mut self, id: &str, change: impl FnOnce(&mut T)) -> Result<T, RecordError> {
        let mut record = self.get(id)?.ok_or_else(|| RecordError::NotFound {
            table: T::TABLE,
            id: id.to_string(),
        })?;
        let before = to_fields(&record, id)?;
        change(&mut record);
        if record.id() != id {
            return Err(RecordError::IdChanged {
                table: T::TABLE,
                id: id.to_string(),
            });
        }
        let after = to_fields(&record, id)?;

        let keys: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
        let changed: Vec<(&str, Value)> = keys
            .into_iter()
            .filter(|k| before.get(*k) != after.get(*k))
            // A key that vanished (e.g. skip_serializing_if) is written as null.
            .map(|k| (k.as_str(), after.get(k).cloned().unwrap_or(Value::Null)))
            .collect();
        if !changed.is_empty() {
            self.store.update(T::TABLE, id, &changed)?;
        }
        Ok(record)
    }

    /// Soft-deletes a record (see the README's auto-restore rule).
    pub fn delete(&mut self, id: &str) -> Result<(), RecordError> {
        self.store.delete(T::TABLE, id)?;
        Ok(())
    }

    /// Permanently removes a record. Irreversible.
    pub fn purge(&mut self, id: &str) -> Result<(), RecordError> {
        self.store.purge(T::TABLE, id)?;
        Ok(())
    }
}

/// Serializes a record to its stored fields: the JSON object minus the id.
fn to_fields<T: Record>(record: &T, id: &str) -> Result<Map<String, Value>, RecordError> {
    let value = serde_json::to_value(record).map_err(|source| RecordError::Serde {
        table: T::TABLE,
        id: id.to_string(),
        source,
    })?;
    let bad_shape = || RecordError::BadShape {
        table: T::TABLE,
        id: id.to_string(),
    };
    let Value::Object(mut fields) = value else {
        return Err(bad_shape());
    };
    // The id must round-trip as the entity id, so it has to be a string.
    if !matches!(fields.remove(T::ID_FIELD), Some(Value::String(_))) {
        return Err(bad_shape());
    }
    if let Some(field) = fields.keys().find(|k| k.starts_with('_')) {
        return Err(RecordError::ReservedField {
            table: T::TABLE,
            id: id.to_string(),
            field: field.clone(),
        });
    }
    Ok(fields)
}
