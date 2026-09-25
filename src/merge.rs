use std::collections::HashMap;

use serde_json::Value;

use crate::eavc::Op;

pub const PURGE: &str = "_purge";
pub const DELETED: &str = "_deleted";

/// True if this op is a purge tombstone (`_purge = true`).
pub fn is_purge(op: &Op) -> bool {
    op.field == PURGE && op.value == Value::Bool(true)
}

/// Returns true if `new` beats `old` for the same (tbl, id, field).
///
/// Plain LWW on `tx` (its ms timestamp, then its random bits), with one
/// exception: a purge tombstone beats any non-tombstone `_purge` value
/// regardless of tx. Exact tx ties (which correct writers never produce)
/// fall back to comparing the value's JSON, then the user. That
/// makes purge irreversible, which is what lets the compactor strip purged
/// fields without a partial view of the WAL ever losing data.
pub fn wins(new: &Op, old: &Op) -> bool {
    if new.field == PURGE {
        let (n, o) = (is_purge(new), is_purge(old));
        if n != o {
            return n;
        }
    }
    if new.tx != old.tx {
        return new.tx > old.tx;
    }
    // Same tx on the same field: two writers collided (or one is buggy).
    // Break the tie on content so every replica still picks the same winner.
    (new.value.to_string(), &new.user) > (old.value.to_string(), &old.user)
}

/// Order-independent merged state, indexed by entity so per-entity reads
/// don't scan everything. The store and the compactor both use this, so there
/// is exactly one definition of how ops combine.
#[derive(Debug, Clone, Default)]
pub struct MergeState {
    /// (tbl, id) → field → winning op.
    entities: HashMap<(String, String), HashMap<String, Op>>,
}

impl MergeState {
    pub fn apply(&mut self, op: Op) {
        let entity = self
            .entities
            .entry((op.tbl.clone(), op.id.clone()))
            .or_default();
        let purged = entity.get(PURGE).is_some_and(is_purge);
        let purging = is_purge(&op);
        if purged && op.field != PURGE {
            // Purged entities never regain field data.
            return;
        }
        if !entity.get(&op.field).is_none_or(|old| wins(&op, old)) {
            return;
        }
        entity.insert(op.field.clone(), op);
        if purging && !purged {
            entity.retain(|field, _| field == PURGE);
        }
    }

    /// The winning facts of one entity, by field.
    pub fn entity(&self, tbl: &str, id: &str) -> Option<&HashMap<String, Op>> {
        self.entities.get(&(tbl.to_string(), id.to_string()))
    }

    /// Every (tbl, id) with at least one fact.
    pub fn entity_keys(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entities.keys().map(|(t, i)| (t.as_str(), i.as_str()))
    }

    pub fn is_purged(&self, tbl: &str, id: &str) -> bool {
        self.entity(tbl, id)
            .and_then(|e| e.get(PURGE))
            .is_some_and(is_purge)
    }

    /// Every winning op.
    pub fn iter(&self) -> impl Iterator<Item = &Op> {
        self.entities.values().flat_map(|fields| fields.values())
    }

    pub fn into_ops(self) -> Vec<Op> {
        self.entities
            .into_values()
            .flat_map(|fields| fields.into_values())
            .collect()
    }
}
