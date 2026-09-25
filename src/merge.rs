use std::collections::HashMap;

use serde_json::Value;
use ulid::Ulid;

use crate::eavc::Op;

pub const PURGE: &str = "_purge";
pub const DELETED: &str = "_deleted";

/// Key for materialized state: (table, entity_id, field).
pub type FieldKey = (String, String, String);

/// True if this op is a purge tombstone (`_purge = true`).
pub fn is_purge(op: &Op) -> bool {
    op.field == PURGE && op.value == Value::Bool(true)
}

/// Returns true if `new` beats `old` for the same (tbl, id, field).
///
/// Plain LWW on `tx` (its ms timestamp, then its random bits), with one
/// exception: a purge tombstone beats any non-tombstone `_purge` value
/// regardless of tx. That
/// makes purge irreversible, which is what lets the compactor strip purged
/// fields without a partial view of the WAL ever losing data.
pub fn wins(new: &Op, old: &Op) -> bool {
    if new.field == PURGE {
        let (n, o) = (is_purge(new), is_purge(old));
        if n != o {
            return n;
        }
    }
    new.tx > old.tx
}

/// Order-independent merged state. The store and the compactor both use this,
/// so there is exactly one definition of how ops combine.
#[derive(Debug, Clone, Default)]
pub struct MergeState {
    winners: HashMap<FieldKey, Op>,
}

impl MergeState {
    pub fn apply(&mut self, op: Op) {
        let purging = is_purge(&op);
        if !purging && op.field != PURGE && self.is_purged(&op.tbl, &op.id) {
            // Purged entities never regain field data.
            return;
        }

        let key = (op.tbl.clone(), op.id.clone(), op.field.clone());
        let replace = self.winners.get(&key).is_none_or(|old| wins(&op, old));
        if !replace {
            return;
        }

        let newly_purged = purging && !self.is_purged(&op.tbl, &op.id);
        let (tbl, id) = (op.tbl.clone(), op.id.clone());
        self.winners.insert(key, op);

        if newly_purged {
            self.winners
                .retain(|(t, i, f), _| !(t == &tbl && i == &id && f != PURGE));
        }
    }

    pub fn is_purged(&self, tbl: &str, id: &str) -> bool {
        self.winners
            .get(&(tbl.to_string(), id.to_string(), PURGE.to_string()))
            .is_some_and(is_purge)
    }

    /// Highest tx among the winning facts of (tbl, id), if any.
    pub fn entity_max_tx(&self, tbl: &str, id: &str) -> Option<Ulid> {
        self.winners
            .iter()
            .filter(|((t, i, _), _)| t == tbl && i == id)
            .map(|(_, op)| op.tx)
            .max()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&FieldKey, &Op)> {
        self.winners.iter()
    }

    pub fn into_ops(self) -> Vec<Op> {
        self.winners.into_values().collect()
    }
}
