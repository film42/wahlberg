//! Shared by the fuzzer and the simulation: a seeded RNG and the spec model.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;
use walburg::eavc::Op;
use walburg::store::Store;

// ---------------------------------------------------------------------------
// rng (splitmix64 — tiny, seedable, good enough for test generation)
// ---------------------------------------------------------------------------

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    pub fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i + 1);
            v.swap(i, j);
        }
    }
    pub fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.below(v.len())]
    }
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name).map(|v| v != "0").unwrap_or(default)
}

/// A seed from the clock, for runs that should explore new cases each time.
pub fn random_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// spec model
// ---------------------------------------------------------------------------

pub type EntityKey = (String, String);
/// None = invisible (never existed or purged). Some((deleted, fields)).
pub type View = BTreeMap<EntityKey, Option<(bool, BTreeMap<String, String>)>>;

/// The README spec, computed from scratch over a set of ops with no regard
/// to order: LWW on `tx` per field (exact ties broken by value JSON, then
/// user), irreversible purge, auto-restore.
pub fn oracle<'a>(
    ops: impl IntoIterator<Item = &'a Op>,
    entities: &[EntityKey],
    autorestore: bool,
) -> View {
    let mut winners: HashMap<(&str, &str, &str), &Op> = HashMap::new();
    let mut purged: HashSet<(&str, &str)> = HashSet::new();
    for op in ops {
        if op.field == "_purge" && op.value == Value::Bool(true) {
            purged.insert((&op.tbl, &op.id));
        }
        let k = (op.tbl.as_str(), op.id.as_str(), op.field.as_str());
        let beats = |e: &&Op| {
            (op.tx, op.value.to_string(), &op.user) > (e.tx, e.value.to_string(), &e.user)
        };
        if winners.get(&k).is_none_or(beats) {
            winners.insert(k, op);
        }
    }

    let mut by_entity: HashMap<(&str, &str), Vec<&Op>> = HashMap::new();
    for ((t, i, _), op) in &winners {
        by_entity.entry((t, i)).or_default().push(op);
    }

    let mut view = View::new();
    for (t, i) in entities {
        let key = (t.clone(), i.clone());
        let facts = by_entity.get(&(t.as_str(), i.as_str()));
        let Some(facts) = facts.filter(|_| !purged.contains(&(t.as_str(), i.as_str()))) else {
            view.insert(key, None);
            continue;
        };
        let mut fields = BTreeMap::new();
        let mut newest_field = None;
        let mut deleted_at = None;
        for op in facts {
            if op.field == "_deleted" {
                if op.value == Value::Bool(true) {
                    deleted_at = Some(op.tx);
                }
            } else if !op.field.starts_with('_') {
                fields.insert(op.field.clone(), op.value.to_string());
                newest_field = newest_field.max(Some(op.tx));
            }
        }
        let deleted = match deleted_at {
            Some(d) => !(autorestore && newest_field.is_some_and(|f| f > d)),
            None => false,
        };
        view.insert(key, Some((deleted, fields)));
    }
    view
}

/// What a store currently shows for each entity, in the oracle's shape.
pub fn store_view(store: &Store, entities: &[EntityKey]) -> View {
    entities
        .iter()
        .map(|(t, i)| {
            let v = store.get_including_deleted(t, i).map(|e| {
                let fields = e
                    .fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect();
                (e.deleted, fields)
            });
            ((t.clone(), i.clone()), v)
        })
        .collect()
}

/// Lines describing where two views differ.
pub fn diff(expected: &View, actual: &View) -> String {
    let mut out = String::new();
    for (k, e) in expected {
        let a = actual.get(k).unwrap_or(&None);
        if e != a {
            out.push_str(&format!(
                "    {}/{}: expected {:?}\n      actual   {:?}\n",
                k.0, k.1, e, a
            ));
        }
    }
    out
}
