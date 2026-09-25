//! Typed records with `#[derive(Record)]`.
//!
//! Two teammates share one WAL directory (think: a network drive). Each edits
//! a different field of the same contact while offline; after syncing, both
//! edits survive, because `update` writes only the fields that changed.
//!
//!     cargo run --example records

use serde::{Deserialize, Serialize};
use wahlberg::Record;
use wahlberg::store::Store;

#[derive(Debug, Clone, Serialize, Deserialize, Record)]
#[record(table = "contacts")]
struct Contact {
    id: String,
    name: String,
    email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    phone: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let share = tempfile::tempdir()?;
    println!("shared WAL dir: {}", share.path().display());

    // Alice creates a contact and publishes it.
    let mut alice = Store::open(share.path(), "alice-laptop", "alice");
    alice.table::<Contact>().insert(&Contact {
        id: "c-1".into(),
        name: "Ada Lovelace".into(),
        email: "ada@example.com".into(),
        phone: None,
        tags: vec!["vip".into()],
    })?;
    alice.flush()?;
    println!("alice: inserted c-1");

    // Bob picks it up.
    let mut bob = Store::open(share.path(), "bob-desktop", "bob");
    bob.sync()?;
    println!(
        "bob:   synced, sees {:?}",
        bob.table::<Contact>().get("c-1")?
    );

    // Both edit offline — different fields of the same record.
    alice
        .table::<Contact>()
        .update("c-1", |c| c.email = "ada@analytical.engine".into())?;
    println!("alice: changed email ({} op pending)", alice.pending_ops());

    bob.table::<Contact>().update("c-1", |c| {
        c.phone = Some("555-0100".into());
        c.tags.push("math".into());
    })?;
    println!(
        "bob:   changed phone + tags ({} ops pending)",
        bob.pending_ops()
    );

    // Both publish, then both sync.
    alice.flush()?;
    bob.flush()?;
    alice.sync()?;
    bob.sync()?;

    let a = alice.table::<Contact>().get("c-1")?.unwrap();
    let b = bob.table::<Contact>().get("c-1")?.unwrap();
    println!("alice: {a:?}");
    println!("bob:   {b:?}");
    assert_eq!(a.email, "ada@analytical.engine");
    assert_eq!(a.phone.as_deref(), Some("555-0100"));
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    println!("both edits survived and both teammates agree");

    // Listing, soft delete, and restore-by-edit.
    let mut contacts = bob.table::<Contact>();
    contacts.insert(&Contact {
        id: "c-2".into(),
        name: "Charles Babbage".into(),
        email: "charles@example.com".into(),
        phone: None,
        tags: vec![],
    })?;
    let names: Vec<String> = contacts.list()?.into_iter().map(|c| c.name).collect();
    println!("bob:   contacts = {names:?}");

    contacts.delete("c-2")?;
    println!("bob:   deleted c-2, contacts = {:?}", contacts.ids());

    Ok(())
}
