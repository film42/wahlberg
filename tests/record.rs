//! Typed records: `#[derive(Record)]` + `Store::table`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;
use walburg::store::Store;
use walburg::{Record, RecordError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Record)]
#[record(table = "users")]
struct User {
    id: String,
    name: String,
    email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    phone: Option<String>,
}

fn alice() -> User {
    User {
        id: "u-1".into(),
        name: "Alice".into(),
        email: "alice@example.com".into(),
        phone: None,
    }
}

fn tmp() -> TempDir {
    tempfile::tempdir().unwrap()
}

#[test]
fn insert_and_get_roundtrip() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    store.table::<User>().insert(&alice()).unwrap();

    assert_eq!(store.table::<User>().get("u-1").unwrap(), Some(alice()));
    assert_eq!(store.table::<User>().get("nope").unwrap(), None);

    // Stored as plain fields under the table; the id is the entity, not a field.
    let raw = store.get("users", "u-1").unwrap();
    assert_eq!(raw.fields["name"], "Alice");
    assert!(!raw.fields.contains_key("id"));
    assert!(
        !raw.fields.contains_key("phone"),
        "None was skipped by serde"
    );
}

#[test]
fn records_replicate_through_the_wal() {
    let dir = tmp();
    let mut a = Store::open(dir.path(), "a", "alice");
    a.table::<User>().insert(&alice()).unwrap();
    a.flush().unwrap();

    let mut b = Store::open(dir.path(), "b", "bob");
    b.sync().unwrap();
    assert_eq!(b.table::<User>().get("u-1").unwrap(), Some(alice()));
}

#[test]
fn update_writes_only_changed_fields() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    store.table::<User>().insert(&alice()).unwrap();
    store.flush().unwrap();

    let updated = store
        .table::<User>()
        .update("u-1", |u| u.email = "new@example.com".into())
        .unwrap();
    assert_eq!(updated.email, "new@example.com");
    assert_eq!(store.pending_ops(), 1, "only `email` should be written");

    // No-op change writes nothing.
    store.flush().unwrap();
    store
        .table::<User>()
        .update("u-1", |u| u.name = "Alice".into())
        .unwrap();
    assert_eq!(store.pending_ops(), 0);
}

#[test]
fn concurrent_updates_to_different_fields_both_survive() {
    let dir = tmp();
    let mut seed = Store::open(dir.path(), "seed", "seed");
    seed.table::<User>().insert(&alice()).unwrap();
    seed.flush().unwrap();

    // Alice and Bob both load the record, then edit different fields offline.
    let mut a = Store::open(dir.path(), "a", "alice");
    let mut b = Store::open(dir.path(), "b", "bob");
    a.sync().unwrap();
    b.sync().unwrap();

    a.table::<User>()
        .update("u-1", |u| u.email = "alice@new.com".into())
        .unwrap();
    b.table::<User>()
        .update("u-1", |u| u.phone = Some("555-1234".into()))
        .unwrap();
    a.flush().unwrap();
    b.flush().unwrap();

    for mut s in [a, b, Store::open(dir.path(), "c", "carol")] {
        s.sync().unwrap();
        let u = s.table::<User>().get("u-1").unwrap().unwrap();
        assert_eq!(
            u.email, "alice@new.com",
            "Bob's save clobbered Alice's email"
        );
        assert_eq!(u.phone.as_deref(), Some("555-1234"));
    }
}

#[test]
fn clearing_a_skipped_option_writes_null() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let mut users = store.table::<User>();
    users
        .insert(&User {
            phone: Some("555".into()),
            ..alice()
        })
        .unwrap();

    // skip_serializing_if drops the key entirely; the store must still learn
    // the field is now empty.
    users.update("u-1", |u| u.phone = None).unwrap();
    assert_eq!(users.get("u-1").unwrap().unwrap().phone, None);
    assert_eq!(
        store.get("users", "u-1").unwrap().fields["phone"],
        Value::Null
    );
}

#[test]
fn update_errors() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let mut users = store.table::<User>();

    assert!(matches!(
        users.update("missing", |_| {}),
        Err(RecordError::NotFound { .. })
    ));

    users.insert(&alice()).unwrap();
    assert!(matches!(
        users.update("u-1", |u| u.id = "u-2".into()),
        Err(RecordError::IdChanged { .. })
    ));
    assert_eq!(store.pending_ops(), 2, "a rejected update writes nothing");
}

#[test]
fn insert_rules() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let mut users = store.table::<User>();
    users.insert(&alice()).unwrap();
    assert!(matches!(
        users.insert(&alice()),
        Err(RecordError::AlreadyExists { .. })
    ));

    // Re-inserting a soft-deleted id brings it back (auto-restore).
    users.delete("u-1").unwrap();
    assert_eq!(users.get("u-1").unwrap(), None);
    users
        .insert(&User {
            name: "Alice 2".into(),
            ..alice()
        })
        .unwrap();
    assert_eq!(users.get("u-1").unwrap().unwrap().name, "Alice 2");

    // Purged ids are gone for good.
    users.purge("u-1").unwrap();
    assert!(matches!(
        users.insert(&alice()),
        Err(RecordError::Purged { .. })
    ));
    assert_eq!(users.get("u-1").unwrap(), None);
}

#[test]
fn list_and_ids_skip_deleted() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let mut users = store.table::<User>();
    for i in [3, 1, 2] {
        users
            .insert(&User {
                id: format!("u-{i}"),
                ..alice()
            })
            .unwrap();
    }
    users.delete("u-2").unwrap();

    assert_eq!(users.ids(), vec!["u-1", "u-3"]);
    let ids: Vec<String> = users.list().unwrap().into_iter().map(|u| u.id).collect();
    assert_eq!(ids, vec!["u-1", "u-3"]);
}

#[test]
fn schema_evolution_with_serde_default() {
    #[derive(Serialize, Deserialize, Record)]
    #[record(table = "users")]
    struct UserV2 {
        id: String,
        name: String,
        #[serde(default)]
        active: bool,
    }

    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    store.table::<User>().insert(&alice()).unwrap();

    // Newer app version: extra field defaults, unknown old fields (email) ignored.
    let v2 = store.table::<UserV2>().get("u-1").unwrap().unwrap();
    assert_eq!(v2.name, "Alice");
    assert!(!v2.active);

    // Updating through V2 leaves fields V2 doesn't know about untouched.
    store
        .table::<UserV2>()
        .update("u-1", |u| u.active = true)
        .unwrap();
    assert_eq!(
        store.table::<User>().get("u-1").unwrap().unwrap().email,
        "alice@example.com"
    );
}

#[test]
fn mismatched_stored_data_is_an_error_not_a_panic() {
    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    // Another client wrote a number where we expect a string.
    store
        .create(
            "users",
            "u-9",
            &[("name", Value::from(42)), ("email", Value::from("x"))],
        )
        .unwrap();

    let err = store.table::<User>().get("u-9").unwrap_err();
    assert!(matches!(err, RecordError::Serde { .. }));
    assert!(err.to_string().contains("users/u-9"), "{err}");
    assert!(store.table::<User>().list().is_err());
    assert_eq!(store.table::<User>().ids(), vec!["u-9"]);
}

#[test]
fn serde_renames_and_custom_id_field() {
    #[derive(Debug, PartialEq, Serialize, Deserialize, Record)]
    #[serde(rename_all = "camelCase")]
    #[record(table = "accounts")]
    struct Account {
        #[record(id)]
        account_id: String,
        display_name: String,
    }

    assert_eq!(<Account as Record>::ID_FIELD, "accountId");

    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let acct = Account {
        account_id: "a-1".into(),
        display_name: "Acme".into(),
    };
    store.table::<Account>().insert(&acct).unwrap();
    assert_eq!(store.table::<Account>().get("a-1").unwrap(), Some(acct));
    assert!(
        store
            .get("accounts", "a-1")
            .unwrap()
            .fields
            .contains_key("displayName")
    );
}

#[test]
fn explicit_serde_rename_on_id() {
    #[derive(Debug, PartialEq, Serialize, Deserialize, Record)]
    struct Note {
        #[record(id)]
        #[serde(rename = "key")]
        note_key: String,
        body: String,
    }

    assert_eq!(<Note as Record>::ID_FIELD, "key");
    assert_eq!(
        <Note as Record>::TABLE,
        "note",
        "default table is snake_case"
    );

    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let note = Note {
        note_key: "n-1".into(),
        body: "hi".into(),
    };
    store.table::<Note>().insert(&note).unwrap();
    assert_eq!(store.table::<Note>().get("n-1").unwrap(), Some(note));
}

#[test]
fn nested_values_are_one_field() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Address {
        city: String,
        zip: String,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize, Record)]
    #[record(table = "people")]
    struct Person {
        id: String,
        address: Address,
        tags: Vec<String>,
    }

    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let p = Person {
        id: "p-1".into(),
        address: Address {
            city: "Provo".into(),
            zip: "84601".into(),
        },
        tags: vec!["a".into(), "b".into()],
    };
    store.table::<Person>().insert(&p).unwrap();
    assert_eq!(store.table::<Person>().get("p-1").unwrap(), Some(p));
    assert_eq!(store.get("people", "p-1").unwrap().fields.len(), 2);
}

#[test]
fn reserved_field_names_are_rejected() {
    #[derive(Serialize, Deserialize, Record)]
    #[record(table = "bad")]
    struct Sneaky {
        id: String,
        #[serde(rename = "_purge")]
        oops: bool,
    }

    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    let err = store
        .table::<Sneaky>()
        .insert(&Sneaky {
            id: "x".into(),
            oops: true,
        })
        .unwrap_err();
    assert!(matches!(err, RecordError::ReservedField { .. }), "{err}");
    assert_eq!(store.pending_ops(), 0);
}

#[test]
fn hand_written_impl_works_without_the_derive() {
    #[derive(Serialize, Deserialize)]
    struct Tag {
        name: String,
        color: String,
    }

    impl Record for Tag {
        const TABLE: &'static str = "tags";
        const ID_FIELD: &'static str = "name";
        fn id(&self) -> &str {
            &self.name
        }
    }

    let dir = tmp();
    let mut store = Store::open(dir.path(), "s", "me");
    store
        .table::<Tag>()
        .insert(&Tag {
            name: "urgent".into(),
            color: "red".into(),
        })
        .unwrap();
    assert_eq!(
        store.table::<Tag>().get("urgent").unwrap().unwrap().color,
        "red"
    );
}
