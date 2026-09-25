use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ulid::Ulid;

/// A single EAVC operation: one field-level change to an entity.
///
/// The "C" in EAVC is context — the transaction id and author that give
/// provenance to every fact in the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Op {
    /// Transaction id, shared by every op in the transaction. A ULID is a
    /// 48-bit millisecond timestamp followed by 80 random bits, so this one
    /// field is both the op's time and its LWW tiebreak.
    pub tx: Ulid,
    pub tbl: String,
    pub id: String,
    pub op: OpType,
    pub field: String,
    /// Value is stored as a JSON-stringified string on the wire,
    /// e.g. `"\"Alice\""` or `"42"`. We parse/emit it transparently.
    #[serde(
        serialize_with = "serialize_value_as_string",
        deserialize_with = "deserialize_value_from_string"
    )]
    pub value: serde_json::Value,
    pub user: String,
}

impl Op {
    /// When the transaction happened: the timestamp embedded in `tx`.
    pub fn ts(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.tx.timestamp_ms() as i64).unwrap()
    }
}

fn serialize_value_as_string<S: Serializer>(
    value: &serde_json::Value,
    s: S,
) -> Result<S::Ok, S::Error> {
    let stringified = serde_json::to_string(value).unwrap_or_default();
    s.serialize_str(&stringified)
}

fn deserialize_value_from_string<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<serde_json::Value, D::Error> {
    let raw: String = String::deserialize(d)?;
    serde_json::from_str(&raw).or(Ok(serde_json::Value::String(raw)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpType {
    Create,
    Update,
    Delete,
}

impl OpType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OpType::Create => "C",
            OpType::Update => "U",
            OpType::Delete => "D",
        }
    }
}

impl Serialize for OpType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(match self {
            OpType::Create => "C",
            OpType::Update => "U",
            OpType::Delete => "D",
        })
    }
}

impl<'de> Deserialize<'de> for OpType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        match raw.as_str() {
            "C" => Ok(OpType::Create),
            "U" => Ok(OpType::Update),
            "D" => Ok(OpType::Delete),
            other => Err(serde::de::Error::custom(format!(
                "unknown op type: {}",
                other
            ))),
        }
    }
}

/// Header written as the first line of every WAL file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalHeader {
    /// Schema version.
    pub v: u32,
    /// File type: "f" = fragment, "c" = compact.
    pub t: String,
    /// Number of ops in the file.
    pub n: usize,
    /// Lowest tx in the file.
    pub lo: Ulid,
    /// Highest tx in the file.
    pub hi: Ulid,
}

/// Debug line written after the header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalDebug {
    pub sid: String,
    pub user: String,
    pub at: DateTime<Utc>,
}

/// Builds the ops for one transaction on one entity, all sharing `tx`.
///
/// Because every field compares with the same `tx`, a transaction wins or
/// loses as a unit — two transactions can never interleave field-by-field.
/// A transaction sets each field at most once; for a repeated field the last
/// value wins.
pub fn transaction(
    tbl: &str,
    id: &str,
    op: OpType,
    fields: &[(&str, serde_json::Value)],
    tx: Ulid,
    user: &str,
) -> Vec<Op> {
    let mut ops: Vec<Op> = Vec::with_capacity(fields.len());
    for (field, value) in fields {
        ops.retain(|o| o.field != *field);
        ops.push(Op {
            tx,
            tbl: tbl.into(),
            id: id.into(),
            op: op.clone(),
            field: field.to_string(),
            value: value.clone(),
            user: user.into(),
        });
    }
    ops
}

impl Op {
    /// A single-op transaction stamped now.
    pub fn new(
        tbl: impl Into<String>,
        id: impl Into<String>,
        op: OpType,
        field: impl Into<String>,
        value: serde_json::Value,
        user: impl Into<String>,
    ) -> Self {
        Self {
            tx: Ulid::new(),
            tbl: tbl.into(),
            id: id.into(),
            op,
            field: field.into(),
            value,
            user: user.into(),
        }
    }
}
