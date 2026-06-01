use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ulid::Ulid;

/// A single EAVC operation: one field-level change to an entity.
///
/// The "C" in EAVC is context — the op_id, timestamp, and author that
/// give provenance to every fact in the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Op {
    #[serde(rename = "opId")]
    pub op_id: Ulid,
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
    pub ts: DateTime<Utc>,
    pub user: String,
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
    /// Lowest op_id in the file.
    pub lo: Ulid,
    /// Highest op_id in the file.
    pub hi: Ulid,
}

/// Debug line written after the header (v2 format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalDebug {
    pub sid: String,
    pub user: String,
    pub at: DateTime<Utc>,
}

/// A materialized field value after LWW resolution.
#[derive(Debug, Clone)]
pub struct Fact {
    pub value: serde_json::Value,
    pub ts: DateTime<Utc>,
    pub op_id: Ulid,
    pub user: String,
}

impl Fact {
    /// Returns true if `op` wins over the current fact under LWW rules:
    /// newer timestamp wins; on tie, higher op_id wins.
    pub fn is_superseded_by(&self, op: &Op) -> bool {
        op.ts > self.ts || (op.ts == self.ts && op.op_id > self.op_id)
    }
}

impl Op {
    pub fn new(
        tbl: impl Into<String>,
        id: impl Into<String>,
        op: OpType,
        field: impl Into<String>,
        value: serde_json::Value,
        user: impl Into<String>,
    ) -> Self {
        Self {
            op_id: Ulid::new(),
            tbl: tbl.into(),
            id: id.into(),
            op,
            field: field.into(),
            value,
            ts: Utc::now(),
            user: user.into(),
        }
    }
}
