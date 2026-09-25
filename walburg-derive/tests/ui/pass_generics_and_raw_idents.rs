use serde::{Deserialize, Serialize};
use walburg::Record;

#[derive(Serialize, Deserialize, Record)]
#[record(table = "boxes")]
struct Boxed<T> {
    id: String,
    #[serde(rename = "type")]
    r#type: T,
}

#[derive(Serialize, Deserialize, Record)]
struct RawId {
    r#id: Box<str>,
}

fn main() {
    assert_eq!(<Boxed<u32> as Record>::TABLE, "boxes");
    assert_eq!(<RawId as Record>::ID_FIELD, "id");
    assert_eq!(<RawId as Record>::TABLE, "raw_id");
}
