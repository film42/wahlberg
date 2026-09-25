use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
struct User {
    #[serde(rename(serialize = "a", deserialize = "b"))]
    id: String,
}

fn main() {}
