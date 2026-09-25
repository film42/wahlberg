use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
#[record(tabel = "users")]
struct User {
    id: String,
}

fn main() {}
