use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
struct User {
    id: u64,
}

fn main() {}
