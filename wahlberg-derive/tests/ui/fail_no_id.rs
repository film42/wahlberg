use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
struct User {
    name: String,
}

fn main() {}
