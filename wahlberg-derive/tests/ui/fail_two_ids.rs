use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
struct User {
    #[record(id)]
    a: String,
    #[record(id)]
    b: String,
}

fn main() {}
