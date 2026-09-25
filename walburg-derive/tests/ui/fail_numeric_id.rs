use serde::{Deserialize, Serialize};
use walburg::Record;

#[derive(Serialize, Deserialize, Record)]
struct User {
    id: u64,
}

fn main() {}
