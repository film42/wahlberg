use serde::{Deserialize, Serialize};
use walburg::Record;

#[derive(Serialize, Deserialize, Record)]
struct User {
    name: String,
}

fn main() {}
