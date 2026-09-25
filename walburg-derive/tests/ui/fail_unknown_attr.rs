use serde::{Deserialize, Serialize};
use walburg::Record;

#[derive(Serialize, Deserialize, Record)]
#[record(tabel = "users")]
struct User {
    id: String,
}

fn main() {}
