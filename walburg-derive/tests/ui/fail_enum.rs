use serde::{Deserialize, Serialize};
use walburg::Record;

#[derive(Serialize, Deserialize, Record)]
enum Status {
    Open,
    Closed,
}

fn main() {}
