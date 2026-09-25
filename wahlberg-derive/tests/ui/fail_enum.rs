use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
enum Status {
    Open,
    Closed,
}

fn main() {}
