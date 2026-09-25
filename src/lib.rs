// Lets `#[derive(Record)]`'s generated `::walburg::...` paths resolve inside
// this crate too.
extern crate self as walburg;

pub mod eavc;
pub mod merge;
pub mod record;
pub mod store;
pub mod wal;

pub use record::{Record, RecordError, Table};
#[cfg(feature = "derive")]
pub use walburg_derive::Record;
