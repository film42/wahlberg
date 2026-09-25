// Lets `#[derive(Record)]`'s generated `::wahlberg::...` paths resolve inside
// this crate too.
extern crate self as wahlberg;

pub mod eavc;
pub mod merge;
pub mod record;
pub mod store;
pub mod wal;

pub use record::{Record, RecordError, Table};
#[cfg(feature = "derive")]
pub use wahlberg_derive::Record;
