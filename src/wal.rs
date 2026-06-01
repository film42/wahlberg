use std::collections::BTreeSet;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::eavc::{Op, WalDebug, WalHeader};

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("corrupt WAL file {path}: {reason}")]
    Corrupt { path: PathBuf, reason: String },
}

/// Writes a batch of ops to a new WAL file in the given directory.
///
/// Returns the path of the written file.
pub fn write_wal(
    dir: &Path,
    ops: &[Op],
    session_id: &str,
    user: &str,
) -> Result<PathBuf, WalError> {
    fs::create_dir_all(dir)?;

    if ops.is_empty() {
        return Err(WalError::Corrupt {
            path: dir.to_path_buf(),
            reason: "cannot write empty WAL file".into(),
        });
    }

    let lo = ops.iter().map(|o| o.op_id).min().unwrap();
    let hi = ops.iter().map(|o| o.op_id).max().unwrap();
    let file_ulid = Ulid::new();

    let filename = format!("{}_{}.wal", file_ulid, session_id);
    let final_path = dir.join(&filename);
    let tmp_path = dir.join(format!(".{}.tmp", filename));

    // Write to tmp first, then rename for atomicity.
    {
        let file = fs::File::create(&tmp_path)?;
        let mut w = BufWriter::new(file);

        let header = WalHeader {
            v: 2,
            t: "f".into(),
            n: ops.len(),
            lo,
            hi,
        };
        serde_json::to_writer(&mut w, &header)?;
        w.write_all(b"\n")?;

        let debug = WalDebug {
            sid: session_id.into(),
            user: user.into(),
            at: chrono::Utc::now(),
        };
        serde_json::to_writer(&mut w, &debug)?;
        w.write_all(b"\n")?;

        for op in ops {
            serde_json::to_writer(&mut w, op)?;
            w.write_all(b"\n")?;
        }

        w.flush()?;
    }

    fs::rename(&tmp_path, &final_path)?;
    Ok(final_path)
}

/// Reads a single WAL file and returns the header and ops.
pub fn read_wal_file(path: &Path) -> Result<(WalHeader, Vec<Op>), WalError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header_line = lines
        .next()
        .ok_or_else(|| WalError::Corrupt {
            path: path.to_path_buf(),
            reason: "empty file".into(),
        })?
        .map_err(WalError::Io)?;

    let header: WalHeader = serde_json::from_str(&header_line)?;

    // v2 has a debug line after the header — skip it.
    if header.v >= 2 {
        let _debug_line = lines.next().ok_or_else(|| WalError::Corrupt {
            path: path.to_path_buf(),
            reason: "missing debug line".into(),
        })?;
    }

    let mut ops = Vec::with_capacity(header.n);
    for line in lines {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let op: Op = serde_json::from_str(&line)?;
        ops.push(op);
    }

    if ops.len() != header.n {
        return Err(WalError::Corrupt {
            path: path.to_path_buf(),
            reason: format!("header says {} ops but found {}", header.n, ops.len()),
        });
    }

    Ok((header, ops))
}

/// Lists WAL files in a directory sorted by filename (ULID order).
pub fn list_wal_files(dir: &Path) -> Result<Vec<PathBuf>, WalError> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".wal") && !name.starts_with('.') {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect();
    files.sort();
    Ok(files)
}

/// Reader that tracks which WAL files have already been consumed.
pub struct WalReader {
    dir: PathBuf,
    processed: BTreeSet<String>,
}

impl WalReader {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            processed: BTreeSet::new(),
        }
    }

    /// Consume all new (unprocessed) WAL files and return their ops in order.
    pub fn consume(&mut self) -> Result<Vec<Op>, WalError> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let files = list_wal_files(&self.dir)?;
        let mut all_ops = Vec::new();

        for path in files {
            let filename = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            if self.processed.contains(&filename) {
                continue;
            }

            match read_wal_file(&path) {
                Ok((_header, ops)) => {
                    all_ops.extend(ops);
                    self.processed.insert(filename);
                }
                Err(WalError::Corrupt { path, reason }) => {
                    eprintln!("skipping corrupt WAL file {}: {}", path.display(), reason);
                    self.processed.insert(filename);
                }
                Err(WalError::Json(ref _e)) => {
                    eprintln!("skipping unparseable WAL file {}: {}", path.display(), _e);
                    self.processed.insert(filename);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(all_ops)
    }

    /// Returns the set of processed filenames.
    pub fn processed_files(&self) -> &BTreeSet<String> {
        &self.processed
    }
}

/// Compact a set of WAL files into a single compact file.
///
/// Reads all ops from the given files, keeps only the LWW-winning op per
/// (table, id, attribute) tuple, writes a new `.compact.wal` file, then
/// removes the source files.
///
/// Returns the path of the new compact file, or None if there were no ops.
pub fn compact(dir: &Path, session_id: &str) -> Result<Option<PathBuf>, WalError> {
    let files = list_wal_files(dir)?;
    if files.is_empty() {
        return Ok(None);
    }

    // Collect all ops from all files.
    let mut all_ops: Vec<Op> = Vec::new();
    let mut source_files: Vec<PathBuf> = Vec::new();

    for path in &files {
        match read_wal_file(path) {
            Ok((_header, ops)) => {
                all_ops.extend(ops);
                source_files.push(path.clone());
            }
            Err(WalError::Corrupt { .. }) | Err(WalError::Json(_)) => {
                // Skip corrupt files during compaction — don't lose good data.
                source_files.push(path.clone());
            }
            Err(e) => return Err(e),
        }
    }

    if all_ops.is_empty() {
        // All files were corrupt or empty — clean them up.
        for path in &source_files {
            let _ = fs::remove_file(path);
        }
        return Ok(None);
    }

    // LWW merge: keep only the winning op per (table, id, field).
    use std::collections::{HashMap, HashSet};
    let mut winners: HashMap<(String, String, String), Op> = HashMap::new();

    for op in all_ops {
        let key = (op.tbl.clone(), op.id.clone(), op.field.clone());
        let dominated = winners.get(&key).map_or(true, |existing| {
            op.ts > existing.ts || (op.ts == existing.ts && op.op_id > existing.op_id)
        });
        if dominated {
            winners.insert(key, op);
        }
    }

    // Collect purged entity IDs — any (tbl, id) with a winning _purge=true.
    let purged: HashSet<(String, String)> = winners
        .iter()
        .filter(|((_, _, field), op)| {
            field == "_purge" && op.value == serde_json::Value::Bool(true)
        })
        .map(|((tbl, id, _), _)| (tbl.clone(), id.clone()))
        .collect();

    // Drop all non-tombstone tuples for purged entities.
    if !purged.is_empty() {
        winners.retain(|(tbl, id, field), _| {
            if purged.contains(&(tbl.clone(), id.clone())) {
                field == "_purge"
            } else {
                true
            }
        });
    }

    let mut merged_ops: Vec<Op> = winners.into_values().collect();
    // Sort by op_id for deterministic output.
    merged_ops.sort_by_key(|op| op.op_id);

    // Write compact file.
    let lo = merged_ops.iter().map(|o| o.op_id).min().unwrap();
    let hi = merged_ops.iter().map(|o| o.op_id).max().unwrap();
    let file_ulid = Ulid::new();
    let filename = format!("{}_{}.compact.wal", file_ulid, session_id);
    let final_path = dir.join(&filename);
    let tmp_path = dir.join(format!(".{}.tmp", filename));

    {
        let file = fs::File::create(&tmp_path)?;
        let mut w = BufWriter::new(file);

        let header = WalHeader {
            v: 2,
            t: "c".into(),
            n: merged_ops.len(),
            lo,
            hi,
        };
        serde_json::to_writer(&mut w, &header)?;
        w.write_all(b"\n")?;

        let debug = WalDebug {
            sid: session_id.into(),
            user: "compactor".into(),
            at: chrono::Utc::now(),
        };
        serde_json::to_writer(&mut w, &debug)?;
        w.write_all(b"\n")?;

        for op in &merged_ops {
            serde_json::to_writer(&mut w, op)?;
            w.write_all(b"\n")?;
        }

        w.flush()?;
    }

    fs::rename(&tmp_path, &final_path)?;

    // Remove source files only after compact file is committed.
    for path in &source_files {
        let _ = fs::remove_file(path);
    }

    Ok(Some(final_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eavc::OpType;

    fn make_op(table: &str, id: &str, attr: &str, val: &str) -> Op {
        Op::new(
            table,
            id,
            OpType::Create,
            attr,
            serde_json::Value::String(val.into()),
            "test-author",
        )
    }

    #[test]
    fn roundtrip_wal_file() {
        let dir = tempfile::tempdir().unwrap();
        let ops = vec![
            make_op("accounts", "a-1", "name", "Alice"),
            make_op("accounts", "a-1", "phone", "555-1234"),
        ];

        let path = write_wal(dir.path(), &ops, "sess-1", "tester").unwrap();
        assert!(path.exists());

        let (header, read_ops) = read_wal_file(&path).unwrap();
        assert_eq!(header.v, 2);
        assert_eq!(header.t, "f");
        assert_eq!(header.n, 2);
        assert_eq!(read_ops.len(), 2);
        assert_eq!(read_ops[0].field, "name");
        assert_eq!(read_ops[1].field, "phone");
        assert_eq!(read_ops[0].value, serde_json::Value::String("Alice".into()));
    }

    #[test]
    fn reader_tracks_processed() {
        let dir = tempfile::tempdir().unwrap();
        let ops1 = vec![make_op("accounts", "a-1", "name", "Alice")];
        let ops2 = vec![make_op("accounts", "a-2", "name", "Bob")];

        write_wal(dir.path(), &ops1, "s1", "t").unwrap();
        write_wal(dir.path(), &ops2, "s1", "t").unwrap();

        let mut reader = WalReader::new(dir.path());

        let first = reader.consume().unwrap();
        assert_eq!(first.len(), 2);

        // Second consume should return nothing new.
        let second = reader.consume().unwrap();
        assert!(second.is_empty());
    }

    #[test]
    fn empty_ops_errors() {
        let dir = tempfile::tempdir().unwrap();
        let result = write_wal(dir.path(), &[], "s1", "t");
        assert!(result.is_err());
    }
}
