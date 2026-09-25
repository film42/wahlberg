use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::hash::{DefaultHasher, Hasher};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use ulid::Ulid;

use crate::eavc::{Op, WalDebug, WalHeader};
use crate::merge::MergeState;

/// Highest WAL schema version this build understands. Files with a higher
/// version are never parsed, merged, or deleted.
pub const SUPPORTED_VERSION: u32 = 2;

static FSYNC: AtomicBool = AtomicBool::new(true);

/// Enable or disable fsync on WAL writes (process-wide, default on).
/// Read-back verification always runs. Disabling trades crash durability
/// for speed — useful in tests, or where the medium ignores fsync anyway.
pub fn set_fsync(enabled: bool) {
    FSYNC.store(enabled, Ordering::Relaxed);
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("corrupt WAL file {path}: {reason}")]
    Corrupt { path: PathBuf, reason: String },
    #[error("unsupported WAL version {v} in {path} (max {SUPPORTED_VERSION})")]
    UnsupportedVersion { path: PathBuf, v: u32 },
}

impl WalError {
    fn is_not_found(&self) -> bool {
        matches!(self, WalError::Io(e) if e.kind() == io::ErrorKind::NotFound)
    }
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

    let filename = format!("{}_{}.wal", Ulid::new(), session_id);
    write_verified(dir, &filename, "f", ops, session_id, user)
}

/// Hashes every byte that passes through it.
struct HashingWriter<W> {
    inner: W,
    hasher: DefaultHasher,
    len: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: DefaultHasher::new(),
            len: 0,
        }
    }

    fn digest(&self) -> (u64, u64) {
        (self.hasher.finish(), self.len)
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.write(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn write_line<W: Write, T: Serialize>(w: &mut W, value: &T) -> Result<(), WalError> {
    serde_json::to_writer(&mut *w, value)?;
    w.write_all(b"\n")?;
    Ok(())
}

/// Atomically writes a WAL file and proves it landed:
///
/// 1. stream to `.{filename}.tmp`, hashing as we go
/// 2. fsync (best-effort — some network mounts reject it)
/// 3. rename to `filename`, fsync the directory (best-effort)
/// 4. read the final file back and compare its hash and length
///
/// Callers may only rely on (e.g. delete sources because of) a file this
/// function returned `Ok` for.
fn write_verified(
    dir: &Path,
    filename: &str,
    file_type: &str,
    ops: &[Op],
    session_id: &str,
    user: &str,
) -> Result<PathBuf, WalError> {
    let final_path = dir.join(filename);
    let tmp_path = dir.join(format!(".{}.tmp", filename));

    let written = (|| -> Result<(u64, u64), WalError> {
        let file = fs::File::create(&tmp_path)?;
        let mut w = HashingWriter::new(BufWriter::new(file));

        let header = WalHeader {
            v: SUPPORTED_VERSION,
            t: file_type.into(),
            n: ops.len(),
            lo: ops.iter().map(|o| o.op_id).min().unwrap(),
            hi: ops.iter().map(|o| o.op_id).max().unwrap(),
        };
        write_line(&mut w, &header)?;
        let debug = WalDebug {
            sid: session_id.into(),
            user: user.into(),
            at: chrono::Utc::now(),
        };
        write_line(&mut w, &debug)?;
        for op in ops {
            write_line(&mut w, op)?;
        }

        let digest = w.digest();
        let file = w.inner.into_inner().map_err(|e| e.into_error())?;
        if FSYNC.load(Ordering::Relaxed)
            && let Err(e) = file.sync_all()
        {
            eprintln!(
                "warning: fsync failed for {}: {}; relying on read-back verification",
                tmp_path.display(),
                e
            );
        }
        Ok(digest)
    })();

    let expected = match written {
        Ok(d) => d,
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }
    };

    if let Err(e) = fs::rename(&tmp_path, &final_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e.into());
    }
    if FSYNC.load(Ordering::Relaxed) {
        sync_dir(dir);
    }

    let mut reread = HashingWriter::new(io::sink());
    io::copy(&mut fs::File::open(&final_path)?, &mut reread)?;
    if reread.digest() != expected {
        // Our own file, known bad: remove it so readers don't trip on it.
        // The caller still holds the ops and can retry.
        let _ = fs::remove_file(&final_path);
        return Err(WalError::Corrupt {
            path: final_path,
            reason: "read-back verification failed".into(),
        });
    }

    Ok(final_path)
}

/// Persists the rename. Best-effort: not meaningful on SMB, and std can't
/// open a directory handle on Windows.
fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
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

    // Refuse newer formats outright: parsing them with this build's schema
    // would silently drop fields we don't know about.
    if header.v == 0 || header.v > SUPPORTED_VERSION {
        return Err(WalError::UnsupportedVersion {
            path: path.to_path_buf(),
            v: header.v,
        });
    }

    // v2 has a debug line after the header — skip it.
    if header.v >= 2 {
        lines.next().ok_or_else(|| WalError::Corrupt {
            path: path.to_path_buf(),
            reason: "missing debug line".into(),
        })??;
    }

    // Don't trust `n` for allocation — a corrupt header could claim 2^60 ops.
    let mut ops = Vec::with_capacity(header.n.min(4096));
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

fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string()
}

/// Reader that tracks which WAL files have already been consumed.
///
/// A file is marked processed only once its ops are returned. Files that
/// can't be read (partially arrived, corrupt, newer version, transient IO
/// error) are skipped and retried on the next `consume`. Files that vanish
/// were removed by a compactor, whose output carries their ops.
pub struct WalReader {
    dir: PathBuf,
    processed: BTreeSet<String>,
    /// In-memory quarantine: filename → last error. Never replicated.
    failing: HashMap<String, String>,
}

impl WalReader {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            processed: BTreeSet::new(),
            failing: HashMap::new(),
        }
    }

    /// Consume all new (unprocessed) WAL files and return their ops in order.
    pub fn consume(&mut self) -> Result<Vec<Op>, WalError> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let files = list_wal_files(&self.dir)?;
        let mut all_ops = Vec::new();

        let listed: BTreeSet<String> = files.iter().map(|p| file_name(p)).collect();
        self.failing.retain(|name, _| listed.contains(name));

        for path in files {
            let filename = file_name(&path);
            if self.processed.contains(&filename) {
                continue;
            }

            match read_wal_file(&path) {
                Ok((_header, ops)) => {
                    all_ops.extend(ops);
                    self.processed.insert(filename.clone());
                    self.failing.remove(&filename);
                }
                Err(e) if e.is_not_found() => {}
                Err(e) => {
                    let msg = e.to_string();
                    if self.failing.get(&filename) != Some(&msg) {
                        eprintln!("skipping WAL file {} (will retry): {}", path.display(), msg);
                    }
                    self.failing.insert(filename, msg);
                }
            }
        }

        Ok(all_ops)
    }

    /// Returns the set of processed filenames.
    pub fn processed_files(&self) -> &BTreeSet<String> {
        &self.processed
    }

    /// Files currently being skipped, with the last error seen for each.
    pub fn failing_files(&self) -> &HashMap<String, String> {
        &self.failing
    }
}

/// Compact the WAL directory into a single compact file.
///
/// Reads every file it can, merges them (LWW + purge), writes a verified
/// `.compact.wal`, then deletes exactly the files it read. Files it could not
/// read are left untouched — they may be partially arrived, from a newer
/// version, or belong to another writer mid-sync. A file that disappears
/// mid-compaction was taken by a concurrent compactor and is skipped.
///
/// Returns the path of the new compact file, or None if nothing was compacted.
pub fn compact(dir: &Path, session_id: &str) -> Result<Option<PathBuf>, WalError> {
    let files = list_wal_files(dir)?;

    let mut state = MergeState::default();
    let mut source_files: Vec<PathBuf> = Vec::new();

    for path in &files {
        match read_wal_file(path) {
            Ok((_header, ops)) => {
                for op in ops {
                    state.apply(op);
                }
                source_files.push(path.clone());
            }
            Err(e) if e.is_not_found() => {}
            Err(e) => {
                eprintln!("compaction: leaving {} in place: {}", path.display(), e);
            }
        }
    }

    if source_files.is_empty() {
        return Ok(None);
    }

    let mut merged_ops = state.into_ops();
    // Sort by op_id for deterministic output.
    merged_ops.sort_by_key(|op| op.op_id);

    let compact_path = if merged_ops.is_empty() {
        // Every source was verified readable and held no ops.
        None
    } else {
        let filename = format!("{}_{}.compact.wal", Ulid::new(), session_id);
        Some(write_verified(
            dir,
            &filename,
            "c",
            &merged_ops,
            session_id,
            "compactor",
        )?)
    };

    // Only reached once the compact file is verified on disk.
    for path in &source_files {
        let _ = fs::remove_file(path);
    }

    Ok(compact_path)
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
