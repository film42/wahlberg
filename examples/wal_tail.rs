use std::collections::BTreeSet;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use clap::Parser;
use wahlberg::wal;

#[derive(Parser)]
#[command(name = "wal-tail", about = "Tail a WAL directory and log changes as they land")]
struct Args {
    /// Path to the WAL directory
    #[arg(long)]
    wal_dir: PathBuf,

    /// Follow mode — keep watching for new files
    #[arg(short, long, default_value_t = false)]
    follow: bool,

    /// Poll interval in milliseconds (only used with -f)
    #[arg(long, default_value_t = 500)]
    interval: u64,
}

fn main() {
    let args = Args::parse();

    if !args.wal_dir.exists() {
        eprintln!("error: WAL directory does not exist: {}", args.wal_dir.display());
        std::process::exit(1);
    }

    let mut processed: BTreeSet<String> = BTreeSet::new();

    loop {
        let files = match wal::list_wal_files(&args.wal_dir) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("error listing WAL files: {}", e);
                if args.follow {
                    thread::sleep(Duration::from_millis(args.interval));
                    continue;
                } else {
                    std::process::exit(1);
                }
            }
        };

        for path in files {
            let filename = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            if processed.contains(&filename) {
                continue;
            }

            processed.insert(filename.clone());

            match wal::read_wal_file(&path) {
                Ok((header, ops)) => {
                    let is_compact = header.t == "c";

                    if is_compact {
                        // Compaction file — just log the summary, not every tuple.
                        let mut tables: BTreeSet<&str> = BTreeSet::new();
                        for op in &ops {
                            tables.insert(&op.tbl);
                        }
                        println!(
                            "\x1b[33m[compact]\x1b[0m {} — {} tuples across {} tables [{}]",
                            filename,
                            ops.len(),
                            tables.len(),
                            tables.into_iter().collect::<Vec<_>>().join(", "),
                        );
                        continue;
                    }

                    // Fragment file — log each op.
                    let user = ops.first().map(|o| o.user.as_str()).unwrap_or("?");
                    println!(
                        "\x1b[36m[wal]\x1b[0m {} — {} ops by \x1b[1m{}\x1b[0m",
                        filename,
                        header.n,
                        user,
                    );

                    for op in &ops {
                        use wahlberg::eavc::OpType;
                        let op_label = match op.op {
                            OpType::Create => "\x1b[32m+\x1b[0m",
                            OpType::Update => "\x1b[33m~\x1b[0m",
                            OpType::Delete => "\x1b[31m-\x1b[0m",
                        };

                        let value_str = match &op.value {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };

                        // Truncate long values.
                        let display_val = if value_str.len() > 80 {
                            format!("{}...", &value_str[..77])
                        } else {
                            value_str
                        };

                        println!(
                            "  {} {}/{}.{} = {}",
                            op_label, op.tbl, op.id, op.field, display_val,
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "\x1b[31m[error]\x1b[0m {} — {}",
                        filename, e,
                    );
                }
            }
        }

        if !args.follow {
            break;
        }

        thread::sleep(Duration::from_millis(args.interval));
    }
}
