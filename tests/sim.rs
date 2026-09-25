//! Chaotic lifecycle simulation.
//!
//! N simulated teammates share one WAL directory. Each step, one of them does
//! something: insert / update / delete / purge (with cascade) records, flush,
//! sync, compact (full, partial, racing another compactor, or crashing
//! halfway), crash and restart, or the environment misbehaves (a flushed file
//! arrives only partially, stray temp files, files from a newer version).
//! Everything is single-threaded and seeded, so a seed replays a run exactly.
//!
//! Correctness is measured against *acknowledged* ops — ops whose `flush()`
//! returned Ok, i.e. what the app told the user was saved:
//!
//!   durability   after every compaction/crash step, merging what's on disk
//!                equals the spec model of all acknowledged ops
//!   converge     at the end, every session, a fresh reader, and a fresh
//!                reader after a final compaction all equal that model
//!   read-own     after a successful update, the session sees its own write
//!                (runs with synced clocks only: under clock skew a later
//!                edit can lose to a future-dated write, an accepted v3
//!                limitation)
//!   decode       typed records always decode (nothing torn)
//!   orphans      after settling, no task references a purged project
//!   junk         files of another version are never deleted
//!
//! Inserts mostly mint fresh ids, so the data set grows over the run; a
//! share of them reuse an old id to exercise already-exists / purged /
//! revive-after-delete.
//!
//! Knobs: SIM_SEED, SIM_RUNS (2), SIM_STEPS (1000), SIM_SESSIONS (4),
//! SIM_REUSE (percent of inserts that reuse an id, 15), SIM_SKEW (unset:
//! odd seeds run with skewed clocks — one session 5s ahead, the rest within
//! ±300ms — and even seeds with synced clocks; 0/1 forces it).
//!
//!     SIM_STEPS=20000 SIM_SESSIONS=6 cargo test --release --test sim -- --nocapture

mod common;

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use common::{EntityKey, Rng, View, diff, env_u64, random_seed, store_view};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use walburg::eavc::Op;
use walburg::store::Store;
use walburg::{Record, RecordError, wal};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Record)]
#[record(table = "projects")]
struct Project {
    id: String,
    name: String,
    #[serde(default)]
    owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Record)]
#[record(table = "tasks")]
struct Task {
    id: String,
    project: String,
    title: String,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    points: i64,
}

#[derive(Clone, Copy)]
struct Cfg {
    steps: u64,
    sessions: usize,
    reuse_pct: u64,
    /// None = decided per seed (odd seeds skewed).
    skew: Option<bool>,
}

struct Session {
    name: String,
    incarnation: u32,
    skew_ms: i64,
    store: Store,
    /// A half-arrived file has landed since this session last synced, so it
    /// may still hold incomplete records.
    stale: bool,
}

type Failure = (&'static str, String);

struct Sim {
    cfg: Cfg,
    /// Session clocks disagree this run.
    skewed: bool,
    dir: PathBuf,
    scratch: PathBuf,
    _root: tempfile::TempDir,
    rng: Rng,
    /// Simulated wall clock (ms), shared with every session's tx source.
    clock: Arc<AtomicU64>,
    sessions: Vec<Session>,
    /// Every op whose flush returned Ok.
    acked: Vec<Op>,
    /// Flushed files that have only partially arrived: name → full bytes.
    inflight: BTreeMap<String, Vec<u8>>,
    /// Newer-version files that must never be deleted.
    junk: Vec<PathBuf>,
    /// Every id ever minted, per table, and both together for views.
    projects: Vec<String>,
    tasks: Vec<String>,
    entities: Vec<EntityKey>,
    step: u64,
    file_seq: u64,
    stats: BTreeMap<&'static str, u64>,
    max_files: usize,
    log: VecDeque<String>,
}

const PROJECT_NAMES: &[&str] = &["Apollo", "Borealis", "Cascade", "Delta", "Ember"];
const TITLES: &[&str] = &[
    "Draft spec",
    "Fix bug",
    "Review PR",
    "Ship it",
    "Write docs",
    "Refactor",
];

impl Sim {
    fn new(cfg: Cfg, seed: u64) -> Self {
        let skewed = cfg.skew.unwrap_or(seed % 2 == 1);
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("share");
        let scratch = root.path().join("scratch");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&scratch).unwrap();

        let mut sim = Sim {
            cfg,
            skewed,
            dir,
            scratch,
            _root: root,
            rng: Rng(seed),
            clock: Arc::new(AtomicU64::new(1_800_000_000_000)),
            sessions: Vec::new(),
            acked: Vec::new(),
            inflight: BTreeMap::new(),
            junk: Vec::new(),
            projects: Vec::new(),
            tasks: Vec::new(),
            entities: Vec::new(),
            step: 0,
            file_seq: 0,
            stats: BTreeMap::new(),
            max_files: 0,
            log: VecDeque::new(),
        };
        for i in 0..cfg.sessions {
            let skew_ms = match i {
                _ if !skewed => 0,
                0 => 0,
                1 => 5_000,
                _ => sim.rng.below(600) as i64 - 300,
            };
            let store = sim.open_store(&format!("u{i}"), 0, skew_ms);
            sim.sessions.push(Session {
                name: format!("u{i}"),
                incarnation: 0,
                skew_ms,
                store,
                stale: false,
            });
        }
        sim
    }

    fn open_store(&mut self, name: &str, incarnation: u32, skew_ms: i64) -> Store {
        let mut store = Store::open(&self.dir, &format!("{name}-{incarnation}"), name);
        let clock = self.clock.clone();
        let mut r = Rng(self.rng.next());
        store.set_tx_source(move || {
            let now = (clock.load(Ordering::Relaxed) as i64 + skew_ms).max(0) as u64;
            Ulid::from_parts(
                now,
                ((r.next() as u128) << 16) | (r.next() as u128 & 0xFFFF),
            )
        });
        store.sync().unwrap();
        store
    }

    fn bump(&mut self, stat: &'static str) {
        *self.stats.entry(stat).or_default() += 1;
    }

    fn note(&mut self, s: usize, what: String) {
        if self.log.len() == 40 {
            self.log.pop_front();
        }
        self.log.push_back(format!(
            "#{} {}: {}",
            self.step, self.sessions[s].name, what
        ));
    }

    fn next_name(&mut self, suffix: &str) -> String {
        self.file_seq += 1;
        format!("{:010}_{suffix}", self.file_seq)
    }

    /// A fresh id most of the time; sometimes one that was used before.
    fn mint_or_reuse(&mut self, table: &'static str) -> String {
        let reuse = self.rng.chance(self.cfg.reuse_pct);
        let pool = if table == "projects" {
            &self.projects
        } else {
            &self.tasks
        };
        if reuse && !pool.is_empty() {
            return self.rng.pick(pool).clone();
        }
        let id = if table == "projects" {
            format!("p{:05}", self.projects.len())
        } else {
            format!("t{:06}", self.tasks.len())
        };
        if table == "projects" {
            self.projects.push(id.clone());
        } else {
            self.tasks.push(id.clone());
        }
        self.entities.push((table.to_string(), id.clone()));
        id
    }

    fn expected(&self) -> View {
        common::oracle(&self.acked, &self.entities, true)
    }

    // ------------------------------------------------------------------
    // one step
    // ------------------------------------------------------------------

    fn step(&mut self) -> Result<(), Failure> {
        self.step += 1;
        // At least 1ms per step, so with synced clocks a later step's write
        // is always later than an earlier step's.
        let advance = 1 + self.rng.below(25) as u64;
        self.clock.fetch_add(advance, Ordering::Relaxed);
        let s = self.rng.below(self.sessions.len());

        // (weight, action)
        const ACTIONS: &[(u64, &str)] = &[
            (8, "insert_project"),
            (20, "insert_task"),
            (25, "update_task"),
            (6, "update_project"),
            (6, "delete_task"),
            (2, "delete_project"),
            (1, "purge_task"),
            (1, "purge_project"),
            (14, "flush"),
            (12, "sync"),
            (2, "compact_full"),
            (3, "compact_partial"),
            (1, "compact_race"),
            (1, "compact_crash"),
            (1, "crash"),
            (4, "land_inflight"),
            (1, "stray_tmp"),
        ];
        let total: u64 = ACTIONS.iter().map(|(w, _)| w).sum();
        let mut roll = self.rng.next() % total;
        let action = ACTIONS
            .iter()
            .find(|(w, _)| {
                if roll < *w {
                    true
                } else {
                    roll -= w;
                    false
                }
            })
            .unwrap()
            .1;

        match action {
            "insert_project" => self.insert_project(s),
            "insert_task" => self.insert_task(s),
            "update_task" => self.update_task(s),
            "update_project" => self.update_project(s),
            "delete_task" => self.delete(s, "tasks"),
            "delete_project" => self.delete(s, "projects"),
            "purge_task" => self.purge_task(s),
            "purge_project" => self.purge_project(s),
            "flush" => self.flush(s, true),
            "sync" => self.sync(s),
            "compact_full" => self.compact_full(s),
            "compact_partial" => self.compact_partial(s),
            "compact_race" => self.compact_race(s),
            "compact_crash" => self.compact_crash(s),
            "crash" => self.crash(s),
            "land_inflight" => self.land_one(),
            "stray_tmp" => self.stray_tmp(),
            _ => unreachable!(),
        }?;

        let files = wal::list_wal_files(&self.dir).unwrap().len();
        self.max_files = self.max_files.max(files);
        Ok(())
    }

    // ------------------------------------------------------------------
    // record actions
    // ------------------------------------------------------------------

    fn alive_projects(&mut self, s: usize) -> Vec<String> {
        self.sessions[s].store.table::<Project>().ids()
    }

    fn insert_project(&mut self, s: usize) -> Result<(), Failure> {
        let id = self.mint_or_reuse("projects");
        let p = Project {
            id: id.clone(),
            name: self.rng.pick(PROJECT_NAMES).to_string(),
            owner: None,
        };
        let res = self.sessions[s].store.table::<Project>().insert(&p);
        self.record_result("insert_project", s, &id, res.map(|_| ()))
    }

    fn insert_task(&mut self, s: usize) -> Result<(), Failure> {
        let projects = self.alive_projects(s);
        if projects.is_empty() {
            return Ok(());
        }
        let id = self.mint_or_reuse("tasks");
        let t = Task {
            id: id.clone(),
            project: self.rng.pick(&projects).clone(),
            title: self.rng.pick(TITLES).to_string(),
            done: false,
            points: self.rng.below(8) as i64,
        };
        let res = self.sessions[s].store.table::<Task>().insert(&t);
        self.record_result("insert_task", s, &id, res.map(|_| ()))
    }

    fn update_task(&mut self, s: usize) -> Result<(), Failure> {
        let ids = self.sessions[s].store.table::<Task>().ids();
        if ids.is_empty() {
            return Ok(());
        }
        let id = self.rng.pick(&ids).clone();
        let projects = self.alive_projects(s);
        let (a, b, c, d) = (
            self.rng.chance(50),
            self.rng.chance(40),
            self.rng.chance(40),
            self.rng.chance(10),
        );
        let title = self.rng.pick(TITLES).to_string();
        let points = self.rng.below(13) as i64;
        let move_to = (!projects.is_empty()).then(|| self.rng.pick(&projects).clone());
        let res = self.sessions[s].store.table::<Task>().update(&id, |t| {
            if a {
                t.title = title;
            }
            if b {
                t.done = !t.done;
            }
            if c {
                t.points = points;
            }
            if d && let Some(p) = move_to {
                t.project = p;
            }
        });
        if let (Ok(updated), false) = (&res, self.skewed) {
            let seen = self.sessions[s].store.table::<Task>().get(&id);
            if !matches!(&seen, Ok(Some(t)) if t == updated) {
                return Err((
                    "read-own",
                    format!("tasks/{id}: wrote {updated:?}, then read {seen:?}"),
                ));
            }
        }
        self.record_result("update_task", s, &id, res.map(|_| ()))
    }

    fn update_project(&mut self, s: usize) -> Result<(), Failure> {
        let ids = self.alive_projects(s);
        if ids.is_empty() {
            return Ok(());
        }
        let id = self.rng.pick(&ids).clone();
        let owner = format!("owner{}", self.rng.below(5));
        let name = self.rng.pick(PROJECT_NAMES).to_string();
        let rename = self.rng.chance(50);
        let res = self.sessions[s].store.table::<Project>().update(&id, |p| {
            p.owner = Some(owner);
            if rename {
                p.name = name;
            }
        });
        self.record_result("update_project", s, &id, res.map(|_| ()))
    }

    fn delete(&mut self, s: usize, table: &'static str) -> Result<(), Failure> {
        let ids = match table {
            "tasks" => self.sessions[s].store.table::<Task>().ids(),
            _ => self.alive_projects(s),
        };
        if ids.is_empty() {
            return Ok(());
        }
        let id = self.rng.pick(&ids).clone();
        self.sessions[s].store.delete(table, &id).unwrap();
        self.bump(if table == "tasks" {
            "delete_task"
        } else {
            "delete_project"
        });
        self.note(s, format!("delete {table}/{id}"));
        Ok(())
    }

    fn purge_task(&mut self, s: usize) -> Result<(), Failure> {
        if self.tasks.is_empty() {
            return Ok(());
        }
        let id = self.rng.pick(&self.tasks).clone();
        if self.sessions[s]
            .store
            .get_including_deleted("tasks", &id)
            .is_none()
        {
            return Ok(());
        }
        self.sessions[s].store.purge("tasks", &id).unwrap();
        self.bump("purge_task");
        self.note(s, format!("purge tasks/{id}"));
        Ok(())
    }

    /// Purges a project and, as the app's cascade, every task this session
    /// can see that belongs to it. Tasks it can't see yet are caught later
    /// by `repair_orphans`.
    fn purge_project(&mut self, s: usize) -> Result<(), Failure> {
        if self.projects.is_empty() {
            return Ok(());
        }
        let id = self.rng.pick(&self.projects).clone();
        if self.sessions[s]
            .store
            .get_including_deleted("projects", &id)
            .is_none()
        {
            return Ok(());
        }
        self.sessions[s].store.purge("projects", &id).unwrap();
        let children = self.tasks_of(s, &id);
        for t in &children {
            self.sessions[s].store.purge("tasks", t).unwrap();
        }
        self.bump("purge_project");
        *self.stats.entry("cascade_purged_tasks").or_default() += children.len() as u64;
        self.note(
            s,
            format!("purge projects/{id} (+{} tasks)", children.len()),
        );
        Ok(())
    }

    /// Tasks (live or soft-deleted) this session sees under `project`.
    fn tasks_of(&self, s: usize, project: &str) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|t| {
                self.sessions[s]
                    .store
                    .get_including_deleted("tasks", t)
                    .is_some_and(|e| {
                        e.fields.get("project").and_then(|v| v.as_str()) == Some(project)
                    })
            })
            .cloned()
            .collect()
    }

    /// The cascade's repair pass: purge tasks whose project is purged.
    fn repair_orphans(&mut self, s: usize) -> usize {
        let orphans: Vec<String> = self
            .tasks
            .iter()
            .filter(|t| {
                self.sessions[s]
                    .store
                    .get_including_deleted("tasks", t)
                    .and_then(|e| {
                        e.fields
                            .get("project")
                            .and_then(|v| v.as_str())
                            .map(String::from)
                    })
                    .is_some_and(|p| self.sessions[s].store.is_purged("projects", &p))
            })
            .cloned()
            .collect();
        for t in &orphans {
            self.sessions[s].store.purge("tasks", t).unwrap();
        }
        orphans.len()
    }

    fn record_result(
        &mut self,
        stat: &'static str,
        s: usize,
        id: &str,
        res: Result<(), RecordError>,
    ) -> Result<(), Failure> {
        match res {
            Ok(()) => {
                self.bump(stat);
                self.note(s, format!("{stat} {id}"));
                Ok(())
            }
            Err(
                RecordError::AlreadyExists { .. }
                | RecordError::Purged { .. }
                | RecordError::NotFound { .. },
            ) => {
                self.bump("rejected");
                Ok(())
            }
            Err(RecordError::Serde { .. }) if self.may_be_incomplete(s) => {
                self.bump("incomplete_seen");
                Ok(())
            }
            Err(e) => Err(("decode", format!("{stat} {id}: {e}"))),
        }
    }

    // ------------------------------------------------------------------
    // storage actions
    // ------------------------------------------------------------------

    fn flush(&mut self, s: usize, allow_inflight: bool) -> Result<(), Failure> {
        let res = self.sessions[s].store.flush();
        let path = match res {
            Ok(Some(p)) => p,
            Ok(None) => return Ok(()),
            Err(e) => return Err(("flush", e.to_string())),
        };
        // Acknowledged: record exactly what landed.
        let (_, ops) = wal::read_wal_file(&path).map_err(|e| ("flush", e.to_string()))?;
        let n = ops.len();
        self.acked.extend(ops);

        let sid = format!("{}-{}", self.sessions[s].name, self.sessions[s].incarnation);
        let name = self.next_name(&format!("{sid}.wal"));
        let dest = self.dir.join(&name);
        fs::rename(&path, &dest).unwrap();

        if allow_inflight && self.rng.chance(12) {
            // Sync-folder style: the file is visible but only half there.
            let full = fs::read(&dest).unwrap();
            fs::write(&dest, &full[..full.len() / 2]).unwrap();
            self.inflight.insert(name.clone(), full);
            self.bump("inflight");
        }
        self.bump("flush");
        *self.stats.entry("acked_ops").or_default() += n as u64;
        self.note(s, format!("flush {n} ops -> {name}"));
        Ok(())
    }

    fn sync(&mut self, s: usize) -> Result<(), Failure> {
        self.sessions[s]
            .store
            .sync()
            .map_err(|e| ("sync", e.to_string()))?;
        self.sessions[s].stale = false;
        let repaired = self.repair_orphans(s);
        *self.stats.entry("orphans_repaired").or_default() += repaired as u64;
        self.check_decode(s)?;
        self.bump("sync");
        self.note(s, format!("sync (repaired {repaired} orphans)"));
        Ok(())
    }

    /// Whether session `s` may legitimately see incomplete records: some
    /// file is still half-arrived, or one landed since `s` last synced.
    fn may_be_incomplete(&self, s: usize) -> bool {
        !self.inflight.is_empty() || self.sessions[s].stale
    }

    /// Records may be incomplete only while `may_be_incomplete`; otherwise
    /// every record must decode.
    fn check_decode(&mut self, s: usize) -> Result<(), Failure> {
        let store = &mut self.sessions[s].store;
        let mut invalid = store.table::<Project>().invalid();
        invalid.extend(store.table::<Task>().invalid());
        if invalid.is_empty() {
            return Ok(());
        }
        if !self.may_be_incomplete(s) {
            return Err((
                "decode",
                format!(
                    "{} undecodable record(s), e.g. {}",
                    invalid.len(),
                    invalid[0]
                ),
            ));
        }
        *self.stats.entry("incomplete_seen").or_default() += invalid.len() as u64;
        Ok(())
    }

    /// Renames a compactor's output to a deterministic name and moves it
    /// into the share.
    fn adopt_output(&mut self, path: &Path) {
        let name = self.next_name("cmp.compact.wal");
        fs::rename(path, self.dir.join(name)).unwrap();
    }

    fn compact_full(&mut self, s: usize) -> Result<(), Failure> {
        let out = wal::compact(&self.dir, "cmp").map_err(|e| ("compact", e.to_string()))?;
        if let Some(p) = out {
            self.adopt_output(&p);
        }
        self.bump("compact_full");
        self.note(s, "compact full".into());
        self.check_durability("compact_full")
    }

    /// Moves a random subset of files into `side` (what a compactor with a
    /// stale listing would see) and returns their names.
    fn take_subset(&mut self, side: &Path, copy: Option<&Path>) -> Vec<String> {
        fs::create_dir_all(side).unwrap();
        let mut taken = Vec::new();
        for f in wal::list_wal_files(&self.dir).unwrap() {
            if self.rng.chance(60) {
                let name = f.file_name().unwrap().to_string_lossy().to_string();
                if let Some(copy) = copy {
                    fs::create_dir_all(copy).unwrap();
                    fs::copy(&f, copy.join(&name)).unwrap();
                }
                fs::rename(&f, side.join(&name)).unwrap();
                taken.push(name);
            }
        }
        taken
    }

    /// Runs a compactor over `side`, then returns everything left there
    /// (outputs and untouched files) to the share.
    fn compact_side(&mut self, side: &Path) -> Result<(), Failure> {
        let out = wal::compact(side, "cmp").map_err(|e| ("compact", e.to_string()))?;
        if let Some(p) = &out {
            self.adopt_output(p);
        }
        for f in fs::read_dir(side).unwrap() {
            let f = f.unwrap().path();
            fs::rename(&f, self.dir.join(f.file_name().unwrap())).unwrap();
        }
        fs::remove_dir(side).unwrap();
        Ok(())
    }

    fn compact_partial(&mut self, s: usize) -> Result<(), Failure> {
        let side = self.scratch.join("side");
        let taken = self.take_subset(&side, None);
        self.compact_side(&side)?;
        self.bump("compact_partial");
        self.note(s, format!("compact partial ({} files)", taken.len()));
        self.check_durability("compact_partial")
    }

    /// Two compactors with the same snapshot: A compacts and deletes the
    /// sources; B compacted a copy and publishes a duplicate afterwards.
    fn compact_race(&mut self, s: usize) -> Result<(), Failure> {
        let side_a = self.scratch.join("race_a");
        let side_b = self.scratch.join("race_b");
        let taken = self.take_subset(&side_a, Some(&side_b));
        self.compact_side(&side_a)?;
        if !taken.is_empty() {
            let out = wal::compact(&side_b, "cmp2").map_err(|e| ("compact", e.to_string()))?;
            if let Some(p) = out {
                self.adopt_output(&p);
            }
            // B's leftovers are copies of files A already returned.
            fs::remove_dir_all(&side_b).unwrap();
        }
        self.bump("compact_race");
        self.note(s, format!("compact race ({} files)", taken.len()));
        self.check_durability("compact_race")
    }

    /// A compactor crashes after publishing its output but before deleting
    /// any sources: everything is left duplicated.
    fn compact_crash(&mut self, s: usize) -> Result<(), Failure> {
        let backup = self.scratch.join("backup");
        fs::create_dir_all(&backup).unwrap();
        let before = wal::list_wal_files(&self.dir).unwrap();
        for f in &before {
            fs::copy(f, backup.join(f.file_name().unwrap())).unwrap();
        }
        let out = wal::compact(&self.dir, "cmp").map_err(|e| ("compact", e.to_string()))?;
        if let Some(p) = out {
            self.adopt_output(&p);
        }
        for f in &before {
            if !f.exists() {
                fs::copy(backup.join(f.file_name().unwrap()), f).unwrap();
            }
        }
        fs::remove_dir_all(&backup).unwrap();
        self.bump("compact_crash");
        self.note(s, "compact crash (sources kept)".into());
        self.check_durability("compact_crash")
    }

    /// The app dies: unflushed edits are lost (they were never acknowledged).
    fn crash(&mut self, s: usize) -> Result<(), Failure> {
        let lost = self.sessions[s].store.pending_ops();
        *self.stats.entry("lost_unflushed_ops").or_default() += lost as u64;
        let (name, skew) = (self.sessions[s].name.clone(), self.sessions[s].skew_ms);
        self.sessions[s].incarnation += 1;
        let inc = self.sessions[s].incarnation;
        self.sessions[s].store = self.open_store(&name, inc, skew);
        self.sessions[s].stale = false;
        self.bump("crash");
        self.note(s, format!("crash (lost {lost} unflushed ops)"));
        self.check_durability("crash")
    }

    fn land_one(&mut self) -> Result<(), Failure> {
        let Some(name) = self.inflight.keys().next().cloned() else {
            return Ok(());
        };
        self.land(&name)
    }

    fn land(&mut self, name: &str) -> Result<(), Failure> {
        let full = self.inflight.remove(name).unwrap();
        let path = self.dir.join(name);
        if !path.exists() {
            return Err((
                "durability",
                format!("in-flight file {name} was deleted before it fully arrived"),
            ));
        }
        fs::write(&path, full).unwrap();
        for sess in &mut self.sessions {
            sess.stale = true;
        }
        self.bump("landed");
        Ok(())
    }

    fn stray_tmp(&mut self) -> Result<(), Failure> {
        let name = format!(".{}_stray.wal.tmp", self.step);
        fs::write(self.dir.join(name), b"{\"v\":3,\"t\":\"f\",\"n\":9").unwrap();
        self.bump("stray_tmp");
        Ok(())
    }

    fn add_junk_version_file(&mut self) {
        let id = Ulid::from_parts(1_700_000_000_000, 7);
        let body = format!(
            "{{\"v\":4,\"t\":\"f\",\"n\":1,\"lo\":\"{id}\",\"hi\":\"{id}\"}}\n\
             {{\"sid\":\"future\",\"user\":\"u\",\"at\":\"2030-01-01T00:00:00Z\"}}\n\
             {{\"tx\":\"{id}\",\"tbl\":\"tasks\",\"id\":\"t000000\",\"op\":\"U\",\"field\":\"title\",\
             \"value\":\"\\\"from the future\\\"\",\"user\":\"u\"}}\n"
        );
        let path = self.dir.join("0000000000_future.wal");
        fs::write(&path, body).unwrap();
        self.junk.push(path);
    }

    // ------------------------------------------------------------------
    // checks
    // ------------------------------------------------------------------

    /// Everything readable on disk (plus the full contents of in-flight
    /// files), merged, must equal the model of every acknowledged op.
    fn check_durability(&mut self, after: &str) -> Result<(), Failure> {
        let mut on_disk: Vec<Op> = Vec::new();
        for f in wal::list_wal_files(&self.dir).unwrap() {
            let name = f.file_name().unwrap().to_string_lossy().to_string();
            let path = match self.inflight.get(&name) {
                Some(full) => {
                    let p = self.scratch.join("inflight.wal");
                    fs::write(&p, full).unwrap();
                    p
                }
                None => f.clone(),
            };
            match wal::read_wal_file(&path) {
                Ok((_, ops)) => on_disk.extend(ops),
                Err(_) if self.junk.contains(&f) => {}
                Err(e) => {
                    return Err((
                        "durability",
                        format!("unexpected unreadable file {name}: {e}"),
                    ));
                }
            }
        }
        let expected = self.expected();
        let actual = common::oracle(&on_disk, &self.entities, true);
        if actual != expected {
            let mut detail = format!("after {after}:\n{}", diff(&expected, &actual));
            for (k, e) in &expected {
                if actual.get(k) != Some(e) {
                    detail.push_str(&format!(
                        "  {}/{} acked ops:\n{}",
                        k.0,
                        k.1,
                        op_lines(&self.acked, k)
                    ));
                    detail.push_str(&format!(
                        "  {}/{} on-disk ops:\n{}",
                        k.0,
                        k.1,
                        op_lines(&on_disk, k)
                    ));
                }
            }
            return Err(("durability", detail));
        }
        Ok(())
    }

    /// Lands everything, flushes and syncs everyone, runs the cascade repair
    /// to a fixpoint, then checks convergence and the end-state invariants.
    fn settle_and_check(&mut self) -> Result<(), Failure> {
        let names: Vec<String> = self.inflight.keys().cloned().collect();
        for name in names {
            self.land(&name)?;
        }
        for s in 0..self.sessions.len() {
            self.flush(s, false)?;
        }
        loop {
            let mut repaired = 0;
            for s in 0..self.sessions.len() {
                self.sessions[s]
                    .store
                    .sync()
                    .map_err(|e| ("sync", e.to_string()))?;
                self.sessions[s].stale = false;
                repaired += self.repair_orphans(s);
                self.flush(s, false)?;
            }
            *self.stats.entry("orphans_repaired").or_default() += repaired as u64;
            if repaired == 0 {
                break;
            }
        }
        for s in 0..self.sessions.len() {
            self.sessions[s]
                .store
                .sync()
                .map_err(|e| ("sync", e.to_string()))?;
        }
        self.check_durability("settle")?;

        let expected = self.expected();
        for s in 0..self.sessions.len() {
            let view = store_view(&self.sessions[s].store, &self.entities);
            if view != expected {
                return Err((
                    "converge",
                    format!(
                        "session {}:\n{}",
                        self.sessions[s].name,
                        diff(&expected, &view)
                    ),
                ));
            }
            self.check_decode(s)?;
        }
        let fresh = self.fresh_view();
        if fresh != expected {
            return Err((
                "converge",
                format!("fresh reader:\n{}", diff(&expected, &fresh)),
            ));
        }
        let out = wal::compact(&self.dir, "final").map_err(|e| ("compact", e.to_string()))?;
        if let Some(p) = out {
            self.adopt_output(&p);
        }
        let compacted = self.fresh_view();
        if compacted != expected {
            return Err((
                "converge",
                format!("after final compaction:\n{}", diff(&expected, &compacted)),
            ));
        }

        // No live or soft-deleted task may point at a purged project.
        for ((t, id), v) in &expected {
            if let (true, Some((_, fields))) = (t == "tasks", v) {
                let project = fields.get("project").cloned().unwrap_or_default();
                let key = (
                    "projects".to_string(),
                    project.trim_matches('"').to_string(),
                );
                if matches!(expected.get(&key), Some(None)) {
                    return Err((
                        "orphans",
                        format!("tasks/{id} still references purged {}", key.1),
                    ));
                }
            }
        }
        for j in &self.junk {
            if !j.exists() {
                return Err(("junk", format!("{} was deleted", j.display())));
            }
        }
        Ok(())
    }

    fn fresh_view(&self) -> View {
        let mut r = Store::open(&self.dir, "verifier", "verifier");
        r.sync().unwrap();
        store_view(&r, &self.entities)
    }

    fn summary(&self, seed: u64, elapsed: std::time::Duration) -> String {
        let expected = self.expected();
        let alive = expected
            .values()
            .filter(|v| matches!(v, Some((false, _))))
            .count();
        let deleted = expected
            .values()
            .filter(|v| matches!(v, Some((true, _))))
            .count();
        let bytes: u64 = wal::list_wal_files(&self.dir)
            .unwrap()
            .iter()
            .map(|f| f.metadata().unwrap().len())
            .sum();
        let stats: Vec<String> = self.stats.iter().map(|(k, v)| format!("{k}={v}")).collect();
        format!(
            "seed={seed} steps={} sessions={} clocks={} in {:.2?}\n  entities: alive={alive} soft-deleted={deleted} gone={} | files: max={} final={} ({} KB)\n  {}",
            self.step,
            self.sessions.len(),
            if self.skewed { "skewed" } else { "synced" },
            elapsed,
            expected.len() - alive - deleted,
            self.max_files,
            wal::list_wal_files(&self.dir).unwrap().len(),
            bytes / 1024,
            stats.join(" "),
        )
    }

    fn failure_report(&self, seed: u64, (check, detail): &Failure) -> String {
        let log: Vec<&str> = self.log.iter().map(String::as_str).collect();
        format!(
            "[FAIL {check}] seed={seed} at step {}\n{detail}\n  last actions:\n    {}\n  replay: SIM_SEED={seed} SIM_RUNS=1 cargo test --test sim -- --nocapture",
            self.step,
            log.join("\n    "),
        )
    }
}

/// The ops for one entity, oldest first, one per line.
fn op_lines(ops: &[Op], (tbl, id): &EntityKey) -> String {
    let mut mine: Vec<&Op> = ops
        .iter()
        .filter(|o| &o.tbl == tbl && &o.id == id)
        .collect();
    mine.sort_by_key(|o| (o.tx, o.field.clone()));
    mine.dedup_by(|a, b| a.tx == b.tx && a.field == b.field);
    mine.iter()
        .map(|o| format!("      {} {}={} ({})\n", o.tx, o.field, o.value, o.user))
        .collect()
}

fn run(cfg: Cfg, seed: u64) -> Result<String, String> {
    walburg::wal::set_fsync(false);
    let started = Instant::now();
    let mut sim = Sim::new(cfg, seed);
    sim.add_junk_version_file();
    for _ in 0..cfg.steps {
        if let Err(f) = sim.step() {
            return Err(sim.failure_report(seed, &f));
        }
    }
    if let Err(f) = sim.settle_and_check() {
        return Err(sim.failure_report(seed, &f));
    }
    Ok(sim.summary(seed, started.elapsed()))
}

#[test]
fn chaotic_lifecycle() {
    let cfg = Cfg {
        steps: env_u64("SIM_STEPS", 1000),
        sessions: env_u64("SIM_SESSIONS", 4) as usize,
        reuse_pct: env_u64("SIM_REUSE", 15),
        skew: std::env::var("SIM_SKEW").ok().map(|v| v != "0"),
    };
    let runs = env_u64("SIM_RUNS", 2);
    let base = env_u64("SIM_SEED", random_seed());

    let mut failures = Vec::new();
    for r in 0..runs {
        let seed = base.wrapping_add(r);
        match run(cfg, seed) {
            Ok(summary) => println!("{summary}"),
            Err(report) => {
                println!("{report}");
                failures.push(seed);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "simulation failed for seeds {failures:?}"
    );
}
