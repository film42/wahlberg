# Wahlberg

An append-only WAL (Write-Ahead Log) protocol and storage engine for offline-first, multi-writer applications. Data replicates through a shared directory of immutable NDJSON files. No server, no coordination, no locks.

## Data Model: EAVC

Every mutation in the system is a single **op** — one field-level fact about one entity:

| Concept | Description |
|---------|-------------|
| **E** (Entity) | Identified by `(tbl, id)`. `tbl` is a freeform table name, `id` is a unique entity identifier. |
| **A** (Attribute) | The `field` being set. Freeform string. |
| **V** (Value) | The value for that field. Any JSON-representable scalar. |
| **C** (Context) | Provenance: `tx` (transaction ULID, which carries the timestamp), `user` (user ID of who wrote it). |

An entity is not a single record — it is the set of all winning facts for a given `(tbl, id)` pair, materialized by applying LWW across all ops.

### Op Schema

```json
{
  "tx":   "01KRMC0JM9WYZJD74TJ1K1DRB8",
  "tbl":  "accounts",
  "id":   "acc-42",
  "op":   "U",
  "field":"name",
  "value":"\"Alice\"",
  "user": "Garrett"
}
```

| Field   | Type   | Description |
|---------|--------|-------------|
| `tx`    | ULID   | Transaction ID, shared by every op in the transaction. It is the op's **timestamp and LWW key** in one field (see [Why `tx` is a ULID](#why-tx-is-a-ulid)). |
| `tbl`   | string | Table/collection name. Freeform — any string is valid. |
| `id`    | string | Entity identifier. Unique within a table. |
| `op`    | string | `"C"` (create), `"U"` (update), or `"D"` (delete). **Informational only** — the materializer does not branch on this. All ops are idempotent writes. |
| `field` | string | The attribute being set. System fields start with `_` (see [Tombstones](#tombstones)). |
| `value` | string | **JSON-stringified** value. `"\"Alice\""` for a string, `"42"` for a number, `"true"` for a boolean. Parsed back to a typed value on read. |
| `user`  | string | User ID of the author. In systems without a user registry, this may be a display name. |

### Why `tx` is a ULID

A ULID is a 48-bit millisecond Unix timestamp followed by 80 random bits, encoded as 26 characters of Crockford base32. So one `tx` field carries everything that a separate timestamp and a unique tiebreak ID would:

- **Time:** the op's timestamp is the first 10 characters (`01KRMC0JM9` → `2026-05-14T23:09:11.177Z`). Every ULID library can extract it; there is no separate `ts` field because it would duplicate these bits.
- **Order:** comparing two ULIDs compares their timestamps first and their random bits second, which gives LWW a total order with a deterministic tiebreak. The canonical encoding is fixed-width, so string order equals ULID order.

Implementations MUST compare `tx` as a ULID (or as its canonical uppercase string). Crockford base32 is case-insensitive, so a lowercase `tx` denotes the same value.

This is purely a space saving over storing `ts` + a per-op ID: 34 bytes per op instead of 68.

### Value Encoding

Values are double-encoded on the wire: the JSON value is stringified into a JSON string field. This preserves type information (distinguishes `42` from `"42"` from `true`) without requiring a schema.

| In-memory value | Wire encoding |
|-----------------|---------------|
| `"Alice"` (string) | `"\"Alice\""` |
| `42` (number) | `"42"` |
| `true` (boolean) | `"true"` |
| `null` | `"null"` |

Implementations MUST parse the `value` field as a JSON string, then parse its contents as JSON to recover the typed value.

## WAL File Format

WAL files are NDJSON (newline-delimited JSON). Each file has three sections:

```
{header}
{debug}
{op}
{op}
...
```

### Header (line 1)

```json
{"v":3,"t":"f","n":9,"lo":"01KRM...","hi":"01KRM..."}
```

| Field | Type   | Description |
|-------|--------|-------------|
| `v`   | integer | Schema version. Current: `3`. v3 is not backwards compatible with v1/v2. Readers MUST NOT parse files with a version they don't support (fields would be silently dropped or misread), and compactors MUST NOT merge or delete them. |
| `t`   | string  | File type: `"f"` = fragment, `"c"` = compact. |
| `n`   | integer | Number of ops in the file (not counting header or debug lines). |
| `lo`  | ULID    | Lowest `tx` in the file. |
| `hi`  | ULID    | Highest `tx` in the file. |

### Debug (line 2)

```json
{"sid":"JT883PRP","user":"Garrett","at":"2026-05-14T23:09:11.684Z"}
```

| Field  | Type   | Description |
|--------|--------|-------------|
| `sid`  | string | Session identifier of the writer. |
| `user` | string | User ID of the writer. |
| `at`   | string | ISO 8601 timestamp of when the file was written. |

The debug line is metadata for humans and tooling. Readers MUST skip it (do not parse as an op).

### Ops (lines 3+)

One op per line, as defined in [Op Schema](#op-schema). The number of op lines MUST equal the header's `n` field. If it doesn't, the file is corrupt and MUST be skipped.

### File Naming

```
{ULID}_{sessionId}.wal           # fragment
{ULID}_{sessionId}.compact.wal   # compact file
```

The leading ULID ensures files sort in creation order. Files starting with `.` are in-progress temporary files and MUST be ignored by readers.

### Atomic Writes

Writers MUST write atomically:

1. Write to a temporary file: `.{filename}.tmp`
2. Flush / fsync (best-effort — some network mounts reject fsync)
3. Rename to the final filename
4. Read the final file back and verify it matches what was written (hash + length)

A writer MUST NOT treat a file as committed (e.g. clear its outbox, or delete compaction sources) until step 4 succeeds.

This ensures readers never see a partial file. Any filesystem that supports atomic rename (local, SMB, NFS, FUSE mounts) is a valid transport.

## Conflict Resolution: Last-Write-Wins (LWW)

Every `(tbl, id, field)` tuple has exactly one winning value at any point in time, determined by LWW:

**Higher `tx` wins.** Because a ULID's timestamp is its most significant part, this means the later transaction wins; within the same millisecond, the random bits decide deterministically. The one exception is `_purge` (see [Tombstones](#tombstones)).

This means:
- Two users editing **different fields** on the same entity: both writes survive. No conflict.
- Two users editing the **same field** at the same time: the later `tx` wins. Deterministic, no coordination needed.
- Any set of ops applied in any order produces the **same final state**. The system is convergent.

### Materialization

To materialize an entity `(tbl, id)`:

1. Collect all ops where `tbl` and `id` match.
2. For each unique `field`, keep only the LWW-winning op.
3. The entity's fields are the winning values. If `_deleted` is `true`, the entity is soft-deleted. If `_purge` is `true`, the entity is purged (see below).

## Tombstones

System fields (prefixed with `_`) control entity lifecycle. They participate in LWW like any other field.

### `_deleted` — Soft Delete

Setting `_deleted` to `true` marks an entity as soft-deleted. The entity's field data is preserved. Readers SHOULD exclude soft-deleted entities from default queries but MAY expose them through explicit "include deleted" APIs.

**Auto-restore rule:** If any non-system field has a `tx` greater than the `_deleted` field's `tx`, the entity is considered alive. This handles the case where user A deletes an entity while user B (who hasn't synced yet) edits a field — the edit wins, and the entity comes back. This is intentional: if someone was actively editing it, the delete was premature.

To undo a soft delete explicitly, write `_deleted` with value `false`.

### `_purge` — Hard Delete

Setting `_purge` to `true` permanently removes an entity. Unlike soft delete:

- The materializer MUST drop **all** field tuples for the entity from its internal state, retaining only the `_purge` tombstone.
- The compactor MUST strip all non-`_purge` tuples for the entity, emitting only the tombstone.
- The `_purge` tombstone MUST be retained and replicated so that other readers learn about the purge.
- Purged entities MUST NOT appear in any query, including "include deleted" queries.

Purge is eventual. Field tuples may exist in WAL files on disk until compaction runs. Over successive compaction passes, all field data for purged entities is eroded from the filesystem. The tombstone itself persists indefinitely (it is the only record that the entity ever existed and should not be re-created).

**Purge wins over everything.** A `_purge` tombstone cannot be overridden by a newer field write, nor by a `_purge` write with any other value, regardless of `tx`. For the `_purge` field, `true` beats every non-`true` value; among `true` values, normal LWW applies. Once purged, the entity is gone.

This irreversibility is load-bearing: it is what makes it safe for a compactor with a partial view of the WAL to strip field data. If purge could be undone, a compactor that saw the purge but not the later un-purge would destroy data permanently.

## Transactions

Every op produced by one logical action on one entity shares the same `tx`. `tx` is the transaction ID: group ops by `tx` to recover transaction boundaries.

Writers MUST:
- Generate one `tx` per action and assign it to every op the action produces.
- Set each field at most once per transaction.

A shared `tx` means transactions never tear. Two transactions that land in the same millisecond still compare the same way on every field, so one wins entirely. They can't end up with A winning field `a` while B wins field `b`.

### Choosing `tx`

A writer SHOULD generate a fresh ULID for `tx`, but it MUST be strictly greater than (a) the last `tx` it wrote and (b) every `tx` it has seen on the target entity. If the fresh ULID isn't greater, increment the largest of those by one instead (monotonic ULID increment).

This guarantees that a write always beats the state it was made against. Rapid edits never tie. An edit or delete made after syncing a write from a peer whose clock runs ahead still sticks, instead of silently losing. And a delete is never instantly undone by auto-restore. Because the floor is bumped by incrementing its random bits, the embedded time only runs ahead of the wall clock when the floor itself was ahead. In effect, `tx` behaves like a hybrid logical clock.

## Replication

Replication requires nothing more than a shared directory that supports "create file" and "list files."

### Writing (Publishing)

1. Produce ops from user actions.
2. Buffer ops in an outbox.
3. When ready to publish, write all buffered ops to a single WAL file (atomic write).

One WAL file = one batch. This provides application-level atomicity — readers either see all ops in a file or none.

### Reading (Consuming)

1. List all `.wal` files in the directory, sorted by filename (ULID order).
2. Skip files that have already been processed (track by filename).
3. For each new file: parse the header, skip the debug line, read the ops.
4. Apply each op to the materializer using LWW.
5. Mark the file as processed.

Readers MUST handle:
- **Unreadable files:** If `header.n` doesn't match the actual op count, JSON or UTF-8 parsing fails, the version is unsupported, or any other IO error occurs, skip the file and log a warning. Do NOT mark it processed — retry it on the next read. On shared drives and sync folders, "corrupt" often means "not fully arrived yet".
- **Missing files:** Files may be removed by compaction between listing and reading. Treat as a no-op (and not as processed); the compactor's output carries their ops.
- A file is marked processed only once its ops have been applied. An error on one file MUST NOT discard ops from other files.
- **Duplicate ops:** LWW is idempotent. Re-applying the same op produces the same result.

### Convergence

Because LWW is deterministic and order-independent, any two readers that have seen the same set of ops will have identical materialized state, regardless of the order they processed the files.

## Compaction

Over time, the WAL directory accumulates many small fragment files. Compaction merges them into a single compact file.

### Algorithm

1. List all WAL files in the directory.
2. Read all ops from all files.
3. LWW merge: for each unique `(tbl, id, field)`, keep only the winning op.
4. **Purge pass:** for any entity with a winning `_purge` tombstone, drop all non-`_purge` tuples.
5. Write a new `.compact.wal` file (atomic write, type `"c"` in the header).
6. Delete the source files only after the compact file is verified on disk (see [Atomic Writes](#atomic-writes)).

The source set is exactly the files the compactor successfully read. Files it could not read (partial, corrupt, newer version) are never deleted — deleting them could destroy another writer's data. Files that vanish mid-compaction were taken by a concurrent compactor and are skipped.

### Properties

- **Correctness:** Compaction produces exactly the same materialized state as reading all source files. It is a pure optimization.
- **Idempotency:** Running compaction twice produces the same result. Two concurrent compactors may produce duplicate compact files, but LWW ensures correctness.
- **Purge erosion:** Each compaction pass strips field tuples for purged entities. After one pass, only the `_purge` tombstone remains for that entity.

### Safety

- Only delete source files after the compact file is committed and verified on disk.
- Only delete files that were successfully read. Never delete a file because it failed to parse.
- If a new fragment file appears between the read and delete phases, it will NOT be deleted (it wasn't in the source list). It will be picked up by the next compaction or by readers.
- In multi-writer environments, consider a file-level lock (`.compact.lock`) to prevent concurrent compactors from racing. The protocol is correct without it, but concurrent compaction wastes work.

## Rust Library Usage

```rust
use wahlberg::store::Store;
use serde_json::Value;

// Open a store backed by a WAL directory.
let mut store = Store::open("./wal", "my-session", "Garrett");

// Sync: read all new WAL files from disk.
store.sync()?;

// Write operations — buffered in memory until flush.
store.create("contacts", "c-1", &[
    ("name",  Value::String("Alice".into())),
    ("email", Value::String("alice@example.com".into())),
])?;

store.update("contacts", "c-1", &[
    ("phone", Value::String("555-1234".into())),
])?;

// Soft delete (reversible, fields preserved).
store.delete("contacts", "c-1")?;

// Hard delete (permanent, fields eroded by compaction).
store.purge("contacts", "c-1")?;

// Flush buffered ops to a WAL file on disk.
store.flush()?;

// Read.
let entity = store.get("contacts", "c-1");
let all_contacts = store.list("contacts");
let tables = store.tables();
```

### Typed Records

Derive `Record` on a serde struct to get a typed table instead of field tuples (the `derive` feature, on by default):

```rust
use serde::{Deserialize, Serialize};
use wahlberg::Record;

#[derive(Serialize, Deserialize, Record)]
#[record(table = "contacts")]   // default: the struct name in snake_case
struct Contact {
    id: String,                 // or mark another field with #[record(id)]
    name: String,
    email: String,
    #[serde(default)]           // fields added later just work
    phone: Option<String>,
}

let mut contacts = store.table::<Contact>();
contacts.insert(&contact)?;                                    // new record: writes every field
let c: Option<Contact> = contacts.get("c-1")?;
let all: Vec<Contact> = contacts.list()?;
contacts.update("c-1", |c| c.email = "new@example.com".into())?; // writes only `email`
contacts.delete("c-1")?;
contacts.purge("c-1")?;
```

- **Each serde key is one field.** Nested structs and `Vec`s are stored as a single field. serde attributes (`rename`, `rename_all`, `default`, `skip_serializing_if`) apply as usual.
- **`update` writes only the fields that changed.** Two teammates editing different fields of the same record both keep their edits. Whole-record saves are intentionally not offered, because they would overwrite a teammate's concurrent edit with a stale value.
- **The id is the entity id, not a stored field.** It must be string-like (`AsRef<str>`) and can't be changed by `update`.
- **Reading is fallible.** If stored data doesn't fit the struct (another client wrote a different type), `get` returns an error naming the record instead of panicking. Fields the struct doesn't know about are ignored and left untouched.
- **Rules:** `insert` fails if the id is live or purged. Inserting over a soft-deleted id brings it back. Field names starting with `_` are reserved.

`Record` is a small trait (`TABLE`, `ID_FIELD`, `id()`), so you can implement it by hand without the derive.

### Examples

```bash
# Typed records: two teammates edit the same contact concurrently
cargo run --example records

# Export WAL to SQLite (rebuilds the output to mirror current state;
# purged data is removed and overwritten via PRAGMA secure_delete)
cargo run --example wal-exporter -- --wal-dir ./wal --output-db ./out.db

# Tail WAL directory (one-shot or follow mode)
cargo run --example wal-tail -- --wal-dir ./wal
cargo run --example wal-tail -- --wal-dir ./wal -f
```

## Implementing in Another Language

To implement a compatible reader/writer, you need:

1. **ULID generation** — with monotonic increment support (for [Choosing `tx`](#choosing-tx)). Libraries exist for every major language.
2. **NDJSON parsing** — read one JSON object per line.
3. **A `HashMap<(tbl, id, field), Fact>` for materialized state** — where `Fact` holds `{value, tx}`. On each op, compare with the existing fact using LWW rules and keep the winner.
4. **Atomic file writes** — write to tmp, rename to final.
5. **File listing + sorting** — list `.wal` files, sort lexicographically (ULID order), track which ones you've processed.

That's it. There is no handshake, no protocol negotiation, no schema registry. The shared directory IS the protocol.

### Pseudocode: Materializer

```
function wins(new, old):
    if new.field == "_purge" and is_purge(new) != is_purge(old):
        return is_purge(new)            // purge is irreversible
    return new.tx > old.tx             // ULID order: time, then random bits

function apply(op, current_state):
    if op.field != "_purge" and is_purged(op.tbl, op.id, current_state):
        return                          // purged entities never regain fields

    key = (op.tbl, op.id, op.field)
    existing = current_state.get(key)
    if existing is not None and not wins(op, existing):
        return

    was_purged = is_purged(op.tbl, op.id, current_state)
    current_state.set(key, op)

    if is_purge(op) and not was_purged:
        // drop all tuples for this (tbl, id) except _purge
        for each key (t, eid, f) in current_state:
            if t == op.tbl and eid == op.id and f != "_purge":
                current_state.remove(key)
```

### Pseudocode: Reader

```
function consume(wal_dir, processed_set):
    files = list_wal_files(wal_dir)   // sorted by filename
    ops = []

    for file in files:
        if file.name in processed_set:
            continue

        lines = read_lines(file)
        header = parse_json(lines[0])
        // skip lines[1] (debug line)

        file_ops = []
        for line in lines[2..]:
            op = parse_json(line)
            op.value = parse_json(op.value)   // double-decode
            file_ops.append(op)

        if len(file_ops) != header.n:
            log_warning("corrupt file, skipping")
            continue

        ops.extend(file_ops)
        processed_set.add(file.name)

    return ops
```

## Design Assumptions

- **Clocks are reasonably synchronized.** LWW depends on timestamps being "close enough" (NTP-level). A machine with a clock 5 minutes ahead will win conflicts against *concurrent* writes. Writes made after syncing its data still win, because of the rule in [Choosing `tx`](#choosing-tx). This is acceptable for small teams on real computers; not suitable for adversarial or high-clock-skew environments.
- **Writers are trusted.** Any writer can write any field to any table. There is no schema enforcement at the protocol level. Validation belongs in the application layer.
- **The shared directory is durable.** The protocol assumes files, once committed, are not silently corrupted or truncated by the filesystem. It handles missing files (compaction may remove them) and corrupt files (header validation) gracefully.

## Future Work

### Hybrid Logical Clocks (HLC)

[Choosing `tx`](#choosing-tx) already gives the main HLC guarantee: if peer A writes, and peer B syncs and then writes the same entity, B's write wins even if B's wall clock is behind. It does this with no extra wire fields.

A full HLC would extend that from "the entity you wrote" to "everything you've synced" (advance the local clock on every remote op, not just on the target entity's). It has the same tradeoff as any HLC: one machine with a badly wrong clock drags everyone's `tx` forward. That's worth doing only if cross-entity causality starts to matter.

References:
- Kulkarni et al., "Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases" (2014)
- The `hlc` and `uhlc` Rust crates
