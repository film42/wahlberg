# Wahlberg

An append-only WAL (Write-Ahead Log) protocol and storage engine for offline-first, multi-writer applications. Data replicates through a shared directory of immutable NDJSON files. No server, no coordination, no locks.

## Data Model: EAVC

Every mutation in the system is a single **op** — one field-level fact about one entity:

| Concept | Description |
|---------|-------------|
| **E** (Entity) | Identified by `(tbl, id)`. `tbl` is a freeform table name, `id` is a unique entity identifier. |
| **A** (Attribute) | The `field` being set. Freeform string. |
| **V** (Value) | The value for that field. Any JSON-representable scalar. |
| **C** (Context) | Provenance: `opId` (ULID), `ts` (ISO 8601 timestamp), `user` (user ID of who wrote it). |

An entity is not a single record — it is the set of all winning facts for a given `(tbl, id)` pair, materialized by applying LWW across all ops.

### Op Schema

```json
{
  "opId": "01KRMC0JM9WYZJD74TJ1K1DRB8",
  "tbl":  "accounts",
  "id":   "acc-42",
  "op":   "U",
  "field":"name",
  "value":"\"Alice\"",
  "ts":   "2026-05-14T23:09:11.176Z",
  "user": "Garrett"
}
```

| Field   | Type   | Description |
|---------|--------|-------------|
| `opId`  | ULID   | Globally unique, monotonic within a session. Used as LWW tiebreaker. |
| `tbl`   | string | Table/collection name. Freeform — any string is valid. |
| `id`    | string | Entity identifier. Unique within a table. |
| `op`    | string | `"C"` (create), `"U"` (update), or `"D"` (delete). **Informational only** — the materializer does not branch on this. All ops are idempotent writes. |
| `field` | string | The attribute being set. System fields start with `_` (see [Tombstones](#tombstones)). |
| `value` | string | **JSON-stringified** value. `"\"Alice\""` for a string, `"42"` for a number, `"true"` for a boolean. Parsed back to a typed value on read. |
| `ts`    | string | ISO 8601 UTC timestamp. All ops in a single transaction share the same `ts`. |
| `user`  | string | User ID of the author. In systems without a user registry, this may be a display name. |

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
{"v":2,"t":"f","n":9,"lo":"01KRM...","hi":"01KRM..."}
```

| Field | Type   | Description |
|-------|--------|-------------|
| `v`   | integer | Schema version. Current: `2`. |
| `t`   | string  | File type: `"f"` = fragment, `"c"` = compact. |
| `n`   | integer | Number of ops in the file (not counting header or debug lines). |
| `lo`  | ULID    | Lowest `opId` in the file. |
| `hi`  | ULID    | Highest `opId` in the file. |

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
2. Flush / fsync
3. Rename to the final filename

This ensures readers never see a partial file. Any filesystem that supports atomic rename (local, SMB, NFS, FUSE mounts) is a valid transport.

## Conflict Resolution: Last-Write-Wins (LWW)

Every `(tbl, id, field)` tuple has exactly one winning value at any point in time, determined by LWW:

1. **Higher `ts` wins.** The op with the later timestamp takes precedence.
2. **On tie, higher `opId` wins.** ULIDs are lexicographically comparable, providing a deterministic tiebreaker.

This means:
- Two users editing **different fields** on the same entity: both writes survive. No conflict.
- Two users editing the **same field** at the same time: the later timestamp wins. Deterministic, no coordination needed.
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

**Auto-restore rule:** If any non-system field has a `ts` newer than the `_deleted` field's `ts`, the entity is considered alive. This handles the case where user A deletes an entity while user B (who hasn't synced yet) edits a field — the edit wins, and the entity comes back. This is intentional: if someone was actively editing it, the delete was premature.

To undo a soft delete explicitly, write `_deleted` with value `false`.

### `_purge` — Hard Delete

Setting `_purge` to `true` permanently removes an entity. Unlike soft delete:

- The materializer MUST drop **all** field tuples for the entity from its internal state, retaining only the `_purge` tombstone.
- The compactor MUST strip all non-`_purge` tuples for the entity, emitting only the tombstone.
- The `_purge` tombstone MUST be retained and replicated so that other readers learn about the purge.
- Purged entities MUST NOT appear in any query, including "include deleted" queries.

Purge is eventual. Field tuples may exist in WAL files on disk until compaction runs. Over successive compaction passes, all field data for purged entities is eroded from the filesystem. The tombstone itself persists indefinitely (it is the only record that the entity ever existed and should not be re-created).

**Purge wins over everything.** A `_purge` tombstone cannot be overridden by a newer field write. Once purged, the entity is gone.

## Transactions

There is no dedicated transaction ID. All ops in a single logical action MUST share the same `ts` value. This is the transaction boundary: `(id, ts)` groups ops that were part of the same mutation.

Writers MUST:
- Generate a single timestamp at the start of an action.
- Assign that timestamp to every op produced by that action.
- Generate a unique `opId` (ULID) for each individual op.

Readers that need to identify transaction boundaries can group ops by `(id, ts)`.

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
- **Corrupt files:** If `header.n` doesn't match the actual op count, or JSON parsing fails, skip the file and log a warning.
- **Missing files:** Files may be removed by compaction between listing and reading. Treat as a no-op.
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
6. Delete the source files only after the compact file is successfully written.

### Properties

- **Correctness:** Compaction produces exactly the same materialized state as reading all source files. It is a pure optimization.
- **Idempotency:** Running compaction twice produces the same result. Two concurrent compactors may produce duplicate compact files, but LWW ensures correctness.
- **Purge erosion:** Each compaction pass strips field tuples for purged entities. After one pass, only the `_purge` tombstone remains for that entity.

### Safety

- Only delete source files after the compact file is committed to disk.
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

### Examples

```bash
# Export WAL to SQLite
cargo run --example wal-exporter -- --wal-dir ./wal --output-db ./out.db

# Tail WAL directory (one-shot or follow mode)
cargo run --example wal-tail -- --wal-dir ./wal
cargo run --example wal-tail -- --wal-dir ./wal -f
```

## Implementing in Another Language

To implement a compatible reader/writer, you need:

1. **ULID generation** — monotonic within a session, globally unique. Libraries exist for every major language.
2. **NDJSON parsing** — read one JSON object per line.
3. **A `HashMap<(tbl, id, field), Fact>` for materialized state** — where `Fact` holds `{value, ts, opId}`. On each op, compare with the existing fact using LWW rules and keep the winner.
4. **Atomic file writes** — write to tmp, rename to final.
5. **File listing + sorting** — list `.wal` files, sort lexicographically (ULID order), track which ones you've processed.

That's it. There is no handshake, no protocol negotiation, no schema registry. The shared directory IS the protocol.

### Pseudocode: Materializer

```
function apply(op, current_state):
    key = (op.tbl, op.id, op.field)
    existing = current_state.get(key)

    if existing is None:
        current_state.set(key, op)
        return

    if op.ts > existing.ts:
        current_state.set(key, op)     // newer timestamp wins
    else if op.ts == existing.ts and op.opId > existing.opId:
        current_state.set(key, op)     // tiebreak: higher ULID wins

    if op.field == "_purge" and op.value == true:
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

- **Clocks are reasonably synchronized.** LWW depends on timestamps being "close enough" (NTP-level). A machine with a clock 5 minutes ahead will silently win all conflicts. This is acceptable for small teams on real computers; not suitable for adversarial or high-clock-skew environments.
- **Writers are trusted.** Any writer can write any field to any table. There is no schema enforcement at the protocol level. Validation belongs in the application layer.
- **The shared directory is durable.** The protocol assumes files, once committed, are not silently corrupted or truncated by the filesystem. It handles missing files (compaction may remove them) and corrupt files (header validation) gracefully.

## Future Work

### Hybrid Logical Clocks (HLC)

The current protocol uses wall-clock timestamps (`ts`) for LWW ordering. This works well when peers have reasonably synchronized clocks (NTP), but breaks down in environments with significant clock skew — a peer whose clock is ahead will silently win every conflict, even when its writes are causally later.

A Hybrid Logical Clock replaces the raw wall-clock timestamp with a `(physical_time, logical_counter, node_id)` tuple. The physical component tracks real time (like `ts` does today), but the logical counter increments when a node receives an event with a physical time equal to or greater than its own — ensuring that causally-later events always have a higher HLC value, even if the wall clock hasn't advanced.

What this would change in the protocol:

- The `ts` field would become an HLC value instead of a raw ISO 8601 timestamp. The wire encoding could be a single sortable string (e.g. `"{physical_ms}-{logical}-{node}"`) to preserve lexicographic comparison.
- LWW comparison would use HLC ordering instead of raw timestamp comparison. The ULID tiebreak would remain as a fallback for true ties.
- Writers would need to maintain HLC state: on each local event, `physical = max(wall_clock, last_physical)` and increment the logical counter. On receiving remote ops (during sync), advance the local HLC if the remote HLC is ahead.
- Transaction grouping (`(id, ts)`) would still work — all ops in a transaction share the same HLC value.

The key benefit is **causal consistency without coordination**: if peer A writes, peer B syncs and then writes, B's write is guaranteed to have a higher HLC than A's — even if B's wall clock is behind. This eliminates the silent-winner problem for peers that communicate, while remaining compatible with the append-only WAL architecture.

HLC adds minimal overhead (one counter per node, one comparison per op) and does not require changes to the file format, compaction, or replication model. It is a drop-in replacement for the timestamp comparison in the LWW function.

References:
- Kulkarni et al., "Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases" (2014)
- The `hlc` and `uhlc` Rust crates
