# slimSync — Lightweight Edge Sync Client

> Pushes unstructured files to slimHub with high quality, security, and incrementality — without decrypting, peeking, or reloading content.

**[English](README.en.md) | [中文](README.md)**

---

## 1. Design Philosophy

slimSync sits at the very front of the "troika" (three-service architecture) and is bound by three iron rules:

1. **Slice accurately** — slice boundaries align with semantic structure to avoid semantic fragmentation
2. **Slice fast** — impose no significant CPU/IO burden on the client machine
3. **Slice blind** — never decrypt file content; rely on Blind-IDs as the dedup credential

---

## 2. Dual-Track Hybrid Slicing Engine

To align with slimRagSvr's "anchor inheritance," slimSync uses a dual-track approach: structure track + compute track.

```
                                          ┌──────────────────┐
        file change events (notify) ─────►│  dual-track       │
                                          │  slice scheduler  │
                                          └────────┬─────────┘
                                                   │
                          ┌────────────────────────┼────────────────────────┐
                          ▼                        ▼                        ▼
              ┌─────────────────────┐   ┌──────────────────────┐   ┌──────────────────┐
              │ structure track:   │   │ compute track:       │   │ preprocess/append │
              │ AST skeleton       │   │ FastCDC rolling      │   │ detection         │
              │ (Tree-sitter stream)│  │ (Gear hash window)   │   │ (Last-Synced)     │
              └──────────┬──────────┘   └──────────┬───────────┘   └────────┬─────────┘
                         │                         │                        │
                         └────────────────┬────────┘                        │
                                          ▼                                 │
                              ┌──────────────────────┐                      │
                              │ structure-aware      │◄─────────────────────┘
                              │ merge (fine-tune cut │
                              │ to paragraph bounds) │
                              └──────────┬───────────┘
                                         ▼
                               ┌────────────────────┐
                               │ plaintext chunk    │
                               │ sequence           │
                               │ (with physical     │
                               │ offset bounds)     │
                               └────────────────────┘
```

### 2.1 Structure Track: Lightweight Streaming AST Skeleton

| File type | Parsing engine | Extracted skeleton |
|-----------|---------------|--------------------|
| Code (.rs/.go/.py/.c) | Tree-sitter streaming | Function/class/interface declaration bounds |
| Config (.yaml/.json/.toml) | Tree-sitter | Top-level Object/Array block bounds |
| Markdown (.md) | Tree-sitter | H1/H2 paragraph bounds |
| CAD/PCB (.brd/.sch) | Lightweight C-binding parser | Netlist segments, component symbol byte bounds |
| DXF | Lightweight C-binding parser | Geometric primitives, layer block bounds |

Physical output: an explicit array of physical boundaries (Offsets Matrix).

Note: **No compilation, no rendering** — only streaming lexical scanning, completed in seconds.

### 2.2 Compute Track: FastCDC Rolling-Hash Fallback

For long text, PDF tables, large log segments, or unrecognized binary assets, FastCDC sliding-window slicing kicks in.

Core parameters:
- Minimum slice: 2KB
- Average slice: 8KB
- Maximum slice: 64KB

### 2.3 Structure-Aware Merge

Key innovation: when FastCDC's sliding window searches for a cut-point, it preferentially aligns to the physical boundary array produced by the structure track.

```
Example:
  FastCDC plans to cut at byte 800
  structure track hints byte 850 is the end of a complete Markdown paragraph
  → the slice window automatically "adsorbs" backward to byte 850
```

Result: every Chunk has both perfect semantic structural continuity and FastCDC's natural rolling resistance to shifts.

---

## 3. On-Device Ledger (LMDB)

slimSync maintains an ultra-lightweight LMDB locally (Rust `heed` wrapper), acting as a "time memory mirror."

### 3.1 Database Design

#### db_file_offsets — resumable offsets for appended files

| Key | Value | Description |
|-----|-------|-------------|
| `file_ino + dev_id` (8B) | `last_synced_offset` (8B) + `file_mtime` (8B) | safely confirmed sync checkpoint |
| ... | ... | one record per file |

Purpose: for high-frequency append files (rolling logs, minute-order streams), only read the incremental delta from this pointer onward, natively supporting resumable sync.

#### db_chunk_hashes — versioned Blind-ID sequence chain

| Key | Value | Description |
|-----|-------|-------------|
| `file_blind_id` (16B) + `version_seq` (4B) | `[Blind-ID_1, Blind-ID_2, ...]` | fingerprint chain of chunks in this version |
| `Blind-ID` (16B) | `file_blind_id` (16B) + `offset` (4B) + `length` (4B) | Chunk → file mapping |

---

## 4. Blind Fingerprint & End-to-End Encryption

```
[ plaintext Chunk ] ──► HMAC-SHA256(Chunk, Pre-Shared-Salt) ──► Blind-ID (16B, for dedup)
      │
      ▼
ChaCha20-Poly1305 encryption ──► [ ciphertext Payload ] ──► sent to slimHub
```

### 4.1 Blind-ID Generation

```
Blind-ID = HMAC-SHA256(Chunk_Text, Pre-Shared-Salt)[0..16]
```

- Does not compute plain SHA-256 on plaintext (prevents rainbow-table attacks to reverse the original content)
- Fixed 16-byte length, serving as the unique global dedup credential
- Only Blind-IDs and ciphertext circulate through slimHub and the network — nothing human-readable can be recovered

### 4.2 E2EE Encryption

| Algorithm | Purpose |
|-----------|---------|
| ChaCha20-Poly1305 | symmetric encryption of chunk plaintext |
| Pre-Shared Key | distributed between edge and slimRagSvr via an out-of-band channel |

---

## 5. Zenoh State Machine: Ask Before Sending

slimSync does not rush to encrypt and send. Instead, it implements "ask-before-send" blind dedup via Zenoh's distributed Query-Reply.

```
slimSync (edge)                           slimHub / slimRagSvr
      │                                         │
      ├─ 1. file change → dual-track slicing    │
      ├─ 2. compute Blind-ID                   │
      │                                         │
      ├─ 3. zenoh.get("slim/status/exists/{id}") ──►
      ◄──────────────── 4. returns True / False ──┤
      │                                         │
      ├─ [if False] cold data ──────┐           │
      │  encrypt Payload            │           │
      │  zenoh.put("slim/sync/      │           │
      │    chunks/{cat}/{id}") ────►│           │
      │                             │           │
      ├─ [if True] exists globally ─┘           │
      │  skip ciphertext transfer               │
      │  zenoh.put("slim/sync/      │           │
      │    metadata/{file_id}") ────►           │
      │  (tens-of-bytes relation frame only)    │
```

### 5.1 State Machine States

| State | Action | Description |
|-------|--------|-------------|
| `IDLE` | wait for notify events | monitor file changes |
| `SLICING` | dual-track engine runs | generate chunk sequence |
| `QUERYING` | zenoh.get blind dedup query | ask whether it exists globally |
| `TRANSMITTING` | zenoh.put sends ciphertext | only cold data needs transfer |
| `META_ONLY` | zenoh.put sends relation frame | exists globally; only update pointer |
| `COMMITTED` | update local LMDB offset | confirm write complete |

### 5.2 Online/Offline Dual-Track Dedup Flow

After computing the Blind-ID, slimSync applies dual-track determination through `sent_hashes`:

```
                       compute Blind-ID
                            │
                            ▼
                  look up local sent_hashes table
                            │
             ┌───────────────┴───────────────┐
             ▼                               ▼
        hit, confirmed=1                  miss
             │                               │
             ▼                               ├── [online]  zenoh.get() query global
     skip physical transfer                  ├── [offline] encrypt directly into PUB queue
     only send relation frame                └── write sent_hashes confirmed=0
             │                               │
             ▼                               ▼
     compute cost 0%                   remote true → write sent_hashes confirmed=1
                                        remote false → encrypt & send, confirmed=0
```

### 5.3 Compute-Downshift Feedback

| Scenario | Bandwidth | slimRagSvr compute |
|----------|-----------|--------------------|
| Cold data (new Chunk) | transfers ciphertext ~8KB | needs Embedding |
| Hot data (exists globally) | **zero-byte transfer** | **0% compute (skips dedup Tokenize/Embedding)** |

---

## 6. On-Device Ledger: The SQLite-over-LMDB Tradeoff

> For crash consistency and checkpoint reconciliation, SQLite's ACID transactions and relational queries decisively outperform LMDB's pure KV model.

### 6.1 Why SQLite (not LMDB) on the Edge

| Dimension | LMDB (heed) | SQLite (rusqlite WAL) |
|-----------|-------------|------------------------|
| Data model | pure KV; relations need manual emulation | relational; native multi-dimensional queries and comparisons |
| Transactions | MVCC, single-KV ops | **full ACID via WAL**, multi-table transactional rollback |
| Crash recovery | page-level atomic, but no WAL rollback analysis | WAL log ensures rollback to last clean commit after power loss |
| Reconciliation | needs manual serialization | SQL natively supports intersect/difference operations |
| Edge footprint | ~100KB DB | ~1MB DB (acceptable) |

**Conclusion**: the edge does not need massive throughput — it needs **absolute transactional robustness and relational query capability**. SQLite (WAL mode) is the correct choice for the edge ledger.

### 6.2 Local SQLite Ledger Design

```sql
-- Core Checkpoint table
CREATE TABLE sync_checkpoints (
    file_path TEXT PRIMARY KEY,
    file_id_prefix BLOB,                  -- Blind-ID prefix of the file's initial metadata
    last_mtime_ns INTEGER NOT NULL,       -- file mtime at the last checkpoint
    last_verified_offset INTEGER NOT NULL,-- confirmed physical offset at the last checkpoint
    last_chunk_hash BLOB,                -- Blind-ID of the last successfully PUB'd Chunk
    st_dev INTEGER NOT NULL DEFAULT 0,   -- device number (detect CoW overwrite/rotation stand-in)
    st_ino INTEGER NOT NULL DEFAULT 0,   -- inode number (detect CoW overwrite/rotation stand-in)
    status TEXT DEFAULT 'IN_SYNC'         -- SYNCING / IN_SYNC / CRASHED
);

-- Versioned Blind-ID chain
CREATE TABLE chunk_hashes (
    file_path TEXT NOT NULL,
    version_seq INTEGER NOT NULL,
    blind_id BLOB NOT NULL,               -- 16B blind fingerprint
    chunk_offset INTEGER NOT NULL,
    chunk_length INTEGER NOT NULL,
    PRIMARY KEY (file_path, version_seq)
);

-- Sent fingerprint cache (resolves the offline blind-dedup paradox)
CREATE TABLE sent_hashes (
    blind_id BLOB PRIMARY KEY,
    file_path TEXT NOT NULL,
    sent_at INTEGER NOT NULL,
    confirmed INTEGER DEFAULT 0           -- 0=local PUB queue only, 1=confirmed persisted remotely
);
CREATE INDEX idx_sent_hashes_confirmed ON sent_hashes(confirmed);

-- Persistent dirty-page markers (plug debounce-window data loss)
CREATE TABLE dirty_files (
    file_path TEXT PRIMARY KEY,
    first_dirty_at INTEGER NOT NULL,      -- time of first Modify trigger
    last_dirty_at INTEGER NOT NULL        -- time of latest Modify trigger
);
```

---

## 7. On-Device Autonomy: Checkpoint-Based Crash Recovery

### 7.1 Core Principle

**slimSync does not depend on any remote (slimHub / slimRagSvr) for recovery.** The local SQLite ledger is the single source of truth, achieving a fully offline self-healing loop.

### 7.2 Checkpoint Definition

A Checkpoint is a dual-anchor signature of "timestamp + physical position":
> All data before this time point and before this file's physical offset has been confirmed by slimSync as successfully generated and committed to the local PUB buffer.

### 7.3 Crash Recovery State Machine

```
slimSync process starts
       │
       ▼
[1. read Checkpoint records from local SQLite]
       │
       ┌─────────────────┴─────────────────┐
       ▼                                   ▼
【mtime unchanged】                【mtime changed】
(no one touched the file           (file appended/modified)
 during the outage)
       │                                   │
       ▼                                   ▼
pointer stays put                 seek to last_verified_offset
keep listening for changes         structural plugin incremental slicing
                                   → encrypt → push into PUB queue
                                   → transactionally update Checkpoint
```

#### Case A: mtime == last_mtime_ns

Physical conclusion: the file was not written during the outage.

Action: trust `last_verified_offset`, keep the pointer in place, start `notify` and continue listening for changes. Physical I/O is zero.

#### Case B: mtime > last_mtime_ns

Physical conclusion: the file was appended or overwritten during the outage.

Action:
1. `seek(last_verified_offset)` — jump directly to the checkpoint position
2. The structural plugin incrementally slices forward from that position
3. Encrypt → compute Blind-ID → push into the Zenoh PUB buffer
4. Transactionally update `last_mtime_ns` and `last_verified_offset`

### 7.4 Extreme Crash: Defending Against Dirty Checkpoint Data

If power loss occurs within the window where "slicing/encryption finished but the SQLite transaction was not committed":

```
scan sync_checkpoints after restart
       │
       └─ status = 'SYNCING'
               │
               ▼
        read the last Chunk before last_verified_offset
        recompute Hash
               │
       ┌───────┴───────┐
       ▼               ▼
   Hash matches   Hash mismatch
   (data safe)    (disk hole)
       │               │
       ▼               ▼
    continue      roll back one full Chunk
    from offset   re-slice from safe boundary
```

### 7.5 Dirty-Page Fallback Recovery After Crash

After a process crash and restart, the first thing cold start does is fetch the `dirty_files` table:

```sql
-- find residual assets that were inside the debounce window and may have been lost before the crash
SELECT file_path, first_dirty_at, last_dirty_at
FROM dirty_files
ORDER BY first_dirty_at;
```

For each record, combined with `sync_checkpoints.last_verified_offset`, incrementally slice forward from that offset, ensuring zero data loss during the debounce window. After processing, delete the corresponding dirty-page marker and advance the Checkpoint in the same SQLite transaction.

---

## 8. Cold Start: How to Obtain the Change List

> This is the core missing link in earlier designs. On restart, slimSync faces a "black-box filesystem" and cannot magically know which files changed.

### 8.1 Full-Metadata Blind Scan (cross-platform, recommended MVP)

Read no file content — only VFS metadata (`stat`) — and perform relational diffing via a SQLite temp table.

```
slimSync starts
       │
       ▼
[1. multi-threaded metadata scan]
    use Rust walkdir to traverse monitored dirs at tens of thousands of files/sec
    capture only three fields: (file_path, mtime_ns, file_size)
    write into temp table temp_scan
       │
       ▼
[2. SQL relational diff → three change lists]
```

```sql
-- new files
SELECT scan.file_path FROM temp_scan scan
LEFT JOIN sync_checkpoints c ON scan.file_path = c.file_path
WHERE c.file_path IS NULL;

-- modified/appended files
SELECT scan.file_path FROM temp_scan scan
JOIN sync_checkpoints c ON scan.file_path = c.file_path
WHERE scan.mtime_ns > c.last_mtime_ns
   OR scan.file_size != c.last_verified_offset;

-- deleted files
SELECT c.file_path FROM sync_checkpoints c
LEFT JOIN temp_scan scan ON c.file_path = scan.file_path
WHERE scan.file_path IS NULL;
```

**Performance**: pure metadata + NVMe SSD, ~100–300ms for 100K files.

### 8.2 Platform-Specific Approaches

| Platform | Cold-start change list | Runtime tracking |
|----------|------------------------|------------------|
| **Windows** | **USN Journal API** (second-level, scan-free) | `ReadDirectoryChangesW` / continuous USN consumption |
| **Linux** | multi-threaded metadata diff scan | `fanotify` / `inotify` + in-memory debounce |
| **macOS** | multi-threaded metadata diff scan | `FSEvents` (built-in history backtrack) |

> **Why can't Linux read a Journal like Windows USN?**
> - The Linux Ext4/XFS Journal is a purely "crash-recovery log" recording physical sector changes, not file semantics
> - No user-space API exposes Journal content (forcibly reading it requires root + raw block parsing, equivalent to rewriting half a filesystem)
> - In contrast, Windows USN is a change-tracking API purpose-built for the application layer, with a stable public interface

---

## 9. Runtime High-Frequency Tracking

### 9.1 Kernel Event Driven

Using the Rust `notify` library (automatically adapting to each platform's kernel API):

| Platform | API |
|----------|-----|
| Linux | `inotify` / `fanotify` |
| macOS | `FSEvents` |
| Windows | `ReadDirectoryChangesW` |

It listens for three core events: `Create` / `Modify(Data)` / `Remove`.

### 9.2 WAL-Level Persistent Debounce (Dirty Flag)

For high-frequency write scenarios (e.g., logs at thousands of lines per second), pure in-memory debounce risks data loss on crash. Upgrade to persistent dirty-page markers:

```
OS kernel event stream (high-frequency Modify)
       │
       ▼
[1. persistent dirty-page marker]
   notify receives event → immediately write/update SQLite dirty_files table
   → data is safely persisted to disk
       │
       ▼
[2. hard delay window]
   set max hard delay 500ms
   timer check: current_time - first_dirty_at > 500ms
   → force-trigger slice loading (no infinite reset)
       │
       ▼
[3. incremental reconciliation & loading]
   1. read last_verified_offset from SQLite
   2. seek(offset) → read new physical bytes
   3. FastCDC slicing → encrypt → PUB queue
   4. transactionally advance Checkpoint + delete dirty_files record
```

### 9.3 Runtime Incremental Load Flow

```
1. query local SQLite
   read that file's last_verified_offset

2. physical locate + incremental read
   seek(offset)
   Δ = current_size - offset
   read only Δ bytes

3. slice + encrypt + PUB
   FastCDC rolling slicing
   ChaCha20 encryption
   zenoh.put push into async send queue

4. transactionally advance Checkpoint
   UPDATE sync_checkpoints
   SET last_verified_offset = current_size,
       last_mtime_ns = new_mtime
   WHERE file_path = ?
```

### 9.4 Special Boundary: Log Rotation

When `trade.log` is renamed and a new empty file created, the tracking mechanism must handle the anomaly `current_size < last_verified_offset`.

Upgrade the detection dimension: instead of relying solely on size comparison, introduce `(st_dev, st_ino)` two-dimensional auditing.

```
capture Modify event, after debounce completes:

[Gate 1: inode detection]
   compare current (st_dev, st_ino) against the value recorded in SQLite
       │
       ├── inode changed → judge as new-file stand-in, zero the pointer, slice from scratch
       │                 → the old file goes through the new-file path via Create event
       │
       └── inode unchanged → enter Gate 2
                          │
                          ▼
                [Gate 2: size & CoW detection]
                   compare current_size with last_verified_offset
                          │
                   ├── size >= offset → normal linear append, incremental load flow
                   │
                   ├── size < offset  → rotation truncation, zero pointer and re-slice
                   │
                   └── size == offset but mtime changed
                       → CoW overwrite, zero pointer and re-slice
```

---

## 10. Full Lifecycle Loop

```
┌─────────────────────────────────────────────────────────────────┐
│                     slimSync full lifecycle                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [cold start]                                                    │
│   ├─ multi-threaded VFS metadata scan (or Windows USN)          │
│   ├─ fetch dirty_files table (locate pre-crash debounce assets) │
│   ├─ SQL diff → new/modified/deleted three change lists         │
│   └─ run Checkpoint audit on change list → load into PUB queue  │
│                                                                  │
│  [normal operation]                                             │
│   ├─ notify kernel event listening                              │
│   ├─ persistent dirty marker: immediately write dirty_files      │
│   ├─ 500ms hard delay window → force-trigger slicing             │
│   ├─ sent_hashes dual-track dedup (online Query / offline cache) │
│   ├─ seek(last_offset) → incremental slice → encrypt → PUB       │
│   └─ SQLite transaction advances Checkpoint + clears dirty marker│
│                                                                  │
│  [crash recovery]                                                │
│   ├─ read local SQLite Checkpoint                                │
│   ├─ mtime + inode comparison → branch                          │
│   ├─ seek + tail Hash verification                               │
│   └─ incremental backtrack + PUB refill                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 11. Interface Contract: slim-common Shared Crate

The communication types and topic constants between slimSync and slimHub / slimRagSvr are single-sourced in the independent `slim-common` repository (introduced as a git dependency).

```
slim-common (independent repo)
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── types.rs    ← ChunkMessage, FileMetadata, AuditQuery structs, etc.
    └── topics.rs   ← Zenoh topic constants (compile-time alignment)
```

- Serialization uses `serde + postcard`, zero external toolchain dependencies
- Any field or topic path change is done by editing `src/types.rs` / `topics.rs` in the slim-common repo
- All three modules fail to compile immediately after a change, guaranteeing absolute compile-time field alignment

---

## 12. Resource Constraints & Deployment

| Metric | Target |
|--------|--------|
| Binary size | < 5MB (statically compiled) |
| Resident memory | < 15MB RSS |
| CPU usage | idle < 0.1%, short bursts during slicing/cold start |
| Local storage | SQLite < 50MB (millions of tracked files) |
| Platforms | x86_64 / ARM64 / ARMv7 / Windows / macOS / Linux |
| Self-healing | fully offline autonomy, no remote dependency |

---

## 13. Summary

slimSync achieves the edge-side goals of "ultra-lightweight + high precision + blind security + offline autonomy" through the following design:

1. **Dual-track slicing** — AST structure track + FastCDC compute track, merge ensures semantic continuity
2. **HMAC Blind-ID** — 16-byte blind fingerprint, global dedup credential, cannot reverse to original content
3. **sent_hashes dual-track dedup** — online Zenoh Query + offline local cache, resolves the offline paradox
4. **Local SQLite ledger** — ACID transactions + WAL-level persistent dirty-page markers
5. **On-device autonomous Checkpoint** — no remote dependency, self-healing after power loss
6. **Cold-start change list** — multi-threaded metadata scan + SQL diff (Windows can use USN)
7. **WAL-level persistent debounce** — dirty_files persistence + 500ms hard delay window, zero loss
8. **Log rotation tolerance** — (st_dev, st_ino) two-dimensional audit + size detection + CoW overwrite detection
