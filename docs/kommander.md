# Solarium.Kommander

`Solarium.Kommander` is a [Kommander](https://github.com/kahunakv/kommander) write-ahead log on Solarium: `SolariumWAL` implements Kommander's `IWAL`, so a host passes it to `RaftManager` where it passes `RocksDbWAL` today, with no change to Kommander. It targets Kommander 1.12.0 and .NET 10.

```csharp
using Kommander;
using Kommander.Discovery;
using Kommander.Time;
using Solarium.Kommander;

using SolariumWAL wal = new("/var/lib/node1/raft", "v1", logger, syncWrites: true);

RaftManager raft = new(configuration, new StaticDiscovery(peers), wal, communication, new HybridLogicalClock(), logger);
```

On Linux, the io_uring environment goes through the options; the host owns it and disposes it after the WAL:

```csharp
using IoUringStorageEnvironment environment = new();
using SolariumWAL wal = new(path, "v1", logger, syncWrites: true, new SolariumWalOptions { Environment = environment });
```

## What it is

`SolariumWAL` is a port of Kommander's `RocksDbWAL` (commit `1bee500`, the release 1.11.3), not a new design. It keeps:

- the column families `default`, `metadata` and `shard0` to `shard7`, with partition `p` in `shard{p % 8}`;
- one row per log entry under the key `{partition:D10}:{id:D20}`, holding the `RaftLogMessage` protobuf encoding, written and read by hand as `RocksDbWAL` does;
- the per-partition metadata keys: the last checkpoint, the commit frontier and the compaction floor, and the hard state and HLC floor that Kommander writes through `SetMetaData`;
- the format version `2.0.0` in the `version` metadata key, checked at open;
- the commit frontier (a commit marker is a small metadata put, not a rewrite of the row), the compaction floor (the dead prefix is deleted by a floor, a range delete confined to the active memtable, and whole-file drops with `DeleteFilesInRange`), and the snapshot boundary.

It also fixes two bugs of `RocksDbWAL` at that commit. Kommander 1.12.0 made the same two fixes to `RocksDbWAL` (`d72da2d` and `be2a7ff`), its only changes to it since, so neither is a difference from the release it targets:

- **The header reader skips length-delimited fields correctly.** In 1.11.3, `RocksDbWAL`'s `ReadHeaderFromWire` and `ReadTermFromWire` skip a length-delimited field with `pos += (int)ReadVarint(data, ref pos)`, which C# evaluates with `pos` read before the call advances it, so the skip lands one byte short and the payload parses as fields. A `Proposed` row (no type field) with the log type `op` and a 32-byte payload then reads with the payload's first byte as its type, so the proposed-tail sweep can keep an uncommitted entry. `SolariumWAL` reads the length first, and refuses a length past the end of the row.
- **The commit frontier never moves back under concurrent commits.** In 1.11.3, `RocksDbWAL` reads a partition's frontier, probes the rows above it and puts the advance under the shared side of its write guard alone, so two commits of one partition that overlap can land in the opposite order: the lower advance lands last, both report success with sync on, and after a reopen the commits that the higher one absorbed read `Proposed`. `SolariumWAL` serializes the advances of each partition (see Concurrency).

The rows and keys are the same bytes, but the files are Solarium's: a `RocksDbWAL` directory does not open as a `SolariumWAL` one, or the other way. Moving a node between them is a copy of the rows, which the package does not provide.

## Options

`SolariumWalOptions` sets the shard tuning, as `RocksDbWalTuning` does for RocksDB:

| Option | Default | Meaning |
| --- | --- | --- |
| `ShardWriteBufferSizeBytes` | 128 MiB | The memtable size of each shard: the flush unit. RocksDB's tuning merges two 64 MB memtables per flush; Solarium does not merge memtables, so one 128 MiB memtable is the same unit. |
| `ShardMaxWriteBufferNumber` | 3 | Memtables a shard holds before writes wait for a flush; at least 2. |
| `ShardLevel0FileNumCompactionTrigger`, `ShardLevel0SlowdownWritesTrigger`, `ShardLevel0StopWritesTrigger` | 8, 28, 44 | The level 0 triggers. A partition that owns its shard writes files that do not overlap, so they move down the levels without a rewrite. |
| `MaxTotalWalSizeFlushUnits` | 2 | The WAL size cap, in flush units: past it, the families that keep the oldest WAL file are flushed, so a shard with a trickle of writes does not keep every WAL file. 0 leaves Solarium's default. |
| `MaxBackgroundCompactions` | the compactions of `clamp(CPUs / 2, 2, 8)` jobs: 6 on 16 CPUs | The compactions that run at once, as `RocksDbWAL`'s `max_background_jobs`, which RocksDB splits into a quarter for flushes and the rest for compactions. |
| `Environment` | the default environment | Where the files go. |
| `TickSource` | the system clock | The clock of the failure log cadence and the probe interval. |

`syncWrites: false` makes every write unsynced, as in `RocksDbWAL`; then a write survives a process crash but not a power loss.

## Concurrency

Every member is safe from any thread. Appends hold the shared side of a write guard, so they overlap and Solarium's group commit joins their WAL flushes. The operations that scan and then delete (the truncations and the snapshot boundary), and appends that carry a checkpoint, hold its exclusive side, so an append cannot land between their scan and their batch. A write with commit markers also holds a lock for each partition it commits, taken in partition order, from its read of the commit frontier to the write of the new one, and a partition delete holds it for its write: an advance computed from an old frontier cannot land after a newer one. Appends to other partitions, and appends without commit markers, do not wait for it.

## After a storage failure

A failed write stops every later write of a Solarium database until it is opened again. After a failure, `SolariumWAL` asks two questions: does the volume take a small durable write (a probe file, written through the storage environment), and does the database (a canary put)? Only when the volume does and the database does not does it close the database and open it again; then it retries the operation once. While the volume still refuses writes, every write reports `Errored` and nothing reopens; Kommander retries on its own cadence. A reopen that fails leaves the WAL closed, and the next failed write tries again. Nothing acknowledged is lost: synced writes were flushed, and unsynced ones reached the OS.

## Metrics

Kommander publishes its `raft.wal.*` instruments on its meter, `"Kommander"`. Most come from Kommander itself whatever the WAL: batches, operations, batch size, queue depth, the oldest pending write, write duration, and compaction passes. The ones that read the storage engine Kommander wires to `RocksDbWAL` alone, so `SolariumWAL` publishes them on its own meter, `SolariumWalMetrics.MeterName` (`"Solarium.Kommander"`), with the same names, units and meanings. A host listens to both:

```csharp
builder.Services.AddOpenTelemetry().WithMetrics(metrics => metrics
    .AddMeter("Kommander")
    .AddMeter(SolariumWalMetrics.MeterName));
```

| Instrument | Kind | Value |
| --- | --- | --- |
| `raft.wal.delayed_write_rate` | gauge, By/s | The rate writes are slowed to, or 0 (`rocksdb.actual-delayed-write-rate`). |
| `raft.wal.write_stalled` | gauge | 1 while writes are stopped, else 0 (`rocksdb.is-write-stopped`). |
| `raft.wal.shard_live_sst_bytes` | gauge, By | The live SSTable bytes of the shard families. |
| `raft.wal.shard_l0_files`, `raft.wal.shard_l6_files` | gauge | The SSTables of the shard families in level 0 and in level 6, the bottom level. |
| `raft.wal.table_readers_memory` | gauge, By | The bytes the open SSTables keep in memory: range tombstones, and index and filter blocks unless the block cache holds them. |
| `raft.wal.block_cache_usage` | gauge, By | The bytes in the WAL's block cache, the whole cache when it is shared; only with `SolariumWalOptions.BlockCache`. |
| `raft.wal.block_cache_pinned_usage` | gauge, By | The bytes of that cache that cannot be evicted: level 0 index and filter blocks, and memtable charges. |
| `raft.wal.memtable_memory` | gauge, By | The bytes of the memtables of every family, flushing ones included. |
| `raft.wal.alive_log_bytes` | gauge, By | The bytes of the WAL files in the database directory, which a restart replays. |
| `raft.wal.engine_reopens_total` | counter | Reopens of the database to clear a storage failure, tagged `outcome`: `success` or `failed`. |

Each open `SolariumWAL` reports one measurement per gauge, without tags, as each `RocksDbWAL` does, and stops at `Dispose`. `raft.wal.block_cache_usage` and `raft.wal.block_cache_pinned_usage` come only from a WAL with a `SolariumWalOptions.BlockCache`, and report the whole cache, as `RocksDbWAL`'s report its shared cache. The instruments cost nothing while no listener is attached.

## Differences from RocksDbWAL

- **The shared resources, as two options.** `SolariumWalOptions.WriteBufferManager` caps the memtables of the WAL and of the host's other Solarium databases together, as the write-buffer manager of `RocksDbSharedResources` does, and the WAL warns at open, as `RocksDbWAL` does, when the cap is below two shard memtables. `SolariumWalOptions.BlockCache` holds the index and filter blocks of every family, pinned at level 0, as `RocksDbWAL` sets them on its shared path; a `WriteBufferManager` created with the same cache puts the memtables under its capacity too, as `CreateWithUnifiedBudget`. The cache holds no data blocks. The scans of dead rows (`CountPersistedLogs`, a compaction pass's count) read with `FillCache` off, as `RocksDbWAL`'s.
- **The engine gauges are on a second meter.** Kommander publishes the engine `raft.wal.*` gauges of `RocksDbWAL` on its own meter; `SolariumWAL`'s are on `"Solarium.Kommander"`, so a host adds that meter (see Metrics). A WAL without a block cache reports no block cache gauges.
- **A write after `Dispose` throws** `ObjectDisposedException`, as a read does. `RocksDbWAL` reports it as a storage failure first and probes the closed WAL's volume.
- **A new WAL is one whose database holds nothing.** `RocksDbWAL` takes a WAL as new when its directory does not exist, and refuses an existing directory without a `version` key. So a directory that a host created first, or one that a crash during the first open left before the unsynced version reached the disk, never opens. `SolariumWAL` writes the version synced, before any row, and refuses only a database that holds data without one.
- **The WAL file after a flush.** Solarium starts a new WAL file at the first write after a flush, not at the flush, so a closed WAL keeps its last file until then; see [rocksdb-differences.md](rocksdb-differences.md).

## Tests

`tests/Solarium.Kommander.Tests` holds Kommander's own conformance suite for `IWAL` backends, copied unchanged from the same commit, and the behavior tests of Kommander's `RocksDbWAL` suites ported to `SolariumWAL`: reads and writes, checkpoints, the commit frontier, the compaction floor and its file layout, truncation and the snapshot boundary, the torn WAL tail, and the recovery after a write failure. Each runs on the default and the io_uring environment.

`WalCrashTests` crashes a `SolariumWAL` at random storage operations of a random Kommander workload (appends, commit markers, backfills, checkpoints, compaction passes, truncations, snapshot boundaries, partition deletes and hard state, synced and not, on small memtables so flushes, WAL rotation, compactions and file drops run too), through `PowerLossEnvironment`: power losses that keep only flushed bytes or part of the rest, in order or not, and process crashes that keep everything written. A second `SolariumWAL` that never crashes runs the same operations as the oracle. Each operation is one atomic write, so the reopened WAL must read as the reference did after one operation: at or past the last synced one that changed what a read sees after a power loss, and at or past the last one that returned after a process crash. Each run crashes and reopens three times. It runs 24 runs on each environment; `SOLARIUM_WAL_CRASH_ITERATIONS` and `SOLARIUM_WAL_CRASH_SEED` change that.

`MetricsTests` reads the instruments through a `MeterListener`: an open WAL's gauges equal its engine's readers and stop at `Dispose`, and reopens count by outcome.

`ClusterTests` runs Kommander clusters of three nodes in one process, each node on its own `SolariumWAL` with small memtables, over Kommander's in-memory transport, on each storage environment. Every node holds the same committed log after replication with checkpoints across three partitions, read back in part from SSTables. A cluster stopped and started again on its directories reads the same log, hands every committed entry to the application on restore, and numbers new entries after the restored ones. A follower stopped without leaving the cluster and started again on its directory restores what it had and catches up on what it missed. The tests run alone (`SerialCollection`), because their elections are timed in milliseconds.

[benchmarks.md](benchmarks.md) compares the write path with `RocksDbWAL`.
