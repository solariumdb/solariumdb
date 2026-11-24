# Solarium.Kommander

`Solarium.Kommander` is a [Kommander](https://github.com/kahunakv/kommander) write-ahead log on Solarium. `SolariumWAL` implements Kommander's `IWAL`, and you pass it to `RaftManager` in place of `RocksDbWAL`. It targets Kommander 1.13.2 and .NET 10.

```csharp
using Kommander;
using Kommander.Discovery;
using Kommander.Time;
using Solarium.Kommander;

using SolariumWAL wal = new("/var/lib/node1/raft", "v1", logger, syncWrites: true);

RaftManager raft = new(configuration, new StaticDiscovery(peers), wal, communication, new HybridLogicalClock(), logger);
```

On Linux, you can set the io_uring environment in the options. The host owns the environment and disposes it after the WAL.

```csharp
using IoUringStorageEnvironment environment = new();
using SolariumWAL wal = new(path, "v1", logger, syncWrites: true, new SolariumWalOptions { Environment = environment });
```

With `syncWrites: false`, a write survives a process crash but not a power loss.

## Compatibility with RocksDbWAL

`SolariumWAL` is a port of Kommander's `RocksDbWAL` (release 1.11.3, with the storage-damage contract of 1.13.1). The rows and keys are the same bytes.

| Part | Layout |
| --- | --- |
| Column families | `default`, `metadata`, `shard0` to `shard7`. Partition `p` is in `shard{p % 8}`. |
| Log entries | One row per entry under `{partition:D10}:{id:D20}`, with the `RaftLogMessage` protobuf encoding |
| Metadata keys per partition | The last checkpoint, the commit frontier and the compaction floor; the hard state and the HLC floor through `SetMetaData` |
| Storage-damage keys | `raft_acked_presence_p{partition}`, `raft_lost_synced_writes`, `raft_reconciled_lost_synced_writes_p{partition}`, `raft_engine_identity` |
| Format version | `2.0.0` in the `version` metadata key, checked at open |

A commit marker is a small metadata put and does not rewrite the row. Compaction deletes the dead prefix with a floor, a range delete in the active memtable, and file drops through `DeleteFilesInRange`.

The files are Solarium's. A `RocksDbWAL` directory does not open as a `SolariumWAL` directory, and the package does not include a tool to copy the rows.

## Options

`SolariumWalOptions` sets the shard tuning, as `RocksDbWalTuning` does for RocksDB.

| Option | Default | Meaning |
| --- | --- | --- |
| `ShardWriteBufferSizeBytes` | 128 MiB | The memtable size of each shard, which is the flush unit. RocksDB's tuning merges two 64 MB memtables per flush. |
| `ShardMaxWriteBufferNumber` | 3 | The memtables of a shard before writes wait for a flush. At least 2. |
| `ShardLevel0FileNumCompactionTrigger`, `ShardLevel0SlowdownWritesTrigger`, `ShardLevel0StopWritesTrigger` | 8, 28, 44 | The level 0 triggers. |
| `MaxTotalWalSizeFlushUnits` | 2 | The WAL size cap, in flush units. Past it, the families that need the oldest WAL file are flushed. 0 selects Solarium's default. |
| `MaxBackgroundCompactions` | 6 on 16 CPUs | The compactions that can be active at once: the compaction share of `clamp(CPUs / 2, 2, 8)` background jobs, as in `RocksDbWAL`. |
| `WriteBufferManager` | none | Caps the memtables of the WAL together with those of the host's other Solarium databases. The WAL logs a warning at open if the cap is below two shard memtables. |
| `BlockCache` | none | Caches the index and filter blocks of all families, pinned at level 0. It does not cache data blocks. A `WriteBufferManager` created with the same cache puts the memtables under its capacity too. |
| `Environment` | the default environment | The storage environment of the files. |
| `TickSource` | the system clock | The clock of the failure log cadence and the probe interval. |

## Concurrency

All members are safe to call from any thread. Appends hold a shared lock and overlap, and group commit joins their WAL flushes. Truncations and the snapshot boundary take an exclusive lock, and so do appends that carry a checkpoint.

A write with commit markers also locks each partition that it commits, in partition order. The lock covers the read of the commit frontier and the write of the new one, which means that the frontier cannot move back. A partition delete takes the same lock. Appends without commit markers do not wait for it.

## After a storage failure

After a failed write, a Solarium database fails all writes until it is reopened. `SolariumWAL` then tests the volume with a probe file and the database with a canary put.

- If only the database fails, the WAL closes and reopens the database and retries the operation once.
- If the volume fails too, writes return `Errored`. Kommander retries on its cadence.
- If the reopen fails, the WAL remains closed until the next failed write tries again.

Acknowledged writes are not lost. Synced writes were flushed, and unsynced ones had reached the OS. Damage to the files is handled differently, as the next section describes.

## Storage damage

`SolariumWAL` implements Kommander 1.13's storage-damage contract.

### Fail-stop

A `SolariumException` with `IsCorruption` fail-stops the WAL. It can come from a read, a write, a reopen or the open.

- The WAL logs one Critical line and increments `raft.wal.engine_fail_stops_total`.
- Later writes return `Errored` and do not touch the database. The recovery does not probe or reopen.
- `GetFailStopReason` names the damaged file. Reads continue, and a read of the damage throws again. A scan throws at a damaged block and cannot end short.
- Kommander moves the partitions to `EngineFailed`, and a leader steps down. A restore that reads the damage ends in `RestoreFailed`.
- If the damage is found at open, the open throws and the node does not start.

### Term and vote

`SetMetaData` and `PersistHardState` use the WAL's write options, and are synced under `syncWrites`. This prevents a node from granting two candidates the same term across a power loss. `PersistHardState` also appends the term and vote to the hard-state ledger. They count as durable after both writes.

### Acknowledged-presence watermark

`Write(logs, sync, acknowledgedPresence)` stores, in the batch of the rows, the highest id that the node will have reported as contiguously held for each partition. Truncations, the sweep of `Proposed` rows and a truncating snapshot boundary clamp it. A snapshot boundary above the log raises it, and a partition delete removes it. Kommander quarantines a partition whose restored log is shorter than its watermark.

### Ledgers

Both ledgers are files in the database directory.

`.kommander-sync-ledger` detects a WAL file that was cut after its fsync. After a synced write and before it returns, it attests the last sequence number of the write group (`WriteBatch.GroupLastSequence`). A group costs at most one ledger flush. The file has two 512-byte slots written in turn.

- If the ledger is ahead of the replayed database at open, the WAL persists a lost-synced-writes verdict. All partitions carry it (`GetLostSyncedWritesReason`) until Kommander reconciles it from a leader (`MarkLostSyncedWritesReconciled`). Writes continue, since replication repairs a lost tail.
- A missing, torn or unreadable ledger, or one of another database, is not a loss. The WAL uses the other slot or starts a new ledger.
- With `syncWrites` off, the ledger is written at open only.

`.kommander-hard-state` has each partition's last term and vote. A term or vote that is ahead of the database is written back at open, before Kommander reads the hard state. A torn last record is cut. Past 256 KiB, the file is rewritten with one record per partition.

If the database files are erased and the ledgers are left, the directory opens as a new database with a verdict. If you repair damage by moving the directory aside, the ledgers move too. The node then does not remember its term or its vote, as with `RocksDbWAL`.

## Metrics

Kommander publishes most `raft.wal.*` instruments on its meter, `"Kommander"`. The ones that read the storage engine are on `SolariumWalMetrics.MeterName` (`"Solarium.Kommander"`), with the same names, units and meanings as for `RocksDbWAL`. Add both meters.

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
| `raft.wal.shard_l0_files`, `raft.wal.shard_l6_files` | gauge | The SSTables of the shard families in level 0 and in level 6. |
| `raft.wal.table_readers_memory` | gauge, By | The memory of the open SSTables: range tombstones, and index and filter blocks that are not in the block cache. |
| `raft.wal.block_cache_usage` | gauge, By | The bytes in `SolariumWalOptions.BlockCache`, including other users of the cache. |
| `raft.wal.block_cache_pinned_usage` | gauge, By | The bytes of that cache that cannot be evicted. |
| `raft.wal.memtable_memory` | gauge, By | The memtables of all families, including those being flushed. |
| `raft.wal.alive_log_bytes` | gauge, By | The WAL files in the database directory, which a restart replays. |
| `raft.wal.engine_reopens_total` | counter | Reopens after a storage failure, tagged `outcome`: `success` or `failed`. |
| `raft.wal.engine_fail_stops_total` | counter | WALs that fail-stopped on damage, or failed to open on it. |
| `raft.wal.sync_ledger_flushes_total` | counter | Flushes of the sync ledger. |
| `raft.wal.lost_synced_writes_total` | counter | Opens that found lost synced writes through the sync ledger. |

Each open `SolariumWAL` publishes one untagged measurement per gauge until `Dispose`. The block cache gauges need a `BlockCache`.

## Differences from RocksDbWAL

- **Write after `Dispose`.** It throws `ObjectDisposedException`. In `RocksDbWAL`, this is a storage failure first, and the volume is probed.
- **New WAL.** A WAL is new when its database is empty. `RocksDbWAL` checks whether the directory exists and fails on an existing one that lacks a `version` key. `SolariumWAL` writes the version synced, before any row. It can open a directory that a host created first.
- **Deleted WAL file.** The open throws a corruption exception, since Solarium's manifest lists all live WAL files. `RocksDbWAL` opens on its tables alone and finds the loss through its sync ledger.
- **Ledger binding at open.** `SolariumWAL` binds its ledger at the sequence of a synced canary write. `RocksDbWAL` uses the latest sequence. With `syncWrites` off, a power loss right after its open reads as lost synced writes.
- **Fail-stop reason.** After a compaction met damage, `GetFailStopReason` includes the wrapped failures, which name the table and the offset. `RocksDbWAL` gives the status text.
- **WAL file after a flush.** A new file starts at the first write after the flush.

## Tests

`tests/Solarium.Kommander.Tests` has an unchanged copy of Kommander's conformance suite for `IWAL` backends. It also has the `RocksDbWAL` behavior tests. They execute on the default and the io_uring environment.

| Tests | What they check |
| --- | --- |
| `WalCrashTests` | Crashes at random storage operations of a random Kommander workload. A second WAL that does not crash is the oracle. The reopened WAL must match it after one operation, at or past the last one that had to survive. |
| `LostSyncedWriteTests` | Cut, deleted and erased files give a verdict, and it is persisted and reconciled. Torn unsynced records and other benign states do not. |
| `SyncLedgerAttestationTests` | A group of sixteen writers costs at most one ledger flush. |
| `StorageDamageTests` | The flush of a vote, the watermark on all its paths, and the fail-stop. |
| `StorageVerdictTests` | The verdicts in a `RaftManager`: damaged nodes do not lead, followers are quarantined and repaired, and a damaged leader steps down. |
| `MetricsTests` | The instruments, read through a `MeterListener`. |
| `ClusterTests` | Three-node clusters in one process, with replication and restarts. A stopped follower catches up. |

`ClusterTests` are in `SerialCollection` and do not overlap with other tests, because their elections are timed in milliseconds. The follower case of `StorageVerdictTests` turns Kommander's `WalSingleFsyncCommit` off. With Kommander's default, a follower can acknowledge a row in an unsynced batch of commit markers, and a ledger cannot tell that loss from a torn tail.

For crash test settings, see [testing.md](testing.md). For a comparison with `RocksDbWAL`, see [benchmarks.md](benchmarks.md).
