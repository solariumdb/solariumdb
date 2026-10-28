# SolariumDb.Kommander

A write-ahead log for [Kommander](https://github.com/kahunakv/kommander), the Raft library, on Solarium: an embedded key-value store for .NET, written in C#, whose API and semantics follow RocksDB's. `SolariumWAL` implements Kommander's `IWAL`, so a node passes it to `RaftManager` where it passes `RocksDbWAL`, with no change to Kommander and no native library to ship.

It targets Kommander 1.12.0 and .NET 10. The on-disk format may still change before version 1.0.

## Use

```sh
dotnet add package SolariumDb.Kommander
```

```csharp
using Kommander;
using Kommander.Discovery;
using Kommander.Time;
using Solarium.Kommander;

SolariumWAL wal = new("/var/lib/node1/raft", "v1", logger, syncWrites: true);

RaftManager raft = new(configuration, new StaticDiscovery(peers), wal, communication, new HybridLogicalClock(), logger);
```

The WAL lives in the directory `{path}/{revision}`, created when missing, and one process opens it at a time. `RaftManager` disposes its WAL when it is disposed.

On Linux, the `SolariumDb.IoUring` package writes the files through io_uring. The host owns the environment and disposes it after the WAL:

```csharp
using Solarium.IoUring;

using IoUringStorageEnvironment environment = new();
SolariumWAL wal = new(path, "v1", logger, syncWrites: true, new SolariumWalOptions { Environment = environment });
```

io_uring matches the default environment on synced writes, whose fsync does not go through the ring, and is slower on unsynced ones, since each write is a hand-off to the reactor thread.

## Durability

- `syncWrites: true` (the default): a write is flushed to the disk before `Write` returns, so it survives a power loss.
- `syncWrites: false`: a write reaches the operating system before `Write` returns, so it survives a process crash but not a power loss.
- Hard state (the term and the vote) goes through `SetMetaData`, which is not synced, as in `RocksDbWAL`; Kommander's `IWAL` documents that choice.

Crash tests cut a WAL at random storage operations of a random Kommander workload, with power losses that keep only flushed bytes and process crashes that keep everything. After every crash, the reopened WAL reads as it did after some operation at or past the last durable one.

## Format

`SolariumWAL` is a port of Kommander's `RocksDbWAL`, not a new design. It has the same column families (`default`, `metadata`, `shard0` to `shard7`), the same keys, rows encoded as `RaftLogMessage` protobufs, the same metadata, and the format version `2.0.0`. The rows are the same bytes, but the files are Solarium's: a `RocksDbWAL` directory does not open as a `SolariumWAL` one, or the other way. Moving a node from one to the other means copying the rows, which this package does not do.

## Options

`SolariumWalOptions` sets the shard tuning, as `RocksDbWalTuning` does for RocksDB.

| Option | Default | Meaning |
| --- | --- | --- |
| `ShardWriteBufferSizeBytes` | 128 MiB | The memtable size of each shard: the flush unit. |
| `ShardMaxWriteBufferNumber` | 3 | Memtables a shard holds before writes wait for a flush; at least 2. |
| `ShardLevel0FileNumCompactionTrigger`, `ShardLevel0SlowdownWritesTrigger`, `ShardLevel0StopWritesTrigger` | 8, 28, 44 | The level 0 triggers. A partition that owns its shard writes files that do not overlap, so they move to the sorted level without a rewrite. |
| `MaxTotalWalSizeFlushUnits` | 2 | The cap on the WAL files, in flush units: past it, the shards that keep the oldest WAL file are flushed. 0 leaves Solarium's default. |
| `Environment` | the default environment | Where the files go. |
| `TickSource` | the system clock | The clock of the failure log cadence and the probe interval. |

## After a storage failure

A failed write stops every later write of a Solarium database until it is opened again. After a failure, `SolariumWAL` checks whether the volume takes a small durable write and whether the database does. Only when the volume does and the database does not does it reopen the database and retry the operation once. While the volume still refuses writes (a full disk, say), every write reports `Errored` and Kommander retries on its own cadence. Nothing acknowledged is lost.

## Metrics

Kommander publishes most of its `raft.wal.*` instruments itself, whatever the WAL. The ones that read the storage engine it publishes for `RocksDbWAL` only, so `SolariumWAL` publishes them on its own meter, `SolariumWalMetrics.MeterName` (`"Solarium.Kommander"`), with the same names, units and meanings. Listen to both meters:

```csharp
builder.Services.AddOpenTelemetry().WithMetrics(metrics => metrics
    .AddMeter("Kommander")
    .AddMeter(SolariumWalMetrics.MeterName));
```

The gauges are `raft.wal.delayed_write_rate`, `raft.wal.write_stalled`, `raft.wal.shard_live_sst_bytes`, `raft.wal.shard_l0_files`, `raft.wal.shard_l6_files`, `raft.wal.table_readers_memory`, `raft.wal.memtable_memory` and `raft.wal.alive_log_bytes`, plus the counter `raft.wal.engine_reopens_total`, tagged `outcome`. A WAL with a `BlockCache` also reports `raft.wal.block_cache_usage` and `raft.wal.block_cache_pinned_usage`.

## Differences from RocksDbWAL

- **The shared resources, as two options.** `SolariumWalOptions.WriteBufferManager` caps the memtables of the WAL and the host's other Solarium databases together, the write-buffer manager of `RocksDbSharedResources`, and `SolariumWalOptions.BlockCache` holds the index and filter blocks of the WAL's tables, its block cache, without data blocks. A manager created with the cache puts both under one capacity.
- **A write after `Dispose` throws** `ObjectDisposedException`, as a read does.
- **A new WAL is one whose database holds nothing.** `RocksDbWAL` refuses a directory that exists before its first open, such as one a host created; `SolariumWAL` opens it.

## Performance

On one machine (Intel i7-13620H, NVMe drive with a 4 to 8 ms fsync, Linux, the server GC), through Kommander's `IWAL` with default tuning:

- **Synced writes** ran at 0.85 to 1.18 times `RocksDbWAL`'s rate, within the spread of the drive's fsync times. Both engines group concurrent writers into one fsync.
- **Unsynced writes** ran at 1.0 to 1.6 times `RocksDbWAL`'s rate on the default environment, with a lower median latency.
- **Tail latency follows the ingest rate**, because Solarium's memtables are on the managed heap. At 20 MB/s per node the p99.9 and the longest write matched RocksDB's (about 80 µs and 15 ms). At 100 MB/s, gen 2 collections stalled writes for up to about 90 ms, where RocksDB's longest write stayed near 15 ms. As fast as the CPU allows, the p99.9 rose to about 2 ms against RocksDB's 18 µs.

## Tests

The package passes Kommander's conformance suite for WAL backends and the behavior tests of `RocksDbWAL`, ported, on the default and the io_uring environments. It also passes the crash tests above, and runs three-node Kommander clusters through replication, a restart of the whole cluster, and a follower that restarts and catches up.

## License

MIT.
