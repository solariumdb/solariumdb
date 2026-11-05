# Solarium.Kahuna

`Solarium.Kahuna` is a [Kahuna](https://github.com/kahunakv/kahuna) storage engine on Solarium. It holds both
things a Kahuna node stores, so a node runs with no native RocksDB library:

| Kahuna option | Provider | Stores |
| --- | --- | --- |
| `--storage solarium` | `SolariumPersistenceBackendProvider` | the key-value and lock data, through `SolariumPersistenceBackend` |
| `--wal-storage solarium` | `SolariumWalProvider` | the Raft log, through `SolariumWAL` (`Solarium.Kommander`) |

It needs Kahuna 1.15.4 or later. 1.13.4 brought the public storage contract: `IPersistenceBackend` with
immutable rows, and the `StorageProviders` registry of `IPersistenceBackendProvider` and `IWalProvider`.
1.14.1 brought the storage settings that pace the compactions, 1.14.2 the memory that the WAL and the store
share (below), 1.14.4 the `--extension-option` settings and the System.IO.Hashing version this package
uses, 1.15.3, with Kommander 1.13.1, the storage-damage contract of the store and the WAL (below), and 1.15.4
Kommander 1.13.2, whose snapshot installs no longer flood a follower with appends it cannot apply.

## Use it

The Kahuna server loads it as an extension assembly. Publish the package, then point the server at the
published assembly:

```sh
dotnet publish src/Solarium.Kahuna -c Release -o /opt/kahuna/solarium

kahuna-server --extension-assembly /opt/kahuna/solarium/Solarium.Kahuna.dll \
              --storage solarium --storage-path /var/lib/kahuna/data \
              --wal-storage solarium --wal-path /var/lib/kahuna/wal
```

The published folder holds only what the server lacks: the Solarium assemblies, ioxide, the io_uring
runtime, and K4os.Compression.LZ4, the tables' LZ4, about 4 MB. Kahuna.Core, Kommander and their
dependencies are left out, because the server's loader always takes the server's own copy of an assembly the
server has. Every node of a cluster loads the same artifact; the server logs each file's SHA-256 and the
engines it registered.

Two of Kahuna's storage settings pace the store's compactions. `--storage-compaction-rate-mbps` gives the
store a `RateLimiter` of that rate for its flush and compaction writes, and runs `max(2, CPUs / 2)`
compactions at once, as the RocksDB backend does; `--storage-max-background-compactions` sets that count
instead. With neither, the store runs one compaction at a time and paces nothing. The limiter paces flushes
too, so its rate must stay above what the store ingests, and below what leaves the disk to the WAL's syncs: on
a disk whose synced writes took 4 to 8 ms, 50 made the store's synced batches of 64 rows 2.5 times as fast as
one unpaced compaction, but on the same disk at about 1 ms, the synced batches outran 25 and 50, and flushes
held them to 0.2 to 0.4 times their unpaced rate (see "Paced background writes" in
[benchmarks.md](benchmarks.md)). Too low a rate lets level 0 grow until writes slow down.

Two settings give the WAL and the store of a node memory to share; both providers take the node's
`SharedStorageMemory` and use the same Solarium objects for it:

- `--storage-shared-cache-mb`: one `Cache` for the index and filter blocks of both, pinned at level 0, in place
  of the store's process-wide cache (below); the WAL, which otherwise keeps them resident, joins it. Size it to
  hold every index and filter of both, about 2% of their table bytes, 4% with LZ4, plus the memtable budget.
- `--storage-shared-memtable-budget-mb`: one `WriteBufferManager` that keeps their memtables together under
  it, as the RocksDB pair's shared write-buffer manager does. When they pass it, the database that writes next
  flushes its own oldest memtable, never the other's; nothing stalls. With a shared cache, it is charged into
  the cache, so one size bounds both, as the RocksDB pair's unified budget. Size it above two of the WAL's
  128 MiB shard memtables, or the WAL warns at open: a smaller budget flushes the WAL's memtables before they
  fill.

Both are 0 by default. A solarium WAL beside another store, or a solarium store beside another WAL, uses them
alone.

Neither budget is on the managed heap, as with RocksDB: the memtables' entries are in native arenas, and the
cache's data blocks (`CacheDataBlocks`) in native memory, so a node sized for RocksDB needs no larger GC heap limit
(`DOTNET_GCHeapHardLimit*`) for Solarium, and the GC does not copy or mark them. What stays on the heap is the index
and filter partitions in the cache, about 2% of the table bytes (4% with LZ4), and their managed handles. The arenas'
chunks are reused from one memtable to the next while they stay within the memtable budget, so the process holds at
most the budget for memtables, plus up to one partly used 64 KiB chunk per processor and memtable.

An embedded Kahuna node registers the providers in code instead:

```csharp
using Solarium.Kahuna;

EmbeddedKahunaOptions options = new() { Storage = "solarium", WalStorage = "solarium", /* paths... */ };
options.StorageProviders.Register(new SolariumPersistenceBackendProvider());
options.StorageProviders.Register(new SolariumWalProvider());
```

### The storage environment

The parameterless constructors, which the extension loader calls, open the WAL and the store on the
environment the node chooses with `--extension-option solarium.environment=default|io_uring`, or
`EmbeddedKahunaOptions.ExtensionOptions` in code. Without it, they use the default environment. `io_uring` needs Linux with io_uring. It is one `IoUringStorageEnvironment` for every Solarium
database of the process, and it lives as long as the process.

```sh
kahuna-server --extension-assembly /opt/kahuna/solarium/Solarium.Kahuna.dll \
              --storage solarium --wal-storage solarium \
              --extension-option solarium.environment=io_uring
```

Choose io_uring when lookups often miss the page cache. Its batched reads (`MultiGet`, which
`GetKeyValues` uses) keep the misses in flight together, 1.6 to 2.4 times as fast from the drive (see
[storage-environments.md](storage-environments.md)). It costs 3% to 5% on batched reads that hit the page
cache. Its writes hand off to one reactor thread, so they slow down when readers keep every CPU busy.

A `solarium.` key other than `solarium.environment`, or an environment other than `default` and `io_uring`,
fails the open with a message that names it. A provider given an `IStorageEnvironment` in its constructor
uses that one whatever the node's settings. `OpenAt` and `MergeCheckpointInto`, which Kahuna calls for
backups and restores without a context, use the default environment unless the provider was given one. The
environment changes how the files are read and written, not what they hold.

## What it keeps from the RocksDB backend

`SolariumPersistenceBackend` is a port of Kahuna's `RocksDbPersistenceBackend`, not a new design:

- the database at `{storage path}/{revision}`, with the column families `kv` and `locks`;
- the rows: Kahuna's `RocksDbKeyValueMessage` and `RocksDbLockMessage` protobufs, from `Kahuna.Core`;
- the keys: `{key}~CURRENT` for the current row, `{key}~{revision}` for each retained revision, and
  `{key}~NOREV` for the no-revision provenance;
- the tilde-key registry, the durability floors, and the pruned-history floor with its legacy key;
- revision pruning, with its memo, its sweep and its quiescence proof;
- the scans, including the snapshot prefix scan and the seek over a deep revision run;
- the storage-failure recovery: close and reopen in place, behind a swap fence;
- checkpoints: plain, exact as-of, key-filtered, and the merge of one node's share into another;
- the storage-damage contract: the fail-stop on damage, the declared durability floors and the store identity.

## Storage damage

Kahuna 1.15 added a storage-damage contract to `IPersistenceBackend`, with defaults that do nothing;
`SolariumPersistenceBackend` implements it, as the RocksDB backend does.

- **Fail-stop on damage.** The first `SolariumException` with `IsCorruption` from any read, batched read, scan,
  write or open (a block that fails its checksum, a table that is not the one the manifest records, a write
  refused after a compaction met damage) fail-stops the store, with one Critical line. From then on every
  operation throws `StorageCorruptionException`, which Kahuna answers with the terminal `StorageCorrupt`, not
  `MustRetry`; the store methods return false; `GetFailStopReason` names the damage, with the failures a refused
  write wraps; and `TryRecoverFromStorageFailure` declines without closing or reopening the store, so its files
  stay as they are. A Solarium iterator throws at a damaged block, so no scan ends short. Kahuna then withholds
  candidacy and steps down every partition the node hosts, and reports the fault in its storage health. A
  damaged open throws `StorageCorruptionException`, and the node does not start.
- **Durability floors.** `KeepsDurabilityFloors` is true: the store keeps every floor it is given, so a data
  partition with none was never flushed here, and Kahuna answers 0 for it, which lets Kommander refuse to replay
  a compacted suffix onto a store that lost its prefix. A floor row that is not 8 bytes is damage: it fails the
  open, and a read of it fail-stops the store.
- **Store identity.** `SupportsStoreIdentity` is true. The identity, the node's Raft endpoint, a creation id and
  a time, is a synced row in the reserved `\0` namespace, `\0store_identity`, which no scan sees and which is
  not durable state (`HoldsDurableState`). Kahuna stamps a fresh store at its first open, refuses a store
  stamped by another node, stamps a store written before the identity existed, and reports a store found empty
  under a log with history as lost or replaced. An as-of image and a merge carry no identity, so a restore
  or a seed is stamped by the node that opens it, not refused as foreign; a plain checkpoint, a copy of the
  node's own store, keeps it, as the RocksDB backend's does. A row that does not decode is damage.

## Differences from the RocksDB backend

- **An index and filter cache, not a block cache.** Without `--storage-shared-cache-mb`, the store's tables keep
  their index and filter blocks in a 256 MiB cache that every Solarium store of the process shares, pinned at
  level 0, as the RocksDB backend keeps them in its process-wide 256 MiB block cache. The cache holds no data
  blocks: the OS page cache serves them. 256 MiB holds the indexes and filters of about 12 GB of tables, 6 GB with LZ4 (2% and 4% of the
  table bytes). Past that, a lookup that misses reads one partition of its table's index and filter again,
  a few kilobytes: with a cache of a quarter of them, lookups from one thread ran about 2.5 times slower in
  [benchmarks.md](benchmarks.md). Tables written before partitions keep one whole index and filter until a
  compaction rewrites them, and a miss on one of them reads it all.
- **No RocksDB tuning.** Direct reads, Bloom filter settings, statistics and `--rocksdb-shared-memory` do not
  apply; the shared cache and memtable budget do. Solarium's tables have a fixed 10-bit Bloom filter. The kv
  family keeps a 64 MiB memtable with up to three, and the locks family 8 MiB with up to two, as the RocksDB
  backend sizes them.
- **Checkpoints hold no WAL.** A checkpoint is `SolariumDb.CreateCheckpoint`: its tables are hard links to
  the store's, as RocksDB's are, or copies when the checkpoint is on another file system, and what the
  memtables hold goes to tables of the copy, so the live store is not flushed. A live checkpoint is staged
  on the store's file system, as the RocksDB backend's is (`StagesLiveCheckpointsInStore` is true). The
  checkpoint directory is a database directory, opened at `{root}/{revision}`, as RocksDB's.
- **LZ4, and RocksDB's levels.** Both families compress their tables with LZ4, where the RocksDB backend
  uses RocksDB's default, Snappy. Solarium compacts into RocksDB's levels, sized as RocksDB sizes them.
  Without `--storage-compaction-rate-mbps`, it runs one compaction at a time, where the RocksDB backend runs
  `max(2, CPUs / 2)`: unpaced, several compactions take the disk from the synced writes and slow them.
  [benchmarks.md](benchmarks.md) measures both backends.
- **The trim is not rate limited.** Solarium's `RateLimiter` paces flush and compaction writes only, where
  `BackupCaptureThrottleBytesPerSec` charges the trim's reads too (RocksDB's `kAllIo`), so the backend does
  not give it to the as-of trim of a checkpoint copy.
- **The purge flushes first.** Solarium's `CompactRange` does not flush a memtable first, so the trim and
  the merge flush each family before they compact it, which a full purge of the copy needs.
- **A key's rows go with one range tombstone.** `DeleteKeyValues` and `DeleteLocks` delete each key's rows,
  `key~` up to `key\x7F`, with one `DeleteRange`, where the RocksDB backend walks the rows and deletes each
  one. The rows are the same, but the batch has one operation per key, however deep its history. A
  whole-partition purge before a snapshot install deletes 256 keys a call; with a delete per row, a call on a
  store with millions of history rows built them all on the managed heap at once, where the RocksDB backend's
  batch is native memory.
- **Batched reads go one family per call.** Solarium's `MultiGet` reads one column family per call; every
  batched read here reads the kv family only.

## Building against Kahuna

`Solarium.Kahuna` references the `Kahuna.Core` package from nuget.org. The server's loader hands an extension
the server's own copy of every assembly the server has, so the server's `Kahuna.Core` must have at least the
assembly version this package was built against: a server older than the referenced `Kahuna.Core` fails to load
the extension. Raise the reference only to a Kahuna release the servers that load it already run.

## Tests

`tests/Solarium.Kahuna.Tests` holds the behavior tests of Kahuna's RocksDB backend, ported to
`SolariumPersistenceBackend`: the backend tests of `TestPersistenceBackends`, revision pruning (memo, sweep
quiescence, time budget, snapshot floor), as-of reads (revision at or before, probes, run seeks, prefix
scans, batched gets, the monotonic current head, no-revision writes), checkpoints (as-of, the pruned-history
floor, key-filtered composition and merge) and the storage-failure recovery. The tests of Kahuna's memory
and SQLite backends, its background writer and its backup driver stay in Kahuna. A recovery test injects a
write failure through `HookedEnvironment` on the default and io_uring environments; the other ported tests
run on the default environment, as Kahuna's do.

`TestStorageCorruption` damages real files, on both environments: a flipped byte in a data block makes a range
scan, a point read, a batched read and a whole-family scan throw `StorageCorruptionException`, and fail-stops the
store, which then refuses everything and declines the recovery with its files unchanged; damage that a
compaction finds fail-stops it through the refused write; a truncated table, a floor row of the wrong length and
an identity row that does not decode are refused; and it checks the identity, the durable state, and that an
as-of image and a merge carry no identity. `TestStorageCorruptionNode` runs a whole embedded node on a Solarium
store and WAL: a damaged block answers the first read with `StorageCorrupt` and takes the node out of
consensus, and a single node whose store directory was lost under a surviving log reports it as replaced and
rebuilds it from the log. A three-node cluster with a wiped store is not tested here.

End to end, a Kahuna server loaded the published extension, took writes over REST on `--storage solarium
--wal-storage solarium`, and read every key back after a SIGKILL and a restart, as the same run did on
RocksDB.
