# Solarium.Kahuna

`Solarium.Kahuna` is a [Kahuna](https://github.com/kahunakv/kahuna) storage engine on Solarium. It covers both
things that a Kahuna node stores, and the node does not need the native RocksDB library.

| Kahuna option | Provider | Stores |
| --- | --- | --- |
| `--storage solarium` | `SolariumPersistenceBackendProvider` | The key-value and lock data, through `SolariumPersistenceBackend` |
| `--wal-storage solarium` | `SolariumWalProvider` | The Raft log, through `SolariumWAL` ([Solarium.Kommander](kommander.md)) |

It needs Kahuna 1.16.4 or later.

## Use it

Publish the package, then point the Kahuna server at the published assembly.

```sh
dotnet publish src/Solarium.Kahuna -c Release -o /opt/kahuna/solarium

kahuna-server --extension-assembly /opt/kahuna/solarium/Solarium.Kahuna.dll \
              --storage solarium --storage-path /var/lib/kahuna/data \
              --wal-storage solarium --wal-path /var/lib/kahuna/wal
```

The published folder is about 4 MB. It leaves out Kahuna.Core, Kommander and their dependencies, since the server
uses its copies of them. All the nodes of a cluster load the same artifact. The server logs each file's
SHA-256 and the engines it registered.

An embedded Kahuna node registers the providers in code.

```csharp
using Solarium.Kahuna;

EmbeddedKahunaOptions options = new() { Storage = "solarium", WalStorage = "solarium", /* paths... */ };
options.StorageProviders.Register(new SolariumPersistenceBackendProvider());
options.StorageProviders.Register(new SolariumWalProvider());
```

## Settings

| Setting | Default | Effect |
| --- | --- | --- |
| `--storage-compaction-rate-mbps` | none | Gives the store a `RateLimiter` for its flush and compaction writes, and allows `max(2, CPUs / 2)` compactions at once. |
| `--storage-max-background-compactions` | 1 if no rate is set | Sets the number of compactions at once. |
| `--storage-shared-cache-mb` | 0 | One `Cache` for the index and filter blocks of the WAL and the store, pinned at level 0. |
| `--storage-shared-memtable-budget-mb` | 0 | One `WriteBufferManager` for the memtables of the WAL and the store. |
| `--extension-option solarium.environment` | `default` | The storage environment: `default` or `io_uring`. |

### Compaction rate

The limiter paces flushes too. Set the rate above what the store ingests and below what would leave the disk no
room for the WAL's syncs. If it is too low, level 0 grows until writes slow down. On a disk whose synced writes
took 4 to 8 ms, a rate of 50 made synced batches of 64 rows 2.5 times as fast as one unpaced compaction. On the
same disk at about 1 ms, rates of 25 and 50 slowed the batches to 0.2 to 0.4 times the unpaced rate (see
[benchmarks.md](benchmarks.md#paced-background-writes)).

### Shared memory

Size the shared cache to fit all the index and filter blocks of the WAL and the store: about 2% of their table
bytes, or 4% with LZ4. With a shared cache, the memtable budget is charged into it. Add the
budget to its size.

When the memtables pass their budget, the database that writes next flushes its oldest memtable. Writes do not
stall. Size the budget above two of the WAL's 128 MiB shard memtables, or the WAL logs a warning at open and its
memtables are flushed before they fill. If only the store or only the WAL is Solarium's, it uses the settings
alone.

Memtable entries and cached data blocks are native memory, as with RocksDB. A node sized for RocksDB does not
need a larger GC heap limit (`DOTNET_GCHeapHardLimit*`). The index and filter partitions in the cache are on the
heap. Memtable chunks are reused up to the budget. One partly used 64 KiB chunk per processor and memtable can
come on top of it.

### Storage environment

```sh
kahuna-server --extension-assembly /opt/kahuna/solarium/Solarium.Kahuna.dll \
              --storage solarium --wal-storage solarium \
              --extension-option solarium.environment=io_uring
```

In code, use `EmbeddedKahunaOptions.ExtensionOptions` to set the environment. A provider that gets an
`IStorageEnvironment` in its constructor uses that one. `OpenAt` and `MergeCheckpointInto`, which Kahuna calls
for backups and restores, use the default environment unless the provider has one. An unknown `solarium.` key or
environment fails the open with a message that names it.

`io_uring` needs Linux. All the Solarium databases of the process share one `IoUringStorageEnvironment`. Choose
it when lookups often miss the page cache: batched reads are then 1.6 to 2.4 times as fast. It costs 3% to 5% on
batched reads that hit the page cache, and its writes slow down when readers use all CPUs. See
[storage-environments.md](storage-environments.md).

## Compatibility with the RocksDB backend

`SolariumPersistenceBackend` is a port of Kahuna's `RocksDbPersistenceBackend`.

| Part | Layout |
| --- | --- |
| Database | `{storage path}/{revision}`, with the column families `kv` and `locks` |
| Rows | Kahuna's `RocksDbKeyValueMessage` and `RocksDbLockMessage` protobufs |
| Keys | `{key}~CURRENT` for the current row, `{key}~{revision}` for each retained revision, `{key}~NOREV` for the no-revision provenance |

It also ports the durability floors, the scans, the checkpoints with their merge, the revision pruning, and the
recovery from a storage failure by a reopen in place.

A deleted key is released when age retention has passed its delete record and no snapshot hold is older. All its
rows go except the current one, which has the revision that the next write continues from. The prune deletes in
unsynced batches of up to 2,048 rows, and asks for a flush of the kv family when a sweep ends. If a power loss
drops a batch, the next pass deletes the rows again.

## Storage damage

The store implements Kahuna 1.15's storage-damage contract.

- **Fail-stop.** The first `SolariumException` with `IsCorruption` fail-stops the store and logs one Critical
  line. Later operations throw `StorageCorruptionException`, which Kahuna answers with the terminal
  `StorageCorrupt`. `GetFailStopReason` names the damage. `TryRecoverFromStorageFailure` declines, and the files
  are not changed. A scan throws at a damaged block and cannot end short. Kahuna then withholds candidacy and
  steps down the node's partitions. If the damage is found at open, the node does not start.
- **Durability floors.** `KeepsDurabilityFloors` is true. A data partition that lacks a floor was not flushed to
  this store. Kahuna answers 0 for it, and Kommander can refuse to replay a compacted suffix onto a store that
  lost its prefix. A floor row that is not 8 bytes is damage.
- **Store identity.** `SupportsStoreIdentity` is true. The identity is a synced row, `\0store_identity`, with the
  node's Raft endpoint, a creation id and a time. Scans do not return it. Kahuna stamps a new or older store at its first open. A
  store that another node stamped fails to open, and an empty store under a log with history is reported as lost
  or replaced. As-of images and merges carry no identity, and the node that opens them stamps them. A plain
  checkpoint has the identity of its store. A row that does not decode is damage.

## Differences from the RocksDB backend

- **Cache.** By default, the Solarium stores of a process share a 256 MiB cache for index and filter blocks,
  pinned at level 0. This is enough for about 12 GB of tables, or 6 GB with LZ4. Past that, a lookup that misses
  reads one partition again, a few kilobytes. With a cache of a quarter of the partitions, lookups from
  one thread were about 2.5 times slower. A miss on a table written before partitions reads its complete index
  and filter. Data blocks use the OS page cache, where RocksDB has them in its block cache.
- **Tuning.** Direct reads, Bloom filter settings, statistics and `--rocksdb-shared-memory` do not apply. The
  Bloom filter has 10 bits per key. The kv family has up to three memtables of 64 MiB and the locks family up to
  two of 8 MiB, as in the RocksDB backend.
- **Checkpoints.** They have no WAL, and the live store is not flushed. The tables are hard links, or copies on
  another file system. The content of the memtables goes to tables of the copy.
- **Compression.** Both families use LZ4. The RocksDB backend uses Snappy.
- **Compactions.** Unless a rate is set, one compaction is active at a time. The RocksDB backend allows
  `max(2, CPUs / 2)`. Several unpaced compactions slow the synced writes.
- **Trim rate.** The as-of trim of a checkpoint copy is not rate limited. `BackupCaptureThrottleBytesPerSec`
  also charges reads (RocksDB's `kAllIo`), and Solarium's limiter paces writes only.
- **Purge.** The trim and the merge flush each family before they compact it, because `CompactRange` does not
  flush a memtable.
- **Deletes.** `DeleteKeyValues` and `DeleteLocks` use one `DeleteRange` per key, from `key~` up to `key\x7F`.
  The RocksDB backend deletes row by row. A batch has one operation per key, which bounds the managed memory of a
  purge on a store with a deep history.
- **Delete-triggered compaction.** There is no equivalent of `CompactOnDeletionCollector`. On a store that takes
  no other writes, the disk space of deleted keys is not released until writes push the deletes down.
- **`MultiGet`** reads one column family per call. Batched reads here use the kv family only.

## Building against Kahuna

The package references `Kahuna.Core` from nuget.org. The server gives an extension its copy of each assembly it
has. If its `Kahuna.Core` is older than the referenced version, the extension fails to load. Raise the
reference only to a Kahuna release that the servers already use.

## Tests

`tests/Solarium.Kahuna.Tests` has the behavior tests of Kahuna's RocksDB backend, ported to
`SolariumPersistenceBackend`. They use the default environment, as Kahuna's do. The failure and damage tests use
both environments.

| Tests | What they check |
| --- | --- |
| `TestStorageCorruption` | Damaged blocks, tables and rows fail-stop the store or fail its open. It also checks the identity. |
| `TestStorageCorruptionNode` | A complete embedded node on damage or a lost store. A single node does not lead when its store's floor is below the log's compaction floor. In a three-node cluster, the follower is rebuilt from the leader's snapshot. |
| `TestDeletedKeyDiskRelease` | A pruned key is released, and can be written again after a restart. |
| `TestPruneWriteFailure` | A failed first batch of a sweep changes no row and no floor. The pass after the recovery releases all the keys. |

Three ported tests are skipped. The two cases of `DeletedKeySpace_GivesItsDiskSpaceBack_WithNoOtherWrites` need
delete-triggered compaction. `DeleteRecordInsideTheRetentionAge_KeepsItsHistoryRow_UntilTheAgePassesIt` needs age
retention to count from the commit of the replacing revision. This store and the released RocksDB backend count
it from the revision's commit.

The package was also tested end to end. A Kahuna server loaded the published extension, took writes over REST,
and read all the keys back after a SIGKILL and a restart.
