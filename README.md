# Solarium

Solarium is an embedded key-value store for .NET, written in managed code. Its design and API follow RocksDB: one database per directory, with column families, atomic write batches and ordered iterators.

It is built as the log store of [Kommander](https://github.com/kahunakv/kommander), a Raft library. It covers the part of RocksDB that Kommander uses, and RocksDB is its reference behavior. The `SolariumDb.Kommander` package is a Kommander WAL backend that replaces RocksDB and needs no native library.

## Packages

| Package | What it is |
| --- | --- |
| `SolariumDb` | The store, for all platforms that .NET 10 supports. Memtables and cached data blocks are native memory, outside the GC heap. |
| `SolariumDb.IoUring` | A storage environment for Linux 6.1 or later that writes files through io_uring, on the [ioxide](https://github.com/MDA2AV/ioxide) runtime. |
| `SolariumDb.Kommander` | `SolariumWAL`, a Kommander `IWAL` with the on-disk layout of Kommander's `RocksDbWAL`. |
| `SolariumDb.Kahuna` | A Kahuna storage engine: the persistence backend and the Raft WAL. A Kahuna server loads it with `--extension-assembly`. |

All four are MIT-licensed.

## Use it

```csharp
using System.Text;
using Solarium;

using SolariumDb db = SolariumDb.Open("./data",
    new DbOptions { CreateMissingColumnFamilies = true },
    columnFamilies: ["metadata"]);

ColumnFamily metadata = db.GetColumnFamily("metadata");

// All or nothing, across column families; synced, so it survives a power loss once Write returns.
db.Write(new WriteBatch()
    .Put("log:0001"u8, "entry"u8)
    .Put("last"u8, "0001"u8, metadata),
    WriteOptions.Synced);

byte[]? value = db.Get("log:0001"u8);

using Iterator iterator = db.NewIterator(options: new ReadOptions { IterateUpperBound = "log;"u8.ToArray() });
for (iterator.Seek("log:"u8); iterator.Valid(); iterator.Next())
    Console.WriteLine(Encoding.UTF8.GetString(iterator.Key()));

db.DeleteRange("log:0000"u8, "log:0001"u8);
```

You can also use it as Kommander's WAL.

```csharp
using Solarium.Kommander;

using SolariumWAL wal = new("/var/lib/node1/raft", "v1", logger, syncWrites: true);
RaftManager raft = new(configuration, discovery, wal, communication, new HybridLogicalClock(), logger);
```

All members are safe to call from any thread. Concurrent writes share their WAL appends and flushes through group commit, and reads take no locks.

## Status

Solarium is young, and its on-disk format may change.

| Area | What works |
| --- | --- |
| API | Column families, atomic batches across them, `DeleteRange`, `DeleteFilesInRange`, checkpoints, and iterators with an upper bound. |
| Writes and reads | Group commit with pipelined writes, and lock-free reads on a skip-list memtable. |
| Recovery | A torn WAL tail is cut. Damage to flushed data fails the open. Power-loss and kill tests check both. |
| Corruption checks | Checksums are bound to a block's table and offset, and each table file is checked against the manifest. Damage throws `SolariumException` with `IsCorruption`. |
| Storage | Memtable flushes, LZ4 compression, WAL rotation with a size cap, and leveled compaction with dynamic level sizing and concurrent compactions. |
| Resource limits | A rate limiter, `bytes_per_sync`, write stalls, a memtable budget that databases can share (`WriteBufferManager`), and a block cache. |
| Monitoring | The RocksDB properties and live-file metadata that Kommander reads. |
| `Solarium.Kommander` | Passes Kommander's conformance suite and the behavior tests of its RocksDB backend, and implements its storage-damage contract. |
| `Solarium.Kahuna` | Passes the behavior tests of Kahuna's RocksDB backend, and implements its storage-damage contract. |

These parts are not done yet.

- A block cache that helps data blocks on stores larger than the cache. With `CacheDataBlocks` (off by default), reads are faster on a store that fits in the cache and slower on a store ten times its size.
- Recovery in the engine after a WAL, flush or compaction failure. The database refuses writes until you reopen it. `SolariumWAL` and the Kahuna store reopen it themselves, except after corruption, where they fail-stop.
- Subcompactions, which split one large merge across threads, and more than one flush thread.

## Documentation

| Document | Content |
| --- | --- |
| [docs/api.md](docs/api.md) | The public members and options with their RocksDB equivalents, and the supported properties. |
| [docs/design.md](docs/design.md) | The write and read paths, flushes, compactions, SSTables, and the files in the data directory. |
| [docs/durability.md](docs/durability.md) | What a write survives, the WAL format, and recovery. |
| [docs/storage-environments.md](docs/storage-environments.md) | The default and the io_uring environments, and when to choose each one. |
| [docs/rocksdb-differences.md](docs/rocksdb-differences.md) | The deliberate differences from RocksDB. |
| [docs/kommander.md](docs/kommander.md) | `Solarium.Kommander`: its options, its recovery, and its differences from `RocksDbWAL`. |
| [docs/kahuna.md](docs/kahuna.md) | `Solarium.Kahuna`: how to load it into a Kahuna server, and its differences from Kahuna's RocksDB backend. |
| [docs/testing.md](docs/testing.md) | How to run the tests, with io_uring through Docker, and what the crash tests check. |
| [docs/benchmarks.md](docs/benchmarks.md) | The benchmarks against RocksDB, and their results. |

## Build and test

```sh
dotnet build
dotnet test
```

The SDK is pinned in `global.json` (.NET 10). On Linux with io_uring, the storage tests use both environments. For other platforms, see [docs/testing.md](docs/testing.md).

## License

MIT; see [LICENSE](LICENSE).
