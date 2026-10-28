# Solarium

Solarium is an embedded key-value store for .NET, written in managed code. Its design and its API follow RocksDB: a database in one directory, column families, atomic write batches across them, a write-ahead log (WAL), sorted in-memory tables (memtables), sorted files on disk (SSTables), compaction, range deletes, and ordered iterators.

It exists to be a durable log store for [Kommander](https://github.com/kahunakv/kommander), the Raft library: it covers the part of RocksDB that Kommander's Raft log uses, with RocksDB as the reference behavior, and it ships a Kommander WAL backend, `SolariumDb.Kommander`, that a host plugs into Kommander in place of RocksDB, with no native library.

## Packages

| Package | What it is |
| --- | --- |
| `SolariumDb` | The store. Pure managed code, on every platform .NET 10 runs on. |
| `SolariumDb.IoUring` | A storage environment for Linux 6.1 or later that does its file writes through io_uring, on the [ioxide](https://github.com/MDA2AV/ioxide) runtime. |
| `SolariumDb.Kommander` | `SolariumWAL`, a Kommander `IWAL` on Solarium, with the on-disk layout of Kommander's `RocksDbWAL`. |
| `SolariumDb.Kahuna` | A Kahuna storage engine: the persistence backend and the Raft WAL on Solarium, which a Kahuna server loads with `--extension-assembly`. |

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

As Kommander's WAL:

```csharp
using Solarium.Kommander;

using SolariumWAL wal = new("/var/lib/node1/raft", "v1", logger, syncWrites: true);
RaftManager raft = new(configuration, discovery, wal, communication, new HybridLogicalClock(), logger);
```

Every member is safe to call from any thread. Writes from many threads share their WAL appends and flushes through group commit, and reads take no locks.

## Status

Solarium is young, and its on-disk format may still change. Done:

- the embedded API, column families, atomic batches across them, `DeleteRange`, and iterators with an upper bound;
- group commit with pipelined writes, and lock-free reads on a skip-list memtable;
- WAL recovery that cuts a torn tail and refuses damage to flushed data, checked by power-loss and kill tests;
- memtable flushes to SSTables, LZ4 block compression, WAL rotation with a size cap, the manifest, leveled compaction into RocksDB's seven levels with its dynamic level sizing and concurrent compactions, a rate limiter and `bytes_per_sync` for flushes and compactions, a memtable budget shared across databases (`WriteBufferManager`), a block cache that bounds the memory of the tables' index and filter blocks, can hold the memtable budget too, and caches data blocks when asked, `DeleteFilesInRange`, checkpoints, and write stalls;
- the RocksDB properties and live-file metadata that Kommander reads;
- `Solarium.Kommander`, which passes Kommander's conformance suite for WAL backends and the behavior tests of its RocksDB backend, runs three-node Kommander clusters in the tests, and publishes the engine `raft.wal.*` metrics;
- `Solarium.Kahuna`, a port of Kahuna's RocksDB persistence backend that passes its behavior tests, with providers that a Kahuna server loads as an extension for `--storage solarium` and `--wal-storage solarium`.

Not done yet:

- a block cache that pays for data blocks on many threads: with the LRU cache, caching them (`CacheDataBlocks`, off by default) made reads slower from 8 threads and on stores larger than the cache, so the OS page cache serves them;
- recovery after a WAL, flush or compaction failure inside the engine: the database refuses all writes until it is opened again (`SolariumWAL` reopens it on its own);
- subcompactions, which split one large merge across threads, and more than one flush thread.

## Documentation

| Document | Content |
| --- | --- |
| [docs/api.md](docs/api.md) | Every public member and option, with its RocksDB equivalent, and the supported properties. |
| [docs/design.md](docs/design.md) | Column families, group commit, lock-free reads, flushes, compactions, write stalls, SSTables, and the files in the data directory. |
| [docs/durability.md](docs/durability.md) | What a write survives, the WAL format, and recovery after a crash or a power loss. |
| [docs/storage-environments.md](docs/storage-environments.md) | The default and the io_uring environments, and what io_uring costs and gains. |
| [docs/rocksdb-differences.md](docs/rocksdb-differences.md) | Every deliberate difference from RocksDB. |
| [docs/kommander.md](docs/kommander.md) | `Solarium.Kommander`: what it keeps from `RocksDbWAL`, its options, and its recovery. |
| [docs/kahuna.md](docs/kahuna.md) | `Solarium.Kahuna`: loading it into a Kahuna server, what it keeps from Kahuna's RocksDB backend, and its differences. |
| [docs/testing.md](docs/testing.md) | Running the tests, on io_uring through Docker, and the crash and kill tests. |
| [docs/benchmarks.md](docs/benchmarks.md) | The benchmarks against RocksDB, and their results. |

## Build and test

```sh
dotnet build
dotnet test
```

The SDK is pinned in `global.json` (.NET 10). On Linux with io_uring, the storage tests run on both environments; [docs/testing.md](docs/testing.md) explains how to run them elsewhere through Docker.

## License

MIT; see [LICENSE](LICENSE).
