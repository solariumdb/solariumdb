# SolariumDb

An embedded key-value store for .NET, written in managed code, whose API and semantics follow RocksDB's: a
database in one directory, column families, atomic write batches across them, range deletes, ordered iterators,
flushes and compaction. It needs no native library and runs on every platform .NET 10 runs on.

## Use it

```sh
dotnet add package SolariumDb
```

```csharp
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
```

Every member is safe to call from any thread.

## Related packages

| Package | What it is |
| --- | --- |
| `SolariumDb.IoUring` | A storage environment for Linux 6.1 or later that writes the files through io_uring. |
| `SolariumDb.Kommander` | `SolariumWAL`, a write-ahead log for the Kommander Raft library on Solarium. |
| `SolariumDb.Kahuna` | A Kahuna storage engine on Solarium, loaded with `--extension-assembly`. |

## License

MIT.
