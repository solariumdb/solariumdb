# SolariumDb.Kahuna

A [Kahuna](https://github.com/kahunakv/kahuna) storage engine on Solarium, an embedded key-value store for .NET
with RocksDB's API, written in managed code. It provides both engines a Kahuna node uses, so a node runs with
no native RocksDB library:

- `--storage solarium`: the key-value and lock data (`SolariumPersistenceBackendProvider`), a port of Kahuna's
  RocksDB backend with the same rows, keys and column families;
- `--wal-storage solarium`: the Raft log (`SolariumWalProvider`), on `SolariumWAL` from `SolariumDb.Kommander`.

## Use it

Publish the package and load it into the Kahuna server as an extension assembly:

```sh
dotnet publish -c Release -o /opt/kahuna/solarium

kahuna-server --extension-assembly /opt/kahuna/solarium/Solarium.Kahuna.dll \
              --storage solarium --storage-path /var/lib/kahuna/data \
              --wal-storage solarium --wal-path /var/lib/kahuna/wal
```

Every node of a cluster loads the same artifact. An embedded node registers the providers instead:

```csharp
options.StorageProviders.Register(new SolariumPersistenceBackendProvider());
options.StorageProviders.Register(new SolariumWalProvider());
```

`--extension-option solarium.environment=io_uring` opens the WAL and the store on Solarium's io_uring storage
environment (Linux only; `EmbeddedKahunaOptions.ExtensionOptions` in code). `default`, or no setting, keeps the
default environment, and an unknown `solarium.` setting stops the node at startup.

`--storage-compaction-rate-mbps` paces the store's flush and compaction writes with a Solarium `RateLimiter`
and runs `max(2, CPUs / 2)` compactions at once; `--storage-max-background-compactions` sets that count. With
neither, the store runs one compaction at a time, unpaced. Choose a rate of about a third to half of what the
disk commits in synced writes.

`--storage-shared-cache-mb` gives the WAL and the store one cache for their tables' index and filter blocks,
and `--storage-shared-memtable-budget-mb` one `WriteBufferManager` for their memtables, charged into the cache
when both are set. 0, the default for each, keeps them apart. Size the budget above 256 MiB, two of the WAL's
shard memtables, or the WAL warns at open.

It needs Kahuna 1.14.4 or later: 1.13.4 brought the public storage contract (`IPersistenceBackend`,
`IPersistenceBackendProvider`, `IWalProvider`), 1.14.1 the two compaction settings, 1.14.2 the shared
cache and memtable budget, and 1.14.4 the `--extension-option` settings. The server's `Kahuna.Core` must be
at least the version this package references.
The tables keep their index and filter blocks in a 256 MiB cache that every store of the process shares unless
the node shares one with its WAL, as the RocksDB backend's block cache, enough for about 12 GB of tables (6 GB
with LZ4); data blocks are not cached. RocksDB tuning (direct reads, statistics, the shared block cache) does
not apply, a checkpoint holds no WAL, and the as-of trim of a checkpoint is not rate limited.
