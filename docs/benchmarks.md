# Benchmarks

Three benchmark projects compare Solarium with RocksDB.

| Project | Measures |
| --- | --- |
| `benchmarks/Solarium.Benchmarks` | BenchmarkDotNet cases for `Put`, reads and storage files, and scenarios that execute a complete workload. |
| `benchmarks/Solarium.Kommander.Benchmarks` | Kommander's Raft log: `SolariumWAL` against `RocksDbWAL` through Kommander's `IWAL`. |
| `benchmarks/Solarium.Kahuna.Benchmarks` | Kahuna's store: `SolariumPersistenceBackend` against `RocksDbPersistenceBackend` through Kahuna's `IPersistenceBackend`. |

The results come from one session on 2026-10-29, unless a section gives another date. The machine was a 13th Gen Intel Core i7-13620H (16 CPUs) with 31 GiB of memory and Linux 6.12. The drive was a Samsung PM9B1 NVMe with ext4, where a synced 1 KiB append took about 1 ms at the median. The benchmarks used the server GC, as Kahuna's and Kommander's servers do.

Note that the GC changes the results. Under the workstation GC, BenchmarkDotNet's default, collections took 20% to 29% of an unsynced `Put`. Under the server GC they took 7% to 9%.

## Running them

### `Solarium.Benchmarks`

It needs io_uring. You can use Docker.

```sh
docker build -t solarium-test .
docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1 --entrypoint dotnet solarium-test \
    run -c Release --no-build --project benchmarks/Solarium.Benchmarks -- --filter '*'
```

On the host, start it in the repository, and point `TMPDIR` at the drive to measure. All cases write under the temporary directory.

The `Put` cases compare several stores.

| Store | Configuration |
| --- | --- |
| `solarium-default`, `solarium-io_uring` | Solarium on each storage environment |
| `rocksdb` | RocksDB through `Kommander.RocksDB`, with its defaults |
| `rocksdb-pipelined` | RocksDB with `enable_pipelined_write` |
| `rocksdb-serial` | RocksDB with `allow_concurrent_memtable_write` off |
| `rocksdb-block` | RocksDB with `enable_write_thread_adaptive_yield` off |
| `rocksdb-serial-block` | RocksDB with both off |

| Case | Workload |
| --- | --- |
| `SyncedPut` | Synced `Put`s |
| `UnsyncedPut` | Unsynced `Put`s, with memtable flushes (`NoFlush: False`) or into one large memtable |
| `UnsyncedPutWithReaders` | Unsynced `Put`s beside 4 or 16 threads that `Get` existing keys |

The cases use 1, 2, 4 and 16 writer threads and 128-byte values. Each iteration opens a new database and writes the same number of `Put`s.

A `--filter` selects cases by their parameters, in the order `NoFlush` or `Readers`, then `Store`, then `Threads`. An example is `'*UnsyncedPut(NoFlush: True, Store: "solarium-default"*'`. The `Allocated` column counts managed memory only, which excludes RocksDB's allocations and Solarium's memtables.

At the end, a `Put` case prints diagnostic lines that start with `//`: `Write groups`, `CPU time`, `Threads` (Linux) and `Reads`.

Environment variables change the configuration.

| Variable | Effect |
| --- | --- |
| `SOLARIUM_BENCH_THREADS` | A comma-separated list of writer thread counts. |
| `SOLARIUM_WRITER_SPIN_US`, `SOLARIUM_WRITER_SHORT_SPIN_US` | The long and the short spin of a waiting writer, in microseconds. |
| `SOLARIUM_WRITER_SPIN_PROBE_INTERVAL` | The interval of the spin probes; 0 gives a fixed spin of `SOLARIUM_WRITER_SPIN_US`. |
| `SOLARIUM_ALLOW_CONCURRENT_MEMTABLE_WRITE=0` | The leader inserts every batch of its group. |
| `SOLARIUM_ENABLE_PIPELINED_WRITE=0` | Turns off pipelined commits. |
| `SOLARIUM_MEASURE_GROUP_STAGES=1` | Times the stages of each group on the leader's thread and prints them. |

The scenarios execute a complete workload.

```sh
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- compaction own|shared|random solarium|rocksdb [name=value ...]
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- synced [compactions=N] [rows=N] [seconds=N] [rate=MiB/s] [auto=true] [bytespersync=KiB] [dir=path]
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- sibling-load writer-spin|busy <threads>
```

The compaction scenarios use Kommander's shard options: at most 8 background jobs, level 0 triggers of 8, 28 and 44, a WAL cap of two memtables, and 16 MiB memtables. Compression is off in both engines.

| Scenario | Workload |
| --- | --- |
| `own` | Kommander's Raft log with one partition per family. 4 partitions append 4 million rows each, 256-byte values in batches of 100. Each 100,000 rows, a partition raises its floor to retain 1 million rows and drops the files below it. |
| `shared` | 16 partitions on 8 families. The floors' tombstones cover the complete dead range. |
| `random` | 12 million `Put`s from 4 threads, with keys drawn from 4 million. |
| `synced` | Loads 2 million random keys with 1 KiB values, then writes synced batches of 1,024 rows over them. |
| `sibling-load` | A load on the container's CPUs, to measure the cost of a busy hyperthread sibling. |

A compaction scenario prints its write amplification (flush and compaction bytes over the user's, excluding the WAL), the deepest level 0, the stall time, and the live table bytes against the rows kept. The `name=value` arguments set sizes and options. For example, `compactions` sets Solarium's compaction threads, 6 by default.

### `Solarium.Kommander.Benchmarks`

It measures `RocksDbWAL` from Kommander 1.11.3 and `SolariumWAL` on both environments, with their default tuning. Each writer thread has one partition, as in `RaftManager`.

```sh
dotnet run -c Release --project benchmarks/Solarium.Kommander.Benchmarks -- [--cases quick|full] [--only <text>] \
    [--engines rocksdb,solarium,solarium-io_uring] [--repeats 3] [--warmup 2] [--seconds 8] [--settle 2] \
    [--rate <ops/s per writer>] [--dir <path>] [--csv <file>]
```

| Workload | Operations |
| --- | --- |
| `append` | One `Proposed` entry per `Write` |
| `commit` | An entry, then its `Committed` marker |
| `batch` | 64 entries per `Write` |
| `mixed` | An append and a commit per entry, with a checkpoint and a compaction pass each 1,000 entries. One reader per 4 writers reads the last 64 entries of a random partition. |

The engines of a case take turns in a rotating order. A run warms up for 2 s, measures for 8 s, and checks the log afterward. The summary has the median of three repeats.

You should put synced cases on the drive and unsynced cases on tmpfs (`--dir /dev/shm`). Unsynced runs on a disk measure the kernel's writeback, and repeats differed up to ten times. On a 16 GB tmpfs, limit Solarium's unsynced 4 KiB appends and batches to 4 to 5 s, or they fill it.

### `Solarium.Kahuna.Benchmarks`

It opens each backend through its storage provider, with Kahuna's default configuration. `rocksdb` has direct reads and a 256 MiB block cache. `rocksdb-buffered` turns direct reads off and uses the page cache, as Solarium does.

```sh
dotnet run -c Release --project benchmarks/Solarium.Kahuna.Benchmarks -- --dir <path on the disk> [--cases quick|full] \
    [--datasets small,large,history] [--engines rocksdb,rocksdb-buffered,solarium,solarium-io_uring] [--repeats 3] \
    [--warmup 2] [--seconds 8] [--threads 1,8] [--only <text>] [--csv <file>]
```

| Dataset | Keys | Values | Note |
| --- | --- | --- | --- |
| `small` | 200,000 | 256 bytes | Fits in the page cache. |
| `large` | 2,000,000 | 1 KiB | About 2 GB of values. |
| `history` | 10,000 | 256 bytes | Each key written 8 times. |

Keys come in groups of 100 that share a prefix, and each row has a random value.

1. Load the dataset in synced batches of 1,024 rows.
2. Reopen the store.
3. Wait for the drive to go quiet ("drain").
4. Measure the cases.

| Case | Operation |
| --- | --- |
| `get`, `get-miss`, `get-batch16` | Point reads, misses, and batches of 16 keys |
| `prefix100`, `range100` | Scans of 100 rows |
| `store b1`, `store b64`, `store b1024` | Synced writes of 1, 64 and 1,024 rows per call |
| `mixed` | 8 readers beside a writer paced to 20,000 rows per second |
| `checkpoint`, `checkpoint-asof` | Checkpoints |
| `prune` | A revision sweep, on `history` only |

The output also has the CPU, the allocations and the drive I/O per operation.

## Results

### `Put`

Time per unsynced `Put`, with memtable flushes (`UnsyncedPut`, `NoFlush: False`).

| Threads | Solarium | Solarium, io_uring | RocksDB | Fastest RocksDB variant |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 934 ns | 5,988 ns | 1,282 ns | 1,279 ns (`serial-block`) |
| 2 | 1,123 ns | 6,213 ns | 1,844 ns | 1,235 ns (`pipelined`) |
| 4 | 864 ns | 3,820 ns | 1,184 ns | 906 ns (`pipelined`) |
| 16 | 808 ns | 2,260 ns | 1,304 ns | 1,255 ns (`pipelined`) |

- **Unsynced writes** are faster than RocksDB's. With flushes off, Solarium took 704 to 1,032 ns and RocksDB 1,194 to 1,931 ns.
- **Readers** slow Solarium's writes down less. With 4 readers, Solarium took 1.0 to 1.5 µs per `Put` and RocksDB's defaults 1.6 to 3.0 µs. With 16 readers, Solarium took 2.8 to 3.3 µs, RocksDB's defaults 4.4 to 21.8 µs, and its best variants 4.3 to 7.0 µs. The 16 readers made 9.1 to 9.9 million `Get`s per second beside Solarium's writers, and 5.4 to 5.9 million beside RocksDB's.
- **io_uring** took 3 to 7 times as long for unsynced writes, since each write is a hand-off to the reactor thread. With 16 readers it took 37 to 44 µs per `Put`.
- **Synced writes** are bound by the drive. From 1 and 16 threads, Solarium matched RocksDB within 4%: 975 µs and 130 µs per `Put`, against 974 µs and 125 µs. From 2 and 4 threads it took 875 µs and 467 µs, against 636 µs and 384 µs. Solarium on io_uring took 740 µs and 501 µs there.
- **Allocations** were 560 to 580 B of managed memory per synced `Put`. RocksDB's binding allocated none.

### Memtable and range tombstone reads

`MemTableReadBenchmarks` uses a memtable of 921,600 entries.

| | Time | Allocated |
| --- | ---: | ---: |
| `Get` | 799 ns | 152 B |
| `IteratorNext` | 19.9 ns | 0 B |
| `IteratorPrev` | 296 ns | 264 B |

`Next` allocates nothing, since the iterator reuses its buffers. `Prev` copies the key of each source at each step.

`TableReadBenchmarks` reads the same entries from one SSTable. `GetMissing` reads absent keys in the table's range, which the Bloom filter answers. `IteratorNext` covers 65,536 rows. The blocks come from the file or from a block cache that fits the table (`CacheDataBlocks`). These results are from a development machine (macOS, Apple silicon, the file in the page cache).

| | Time, file | Time, cached | Allocated |
| --- | ---: | ---: | ---: |
| `Get` | 1,115 ns | 371 ns | 152 B |
| `GetMissing` | 97 ns | 89 ns | 0 B |
| `IteratorNext` | 38 ns | 26 ns | 0 B |

`CompactRangeBenchmarks` times one manual compaction and one flush on the same development machine. The store has 1,000,000 `Put`s of 31-byte keys, drawn from 500,000, and 128-byte values, in four level 0 tables. The flush writes the last quarter. The results are medians of 10 iterations and include the flush to the drive.

| | Time, no compression | Time, LZ4 | Allocated |
| --- | ---: | ---: | ---: |
| `CompactRange` | 103 ms | 99 ms | 7.4 MB |
| `Flush` | 45 ms | 46 ms | 2.8 MB |

With a compaction readahead of 2 MiB, RocksDB's default, the uncompressed merge took 144 ms. Solarium uses 256 KiB.

`RangeTombstoneReadBenchmarks` reads live keys beside fragmented range tombstones.

| Tombstones | `Get`, memtable | `Next`, memtable | `Get`, table | `Next`, table |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 164 ns | 15 ns | 628 ns | 94 ns |
| 128 | 186 ns | 35 ns | 652 ns | 108 ns |
| 1,024 | 195 ns | 42 ns | 661 ns | 112 ns |
| 8,192 | 204 ns | 48 ns | 687 ns | 120 ns |

`RangeTombstoneChurnBenchmarks` alternates a `DeleteRange` with a `Get` or the `Seek` of a new iterator, beside 0 or 4 reader threads. A pair took 3 to 12 µs at all tombstone counts from 0 to 8,192.

### Storage files

Time per positional read or write on the drive (`StorageFileBenchmarks`, `StorageFileWriteBenchmarks`).

| | 4 KB | 64 KB |
| --- | ---: | ---: |
| read, default | 344 ns | 5.40 µs |
| read, io_uring | 333 ns | 5.38 µs |
| write, default | 0.93 µs | 13.8 µs |
| write, io_uring | 6.33 µs | 21.7 µs |

For the reasons, see [storage-environments.md](storage-environments.md).

### Compaction scenarios

Each scenario ran once.

| | `own`, Solarium | `own`, RocksDB | `shared`, Solarium | `shared`, RocksDB | `random`, Solarium | `random`, RocksDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| rows per second | 1,774,909 | 2,050,722 | 1,526,227 | 1,667,398 | 607,302 | 342,799 |
| write amplification | 1.07 | 0.96 | 2.14 | 2.31 | 4.63 | 4.31 |
| compaction bytes written | 0 | 0 | 4,719 MiB | 5,969 MiB | 11,084 MiB | 10,311 MiB |
| most level 0 files | 8 | 9 | 9 | 16 | 28 | 23 |
| writes stalled | 0 s | 0 s | 0 s | 0 s | 0.5 s | 6.0 s |
| settled live tables against the rows kept | 1.07x | 0.98x | 1.78x | 1.25x | 1.27x | 1.23x |

- **`own`** wrote no compaction bytes in either engine. Solarium moved all 289 flushed files down as they were.
- **`random`** was 1.8 times as fast as RocksDB, for 7% more compaction bytes.
- **`shared`** wrote less than RocksDB but settled at 1.78 times the rows kept, against 1.25. The cause was not measured.

### Synced writes beside background writes

The table shows 60 seconds of synced batches after the load (`synced`).

| Compaction threads, pacing | Synced rows per second |
| --- | ---: |
| 1, none | 50,555 |
| 1, `BytesPerSync` 1 MiB | 59,830 |
| 8, none | 64,502 |
| 8, `BytesPerSync` 1 MiB | 61,411 |
| 8, limiter of 25 MiB/s | 13,424 |
| 8, limiter of 50 MiB/s | 25,160 |
| 8, limiter of 50 MiB/s, auto-tuned | 25,215 |

When the limiter was off, no write stalled. The rate moved between 16,000 and 100,000 rows per second with the active compactions.

### Paced background writes

`DbOptions.RateLimiter` paces flushes as well as compactions. At 25 or 50 MiB/s, flushes fell behind the 50 to 65 MB/s that the synced batches wrote unpaced, and the batches slowed to 0.2 to 0.4 times the unpaced rate.

A limiter helps only if its rate is above the store's ingest and leaves the WAL part of the drive. On 2026-10-27, synced writes on this drive took 4 to 8 ms. There, a limiter of 50 MiB/s brought 8 compaction threads back to the rate of one and made Kahuna's `store b64` 2.5 to 3 times as fast. An auto-tuned limiter dropped to about 3 MiB/s after a quiet period, and the batches waited for level 0.

`BytesPerSync` alone changed the rate by -5% to +18%, which is within the spread of the compactions.

### The Kommander WAL

Entries per second, the median of three repeats. Synced cases used the drive, and unsynced cases used tmpfs.

| Case | RocksDB | Solarium | Solarium, io_uring | Solarium / RocksDB |
| --- | ---: | ---: | ---: | ---: |
| append, synced, 1 writer, 1 KiB | 953 | 962 | 934 | 1.01 |
| append, synced, 4 writers | 2,491 | 2,290 | 2,182 | 0.92 |
| append, synced, 16 writers | 7,468 | 7,387 | 7,337 | 0.99 |
| append, synced, 64 writers | 27,403 | 29,308 | 27,612 | 1.07 |
| append, synced, 16 writers, 128 B | 8,108 | 7,489 | 7,207 | 0.92 |
| append, synced, 16 writers, 4 KiB | 7,076 | 7,383 | 6,966 | 1.04 |
| commit, synced, 1 writer | 493 | 491 | 477 | 1.00 |
| commit, synced, 16 writers | 3,640 | 3,910 | 3,547 | 1.07 |
| batch of 64, synced, 1 writer | 54,991 | 56,463 | 51,271 | 1.03 |
| batch of 64, synced, 16 writers | 90,367 | 108,828 | 99,295 | 1.20 |
| mixed, synced, 16 writers | 3,419 | 3,473 | 3,135 | 1.02 |
| append, unsynced, 1 writer, 1 KiB | 453,489 | 748,007 | 162,199 | 1.65 |
| append, unsynced, 4 writers | 416,508 | 807,546 | 225,740 | 1.94 |
| append, unsynced, 16 writers | 373,158 | 892,220 | 383,954 | 2.39 |
| append, unsynced, 4 writers, 128 B | 804,853 | 1,212,970 | 288,074 | 1.51 |
| append, unsynced, 4 writers, 4 KiB (5 s) | 170,075 | 393,455 | 161,886 | 2.31 |
| commit, unsynced, 4 writers | 294,929 | 464,760 | 137,125 | 1.58 |
| batch of 64, unsynced, 4 writers (4 s) | 610,612 | 1,683,181 | 1,584,192 | 2.76 |
| mixed, unsynced, 4 writers | 189,440 | 286,339 | 98,292 | 1.51 |

- **Synced writes** are bound by the drive. The engines were within 8% of each other, except batches from 16 writers at 1.2 times. Solarium used 25 to 54 µs of CPU per synced append, and RocksDB 39 to 112 µs.
- **Unsynced writes** measure the CPU. Solarium wrote 1.5 to 2.8 times RocksDB's entries, with a lower median latency. Its p99.9 was higher for appends and commits of 1 KiB or more from 4 or more writers: 0.2 to 1.9 ms, against 0.02 to 0.4 ms. Collections paused it 230 to 390 ms in 10 s.
- **Allocations** were 1.6 to 9.2 KB of managed memory per entry, about twice the payload. RocksDB's binding allocated 0 to 216 B. The source was not profiled.
- **io_uring** wrote 0.36 to 1.03 times RocksDB's unsynced entries, and 2.59 times in batches of 64, which amortize the hand-off.
- **The mixed readers** made 218,000 to 230,000 reads per second in the synced case, against RocksDB's 126,000. In the unsynced case they made 41,000 to 45,000 on the default environment and 50,000 to 58,000 on io_uring, against 35,000 to 37,000.

`--rate` paces each writer. This test used four unsynced writers of 1 KiB entries, with 20 s measured and one repeat.

| Ingest | p99.9, RocksDB / Solarium | Longest write, RocksDB / Solarium | GC pause, Solarium |
| --- | --- | --- | ---: |
| As fast as possible, about 410,000 and 770,000 entries/s (5 s measured) | 27 µs / 98 µs | 13 ms / 24 ms | 160 ms (3.2%) |
| 25,000 entries/s per writer (100 MB/s) | 75 µs / 238 µs | 13 ms / 26 ms | 186 ms (0.9%) |
| 5,000 entries/s per writer (20 MB/s) | 85 µs / 242 µs | 11 ms / 13 ms | 40 ms (0.2%) |

### Kahuna's persistence backend

Operations per second (rows for writes and scans, keys for batched reads), or seconds. The values are medians of three repeats, in the page cache.

This table is for `large`.

| Case | rocksdb | rocksdb-buffered | Solarium | Solarium, io_uring | Solarium / rocksdb-buffered |
| --- | ---: | ---: | ---: | ---: | ---: |
| load | 66,235 | 72,476 | 95,677 | 93,797 | 1.32 |
| reopen | 0.12 s | 0.33 s | 0.12 s | 0.12 s | 2.75 |
| drain after load | 6 s | 4 s | 12 s | 12 s | 0.33 |
| get, 1 thread | 14,129 | 212,228 | 367,200 | 366,679 | 1.73 |
| get, 8 threads | 89,248 | 881,253 | 1,600,289 | 1,559,841 | 1.82 |
| get-miss, 1 thread | 1,041,139 | 1,275,127 | 1,342,035 | 1,308,867 | 1.05 |
| get-miss, 8 threads | 3,758,767 | 3,698,011 | 4,024,959 | 3,230,272 | 1.09 |
| get-batch16, 8 threads | 98,205 | 931,635 | 1,613,460 | 1,598,353 | 1.73 |
| prefix100, 1 thread | 98,599 | 555,101 | 849,881 | 819,671 | 1.53 |
| prefix100, 8 threads | 263,471 | 2,323,220 | 3,136,160 | 3,125,586 | 1.35 |
| store b1 | 292 | 944 | 977 | 970 | 1.03 |
| store b64 | 12,432 | 26,711 | 34,479 | 31,759 | 1.29 |
| store b1024 | 14,208 | 66,687 | 74,238 | 74,239 | 1.11 |
| mixed | 47,362 | 670,698 | 1,116,626 | 1,125,371 | 1.66 |
| checkpoint | 0.03 s | 0.04 s | 0.04 s | 0.04 s | 1.0 |
| checkpoint-asof | 12.1 s | 16.9 s | 16.7 s | 18.1 s | 1.01 |

This table compares `small` and `history` with `rocksdb-buffered`.

| Case | `small`, rocksdb-buffered | `small`, Solarium | `history`, rocksdb-buffered | `history`, Solarium |
| --- | ---: | ---: | ---: | ---: |
| load | 215,043 | 254,664 | 219,849 | 258,421 |
| get, 1 thread | 513,416 | 564,016 | 637,572 | 1,242,530 |
| get, 8 threads | 2,285,000 | 2,537,588 | 2,513,647 | 3,609,700 |
| get-miss, 1 thread | 2,138,596 | 2,127,855 | 2,724,889 | 1,842,485 |
| get-miss, 8 threads | 6,434,237 | 5,442,645 | 5,221,716 | 4,363,527 |
| get-batch16, 8 threads | 2,723,674 | 2,536,319 | 3,208,045 | 5,475,049 |
| prefix100, 8 threads | 5,674,143 | 7,637,274 | 1,969,776 | 3,687,772 |
| store b1024 | 126,205 | 160,767 | 190,204 | 224,635 |
| mixed | 1,506,173 | 2,133,669 | 3,035,821 | 3,543,188 |
| checkpoint-asof | 3.72 s | 3.22 s | 3.55 s | 3.05 s |
| prune | - | - | 180,340 | 236,408 |

- **Reads** from the page cache are faster than `rocksdb-buffered`'s: 1.05 to 1.8 times on `large`, 1.4 to 1.95 times on `history`, and 0.93 to 1.35 times on `small`. Misses on `small` and `history` are slower, at 0.68 to 0.99 times. The cause was not measured. It may be that part of these stores is in memtables, which Solarium replays from its WAL at open.
- **io_uring** reads as fast as the default environment, except misses from 8 threads on `large`, at 0.8 times.
- **Kahuna's RocksDB defaults** are slow on reads that miss the block cache. With direct reads, each of these blocks comes from the drive.
- **Writes** of one synced row are bound by the drive. Larger synced batches and loads were 1.07 to 1.32 times as fast as `rocksdb-buffered`'s. Solarium wrote 7,013 bytes per row for the `large` load, against 7,958, and 9,771 for `store b1024`, against 12,561.
- **The drain** after the load is longer. The remaining compactions wrote 2.2 to 4.3 GB on Solarium and 0.7 to 1.6 GB on RocksDB, which uses up to 8 compactions at once. `Solarium.Kahuna` uses one.
- **Checkpoints** link tables, as RocksDB's do. `checkpoint-asof` also trims and compacts the copy.

### Larger than memory

This is the `large` dataset with one repeat per engine, under a 2 GiB memory limit (`systemd-run --user --scope -p MemoryMax=2G -p MemorySwapMax=0`). The limit makes the page cache smaller than the stores.

| Case | rocksdb | rocksdb-buffered | Solarium | Solarium, io_uring |
| --- | ---: | ---: | ---: | ---: |
| load | 66,671 | 42,517 | 38,513 | 28,612 |
| get, 1 thread | 15,242 | 24,348 | 30,944 | 37,564 |
| get, 8 threads | 94,465 | 148,470 | 176,033 | 208,798 |
| get-miss, 1 thread | 1,008,256 | 1,221,601 | 1,080,098 | 1,130,674 |
| get-batch16, 8 threads | 105,631 | 159,149 | 180,477 | 309,438 |
| prefix100, 8 threads | 309,023 | 607,608 | 1,117,192 | 1,392,364 |
| store b1024 | 12,928 | 28,287 | 9,344 | 38,912 |
| mixed, gets | 45,232 | 91,242 | 71,179 | 43,942 |
| mixed, writer rows per second | 7,320 | 13,191 | 7,176 | 11,120 |
| checkpoint-asof | 37.1 s | 30.1 s | 17.4 s | 25.7 s |

- **Gets and scans** are bound by the drive. Solarium made 1.2 to 1.3 times `rocksdb-buffered`'s gets and 1.8 times its scans. A scan read 0.7 KB, where RocksDB read 2.1 KB.
- **Writes** are slower under the limit. Solarium's load was 0.9 times as fast, `store b1024` 0.33 times, and the mixed writer 0.54 times. On io_uring, the load was slower and `store b1024` was 1.4 times as fast as `rocksdb-buffered`'s. With one repeat, these rows vary more than the medians above.

### Data blocks in a cache

For this test, all families of the Kahuna store cached their data blocks (`ColumnFamilyOptions.CacheDataBlocks`) in Kahuna's shared 256 MiB cache. `small` fits in the cache, and `large` is about ten times its size. The values are ratios to the results above, for the default environment and io_uring. There were three sessions on `small` and two on `large`.

| Case | `small` | `large` |
| --- | ---: | ---: |
| get, 1 thread | 1.33, 1.34 | 0.65, 0.68 |
| get, 8 threads | 1.16, 1.14 | 0.81, 0.85 |
| get-batch16, 1 thread | 1.35, 1.39 | 0.67, 0.69 |
| get-batch16, 8 threads | 1.40, 1.46 | 0.81, 0.82 |
| get-miss, 1 thread | 0.96, 0.96 | 0.86, 0.93 |
| get-miss, 8 threads | 0.75, 0.77 | 0.91, 1.27 |
| prefix100, 1 thread | 1.14, 1.08 | 0.71, 0.73 |
| prefix100, 8 threads | 1.05, 1.03 | 0.74, 0.77 |
| range100, 8 threads | 1.05, 1.01 | 0.78, 0.78 |
| mixed | 1.25, 1.34 | 0.87, 0.87 |

- **On a store that fits,** gets were 1.14 to 1.46 times as fast, and the mixed workload 1.25 to 1.34 times. Misses from 8 threads were 0.75 times as fast. That was not profiled.
- **On a store ten times the cache,** reads were 0.65 to 0.93 times as fast, except misses from 8 threads on io_uring. Most reads miss, and a miss copies its block into the cache and evicts another under the shard's lock.

For this reason `CacheDataBlocks` is off by default, and `SolariumWAL` and `Solarium.Kahuna` do not set it.

### Index and filter blocks in a cache

A scratch program, which is not in the repository, measured `Get` on 2026-10-26. The stores had Kahuna's shapes, were fully compacted, and were in tmpfs.

| `Get`, thousands per second | Resident | Cache holds all | Cache of a quarter |
| --- | ---: | ---: | ---: |
| `small`, 1 thread | 540 | 528 | 212 |
| `small`, 16 threads | 5,127 | 4,921 | 244 |
| `large` LZ4, 1 thread | 489 | 464 | 213 |
| `large` LZ4, 16 threads | 4,394 | 4,068 | 289 |

- A cache that fits all the partitions was 2% to 7% slower than resident ones.
- A cache of a quarter of them was 2.5 times slower from one thread, and 11 to 20 times slower from 16, where the misses' decoded partitions were garbage.
- Opening a store adds about 1.2% to 1.7% of its table bytes to the heap, or 2.4% to 3.4% with LZ4. A reader whose partitions are cached has only the top-level index, 0.04% of its table.
