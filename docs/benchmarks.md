# Benchmarks

Four benchmark projects compare Solarium with RocksDB, the reference behavior:

| Project | Measures |
| --- | --- |
| `benchmarks/Solarium.Benchmarks` | BenchmarkDotNet: `Put`, memtable and range tombstone reads, raw `IStorageFile` I/O. Also three whole-workload scenarios: compaction, synced writes beside background writes, and a hyperthread sibling load. |
| `benchmarks/Solarium.Kommander.Benchmarks` | Kommander's Raft log: `SolariumWAL` against `RocksDbWAL` through Kommander's `IWAL`. |
| `benchmarks/Solarium.Kahuna.Benchmarks` | Kahuna's store: `SolariumPersistenceBackend` against `RocksDbPersistenceBackend` through Kahuna's `IPersistenceBackend`. |

Every result below comes from one session on 2026-10-29, on the commit that moved memtables and cached data blocks to native memory, unless its section gives another date:

- 13th Gen Intel Core i7-13620H (16 CPUs), 31 GiB of memory, Linux 6.12;
- ext4 on a Samsung PM9B1 NVMe drive, where a synced 1 KiB append took about 1 ms at the median;
- every benchmark under the server GC, as Kahuna's and Kommander's servers run.

Results taken on the same machine before 2026-10-29 are in this file's history. Synced writes then took 4 to 8 ms on this drive, so synced rates there are 2 to 5 times lower than here; the ratios between the engines carry over.

The GC changes the result of a change. Under the workstation GC, BenchmarkDotNet's default, collections took 20% to 29% of an unsynced `Put`, against 7% to 9% under the server GC. A change measured under one GC can therefore lose under the other.

## Running them

### `Solarium.Benchmarks`

It needs io_uring and stops if it is not available. In Docker:

```sh
docker build -t solarium-test .
docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1 --entrypoint dotnet solarium-test \
    run -c Release --no-build --project benchmarks/Solarium.Benchmarks -- --filter '*'
```

On the host, run it from the repository, so BenchmarkDotNet finds the project, with `TMPDIR` on the drive to measure: every case writes under the temporary directory.

The `Put` cases compare these stores:

- Solarium on each storage environment (`solarium-default`, `solarium-io_uring`);
- RocksDB through `Kommander.RocksDB`, Kommander's binding, with its defaults (`rocksdb`);
- RocksDB with `enable_pipelined_write` (`rocksdb-pipelined`);
- RocksDB with `allow_concurrent_memtable_write` off (`rocksdb-serial`), with `enable_write_thread_adaptive_yield` off (`rocksdb-block`), or with both off (`rocksdb-serial-block`).

The cases:

- **`SyncedPut`**: synced `Put`s.
- **`UnsyncedPut`**: unsynced `Put`s, with memtable flushes (`NoFlush: False`) or into one memtable that holds the whole iteration.
- **`UnsyncedPutWithReaders`**: unsynced `Put`s beside 4 or 16 threads that `Get` keys already written.

Each runs from 1, 2, 4 and 16 writer threads, with 128-byte values. Every iteration opens a new, empty database and writes the same number of `Put`s.

A `--filter` selects cases by their parameters, in the order `NoFlush` or `Readers`, then `Store`, then `Threads`. For example: `'*UnsyncedPut(NoFlush: True, Store: "solarium-default"*'`. The `Allocated` column counts managed memory only, so it leaves out RocksDB's allocations and Solarium's memtables.

At the end of each `Put` case, the benchmark prints diagnostic lines that start with `//`:

- `// Write groups`: the write groups by their number of writers, the waits for a leader, the spins and the concurrent inserts.
- `// CPU time`: the process's user and system time during the writes.
- `// Threads`: the time each kind of thread ran and waited in a run queue, on Linux.
- `// Reads`: the reads per second, in the cases with readers.

Environment variables change a run:

| Variable | Effect |
| --- | --- |
| `SOLARIUM_BENCH_THREADS` | A comma-separated list of writer thread counts in place of 1, 2, 4 and 16. |
| `SOLARIUM_WRITER_SPIN_US`, `SOLARIUM_WRITER_SHORT_SPIN_US` | The long and the short spin of a waiting writer, in microseconds. |
| `SOLARIUM_WRITER_SPIN_PROBE_INTERVAL` | The interval of the spin probes; 0 gives a fixed spin of `SOLARIUM_WRITER_SPIN_US`. |
| `SOLARIUM_ALLOW_CONCURRENT_MEMTABLE_WRITE=0` | The leader inserts every batch of its group. |
| `SOLARIUM_ENABLE_PIPELINED_WRITE=0` | Turns off pipelined commits, RocksDB's `enable_pipelined_write`, which Solarium turns on by default. |
| `SOLARIUM_MEASURE_GROUP_STAGES=1` | Times the stages of each group on the leader's thread and prints them. |

The scenarios run a whole workload to its end:

```sh
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- compaction own|shared|random solarium|rocksdb [name=value ...]
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- synced [compactions=N] [rows=N] [seconds=N] [rate=MiB/s] [auto=true] [bytespersync=KiB] [dir=path]
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- sibling-load writer-spin|busy <threads>
```

The compaction scenarios run with Kommander's shard options: at most 8 background jobs, level 0 triggers of 8, 28 and 44, a WAL cap of two memtables, 16 MiB memtables, and no compression in either engine.

- `own` is Kommander's Raft log with one partition per family. 4 partitions append 4 million rows each, 256-byte values in batches of 100. Every 100,000 rows, each partition raises its floor to keep 1 million rows and drops the files below the floor.
- `shared` puts 16 partitions on 8 families, as Kommander does past 8 partitions, so the floors' tombstones cover the whole dead range.
- `random` writes 12 million `Put`s of keys drawn uniformly from 4 million, from 4 threads.

Each scenario prints its write amplification (flush and compaction bytes over the user's, without the WAL), the deepest level 0, the stall time, and the live table bytes against the rows kept. The `name=value` arguments set its sizes and options; `compactions` sets Solarium's compaction threads, 6 by default.

`synced` loads 2 million random keys with 1 KiB values, then writes synced batches of 1,024 rows over them. Each second it prints the rows written, the memtable switches that waited for a flush, the stalls, and the time in the WAL append. `sibling-load` runs a load on the CPUs given to its container, to measure what a busy hyperthread sibling costs a case.

### `Solarium.Kommander.Benchmarks`

It measures `RocksDbWAL` from Kommander 1.11.3, and `SolariumWAL` on the default and the io_uring environment, each with its default tuning. Each writer thread writes its own partition, as `RaftManager` does.

```sh
dotnet run -c Release --project benchmarks/Solarium.Kommander.Benchmarks -- [--cases quick|full] [--only <text>] \
    [--engines rocksdb,solarium,solarium-io_uring] [--repeats 3] [--warmup 2] [--seconds 8] [--settle 2] \
    [--rate <ops/s per writer>] [--dir <path>] [--csv <file>]
```

The workloads:

- **`append`**: one `Proposed` entry per `Write`.
- **`commit`**: an entry, then its `Committed` marker.
- **`batch`**: 64 entries per `Write`.
- **`mixed`**: an append and its commit per entry, with a checkpoint and a compaction pass every 1,000 entries. Reader threads, one per 4 writers, read the last 64 entries of a random partition.

The engines of a case take turns in an order that rotates. Each run warms up for 2 s and measures for 8, and checks the log afterward. The summary takes the median of three repeats.

Run synced cases on the drive and unsynced cases on tmpfs (`--dir /dev/shm`). On a disk, unsynced runs measure the kernel's writeback, not the engine: repeats differed up to ten times. On a 16 GB tmpfs, measure Solarium's unsynced 4 KiB appends and batches for 4 to 5 s: 8 s of them fill it.

### `Solarium.Kahuna.Benchmarks`

It opens each backend through its storage provider with Kahuna's default configuration:

- `rocksdb`: Kahuna's RocksDB defaults, with direct reads and a 256 MiB block cache;
- `rocksdb-buffered`: the same with direct reads off, so its tables are read through the page cache, as Solarium's are;
- Solarium on the default environment (`solarium`) and on io_uring (`solarium-io_uring`).

```sh
dotnet run -c Release --project benchmarks/Solarium.Kahuna.Benchmarks -- --dir <path on the disk> [--cases quick|full] \
    [--datasets small,large,history] [--engines rocksdb,rocksdb-buffered,solarium,solarium-io_uring] [--repeats 3] \
    [--warmup 2] [--seconds 8] [--threads 1,8] [--only <text>] [--csv <file>]
```

The datasets:

| Dataset | Keys | Values | Note |
| --- | --- | --- | --- |
| `small` | 200,000 | 256 bytes | Fits in the page cache. |
| `large` | 2,000,000 | 1 KiB | About 2 GB of values. |
| `history` | 10,000 | 256 bytes | Each key written 8 times. |

Keys come in groups of 100 that share a prefix, and every row has its own random value, so compression does not shrink the store to nothing.

A session runs these steps:

1. Load the dataset in synced batches of 1,024 rows, as Kahuna's background writer does.
2. Reopen the store.
3. Wait for the drive to go quiet ("drain").
4. Run the measured cases:
   - `get`, `get-miss` and `get-batch16` (16 keys);
   - `prefix100` and `range100` (100 rows);
   - `store` of 1, 64 and 1,024 rows per synced call;
   - `mixed`: 8 readers beside a writer paced to 20,000 rows per second;
   - `checkpoint` and `checkpoint-asof`;
   - on `history` only, a `prune` sweep.

Each case also gives the CPU, the allocations, and the bytes read from and written to the drive per operation, from `/proc/self/io`.

## Results

### `Put`

Time per unsynced `Put`, with memtable flushes (`UnsyncedPut`, `NoFlush: False`). The fastest RocksDB variant for each thread count is named in the last column:

| Threads | Solarium | Solarium, io_uring | RocksDB | Fastest RocksDB variant |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 934 ns | 5,988 ns | 1,282 ns | 1,279 ns (`serial-block`) |
| 2 | 1,123 ns | 6,213 ns | 1,844 ns | 1,235 ns (`pipelined`) |
| 4 | 864 ns | 3,820 ns | 1,184 ns | 906 ns (`pipelined`) |
| 16 | 808 ns | 2,260 ns | 1,304 ns | 1,255 ns (`pipelined`) |

- **Unsynced writes are faster than RocksDB's.** Without flushes (`NoFlush: True`), Solarium took 704 to 1,032 ns and RocksDB 1,194 to 1,931 ns.
- **Readers slow them down less.** With 4 readers, Solarium took 1.0 to 1.5 µs per `Put`, against 1.6 to 3.0 µs for RocksDB with its defaults. With 16 readers on the 16 CPUs, it took 2.8 to 3.3 µs. RocksDB took 4.4 to 21.8 µs with its defaults, and 4.3 to 7.0 µs with its best variant for each thread count. The 16 readers made 9.1 to 9.9 million `Get`s per second beside Solarium's writers, and 5.4 to 5.9 million beside those of RocksDB's variants.
- **io_uring is slow for unsynced writes.** Each write is a hand-off to the reactor thread, so it took 3 to 7 times as long as on the default environment. With 16 readers taking every CPU, the reactor waits for a CPU: 37 to 44 µs per `Put`. On the commit before memtables moved to native memory, the case with 1 writer took 25 µs, with readers that made 8.8 million `Get`s per second against 10.7 million now. Faster reads probably leave the reactor less CPU; that was not measured further.
- **Synced writes are bound by the drive.** From 1 and 16 threads, Solarium matched RocksDB within 4%: 975 µs and 130 µs per `Put`, against 974 µs and 125 µs. From 2 and 4 threads it took 875 µs and 467 µs, against 636 µs and 384 µs. Solarium on io_uring took 740 µs and 501 µs there.

Solarium allocated 560 to 580 B of managed memory per synced `Put`, and RocksDB's binding none.

### Memtable and range tombstone reads

`MemTableReadBenchmarks`, on a memtable of 921,600 entries:

| | Time | Allocated |
| --- | ---: | ---: |
| `Get` | 799 ns | 208 B |
| `IteratorNext` | 19.9 ns | 0 B |
| `IteratorPrev` | 296 ns | 320 B |

A memtable node and its key are one place in its native arena, so each step of a search reads one place where a managed node, its links and its key were three objects. `Prev` copies the key of each source at each step. The iterator copies each position into buffers it reuses, so `Next` allocates nothing, and `Key()` and `Value()` are valid until the iterator moves, as in RocksDB.

`RangeTombstoneReadBenchmarks`, live keys beside range tombstones that the memtable or the table keeps fragmented:

| Tombstones | `Get`, memtable | `Next`, memtable | `Get`, table | `Next`, table |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 164 ns | 15 ns | 628 ns | 94 ns |
| 128 | 186 ns | 35 ns | 652 ns | 108 ns |
| 1,024 | 195 ns | 42 ns | 661 ns | 112 ns |
| 8,192 | 204 ns | 48 ns | 687 ns | 120 ns |

Before fragmentation, a read scanned every tombstone of each source: 37 µs per `Get` beside 8,192 of them.

`RangeTombstoneChurnBenchmarks` alternates a `DeleteRange` with a `Get` or with the `Seek` of a new iterator, with 0 or 4 threads reading meanwhile. A pair took 3 to 12 µs at every tombstone count from 0 to 8,192, and allocated 1.3 to 3.1 KB without readers. The runs that each delete builds keep the read after it from rebuilding all the fragments (see [design.md](design.md)). When the memtable fragmented all its tombstones at the first read after each delete, as RocksDB does, a pair took 2 ms at 8,192.

### Storage files

`StorageFileBenchmarks` and `StorageFileWriteBenchmarks`, time per positional read or write, on the drive:

| | 4 KB | 64 KB |
| --- | ---: | ---: |
| read, default | 344 ns | 5.40 µs |
| read, io_uring | 333 ns | 5.38 µs |
| write, default | 0.93 µs | 13.8 µs |
| write, io_uring | 6.33 µs | 21.7 µs |

Reads match because the io_uring environment reads with `pread` on the calling thread. A write is a hand-off to the reactor (see [storage-environments.md](storage-environments.md)).

### Compaction scenarios

One run of each:

| | `own`, Solarium | `own`, RocksDB | `shared`, Solarium | `shared`, RocksDB | `random`, Solarium | `random`, RocksDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| rows per second | 1,774,909 | 2,050,722 | 1,526,227 | 1,667,398 | 607,302 | 342,799 |
| write amplification | 1.07 | 0.96 | 2.14 | 2.31 | 4.63 | 4.31 |
| compaction bytes written | 0 | 0 | 4,719 MiB | 5,969 MiB | 11,084 MiB | 10,311 MiB |
| most level 0 files | 8 | 9 | 9 | 16 | 28 | 23 |
| writes stalled | 0 s | 0 s | 0 s | 0 s | 0.5 s | 6.0 s |
| settled live tables against the rows kept | 1.07x | 0.98x | 1.78x | 1.25x | 1.27x | 1.23x |

- **Appended keys write nothing.** In Kommander's `own` layout, Solarium moved all 289 flushed files down without a rewrite, and RocksDB wrote no compaction byte either.
- **`random` ran 1.8 times as fast as RocksDB,** for 7% more compaction bytes, with level 0 at the slowdown trigger in 24 of 1,056 samples.
- **`shared` settles with more dead rows.** It wrote less than RocksDB, but settled at 1.78 times the rows kept, against 1.25; the cause was not measured.

### Synced writes beside background writes

`synced`, 60 seconds of synced batches after the load:

| Compaction threads, pacing | Synced rows per second |
| --- | ---: |
| 1, none | 50,555 |
| 1, `BytesPerSync` 1 MiB | 59,830 |
| 8, none | 64,502 |
| 8, `BytesPerSync` 1 MiB | 61,411 |
| 8, limiter of 25 MiB/s | 13,424 |
| 8, limiter of 50 MiB/s | 25,160 |
| 8, limiter of 50 MiB/s, auto-tuned | 25,215 |

Without a limiter, no memtable switch waited and no write stalled. The rate moved from second to second with the compactions running, from 16,000 to 100,000 rows per second.

### Paced background writes

`DbOptions.RateLimiter` paces flushes as well as compactions. At 25 or 50 MiB/s, flushes fell behind the 50 to 65 MB/s that the synced batches wrote unpaced: a memtable switch waited for a flush in every sample, and the batches slowed to 0.2 to 0.4 times the unpaced rate.

A limiter helps only while its rate stays above the store's ingest and leaves the WAL part of the drive. On 2026-10-27, synced writes on this drive took 4 to 8 ms, and the batches made 11,000 to 13,000 rows per second. There, a limiter of 50 MiB/s brought 8 compaction threads back to the rate of one, and made Kahuna's `store b64` 2.5 to 3 times as fast. An auto-tuned limiter dropped to about 3 MiB/s after a quiet period, and the batches waited for level 0.

`BytesPerSync` alone changed the rate by -5% to +18%, within the spread of the compactions.

### The Kommander WAL

Entries per second, the median of three repeats. Synced cases ran on the drive, unsynced ones on tmpfs:

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

- **Synced writes are bound by the drive.** Both engines group concurrent writers into one sync and came out within 8% of each other, except batches from 16 writers at 1.2 times. Solarium spent less CPU per entry: 25 to 54 µs per synced append, against 39 to 112 µs for RocksDB.
- **Unsynced writes measure the CPU.** Solarium wrote 1.5 to 2.8 times RocksDB's entries, with a lower median latency. Its p99.9 was higher in the appends and commits of 1 KiB or more from 4 writers or more: 0.2 to 1.9 ms, against RocksDB's 0.02 to 0.4 ms. Collections paused it 230 to 390 ms of each 10 s run. It allocated 1.6 to 9.2 KB of managed memory per entry, about twice the payload, where RocksDB's binding allocated 0 to 216 B. Where those allocations come from was not profiled; the memtable is native, so they come from the write path above it.
- **io_uring is slower for unsynced writes,** 0.36 to 1.03 times RocksDB's: each write is a hand-off to the reactor. Batches of 64 amortize it, at 2.59 times RocksDB's.
- **The mixed workload's readers read more on Solarium.** Synced, they made 218,000 to 230,000 reads per second, against RocksDB's 126,000. Unsynced, they made 41,000 to 45,000 on the default environment and 50,000 to 58,000 on io_uring, against 35,000 to 37,000.

`--rate` paces each writer. Four unsynced writers of 1 KiB entries, 20 s measured, one repeat:

| Ingest | p99.9, RocksDB / Solarium | Longest write, RocksDB / Solarium | GC pause, Solarium |
| --- | --- | --- | ---: |
| As fast as possible, about 410,000 and 770,000 entries/s (5 s measured) | 27 µs / 98 µs | 13 ms / 24 ms | 160 ms (3.2%) |
| 25,000 entries/s per writer (100 MB/s) | 75 µs / 238 µs | 13 ms / 26 ms | 186 ms (0.9%) |
| 5,000 entries/s per writer (20 MB/s) | 85 µs / 242 µs | 11 ms / 13 ms | 40 ms (0.2%) |

### Kahuna's persistence backend

Operations per second (rows for writes and scans, keys for batched reads), or seconds for the cases that run once. The median of three repeats, in the page cache.

`large`:

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

`small` and `history`, Solarium against `rocksdb-buffered`:

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

- **Reads from the page cache beat RocksDB's, with no block cache for data.** On `large`, Solarium made 1.05 to 1.8 times `rocksdb-buffered`'s gets and scans; on `history`, 1.4 to 1.95 times; on `small`, 0.93 to 1.35 times. Misses are the exception on `small` and `history`: 0.68 to 0.99 times. Those stores are small enough that part of them is probably still in memtables, which Solarium replays from its WAL at open, where RocksDB flushes them; that was not measured.
- **io_uring reads as fast as the default environment,** except misses from 8 threads on `large`, at 0.8 times: its reads are `pread`s on the calling thread.
- **Kahuna's RocksDB defaults lose on every read that misses the block cache.** With direct reads, each such block comes from the drive. On `large`, `rocksdb` read 3 KB per get from the drive and made 14,000 gets per second from one thread.
- **Writes.** Synced writes of one row are bound by the drive on every engine. Larger synced batches, and loads, ran 1.07 to 1.32 times `rocksdb-buffered`'s. Solarium wrote fewer bytes per row than RocksDB: 7,013 against 7,958 for the `large` load, and 9,771 against 12,561 for `store b1024`. The mixed writer reached its 20,000 rows per second on every engine but `rocksdb`.
- **The drain after the load is longer.** Every store settled at 2.4 to 2.6 GB. The compactions that the load left behind wrote 2.2 to 4.3 GB on Solarium, against 0.7 to 1.6 GB on RocksDB, which runs up to 8 compactions at once where `Solarium.Kahuna` runs one.
- **Checkpoints link tables,** as RocksDB's do. `checkpoint-asof` trims the copy and compacts it, and took as long as `rocksdb-buffered`'s.

### Larger than memory

The `large` dataset again, one repeat per engine, each in its own process under a 2 GiB memory limit (`systemd-run --user --scope -p MemoryMax=2G -p MemorySwapMax=0`). The limit bounds the page cache to less than any store:

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

- **Gets and scans are bound by the drive.** Each engine read about 3 to 4 KB from the drive per get. Solarium made 1.2 to 1.3 times `rocksdb-buffered`'s gets, and 1.8 times its scans; a scan read 0.7 KB where RocksDB read 2.1 KB.
- **Writes lose under the limit.** Solarium's load ran at 0.9 times, `store b1024` at 0.33 times, and its mixed writer at 0.54 times. Solarium on io_uring loaded slower, but wrote `store b1024` at 1.4 times `rocksdb-buffered`'s rows. Each engine ran once, so these rows vary more than the medians above.

### Data blocks in a cache

`ColumnFamilyOptions.CacheDataBlocks` puts the data blocks that reads take into the block cache, verified and decompressed, in native memory. The read cases of a build in which every family of the Kahuna store caches its data blocks in Kahuna's shared 256 MiB cache. Three sessions on `small`, which fits in the cache, and two on `large`, about ten times its size. Ratios to the results above, for the default environment and io_uring:

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

- **On a store that fits, it pays from 1 and from 8 threads:** 1.14 to 1.46 times the gets, and 1.25 to 1.34 times in the mixed workload. Each hit saves a system call, a checksum and an LZ4 decompression. Misses from 8 threads lose, at 0.75 times; that was not profiled.
- **On a store ten times the cache, it loses, at 0.65 to 0.93 times,** except misses from 8 threads on io_uring. Most reads miss, and a miss copies its block into the cache and evicts another under the shard's lock. The garbage collector no longer pays for it: a get allocated about 2.4 KB of managed memory with the cache or without, and gen 2 collections stayed at 0 to 2 per case.

So `CacheDataBlocks` stays off by default, and neither `SolariumWAL` nor `Solarium.Kahuna` sets it. A store that fits in its cache gains from it.

### Index and filter blocks in a cache

This one comes from a scratch program, run on 2026-10-26 on fully compacted stores in tmpfs, and is not in the repository. It measured `Get` on stores of Kahuna's shapes, with each table's index and filter split into partitions of about 4 KB under a top-level index:

| `Get`, thousands per second | Resident | Cache holds all | Cache of a quarter |
| --- | ---: | ---: | ---: |
| `small`, 1 thread | 540 | 528 | 212 |
| `small`, 16 threads | 5,127 | 4,921 | 244 |
| `large` LZ4, 1 thread | 489 | 464 | 213 |
| `large` LZ4, 16 threads | 4,394 | 4,068 | 289 |

- A cache that holds every partition read 2% to 7% slower than resident ones.
- A cache of a quarter of them read 2.5 times slower from one thread, and 11 to 20 times slower from 16, where the misses' decoded partitions were garbage. Before partitions, a miss decoded a table's whole index and filter, about 1 MB, and lookups ran hundreds of times slower.
- Opening a store adds about 1.2% to 1.7% of its table bytes to the heap, 2.4% to 3.4% with LZ4. A reader whose partitions are cached keeps only the top-level index, 0.04% of its table.
