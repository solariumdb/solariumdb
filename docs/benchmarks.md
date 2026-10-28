# Benchmarks

The BenchmarkDotNet project in `benchmarks/Solarium.Benchmarks` measures synced and unsynced `Put` from 1, 2, 4 and 16 threads (unsynced with memtable flushes and without them), unsynced `Put` with 4 or 16 reader threads that call `Get` during the writes, `Get` and iterator steps (`Next` and `Prev`) on a full memtable (`MemTableReadBenchmarks`), `Get` and `Next` of live keys beside 0 to 8,192 range tombstones in the memtable or in a table (`RangeTombstoneReadBenchmarks`), range deletes that alternate with reads (`RangeTombstoneChurnBenchmarks`), and raw `IStorageFile` reads and writes of 4 KB and 64 KB. It needs io_uring and stops if it is not available.

The `Put` and memtable read benchmarks run the server GC, as Kahuna's and Kommander's servers do; BenchmarkDotNet's default is the workstation GC. The GC changes the result of a change: under the workstation GC, collections took 20% to 29% of the time of an unsynced `Put`, and under the server GC 7% to 9%, so the server GC was 11% to 21% faster. A memtable that copied its keys and values into chunks on the pinned object heap, as RocksDB's arena does, made `Put` 4% to 9% faster under the workstation GC and up to 12% slower under the server GC. The figures elsewhere in these documents come from runs under the workstation GC, unless they say otherwise.

The `Put` benchmarks also run RocksDB, the reference behavior, through `Kommander.RocksDB`, the binding that Kommander uses: with RocksDB's defaults (`rocksdb`), with `enable_pipelined_write` (`rocksdb-pipelined`), and with `allow_concurrent_memtable_write` or `enable_write_thread_adaptive_yield` off, or both (`rocksdb-serial`, `rocksdb-block`, `rocksdb-serial-block`). Both engines get the same keys, values, write buffer size and job. A `--filter` pattern can select cases by parameter, for example `'*UnsyncedPut(NoFlush: True, Store: "solarium-default"*'`. The parameters appear in the case names in this order: `NoFlush` or `Readers`, then `Store`, then `Threads`. To run other thread counts, set `SOLARIUM_BENCH_THREADS` to a comma-separated list, for example `6,8,12`; it replaces 1, 2, 4 and 16. The `Allocated` column counts managed memory only, so it leaves out RocksDB's native allocations.

The `Put` and raw write benchmarks use fixed iteration and invocation counts, so every case of a benchmark writes the same amount of data. Each `Put` iteration opens a new, empty database, and each raw write iteration starts from an empty file. A database or a file that lives for the whole run makes later iterations slower than early ones. In Docker:

```sh
docker build -t solarium-test .
docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1 --entrypoint dotnet solarium-test \
    run -c Release --no-build --project benchmarks/Solarium.Benchmarks -- --filter '*'
```

At the end of each `Put` case, the benchmark prints the write groups that group commit formed, by their number of writers (for RocksDB, only the total, from its `rocksdb.write.self` ticker), the fewest and most groups in one invocation, the number of SSTables flushed, and for Solarium the writers that waited for a leader, those of them that waited for a WAL flush without a spin, the times they blocked, and the offers of leadership that a running writer overtook, on lines that start with `// Write groups`. A `// Write groups, writer spin` line gives the probes of the long spin, the share of them that blocked, the turns that spun short, and the credit at the end. Another such line gives the waits that spun, by the time until the writer's turn came: up to 1, 5, 20 and 100 µs, and longer. The process CPU time during the writes, user and system, is on a line that starts with `// CPU time`; it leaves out the start of the process and the open and close of each database. On Linux, a line that starts with `// Threads` gives the time that the writer threads, the reader threads and the io_uring reactor spent on a CPU and waiting in a run queue for one, from `/proc/self/task/*/schedstat`. The cases with readers also print the reads per second during the writes, on a line that starts with `// Reads`. The reads use the CPU time that the writers leave, so they show what a spin costs other work on the host. To measure another long spin, set `SOLARIUM_WRITER_SPIN_US` (in microseconds) for the benchmark process, and `SOLARIUM_WRITER_SHORT_SPIN_US` for another short spin. To set the interval of the probes, set `SOLARIUM_WRITER_SPIN_PROBE_INTERVAL`; 0 gives a fixed spin of `SOLARIUM_WRITER_SPIN_US`. A `// Write groups, concurrent inserts` line gives the groups whose writers inserted their own batches at once, the members whose inserts the leader launched, the members whose batches the leader inserted because they had blocked, the links that a concurrent insert searched again, and the times a writer blocked after its insert. `SOLARIUM_ALLOW_CONCURRENT_MEMTABLE_WRITE=0` makes the leader insert every batch of its group, so one image measures both. `SOLARIUM_MEASURE_GROUP_STAGES=1` times the stages of each group on the leader's thread, and a `// Write groups, stages per group` line gives the preparation (memtable switches and WAL rotation), the encode, the WAL append, the memtable inserts up to the publication, the hand-off of the leadership, the gap until the next group's leader starts, and, with pipelined commits, the wait of a memtable stage's leader until its stage starts, per group and per `Put`. `SOLARIUM_ENABLE_PIPELINED_WRITE=0` turns off the internal option `EnablePipelinedWrite`, on by default, RocksDB's `enable_pipelined_write`: a group's leader gives up its leadership after the WAL append, so the next group appends while the memtables are filled. A group that finds no memtable stage running starts one; otherwise it joins a memtable queue, and the next stage inserts and publishes every group in the queue, as RocksDB's memtable writer groups. A `// Write groups, memtable stages` line then gives the stages, the groups per stage, and the times a group's leader blocked in the memtable queue.

To measure what a busy hyperthread sibling costs a case, run a load in a second container on the sibling CPUs, and stop it after the case:

```sh
docker run --rm --cpuset-cpus=1,3 --entrypoint dotnet solarium-test \
    run -c Release --no-build --project benchmarks/Solarium.Benchmarks -- sibling-load writer-spin 2
```

`writer-spin` runs the spin of a waiting writer; `busy` runs integer work with no pause.

The compaction scenarios run a whole workload to its end, on Solarium or on RocksDB with Kommander's shard options (8 background jobs at most, level 0 triggers of 8, 28 and 44, a WAL cap of two memtables), one memtable per flush, and no compression in either engine:

```sh
dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- compaction own|shared|random solarium|rocksdb [name=value ...]
```

- `own` is Kommander's Raft log with one partition per family: 4 partitions append 4 million rows each, with 256-byte values in batches of 100, and every 100,000 rows raise their floor to keep 1 million rows, write the floor and a tombstone confined to the active memtable, and drop the files below the floor.
- `shared` puts 16 partitions on 8 families, as Kommander does past 8 partitions, so the tombstones cover the whole dead range: 1 million rows each, keeping 250,000.
- `random` writes 12 million `Put`s of uniformly random keys from 4 million, from 4 threads.

All three use 16 MiB memtables, so a run makes hundreds of flushes. Each prints the bytes of the keys and values written, the bytes flushes and compactions wrote and their ratio (the write amplification, without the WAL), the level 0 files moved without a rewrite (Solarium only), the most level 0 files a family held and the time writes stalled, and the live SSTable bytes against the rows the floors keep, at the end of the writes and after compactions settle. The `name=value` arguments change the sizes and options: `partitions`, `families`, `rows`, `batch`, `pass`, `window`, `keys`, `puts`, `threads`, `value`, `buffer` (MiB), `buffers`, `trigger`, `slowdown`, `stop`, and `compactions`, Solarium's compaction threads (by default the compactions of RocksDB's background jobs there, 6 on 16 CPUs). The bytes, moves and level 0 depths repeat from run to run; rows per second follow the disk and do not.

## The Kommander WAL against RocksDB

`benchmarks/Solarium.Kommander.Benchmarks` measures Kommander's write path on its two durable WAL backends: `RocksDbWAL` from Kommander 1.11.3, and `SolariumWAL` on the default and the io_uring environment, each with its default tuning. It drives them through Kommander's `IWAL` interface, as `RaftManager` does: each writer thread writes its own partition, with log ids that only grow.

```sh
dotnet run -c Release --project benchmarks/Solarium.Kommander.Benchmarks -- [--cases quick|full] [--only <text>] \
    [--engines rocksdb,solarium,solarium-io_uring] [--repeats 3] [--warmup 2] [--seconds 8] [--settle 2] [--dir <path>] [--csv <file>]
```

The workloads:

- `append`: one `Proposed` entry per `Write`, as a leader appends.
- `commit`: a `Proposed` entry, then its `Committed` marker in a second `Write`; `SolariumWAL` and `RocksDbWAL` both absorb the marker into the commit frontier.
- `batch`: 64 `Proposed` entries per `Write`, as a follower appends a replicated batch.
- `mixed`: an append and its commit per entry, a checkpoint and a compaction pass every 1,000 entries, and reader threads (one per 4 writers) that read the last 64 entries of a random partition, as a follower's catch-up does.

Each case runs on 1 to 64 writer threads, synced or not, with 128-byte to 4-KiB payloads. A run opens a new WAL in a new directory, warms up for 2 seconds, measures for 8, and checks that the log holds what the writers were told was written. The engines of a case take turns, in an order that rotates with the case and the repeat, and the summary takes the median of three repeats. Before each run, `--settle` runs `sync` and waits, so a run does not pay for the writeback that the run before it left. Latency is that of one operation: one `Write` for an append or a batch, the append and its commit together for the other two. It is kept in a histogram with 1.6% precision.

The allocated bytes per entry are not comparable between the engines: RocksDB's memtable is native memory, which .NET's allocation counter does not see, and Solarium's is on the managed heap, so its copy of every entry counts. In the mixed workload, the CPU and the allocations per entry include the readers'.

Unsynced writes are measured on tmpfs (`--dir /dev/shm`). On a disk, a run of them writes 100 to 200 MB/s of WAL and flushes for 10 seconds, the kernel throttles every writer while that writeback is due, and the result is the disk's: on the NVMe drive below, repeats of the same case differed up to ten times, and some runs wrote nothing for their 8 seconds. On tmpfs, the unsynced cases measure the CPU cost of each engine, its flushes included.

### Results, 2026-10-22

On a 13th Gen Intel Core i7-13620H (16 CPUs), Linux 6.1, ext4 on a Samsung NVMe drive whose fsync takes 4 to 8 ms, with the server GC. Synced cases ran on the drive, unsynced cases on tmpfs. Entries per second, the median of three repeats; synced repeats differed by 5% to 35% from the drive's fsync times, unsynced ones by 1% to 6%. The synced rows other than `mixed` come from a run before range tombstones were fragmented; those workloads write no range tombstone, so the change does not reach them.

| Case | RocksDB | Solarium | Solarium, io_uring | Solarium / RocksDB |
| --- | ---: | ---: | ---: | ---: |
| append, synced, 1 writer, 1 KiB | 213 | 205 | 204 | 0.96 |
| append, synced, 4 writers | 555 | 503 | 500 | 0.91 |
| append, synced, 16 writers | 1,763 | 1,781 | 1,757 | 1.01 |
| append, synced, 64 writers | 6,938 | 6,549 | 6,839 | 0.94 |
| append, synced, 16 writers, 128 B | 1,975 | 1,935 | 2,015 | 0.98 |
| append, synced, 16 writers, 4 KiB | 1,888 | 2,010 | 1,707 | 1.06 |
| commit, synced, 1 writer | 115 | 119 | 113 | 1.04 |
| commit, synced, 16 writers | 969 | 937 | 924 | 0.97 |
| batch of 64, synced, 1 writer | 12,600 | 14,856 | 12,584 | 1.18 |
| batch of 64, synced, 16 writers | 22,480 | 24,736 | 23,088 | 1.10 |
| mixed, synced, 16 writers | 982 | 837 | 811 | 0.85 |
| append, unsynced, 1 writer, 1 KiB | 429,534 | 464,336 | 117,302 | 1.08 |
| append, unsynced, 4 writers | 408,847 | 482,727 | 180,825 | 1.18 |
| append, unsynced, 16 writers | 304,101 | 486,631 | 279,882 | 1.60 |
| append, unsynced, 4 writers, 128 B | 749,582 | 747,611 | 212,613 | 1.00 |
| append, unsynced, 4 writers, 4 KiB | 167,539 | 236,675 | 71,890 | 1.41 |
| commit, unsynced, 4 writers | 280,744 | 279,541 | 94,083 | 1.00 |
| batch of 64, unsynced, 4 writers | 644,339 | 874,023 | 225,635 | 1.36 |
| mixed, unsynced, 4 writers | 230,070 | 226,300 | 77,345 | 0.98 |

- **Synced writes are bound by the drive.** Both engines group concurrent writers into one fsync and reach the same rates within the repeat-to-repeat spread. Solarium spent less CPU per entry (26 to 55 µs against 46 to 74 µs for appends), but CPU is not what limits a synced write.
- **Unsynced writes measure the CPU cost.** Solarium on the default environment wrote 1.0 to 1.6 times as many entries as RocksDB, and its median latency was lower. Its p99.9 latency was higher, 2 to 6 ms against RocksDB's 0.01 to 0.6 ms, in the cases that write the most bytes. The cause was not measured; garbage collection of its managed memtables and memtable switches are the likely ones.
- **io_uring is slower for unsynced writes**, 0.3 to 0.9 times RocksDB: each write is a hand-off to the reactor thread, as [storage-environments.md](storage-environments.md) measures. It gains nothing on synced writes, whose fsync does not go through the ring.
- **The mixed workloads.** Their readers read the last 64 entries of a partition beside the compaction tombstones that each pass writes into the active memtable. Unsynced, Solarium's reader made 36,400 reads per second against RocksDB's 35,600, and Solarium on io_uring 66,000; synced, Solarium made 196,000 against RocksDB's 148,000. Synced mixed writes came out at 0.85 times RocksDB's here, and at 0.89 and 1.00 in other runs; the next section shows that the readers' garbage, not the write path, sets that ratio.
- **These are the results with fragmented range tombstones.** Before them, each read scanned every tombstone of each source, and the unsynced mixed workload, which writes about 40 tombstones a second per partition, ran at 0.78 times RocksDB's writes with 8,500 reads per second. `RangeTombstoneReadBenchmarks` measures the lookups alone:

| Tombstones | `Get`, memtable | `Next`, memtable | `Get`, table | `Next`, table |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 0.20 µs | 12 ns | 0.65 µs | 88 ns |
| 128, scanned | 0.78 µs | 586 ns | 1.24 µs | 604 ns |
| 128, fragmented | 0.22 µs | 32 ns | 0.68 µs | 106 ns |
| 8,192, scanned | 37.0 µs | 36,683 ns | 37.2 µs | 32,952 ns |
| 8,192, fragmented | 0.23 µs | 48 ns | 0.68 µs | 119 ns |

- **Range deletes between reads.** `RangeTombstoneChurnBenchmarks` alternates a `DeleteRange` with a `Get` of a live key, or with the `Seek` of a new iterator, in the active memtable, from 0 to 8,192 tombstones, with 0 or 4 threads reading meanwhile; the time is per pair. When the memtable fragmented all of its tombstones at the first read after each delete, as RocksDB does, the pair cost 236 µs and allocated 185 KB at 1,024 tombstones, and 2,006 µs and 1.39 MB at 8,192 (2,751 µs with 4 readers, which each built the same fragments). With the runs that the delete builds (see [design.md](design.md)), the pair costs 4 to 9 µs and allocates 1.3 to 2.7 KB at every count, and `RangeTombstoneReadBenchmarks` reads the same within 4%. The cost moved to the deletes that build a large run: one `DeleteRange` per doubling builds the run of all the tombstones, 8.6 to 12 ms for the one that makes 16,384 in a probe, against a mean of 3.2 to 4.3 µs per delete over those 16,384.

### Tail latency and garbage collection

Each run also prints the longest operation, the time the GC paused the process and its gen 2 collections over the measured seconds, and for Solarium the memtable switches that waited for a flush (none, except a few in the unsynced batch case). `--rate` paces each writer to a number of operations per second.

Solarium's memtables are on the managed heap, so its GC works in proportion to what is written. Four unsynced writers of 1 KiB entries on tmpfs, 20 seconds measured:

| Ingest | GC pause, Solarium | p99.9, RocksDB / Solarium | Longest write, RocksDB / Solarium |
| --- | ---: | --- | --- |
| As fast as possible (about 480 MB/s) | 7.1 s (35%) | 18 µs / 2,064 µs | 40 ms / 16 ms |
| 25,000 entries/s per writer (100 MB/s) | 1.6 s (8%) | 72 µs / 72 µs | 14 ms / 89 ms |
| 5,000 entries/s per writer (20 MB/s) | 0.27 s (1.3%) | 79 µs / 82 µs | 15 ms / 12 ms |

At the rates of a busy Raft node, Solarium's tail matches RocksDB's. Past about 100 MB/s per node, gen 2 collections pause the process for up to about 90 ms, where RocksDB's longest write stays near 15 ms. Unpaced, many short collections stop every writer at once, and the p99.9 becomes their length. A larger gen 0 budget (`DOTNET_GCgen0size=0x10000000`) brought the unpaced 4-writer p99.9 from 2 ms to 20 µs, but not the total pause, which comes from the gen 2 collections.

The mixed workload's readers read as fast as they can, and every read returns new `RaftLog` objects and payloads, so the readers' garbage pauses the writers too. Solarium's readers make about 45% more reads than RocksDB's, and its process paused 3.5 s of 8 against RocksDB's 1.6 s; that is what separated the synced mixed results. With five repeats, synced mixed writes came out at 1.00 times RocksDB's; without readers, at 0.97, with 40% less CPU per entry; without compaction passes, at 0.92. A fair comparison of the mixed workload paces its readers.

## Kahuna's persistence backend against RocksDB

`benchmarks/Solarium.Kahuna.Benchmarks` measures Kahuna's two persistence backends through Kahuna's `IPersistenceBackend`, as a Kahuna node calls them: `RocksDbPersistenceBackend` from Kahuna 1.13.4 and `SolariumPersistenceBackend`, each opened through its storage provider with Kahuna's default configuration. The engines are `rocksdb` (Kahuna's defaults: direct reads and a 256 MiB block cache), `rocksdb-buffered` (the same with `RocksDbDirectReads` off, so its tables are read through the page cache, as Solarium's are), and Solarium on the default and the io_uring environment.

```sh
dotnet run -c Release --project benchmarks/Solarium.Kahuna.Benchmarks -- --dir <path on the disk> [--cases quick|full] \
    [--datasets small,large,history] [--scale 1] [--engines rocksdb,rocksdb-buffered,solarium,solarium-io_uring] \
    [--repeats 3] [--warmup 2] [--seconds 8] [--settle 2] [--drain 900] [--threads 1,8] [--mixed-readers 8] \
    [--mixed-rate 20000] [--only <text>] [--csv <file>]
```

Three datasets: `small`, 200,000 keys of 256 bytes; `large`, 2,000,000 keys of 1 KiB, about 2 GB of values; and `history`, 10,000 keys of 256 bytes written 8 times each. Keys come in groups of 100 that share a prefix. Every row gets its own random value: with one value repeated in every row, compression would shrink a store and its writes to almost nothing, which measures the codec rather than the engine.

A session loads a dataset into a new store from one writer in synced batches of 1,024 rows, as Kahuna's background writer does, in a shuffled order. It reopens the store, then waits until the process has read and written less than 1 MiB of the drive in 2 seconds (`drain after load`), so a read case does not run beside the compactions that the load left. Then it measures:

- `get`, `get-miss` and `get-batch16` (`GetKeyValues` of 16 keys), from 1 and 8 threads;
- `prefix100` (`GetKeyValueByPrefix` of one group) and `range100` (`GetKeyValueByRange`, 100 rows);
- `store` of 1, 64 and 1,024 rows per synced call, from one writer;
- `mixed`: 8 readers beside a writer paced to 20,000 rows per second in batches of 64;
- after another drain, `checkpoint` and `checkpoint-asof`, once each;
- on `history`, a `prune` sweep that keeps the newest revision of each key.

Each case warms up for 2 seconds and measures for 8. A session checks a sample of its reads against the revisions it wrote, after the reopen and at the end. Besides the latency, the CPU and the allocations, each case gives the bytes that the process read from and wrote to the drive per operation, from `/proc/self/io`, with the flushes and compactions on background threads. A read counts there only when the page cache misses. The allocated bytes are not comparable between the engines, for the reason the WAL benchmark gives.

### Results, 2026-10-24

The machine of the WAL results, on Linux 6.12, ext4, with the server GC. Medians of three repeats, in operations per second (rows for writes and scans, keys for batched reads), or seconds for the cases that run once. On `small` and `history`, every engine's store fits in the page cache.

| Case | rocksdb | rocksdb-buffered | Solarium | Solarium, io_uring | Solarium / rocksdb-buffered |
| --- | ---: | ---: | ---: | ---: | ---: |
| small, load | 50,701 | 47,149 | 42,419 | 38,858 | 0.90 |
| small, reopen | 0.66 s | 0.64 s | 0.21 s | 0.23 s | 3.05 |
| small, get, 1 thread | 305,654 | 387,057 | 371,883 | 233,793 | 0.96 |
| small, get-miss, 1 thread | 1,643,916 | 1,640,856 | 806,564 | 758,422 | 0.49 |
| small, get-batch16, 8 threads | 2,416,775 | 2,402,715 | 1,634,892 | 969,171 | 0.68 |
| small, prefix100, 8 threads | 5,064,542 | 5,124,609 | 7,089,193 | 3,069,796 | 1.38 |
| small, store b1 | 218 | 225 | 223 | 223 | 0.99 |
| small, store b1024 | 31,359 | 49,535 | 31,871 | 25,984 | 0.64 |
| small, mixed | 491,734 | 1,362,272 | 1,738,255 | 735,949 | 1.28 |
| small, checkpoint | 0.12 s | 0.55 s | 3.27 s | 3.88 s | 0.17 |
| history, get, 1 thread | 469,081 | 465,442 | 694,974 | 698,950 | 1.49 |
| history, prefix100, 1 thread | 333,518 | 330,346 | 549,921 | 559,401 | 1.66 |
| history, prune | 13,585 | 11,907 | 11,799 | 11,058 | 0.99 |
| large, load | 9,793 | 10,899 | 5,619 | 8,512 | 0.52 |
| large, drain after load | 20 s | 14 s | 40 s | 64 s | 0.35 |
| large, get, 1 thread | 8,017 | 171,679 | 268,406 | 137,961 | 1.56 |
| large, get, 8 threads | 66,161 | 756,402 | 1,022,420 | 387,743 | 1.35 |
| large, get-miss, 1 thread | 807,996 | 994,708 | 1,577,726 | 1,500,257 | 1.59 |
| large, prefix100, 1 thread | 73,512 | 441,441 | 804,971 | 342,643 | 1.82 |
| large, store b1 | 156 | 198 | 219 | 221 | 1.11 |
| large, store b64 | 3,960 | 5,888 | 6,648 | 7,040 | 1.13 |
| large, store b1024 | 7,808 | 6,528 | 5,632 | 9,600 | 0.86 |
| large, mixed | 5,343 | 639,247 | 948,607 | 305,464 | 1.48 |
| large, checkpoint | 0.50 s | 0.13 s | 41.8 s | 27.3 s | 0.003 |
| large, checkpoint-asof | 46.7 s | 42.5 s | 79.6 s | 146.5 s | 0.53 |

- **Reads from the page cache.** With its tables in the page cache, Solarium matched or beat RocksDB on gets and scans, with no block cache: 1.05 to 1.85 times `rocksdb-buffered` on `large`, 1.15 to 1.75 times on `history`, and 0.96 to 1.5 times on `small`. Batched gets on `small` came out at 0.68 to 0.82 times. Solarium lost on misses on `small` and `history`, 0.44 to 0.49 times from one thread, at about 1 µs each, probably because part of the data was still in memtables, which Solarium replays from its WAL at open where RocksDB flushes them; on `large` its misses were faster. Solarium on io_uring made 0.4 to 0.6 times the reads of the default environment on `small` and `large`, and the same on `history`. The io_uring environment has since read with `pread` on the calling thread, and its gets match the default environment's ([storage-environments.md](storage-environments.md)).
- **Kahuna's RocksDB defaults lose on reads that miss its block cache.** With direct reads, every block that misses the 256 MiB block cache comes from the drive. On `large`, `rocksdb` read about 3 KB per get and made 8,000 gets per second from one thread, against 172,000 with the page cache.
- **Synced writes of one or 64 rows are bound by the drive** and came out the same on `small` and `history`. On `large`, Solarium made 1.1 times `rocksdb-buffered`'s rows and 1.4 to 1.7 times `rocksdb`'s. Both backends read each key's current row before they write it, and `rocksdb` reads it from the drive, 2.8 to 3 KB per row.
- **Large writes are where Solarium loses,** 0.52 times on the `large` load and 0.64 to 0.88 times for batches of 1,024. The table below has the cause.
- **Checkpoints.** RocksDB hard-links its tables into a checkpoint; Solarium copied them when these results were taken. A checkpoint of `large` took 42 s against 0.1 to 0.5 s. Solarium now links them too; see "Checkpoints that link" below. `checkpoint-asof` rewrites the rows on both engines and took 1.2 to 1.9 times as long on Solarium.
- **A prune sweep is bound by the drive** on both engines, which sync one write per key.
- **Mixed reads and writes.** The writer reached its 20,000 rows per second on no engine: its synced batches of 64 rows made 8,000 to 13,600 rows per second on `small` and `history`, and on `large` 520 to 3,100 on RocksDB, 1,300 to 1,500 on Solarium, and 4,500 to 4,800 on Solarium on io_uring. The readers' gets came out highest on Solarium. Kahuna's RocksDB defaults made 5,343 gets per second on `large`, reading 17 KB from the drive per get.

Bytes written to the drive per row on `large`, from `/proc/self/io`, and the store's size after the load's compactions settle:

| | rocksdb | rocksdb-buffered | Solarium | Solarium, io_uring |
| --- | ---: | ---: | ---: | ---: |
| load, bytes written per row | 7,954 | 8,074 | 17,471 | 9,758 |
| store b1024, bytes written per row | 5,673 | 12,947 | 18,740 | 8,974 |
| store after the drain | 2.5 GB | 2.5 GB | 4.2 GB | 4.2 GB |
| the drain after the load wrote | 1.4 GB | 1.4 GB | 4.2 GB | 4.2 GB |

Two things make Solarium write more:

- **No compression** when these results were taken; see "Compression" below. Kahuna writes each value twice: in the key's current row and in its history row. The two rows sort next to each other, so RocksDB's default compression, Snappy, stores the second copy in a few bytes. Solarium stores both, so its store is 1.7 times RocksDB's.
- **Two levels.** Solarium has a level 0 and a bottom level. With keys spread over the whole key space, every compaction of level 0 rewrites the whole bottom level: the drain after the load rewrote all 4.2 GB. RocksDB's levels rewrite a part of the tree per compaction.

A load's rate follows its bytes. Solarium on io_uring wrote half the bytes per row of the default environment and loaded 1.5 times as fast, probably because each of its compactions took more level 0 tables; that was not measured.

### Larger than memory

The `large` dataset again, one repeat per engine, each in its own process under a 2 GiB memory limit (`systemd-run --user --scope -p MemoryMax=2G -p MemorySwapMax=0`), which bounds the page cache to less than any store:

| Case | rocksdb | rocksdb-buffered | Solarium | Solarium, io_uring |
| --- | ---: | ---: | ---: | ---: |
| load | 9,295 | 10,301 | 6,028 | 7,882 |
| get, 1 thread | 7,930 | 11,700 | 10,024 | 7,882 |
| get, 8 threads | 64,812 | 96,029 | 70,771 | 74,936 |
| get-miss, 1 thread | 803,686 | 957,217 | 1,506,422 | 1,368,489 |
| prefix100, 8 threads | 217,210 | 386,672 | 331,072 | 318,648 |
| store b1024 | 8,320 | 8,064 | 7,408 | 6,272 |
| mixed, gets | 5,356 | 16,773 | 11,563 | 21,986 |
| mixed, writer rows per second | 816 | 2,088 | 1,208 | 2,432 |
| checkpoint | 1.6 s | 0.4 s | 53.6 s | 50.4 s |

Every engine's gets were bound by the drive, 100 to 140 µs each. Solarium read 6.5 KB from the drive per get, against 3 to 3.8 KB for RocksDB, and made 0.74 to 0.86 times `rocksdb-buffered`'s gets. Its misses were the fastest, because its Bloom filters stay in memory. Its checkpoint, which then copied its tables, copied the 4.4 GB store and read all of it from the drive.

### Checkpoints that link

`CreateCheckpoint` now hard-links the tables, as RocksDB does. The checkpoint cases of `large` again, after the load and its drain, with no read or write case before them (`--only checkpoint`), three repeats:

| Case | rocksdb | Solarium | Solarium, io_uring |
| --- | ---: | ---: | ---: |
| checkpoint | 0.03 s | 0.10 s | 0.12 s |
| checkpoint, bytes written | 0 | 7 MiB | 7 MiB |
| checkpoint-asof | 19.8 s | 41.1 s | 63.2 s |
| checkpoint-asof, bytes written | 1.4 GB | 4.2 GB | 4.2 GB |

A plain checkpoint now writes only the memtables. `checkpoint-asof` trims the copy and compacts it, which rewrites the whole copy on Solarium's two levels; that is the compaction above, not the checkpoint.

### Compression

Solarium's Kahuna backend now compresses the data blocks of `kv` and `locks` with LZ4, as RocksDB compresses them with Snappy. The `large` cases again, three repeats in the page cache, against the uncompressed results above:

| Case | rocksdb | rocksdb-buffered | Solarium, uncompressed | Solarium, LZ4 | Solarium, io_uring, uncompressed | Solarium, io_uring, LZ4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 9,350 | 9,927 | 5,619 | 7,535 | 8,512 | 12,314 |
| load, bytes written per row | 7,952 | 8,026 | 17,471 | 10,929 | 9,758 | 6,556 |
| store after the drain | 2.5 GB | 2.5 GB | 4.2 GB | 2.5 GB | 4.2 GB | 2.4 GB |
| the drain after the load wrote | 1.4 GB | 1.4 GB | 4.2 GB | 2.4 GB | 4.2 GB | 2.4 GB |
| drain after load | 18 s | 16 s | 40 s | 36 s | 64 s | 54 s |
| get, 1 thread | 8,149 | 167,607 | 268,406 | 279,489 | 137,961 | 142,465 |
| get, 8 threads | 66,068 | 754,977 | 1,022,420 | 1,224,360 | 387,743 | 539,924 |
| prefix100, 1 thread | 73,948 | 446,591 | 804,971 | 794,833 | 342,643 | 357,609 |
| store b1024 | 7,936 | 5,504 | 5,632 | 8,576 | 9,600 | 12,288 |
| store b1024, bytes written per row | 5,572 | 16,287 | 18,740 | 9,020 | 8,974 | 6,325 |
| mixed | 5,390 | 639,426 | 948,607 | 1,061,012 | 305,464 | 527,494 |
| checkpoint-asof | 44.0 s | 40.5 s | 79.6 s | 42.5 s | 146.5 s | 55.0 s |
| checkpoint-asof, bytes written | 2.9 GB | 2.9 GB | 8.7 GB | 2.9 GB | 13.2 GB | 2.9 GB |

The uncompressed `checkpoint-asof` ran before checkpoints linked; the rest of its time is the compaction of the copy, which now rewrites 2.5 GB instead of 4.2 GB.

And under the 2 GiB memory limit of "Larger than memory", one repeat:

| Case | rocksdb | rocksdb-buffered | Solarium, uncompressed | Solarium, LZ4 | Solarium, io_uring, uncompressed | Solarium, io_uring, LZ4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 9,295 | 10,301 | 6,028 | 10,300 | 7,882 | 10,897 |
| get, 1 thread | 7,930 | 11,700 | 10,024 | 12,550 | 7,882 | 10,187 |
| get, 8 threads | 64,812 | 96,029 | 70,771 | 122,944 | 74,936 | 126,075 |
| get-miss, 1 thread | 803,686 | 957,217 | 1,506,422 | 1,526,141 | 1,368,489 | 1,478,586 |
| prefix100, 8 threads | 217,210 | 386,672 | 331,072 | 686,232 | 318,648 | 415,072 |
| store b1024 | 8,320 | 8,064 | 7,408 | 6,784 | 6,272 | 8,448 |
| mixed, gets | 5,356 | 16,773 | 11,563 | 34,482 | 21,986 | 34,316 |
| mixed, writer rows per second | 816 | 2,088 | 1,208 | 2,795 | 2,432 | 3,208 |

- **The store is RocksDB's size.** The current row and the history row of a key hold the same value next to each other, and LZ4 stores the second copy in a few bytes, as Snappy does. The store settled at 2.4 to 2.5 GB on every engine.
- **Fewer bytes, faster writes.** A load wrote 0.6 to 0.7 times the bytes per row and ran 1.3 to 1.7 times as fast, as fast as RocksDB under the memory limit. Solarium still writes more than RocksDB on the default environment, because each compaction of level 0 still rewrites the whole bottom level: the drain after the load rewrote all 2.4 GB, against RocksDB's 1.4 GB.
- **Decompression costs no measured read time.** Without a block cache, every read of a compressed block decompresses it. In the page cache, gets and scans came out the same or faster, at the same CPU per get, 3.6 µs from one thread. Under the memory limit, a get read 4.2 KB from the drive instead of 6.5 KB, against 3 to 3.8 KB for RocksDB. Gets made 0.87 to 1.3 times `rocksdb-buffered`'s, the lowest from one thread on io_uring, and scans 1.1 to 1.8 times.
- `small` and `history` live in the memtables and in a few tables, and came out the same as without compression, within the noise of the repeats.

### Leveled compaction

Solarium now compacts into RocksDB's seven levels, sized as RocksDB's dynamic level sizing does, with one compaction at a time. The compaction scenarios, once each, on the same machine as the version with two levels (`shared` and `random` again on the commit before), with the data directory on the drive:

| | Solarium, two levels | Solarium, levels | RocksDB |
| --- | ---: | ---: | ---: |
| `own`, compaction bytes written | 0 | 0 | 0 |
| `own`, files moved without a rewrite | 289 | 289 | - |
| `shared`, write amplification | 2.17 | 2.19 | - |
| `random`, write amplification | 4.70 | 4.18 | 4.62 |
| `random`, rows per second | 60,356 | 65,552 | 49,437 |
| `random`, most level 0 files | 39 | 33 | 17 |
| `random`, settled live SSTables against the rows kept | 1.08x | 1.35x | 1.27x |

The `large` Kahuna cases, three repeats of the Solarium engines, against the compressed results above (two levels) and the RocksDB engines of that run:

| Case | rocksdb | rocksdb-buffered | Solarium, two levels | Solarium, levels | Solarium, io_uring, two levels | Solarium, io_uring, levels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 9,350 | 9,927 | 7,535 | 9,083 | 12,314 | 13,129 |
| load, bytes written per row | 7,952 | 8,026 | 10,929 | 9,446 | 6,556 | 6,152 |
| drain after load | 18 s | 16 s | 36 s | 6 s | 54 s | 92 s |
| get, 1 thread | 8,149 | 167,607 | 279,489 | 253,484 | 142,465 | 139,818 |
| get, 8 threads | 66,068 | 754,977 | 1,224,360 | 953,106 | 539,924 | 492,566 |
| get-miss, 1 thread | 815,254 | 997,642 | 1,627,186 | 1,124,245 | 1,535,399 | 1,372,380 |
| prefix100, 1 thread | 73,948 | 446,591 | 794,833 | 596,825 | 357,609 | 328,535 |
| store b64 | 3,824 | 7,584 | 5,288 | 3,080 | 6,096 | 6,880 |
| store b64, bytes written per row | 3,571 | 3,593 | 3,284 | 23,745 | 3,571 | 3,760 |
| store b1024 | 7,936 | 5,504 | 8,576 | 7,936 | 12,288 | 10,624 |
| checkpoint-asof | 44.0 s | 40.5 s | 42.5 s | 53.2 s | 55.0 s | 61.7 s |

- **Random keys write fewer bytes.** A merge of level 0 rewrites the base level and the merges below take one file each, so the `random` scenario wrote 4.18 times its bytes against 4.70 with two levels, below RocksDB's 4.62, and the `large` load wrote 0.86 to 0.94 times the bytes per row and ran 1.07 to 1.2 times as fast. The store holds more dead versions in the levels above the bottom: 1.35 times the rows kept after the `random` scenario settled, against 1.08 with everything in one level and 1.27 for RocksDB.
- **Appended keys still write nothing.** Kommander's `own` layout moved all 289 flushed files down without a rewrite.
- **The drain is shorter, and the work moves later.** A compaction starts only at a score of 1, so the drain after the load ended with up to three files in level 0 and a full base level, after 0 to 10 seconds against 36. The next flushes, during `store b64` and `store b1024`, then set off a merge of level 0 into level 5 and merges of level 5 into level 6, at 23 to 30 KB written per row, which slowed `store b64` to 0.58 times. On io_uring, whose load is faster, the load left more behind, and its drain wrote 1.5 to 4.9 GB in 34 to 108 seconds. RocksDB runs up to 8 compactions at once there; Solarium runs one.
- **Reads search one more level.** With data in levels 5 and 6, a get searches one more sorted level: 0.78 to 0.98 times the gets of two levels, 0.69 to 0.89 times the misses, and 0.75 to 0.92 times the scans. On the default environment they still beat `rocksdb-buffered`'s, 1.1 to 1.5 times; on io_uring, gets from one thread came out at 0.83 times.
- **`checkpoint-asof` takes longer.** Its `CompactRange` now merges level 0 into the base level and then the base level into level 6, which wrote 3.1 to 3.6 GB against 2.9 GB in one merge.

### Concurrent compactions

`MaxBackgroundCompactions` runs several compactions at once. The compaction scenarios, once each, with Solarium's compaction threads set by `compactions=`; RocksDB ran its 8 background jobs, 6 of them compactions:

| | 1 thread | 2 threads | 6 threads | RocksDB |
| --- | ---: | ---: | ---: | ---: |
| `random`, write amplification | 4.28 | 5.00 | 5.06 | 4.62 |
| `random`, rows per second | 73,422 | 53,014 | 55,294 | 49,437 |
| `random`, most level 0 files | 32 | 29 | 22 | 17 |
| `random`, writes stalled | 3.5 s | 0.7 s | 0 s | 131.8 s |
| `own`, rows per second | 121,049 | - | 146,489 | 121,008 |
| `own`, compaction bytes written | 0 | - | 0 | 0 |
| `shared`, most level 0 files | 11 | - | 8 | - |
| `shared`, write amplification | 2.19 | - | 2.15 | - |

The `large` Kahuna cases, three repeats, with the compaction threads of `Solarium.Kahuna` at 1, 2 and 8 (`max(2, CPUs / 2)`, the RocksDB backend's) on the default environment, and at 1 and 8 on io_uring. `Solarium.Kahuna` keeps one, for the synced writes below:

| Case | 1 thread | 2 threads | 8 threads | io_uring, 1 thread | io_uring, 8 threads |
| --- | ---: | ---: | ---: | ---: | ---: |
| load | 9,083 | 8,555 | 8,753 | 13,129 | 8,924 |
| load, bytes written per row | 9,446 | 9,908 | 10,261 | 6,152 | 7,872 |
| drain after load | 6 s | 0 s | 6 s | 92 s | 26 s |
| get, 8 threads | 953,106 | 1,024,599 | 1,136,924 | 492,566 | 550,232 |
| store b64 | 3,080 | 2,840 | 2,528 | 6,880 | 5,520 |
| store b1024 | 7,936 | 2,432 | 2,816 | 10,624 | 10,624 |
| store b1024, bytes written per row | 13,316 | 30,994 | 28,880 | 6,915 | 6,020 |
| checkpoint-asof | 53.2 s | 45.2 s | 47.5 s | 61.7 s | 70.8 s |

- **Level 0 stays lower and writes stop stalling.** With 6 threads, the `random` scenario's level 0 peaked at 22 files against 32, under the slowdown trigger, and no write waited. Kommander's `own` layout, whose compactions are moves, ran 1.2 times as fast, still with no compaction byte written. On io_uring, the drain after the `large` load took 26 seconds against 92.
- **More threads write more bytes.** With one thread, level 0's score always wins, so the merges of the sorted levels wait, and each merge of level 0 takes many files at once over the base level. With two threads or more, the merges below the base level run as soon as their level passes its target, each rewriting about ten times its size from level 6, as in RocksDB. The `random` scenario wrote 5.0 times its bytes against 4.28, close to RocksDB's 4.62, and ran at 0.72 to 0.75 times the rows per second of one thread, still above RocksDB's.
- **Synced writes slow down while the levels catch up.** In the `large` cases, the merges the load left behind run during `store b64` and `store b1024`, 22 to 31 KB written per row, and synced batches of 1,024 rows came out at 0.31 to 0.35 times one thread's on the default environment. Two threads did the same as eight: what slows the writes is that the sorted levels compact at all while they run. RocksDB, with 8 compactions, kept `store b1024` at 7,936 rows per second there. Its scores for dynamic level sizing put level 0 first by size as well as by file count, and discount a level by the bytes about to come into it, which Solarium's do not (see `rocksdb-differences.md`).

### Synced writes beside background writes

`Solarium.Benchmarks synced [compactions=N] [rows=N] [seconds=N] [dir=path]` loads random keys with 1 KiB values, then writes synced batches of 1,024 rows over them, and prints each second the rows written, the memtable switches that waited for a flush, the write stalls, and the time in the WAL append with its sync. With 2 million rows, one compaction thread and then eight, it showed no memtable wait and no stall: the time went to the append. With no flush or compaction running, the batches made about 29,000 rows per second, at about 500 ms of append time per second. During a flush they made about 8,000; during one compaction, 4,000 to 10,000; during three or four compactions, 700 to 3,000.

The drive of these results, a Samsung PM9B1 on ext4, commits synced writes at about 100 MB/s: a 2.5 MB append and its sync took 24 ms at the median. With one other thread that writes 64 MB files and syncs each at its end, the median sync took 154 ms and the 99th percentile 839 ms; with four such threads, 1,200 and 1,708 ms. A `sync_file_range` every 1 MB of those files, RocksDB's `bytes_per_sync`, brought four threads to 573 and 970 ms. So on this drive, the synced writes are bound by its bandwidth, and every byte that flushes and compactions write comes out of their share. More compaction threads do not add much work (the `random` scenario wrote 5.06 times its bytes against 4.28), but they do it at once, right after the flushes, where the synced writes need the drive. RocksDB's scores for dynamic level sizing, tried in Solarium, changed nothing there: with several threads, the order of the compactions does not change that they run.

### Paced background writes

`DbOptions.RateLimiter` paces flush and compaction writes, and `DbOptions.BytesPerSync` starts the writeback of each table as it is written. The synced batches of `Solarium.Benchmarks synced`, 60 seconds after a load of 2 million rows, with the limiter out of the way during the load (`rate=` sets it before the batches):

| Compaction threads, pacing | Synced rows per second |
| --- | ---: |
| 1, none | 13,153 |
| 1, `BytesPerSync` 1 MiB | 13,286 |
| 8, none | 10,927 |
| 8, `BytesPerSync` 1 MiB | 11,501 |
| 8, limiter of 25 MiB/s | 13,341 |
| 8, limiter of 50 MiB/s | 13,210 |
| 8, limiter of 50 MiB/s, auto-tuned | 3,941 |

The `large` Kahuna cases, three repeats, with `Solarium.Kahuna` set to 8 compaction threads and a limiter of 50 MiB/s, against one thread and eight without a limiter, and against the RocksDB engines of the compressed results:

| Case | 1 thread | 8 threads | 8 threads, 50 MiB/s | rocksdb | rocksdb-buffered |
| --- | ---: | ---: | ---: | ---: | ---: |
| load | 9,083 | 8,753 | 10,800 | 9,350 | 9,927 |
| load, bytes written per row | 9,446 | 10,261 | 6,658 | 7,952 | 8,026 |
| drain after load | 6 s | 6 s | 78 s | 18 s | 16 s |
| get, 8 threads | 953,106 | 1,136,924 | 1,131,218 | 66,068 | 754,977 |
| get-miss, 1 thread | 1,124,245 | 1,157,447 | 1,478,010 | 815,254 | 997,642 |
| store b64 | 3,080 | 2,528 | 7,576 | 3,824 | 7,584 |
| store b1024 | 7,936 | 2,816 | 8,064 | 7,936 | 5,504 |
| mixed | 996,438 | 926,805 | 944,142 | 5,390 | 639,426 |
| checkpoint-asof | 53.2 s | 47.5 s | 45.5 s | 44.0 s | 40.5 s |

- **A fixed rate gives the synced writes their drive back.** With 8 threads and a limiter of 25 or 50 MiB/s, the synced batches came back to the rate of one thread, and in the `large` cases `store b64` ran 2.5 to 3 times as fast as one thread or eight unpaced, as fast as `rocksdb-buffered`, and `store b1024` matched RocksDB's. The load ran 1.2 times as fast as one thread and wrote 0.7 times the bytes per row while it ran, faster than RocksDB.
- **The work moves to the drain.** At 50 MiB/s, the merges that the load left behind took 70 to 82 seconds after it, against 6 for one thread and 18 for RocksDB, which paces nothing.
- **`BytesPerSync` alone helps a little**, 5% with 8 threads.
- **Auto-tuning collapsed here.** A tune that finds no write waiting drops an auto-tuned limiter to a twentieth of its maximum, as RocksDB's, and it climbs back 5% every 100 refill periods, 10 seconds at the default period: after the drain, the rate fell to about 3 MiB/s, level 0 grew past the slowdown trigger, and the synced batches waited for it. An auto-tuned limiter fits a high maximum that only smooths bursts; it does not protect the synced writes.
- **The rate is the drive's.** 25 to 50 MiB/s is about a third to a half of what this drive commits in synced writes; a faster drive needs a higher rate, and too low a rate lets level 0 grow until writes stall.

The measurements answer what Solarium needs before it can replace RocksDB under Kahuna. Its reads need no block cache, its store is as small as RocksDB's, its compactions no longer rewrite the whole bottom level, they can run at once, and a rate limiter keeps them from taking the drive from the synced writes. What is left is choosing the rate for a drive, which RocksDB leaves to its host too.

## Index and filter blocks in a cache

A scratch program measured `Get` and a full scan on fully compacted stores of Kahuna's `small` shape (1,000,000 rows, 24-byte keys, 256-byte values) and its `large` shape (500,000 rows, 1 KB values), with and without LZ4, on tmpfs, so every read came from memory as from the page cache. Each read case ran 5 seconds of `Get` on random written keys from 1 and from 16 threads, on an i7-13620H (16 threads), on 2026-10-26.

The heap that opening the store adds, as a share of the tables' bytes, before and after the index became one array of keys instead of one array per key, measured as the heap growth from opening each store after a full compaction:

| Shape | Before | After |
| --- | --- | --- |
| Kommander, 31-byte keys, 256-byte values | 2.34%, 4.70% with LZ4 | 1.69%, 3.40% |
| Kahuna `small` | 2.22%, 4.29% | 1.55%, 3.01% |
| Kahuna `large` | 1.88%, 3.64% | 1.23%, 2.38% |

`Get`, in thousands per second (resident: before the change, then after it; a cache that holds every index and filter, pinned at level 0; a cache of a quarter of them):

| Shape | Threads | Resident, before | Resident | Cache holds all | Cache of a quarter |
| --- | --- | --- | --- | --- | --- |
| `small` | 1 | 521 | 543 | 543 | 1.4 |
| `small` | 16 | 5,200 | 5,368 | 4,983 | 5.8 |
| `small` LZ4 | 1 | 464 | 463 | 471 | 0.6 |
| `small` LZ4 | 16 | 4,759 | 4,788 | 4,310 | 2.7 |
| `large` | 1 | 504 | 505 | 500 | 1.8 |
| `large` | 16 | 2,820 | 2,757 | 3,634 | 3.2 |
| `large` LZ4 | 1 | 469 | 466 | 472 | 0.9 |
| `large` LZ4 | 16 | 4,189 | 4,304 | 4,045 | 3.3 |

- The compact index changed no read: the resident columns are within the noise of each other.
- A cache that holds everything costs nothing on one thread, and 6% to 10% from 16 threads on three of the shapes, where each lookup takes a shard's lock. The `large` case without compression ran low resident from 16 threads in both builds, so its gain is noise.
- A cache smaller than the indexes and filters it serves thrashed: a lookup on a table whose entry was not cached read and decoded the table's whole index and filter, about 1 MB for a 64 MB table, and the entry was larger than a shard, so it was evicted as it went in. Lookups ran 300 to 700 times slower, with hundreds of gen 2 collections. A scan was the same in every case: an iterator keeps each table's index.

The same runs after the index and filter became partitions of about 4 KB under a top-level index (table version 4), on the same day, with the stores written again:

| Shape | Threads | Resident | Cache holds all | Cache of a quarter |
| --- | --- | --- | --- | --- |
| `small` | 1 | 540 | 528 | 212 |
| `small` | 16 | 5,127 | 4,921 | 244 |
| `small` LZ4 | 1 | 473 | 460 | 208 |
| `small` LZ4 | 16 | 4,830 | 4,478 | 263 |
| `large` | 1 | 541 | 503 | 218 |
| `large` | 16 | 3,185 | 3,404 | 313 |
| `large` LZ4 | 1 | 489 | 464 | 213 |
| `large` LZ4 | 16 | 4,394 | 4,068 | 289 |

- A miss now reads one partition, a few kilobytes, so a cache of a quarter of the partitions made lookups from one thread about 2.5 times slower than a cache that holds them all, where it made them hundreds of times slower before. From 16 threads it was 11 to 20 times slower: the misses allocate their decoded partitions, about 2 seconds of the 5 were garbage collection pauses, and a cache of 1 MB has two shards to share.
- A cache that holds every partition reads 2% to 5% slower than resident ones from one thread, and up to 7% from 16, within the noise of the `large` case.
- The heap that opening adds is the same as before partitions within the noise (4.43 MB for `small` against 4.19 MB), and a reader whose partitions are cached keeps 0.04% of its table's bytes: the top-level index.

## Data blocks in a cache

`ColumnFamilyOptions.CacheDataBlocks` puts the data blocks that reads take into the block cache, verified and decompressed, as RocksDB does with any block cache. The Kahuna cases (`Solarium.Kahuna.Benchmarks --only " t"`), with Kahuna's 256 MiB shared cache and LZ4 tables, compared two builds on 2026-10-27: the commit before, and the same with every family of the store caching its data blocks. `small` (200,000 keys of 256 bytes) fits in the cache; `large` (2,000,000 keys of 1 KiB) is about ten times it. Medians of three sessions on `small` and two on `large`, in operations per second (keys for batched gets), as a ratio to the build without data blocks, for the default environment and io_uring:

| Case | `small` | `large` |
| --- | ---: | ---: |
| get, 1 thread | 1.16, 1.14 | 0.76, 0.76 |
| get, 8 threads | 0.61, 0.60 | 0.68, 0.81 |
| get-batch16, 1 thread | 1.12, 1.14 | 0.77, 0.78 |
| get-batch16, 8 threads | 0.69, 0.52 | 0.74, 0.79 |
| get-miss, 1 thread | 1.20, 1.19 | 0.97, 0.98 |
| prefix100, 1 thread | 1.13, 1.11 | 0.76, 0.72 |
| prefix100, 8 threads | 0.99, 0.75 | 0.57, 0.49 |
| range100, 1 thread | 1.13, 1.10 | 0.73, 0.70 |
| range100, 8 threads | 0.93, 0.87 | 0.59, 0.58 |
| mixed, 8 readers and a writer | 1.00, 1.07 | 0.61, 0.63 |

- **From one thread, on a store that fits, it pays:** 11% to 20% more reads, the system call, the checksum and the LZ4 decompression of each read saved.
- **From 8 threads it loses, even with every block cached.** On `small`, the CPU per get went from 3.9 to 6.4 µs, and gets scaled 2.8 times from 1 to 8 threads against 5.3 times without data blocks. Each get takes two shard locks instead of one, its partition's and its block's, and moves each entry in its shard's LRU lists. Lock contention, and the cache lines of the shards moving between cores, are the likely cause; it was not profiled.
- **On a store larger than the cache, it loses on every case,** from 0.49 to 0.81 times. Most reads miss, and each miss copies its block, about 4 KiB, into an array that lives until the cache evicts it: the allocations per get went from 2.4 to 6.6 KB, the GC pauses rose about three times, and the gen 2 collections from 0 to 2 per case to 5 to 42.

So `CacheDataBlocks` is off by default, and neither `SolariumWAL` nor `Solarium.Kahuna` sets it. A cache whose lookups take no lock and whose blocks live in reused memory, as RocksDB's `HyperClockCache`, would remove both costs.
