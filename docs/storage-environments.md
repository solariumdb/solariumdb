# Storage environments

All file I/O goes through a storage environment, as in RocksDB's `Env`. The default environment works on every platform; the `Solarium.IoUring` package adds an io_uring environment for Linux, on the [ioxide](https://github.com/MDA2AV/ioxide) runtime.

```csharp
using Solarium.IoUring;

using IoUringStorageEnvironment environment = new();   // dispose after every database that uses it
using SolariumDb db = SolariumDb.Open("./data", new DbOptions { Environment = environment });
```

| Environment | I/O | Flush |
| --- | --- | --- |
| `DefaultStorageEnvironment` (all platforms) | `RandomAccess` (pread / pwrite) | `RandomAccess.FlushToDisk` |
| `IoUringStorageEnvironment` (Linux) | writes: `IORING_OP_WRITE` on one ioxide reactor thread; reads: pread on the calling thread; batch reads: `preadv2(RWF_NOWAIT)` on the calling thread, then `IORING_OP_READ` for what the page cache misses | `fdatasync` on the calling thread |

ioxide has no fsync operation in its API, so the io_uring environment flushes with `fdatasync` on the calling thread, not through the ring. Every read and write is a hand-off to the reactor thread, and that hand-off costs more than the system call it replaces. Measured on Linux 6.1 with ext4 on an NVMe drive:

- A 4 KB read from the page cache takes 2.6 µs instead of 0.4 µs. The kernel completes it inline, so the extra cost is the hand-off alone, about 2.3 µs.
- A 4 KB write takes 6.9 µs instead of 1.5 µs, and a 64 KB write 29 µs instead of 20 µs. On this kernel, io_uring cannot complete a buffered ext4 write inline and passes it to one of its worker threads (`iou-wrk`). The extra cost is the hand-off plus that second hop, and it grows with the size of the write.
- An unsynced `Put` from one thread takes 6.9 µs instead of 3.0 µs.
- A synced `Put` costs the same in both. The device flush takes milliseconds, and neither environment sends it through the ring.

One operation at a time, as the engine issues them, io_uring is never faster. It can only win when many operations are in flight at once.

So the io_uring environment reads with `pread` on the calling thread. A table read is one blocking read, usually from the page cache, and through the ring the reads of every thread ran one at a time on the reactor thread. On Kahuna's `large` dataset (2,000,000 rows of 1 KiB, `Solarium.Kahuna.Benchmarks`, Linux 6.12, ext4 on NVMe, 2026-10-26, one repeat), before and after:

| Case | default | io_uring, reads on the ring | io_uring, reads with pread |
| --- | ---: | ---: | ---: |
| get, 1 thread | 278,806 | 142,851 | 289,846 |
| get, 8 threads | 1,249,720 | 504,846 | 1,177,798 |
| get-batch16, 8 threads | 1,174,790 | 515,716 | 1,276,174 |
| drain after load | 12 s | 26 s | 14 s |

The drain is the compactions that the load left behind, whose input reads went through the ring too. A thread that writes reuses one completion and its event for every write, since it has at most one write in flight. The cost of reads that no longer wait for the reactor: where readers keep every CPU busy, a write waits for the reactor thread to get one. In a test with six readers in a tight loop beside one writer on four CPUs, the writer flushed 151 to 166 tables in 3 seconds on io_uring, against 390 with reads on the ring and 700 to 970 on the default environment.

`MultiRead` reads a batch of requests, across files, for `MultiGet`. The default environment reads them one after another. The io_uring environment first reads what the page cache holds on the calling thread, with `preadv2(RWF_NOWAIT)`, which fails with `EAGAIN` rather than wait for the drive, and sends the rest to the ring together, then waits for all of them: the drive serves them at once, where `pread` would wait for each in turn. Reads that the page cache holds stay off the ring, where they would run one at a time on the reactor thread. A file system without `RWF_NOWAIT`, such as tmpfs, is read with `pread`. Cold random reads of 4 KiB from the NVMe drive above, in batches of 16, from one thread: 8,300 to 10,200 per second with `pread`, 60,000 to 65,600 through the ring; from 8 threads, 133,000 to 135,000 against 147,000 to 153,000, where the drive is near its limit either way.

`MultiGet` reads through it, on Kahuna's `large` dataset (`get-batch16`, `GetKeyValues` of 16 keys, keys per second, 2026-10-26). Under the 2 GiB memory limit of [benchmarks.md](benchmarks.md), where blocks come from the drive, one repeat:

| Case | default, before | default, after | io_uring, before | io_uring, after |
| --- | ---: | ---: | ---: | ---: |
| 1 thread | 40,757 | 41,529 | 43,503 | 104,353 |
| 8 threads | 271,074 | 277,956 | 277,763 | 450,378 |

In the page cache, the medians of three repeats:

| Case | default, before | default, after | io_uring, before | io_uring, after |
| --- | ---: | ---: | ---: | ---: |
| 1 thread | 285,442 | 285,456 | 290,372 | 276,094 |
| 8 threads | 1,191,326 | 1,228,266 | 1,221,028 | 1,186,966 |

Before, `MultiGet` read each key's block with its own `pread`. From the drive, io_uring now reads 2.4 times as many keys from one thread and 1.6 times from eight; the p99 of a call from one thread fell from 1,065 to 397 µs. In the page cache, io_uring pays 3% to 5% for the `preadv2` and the batch, and the default environment is unchanged.

A write that the ring completes with `ECANCELED` is done again with `pwrite` on the calling thread, and counted in `IoUringStorageEnvironment.CanceledOperations`. The kernel reports `ECANCELED` for io-wq work that never ran; one path on 6.1 is an io-wq that cannot start the first worker for a kind of work, which fails what it queued. It was seen once, on the first write of an `Open`, in a full parallel test run; a load of 205,000 new reactors writing new files did not repeat it, so the exact cause is not established. A retry on the calling thread is safe whatever the cause, because a positional write done again at the same offset leaves the same bytes, and it does not depend on the reactor or io-wq. Without it, the cancelled write failed that `Open`, and a cancelled WAL write would stop a database's writes until it is opened again.

Every file of a database goes through its environment, the directory operations included, as in RocksDB's `FileSystem`: `FileExists`, `GetChildren`, `DeleteFile`, `RenameFile`, `TryLinkFile` (a hard link, which a checkpoint makes of each table), `CreateDirectory` and `FlushDirectory` (an fsync of the directory). A file adds `SyncRange`, which starts the writeback of a range without waiting for it: `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux, on the calling thread on io_uring too, since the reactor has no such operation; nothing elsewhere. They have implementations on .NET's file APIs, and on libc or Win32 for the link and the directory fsync, which both environments use; a test environment overrides them to record or drop what a crash would lose. Only the `LOCK` file, a process lock, is opened directly.
