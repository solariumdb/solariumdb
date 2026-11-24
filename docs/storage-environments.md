# Storage environments

All file I/O goes through a storage environment, as in RocksDB's `Env`. The default environment works on all platforms. The `Solarium.IoUring` package adds an io_uring environment for Linux, on the [ioxide](https://github.com/MDA2AV/ioxide) runtime.

```csharp
using Solarium.IoUring;

using IoUringStorageEnvironment environment = new();   // dispose after every database that uses it
using SolariumDb db = SolariumDb.Open("./data", new DbOptions { Environment = environment });
```

| | `DefaultStorageEnvironment` | `IoUringStorageEnvironment` |
| --- | --- | --- |
| Platforms | All | Linux |
| Writes | `pwrite` | `IORING_OP_WRITE` on one ioxide reactor thread |
| Reads | `pread` | `pread` on the calling thread |
| Batch reads (`MultiRead`) | `pread`, one request after another | `preadv2(RWF_NOWAIT)` on the calling thread, then `IORING_OP_READ` for page cache misses |
| Flush | `fdatasync` on Linux, `F_FULLFSYNC` on macOS, `FlushFileBuffers` on Windows | `fdatasync` on the calling thread |

## Which one to choose

Choose io_uring when `MultiGet` calls often miss the page cache. Its batched reads then reach the drive together. For other workloads, the default environment is usually as fast or faster.

Each io_uring write is a hand-off to the reactor thread, and the hand-off costs more than the system call it replaces. These are measurements on Linux 6.1 with ext4 on an NVMe drive.

| Operation | Default | io_uring |
| --- | ---: | ---: |
| 4 KB write | 1.5 µs | 6.9 µs |
| 64 KB write | 20 µs | 29 µs |
| Unsynced `Put`, one thread | 3.0 µs | 6.9 µs |

A synced `Put` costs the same in both, because the device flush takes milliseconds and does not go through the ring. When readers use all CPUs, io_uring writes also wait for the reactor thread to get a CPU. With six readers and one writer on four CPUs, the writer flushed 151 to 166 tables in 3 seconds on io_uring and 700 to 970 on the default environment.

## Batch reads

`MultiGet` reads its blocks with `MultiRead`, across files. The io_uring environment first reads what the page cache has, with `preadv2(RWF_NOWAIT)`. It sends the misses to the ring together, and the drive serves them at once. A file system that does not support `RWF_NOWAIT`, such as tmpfs, is read with `pread`.

These are the keys per second of `get-batch16` (16 keys per call) on Kahuna's `large` dataset, 2026-10-26. The drive rows used the 2 GiB memory limit described in [benchmarks.md](benchmarks.md), with one repeat. The page cache rows are medians of three repeats.

| Case | Default | io_uring |
| --- | ---: | ---: |
| From the drive, 1 thread | 41,529 | 104,353 |
| From the drive, 8 threads | 277,956 | 450,378 |
| Page cache, 1 thread | 285,456 | 276,094 |
| Page cache, 8 threads | 1,228,266 | 1,186,966 |

From the drive with one thread, the p99 of a call fell from 1,065 to 397 µs when `MultiGet` moved to `MultiRead`. In the page cache, io_uring pays 3% to 5% for the `preadv2` and the batch.

## Implementation notes

- Both environments flush with `fdatasync` on Linux, as RocksDB does by default. It makes the data and the length durable and skips the journal commit for a changed timestamp. The io_uring environment flushes on the calling thread, since ioxide does not offer an fsync operation.
- Single reads use `pread` because they block one thread and usually hit the page cache. Through the ring, the reads of all threads were serialized on the reactor thread, and `get` from 8 threads was less than half as fast.
- If the ring completes a write with `ECANCELED`, the calling thread repeats it with `pwrite`. `IoUringStorageEnvironment.CanceledOperations` counts these writes. A positional write at the same offset is safe to repeat. The kernel uses `ECANCELED` for io-wq work that did not run. It was seen once in a test run, and its cause is not established. Without the retry, a canceled WAL write would stop a database's writes until it is reopened.
- `SyncRange` starts the writeback of a range and does not wait for it. It calls `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux, on the calling thread. On other platforms it is a no-op.
- Directory operations also go through the environment, for example `RenameFile`, `TryLinkFile` and `FlushDirectory`. Both environments share their implementations. A test environment can override them to record or drop what a crash would lose. Only the `LOCK` file, a process lock, is opened directly.
