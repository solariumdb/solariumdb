# Differences from RocksDB

RocksDB is the reference behavior. A deliberate difference is listed on this page, and an accidental one is a bug. The comparison is with RocksDB 11.8.1, which is also the native library of the `Kommander.RocksDB` package that the benchmarks use (11.8.1.9002).

## Files and formats

- **Manifest.** It is rewritten completely on each change. RocksDB appends edits to a log.
- **Data blocks.** A block stores each key in full, after 17 bytes of lengths, sequence number and kind. RocksDB stores the part that differs from the previous key, with varint lengths. With Kommander's log keys and 256-byte uncompressed values, Solarium's flushes wrote 8% more bytes than the keys and values. RocksDB's wrote 4% fewer.
- **Compression.** The default is `None` and the only codec is LZ4, from the managed `K4os.Compression.LZ4`. RocksDB defaults to Snappy and has more codecs. Solarium compresses data blocks only. Unless `CacheDataBlocks` is on, a compressed block is decompressed at each read.
- **Index and filter.** All tables have a partitioned index and filter, as with RocksDB's `kTwoLevelIndexSearch` and `partition_filters`, which are opt-in there. A partition has 4 KB of index entries. A table written before partitions has one partition until a compaction rewrites it.
- **Checksums.** A block's checksum is a CRC-32 with a context modifier, as in RocksDB's `format_version` 6. The base is the table's id folded to 32 bits, XOR the sum of the halves of the block's offset. RocksDB uses CRC32c or XXH3, and a per-file base in the footer.
- **Table checks.** An open compares the 64-bit table id, the size, the entry count, the sequence numbers and the key range with the manifest. RocksDB compares the unique id and the size. Solarium also checks a data block's first and last keys against its index entries at each read from the file.
- **WAL.** The file has a header and a version. RocksDB's WAL has 32 KB blocks. Each Solarium record carries the offset that a flush had made durable, and recovery uses it to tell power-loss damage from damage to flushed data (see [durability.md](durability.md)). RocksDB's recovery modes either fail the open after such a power loss or drop data after any damage.

## Block cache

- **Default.** A family has a block cache only if you set `ColumnFamilyOptions.BlockCache`. In RocksDB, a table factory gets a 32 MiB LRU cache by default. `CacheIndexAndFilterBlocks` throws if there is no `BlockCache`.
- **Data blocks.** They are cached only with `CacheDataBlocks`. RocksDB always caches them when it has a cache. With the option off, reads use the OS page cache. The option made reads 1.14 to 1.46 times as fast on a store that fits in the cache, and 0.65 to 0.93 times as fast on a store ten times its size (see [benchmarks.md](benchmarks.md)).
- **Charges.** A data block is charged its decompressed length. A partition's index and filter are one entry, charged the bytes of their arrays. RocksDB caches these as two blocks and charges the allocations.
- **Top-level index.** It stays in the table reader and counts in `estimate-table-readers-mem`. In RocksDB it is in the cache, pinned by `pin_top_level_index_and_filter`.
- **Prefetch.** A table reads, verifies and caches its partitions at open, as with `prefetch_index_and_filter_in_cache`. The output of a compaction does too.
- **Partition misses.** A lookup on an evicted partition reads and decodes its two blocks again, a few kilobytes. RocksDB searches the block in place. With a cache of a quarter of the partitions, lookups from one thread were about 2.5 times slower.
- **Eviction.** There is no `strict_capacity_limit`. When only pinned entries are left, the cache exceeds its capacity, which is also RocksDB's default. A reader that took an index can use it after the eviction, but does not hold a reference in the cache: the charge leaves the cache at once, and the memory is freed after the last reader. An iterator keeps the index partitions it is reading.
- **Closed tables and databases.** When a compaction retires a table, its index and filter partitions are erased immediately and its data blocks are left to the LRU. A database that closes erases its entries. RocksDB leaves both cases to the LRU.
- **Pools.** The high-priority pool is the same as RocksDB's. There is no low-priority pool ratio and no bottom priority.

## Compactions

- **Level sizes.** Levels are always sized dynamically. Static targets are not available. When a family shrinks, the levels above the size it needs are not drained. RocksDB compacts such levels down.
- **Scores.** The level 0 score is the file count. RocksDB also scores level 0 by its bytes. Scores do not discount bytes that compactions are about to bring into a level.
- **Subcompactions.** A merge uses one thread. There is no `max_subcompactions`, whose default is 1.
- **Picking files.** A merge of a sorted level takes one file by `kMinOverlappingRatio`. It does not expand to more files and does not check the overlap two levels down (`max_compaction_bytes`).
- **Moves.** Several level 0 files can move at once into any level. In RocksDB, a move of several files needs a level that has files.
- **Range tombstones.** They are cut at output file boundaries with an excluded user key as the bound. RocksDB uses internal keys and a sentinel.
- **Sequence numbers.** A merge preserves them. RocksDB sets them to 0 in the bottom level.
- **`CompactRange`** does not flush an overlapping memtable first.
- **Threads.** Compactions use up to `MaxBackgroundCompactions` threads and flushes use one. There is no `max_background_jobs` and no `max_background_flushes`. Manual compactions are always exclusive.
- **Readahead.** A compaction reads 256 KiB at a time, and the size is fixed. RocksDB's `compaction_readahead_size` defaults to 2 MiB. With 2 MiB, a merge of tables in the page cache took a quarter longer.

## Checkpoints

- **WAL.** A checkpoint has no WAL files. The content of the memtables goes to new SSTables of the copy, and the source is not flushed. RocksDB copies its WAL files when they are below `log_size_for_flush`, and otherwise flushes the source.
- **Completion.** `FORMAT` is written last, which means that an incomplete checkpoint does not open. RocksDB builds the checkpoint in a `.tmp` directory and renames it.
- **Lock.** The checkpoint holds the `LOCK` of its directory while it writes, and the copy has the `LOCK` file.
- **Close.** `Dispose` stops a running checkpoint and waits for it. In RocksDB, the caller must not close the database during a checkpoint.

## Rate limits and write stalls

- **Rate limiter.** It paces writes only, as RocksDB's default mode. There is no `kReadsOnly`, `kAllIo` or `rate_limiter_priority`. Flushes of a replay in `Open` are not paced. Fairness serves the compactions first on each tenth refill, where RocksDB draws at random.
- **WAL sync.** There is no `wal_bytes_per_sync`, since the WAL syncs complete groups.
- **Delayed write rate.** It is fixed. RocksDB adapts it to the compaction backlog.
- **Stall conditions.** Only the level 0 file count slows or stops writes. There is no `soft_pending_compaction_bytes_limit` and no `WriteOptions.no_slowdown`.
- **`Flush` during a stall** does not wait for the stall to clear. RocksDB's `Flush` waits by default (`allow_write_stall` false).
- **Invalid options throw.** This covers a level 0 trigger or `TargetFileSizeBase` below 1, a later trigger below the previous one, and a `DelayedWriteRate` below 1. RocksDB corrects these values with a warning, or uses a default.

## Memtables

- **`WriteBufferManager`.** It counts the memtables as `cur-size-all-mem-tables` does, and it does not stall writes (`allow_stall`). With a cache, it charges 256 KiB pinned entries and returns them as soon as the memory falls. RocksDB waits until the memory is below three quarters of the reservation.
- **Time of the charge.** A group's bytes are charged after its inserts. With pipelined commits, the check before a group can miss the groups that are in their memtable stage. Replay flushes only at `WriteBufferSize`, as in RocksDB, and the first write after an open past the cap flushes.
- **Release.** A memtable leaves the manager when its flush installs the table, even if readers hold it. In RocksDB it leaves with the last reference.
- **Range tombstones.** The range delete builds an index of fragments in runs that double in size. A read searches each run. In RocksDB, the first read after a range delete rebuilds the fragments of all the tombstones (see [design.md](design.md)).
- **WAL switch.** After `Flush`, the WAL file changes at the next write group.

## Memory figures

- **`cur-size-all-mem-tables`** counts nodes and range tombstones with their fragments. A node is its key, its value, a 24-byte header and 8 bytes per skip-list level, rounded up to 8. An empty memtable is 0. RocksDB counts arena blocks, which is 2 KB for an empty memtable. Solarium's arena also has up to one partly filled 64 KiB chunk per processor that wrote to the memtable.
- **`estimate-table-readers-mem`** counts index keys and handles, filter bits, and range tombstones with their fragments. It does not count allocations. With `CacheIndexAndFilterBlocks` it counts range tombstones and fragments only.

## Write groups

- **Pipelined commits** are on by default. RocksDB's `enable_pipelined_write` is off. In the `Put` benchmarks, pipelining made 3 to 16 unsynced writers 5% to 15% faster, and one writer 1.5% to 1.7% slower. A full memtable is switched one group later, as in RocksDB.
- **Concurrent inserts.** The leader launches the inserts of the members that are spinning and inserts the batches of the blocked members itself. RocksDB launches all members. Where most writers block, launches made writes up to 2.8 times slower, because the group is published only after the blocked member wakes.
- **Publication.** The leader waits for the last insert and publishes the group. In RocksDB, the last writer to finish publishes it. Solarium's leader holds the lock that prevents a memtable switch, and that lock belongs to its thread. RocksDB's `Flush` waits in the write queue.
- **Wakes.** The leader wakes each writer. In RocksDB, a leader of 20 or more writers wakes them through about √n of them.
- **Concurrent range deletions** go to a copy-on-write array under a small lock. RocksDB has a second skip list.
- **Spin.** A waiting writer spins for up to 20 µs before it blocks, or for 1 µs when more than about a quarter of the long spins end in a block. RocksDB spins for about 1 µs and then calls `sched_yield` for up to `write_thread_max_yield_usec`. Solarium does not yield: yields made 16 writers on 16 CPUs 8% to 10% slower.
- **Spin credit.** It has RocksDB's sampling and decay of 1/1024, and it starts 16 blocks above 0. RocksDB's starts at 0, and one early block stops its yields. While the spin is short, Solarium probes in windows of long spins. Single probes among short spins blocked too often to bring the long spin back.
- **Flush waits.** A writer that waits for a WAL flush does not spin. RocksDB does not check for a flush.

## Reads

- **Values in place.** `TryGet` and `MultiGet` with a visitor lend the value to a callback for the length of its call. RocksDB returns a `PinnableSlice` that the caller must reset. A forgotten release would pin the memtables and tables, and a double release would free them under a reader.
- **Pins.** The read pins its version while the callback executes. A `PinnableSlice` pins a data block, and a memtable's value is copied into it.
- **`MultiGet` order.** The keys are visited in the order in which they settle. If a block is damaged, its round throws before it visits the keys that need a block from the file. RocksDB gives each key a status.
- **`MultiGet` I/O.** The blocks of a round are read in one `MultiRead` across tables. There is no `async_io` and no `optimize_multiget_for_io`. RocksDB issues one `MultiRead` per file. On io_uring, page cache hits are read on the calling thread and only the misses go to the ring. RocksDB submits all of them.
- **Iterator readahead** grows from 8 KiB to 256 KiB as in RocksDB. There is no `readahead_size`.
- **`Prev`** searches all the sources at each step, which makes it slower than `Next`.

## Close

`Dispose` flushes the current WAL file, so writes before a clean close survive a power loss. RocksDB's close writes the WAL to the OS and does not flush it, and unsynced writes need `SyncWAL` first. If the flush fails, `Dispose` throws after the close completes.
