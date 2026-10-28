# API

Solarium's public API follows RocksDB's names and semantics. This table maps each member to its RocksDB equivalent; a member with no equivalent states why.

| Member | RocksDB equivalent |
| --- | --- |
| `SolariumDb.Open(path, options, columnFamilies)` | `RocksDb.Open` with `CreateIfMissing` and `CreateMissingColumnFamilies` |
| `Get(key, family, ReadOptions)`, `MultiGet(keys, family, ReadOptions)` | `Get`, `MultiGet`; of the read options, only `fill_cache` applies. `MultiGet` reads the data blocks of its keys together, through `IStorageEnvironment.MultiRead` |
| `Put`, `Delete`, `DeleteRange` | the same; a range whose end comes before its begin throws `ArgumentException` when added to the batch, where RocksDB fails the write with `InvalidArgument`, and an empty range writes nothing |
| `Write(WriteBatch, WriteOptions)` | `Write`, with `WriteOptions.Sync` |
| `NewIterator(family, ReadOptions)` | `NewIterator`, with `iterate_upper_bound` and `fill_cache` |
| `ReadOptions.FillCache` (default true) | `fill_cache`: a data block, or an index and filter read again after an eviction, is not cached |
| `Iterator`: `Seek`, `SeekToFirst`, `SeekToLast`, `Next`, `Prev`, `Valid`, `Key`, `Value` | the same |
| `Flush(family, wait)` | `Flush` with `FlushOptions.wait` |
| `CompactRange(begin, end, family)` | `CompactRange`, without the flush of an overlapping memtable first |
| `DeleteFilesInRange(begin, end, family, includeEnd)` | `DeleteFilesInRange`; `includeEnd` defaults to true, as in RocksDB's C API |
| `CreateCheckpoint(directory)` | `Checkpoint::CreateCheckpoint`; its SSTables are links, or copies across file systems, and it holds no WAL file (see [rocksdb-differences.md](rocksdb-differences.md)) |
| `LatestSequenceNumber` | `GetLatestSequenceNumber` |
| `GetProperty(name, family)` | `GetProperty`, for the integer properties listed below |
| `GetLiveFilesMetaData()`, `LiveFileMetaData` | `GetLiveFilesMetaData`: name, family, level, size, smallest and largest key, sequence numbers, entries |
| `ColumnFamilyOptions.WriteBufferSize` (default 64 MB) | `write_buffer_size` |
| `ColumnFamilyOptions.MaxWriteBufferNumber` (default 2) | `max_write_buffer_number` |
| `ColumnFamilyOptions.Level0FileNumCompactionTrigger` (default 4) | `level0_file_num_compaction_trigger` |
| `ColumnFamilyOptions.Level0SlowdownWritesTrigger` (default 20) | `level0_slowdown_writes_trigger` |
| `ColumnFamilyOptions.Level0StopWritesTrigger` (default 36) | `level0_stop_writes_trigger` |
| `ColumnFamilyOptions.TargetFileSizeBase` (default 64 MB) | `target_file_size_base` |
| `ColumnFamilyOptions.MaxBytesForLevelBase` (default 256 MB) | `max_bytes_for_level_base`, with `level_compaction_dynamic_level_bytes` always on |
| `ColumnFamilyOptions.MaxBytesForLevelMultiplier` (default 10) | `max_bytes_for_level_multiplier` |
| `ColumnFamilyOptions.DisableAutoCompactions` (default false) | `disable_auto_compactions` |
| `ColumnFamilyOptions.Compression` (default `None`; `Lz4`) | `compression`; RocksDB's default is Snappy (see [rocksdb-differences.md](rocksdb-differences.md)) |
| `ColumnFamilyOptions.BlockCache` (default none) | `BlockBasedTableOptions::block_cache`, flat on the family's options: index and filter blocks with `CacheIndexAndFilterBlocks`, data blocks with `CacheDataBlocks` |
| `ColumnFamilyOptions.CacheDataBlocks` (default false) | none: RocksDB caches data blocks whenever it has a block cache; requires a `BlockCache` (see [rocksdb-differences.md](rocksdb-differences.md)) |
| `ColumnFamilyOptions.CacheIndexAndFilterBlocks` (default false) | `BlockBasedTableOptions::cache_index_and_filter_blocks`; requires a `BlockCache` |
| `ColumnFamilyOptions.PinL0FilterAndIndexBlocksInCache` (default false) | `BlockBasedTableOptions::pin_l0_filter_and_index_blocks_in_cache` |
| `ColumnFamilyOptions.CacheIndexAndFilterBlocksWithHighPriority` (default true) | `BlockBasedTableOptions::cache_index_and_filter_blocks_with_high_priority` |
| `Cache(capacity, shardBits, highPriorityPoolRatio)` | `NewLRUCache(capacity, num_shard_bits, false, high_pri_pool_ratio)`, with `strict_capacity_limit = false` and a default ratio of 0.5, with `GetCapacity`, `SetCapacity`, `GetUsage` and `GetPinnedUsage` as `Capacity`, `SetCapacity`, `Usage` and `PinnedUsage` |
| `DbOptions.DefaultColumnFamilyOptions`, `DbOptions.ColumnFamilyOptions` | the options of each column family descriptor |
| `DbOptions.MaxTotalWalSize` (default 0: automatic) | `max_total_wal_size` |
| `DbOptions.DelayedWriteRate` (default 16 MiB/s) | `delayed_write_rate` |
| `DbOptions.MaxWriteBatchGroupSizeBytes` (default 1 MiB) | `max_write_batch_group_size_bytes` |
| `DbOptions.MaxBackgroundCompactions` (default 1) | `max_background_compactions`; flushes have one thread of their own |
| `DbOptions.RateLimiter` (default none) | `rate_limiter`, writes only (`kWritesOnly`): flush writes at high priority, compaction writes at low |
| `DbOptions.BytesPerSync` (default 0, or 1 MiB with a `RateLimiter`) | `bytes_per_sync`, for table files |
| `DbOptions.WriteBufferManager` (default none) | `write_buffer_manager`; overrides `DbWriteBufferSize` |
| `DbOptions.DbWriteBufferSize` (default 0, none) | `db_write_buffer_size`: a manager of that cap for this database alone |
| `WriteBufferManager(bufferSize, cache)` | `WriteBufferManager(buffer_size, cache)` with no stall, with `buffer_size`, `memory_usage`, `mutable_memtable_memory_usage` and `SetBufferSize` as `BufferSize`, `MemoryUsage`, `MutableMemoryUsage` and `SetBufferSize` |
| `RateLimiter(bytesPerSecond, refillPeriod, autoTuned, fairness)` | `NewGenericRateLimiter`, with `SetBytesPerSecond`, `GetSingleBurstBytes`, `GetTotalBytesThrough` and `GetTotalRequests` as `SetBytesPerSecond`, `SingleBurstBytes`, `TotalBytesThrough` and `TotalRequests`, and `TotalWait` |
| `IStorageFile.SyncRange(offset, length)` | `FSWritableFile::RangeSync`: `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux, nothing elsewhere |
| `IStorageEnvironment.MultiRead(requests)`, `ReadRequest(file, buffer, offset)` | `FSRandomAccessFile::MultiRead` with `FSReadRequest`, but one batch spans files; the default reads the requests one after another |

Every member is safe to call from any thread. `Get` and `MultiGet` return arrays that belong to the caller.

`GetProperty` takes RocksDB's names and returns a decimal integer, or null for a name it does not support. For a column family (the default one when none is given): `rocksdb.num-entries-active-mem-table`, `num-immutable-mem-table`, `mem-table-flush-pending`, `cur-size-all-mem-tables`, `estimate-table-readers-mem`, `live-sst-files-size`, `num-files-at-level0` to `num-files-at-level6`, `base-level`, and `compaction-pending`. For the database: `rocksdb.num-running-flushes`, `num-running-compactions`, `is-write-stopped`, and `actual-delayed-write-rate`. The values match RocksDB's for the same writes, flushes and compactions, except for the memory figures noted under the differences in [rocksdb-differences.md](rocksdb-differences.md). `block-cache-capacity`, `block-cache-usage` and `block-cache-pinned-usage` are the family's `BlockCache`'s figures, for everything that shares it, and null for a family without one, as in RocksDB with no block cache.
