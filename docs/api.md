# API

Solarium's public API follows RocksDB's names and semantics. A member that has no RocksDB equivalent is marked "none", with the reason. The differences are described in [rocksdb-differences.md](rocksdb-differences.md).

All members are safe to call from any thread.

## Database

| Member | RocksDB equivalent |
| --- | --- |
| `SolariumDb.Open(path, options, columnFamilies)` | `RocksDb.Open` with `CreateIfMissing` and `CreateMissingColumnFamilies` |
| `Get(key, family, ReadOptions)`, `MultiGet(keys, family, ReadOptions)` | `Get`, `MultiGet`. Of the read options, `fill_cache` applies. The returned arrays belong to the caller. |
| `TryGet(key, ValueReader<TResult>, out result, family, ReadOptions)` | `Get` into a `PinnableSlice`. A callback reads the value in place. |
| `MultiGet(keys, ref IValueVisitor, family, ReadOptions)` | `MultiGet` into `PinnableSlice`s. A struct reads each value in place and gets the key's index. |
| `Put`, `Delete`, `DeleteRange` | The same. A range whose end comes before its begin throws `ArgumentException` when you add it to the batch. In RocksDB the write fails with `InvalidArgument`. An empty range is a no-op. |
| `Write(WriteBatch, WriteOptions)` | `Write`, with `WriteOptions.Sync` |
| `WriteBatch.Sequence` | `WriteBatchInternal::Sequence`: the batch's first sequence number, set when `Write` returns |
| `WriteBatch.GroupLastSequence` | None: RocksDB does not expose `WriteGroup::last_sequence`. It is the last sequence number of the write group. With `WriteOptions.Sync`, all numbers up to it are durable when `Write` returns. |
| `NewIterator(family, ReadOptions)` | `NewIterator`, with `iterate_upper_bound` and `fill_cache` |
| `Iterator`: `Seek`, `SeekToFirst`, `SeekToLast`, `Next`, `Prev`, `Valid`, `Key`, `Value` | The same |
| `Flush(family, wait)` | `Flush` with `FlushOptions.wait` |
| `CompactRange(begin, end, family)` | `CompactRange`, but an overlapping memtable is not flushed first |
| `DeleteFilesInRange(begin, end, family, includeEnd)` | `DeleteFilesInRange`. `includeEnd` defaults to true, as in RocksDB's C API. |
| `CreateCheckpoint(directory)` | `Checkpoint::CreateCheckpoint` |
| `LatestSequenceNumber` | `GetLatestSequenceNumber` |
| `SolariumException.IsCorruption` | `Status::IsCorruption`: damage that no crash explains (see [durability.md](durability.md)). It is also true for an exception that wraps a corruption. |
| `GetProperty(name, family)` | `GetProperty`, for the [integer properties](#properties) |
| `GetLiveFilesMetaData()`, `LiveFileMetaData` | `GetLiveFilesMetaData` |

The values that `ValueReader` and `IValueVisitor` receive belong to the engine and are valid until the callback returns. Copy what you need after that. `Iterator.Key` and `Iterator.Value` are valid until the iterator moves.

## Column family options

| Member | Default | RocksDB equivalent |
| --- | --- | --- |
| `WriteBufferSize` | 64 MB | `write_buffer_size` |
| `MaxWriteBufferNumber` | 2 | `max_write_buffer_number` |
| `Level0FileNumCompactionTrigger` | 4 | `level0_file_num_compaction_trigger` |
| `Level0SlowdownWritesTrigger` | 20 | `level0_slowdown_writes_trigger` |
| `Level0StopWritesTrigger` | 36 | `level0_stop_writes_trigger` |
| `TargetFileSizeBase` | 64 MB | `target_file_size_base` |
| `MaxBytesForLevelBase` | 256 MB | `max_bytes_for_level_base`, with `level_compaction_dynamic_level_bytes` always on |
| `MaxBytesForLevelMultiplier` | 10 | `max_bytes_for_level_multiplier` |
| `DisableAutoCompactions` | false | `disable_auto_compactions` |
| `Compression` | `None` | `compression`; the other value is `Lz4` |
| `BlockCache` | none | `BlockBasedTableOptions::block_cache` |
| `CacheDataBlocks` | false | None: RocksDB always caches data blocks when it has a block cache. Requires a `BlockCache`. |
| `CacheIndexAndFilterBlocks` | false | `cache_index_and_filter_blocks`. Requires a `BlockCache`. |
| `PinL0FilterAndIndexBlocksInCache` | false | `pin_l0_filter_and_index_blocks_in_cache` |
| `CacheIndexAndFilterBlocksWithHighPriority` | true | `cache_index_and_filter_blocks_with_high_priority` |

## Database options

| Member | Default | RocksDB equivalent |
| --- | --- | --- |
| `DefaultColumnFamilyOptions`, `ColumnFamilyOptions` | | The options of each column family descriptor |
| `MaxTotalWalSize` | 0 (automatic) | `max_total_wal_size` |
| `DelayedWriteRate` | 16 MiB/s | `delayed_write_rate` |
| `MaxWriteBatchGroupSizeBytes` | 1 MiB | `max_write_batch_group_size_bytes` |
| `MaxBackgroundCompactions` | 1 | `max_background_compactions`. Flushes use a separate thread. |
| `RateLimiter` | none | `rate_limiter` in mode `kWritesOnly`: flushes at high priority, compactions at low |
| `BytesPerSync` | 0, or 1 MiB with a `RateLimiter` | `bytes_per_sync`, for table files |
| `WriteBufferManager` | none | `write_buffer_manager`. Overrides `DbWriteBufferSize`. |
| `DbWriteBufferSize` | 0 (none) | `db_write_buffer_size` |
| `Environment` | `DefaultStorageEnvironment` | `env` (see [storage-environments.md](storage-environments.md)) |

## Other types and options

| Member | RocksDB equivalent |
| --- | --- |
| `ReadOptions.FillCache` (default true) | `fill_cache`: when false, blocks that the read loads are not cached |
| `Cache(capacity, shardBits, highPriorityPoolRatio)` | `NewLRUCache` with `strict_capacity_limit = false` and a default ratio of 0.5. Its members are `Capacity`, `SetCapacity`, `Usage` and `PinnedUsage`. |
| `WriteBufferManager(bufferSize, cache)` | `WriteBufferManager` with `allow_stall` off. Its members are `BufferSize`, `SetBufferSize`, `MemoryUsage` and `MutableMemoryUsage`. |
| `RateLimiter(bytesPerSecond, refillPeriod, autoTuned, fairness)` | `NewGenericRateLimiter`. Its members are `SetBytesPerSecond`, `SingleBurstBytes`, `TotalBytesThrough`, `TotalRequests` and `TotalWait`. |
| `IStorageFile.SyncRange(offset, length)` | `FSWritableFile::RangeSync` |
| `IStorageEnvironment.MultiRead(requests)`, `ReadRequest(file, buffer, offset)` | `FSRandomAccessFile::MultiRead` with `FSReadRequest`, but one batch can span files. By default the requests are read one after another. |

## Properties

`GetProperty` takes RocksDB's names and returns a decimal integer. It returns null for a name that is not supported. All names have the prefix `rocksdb.`.

| Scope | Names |
| --- | --- |
| Column family (the default one if you give none) | `num-entries-active-mem-table`, `num-immutable-mem-table`, `mem-table-flush-pending`, `cur-size-all-mem-tables`, `estimate-table-readers-mem`, `live-sst-files-size`, `num-files-at-level0` to `num-files-at-level6`, `base-level`, `compaction-pending` |
| The family's `BlockCache`, including what other users of the cache store (null if it has no cache) | `block-cache-capacity`, `block-cache-usage`, `block-cache-pinned-usage` |
| Database | `num-running-flushes`, `num-running-compactions`, `is-write-stopped`, `actual-delayed-write-rate` |

The values match RocksDB's for the same writes, flushes and compactions. The [memory figures](rocksdb-differences.md#memory-figures) are an exception.
