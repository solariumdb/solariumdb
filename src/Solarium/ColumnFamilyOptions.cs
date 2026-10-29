namespace Solarium;

/// <summary>Options for one column family.</summary>
public sealed record ColumnFamilyOptions
{
    /// <summary>The memtable size at which it becomes immutable and is flushed to an SSTable. Default: 64 MB.</summary>
    public long WriteBufferSize { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// The most memtables (the active one and the immutable ones) the family can hold. At this count,
    /// writes wait for a flush. Minimum and default: 2, as in RocksDB.
    /// </summary>
    public int MaxWriteBufferNumber { get; init; } = 2;

    /// <summary>
    /// The number of level 0 SSTables at which a compaction of the family starts. Minimum: 1.
    /// Default: 4, as RocksDB's <c>level0_file_num_compaction_trigger</c>.
    /// </summary>
    public int Level0FileNumCompactionTrigger { get; init; } = 4;

    /// <summary>
    /// The number of level 0 SSTables at which writes to every column family slow down to
    /// <see cref="DbOptions.DelayedWriteRate"/>, so compaction can catch up. At least
    /// <see cref="Level0FileNumCompactionTrigger"/>. Default: 20, as RocksDB's
    /// <c>level0_slowdown_writes_trigger</c>. Not applied when <see cref="DisableAutoCompactions"/> is set.
    /// </summary>
    public int Level0SlowdownWritesTrigger { get; init; } = 20;

    /// <summary>
    /// The number of level 0 SSTables at which writes to every column family wait until compaction
    /// brings the count below it. At least <see cref="Level0SlowdownWritesTrigger"/>. Default: 36, as
    /// RocksDB's <c>level0_stop_writes_trigger</c>. Not applied when <see cref="DisableAutoCompactions"/> is set.
    /// </summary>
    public int Level0StopWritesTrigger { get; init; } = 36;

    /// <summary>
    /// The size at which a compaction ends an output SSTable and starts the next one. A file ends only
    /// between two keys, so it can be a little larger. Default: 64 MB, as RocksDB's
    /// <c>target_file_size_base</c>.
    /// </summary>
    public long TargetFileSizeBase { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// The most bytes the base level, the level that level 0 compacts into, may hold. The levels are
    /// sized as RocksDB's <c>level_compaction_dynamic_level_bytes</c>, its default: the bottom level's
    /// target is its size, each level above has a <see cref="MaxBytesForLevelMultiplier"/>th of the one
    /// below, and the base level is the highest one whose target is still at most this. A family
    /// smaller than this keeps all its sorted data in level 6. Default: 256 MB, as RocksDB's
    /// <c>max_bytes_for_level_base</c>.
    /// </summary>
    public long MaxBytesForLevelBase { get; init; } = 256L * 1024 * 1024;

    /// <summary>
    /// How many times the target of a level is the target of the level above it. Above 1. Default: 10,
    /// as RocksDB's <c>max_bytes_for_level_multiplier</c>.
    /// </summary>
    public double MaxBytesForLevelMultiplier { get; init; } = 10;

    /// <summary>
    /// No compaction starts on its own; <see cref="SolariumDb.CompactRange"/> still compacts, and the
    /// level 0 file count slows and stops no writes. Default: false, as RocksDB's
    /// <c>disable_auto_compactions</c>.
    /// </summary>
    public bool DisableAutoCompactions { get; init; }

    /// <summary>
    /// How the SSTables that flushes and compactions write from now on store their data blocks. Tables
    /// written before keep their own, so the option can change between opens. Default:
    /// <see cref="CompressionType.None"/>; RocksDB's default compresses with Snappy (see
    /// <c>docs/rocksdb-differences.md</c>).
    /// </summary>
    public CompressionType Compression { get; init; } = CompressionType.None;

    /// <summary>
    /// The cache of the family's SSTable blocks, as RocksDB's <c>BlockBasedTableOptions::block_cache</c>: the index
    /// and filter blocks with <see cref="CacheIndexAndFilterBlocks"/>, and the data blocks with
    /// <see cref="CacheDataBlocks"/>. Several families and databases may share one. Default: null, no cache (see
    /// <c>docs/rocksdb-differences.md</c>).
    /// </summary>
    public Cache? BlockCache { get; init; }

    /// <summary>
    /// Keeps the data blocks that reads take from the SSTables in <see cref="BlockCache"/>, verified and
    /// decompressed, which RocksDB does whenever it has a block cache. A read whose block is cached makes no file
    /// read and decompresses nothing; a read that misses caches the block unless <see cref="ReadOptions.FillCache"/>
    /// is false, and compactions never do. It pays where reads keep finding their blocks cached; on a store much
    /// larger than the cache, most reads miss, and the copy and eviction of each miss under a shard's lock made reads
    /// slower than the operating system's page cache alone (see <c>docs/benchmarks.md</c>).
    /// Requires <see cref="BlockCache"/>. Default: false, and every data block is read from the file.
    /// </summary>
    public bool CacheDataBlocks { get; init; }

    /// <summary>
    /// Keeps each SSTable's index and filter in <see cref="BlockCache"/>, which bounds their memory, instead of in
    /// the table reader for as long as the table is open, as RocksDB's <c>cache_index_and_filter_blocks</c>. A
    /// table puts them in the cache when it opens, one entry per partition of about 4 KB of index entries; after
    /// an eviction, the next lookup reads that partition again from the file. A cache that holds them all, about
    /// 2% of the table bytes and 4% with LZ4, reads as fast as resident ones; a smaller one reads partitions
    /// again on its misses. Requires <see cref="BlockCache"/>. Default: false.
    /// </summary>
    public bool CacheIndexAndFilterBlocks { get; init; }

    /// <summary>
    /// Pins the index and filter of level 0 tables in <see cref="BlockCache"/> while they are in level 0, as
    /// RocksDB's <c>pin_l0_filter_and_index_blocks_in_cache</c>: every lookup searches every level 0 table, so
    /// theirs are never evicted. A table that a compaction moves out of level 0 unpins them. Only with
    /// <see cref="CacheIndexAndFilterBlocks"/>. Default: false.
    /// </summary>
    public bool PinL0FilterAndIndexBlocksInCache { get; init; }

    /// <summary>
    /// Puts the index and filter blocks in <see cref="BlockCache"/> at high priority, as RocksDB's
    /// <c>cache_index_and_filter_blocks_with_high_priority</c>, with its default: they join the cache's high-priority
    /// pool (<see cref="Cache.HighPriorityPoolRatio"/>), and the cache evicts the data blocks read only once before
    /// them (<see cref="CacheDataBlocks"/>). Only with <see cref="CacheIndexAndFilterBlocks"/>. Default: true.
    /// </summary>
    public bool CacheIndexAndFilterBlocksWithHighPriority { get; init; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> when a value is out of range; <paramref name="family"/> names the family in the message.</summary>
    internal void Validate(string family)
    {
        if (!Enum.IsDefined(Compression))
            throw new ArgumentException($"Column family '{family}': Compression {(int)Compression} is not a CompressionType.");

        // RocksDB gives a table factory without a block cache a cache of its own, 32 MiB in current releases; Solarium has no
        // default cache, and a flag that changes nothing is more likely a mistake.
        if (CacheIndexAndFilterBlocks && BlockCache is null)
            throw new ArgumentException($"Column family '{family}': CacheIndexAndFilterBlocks needs a BlockCache.");

        if (CacheDataBlocks && BlockCache is null)
            throw new ArgumentException($"Column family '{family}': CacheDataBlocks needs a BlockCache.");

        if (WriteBufferSize < 1 || MaxWriteBufferNumber < 2)
            throw new ArgumentException($"Column family '{family}': WriteBufferSize must be positive and MaxWriteBufferNumber at least 2.");

        // RocksDB raises a trigger of 0 to 1 with a warning in its log; Solarium has no log, and such a value is more likely a mistake.
        if (Level0FileNumCompactionTrigger < 1 || TargetFileSizeBase < 1)
            throw new ArgumentException($"Column family '{family}': Level0FileNumCompactionTrigger and TargetFileSizeBase must be positive.");

        if (MaxBytesForLevelBase < 1 || !(MaxBytesForLevelMultiplier > 1) || double.IsInfinity(MaxBytesForLevelMultiplier))
            throw new ArgumentException($"Column family '{family}': MaxBytesForLevelBase must be positive and MaxBytesForLevelMultiplier a finite number above 1.");

        // RocksDB raises a slowdown trigger below the compaction trigger, and a stop trigger below the
        // slowdown trigger, to the trigger below it.
        if (Level0SlowdownWritesTrigger < Level0FileNumCompactionTrigger || Level0StopWritesTrigger < Level0SlowdownWritesTrigger)
            throw new ArgumentException(
                $"Column family '{family}': Level0SlowdownWritesTrigger ({Level0SlowdownWritesTrigger}) must be at least Level0FileNumCompactionTrigger ({Level0FileNumCompactionTrigger}), and Level0StopWritesTrigger ({Level0StopWritesTrigger}) at least Level0SlowdownWritesTrigger.");
    }
}
