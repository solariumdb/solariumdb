using Kommander.Time;
using Solarium.Storage;

namespace Solarium.Kommander;

/// <summary>
/// How a <see cref="SolariumWAL"/> sizes and opens its database. The defaults follow the shard tuning of
/// Kommander's <c>RocksDbWAL</c>: RocksDB there merges two 64 MB memtables per flush, and Solarium has no
/// merge of memtables, so one 128 MB memtable is the same flush unit.
/// </summary>
public sealed record SolariumWalOptions
{
    /// <summary>The options of a WAL that sets nothing.</summary>
    public static SolariumWalOptions Default { get; } = new();

    /// <summary>
    /// The memtable size of each shard column family, in bytes: the flush unit of the Raft log. Rows that
    /// a compaction pass deletes while they are still in the memtable never reach an SSTable, so a larger
    /// unit writes less. Default: 128 MiB.
    /// </summary>
    public long ShardWriteBufferSizeBytes { get; init; } = 128L * 1024 * 1024;

    /// <summary>The memtables, active and immutable, a shard may hold before writes wait for a flush. Default: 3.</summary>
    public int ShardMaxWriteBufferNumber { get; init; } = 3;

    /// <summary>
    /// The level 0 files of a shard that start a compaction. A partition that owns its shard writes files
    /// that do not overlap, which move to the sorted level without a rewrite. Default: 8.
    /// </summary>
    public int ShardLevel0FileNumCompactionTrigger { get; init; } = 8;

    /// <summary>The level 0 files of a shard that slow writes. Default: 28.</summary>
    public int ShardLevel0SlowdownWritesTrigger { get; init; } = 28;

    /// <summary>The level 0 files of a shard that stop writes. Default: 44.</summary>
    public int ShardLevel0StopWritesTrigger { get; init; } = 44;

    /// <summary>
    /// The bound on the live WAL files, in flush units (<see cref="ShardWriteBufferSizeBytes"/>): above it,
    /// the families that keep the oldest WAL file alive are flushed, so a shard with a trickle of writes
    /// does not keep every WAL file. 0 leaves Solarium's default. Default: 2.
    /// </summary>
    public int MaxTotalWalSizeFlushUnits { get; init; } = 2;

    /// <summary>
    /// The most compactions that run at once (<see cref="DbOptions.MaxBackgroundCompactions"/>). Default:
    /// the compactions of <c>RocksDbWAL</c>'s <c>max_background_jobs</c> of <c>clamp(CPUs / 2, 2, 8)</c>,
    /// as RocksDB splits jobs: a quarter, at least one, for flushes, and the rest, at least one, for
    /// compactions. 6 on 16 CPUs.
    /// </summary>
    public int MaxBackgroundCompactions { get; init; } = CompactionsOf(Math.Clamp(System.Environment.ProcessorCount / 2, 2, 8));

    /// <summary>The compactions that RocksDB runs for <paramref name="jobs"/> background jobs.</summary>
    internal static int CompactionsOf(int jobs) => Math.Max(1, jobs - Math.Max(1, jobs / 4));

    /// <summary>
    /// A cap on the memtables of the WAL and of the host's other Solarium databases together
    /// (<see cref="DbOptions.WriteBufferManager"/>), the memtable half of <c>RocksDbWAL</c>'s
    /// <c>RocksDbSharedResources</c>. Past the cap, the database that writes next flushes its oldest
    /// memtable. The host owns it. Default: none.
    /// </summary>
    public WriteBufferManager? WriteBufferManager { get; init; }

    /// <summary>
    /// A cache for the index and filter blocks of every family of the WAL, pinned at level 0, the block cache
    /// half of <c>RocksDbWAL</c>'s <c>RocksDbSharedResources</c>: it bounds the memory the open tables keep,
    /// which otherwise grows with the log on disk. Give the <see cref="WriteBufferManager"/> the same cache to
    /// put the memtables under the same capacity. The host owns it. Default: none, and the tables keep their
    /// index and filter resident.
    /// </summary>
    public Cache? BlockCache { get; init; }

    /// <summary>Where the files go: the default environment, or the io_uring one on Linux. The host owns it.</summary>
    public IStorageEnvironment Environment { get; init; } = DefaultStorageEnvironment.Instance;

    /// <summary>The clock of the failure log cadence and the filesystem probe interval.</summary>
    public IMonotonicTickSource TickSource { get; init; } = SystemMonotonicTickSource.Instance;
}
