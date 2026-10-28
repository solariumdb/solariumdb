namespace Solarium.Engine;

/// <summary>How the database holds back writes while compaction falls behind, as RocksDB's <c>WriteStallCondition</c>.</summary>
internal enum WriteStallCondition
{
    /// <summary>Writes run at full speed.</summary>
    Normal,

    /// <summary>Writes slow down to <see cref="DbOptions.DelayedWriteRate"/>.</summary>
    Delayed,

    /// <summary>Writes wait until compaction brings the level 0 file count below the stop trigger.</summary>
    Stopped,
}
