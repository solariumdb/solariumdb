namespace Solarium;

/// <summary>Options for one write.</summary>
public sealed record WriteOptions
{
    /// <summary>The shared options of a write that is not synced.</summary>
    public static WriteOptions Default { get; } = new();

    /// <summary>The shared options of a write that is flushed to stable storage before it returns.</summary>
    public static WriteOptions Synced { get; } = new() { Sync = true };

    /// <summary>
    /// Flush the WAL before the write returns, so it survives a power loss. When false, the write
    /// survives a process crash but not a power loss. Default: false, as in RocksDB.
    /// </summary>
    public bool Sync { get; init; }
}
