namespace Solarium;

/// <summary>Options for a read or an iterator.</summary>
public sealed record ReadOptions
{
    /// <summary>The shared options of a read with no upper bound.</summary>
    public static ReadOptions Default { get; } = new();

    /// <summary>An iterator stops before the first key at or after this key. Null: no bound.</summary>
    public byte[]? IterateUpperBound { get; init; }

    /// <summary>
    /// When false, an SSTable whose index and filter a <see cref="Cache"/> has evicted reads them for this read
    /// and does not cache them again, as RocksDB's <c>fill_cache</c>: a read of rows that will not be read again
    /// does not evict the ones that will. True by default. Without a cache, it changes nothing.
    /// </summary>
    public bool FillCache { get; init; } = true;
}
