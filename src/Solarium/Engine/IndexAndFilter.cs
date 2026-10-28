namespace Solarium.Engine;

/// <summary>
/// The decoded index and filter blocks of one partition of an SSTable (<see cref="TablePartitions"/>), which a
/// lookup needs together: a <see cref="TableReader"/> keeps them resident, or caches them as one entry of a
/// <see cref="Cache"/>. Never changed after it is built.
/// </summary>
internal sealed class IndexAndFilter(TableIndex index, BloomFilter filter)
{
    /// <summary>The partition's index: the last key and handle of each of its data blocks.</summary>
    public TableIndex Index { get; } = index;

    /// <summary>The partition's Bloom filter, over the user keys of its data blocks.</summary>
    public BloomFilter Filter { get; } = filter;

    /// <summary>The bytes of their arrays, what a cache charges for them.</summary>
    public long Charge => Index.ArrayBytes + Filter.ByteCount;
}
