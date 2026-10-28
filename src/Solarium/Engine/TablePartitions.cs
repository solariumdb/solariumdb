namespace Solarium.Engine;

/// <summary>
/// The decoded top-level index of an SSTable: for each partition of its index and filter, the last internal key
/// of its last data block and the handles of its index and filter blocks. A table of format version 3 or 2 has
/// one partition, its index and filter blocks. Never changed after it is built.
/// </summary>
internal sealed class TablePartitions(TableIndex index, long[] filterOffsets, int[] filterSizes)
{
    /// <summary>The partitions' last keys and the handles of their index blocks, searched as an index.</summary>
    public TableIndex Index { get; } = index;

    /// <summary>The number of partitions.</summary>
    public int Count => Index.Count;

    /// <summary>The bytes the top-level index keeps: each last key and its handles, without object headers.</summary>
    public long Bytes => Index.KeyAndHandleBytes + ((long)filterOffsets.Length * (sizeof(long) + sizeof(int)));

    /// <summary>The offset of partition <paramref name="partition"/>'s filter block.</summary>
    public long FilterOffset(int partition) => filterOffsets[partition];

    /// <summary>The size of partition <paramref name="partition"/>'s filter block, its checksum included.</summary>
    public int FilterSize(int partition) => filterSizes[partition];
}
