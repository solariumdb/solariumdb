namespace Solarium.Engine;

/// <summary>
/// What a <see cref="TableReader"/> keeps in <see cref="Cache"/>, under (<see cref="Owner"/>, the table's number),
/// as RocksDB's <c>block_cache</c>: its index and filter when <see cref="IndexAndFilter"/> is set (else resident in
/// the reader), at high priority when <see cref="HighPriority"/> is set, and pinned while <see cref="Pin"/> holds, as
/// RocksDB's <c>cache_index_and_filter_blocks</c>, <c>cache_index_and_filter_blocks_with_high_priority</c> and
/// <c>pin_l0_filter_and_index_blocks_in_cache</c>; and its data blocks when <see cref="DataBlocks"/> is set (else
/// read from the file each time).
/// </summary>
internal readonly record struct TableCaching(Cache? Cache, long Owner, bool IndexAndFilter, bool Pin, bool HighPriority = true, bool DataBlocks = false)
{
    /// <summary>A reader with no cache: it keeps its index and filter resident and reads every data block from the file.</summary>
    public static TableCaching Resident => default;
}
