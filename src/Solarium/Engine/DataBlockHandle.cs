namespace Solarium.Engine;

/// <summary>
/// Where a data block of an SSTable is: the index of its partition, the partition's number, and its number in that
/// index, which give its offset and size in the file, checksum included, and the keys it must hold: the read of the
/// block checks its entries against them. <see cref="TableReader.TryLocate"/> returns it.
/// </summary>
internal readonly record struct DataBlockHandle(TableIndex Index, int Partition, int Block)
{
    /// <summary>The offset of the block in the file.</summary>
    public long Offset => Index.Offset(Block);

    /// <summary>The size of the block in the file, its trailer included.</summary>
    public int Size => Index.Size(Block);
}
