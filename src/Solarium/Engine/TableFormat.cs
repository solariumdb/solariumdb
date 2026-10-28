namespace Solarium.Engine;

/// <summary>
/// The SSTable format. All integers are little-endian. Every block is followed by the CRC-32 of its
/// payload; a block handle (offset, size) covers both. A data block also has a type byte between its
/// payload and the CRC-32, which covers the type too, as RocksDB's block trailer.
/// </summary>
/// <remarks>
/// <code>
///   [data block]... [range tombstone block] ([filter partition] [index partition])... [properties block]
///   [top-level index] [footer]
///
///   data block      = [payload][u8 type]          type 0: payload = entry...
///                                                 type 1: payload = [u32 raw size][LZ4 block of entry...]
///   entry           = [u32 key length][key][u64 sequence][u8 kind][u32 value length][value]
///   range tombstone = [u32 count] ([u32 length][begin][u32 length][end][u64 sequence])...
///   filter          = [u32 bit count][u8 hash count][bits]     Bloom filter over the user keys of a partition
///   index           = [u32 count] ([u32 length][last key][u64 last sequence][u64 offset][u32 size])...
///   properties      = [u64 entries][u64 min sequence][u64 max sequence][u32 length][smallest][u32 length][largest]
///   top-level index = [u32 count] ([u32 length][last key][u64 last sequence][u64 index offset][u32 index size]
///                                  [u64 filter offset][u32 filter size])...
///   footer          = 4 handles (range tombstone, filter, properties, top-level index) as [u64 offset][u32 size],
///                     then [u32 format version][u32 CRC-32 of the 52 bytes before it][u64 magic]
/// </code>
/// An index partition holds the last internal key of each of a run of data blocks, and is cut once it
/// reaches <see cref="PartitionSize"/>, as RocksDB's partitioned index with <c>metadata_block_size</c>. Its
/// filter covers the user keys of those blocks; a key whose versions span two partitions is in both filters.
/// The top-level index holds the last internal key of each partition, so the versions of one key may span
/// blocks and partitions. In version 4 the footer's filter handle is (0, 0): each partition names its filter.
/// Version 3 and version 2 tables have one index block and one filter block, which the footer names, and
/// read as one partition. Version 2, written before data blocks had a type, has no type byte: its data
/// blocks are entries.
/// </remarks>
internal static class TableFormat
{
    /// <summary>The target payload size of a data block; a block is closed once it reaches this.</summary>
    public const int BlockSize = 4096;
    /// <summary>The footer size in bytes: four block handles, the version, its checksum, and the magic.</summary>
    public const int FooterSize = (4 * 12) + 4 + 4 + 8;
    /// <summary>The bytes of index entries at which a builder cuts an index partition, as RocksDB's <c>metadata_block_size</c>.</summary>
    public const int PartitionSize = 4096;
    /// <summary>The SSTable format version a builder writes: a partitioned index and filter.</summary>
    public const uint Version = 4;
    /// <summary>The format version with one index and one filter block, which a reader still accepts.</summary>
    public const uint Version3 = 3;
    /// <summary>The format version before data blocks had a type byte, which a reader still accepts.</summary>
    public const uint Version2 = 2;
    /// <summary>The type of a data block whose payload is its entries.</summary>
    public const byte RawBlock = 0;
    /// <summary>The type of a data block whose payload is the raw size and an LZ4 block of its entries.</summary>
    public const byte Lz4Block = 1;
    /// <summary>
    /// The most an LZ4 block can expand, about 255 bytes out for each byte in: a raw size above this
    /// many times the compressed bytes cannot be true, so a reader refuses it before it allocates.
    /// </summary>
    public const int Lz4MaxRatio = 255;
    /// <summary>The last eight bytes of every SSTable.</summary>
    public const ulong Magic = 0x534F4C4152494D31;   // "SOLARIM1"

    /// <summary>The file name of SSTable <paramref name="number"/>.</summary>
    public static string FileName(long number) => $"{number:D6}.sst";
}
