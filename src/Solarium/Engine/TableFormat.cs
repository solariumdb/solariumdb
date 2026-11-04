using System.IO.Hashing;

namespace Solarium.Engine;

/// <summary>
/// The SSTable format. All integers are little-endian. Every block is followed by the CRC-32 of its
/// payload; a block handle (offset, size) covers both. A data block also has a type byte between its
/// payload and the CRC-32, which covers the type too, as RocksDB's block trailer, and from version 5 the
/// offset of its last entry before the type, so a reader checks the block's last key against its index
/// entry without walking the entries before it. From version 5 the stored checksum of a block is its CRC-32 XOR <see cref="ChecksumModifier"/> of the table's id and the
/// block's offset, as RocksDB's context checksum (<c>format_version</c> 6), so a whole, valid block that a
/// misdirected write puts at another block's place, of this table or of another, fails its checksum.
/// </summary>
/// <remarks>
/// <code>
///   [data block]... [range tombstone block] ([filter partition] [index partition])... [properties block]
///   [top-level index] [footer]
///
///   data block      = [payload][u32 last entry][u8 type]   type 0: payload = entry...
///                                                          type 1: payload = [u32 raw size][LZ4 block of entry...]
///                     last entry: the offset of the block's last entry among its (decompressed) entries
///   entry           = [u32 key length][key][u64 sequence][u8 kind][u32 value length][value]
///   range tombstone = [u32 count] ([u32 length][begin][u32 length][end][u64 sequence])...
///   filter          = [u32 bit count][u8 hash count][bits]     Bloom filter over the user keys of a partition
///   index           = [u32 count] ([u32 length][last key][u64 last sequence][u64 offset][u32 size])...
///   properties      = [u64 entries][u64 min sequence][u64 max sequence][u32 length][smallest][u32 length][largest]
///   top-level index = [u32 count] ([u32 length][last key][u64 last sequence][u64 index offset][u32 index size]
///                                  [u64 filter offset][u32 filter size])...
///   footer          = [u64 table id], then 4 handles (range tombstone, filter, properties, top-level index) as
///                     [u64 offset][u32 size], then [u32 format version][u32 CRC-32 of the 60 bytes before it][u64 magic]
/// </code>
/// The table id is a random nonzero number that the builder draws for each table and the manifest records
/// beside the table's number, so an open also refuses a whole table file that another one replaced. The
/// footer's own CRC-32 has no modifier: it covers the id, and the footer is found at the end of the file.
/// An index partition holds the last internal key of each of a run of data blocks, and is cut once it
/// reaches <see cref="PartitionSize"/>, as RocksDB's partitioned index with <c>metadata_block_size</c>. Its
/// filter covers the user keys of those blocks; a key whose versions span two partitions is in both filters.
/// The top-level index holds the last internal key of each partition, so the versions of one key may span
/// blocks and partitions. In versions 5 and 4 the footer's filter handle is (0, 0): each partition names its
/// filter. Version 4 and older tables have no table id, a footer of <see cref="FooterSize4"/> bytes without
/// it, whose CRC-32 covers the 48 bytes of handles and the version, plain CRC-32s on their blocks, and no
/// last entry offset in their data blocks.
/// Version 3 and version 2 tables have one index block and one filter block, which the footer names, and
/// read as one partition. Version 2, written before data blocks had a type, has no type byte: its data
/// blocks are entries. The format version is the <c>u32</c> 16 bytes before the end in every version.
/// </remarks>
internal static class TableFormat
{
    /// <summary>The target payload size of a data block; a block is closed once it reaches this.</summary>
    public const int BlockSize = 4096;
    /// <summary>The bytes after a data block's payload: the offset of its last entry, its type, and its checksum.</summary>
    public const int DataTrailerSize = 4 + DataTrailerSize4;
    /// <summary>The bytes after a data block's payload in a version 4 or 3 table: its type and its checksum.</summary>
    public const int DataTrailerSize4 = 1 + 4;
    /// <summary>The footer size in bytes: the table id, four block handles, the version, its checksum, and the magic.</summary>
    public const int FooterSize = 8 + FooterSize4;
    /// <summary>The footer size of a version 4, 3 or 2 table, which has no table id.</summary>
    public const int FooterSize4 = (4 * 12) + 4 + 4 + 8;
    /// <summary>The bytes of index entries at which a builder cuts an index partition, as RocksDB's <c>metadata_block_size</c>.</summary>
    public const int PartitionSize = 4096;
    /// <summary>
    /// The SSTable format version a builder writes: a partitioned index and filter, checksums bound to the table
    /// and the offset, and data blocks that name their last entry.
    /// </summary>
    public const uint Version = 5;
    /// <summary>The format version with a partitioned index and filter and plain block checksums, which a reader still accepts.</summary>
    public const uint Version4 = 4;
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

    /// <summary>
    /// What a version 5 table XORs into the CRC-32 of the block at <paramref name="offset"/>: zero for a table
    /// whose <paramref name="tableId"/> is 0, one of a version before 5, else RocksDB's
    /// <c>ChecksumModifierForContext</c> with the id folded to 32 bits as its base. Two blocks of one table
    /// below 4 GiB always get different modifiers, so a block moved inside the table always fails; a block of
    /// another table fails unless the two ids' modifiers collide, one chance in 2^32.
    /// </summary>
    public static uint ChecksumModifier(ulong tableId, long offset)
    {
        if (tableId == 0)
            return 0;

        uint contextBase = (uint)tableId ^ (uint)(tableId >> 32);
        return contextBase ^ ((uint)offset + (uint)((ulong)offset >> 32));
    }

    /// <summary>The checksum stored after a block at <paramref name="offset"/> of table <paramref name="tableId"/> whose checked bytes are <paramref name="covered"/>.</summary>
    public static uint BlockChecksum(ReadOnlySpan<byte> covered, ulong tableId, long offset) =>
        Crc32.HashToUInt32(covered) ^ ChecksumModifier(tableId, offset);

    /// <summary>The file name of SSTable <paramref name="number"/>.</summary>
    public static string FileName(long number) => $"{number:D6}.sst";
}
