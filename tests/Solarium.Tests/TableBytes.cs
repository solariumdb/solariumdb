using System.Buffers.Binary;
using Solarium.Engine;

namespace Solarium.Tests;

/// <summary>
/// Reads and patches the bytes of an SSTable file in the layout <see cref="TableFormat"/> documents, for tests that
/// damage a table where only its decoder, or only a check above the checksum, can find the damage.
/// </summary>
internal static class TableBytes
{
    /// <summary>The format version of the table, the <c>u32</c> 16 bytes before its end in every version.</summary>
    public static uint Version(byte[] table) => BinaryPrimitives.ReadUInt32LittleEndian(table.AsSpan(table.Length - 16));

    /// <summary>The format version of the table file at <paramref name="path"/>.</summary>
    public static uint Version(string path) => Version(File.ReadAllBytes(path));

    /// <summary>The table id in the footer of a version 5 table, or 0 for an older one, which has none.</summary>
    public static ulong Id(byte[] table) =>
        Version(table) == TableFormat.Version ? BinaryPrimitives.ReadUInt64LittleEndian(table.AsSpan(table.Length - TableFormat.FooterSize)) : 0;

    /// <summary>
    /// The offset and size (with its checksum) of the block that footer handle <paramref name="handle"/> names: 0 the
    /// range tombstones, 1 the filter, 2 the properties, 3 the (top-level) index. The handles are the first 48 of the
    /// last <see cref="TableFormat.FooterSize4"/> bytes in every version.
    /// </summary>
    public static (int Offset, int Size) FooterHandle(byte[] table, int handle)
    {
        int at = table.Length - TableFormat.FooterSize4 + (handle * 12);
        return ((int)BinaryPrimitives.ReadInt64LittleEndian(table.AsSpan(at)), BinaryPrimitives.ReadInt32LittleEndian(table.AsSpan(at + 8)));
    }

    /// <summary>
    /// The offset and size (with its trailer) of every data block of a version 5 or 4 table, in key order: the
    /// entries of each index partition that the top-level index names, as
    /// <c>[u32 length][last key][u64 last sequence][u64 offset][u32 size]</c>.
    /// </summary>
    public static List<(int Offset, int Size)> DataBlocks(byte[] table)
    {
        List<(int Offset, int Size)> blocks = [];
        (int topLevel, _) = FooterHandle(table, 3);
        int partitions = BinaryPrimitives.ReadInt32LittleEndian(table.AsSpan(topLevel));
        int at = topLevel + 4;
        for (int p = 0; p < partitions; p++)
        {
            int key = BinaryPrimitives.ReadInt32LittleEndian(table.AsSpan(at));
            int index = (int)BinaryPrimitives.ReadInt64LittleEndian(table.AsSpan(at + 4 + key + 8));
            at += 4 + key + 8 + 12 + 12;

            int count = BinaryPrimitives.ReadInt32LittleEndian(table.AsSpan(index));
            int entry = index + 4;
            for (int i = 0; i < count; i++)
            {
                int last = BinaryPrimitives.ReadInt32LittleEndian(table.AsSpan(entry));
                int handle = entry + 4 + last + 8;
                blocks.Add(((int)BinaryPrimitives.ReadInt64LittleEndian(table.AsSpan(handle)), BinaryPrimitives.ReadInt32LittleEndian(table.AsSpan(handle + 8))));
                entry = handle + 12;
            }
        }

        return blocks;
    }

    /// <summary>
    /// Writes the checksum of the block at (<paramref name="offset"/>, <paramref name="size"/>), whose bytes were
    /// changed, as the table's builder would have: its CRC-32 bound to the table's id and the offset.
    /// </summary>
    public static void Reseal(byte[] table, int offset, int size) =>
        BinaryPrimitives.WriteUInt32LittleEndian(table.AsSpan(offset + size - 4),
            TableFormat.BlockChecksum(table.AsSpan(offset, size - 4), Id(table), offset));
}
