using System.Buffers;
using System.IO.Hashing;
using System.Text;
using Solarium.Storage;
using static System.Buffers.Binary.BinaryPrimitives;

namespace Solarium.Engine;

/// <summary>
/// The durable state of a database apart from its WAL: the live WAL files, the column families, the
/// live SSTables of each, and how far each family is flushed. One file, <c>MANIFEST</c>, replaced atomically on every
/// change. Changed under the database's state lock only.
/// </summary>
/// <remarks>
/// Layout, little-endian:
/// <code>
///   [u32 payload length][u32 CRC-32 of payload][payload]
///   payload = [u32 manifest version = 3][u64 next file number][u32 WAL file count][u64 WAL file number]...
///             [u32 family count] family...
///   family  = [u32 id][u32 name length][name, UTF-8][u64 flushed sequence][u32 table count] table...
///   table   = [u64 number][u32 level][u64 size][u64 entry count][u64 min sequence][u64 max sequence]
///             [u32 length][smallest key][u32 length][largest key][u8 flags]
///   flags   = bit 0: the largest key is a range tombstone's end, below which the table ends
/// </code>
/// A family's tables are those of level 0, newest first, then those of each of levels 1 to 6
/// (<see cref="BottomLevel"/>) in key order, where no two tables of a level share a key: each one
/// ends below the next one's smallest key. Version 2 has level 0 and level 6 only and no flags;
/// version 1, written before level 6 existed, has level 0 only. Both still load. A save writes
/// version 3, so a reader built before it refuses the file instead of misreading it.
/// </remarks>
internal sealed class Manifest
{
    /// <summary>The name of the manifest file inside the database directory.</summary>
    public const string FileName = "MANIFEST";
    private const uint Version = 3;
    private const uint Version2 = 2;
    private const uint Version1 = 1;
    private const byte LargestIsExclusiveFlag = 1;

    /// <summary>
    /// The last level, RocksDB's with its default of 7 levels: where its dynamic level sizing puts the
    /// data of a column family below <see cref="ColumnFamilyOptions.MaxBytesForLevelBase"/>, and the
    /// level whose size sets the targets of the levels above it.
    /// </summary>
    public const int BottomLevel = 6;

    /// <summary>The number of levels, 0 to <see cref="BottomLevel"/>, as RocksDB's default <c>num_levels</c>.</summary>
    public const int LevelCount = BottomLevel + 1;

    /// <summary>
    /// The highest column family id. Ids index an array, so a damaged id must not size it; ids are
    /// given in order from 0, so this bounds the families a database can create.
    /// </summary>
    public const int MaxFamilyId = (1 << 20) - 1;

    /// <summary>The next number for a new WAL or SST file. Open raises it above every file it finds.</summary>
    public long NextFileNumber { get; set; } = 1;

    /// <summary>
    /// The WAL files that hold data no SSTable has, oldest first. A file is removed from this list,
    /// durably, before it is deleted; so a listed file that is missing was lost, not cleaned up.
    /// </summary>
    public List<long> WalNumbers { get; } = [];

    /// <summary>Every column family, including the default one.</summary>
    public List<ManifestFamily> ColumnFamilies { get; } = [];

    /// <summary>Reads the manifest of <paramref name="directory"/>, or returns null when there is none. Throws <see cref="SolariumException"/> when the file is corrupt.</summary>
    public static Manifest? Load(IStorageEnvironment environment, string directory)
    {
        string path = Path.Combine(directory, FileName);
        if (!environment.FileExists(path))
            return null;

        byte[] bytes = DurableFiles.ReadAllBytes(environment, path);
        try
        {
            if (bytes.Length < 8 || ReadUInt32LittleEndian(bytes) != bytes.Length - 8)
                throw new InvalidDataException("the length does not match the file size");

            ReadOnlySpan<byte> payload = bytes.AsSpan(8);
            if (Crc32.HashToUInt32(payload) != ReadUInt32LittleEndian(bytes.AsSpan(4)))
                throw new InvalidDataException("the checksum does not match");

            return Decode(payload);
        }
        catch (Exception e) when (e is InvalidDataException or ArgumentOutOfRangeException)
        {
            throw new SolariumException($"'{path}' is corrupt: {e.Message}.", e);
        }
    }

    /// <summary>Replaces the manifest file of <paramref name="directory"/> atomically and durably, through <paramref name="environment"/>.</summary>
    public void Save(IStorageEnvironment environment, string directory)
    {
        ArrayBufferWriter<byte> payload = new();
        Put32(payload, Version);
        Put64(payload, NextFileNumber);
        Put32(payload, (uint)WalNumbers.Count);
        foreach (long number in WalNumbers)
            Put64(payload, number);

        Put32(payload, (uint)ColumnFamilies.Count);

        foreach (ManifestFamily family in ColumnFamilies)
        {
            Put32(payload, (uint)family.Id);
            PutBytes(payload, Encoding.UTF8.GetBytes(family.Name));
            Put64(payload, family.FlushedSequence);
            Put32(payload, (uint)family.Tables.Count);

            foreach (ManifestTable table in family.Tables)
            {
                Put64(payload, table.Number);
                Put32(payload, (uint)table.Level);
                Put64(payload, table.Size);
                Put64(payload, table.EntryCount);
                Put64(payload, table.MinSequence);
                Put64(payload, table.MaxSequence);
                PutBytes(payload, table.Smallest);
                PutBytes(payload, table.Largest);
                payload.Write([table.LargestIsExclusive ? LargestIsExclusiveFlag : (byte)0]);
            }
        }

        byte[] file = new byte[8 + payload.WrittenCount];
        WriteUInt32LittleEndian(file, (uint)payload.WrittenCount);
        WriteUInt32LittleEndian(file.AsSpan(4), Crc32.HashToUInt32(payload.WrittenSpan));
        payload.WrittenSpan.CopyTo(file.AsSpan(8));
        DurableFiles.ReplaceBytes(environment, Path.Combine(directory, FileName), file);
    }

    private static Manifest Decode(ReadOnlySpan<byte> payload)
    {
        int position = 0;
        uint version = Get32(payload, ref position);
        if (version != Version && version != Version2 && version != Version1)
            throw new InvalidDataException($"manifest version {version} is not supported");

        Manifest manifest = new() { NextFileNumber = Get64(payload, ref position) };
        uint walCount = Get32(payload, ref position);
        for (uint w = 0; w < walCount; w++)
            manifest.WalNumbers.Add(Get64(payload, ref position));

        uint familyCount = Get32(payload, ref position);

        for (uint f = 0; f < familyCount; f++)
        {
            uint id = Get32(payload, ref position);
            if (id > MaxFamilyId)
                throw new InvalidDataException($"column family id {id} is above {MaxFamilyId}");

            ManifestFamily family = new()
            {
                Id = (int)id,
                Name = Encoding.UTF8.GetString(GetBytes(payload, ref position)),
                FlushedSequence = Get64(payload, ref position),
            };

            uint tableCount = Get32(payload, ref position);
            for (uint t = 0; t < tableCount; t++)
            {
                long number = Get64(payload, ref position);
                int level = (int)Get32(payload, ref position);
                long size = Get64(payload, ref position);
                long entryCount = Get64(payload, ref position);
                long minSequence = Get64(payload, ref position);
                long maxSequence = Get64(payload, ref position);
                byte[] smallest = GetBytes(payload, ref position);
                byte[] largest = GetBytes(payload, ref position);
                byte flags = version == Version ? Get8(payload, ref position) : (byte)0;
                if ((flags & ~LargestIsExclusiveFlag) != 0)
                    throw new InvalidDataException($"table {number} has unknown flags {flags}");

                family.Tables.Add(new ManifestTable
                {
                    Number = number,
                    Level = level,
                    Size = size,
                    EntryCount = entryCount,
                    MinSequence = minSequence,
                    MaxSequence = maxSequence,
                    Smallest = smallest,
                    Largest = largest,
                    LargestIsExclusive = (flags & LargestIsExclusiveFlag) != 0,
                });
            }

            CheckLevels(family, version);
            manifest.ColumnFamilies.Add(family);
        }

        if (position != payload.Length)
            throw new InvalidDataException("trailing bytes after the last family");

        if (manifest.ColumnFamilies.Count == 0 || manifest.ColumnFamilies.Exists(f => f.Id < 0))
            throw new InvalidDataException("the family list is empty or has a negative id");

        return manifest;
    }

    // Level 0 first, then each sorted level in key order with no shared key. Version 2 has levels 0
    // and 6 only, version 1 level 0 only.
    private static void CheckLevels(ManifestFamily family, uint version)
    {
        List<ManifestTable> tables = family.Tables;
        for (int i = 0; i < tables.Count; i++)
        {
            ManifestTable table = tables[i];
            bool allowed = version switch
            {
                Version1 => table.Level == 0,
                Version2 => table.Level is 0 or BottomLevel,
                _ => table.Level is >= 0 and <= BottomLevel,
            };

            if (!allowed)
                throw new InvalidDataException($"table {table.Number} of family '{family.Name}' is at level {table.Level}");

            if (i == 0)
                continue;

            ManifestTable previous = tables[i - 1];
            if (previous.Level > table.Level)
                throw new InvalidDataException($"table {table.Number} of family '{family.Name}' is at level {table.Level} after level {previous.Level}");

            if (table.Level > 0 && previous.Level == table.Level && !previous.EndsBefore(table))
                throw new InvalidDataException($"tables {previous.Number} and {table.Number} of family '{family.Name}' overlap at level {table.Level}");
        }
    }

    private static void Put32(IBufferWriter<byte> output, uint value)
    {
        WriteUInt32LittleEndian(output.GetSpan(4), value);
        output.Advance(4);
    }

    private static void Put64(IBufferWriter<byte> output, long value)
    {
        WriteInt64LittleEndian(output.GetSpan(8), value);
        output.Advance(8);
    }

    private static void PutBytes(IBufferWriter<byte> output, byte[] bytes)
    {
        Put32(output, (uint)bytes.Length);
        output.Write(bytes);
    }

    private static byte Get8(ReadOnlySpan<byte> payload, ref int position)
    {
        byte value = payload.Slice(position, 1)[0];
        position++;
        return value;
    }

    private static uint Get32(ReadOnlySpan<byte> payload, ref int position)
    {
        uint value = ReadUInt32LittleEndian(payload[position..]);
        position += 4;
        return value;
    }

    private static long Get64(ReadOnlySpan<byte> payload, ref int position)
    {
        long value = ReadInt64LittleEndian(payload[position..]);
        position += 8;
        return value;
    }

    private static byte[] GetBytes(ReadOnlySpan<byte> payload, ref int position)
    {
        uint length = Get32(payload, ref position);
        if (length > payload.Length - position)
            throw new InvalidDataException($"a field of {length} bytes runs past the end");

        byte[] bytes = payload.Slice(position, (int)length).ToArray();
        position += (int)length;
        return bytes;
    }
}
