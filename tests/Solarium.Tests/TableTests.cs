using System.IO.Hashing;
using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>Builds SSTables from memtables and reads them back: versions, cursors, bloom filter, and damage.</summary>
public sealed class TableTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_table_returns_the_newest_version_at_each_snapshot(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        MemTable memTable = new();
        memTable.Add(B("a"), 1, OperationKind.Put, B("a1"));
        memTable.Add(B("a"), 5, OperationKind.Put, B("a5"));
        memTable.Add(B("b"), 2, OperationKind.Put, B("b2"));
        memTable.Add(B("b"), 6, OperationKind.Delete, null);
        memTable.AddRangeTombstone(B("c"), B("e"), 7);
        memTable.Add(B("d"), 3, OperationKind.Put, B("d3"));

        using TableReader table = Build(scope, memTable);

        Assert.Equal("a5", Newest(table, "a", 10));
        Assert.Equal("a1", Newest(table, "a", 4));
        Assert.Null(Newest(table, "a", 0));
        Assert.Equal("delete", Newest(table, "b", 10));
        Assert.Equal("b2", Newest(table, "b", 5));
        Assert.Equal("d3", Newest(table, "d", 10));
        Assert.Null(Newest(table, "zz", 10));

        Assert.Equal(7, table.CoveringTombstone(B("d"), 10));
        Assert.Equal(0, table.CoveringTombstone(B("d"), 6));
        Assert.Equal(0, table.CoveringTombstone(B("e"), 10));

        TableProperties properties = table.Properties;
        Assert.Equal((5L, 1L, 7L, "a", "e"), (properties.EntryCount, properties.MinSequence, properties.MaxSequence, S(properties.Smallest), S(properties.Largest)));
        Assert.True(properties.Size > TableFormat.FooterSize);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_cursor_returns_every_entry_in_internal_key_order_across_many_blocks(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        MemTable memTable = new();
        long sequence = 0;
        for (int i = 0; i < 3000; i++)
        {
            memTable.Add(B($"key{i:D5}"), ++sequence, OperationKind.Put, B(new string('v', i % 50)));
            if (i % 7 == 0)
                memTable.Add(B($"key{i:D5}"), ++sequence, OperationKind.Delete, null);
        }

        using TableReader table = Build(scope, memTable);
        List<Entry> expected = [.. memTable.Entries()];

        IEntryCursor cursor = table.NewCursor(true);
        List<Entry> actual = new();
        for (cursor.SeekToFirst(); cursor.Valid; cursor.Next())
            actual.Add(cursor.Current);

        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(S(expected[i].Key), S(actual[i].Key));
            Assert.Equal(expected[i].Sequence, actual[i].Sequence);
            Assert.Equal(expected[i].Kind, actual[i].Kind);
            Assert.Equal(S(expected[i].Value), S(actual[i].Value));
        }

        cursor.Seek(B("key01500"));
        Assert.Equal("key01500", S(cursor.Current.Key));
        cursor.Seek(B("key01500x"));
        Assert.Equal("key01501", S(cursor.Current.Key));
        cursor.Seek(B("zzz"));
        Assert.False(cursor.Valid);

        Assert.Equal("key02999", S(table.LastKeyBefore(null, true)));
        Assert.Equal("key01499", S(table.LastKeyBefore(B("key01500"), true)));
        Assert.Equal("key01500", S(table.LastKeyBefore(B("key01500x"), true)));
        Assert.Null(table.LastKeyBefore(B("key00000"), true));
    }

    [Fact]
    public void The_bloom_filter_has_no_false_negatives_and_few_false_positives()
    {
        List<ulong> hashes = Enumerable.Range(0, 10_000).Select(i => XxHash3.HashToUInt64(B($"present{i}"))).ToList();
        BloomFilter filter = BloomFilter.Decode(BloomFilter.Build(hashes));

        Assert.All(hashes, h => Assert.True(filter.MayContain(h)));

        int falsePositives = Enumerable.Range(0, 10_000).Count(i => filter.MayContain(XxHash3.HashToUInt64(B($"absent{i}"))));
        Assert.True(falsePositives < 300, $"{falsePositives} false positives in 10,000");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_table_with_only_range_tombstones_works(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        MemTable memTable = new();
        memTable.AddRangeTombstone(B("a"), B("m"), 4);

        using TableReader table = Build(scope, memTable);

        Assert.Null(Newest(table, "c", 10));
        Assert.Equal(4, table.CoveringTombstone(B("c"), 10));
        Assert.Null(table.LastKeyBefore(null, true));
        IEntryCursor cursor = table.NewCursor(true);
        cursor.SeekToFirst();
        Assert.False(cursor.Valid);
    }

    [Fact]
    public void Damage_is_detected()
    {
        MemTable memTable = new();
        for (int i = 0; i < 100; i++)
            memTable.Add(B($"key{i:D3}"), i + 1, OperationKind.Put, B("value"));

        string path = _directory.Combine("damaged.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
        {
            TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones);
        }

        // A data block: found when the block is read.
        byte[] bytes = File.ReadAllBytes(path);
        bytes[10] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        using (TableReader table = TableReader.Open(1, DefaultStorageEnvironment.Instance.OpenFile(path)))
        {
            SolariumException e = Assert.Throws<SolariumException>(() => table.TryGetNewest(B("key000"), 1000, true, out _));
            Assert.Contains("bad checksum", e.Message);
        }

        // The footer: found at open.
        bytes[10] ^= 0xFF;
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        SolariumException footer = Assert.Throws<SolariumException>(() => TableReader.Open(1, DefaultStorageEnvironment.Instance.OpenFile(path)));
        Assert.Contains("magic", footer.Message);
    }

    [Fact]
    public void A_count_or_an_entry_kind_that_a_valid_checksum_covers_but_does_not_fit_is_corruption()
    {
        MemTable memTable = new();
        for (int i = 0; i < 100; i++)
            memTable.Add(B($"key{i:D3}"), i + 1, OperationKind.Put, B("value"));

        memTable.AddRangeTombstone(B("a"), B("b"), 200);
        string path = _directory.Combine("patched.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
            TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones);

        byte[] original = File.ReadAllBytes(path);

        // The range tombstone block (handle 0), the top-level index (handle 3) and the index partition
        // (-1 here) start with their counts.
        foreach ((int handle, int count) in new[] { (0, -1), (0, int.MaxValue), (3, int.MaxValue), (-1, int.MaxValue) })
        {
            byte[] bytes = (byte[])original.Clone();
            (int offset, int size) = handle >= 0 ? Handle(bytes, handle) : PartitionIndex(bytes, 0);
            BitConverter.TryWriteBytes(bytes.AsSpan(offset, 4), count);
            Reseal(bytes, offset, size);
            File.WriteAllBytes(path, bytes);

            // A reader that fails to open does not take the file, so the test closes it: Windows
            // refuses the next write of the file while a handle is open.
            using IStorageFile damaged = DefaultStorageEnvironment.Instance.OpenFile(path);
            SolariumException e = Assert.Throws<SolariumException>(() => TableReader.Open(1, damaged));
            Assert.Contains("does not decode", e.Message);
        }

        // The kind of the first entry of the first data block, which starts the file.
        byte[] kinds = (byte[])original.Clone();
        (int index, _) = PartitionIndex(kinds, 0);
        int firstKey = BitConverter.ToInt32(kinds, index + 4);
        int blockSize = BitConverter.ToInt32(kinds, index + 8 + firstKey + 16);
        kinds[4 + BitConverter.ToInt32(kinds, 0) + 8] = 9;
        Reseal(kinds, 0, blockSize);
        File.WriteAllBytes(path, kinds);
        using TableReader table = TableReader.Open(1, DefaultStorageEnvironment.Instance.OpenFile(path));
        Assert.Contains("does not decode", Assert.Throws<SolariumException>(() => table.TryGetNewest(B("key000"), 1000, true, out _)).Message);
        IEntryCursor cursor = table.NewCursor(true);
        Assert.Contains("does not decode", Assert.Throws<SolariumException>(cursor.SeekToFirst).Message);
    }

    // The first data block, cut short at every byte with its checksum and its index entry made to
    // match: every read of a data block either reads the entries before the cut or reports the
    // table as corrupt, and never fails with another exception.
    [Fact]
    public void A_data_block_cut_short_at_any_field_with_a_valid_checksum_is_corruption_or_reads_the_entries_before_the_cut()
    {
        MemTable memTable = new();
        memTable.Add(B("a"), 1, OperationKind.Put, B("1"));
        memTable.Add(B("b"), 2, OperationKind.Delete, null);
        memTable.Add(B("c"), 3, OperationKind.Put, B("33"));
        string path = _directory.Combine("cut.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
            TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones);

        byte[] original = File.ReadAllBytes(path);
        (int index, int indexSize) = PartitionIndex(original, 0);
        int lastKey = BitConverter.ToInt32(original, index + 4);
        int sizeAt = index + 8 + lastKey + 16;
        int blockSize = BitConverter.ToInt32(original, sizeAt);
        int corrupt = 0;
        for (int payload = 0; payload < blockSize - 4; payload++)
        {
            byte[] bytes = (byte[])original.Clone();
            BitConverter.TryWriteBytes(bytes.AsSpan(sizeAt, 4), payload + 4);
            Reseal(bytes, 0, payload + 4);
            Reseal(bytes, index, indexSize);
            File.WriteAllBytes(path, bytes);

            using TableReader table = TableReader.Open(1, DefaultStorageEnvironment.Instance.OpenFile(path));
            Exception?[] failures =
            [
                Record.Exception(() => table.TryGetNewest(B("c"), 10, true, out _)),
                Record.Exception(() => table.NewCursor(true).SeekToFirst()),
                Record.Exception(() => table.LastKeyBefore(B("d"), true)),
            ];
            foreach (Exception? failure in failures)
                if (failure is not null)
                {
                    SolariumException e = Assert.IsType<SolariumException>(failure);
                    Assert.Contains("data block 0 at offset 0", e.Message);
                    corrupt++;
                }
        }

        Assert.True(corrupt > 0);
    }

    // The offset and size (with its CRC) of the block that footer handle i names.
    private static (int Offset, int Size) Handle(byte[] table, int i)
    {
        int footer = table.Length - TableFormat.FooterSize;
        return ((int)BitConverter.ToInt64(table, footer + (i * 12)), BitConverter.ToInt32(table, footer + (i * 12) + 8));
    }

    // The offset and size (with its CRC) of index partition p's block, from its top-level index entry:
    // [u32 length][last key][u64 last sequence][u64 index offset][u32 index size][u64 filter offset][u32 filter size].
    private static (int Offset, int Size) PartitionIndex(byte[] table, int p)
    {
        (int topLevel, _) = Handle(table, 3);
        int at = topLevel + 4;
        for (int i = 0; i < p; i++)
            at += 4 + BitConverter.ToInt32(table, at) + 8 + 12 + 12;

        int key = BitConverter.ToInt32(table, at);
        return ((int)BitConverter.ToInt64(table, at + 4 + key + 8), BitConverter.ToInt32(table, at + 4 + key + 16));
    }

    // Writes the CRC of a block whose payload was changed, so only its decoder can find the change.
    private static void Reseal(byte[] table, int offset, int size)
    {
        BitConverter.TryWriteBytes(table.AsSpan(offset + size - 4, 4), Crc32.HashToUInt32(table.AsSpan(offset, size - 4)));
    }

    private TableReader Build(EnvironmentScope scope, MemTable memTable)
    {
        IStorageFile file = scope.Environment.OpenFile(_directory.Combine($"{Guid.NewGuid():N}.sst"));
        TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones);
        return TableReader.Open(1, file);
    }

    private static string? Newest(TableReader table, string key, long snapshot)
    {
        if (!table.TryGetNewest(B(key), snapshot, true, out Entry entry))
            return null;

        return entry.Kind == OperationKind.Delete ? "delete" : S(entry.Value);
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();
}
