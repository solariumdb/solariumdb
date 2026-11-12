using Solarium.Engine;
using Solarium.IoUring;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// A table's cursor reads its entries in place and reads the file ahead; a flush takes its entries from the
/// memtable's memory. These tests hold both to what the copies they replaced gave.
/// </summary>
public sealed class TableCursorTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Cases))]
    public void A_cursor_that_reads_ahead_returns_every_entry_from_fewer_reads_than_blocks(string environment, CompressionType compression, int readahead)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        int reads = 0;
        hooks.CorruptedReads = (_, _) =>
        {
            Interlocked.Increment(ref reads);
            return false;
        };

        MemTable memTable = Model(6000);
        using TableReader table = Build(hooks, memTable, compression);
        List<Entry> expected = [.. memTable.Entries()];

        // A block is cut once its entries reach the block size, compressed after or not.
        long blocks = expected.Sum(e => 17L + e.Key.Length + (e.Value?.Length ?? 0)) / (TableFormat.BlockSize + 128);
        Assert.True(blocks > 16, $"the table has only about {blocks} data blocks");

        int before = Volatile.Read(ref reads);
        List<Entry> actual = [];
        using (IEntryCursor cursor = table.NewCursor(fillCache: false, readahead))
        {
            for (cursor.SeekToFirst(); cursor.Valid; cursor.Next())
                actual.Add(cursor.Current);
        }

        AssertSame(expected, actual);
        int scan = Volatile.Read(ref reads) - before;
        Assert.True(scan < blocks / 2, $"the scan of about {blocks} blocks took {scan} reads");

        // A seek into the middle, then the rest: a block that the window does not hold is read from the file.
        int middle = expected.Count / 2;
        actual.Clear();
        using (IEntryCursor cursor = table.NewCursor(fillCache: false, readahead))
        {
            for (cursor.Seek(expected[middle].Key); cursor.Valid; cursor.Next())
                actual.Add(cursor.Current);
        }

        // The seek lands on the first version of the key, which can come before the entry picked.
        int first = expected.FindIndex(e => e.Key.AsSpan().SequenceEqual(expected[middle].Key));
        AssertSame(expected[first..], actual);
        memTable.FreeUnreferenced();
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_seek_finds_the_first_version_of_the_first_key_at_or_after_each_target(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        MemTable memTable = Model(1500);
        using TableReader table = Build(scope.Environment, memTable, CompressionType.None);
        List<Entry> expected = [.. memTable.Entries()];

        using IEntryCursor cursor = table.NewCursor(fillCache: true);
        List<string> targets = ["", "key", "key00000", "key00000x", "key00749", "key00750", "key01499", "key01499x", "zzz"];
        for (int i = 0; i < 1500; i += 37)
            targets.Add($"key{i:D5}");

        foreach (string target in targets)
        {
            cursor.Seek(B(target));
            int index = expected.FindIndex(e => e.Key.AsSpan().SequenceCompareTo(B(target)) >= 0);
            Assert.Equal(index >= 0, cursor.Valid);
            if (index < 0)
                continue;

            Assert.Equal(S(expected[index].Key), S(cursor.CurrentKey));
            Assert.Equal(expected[index].Sequence, cursor.CurrentSequence);
            Assert.Equal(expected[index].Kind, cursor.CurrentKind);
            Assert.Equal(S(expected[index].Value ?? []), S(cursor.CurrentValue));
        }

        memTable.FreeUnreferenced();
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_damaged_block_inside_the_readahead_window_fails_the_scan_at_that_block(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        MemTable memTable = Model(6000);
        string path = _directory.Combine("damaged.sst");
        using (IStorageFile built = scope.Environment.OpenFile(path))
            TableBuilder.Build(built, memTable.Entries(), memTable.Tombstones);

        // A byte of a data block in the middle of the data: the open reads the metadata blocks only.
        byte[] bytes = File.ReadAllBytes(path);
        int damaged = bytes.Length / 3;
        bytes[damaged] ^= 0x40;
        File.WriteAllBytes(path, bytes);

        using TableReader table = TableReader.Open(1, scope.Environment.OpenFile(path));
        int entries = 0;
        using IEntryCursor cursor = table.NewCursor(fillCache: false, TableReader.CompactionReadahead);
        SolariumException e = Assert.Throws<SolariumException>(() =>
        {
            for (cursor.SeekToFirst(); cursor.Valid; cursor.Next())
                entries++;
        });

        Assert.Contains("bad checksum", e.Message);
        Assert.True(entries > 0, "the blocks before the damaged one were not read");
        Assert.False(cursor.Valid);
        memTable.FreeUnreferenced();
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_iterator_holds_a_cached_block_only_until_it_ends_or_is_disposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        // No room: the cache evicts each block at once, so a block lives only while a reader holds it.
        Cache cache = new(0, shardBits: 0);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = true, DisableAutoCompactions = true },
        };

        using SolariumDb db = SolariumDb.Open(_directory.Path, options);
        for (int i = 0; i < 2000; i++)
            db.Put(B($"key{i:D5}"), B(new string('v', 100)));

        db.Flush();
        Assert.Equal(0, cache.NativeBlocks.Count);

        Iterator iterator = db.NewIterator();
        iterator.SeekToFirst();
        Assert.True(iterator.Valid());
        Assert.Equal(1, cache.NativeBlocks.Count);

        // Every step past a block's end lets that block go and takes the next.
        int rows = 0;
        for (; iterator.Valid(); iterator.Next())
        {
            Assert.Equal($"key{rows:D5}", S(iterator.Key()));
            Assert.Equal(1, cache.NativeBlocks.Count);
            rows++;
        }

        Assert.Equal(2000, rows);
        Assert.Equal(0, cache.NativeBlocks.Count);

        iterator.Seek(B("key01000"));
        Assert.Equal(1, cache.NativeBlocks.Count);
        iterator.Dispose();
        Assert.Equal(0, cache.NativeBlocks.Count);
    }

    [Theory]
    [InlineData(long.MaxValue)]
    [InlineData(2500)]
    [InlineData(1)]
    public void A_flush_from_the_memtables_memory_writes_the_table_that_its_flush_entries_give(long snapshot)
    {
        MemTable memTable = Model(2000);
        memTable.Add(B("empty"), 100_000, OperationKind.Put, []);
        memTable.AddRangeTombstone(B("key00100"), B("key00200"), 100_001);
        memTable.AddRangeTombstone(B("key01000"), B("key01001"), 3);
        List<RangeTombstone> tombstones = memTable.TombstonesAt(snapshot);

        string fromEntries = _directory.Combine("entries.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(fromEntries))
        using (TableBuilder builder = new(file, CompressionType.None, default, TableFormat.PartitionSize, id: 7))
        {
            foreach (Entry entry in memTable.FlushEntries(snapshot, tombstones))
                builder.Add(entry);

            builder.Finish(tombstones);
        }

        string fromMemory = _directory.Combine("memory.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(fromMemory))
        using (TableBuilder builder = new(file, CompressionType.None, default, TableFormat.PartitionSize, id: 7))
        {
            memTable.AddFlushEntries(builder, snapshot, tombstones);
            builder.Finish(tombstones);
        }

        Assert.Equal(File.ReadAllBytes(fromEntries), File.ReadAllBytes(fromMemory));
        memTable.FreeUnreferenced();
    }

    /// <summary>Each environment, with and without compression, with the cursor's own readahead (0) and a fixed one.</summary>
    public static TheoryData<string, CompressionType, int> Cases()
    {
        TheoryData<string, CompressionType, int> cases = [];
        foreach (string environment in IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
            foreach (CompressionType compression in new[] { CompressionType.None, CompressionType.Lz4 })
                foreach (int readahead in new[] { 0, 64 * 1024, TableReader.CompactionReadahead })
                    cases.Add(environment, compression, readahead);

        return cases;
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // Keys with one to three versions each, deletes among them, and values of many lengths, some empty.
    private static MemTable Model(int keys)
    {
        MemTable memTable = new();
        long sequence = 0;
        for (int i = 0; i < keys; i++)
        {
            memTable.Add(B($"key{i:D5}"), ++sequence, OperationKind.Put, B(new string((char)('a' + (i % 26)), i % 90)));
            if (i % 7 == 0)
                memTable.Add(B($"key{i:D5}"), ++sequence, OperationKind.Delete, null);

            if (i % 11 == 0)
                memTable.Add(B($"key{i:D5}"), ++sequence, OperationKind.Put, B($"newest{i}"));
        }

        return memTable;
    }

    private TableReader Build(IStorageEnvironment environment, MemTable memTable, CompressionType compression)
    {
        IStorageFile file = environment.OpenFile(_directory.Combine($"{Guid.NewGuid():N}.sst"));
        TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones, compression);
        return TableReader.Open(1, file);
    }

    private static void AssertSame(List<Entry> expected, List<Entry> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(S(expected[i].Key), S(actual[i].Key));
            Assert.Equal(expected[i].Sequence, actual[i].Sequence);
            Assert.Equal(expected[i].Kind, actual[i].Kind);
            Assert.Equal(expected[i].Value, actual[i].Value);
        }
    }
}
