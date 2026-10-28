using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// SSTables whose index and filter are partitioned under a top-level index: lookups, last keys and cursors
/// across partition boundaries read as the memtable they were built from, resident, through a cache that holds
/// nothing, and pinned; a damaged partition fails the open; and a format 4 store, whose tables have one index
/// and one filter, opens, reads, and compacts into partitioned tables.
/// </summary>
public sealed class PartitionTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [InlineData("resident")]
    [InlineData("cache of 1 byte")]
    [InlineData("pinned")]
    public void Lookups_last_keys_and_cursors_across_many_partitions_read_as_the_memtable(string caching)
    {
        MemTable model = Model();
        Cache cache = new(caching == "pinned" ? 64L * 1024 * 1024 : 1, shardBits: 0);
        TableCaching mode = caching == "resident" ? TableCaching.Resident : new TableCaching(cache, Cache.NewOwner(), IndexAndFilter: true, caching == "pinned");

        using TableReader table = Build(model, partitionSize: 60, mode);
        Assert.True(table.PartitionCount > 10, $"{table.PartitionCount} partitions");

        // Every key at snapshots below, inside and above its versions, keys between the written ones, and past both ends.
        List<string> probes = [];
        for (int i = 0; i <= 400; i++)
            probes.AddRange([$"key{i:D4}", $"key{i:D4}x"]);
        probes.AddRange(["", "a", "zzz"]);

        foreach (string probe in probes)
        {
            foreach (long snapshot in new long[] { 1, 150, 300, 450, 700, 900, long.MaxValue })
            {
                bool inModel = model.TryGetNewest(B(probe), snapshot, true, out Entry expected);
                bool inTable = table.TryGetNewest(B(probe), snapshot, true, out Entry actual);
                Assert.Equal(inModel, inTable);
                if (inModel)
                {
                    Assert.Equal(expected.Sequence, actual.Sequence);
                    Assert.Equal(expected.Kind, actual.Kind);
                    Assert.Equal(S(expected.Value), S(actual.Value));
                }
            }

            Assert.Equal(S(model.LastKeyBefore(B(probe), true)), S(table.LastKeyBefore(B(probe), true)));

            IEntryCursor expectedCursor = model.NewCursor(true);
            IEntryCursor actualCursor = table.NewCursor(true);
            expectedCursor.Seek(B(probe));
            actualCursor.Seek(B(probe));
            for (int step = 0; step < 3 && expectedCursor.Valid; step++)
            {
                Assert.True(actualCursor.Valid);
                Assert.Equal(S(expectedCursor.Current.Key), S(actualCursor.Current.Key));
                Assert.Equal(expectedCursor.Current.Sequence, actualCursor.Current.Sequence);
                expectedCursor.Next();
                actualCursor.Next();
            }

            Assert.Equal(expectedCursor.Valid, actualCursor.Valid);
        }

        Assert.Equal(S(model.LastKeyBefore(null, true)), S(table.LastKeyBefore(null, true)));

        // A full scan sees every version in internal key order.
        IEntryCursor all = table.NewCursor(true);
        int count = 0;
        Entry? previous = null;
        for (all.SeekToFirst(); all.Valid; all.Next())
        {
            if (previous is { } before)
                Assert.True(InternalKey.Compare(before.Key, before.Sequence, all.Current.Key, all.Current.Sequence) < 0);

            previous = all.Current;
            count++;
        }

        Assert.Equal(model.EntryCount, count);
        if (caching == "pinned")
            Assert.Equal(cache.Usage, cache.PinnedUsage);
        else if (caching == "cache of 1 byte")
            Assert.True(cache.InsertCount > table.PartitionCount, "the lookups did not read the evicted partitions again");
    }

    [Fact]
    public void Every_partition_of_a_pinned_table_stays_until_it_is_unpinned_and_the_table_leaves_nothing_at_close()
    {
        Cache cache = new(64L * 1024 * 1024, shardBits: 0);
        TableReader table = Build(Model(), partitionSize: 60, new TableCaching(cache, Cache.NewOwner(), IndexAndFilter: true, Pin: true));

        Assert.True(table.PartitionCount > 10);
        long usage = cache.Usage;
        Assert.True(usage > 0);
        Assert.Equal(usage, cache.PinnedUsage);

        // Unpinned, the partitions stay until the cache needs room; a cache of nothing evicts them all, and a
        // lookup reads its partition again.
        table.Unpin();
        Assert.False(table.IsPinned);
        Assert.Equal(0, cache.PinnedUsage);
        Assert.Equal(usage, cache.Usage);
        cache.SetCapacity(0);
        Assert.Equal(0, cache.Usage);
        cache.SetCapacity(64L * 1024 * 1024);
        long inserts = cache.InsertCount;
        Assert.True(table.TryGetNewest(B("key0200"), long.MaxValue, true, out _));
        Assert.True(table.TryGetNewest(B("key0399"), long.MaxValue, true, out _));
        Assert.Equal(inserts + 2, cache.InsertCount);

        // Closing erases every partition it cached, not only the first.
        table.Dispose();
        Assert.Equal(0, cache.Usage);
    }

    [Theory]
    [InlineData("filter")]
    [InlineData("index")]
    public void A_damaged_partition_fails_the_open_naming_the_table_and_the_offset(string block)
    {
        string path = _directory.Combine("partitioned.sst");
        long offset;
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
        {
            TableBuilder builder = new(file, CompressionType.None, default, partitionSize: 60);
            foreach (Entry entry in Model().Entries())
                builder.Add(entry);

            builder.Finish([]);
            using TableReader table = TableReader.Open(1, file);
            (long indexOffset, long filterOffset) = table.PartitionOffsets(5);
            offset = block == "filter" ? filterOffset : indexOffset;
        }

        byte[] bytes = File.ReadAllBytes(path);
        bytes[offset + 6] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        using IStorageFile damaged = DefaultStorageEnvironment.Instance.OpenFile(path);
        SolariumException e = Assert.Throws<SolariumException>(() => TableReader.Open(1, damaged));
        Assert.Contains(TableFormat.FileName(1), e.Message, StringComparison.Ordinal);
        Assert.Contains($"offset {offset}", e.Message, StringComparison.Ordinal);
    }

    // A store that the last release before partitioned indexes wrote: format 4, LZ4 version 3 tables in level 6
    // and level 0 of the default family, a range delete, a second family with one table, and rows in its WAL
    // alone. It opens, moves to the current format, reads, a merge rewrites the default family's tables as version
    // 4 tables while the other family's one table moves as it is, and it reads again after a reopen.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_format_4_store_opens_reads_and_compacts_into_partitioned_tables(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Directory.CreateDirectory(DbPath);
        foreach (string file in Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Fixtures", "format-4")))
            File.Copy(file, Path.Combine(DbPath, Path.GetFileName(file)));

        Assert.All(Directory.GetFiles(DbPath, "*.sst"), file => Assert.Equal(TableFormat.Version3, FooterVersion(file)));
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Compression = CompressionType.Lz4, DisableAutoCompactions = true },
        };
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            Assert.Equal("solarium-format 5", File.ReadAllText(Path.Combine(DbPath, "FORMAT")));
            Assert.All(db.DefaultColumnFamily.Version.Tables, table => Assert.Equal(1, table.PartitionCount));
            AssertFixture(db);

            ColumnFamily other = db.GetColumnFamily("other");
            db.Flush();
            db.CompactRange(null, null);
            db.CompactRange(null, null, other);
            AssertFixture(db);

            // The default family's tables overlap, so the merge rewrote them; the other family's one table moved as it is.
            Assert.All(db.DefaultColumnFamily.Version.Tables, table => Assert.Equal(TableFormat.Version, FooterVersion(Path.Combine(DbPath, TableFormat.FileName(table.Number)))));
            TableReader moved = Assert.Single(other.Version.Tables);
            Assert.Equal(TableFormat.Version3, FooterVersion(Path.Combine(DbPath, TableFormat.FileName(moved.Number))));
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, options, ["other"]);
        AssertFixture(reopened);
    }

    public void Dispose() => _directory.Dispose();

    // 400 keys of one to three versions, every tenth with a delete on top, and key0200 with 400 versions, so its
    // versions span blocks and partitions.
    private static MemTable Model()
    {
        MemTable model = new();
        long sequence = 1;
        for (int i = 0; i < 400; i++)
        {
            string key = $"key{i:D4}";
            int versions = key == "key0200" ? 400 : 1 + (i % 3);
            for (int v = 0; v < versions; v++)
                model.Add(B(key), sequence++, OperationKind.Put, B($"{key}:{v}:" + new string('v', 120)));

            if (i % 10 == 0)
                model.Add(B(key), sequence++, OperationKind.Delete, null);
        }

        return model;
    }

    private TableReader Build(MemTable model, int partitionSize, TableCaching caching)
    {
        IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(_directory.Combine($"{Guid.NewGuid():N}.sst"));
        TableBuilder builder = new(file, CompressionType.None, default, partitionSize);
        foreach (Entry entry in model.Entries())
            builder.Add(entry);

        builder.Finish([]);
        return TableReader.Open(1, file, caching);
    }

    private static uint FooterVersion(string file)
    {
        byte[] bytes = File.ReadAllBytes(file);
        return BitConverter.ToUInt32(bytes, bytes.Length - TableFormat.FooterSize + 48);
    }

    private static void AssertFixture(SolariumDb db)
    {
        for (int i = 0; i < 2000; i++)
        {
            string key = $"k{i:D4}";
            string? expected = i is >= 500 and < 600 ? null
                : i == 1000 ? "wal:1000"
                : i < 200 ? $"zero:{i:D4}"
                : $"six:{i:D4}:" + new string('a', 100);
            Assert.Equal(expected, S(db.Get(B(key))));
        }

        Assert.Equal("wal:2000", S(db.Get(B("k2000"))));
        Assert.Equal("other:0042", S(db.Get(B("o0042"), db.GetColumnFamily("other"))));

        using Iterator iterator = db.NewIterator();
        int count = 0;
        for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
            count++;

        Assert.Equal(2000 - 100 + 1, count);
    }
}
