using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Damage that every block's own checksum passes: a whole, valid block that a misdirected write put at another
/// block's place, of the same table or of another, and a whole table file that another valid one replaced. Each
/// read reports corruption, or the open refuses the store; none answers with a missing key or another key's value.
/// </summary>
public sealed class MisplacedBlockTests : IDisposable
{
    // 200 entries of 4 + 5 + 13 + 100 bytes: 33 to a block, so every data block but the last has the same size.
    private const int Keys = 200;
    private const int ValueSize = 100;

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // The checksum of a version 5 table binds each block to its offset, so two blocks that trade places fail it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Two_equal_sized_data_blocks_swapped_in_a_table_fail_every_read_as_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteStore(scope, DbPath, "a");
        byte[] bytes = File.ReadAllBytes(table);
        List<(int Offset, int Size)> blocks = TableBytes.DataBlocks(bytes);
        Assert.Equal(blocks[0].Size, blocks[1].Size);
        Swap(bytes, blocks[0], blocks[1]);
        File.WriteAllBytes(table, bytes);

        using SolariumDb db = SolariumDb.Open(DbPath, Options(scope));
        AssertEveryReadIsCorruption(db, B("a0005"), "bad checksum");
        AssertEveryReadIsCorruption(db, B("a0040"), "bad checksum");
    }

    // A table whose checksums do not bind a block to its place, as a table before version 5: the blocks pass their
    // checksums at their new places, and the check of each block against its index entry finds them.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Two_data_blocks_swapped_with_checksums_valid_at_their_new_places_fail_every_read_as_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteStore(scope, DbPath, "a");
        byte[] bytes = File.ReadAllBytes(table);
        List<(int Offset, int Size)> blocks = TableBytes.DataBlocks(bytes);
        Swap(bytes, blocks[1], blocks[2]);
        TableBytes.Reseal(bytes, blocks[1].Offset, blocks[1].Size);
        TableBytes.Reseal(bytes, blocks[2].Offset, blocks[2].Size);
        File.WriteAllBytes(table, bytes);

        using SolariumDb db = SolariumDb.Open(DbPath, Options(scope));
        AssertEveryReadIsCorruption(db, B("a0040"), "another place's data");
        AssertEveryReadIsCorruption(db, B("a0070"), "another place's data");

        // The blocks around them are still where the index puts them, and read.
        Assert.Equal(Value("a", 5), S(db.Get(B("a0005"))));
        Assert.Equal(Value("a", 150), S(db.Get(B("a0150"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_data_block_copied_from_another_table_fails_every_read_as_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteStore(scope, DbPath, "a");
        string other = WriteStore(scope, _directory.Combine("other"), "b");
        byte[] bytes = File.ReadAllBytes(table);
        byte[] source = File.ReadAllBytes(other);
        (int offset, int size) = TableBytes.DataBlocks(bytes)[0];
        Assert.Equal((offset, size), TableBytes.DataBlocks(source)[0]);
        source.AsSpan(offset, size).CopyTo(bytes.AsSpan(offset));
        File.WriteAllBytes(table, bytes);

        using (SolariumDb db = SolariumDb.Open(DbPath, Options(scope)))
            AssertEveryReadIsCorruption(db, B("a0005"), "bad checksum");

        // With a checksum valid here, as a table before version 5 would have, its keys still give it away.
        TableBytes.Reseal(bytes, offset, size);
        File.WriteAllBytes(table, bytes);
        using SolariumDb resealed = SolariumDb.Open(DbPath, Options(scope));
        AssertEveryReadIsCorruption(resealed, B("a0005"), "another place's data");
        Assert.Null(Record.Exception(() => resealed.Get(B("b0005"))));
    }

    // Every block of the older table passes its checksum, so only the manifest's record of the table finds it; the
    // open refuses the store, and leaves the table files as they are.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_table_file_replaced_by_an_older_table_of_the_same_size_is_refused_at_open(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(scope)))
        {
            Fill(db, "a");
            db.Flush();
            Fill(db, "b");
            db.Flush();
        }

        string[] tables = [.. Directory.GetFiles(DbPath, "*.sst").Order(StringComparer.Ordinal)];
        Assert.Equal(2, tables.Length);
        Assert.Equal(new FileInfo(tables[0]).Length, new FileInfo(tables[1]).Length);
        File.Copy(tables[0], tables[1], overwrite: true);

        AssertOpenRefuses(scope, tables[1], "table id");
        Assert.Equal(File.ReadAllBytes(tables[0]), File.ReadAllBytes(tables[1]));
    }

    // Two stores that took the same writes have tables of the same numbers, sizes, keys and sequence numbers: the
    // table id alone tells one's table from the other's.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_table_file_replaced_by_the_same_numbered_table_of_another_store_is_refused_at_open(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteStore(scope, DbPath, "a");
        string twin = WriteStore(scope, _directory.Combine("twin"), "a");
        Assert.Equal(Path.GetFileName(table), Path.GetFileName(twin));
        Assert.Equal(new FileInfo(table).Length, new FileInfo(twin).Length);
        File.Copy(twin, table, overwrite: true);

        AssertOpenRefuses(scope, table, "table id");
    }

    // A table written before tables had ids, in a store of format 3: its blocks have plain checksums, which two
    // blocks that trade places pass, and a read that the index sends to either one reports corruption instead of
    // answering "not found" or with the other block's rows.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Two_data_blocks_swapped_in_a_table_without_ids_fail_every_read_as_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Directory.CreateDirectory(DbPath);
        foreach (string file in Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Fixtures", "format-3")))
            File.Copy(file, Path.Combine(DbPath, Path.GetFileName(file)));

        // The level 6 table of the default family: k000 to k044 in its first block, k045 to k089 in its second, of
        // the same size. Level 0 holds newer versions of k000 to k019, and the WAL one of k050.
        string table = Path.Combine(DbPath, "000002.sst");
        byte[] bytes = File.ReadAllBytes(table);
        Assert.Equal(TableFormat.Version2, TableBytes.Version(bytes));
        Swap(bytes, (0, 4189), (4189, 4189));
        File.WriteAllBytes(table, bytes);

        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { DisableAutoCompactions = true } };
        using SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]);
        AssertEveryReadIsCorruption(db, B("k025"), "another place's data");
        AssertEveryReadIsCorruption(db, B("k060"), "another place's data");
        Assert.Equal("zero:005", S(db.Get(B("k005"))));
        Assert.Equal("wal:050", S(db.Get(B("k050"))));
    }

    // A store that the last release before table ids wrote: format 5, a version 3 manifest, LZ4 version 4 tables in
    // level 6 and level 0 of the default family, a range delete, a second family with one table, and rows in its WAL
    // alone. It opens and reads with its tables unchecked by id; a merge rewrites the default family's tables as
    // version 5 tables, whose ids the manifest records, while the other family's table moves as it is, and the store
    // reads again after a reopen, which checks those ids.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_format_5_store_opens_reads_and_compacts_into_tables_whose_ids_the_manifest_records(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Directory.CreateDirectory(DbPath);
        foreach (string file in Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Fixtures", "format-5")))
            File.Copy(file, Path.Combine(DbPath, Path.GetFileName(file)));

        Assert.All(Directory.GetFiles(DbPath, "*.sst"), file => Assert.Equal(TableFormat.Version4, TableBytes.Version(file)));
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Compression = CompressionType.Lz4, DisableAutoCompactions = true },
        };
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            Assert.Equal("solarium-format 6", File.ReadAllText(Path.Combine(DbPath, "FORMAT")));
            AssertFormat5Fixture(db);
            db.Flush();
            db.CompactRange(null, null);
            db.CompactRange(null, null, db.GetColumnFamily("other"));
            AssertFormat5Fixture(db);
        }

        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!;
        foreach (ManifestTable record in manifest.ColumnFamilies.SelectMany(family => family.Tables))
        {
            byte[] bytes = File.ReadAllBytes(Path.Combine(DbPath, TableFormat.FileName(record.Number)));
            Assert.Equal(TableBytes.Id(bytes), record.Id);
            Assert.Equal(TableBytes.Version(bytes) == TableFormat.Version, record.Id != 0);
        }

        ManifestTable moved = Assert.Single(manifest.ColumnFamilies.Single(family => family.Name == "other").Tables);
        Assert.Equal(0UL, moved.Id);
        Assert.Contains(manifest.ColumnFamilies.Single(family => family.Name == ColumnFamily.DefaultName).Tables, record => record.Id != 0);

        using SolariumDb reopened = SolariumDb.Open(DbPath, options, ["other"]);
        AssertFormat5Fixture(reopened);
    }

    // A block whose last entry is the one its index entry holds, but whose first is not after the block before it,
    // holds keys the index sends elsewhere.
    [Fact]
    public void A_data_block_whose_first_key_is_not_after_the_block_before_it_is_corruption()
    {
        string path = _directory.Combine("table.sst");
        MemTable memTable = new();
        for (int i = 0; i < Keys; i++)
            memTable.Add(B($"a{i:D4}"), i + 1, OperationKind.Put, B(Value("a", i)));

        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
            TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones);

        // The first key of block 1, made the first key of block 0: [u32 key length][key] starts the block.
        byte[] bytes = File.ReadAllBytes(path);
        List<(int Offset, int Size)> blocks = TableBytes.DataBlocks(bytes);
        B("a0000").CopyTo(bytes.AsSpan(blocks[1].Offset + 4));
        TableBytes.Reseal(bytes, blocks[1].Offset, blocks[1].Size);
        File.WriteAllBytes(path, bytes);

        using TableReader table = TableReader.Open(1, DefaultStorageEnvironment.Instance.OpenFile(path));
        SolariumException get = Assert.Throws<SolariumException>(() => table.TryGetNewest(B("a0040"), long.MaxValue, true, out _));
        Assert.True(get.IsCorruption);
        Assert.Contains($"data block 1 at offset {blocks[1].Offset} of index partition 0 starts at or before", get.Message);

        IEntryCursor cursor = table.NewCursor(true);
        cursor.SeekToFirst();
        Assert.Contains("starts at or before", Assert.Throws<SolariumException>(() =>
        {
            while (cursor.Valid)
                cursor.Next();
        }).Message);

        Assert.True(table.TryGetNewest(B("a0005"), long.MaxValue, true, out Entry entry));
        Assert.Equal(Value("a", 5), S(entry.Value));
    }

    // A version 5 data block names its last entry, so the check against its index entry decodes that entry
    // alone: a name that is not the block's last entry, under a checksum valid for it, is damage too.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_data_block_that_names_another_entry_than_its_last_fails_every_read_as_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteStore(scope, DbPath, "a");
        byte[] original = File.ReadAllBytes(table);
        (int offset, int size) = TableBytes.DataBlocks(original)[1];
        int payload = size - TableFormat.DataTrailerSize;
        const int Entry = 4 + 5 + 13 + ValueSize;
        Assert.Equal(payload - Entry, BitConverter.ToInt32(original, offset + payload));

        (int LastEntry, string Detail)[] cases =
        [
            (payload - (2 * Entry), "but an entry there does not end the block"),
            (0, "names its first entry as its last"),
            (payload, "not between the end of its first entry"),
            (Entry - 1, "not between the end of its first entry"),
            (payload - Entry + 1, "does not decode"),
            (-1, "names its last entry at -1"),
        ];

        foreach ((int lastEntry, string detail) in cases)
        {
            byte[] bytes = (byte[])original.Clone();
            BitConverter.TryWriteBytes(bytes.AsSpan(offset + payload, 4), lastEntry);
            TableBytes.Reseal(bytes, offset, size);
            File.WriteAllBytes(table, bytes);

            using SolariumDb db = SolariumDb.Open(DbPath, Options(scope));
            AssertEveryReadIsCorruption(db, B("a0040"), detail);
            Assert.Equal(Value("a", 5), S(db.Get(B("a0005"))));
        }
    }

    // The corruption flag that tells damage apart from a failure of the storage, which the next open clears.
    [Fact]
    public void Corruption_is_flagged_and_a_storage_failure_is_not()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using (SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks }))
        {
            hooks.FailWrites = true;
            SolariumException failed = Assert.Throws<SolariumException>(() => db.Put(B("k"), B("v")));
            Assert.False(failed.IsCorruption);
            hooks.FailWrites = false;
            Assert.False(Assert.Throws<SolariumException>(() => db.Put(B("k"), B("v"))).IsCorruption);
        }

        byte[] manifest = File.ReadAllBytes(Path.Combine(DbPath, Manifest.FileName));
        manifest[^1] ^= 0xFF;
        File.WriteAllBytes(Path.Combine(DbPath, Manifest.FileName), manifest);
        Assert.True(Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath)).IsCorruption);
    }

    // Get twice (a read that fills the cache must not cache the block), MultiGet, and an iterator forward through
    // every key, backward through every key, and from a Seek to the key: each one throws a corruption whose message
    // has `detail`.
    private static void AssertEveryReadIsCorruption(SolariumDb db, byte[] key, string detail)
    {
        Action[] reads =
        [
            () => db.Get(key),
            () => db.Get(key),
            () => db.MultiGet([key]),
            () =>
            {
                using Iterator iterator = db.NewIterator();
                for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                {
                }
            },
            () =>
            {
                using Iterator iterator = db.NewIterator();
                for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
                {
                }
            },
            () =>
            {
                using Iterator iterator = db.NewIterator();
                iterator.Seek(key);
            },
        ];

        foreach (Action read in reads)
        {
            SolariumException e = Assert.Throws<SolariumException>(read);
            Assert.True(e.IsCorruption);
            Assert.Contains(detail, e.Message);
        }
    }

    // What the format-5 fixture holds: k0000 to k1999 in level 6, newer versions of k0000 to k0199 in level 0, a range
    // delete of k0500 to k0599, k1000 and k2000 in the WAL alone, and o0000 to o0099 in the other family.
    private static void AssertFormat5Fixture(SolariumDb db)
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

    private void AssertOpenRefuses(EnvironmentScope scope, string table, string detail)
    {
        SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath, Options(scope)));
        Assert.True(e.IsCorruption);
        Assert.Contains($"{table} of column family 'default' is not the table the manifest records", e.Message);
        Assert.Contains(detail, e.Message);
    }

    // A store at path with one table of the keys prefix0000 to prefix0199; returns the table's path.
    private static string WriteStore(EnvironmentScope scope, string path, string prefix)
    {
        using (SolariumDb db = SolariumDb.Open(path, Options(scope)))
        {
            Fill(db, prefix);
            db.Flush();
        }

        return Assert.Single(Directory.GetFiles(path, "*.sst"));
    }

    private static void Fill(SolariumDb db, string prefix)
    {
        for (int i = 0; i < Keys; i++)
            db.Put(B($"{prefix}{i:D4}"), B(Value(prefix, i)));
    }

    private static string Value(string prefix, int i) => $"{prefix}{i:D4}".PadRight(ValueSize, 'v');

    private static void Swap(byte[] bytes, (int Offset, int Size) first, (int Offset, int Size) second)
    {
        Assert.Equal(first.Size, second.Size);
        byte[] saved = bytes.AsSpan(first.Offset, first.Size).ToArray();
        bytes.AsSpan(second.Offset, second.Size).CopyTo(bytes.AsSpan(first.Offset));
        saved.CopyTo(bytes.AsSpan(second.Offset));
    }

    // Data blocks cached, so a read that fills the cache meets the damage too.
    private static DbOptions Options(EnvironmentScope scope) => scope.Options() with
    {
        DefaultColumnFamilyOptions = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            BlockCache = new Cache(1 << 20),
            CacheDataBlocks = true,
        },
    };
}
