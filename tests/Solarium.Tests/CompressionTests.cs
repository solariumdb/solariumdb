using System.IO.Hashing;
using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// <see cref="ColumnFamilyOptions.Compression"/>: data blocks stored as LZ4 read back through every read
/// path, a block that does not shrink is stored as it is, a family can switch compression on and off,
/// a store of format 3 opens and compacts into compressed tables, and damage to a compressed block is
/// reported as corruption.
/// </summary>
public sealed class CompressionTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // Values that compress, values that do not, and empty ones, across level 6, level 0 and the
    // memtable, with deletes and a range delete between them.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Compressed_tables_read_back_through_every_read_path_and_after_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Options(scope, CompressionType.Lz4);
        SortedDictionary<string, string> expected = new(StringComparer.Ordinal);
        Random random = new(7);
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 400; i++)
                {
                    string key = $"k{random.Next(1000):D4}";
                    string value = (i % 3) switch
                    {
                        0 => $"{round}:" + new string((char)('a' + (i % 26)), 200),
                        1 => $"{round}:" + Convert.ToHexString(RandomBytes(random, 100)),
                        _ => "",
                    };
                    db.Put(B(key), B(value));
                    expected[key] = value;
                }

                for (int i = 0; i < 20; i++)
                {
                    string key = $"k{random.Next(1000):D4}";
                    db.Delete(B(key));
                    expected.Remove(key);
                }

                db.Flush();
                if (round == 0)
                    db.CompactRange(null, null);
            }

            db.DeleteRange(B("k0100"), B("k0200"));
            foreach (string key in expected.Keys.Where(k => string.CompareOrdinal(k, "k0100") >= 0 && string.CompareOrdinal(k, "k0200") < 0).ToList())
                expected.Remove(key);

            db.Put(B("k0150"), B("memtable"));
            expected["k0150"] = "memtable";
            Assert.NotEmpty(db.DefaultColumnFamily.Version.Level6.Tables);
            Assert.NotEmpty(db.DefaultColumnFamily.Version.Level0);
            AssertReads(db, expected);
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, options);
        AssertReads(reopened, expected);
        reopened.CompactRange(null, null);
        AssertReads(reopened, expected);
    }

    // The same rows make a smaller table with LZ4 when they repeat, and the same bytes when they do
    // not: a block that compression would shrink by less than an eighth is stored as it is.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Blocks_that_compress_shrink_and_blocks_that_do_not_are_stored_as_they_are(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Random random = new(3);
        MemTable repeating = new();
        MemTable incompressible = new();
        for (int i = 0; i < 500; i++)
        {
            repeating.Add(B($"key{i:D4}"), i + 1, OperationKind.Put, B(new string('v', 300)));
            incompressible.Add(B($"key{i:D4}"), i + 1, OperationKind.Put, RandomBytes(random, 300));
        }

        Assert.True(Build(scope, repeating, CompressionType.Lz4).Length * 4 < Build(scope, repeating, CompressionType.None).Length);
        Assert.Equal(Build(scope, incompressible, CompressionType.None), Build(scope, incompressible, CompressionType.Lz4));
    }

    // Each table keeps how it was written, so a family reads what it wrote before the option changed,
    // and a compaction rewrites both kinds into the current one.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_family_that_switches_compression_on_and_off_reads_its_older_tables(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SortedDictionary<string, string> expected = new(StringComparer.Ordinal);
        CompressionType[] steps = [CompressionType.None, CompressionType.Lz4, CompressionType.None, CompressionType.Lz4];
        for (int step = 0; step < steps.Length; step++)
        {
            using SolariumDb db = SolariumDb.Open(DbPath, Options(scope, steps[step]));
            AssertReads(db, expected);
            for (int i = 0; i < 200; i++)
            {
                string key = $"k{(step * 50) + i:D4}";
                string value = $"{step}:" + new string('x', 100);
                db.Put(B(key), B(value));
                expected[key] = value;
            }

            db.Flush();
            AssertReads(db, expected);
            if (step == steps.Length - 1)
            {
                db.CompactRange(null, null);
                AssertReads(db, expected);
            }
        }
    }

    // A store that the last release before compression wrote: format 3, version 2 tables in level 6
    // and level 0 of two families, and rows in its WAL alone. It opens, moves to the current format, reads, and
    // a merge rewrites the default family's tables as compressed, partitioned tables; the other family's
    // one table moves to level 6 as it is, still version 2, and still reads.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_format_3_store_opens_reads_and_compacts_into_compressed_tables(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Directory.CreateDirectory(DbPath);
        foreach (string file in Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Fixtures", "format-3")))
            File.Copy(file, Path.Combine(DbPath, Path.GetFileName(file)));

        Assert.All(Directory.GetFiles(DbPath, "*.sst"), file => Assert.Equal(TableFormat.Version2, FooterVersion(file)));
        long before = Directory.GetFiles(DbPath, "*.sst").Sum(file => new FileInfo(file).Length);
        DbOptions options = Options(scope, CompressionType.Lz4) with { CreateMissingColumnFamilies = false };
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            Assert.Equal("solarium-format 5", File.ReadAllText(Path.Combine(DbPath, "FORMAT")));
            AssertFixture(db);
            ColumnFamily other = db.GetColumnFamily("other");
            db.CompactRange(null, null);
            db.CompactRange(null, null, other);
            AssertFixture(db);

            TableReader[] merged = [.. db.DefaultColumnFamily.Version.Level0, .. db.DefaultColumnFamily.Version.Level6.Tables];
            Assert.NotEmpty(merged);
            Assert.All(merged, table => Assert.Equal(TableFormat.Version, FooterVersion(Path.Combine(DbPath, TableFormat.FileName(table.Number)))));
            TableReader moved = Assert.Single(other.Version.Level6.Tables);
            Assert.Equal(TableFormat.Version2, FooterVersion(Path.Combine(DbPath, TableFormat.FileName(moved.Number))));
        }

        Assert.True(Directory.GetFiles(DbPath, "*.sst").Sum(file => new FileInfo(file).Length) < before);
        using SolariumDb reopened = SolariumDb.Open(DbPath, options, ["other"]);
        AssertFixture(reopened);
    }

    // A compressed block whose checksum is valid but whose type, raw size or LZ4 bytes are wrong:
    // every read path reports corruption with the table, the block and its offset, and nothing else.
    [Theory]
    [InlineData("type")]
    [InlineData("negative size")]
    [InlineData("huge size")]
    [InlineData("larger size")]
    [InlineData("smaller size")]
    [InlineData("stream")]
    public void A_compressed_block_whose_header_or_stream_is_wrong_is_corruption(string damage)
    {
        MemTable memTable = new();
        for (int i = 0; i < 30; i++)
            memTable.Add(B($"key{i:D3}"), i + 1, OperationKind.Put, B(new string('v', 100)));

        string path = _directory.Combine("damaged.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
            TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones, CompressionType.Lz4);

        // One data block, from the start of the file to the range tombstone block.
        byte[] bytes = File.ReadAllBytes(path);
        int size = (int)BitConverter.ToInt64(bytes, bytes.Length - TableFormat.FooterSize);
        Assert.Equal(TableFormat.Lz4Block, bytes[size - 5]);
        int raw = BitConverter.ToInt32(bytes, 0);
        switch (damage)
        {
            case "type":
                bytes[size - 5] = 7;
                break;
            case "negative size":
                BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), -1);
                break;
            case "huge size":
                BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), int.MaxValue);
                break;
            case "larger size":
                BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), raw + 1);
                break;
            case "smaller size":
                BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), raw - 1);
                break;
            case "stream":
                bytes.AsSpan(4, size - 9).Fill(0xFF);
                break;
        }

        BitConverter.TryWriteBytes(bytes.AsSpan(size - 4, 4), Crc32.HashToUInt32(bytes.AsSpan(0, size - 4)));
        File.WriteAllBytes(path, bytes);

        using TableReader table = TableReader.Open(1, DefaultStorageEnvironment.Instance.OpenFile(path));
        Exception?[] failures =
        [
            Record.Exception(() => table.TryGetNewest(B("key005"), 100, true, out _)),
            Record.Exception(() => table.NewCursor(true).SeekToFirst()),
            Record.Exception(() => table.LastKeyBefore(B("key010"), true)),
        ];
        foreach (Exception? failure in failures)
        {
            SolariumException e = Assert.IsType<SolariumException>(failure);
            Assert.Contains("000001.sst", e.Message);
            Assert.Contains("data block 0 at offset 0", e.Message);
        }
    }

    [Fact]
    public void An_undefined_compression_is_refused_at_open()
    {
        DbOptions options = new() { DefaultColumnFamilyOptions = new ColumnFamilyOptions { Compression = (CompressionType)9 } };
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, options));
    }

    // k000-k099 in level 6, k000-k019 overwritten, k020 deleted and k030-k039 range deleted in level
    // 0, k050 and k100 in the WAL, and o1 in the other family.
    private static void AssertFixture(SolariumDb db)
    {
        SortedDictionary<string, string> expected = new(StringComparer.Ordinal);
        for (int i = 0; i < 100; i++)
            expected[$"k{i:D3}"] = i < 20 ? $"zero:{i:D3}" : $"six:{i:D3}:" + new string('a', 64);

        expected.Remove("k020");
        for (int i = 30; i < 40; i++)
            expected.Remove($"k{i:D3}");

        expected["k050"] = "wal:050";
        expected["k100"] = "wal:100";
        AssertReads(db, expected);
        Assert.Equal("other-table", S(db.Get(B("o1"), db.GetColumnFamily("other"))));
    }

    // Get, MultiGet, and an iterator forward from the first key, backward from the last, and from a
    // Seek to each fifth key.
    private static void AssertReads(SolariumDb db, SortedDictionary<string, string> expected)
    {
        foreach ((string key, string value) in expected)
            Assert.Equal(value, S(db.Get(B(key))));

        Assert.Null(db.Get(B("absent")));
        List<string> keys = [.. expected.Keys, "absent"];
        byte[]?[] values = db.MultiGet([.. keys.Select(B)]);
        for (int i = 0; i < keys.Count; i++)
            Assert.Equal(expected.GetValueOrDefault(keys[i]), values[i] is null ? null : S(values[i]));

        List<string> rows = [.. expected.Select(pair => $"{pair.Key}={pair.Value}")];
        using Iterator iterator = db.NewIterator();
        List<string> forward = [];
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            forward.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        Assert.Equal(rows, forward);
        List<string> backward = [];
        for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
            backward.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        backward.Reverse();
        Assert.Equal(rows, backward);
        List<string> ordered = [.. expected.Keys];
        for (int i = 0; i < ordered.Count; i += 5)
        {
            iterator.Seek(B(ordered[i]));
            Assert.True(iterator.Valid());
            Assert.Equal(ordered[i], S(iterator.Key()));
        }
    }

    private static DbOptions Options(EnvironmentScope scope, CompressionType compression) =>
        scope.Options(createMissingColumnFamilies: true) with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Compression = compression, DisableAutoCompactions = true },
        };

    private byte[] Build(EnvironmentScope scope, MemTable memTable, CompressionType compression)
    {
        string path = _directory.Combine($"{Guid.NewGuid():N}.sst");
        using (IStorageFile file = scope.Environment.OpenFile(path))
            TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones, compression);

        return File.ReadAllBytes(path);
    }

    private static uint FooterVersion(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        return BitConverter.ToUInt32(bytes, bytes.Length - TableFormat.FooterSize + 48);
    }

    private static byte[] RandomBytes(Random random, int count)
    {
        byte[] bytes = new byte[count];
        random.NextBytes(bytes);
        return bytes;
    }
}
