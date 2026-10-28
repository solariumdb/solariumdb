using System.Text;
using Solarium.Engine;
using Solarium.IoUring;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Levels 1 to 5: the base level and the level targets, merges into a level above the bottom that
/// keep the deletes and range tombstones a lower level still needs, range tombstones cut between the
/// output tables, moves at every level, whole-file drops, manual compactions that reach the bottom,
/// and stores written with level 6 only.
/// </summary>
public sealed class LevelTests : IDisposable
{
    private const long Megabyte = 1024L * 1024;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static readonly Comparer<byte[]> KeyOrder = Comparer<byte[]>.Create((a, b) => a.AsSpan().SequenceCompareTo(b));

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    public static TheoryData<string, bool> EnvironmentsAndNewerPuts()
    {
        TheoryData<string, bool> data = [];
        foreach (string name in IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
        {
            data.Add(name, true);
            data.Add(name, false);
        }

        return data;
    }

    [Fact]
    public void The_base_level_rises_as_the_largest_level_grows_as_rockdb_sizes_its_levels()
    {
        ColumnFamilyOptions options = new();

        // Nothing sorted, or less than MaxBytesForLevelBase: level 0 compacts into level 6.
        Assert.Equal(6, Targets(options, (6, 0)).BaseLevel);
        Assert.Equal(6, Targets(options, (6, 10 * Megabyte)).BaseLevel);
        Assert.Equal(6, Targets(options, (6, 256 * Megabyte)).BaseLevel);

        // 300 MB in level 6: level 5 is the base level, with a target of MaxBytesForLevelBase.
        LevelTargets small = Targets(options, (6, 300 * Megabyte));
        Assert.Equal(5, small.BaseLevel);
        Assert.Equal(256 * Megabyte, small.MaxBytes(5));
        Assert.Equal(long.MaxValue, small.MaxBytes(4));

        // 4.2 GB: level 6's size over 10 for level 5, and level 4 as the base level.
        LevelTargets large = Targets(options, (6, 4300 * Megabyte));
        Assert.Equal(4, large.BaseLevel);
        Assert.Equal(256 * Megabyte, large.MaxBytes(4));
        Assert.Equal(430 * Megabyte, large.MaxBytes(5), 1 * Megabyte);

        // 100 GB: level 3, with levels 4 and 5 at a tenth of the level below.
        LevelTargets huge = Targets(options, (6, 100 * 1024 * Megabyte));
        Assert.Equal(3, huge.BaseLevel);
        Assert.Equal(1024 * Megabyte, huge.MaxBytes(4), 1 * Megabyte);
        Assert.Equal(10 * 1024 * Megabyte, huge.MaxBytes(5), 1 * Megabyte);

        // A store that shrank keeps its base level at the first level with files.
        Assert.Equal(5, Targets(options, (5, 100 * Megabyte), (6, 10 * Megabyte)).BaseLevel);

        // The base level never rises past a level with files: level 4 has some, so it is the base.
        Assert.Equal(4, Targets(options, (4, 1 * Megabyte), (6, 1 * Megabyte)).BaseLevel);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_merge_into_a_middle_level_keeps_a_delete_while_a_lower_level_has_its_key_and_drops_it_otherwise(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
        {
            Write(db, ("a1", "old"), ("a2", "old"), ("a3", "old"));
            Write(db, ("a2", "mid"), ("z9", "mid"));
            db.Delete(B("a1"));
            db.Delete(B("a5"));
            Write(db, ("a4", "new"));
        }

        Place(DbPath, 6, 5, 4);

        // Before the second compaction, the merge of level 4 into level 5 is installed and the merge
        // into level 6 has not run.
        List<Entry> middle = [];
        Dictionary<string, string?> reads = [];
        SolariumDb? opened = null;
        int compactions = 0;
        DbOptions options = Manual(scope) with
        {
            BeforeCompaction = () =>
            {
                if (++compactions != 2)
                    return;

                foreach (TableReader table in opened!.DefaultColumnFamily.Version.Levels[5].Tables)
                    middle.AddRange(Entries(table));

                foreach (string key in new[] { "a1", "a2", "a3", "a4", "a5", "z9" })
                    reads[key] = S(opened.Get(B(key)));
            },
        };

        using (SolariumDb db = opened = SolariumDb.Open(DbPath, options))
        {
            db.CompactRange(null, null);
            Assert.Equal(2, compactions);

            // Level 6 has a1, so its delete stays; nothing below level 5 has a5, so its delete goes.
            Assert.Contains(middle, e => S(e.Key) == "a1" && e.Kind == OperationKind.Delete);
            Assert.DoesNotContain(middle, e => S(e.Key) == "a5");
            Assert.Equal(new Dictionary<string, string?> { ["a1"] = null, ["a2"] = "mid", ["a3"] = "old", ["a4"] = "new", ["a5"] = null, ["z9"] = "mid" }, reads);

            // The merge into level 6 drops every delete.
            FamilyVersion version = db.DefaultColumnFamily.Version;
            Assert.Empty(version.Level0);
            Assert.All(Enumerable.Range(1, 5), level => Assert.True(version.Levels[level].IsEmpty));
            Assert.DoesNotContain(version.Level6.Tables.SelectMany(Entries), e => e.Kind != OperationKind.Put);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
            Assert.Equal(["a2=mid", "a3=old", "a4=new", "z9=mid"], Scan(db));
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndNewerPuts))]
    public void A_range_delete_cut_between_the_tables_of_a_middle_level_still_hides_every_key_under_it(string environment, bool newerPuts)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions manual = Manual(scope, targetFileSize: 1024);
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        using (SolariumDb db = SolariumDb.Open(DbPath, manual))
        {
            for (int i = 0; i < 100; i++)
                Put(db, model, $"k{i:D3}", $"old{i}" + new string('o', 100));

            db.Flush();
            Put(db, model, "k050", "mid");
            db.Flush();

            // Every key the range delete hides lies in level 6, so the merge into level 5 must keep it.
            db.DeleteRange(B("k010"), B("k090"));
            foreach (string key in model.Keys.Where(k => string.CompareOrdinal(k, "k010") >= 0 && string.CompareOrdinal(k, "k090") < 0).ToList())
                model.Remove(key);

            if (newerPuts)
                for (int i = 20; i <= 80; i += 10)
                    Put(db, model, $"k{i:D3}", $"new{i}" + new string('n', 400));

            db.Flush();
        }

        Place(DbPath, 6, 5, 4);

        List<TableReader> middle = [];
        List<string> scanned = [];
        List<string> got = [];
        SolariumDb? opened = null;
        int compactions = 0;
        DbOptions options = manual with
        {
            BeforeCompaction = () =>
            {
                if (++compactions != 2)
                    return;

                middle.AddRange(opened!.DefaultColumnFamily.Version.Levels[5].Tables);
                scanned.AddRange(Scan(opened));
                for (int i = 0; i < 100; i++)
                    got.Add(S(opened.Get(B($"k{i:D3}"))) ?? "-");
            },
        };

        using (SolariumDb db = opened = SolariumDb.Open(DbPath, options))
        {
            db.CompactRange(null, null);
            Assert.Equal(2, compactions);

            List<string> expected = [.. model.Select(pair => $"{pair.Key}={pair.Value}")];
            Assert.Equal(expected, scanned);
            Assert.Equal(Enumerable.Range(0, 100).Select(i => model.GetValueOrDefault($"k{i:D3}") ?? "-"), got);

            // With the newer puts the merge wrote several tables, and the delete was cut between them:
            // each table has a piece, and the pieces cover the whole range with no gap.
            List<RangeTombstone> pieces = [.. middle.SelectMany(t => t.Tombstones).OrderBy(t => t.Begin, KeyOrder)];
            if (newerPuts)
            {
                Assert.True(middle.Count >= 3, $"the merge wrote {middle.Count} tables");
                Assert.All(middle, table => Assert.NotEmpty(table.Tombstones));
                Assert.All(middle.Take(middle.Count - 1), table => Assert.True(table.Properties.LargestIsExclusive));
            }
            else
            {
                TableReader only = Assert.Single(middle);
                Assert.Equal(0, only.Properties.EntryCount);
            }

            Assert.Equal("k010", S(pieces[0].Begin));
            Assert.Equal("k090", S(pieces[^1].End));
            for (int i = 1; i < pieces.Count; i++)
                Assert.Equal(S(pieces[i - 1].End), S(pieces[i].Begin));

            for (int i = 1; i < middle.Count; i++)
                Assert.False(middle[i - 1].Properties.Overlaps(middle[i].Properties));

            // The merge into level 6 drops the delete and everything it hid.
            FamilyVersion version = db.DefaultColumnFamily.Version;
            Assert.All(Enumerable.Range(1, 5), level => Assert.True(version.Levels[level].IsEmpty));
            Assert.All(version.Level6.Tables, table => Assert.Empty(table.Tombstones));
            Assert.Equal(expected, Scan(db));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, manual))
            Assert.Equal([.. model.Select(pair => $"{pair.Key}={pair.Value}")], Scan(db));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Appended_keys_move_down_through_the_levels_and_compactions_write_no_byte(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = SmallLevels() };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            for (int flush = 0; flush < 60; flush++)
            {
                for (int i = 0; i < 20; i++)
                    db.Put(B($"k{flush:D3}-{i:D2}"), B(new string((char)('a' + (flush % 26)), 100)));

                db.Flush();
            }

            WaitForCompactions(db);
            (long Moves, long MovedFiles, long Merges, long BytesRead, long BytesWritten) counts = db.CompactionCounts();
            Assert.Equal(0, counts.Merges);
            Assert.Equal(0, counts.BytesWritten);
            Assert.True(counts.Moves > 60, $"{counts.Moves} moves");

            int[] files = [.. Enumerable.Range(0, Manifest.LevelCount).Select(level => int.Parse(db.GetProperty($"rocksdb.num-files-at-level{level}")!))];
            Assert.True(files.Skip(1).Take(5).Count(count => count > 0) >= 2, $"files per level: {string.Join(", ", files)}");
            Assert.Equal(60, files.Sum());
            Assert.Equal(files.Select((_, level) => level == 0 ? db.DefaultColumnFamily.Version.Level0.Length : db.DefaultColumnFamily.Version.Levels[level].Tables.Length), files);
            Assert.True(int.Parse(db.GetProperty("rocksdb.base-level")!) < 6);
            Assert.Equal(1200, Scan(db).Count);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal(1200, Scan(db).Count);
            Assert.Equal(new string('h', 100), S(db.Get(B("k033-07"))));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Dropping_files_in_a_range_takes_whole_files_of_every_sorted_level_and_leaves_the_ones_that_straddle_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
        {
            Write(db, ("a1", "6"), ("c1", "6"));
            Write(db, ("c5", "4"), ("d5", "4"));
            Write(db, ("e1", "4"), ("f1", "4"));
            Write(db, ("g1", "2"), ("i1", "2"));
            Write(db, ("x1", "2"), ("z1", "2"));
            Write(db, ("h1", "0"));
        }

        Place(DbPath, 6, 4, 4, 2, 2, 0);

        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
        {
            db.DeleteFilesInRange(B("d"), B("i9"));

            FamilyVersion version = db.DefaultColumnFamily.Version;
            Assert.Equal(["c5"], version.Levels[4].Tables.Select(t => S(t.Properties.Smallest)));
            Assert.Equal(["x1"], version.Levels[2].Tables.Select(t => S(t.Properties.Smallest)));
            Assert.Single(version.Level6.Tables);
            Assert.Single(version.Level0);
            Assert.Equal(["a1=6", "c1=6", "c5=4", "d5=4", "h1=0", "x1=2", "z1=2"], Scan(db));
            Assert.Equal([0, 2, 4, 6], db.GetLiveFilesMetaData().Select(f => f.Level).Distinct());
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
            Assert.Equal(["a1=6", "c1=6", "c5=4", "d5=4", "h1=0", "x1=2", "z1=2"], Scan(db));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_manual_compaction_takes_deletes_from_a_middle_level_to_the_bottom_and_purges_the_values_they_hide(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
        {
            for (int i = 0; i < 10; i++)
                db.Put(B($"s{i}"), B($"SECRET-{i}-" + new string('s', 50)));

            db.Put(B("t"), B("kept"));
            db.Flush();
            for (int i = 0; i < 4; i++)
                db.Delete(B($"s{i}"));

            db.DeleteRange(B("s4"), B("s9"));
            db.Delete(B("s9"));
            db.Flush();
        }

        Place(DbPath, 6, 3);

        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(scope)))
        {
            db.CompactRange(null, null);

            FamilyVersion version = db.DefaultColumnFamily.Version;
            Assert.All(Enumerable.Range(1, 5), level => Assert.True(version.Levels[level].IsEmpty));
            Assert.Equal(["t=kept"], Scan(db));
            Assert.Equal((2L, 2L, 1L), (db.CompactionCounts().Moves, db.CompactionCounts().MovedFiles, db.CompactionCounts().Merges));
        }

        foreach (string file in Directory.GetFiles(DbPath, "*.sst"))
            Assert.DoesNotContain("SECRET", Encoding.ASCII.GetString(File.ReadAllBytes(file)));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_store_with_level_6_only_opens_and_its_compactions_fill_the_levels_above(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        Random random = new(20261025);
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            for (int i = 0; i < 400; i++)
                Put(db, model, $"k{random.Next(2000):D4}", $"first{i}" + new string('f', 100));

            db.Flush();
            db.CompactRange(null, null);
            Assert.NotEmpty(db.DefaultColumnFamily.Version.Level6.Tables);
        }

        VersionTests.RewriteAsVersion(DbPath, 2);

        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = SmallLevels() };
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal([.. model.Select(pair => $"{pair.Key}={pair.Value}")], Scan(db));
            for (int i = 0; i < 4000; i++)
            {
                string key = $"k{random.Next(2000):D4}";
                if (random.Next(10) == 0)
                {
                    db.Delete(B(key));
                    model.Remove(key);
                }
                else
                    Put(db, model, key, $"second{i}" + new string('s', random.Next(20, 200)));

                if (i % 200 == 199)
                    db.Flush();
            }

            WaitForCompactions(db);
            Assert.True(db.CompactionCounts().Merges > 0);
            Assert.Contains(db.GetLiveFilesMetaData(), f => f.Level is > 0 and < 6);
            Assert.Equal([.. model.Select(pair => $"{pair.Key}={pair.Value}")], Scan(db));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
            Assert.Equal([.. model.Select(pair => $"{pair.Key}={pair.Value}")], Scan(db));
    }

    public void Dispose() => _directory.Dispose();

    // Levels small enough that a few hundred kilobytes fill several of them.
    internal static ColumnFamilyOptions SmallLevels() => new()
    {
        WriteBufferSize = 64 * 1024,
        Level0FileNumCompactionTrigger = 2,
        TargetFileSizeBase = 8 * 1024,
        MaxBytesForLevelBase = 16 * 1024,
        MaxBytesForLevelMultiplier = 2,
    };

    private static LevelTargets Targets(ColumnFamilyOptions options, params (int Level, long Size)[] levels)
    {
        long[] sizes = new long[Manifest.LevelCount];
        foreach ((int level, long size) in levels)
            sizes[level] = size;

        return LevelTargets.Compute(sizes, options);
    }

    private static DbOptions Manual(EnvironmentScope scope, long targetFileSize = 64 * 1024 * 1024) => scope.Options() with
    {
        DefaultColumnFamilyOptions = new ColumnFamilyOptions { DisableAutoCompactions = true, TargetFileSizeBase = targetFileSize },
    };

    // Rewrites the manifest of a closed store so its level 0 tables, oldest first, sit at the given levels.
    internal static void Place(string path, params int[] levels)
    {
        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, path)!;
        ManifestFamily record = manifest.ColumnFamilies.Single(f => f.Name == ColumnFamily.DefaultName);
        List<ManifestTable> oldestFirst = [.. record.Tables.Where(t => t.Level == 0).Reverse()];
        Assert.Equal(levels.Length, oldestFirst.Count);

        List<ManifestTable> placed = [.. oldestFirst.Select((table, i) => VersionTests.AtLevel(table, levels[i]))];
        record.Tables.Clear();
        record.Tables.AddRange(placed.Where(t => t.Level == 0).Reverse());
        record.Tables.AddRange(placed.Where(t => t.Level > 0).OrderBy(t => t.Level).ThenBy(t => t.Smallest, KeyOrder));
        manifest.Save(DefaultStorageEnvironment.Instance, path);
    }

    internal static void Write(SolariumDb db, params (string Key, string Value)[] pairs)
    {
        foreach ((string key, string value) in pairs)
            db.Put(B(key), B(value));

        db.Flush();
    }

    private static void Put(SolariumDb db, SortedDictionary<string, string> model, string key, string value)
    {
        db.Put(B(key), B(value));
        model[key] = value;
    }

    private static void WaitForCompactions(SolariumDb db)
    {
        Assert.True(SpinWait.SpinUntil(
            () => db.GetProperty("rocksdb.compaction-pending") == "0" && db.GetProperty("rocksdb.num-running-compactions") == "0", Timeout),
            "the compactions did not finish");
    }

    internal static List<Entry> Entries(TableReader table)
    {
        List<Entry> entries = [];
        IEntryCursor cursor = table.NewCursor(true);
        for (cursor.SeekToFirst(); cursor.Valid; cursor.Next())
            entries.Add(cursor.Current);

        return entries;
    }

    internal static List<string> Scan(SolariumDb db)
    {
        List<string> pairs = [];
        using Iterator iterator = db.NewIterator();
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            pairs.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        return pairs;
    }
}
