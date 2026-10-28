using System.Text;
using Solarium.Storage;
using static Solarium.Tests.CompactionTests;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// <see cref="SolariumDb.DeleteFilesInRange"/>: which level 6 tables it drops, what reads see after it,
/// and Kommander's floor layout, where flushed files move to level 6 whole and are dropped whole.
/// </summary>
public sealed class DeleteFilesInRangeTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_level_6_tables_whose_keys_all_lie_in_the_range_go_and_the_rest_stay(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options());

        long[] chunks;
        long level0;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            chunks = Chunks(db, "a", "b", "c", "d");
            db.Put(B("b025"), B("new"));
            db.Flush();
            level0 = Assert.Single(db.DefaultColumnFamily.Version.Level0).Number;

            db.DeleteFilesInRange(B("b000"), B("c049"));

            Assert.Equal([chunks[0], chunks[3]], db.DefaultColumnFamily.Version.Level6.Tables.Select(t => t.Number));
            Assert.Equal([level0], db.DefaultColumnFamily.Version.Level0.Select(t => t.Number));
            Assert.Equal(2, db.DroppedFileCount);
            Assert.Equal(new[] { chunks[0], chunks[3], level0 }.Order(), TablesOnDisk(DbPath).Order());
            AssertReads(db);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal([chunks[0], chunks[3]], db.DefaultColumnFamily.Version.Level6.Tables.Select(t => t.Number));
            AssertReads(db);
        }

        // The level 0 table keeps its newer b025; every other key of b and c went with its table.
        static void AssertReads(SolariumDb db)
        {
            Assert.Null(db.Get(B("b010")));
            Assert.Null(db.Get(B("c049")));
            Assert.Equal("new", S(db.Get(B("b025"))));
            Assert.Equal("a10", S(db.Get(B("a010"))));
            Assert.Equal("d49", S(db.Get(B("d049"))));
            Assert.Equal(101, Scan(db).Count);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_table_that_reaches_past_a_bound_stays_and_include_end_decides_a_largest_key_equal_to_the_end(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, Manual(scope.Options()));
        long[] chunks = Chunks(db, "a", "b", "c", "d");

        db.DeleteFilesInRange(B("b001"), B("c048"));
        Assert.Equal(chunks, Level6(db));

        db.DeleteFilesInRange(B("b000"), B("c049"), includeEnd: false);
        Assert.Equal([chunks[0], chunks[2], chunks[3]], Level6(db));

        db.DeleteFilesInRange(null, B("c049"));
        Assert.Equal([chunks[3]], Level6(db));

        db.DeleteFilesInRange(B("d000"), null);
        Assert.Empty(Level6(db));
        Assert.Empty(Scan(db));
        Assert.Equal(4, db.DroppedFileCount);
        Assert.Empty(TablesOnDisk(DbPath));
    }

    [Fact]
    public async Task The_tables_of_a_compaction_that_runs_stay()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, Manual(new DbOptions { Environment = hooks }));
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        long[] chunks = Chunks(db, "a", "b");
        db.Put(B("b010"), B("new"));
        db.Flush();

        // Hold the merge of the new table with b's at the first write of its output.
        HashSet<long> existing = [.. TablesOnDisk(DbPath)];
        hooks.GatedPaths = path => IsNewTable(path, existing);
        hooks.WriteStarted.Reset();
        hooks.WriteGate.Reset();
        Task compact = Task.Run(() => db.CompactRange(B("b000"), B("b049")));
        Assert.True(hooks.WriteStarted.Wait(Timeout));

        db.DeleteFilesInRange(null, null);
        Assert.Equal([chunks[1]], Level6(db));

        hooks.WriteGate.Set();
        await compact.WaitAsync(Timeout);
        Assert.Equal(1, db.DroppedFileCount);
        Assert.Equal(1, db.CompactionCounts().Merges);
        Assert.Null(db.Get(B("a010")));
        Assert.Equal("new", S(db.Get(B("b010"))));
        Assert.Equal("b11", S(db.Get(B("b011"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_iterator_open_across_a_drop_reads_the_dropped_tables_and_they_go_when_it_is_disposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, Manual(scope.Options()));
        long[] chunks = Chunks(db, "a", "b");

        List<string> seen = [];
        using (Iterator iterator = db.NewIterator())
        {
            db.DeleteFilesInRange(null, null);
            Assert.Empty(Level6(db));
            Assert.Empty(Scan(db));
            Assert.Equal(chunks.Order(), TablesOnDisk(DbPath).Order());

            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                seen.Add(S(iterator.Key()));
        }

        Assert.Equal(100, seen.Count);
        Assert.Empty(TablesOnDisk(DbPath));
    }

    [Fact]
    public void A_drop_whose_manifest_save_fails_stops_writes_and_a_reopen_finishes_it()
    {
        int fail = 0;
        DbOptions options = Manual(new DbOptions()) with
        {
            AfterManifestSave = () =>
            {
                if (Interlocked.Exchange(ref fail, 0) == 1)
                    throw new IOException("injected directory flush failure");
            },
        };

        long[] chunks;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            chunks = Chunks(db, "a", "b");
            Volatile.Write(ref fail, 1);
            Assert.Throws<SolariumException>(() => db.DeleteFilesInRange(B("a000"), B("a049")));
            Assert.Throws<SolariumException>(() => db.Put(B("k"), B("v")));
            Assert.Throws<SolariumException>(() => db.DeleteFilesInRange(B("b000"), B("b049")));

            // The version still lists the table, so reads keep it.
            Assert.Equal(chunks, Level6(db));
            Assert.Equal("a10", S(db.Get(B("a010"))));
        }

        // The manifest on disk dropped it, so the reopen deletes its file as an orphan.
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(new DbOptions())))
        {
            Assert.Equal([chunks[1]], Level6(db));
            Assert.Equal([chunks[1]], TablesOnDisk(DbPath));
            Assert.Null(db.Get(B("a010")));
            Assert.Equal("b10", S(db.Get(B("b010"))));
        }
    }

    // Kommander's TestRocksDbCompactionFloor.FlushedFiles_AreMovedNotRewritten_AndDroppedWholeBelowFloor,
    // on Solarium: one partition's flushed files do not overlap, so they move to level 6 with their
    // numbers and no rewrite, and the drop below the floor reclaims exactly the files below it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Flushed_log_files_move_without_a_rewrite_and_the_files_below_the_floor_are_dropped_whole(string environment)
    {
        const int Partition = 1;
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 } };
        using SolariumDb db = SolariumDb.Open(DbPath, options);

        byte[] payload = new byte[4096];
        new Random(4096).NextBytes(payload);
        for (int chunk = 0; chunk < 4; chunk++)
        {
            long first = (chunk * 300) + 1;
            WriteBatch batch = new();
            for (long id = first; id < first + 300; id++)
                batch.Put(LogKey(Partition, id), payload);

            db.Write(batch);
            db.Flush();
        }

        db.Put(LogKey(Partition, 1201), B("checkpoint"));

        // Files never overlap: in key order, each one ends before the next begins.
        List<LiveFileMetaData> files = [.. db.GetLiveFilesMetaData().OrderBy(f => LogId(f.SmallestKey))];
        Assert.Equal(4, files.Count);
        for (int i = 1; i < files.Count; i++)
            Assert.True(LogId(files[i - 1].LargestKey) < LogId(files[i].SmallestKey));

        // The two oldest files hold the rows below the floor; they move first.
        Assert.True(SpinWait.SpinUntil(() => db.GetLiveFilesMetaData().Count(f => f.Level == 6 && LogId(f.LargestKey) < 699) == 2, Timeout), "the oldest files did not move");
        (long Moves, long MovedFiles, long Merges, long BytesRead, long BytesWritten) counts = db.CompactionCounts();
        Assert.True(counts.Moves > 0, "expected a move out of level 0");
        Assert.Equal(0, counts.Merges);

        List<LiveFileMetaData> before = db.GetLiveFilesMetaData();
        List<LiveFileMetaData> dead = [.. before.Where(f => LogId(f.LargestKey) < 699)];
        List<LiveFileMetaData> live = [.. before.Where(f => LogId(f.LargestKey) >= 699)];
        Assert.Equal(2, dead.Count);
        Assert.Equal(2, live.Count);

        // Kommander's floor pass at 700: key(p, 0) to key(p, floor - 1), include_end at its default.
        db.DeleteFilesInRange(LogKey(Partition, 0), LogKey(Partition, 699));

        List<LiveFileMetaData> after = db.GetLiveFilesMetaData();
        Assert.DoesNotContain(after, f => LogId(f.LargestKey) < 699);
        foreach (LiveFileMetaData file in dead)
            Assert.DoesNotContain(after, f => f.FileNumber == file.FileNumber);

        foreach (LiveFileMetaData file in live)
            Assert.Contains(after, f => f.FileNumber == file.FileNumber && f.Size == file.Size);

        Assert.Equal(dead.Sum(f => f.Size), before.Sum(f => f.Size) - after.Sum(f => f.Size));
        Assert.Equal(after.Select(f => f.FileNumber).Order(), TablesOnDisk(DbPath).Order());
        Assert.Equal(0, db.CompactionCounts().Merges);

        // No row was rewritten: the straddling file still holds 601 to 699, and every row reads.
        Assert.Null(db.Get(LogKey(Partition, 600)));
        Assert.Equal(payload, db.Get(LogKey(Partition, 601)));
        Assert.Equal(payload, db.Get(LogKey(Partition, 1200)));
        using Iterator iterator = db.NewIterator();
        int rows = 0;
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            rows++;

        Assert.Equal(601, rows);
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // One level 6 table for each prefix, in order: {prefix}000 to {prefix}049 = "{prefix}{i}",
    // flushed and moved. Returns the table numbers.
    private static long[] Chunks(SolariumDb db, params string[] prefixes)
    {
        foreach (string prefix in prefixes)
        {
            for (int i = 0; i < 50; i++)
                db.Put(B($"{prefix}{i:D3}"), B($"{prefix}{i}"));

            db.Flush();
            db.CompactRange(null, null);
        }

        long[] numbers = Level6(db);
        Assert.Equal(prefixes.Length, numbers.Length);
        Assert.Equal(prefixes.Length, db.CompactionCounts().Moves);
        return numbers;
    }

    private static long[] Level6(SolariumDb db) => [.. db.DefaultColumnFamily.Version.Level6.Tables.Select(t => t.Number)];

    /// <summary>Kommander's log key: the partition and the id in fixed-width decimal, with a separator.</summary>
    internal static byte[] LogKey(int partition, long id) => Encoding.ASCII.GetBytes($"{partition:D10}:{id:D20}");

    private static long LogId(byte[] key) => long.Parse(Encoding.ASCII.GetString(key, 11, 20));
}
