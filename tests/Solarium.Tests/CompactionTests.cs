using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Compactions into level 6: moves of tables that overlap nothing, merges, the tables a compaction
/// must take, reads and iterators that run across one, failures, crashes, and Dispose.
/// </summary>
public sealed class CompactionTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Tables_that_overlap_nothing_move_to_level_6_with_their_numbers_and_no_rewrite(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options(createMissingColumnFamilies: true) with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 4 },
        };

        List<long> flushed = [];
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            db.Put(B("o"), B("1"), other);
            db.Flush(other);

            foreach (string prefix in new[] { "a", "b", "c", "d" })
            {
                for (int i = 0; i < 50; i++)
                    db.Put(B($"{prefix}{i:D3}"), B($"{prefix}{i}"));

                db.Flush();
                flushed.Add(db.GetLiveFilesMetaData().Where(f => f.ColumnFamilyName == ColumnFamily.DefaultName && !flushed.Contains(f.FileNumber)).Single().FileNumber);
            }

            Assert.True(SpinWait.SpinUntil(() => db.DefaultColumnFamily.Version.Level0.Length == 0, Timeout), "the tables did not leave level 0");

            List<LiveFileMetaData> files = db.GetLiveFilesMetaData();
            Assert.Equal(flushed, files.Where(f => f.ColumnFamilyName == ColumnFamily.DefaultName && f.Level == 6).Select(f => f.FileNumber));
            Assert.Equal(0, files.Single(f => f.ColumnFamilyName == "other").Level);
            Assert.Equal((1L, 4L, 0L, 0L, 0L), db.CompactionCounts());
            Assert.Equal((4L, 1L, 0L), db.FamilyCounts(db.DefaultColumnFamily));
            Assert.Equal((1L, 0L, 0L), db.FamilyCounts(other));
            Assert.Equal("c7", S(db.Get(B("c007"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            Assert.Equal(flushed, db.DefaultColumnFamily.Version.Level6.Tables.Select(t => t.Number));
            Assert.Equal(200, Scan(db).Count);
            Assert.Equal("d49", S(db.Get(B("d049"))));
            Assert.Equal("1", S(db.Get(B("o"), db.GetColumnFamily("other"))));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_level_0_table_that_overlaps_an_older_one_ends_the_moves_and_stays(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 3 } };

        using SolariumDb db = SolariumDb.Open(DbPath, options);
        PutRange(db, 0, 50, "1");
        db.Flush();
        PutRange(db, 25, 75, "2");
        db.Flush();
        db.Put(B("z"), B("1"));
        db.Flush();

        Assert.True(SpinWait.SpinUntil(() => db.CompactionCounts().Moves == 1, Timeout), "nothing moved");

        // The oldest table moved; the next one overlaps it, so it and every newer one stay.
        FamilyVersion version = db.DefaultColumnFamily.Version;
        Assert.Equal(2, version.Level0.Length);
        Assert.Single(version.Level6.Tables);
        Assert.Equal((1L, 1L, 0L, 0L, 0L), db.CompactionCounts());
        Assert.Equal("2:30", S(db.Get(B("k030"))));
        Assert.Equal("1:10", S(db.Get(B("k010"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_merge_keeps_only_the_newest_put_of_each_key_that_nothing_deletes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options());

        List<string> expected;
        long output;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            PutRange(db, 0, 100, "1");
            db.Flush();
            PutRange(db, 0, 50, "2");
            for (int i = 50; i < 60; i++)
                db.Delete(B($"k{i:D3}"));

            db.DeleteRange(B("k060"), B("k070"));
            db.Flush();

            expected = Scan(db);
            long inputBytes = db.GetLiveFilesMetaData().Sum(f => f.Size);
            db.CompactRange(null, null);

            LiveFileMetaData merged = Assert.Single(db.GetLiveFilesMetaData());
            Assert.Equal(6, merged.Level);
            Assert.Equal(80, merged.EntryCount);
            Assert.Equal((0L, 0L, 1L, inputBytes, merged.Size), db.CompactionCounts());
            Assert.Equal((2L, 0L, 1L), db.FamilyCounts(db.DefaultColumnFamily));
            Assert.Equal([merged.FileNumber], TablesOnDisk(DbPath));
            output = merged.FileNumber;

            Assert.Equal(expected, Scan(db));
            Assert.Null(db.Get(B("k055")));
            Assert.Null(db.Get(B("k065")));
            Assert.Equal("1:70", S(db.Get(B("k070"))));
            Assert.Equal("2:49", S(db.Get(B("k049"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal(output, Assert.Single(db.GetLiveFilesMetaData()).FileNumber);
            Assert.Equal(expected, Scan(db));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_merge_ends_its_tables_at_the_target_size_between_keys(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options(), targetFileSize: 4096);

        using SolariumDb db = SolariumDb.Open(DbPath, options);
        PutRange(db, 0, 200, "1", padding: 100);
        db.Flush();
        PutRange(db, 100, 300, "2", padding: 100);
        db.Flush();
        List<string> expected = Scan(db);

        db.CompactRange(null, null);

        List<LiveFileMetaData> files = db.GetLiveFilesMetaData();
        Assert.True(files.Count >= 4, $"{files.Count} tables");
        Assert.All(files, f => Assert.Equal(6, f.Level));
        Assert.All(files.SkipLast(1), f => Assert.True(f.Size >= 4096, $"table {f.FileNumber} has {f.Size} bytes"));
        for (int i = 1; i < files.Count; i++)
            Assert.True(files[i - 1].LargestKey.AsSpan().SequenceCompareTo(files[i].SmallestKey) < 0);

        Assert.Equal(300, files.Sum(f => f.EntryCount));
        Assert.Equal(expected, Scan(db));
        Assert.Equal("2:150" + new string('p', 100), S(db.Get(B("k150"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_merge_of_a_range_where_every_key_is_deleted_leaves_no_table(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options());

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            PutRange(db, 0, 100, "1");
            db.Flush();
            db.DeleteRange(B("k000"), B("k050"));
            for (int i = 50; i < 100; i++)
                db.Delete(B($"k{i:D3}"));

            db.Flush();
            db.CompactRange(null, null);

            Assert.Empty(db.GetLiveFilesMetaData());
            Assert.Empty(TablesOnDisk(DbPath));
            Assert.Empty(Scan(db));
            Assert.Equal(1, db.CompactionCounts().Merges);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Empty(Scan(db));
            Assert.Null(db.Get(B("k010")));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_range_compaction_takes_every_level_0_table_that_overlaps_the_tables_in_its_range(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options());

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            // Only the newest table, m to y, is in the range [n, y]. The oldest one, a to m, is not,
            // but it holds an older m. Moved alone, the newest table would land below it, where
            // reads would find the older m first.
            db.Put(B("a"), B("1"));
            db.Put(B("m"), B("old"));
            db.Flush();
            db.Put(B("z"), B("1"));
            db.Flush();
            db.Put(B("m"), B("new"));
            db.Put(B("y"), B("1"));
            db.Flush();
            long untouched = db.DefaultColumnFamily.Version.Level0[1].Number;

            db.CompactRange(B("n"), B("y"));

            FamilyVersion version = db.DefaultColumnFamily.Version;
            Assert.Equal([untouched], version.Level0.Select(t => t.Number));
            TableReader merged = Assert.Single(version.Level6.Tables);
            Assert.Equal("a", S(merged.Properties.Smallest));
            Assert.Equal("y", S(merged.Properties.Largest));
            Assert.Equal(1, db.CompactionCounts().Merges);
            Assert.Equal("new", S(db.Get(B("m"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal(["a=1", "m=new", "y=1", "z=1"], Scan(db));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_range_compaction_moves_tables_that_overlap_nothing_and_leaves_level_6_alone(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, Manual(scope.Options()));

        PutRange(db, 0, 10, "1");
        db.Flush();
        db.CompactRange(null, null);
        Assert.Equal((1L, 1L, 0L, 0L, 0L), db.CompactionCounts());

        // Only level 6 holds the range: as RocksDB with no compaction filter, nothing is rewritten.
        db.Put(B("z"), B("1"));
        db.Flush();
        List<long> before = [.. db.GetLiveFilesMetaData().Select(f => f.FileNumber)];
        db.CompactRange(B("k000"), B("k009"));

        Assert.Equal(before, db.GetLiveFilesMetaData().Select(f => f.FileNumber));
        Assert.Equal((1L, 1L, 0L, 0L, 0L), db.CompactionCounts());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_read_held_between_its_version_and_its_snapshot_reads_the_version_it_entered(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using ManualResetEventSlim entered = new();
        using ManualResetEventSlim release = new();
        int hold = 0;
        DbOptions options = Manual(scope.Options()) with
        {
            AfterReadVersion = () =>
            {
                if (Interlocked.Exchange(ref hold, 0) == 1)
                {
                    entered.Set();
                    release.Wait();
                }
            },
        };

        using SolariumDb db = SolariumDb.Open(DbPath, options);
        db.Put(B("k"), B("old"));
        db.Put(B("other"), B("1"));
        db.Flush();

        // The Get enters the version whose active memtable takes the next write. With the snapshot
        // first, it would read the merge's version at a snapshot older than the only k left there.
        Volatile.Write(ref hold, 1);
        Task<byte[]?> get = Task.Run(() => db.Get(B("k")));
        Assert.True(entered.Wait(Timeout));
        db.Put(B("k"), B("new"));
        db.Flush();
        db.CompactRange(null, null);
        Assert.Equal(1, db.CompactionCounts().Merges);
        release.Set();
        Assert.Equal("new", S(await get.WaitAsync(Timeout)));

        // The same for an iterator, across a second merge that drops the first merge's table.
        entered.Reset();
        release.Reset();
        Volatile.Write(ref hold, 1);
        Task<List<string>> scan = Task.Run(() => Scan(db));
        Assert.True(entered.Wait(Timeout));
        db.Put(B("k"), B("newest"));
        db.Flush();
        db.CompactRange(null, null);
        Assert.Equal(2, db.CompactionCounts().Merges);
        release.Set();
        Assert.Equal(["k=newest", "other=1"], await scan.WaitAsync(Timeout));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_iterator_open_across_a_merge_reads_its_snapshot_and_its_tables_go_when_it_is_disposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, Manual(scope.Options()));

        PutRange(db, 0, 50, "1");
        db.Flush();
        PutRange(db, 0, 25, "2");
        for (int i = 25; i < 30; i++)
            db.Delete(B($"k{i:D3}"));

        db.Flush();
        List<string> expected = Scan(db);
        List<long> inputs = [.. db.GetLiveFilesMetaData().Select(f => f.FileNumber)];

        Iterator iterator = db.NewIterator();
        try
        {
            db.Put(B("k000"), B("3"));
            db.Flush();
            db.CompactRange(null, null);
            Assert.Equal(1, db.CompactionCounts().Merges);
            Assert.DoesNotContain(db.GetLiveFilesMetaData(), f => inputs.Contains(f.FileNumber));
            Assert.Subset(TablesOnDisk(DbPath).ToHashSet(), inputs.ToHashSet());

            List<string> seen = [];
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                seen.Add($"{S(iterator.Key())}={S(iterator.Value())}");

            Assert.Equal(expected, seen);
        }
        finally
        {
            iterator.Dispose();
        }

        Assert.Equal(db.GetLiveFilesMetaData().Select(f => f.FileNumber).Order(), TablesOnDisk(DbPath).Order());
        Assert.Equal("3", S(db.Get(B("k000"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Reads_on_eight_threads_during_moves_and_merges_see_each_key_only_move_forward(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = 16 * 1024,
                MaxWriteBufferNumber = 3,
                Level0FileNumCompactionTrigger = 2,
                TargetFileSizeBase = 16 * 1024,
            },
        };

        const int Keys = 500;
        const int Rounds = 60;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            int done = 0;
            Exception? failure = null;
            Thread[] readers = new Thread[8];
            for (int r = 0; r < readers.Length; r++)
            {
                int seed = r;
                readers[r] = new Thread(() =>
                {
                    try
                    {
                        // Each key is only ever overwritten with a later round, so the round one thread
                        // reads for it never goes down, and a key once seen is never missing again.
                        Random random = new(seed);
                        int[] seen = new int[Keys];
                        Array.Fill(seen, -1);
                        for (int n = 0; Volatile.Read(ref done) == 0; n++)
                        {
                            int i = random.Next(Keys);
                            int round = Round(db.Get(B($"k{i:D3}")), i);
                            Assert.True(round >= seen[i], $"key {i}: round {round} after round {seen[i]}");
                            seen[i] = round;

                            if (n % 64 == 0)
                            {
                                using Iterator iterator = db.NewIterator();
                                iterator.Seek(B($"k{i:D3}"));
                                for (int step = 0; step < 20 && iterator.Valid(); step++, iterator.Next())
                                {
                                    int key = int.Parse(S(iterator.Key())[1..]);
                                    Assert.True(Round(iterator.Value().ToArray(), key) >= 0);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                    }
                });
                readers[r].Start();
            }

            for (int round = 0; round < Rounds; round++)
                for (int i = 0; i < Keys; i++)
                    db.Put(B($"k{i:D3}"), B($"{round}:{i}:" + new string('v', 40)));

            Volatile.Write(ref done, 1);
            foreach (Thread reader in readers)
                reader.Join();

            Assert.Null(failure);
            (long Moves, long MovedFiles, long Merges, long BytesRead, long BytesWritten) counts = db.CompactionCounts();
            Assert.True(counts.Merges > 0, "no tables merged");
            for (int i = 0; i < Keys; i++)
                Assert.Equal(Rounds - 1, Round(db.Get(B($"k{i:D3}")), i));
        }

        // No reader holds a retired version, so every table that left the manifest is gone.
        Assert.Equal(Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!.ColumnFamilies.SelectMany(f => f.Tables).Select(t => t.Number).Order(), TablesOnDisk(DbPath).Order());

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            for (int i = 0; i < Keys; i++)
                Assert.Equal(Rounds - 1, Round(db.Get(B($"k{i:D3}")), i));
        }

        static int Round(byte[]? value, int key)
        {
            if (value is null)
                return -1;

            string[] parts = S(value)!.Split(':');
            Assert.Equal(key, int.Parse(parts[1]));
            return int.Parse(parts[0]);
        }
    }

    [Fact]
    public void A_failed_merge_stops_writes_keeps_reads_and_leaves_its_inputs()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        DbOptions options = Manual(new DbOptions { Environment = hooks });

        List<string> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            PutRange(db, 0, 100, "1");
            db.Flush();
            PutRange(db, 50, 150, "2");
            db.Flush();
            expected = Scan(db);
            List<long> before = [.. db.GetLiveFilesMetaData().Select(f => f.FileNumber)];
            HashSet<long> existing = [.. TablesOnDisk(DbPath)];

            hooks.FailingPaths = path => IsNewTable(path, existing);
            hooks.FailWrites = true;

            Assert.Throws<SolariumException>(() => db.CompactRange(null, null));
            Assert.Throws<SolariumException>(() => db.Put(B("after"), B("1")));
            Assert.Equal(expected, Scan(db));
            Assert.Equal(before, db.GetLiveFilesMetaData().Select(f => f.FileNumber));
            Assert.Equal(existing.Order(), TablesOnDisk(DbPath).Order());
        }

        hooks.FailWrites = false;
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(new DbOptions())))
        {
            Assert.Equal(expected, Scan(db));
            db.CompactRange(null, null);
            Assert.Equal(expected, Scan(db));
            Assert.Equal(1, db.CompactionCounts().Merges);
        }
    }

    [Fact]
    public async Task A_crash_after_a_merge_wrote_its_tables_and_before_the_manifest_lists_them_loses_nothing()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        string copy = _directory.Combine("copy");

        List<string> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(new DbOptions { Environment = hooks }, targetFileSize: 4096)))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            PutRange(db, 0, 200, "1", padding: 50);
            db.Flush();
            PutRange(db, 100, 300, "2", padding: 50);
            db.Flush();
            expected = Scan(db);

            // Hold the merge at the flush of its first table: the table is written, and no manifest lists it.
            HashSet<long> existing = [.. TablesOnDisk(DbPath)];
            hooks.GatedPaths = path => IsNewTable(path, existing);
            hooks.FlushStarted.Reset();
            hooks.FlushGate.Reset();
            Task compact = Task.Run(() => db.CompactRange(null, null));
            Assert.True(hooks.FlushStarted.Wait(Timeout));

            CopyDatabase(DbPath, copy);
            Assert.True(TablesOnDisk(copy).Count > existing.Count, "the merge wrote no table");

            hooks.FlushGate.Set();
            await compact.WaitAsync(Timeout);
        }

        using (SolariumDb db = SolariumDb.Open(copy, Manual(new DbOptions())))
        {
            Assert.Equal(expected, Scan(db));
            Assert.Equal(db.GetLiveFilesMetaData().Select(f => f.FileNumber).Order(), TablesOnDisk(copy).Order());
            Assert.All(db.GetLiveFilesMetaData(), f => Assert.Equal(0, f.Level));
        }
    }

    [Fact]
    public void A_crash_after_the_manifest_lists_a_merge_and_before_its_inputs_are_deleted_loses_nothing()
    {
        string copy = _directory.Combine("copy");

        List<string> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, Manual(new DbOptions())))
        {
            PutRange(db, 0, 100, "1");
            db.Flush();
            PutRange(db, 50, 150, "2");
            db.Flush();
            expected = Scan(db);
            HashSet<long> inputs = [.. TablesOnDisk(DbPath)];

            // The open iterator keeps the inputs on disk after the manifest drops them.
            using (Iterator iterator = db.NewIterator())
            {
                db.CompactRange(null, null);
                Assert.Subset(TablesOnDisk(DbPath).ToHashSet(), inputs);
                CopyDatabase(DbPath, copy);
            }
        }

        using (SolariumDb db = SolariumDb.Open(copy, Manual(new DbOptions())))
        {
            Assert.Equal(expected, Scan(db));
            LiveFileMetaData merged = Assert.Single(db.GetLiveFilesMetaData());
            Assert.Equal(6, merged.Level);
            Assert.Equal([merged.FileNumber], TablesOnDisk(copy));
        }
    }

    [Fact]
    public async Task Dispose_stops_a_merge_and_deletes_the_tables_it_wrote()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        DbOptions options = Manual(new DbOptions { Environment = hooks }, targetFileSize: 8 * 1024);
        SolariumDb db = SolariumDb.Open(DbPath, options);
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();

        // More keys than the merge handles between two checks for a stop.
        for (int i = 0; i < 3000; i++)
            db.Put(B($"k{i:D4}"), B($"1:{i}:" + new string('v', 60)));

        db.Flush();
        for (int i = 0; i < 3000; i += 2)
            db.Put(B($"k{i:D4}"), B($"2:{i}"));

        db.Flush();
        List<string> expected = Scan(db);
        HashSet<long> existing = [.. TablesOnDisk(DbPath)];

        hooks.GatedPaths = path => IsNewTable(path, existing);
        hooks.WriteStarted.Reset();
        hooks.WriteGate.Reset();
        Task compact = Task.Run(() => db.CompactRange(null, null));
        Assert.True(hooks.WriteStarted.Wait(Timeout));

        Task dispose = Task.Run(db.Dispose);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => compact.WaitAsync(Timeout));

        hooks.WriteGate.Set();
        await dispose.WaitAsync(Timeout);

        Assert.Equal(existing.Order(), Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!.ColumnFamilies.SelectMany(f => f.Tables).Select(t => t.Number).Order());
        Assert.Equal(existing.Order(), TablesOnDisk(DbPath).Order());

        using SolariumDb reopened = SolariumDb.Open(DbPath, Manual(new DbOptions()));
        Assert.Equal(expected, Scan(reopened));
    }

    [Fact]
    public void A_compaction_trigger_or_target_file_size_below_1_is_refused()
    {
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath,
            new DbOptions { DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 0 } }));
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath,
            new DbOptions { DefaultColumnFamilyOptions = new ColumnFamilyOptions { TargetFileSizeBase = 0 } }));
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    /// <summary>Returns <paramref name="options"/> with automatic compactions off, so a test decides when each compaction runs.</summary>
    internal static DbOptions Manual(DbOptions options, long targetFileSize = 64L * 1024 * 1024)
    {
        return options with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { DisableAutoCompactions = true, TargetFileSizeBase = targetFileSize },
        };
    }

    /// <summary>Puts k{first:D3} to k{end - 1:D3}, each set to "{tag}:{i}" plus <paramref name="padding"/> bytes.</summary>
    internal static void PutRange(SolariumDb db, int first, int end, string tag, int padding = 0)
    {
        for (int i = first; i < end; i++)
            db.Put(B($"k{i:D3}"), B($"{tag}:{i}" + new string('p', padding)));
    }

    /// <summary>Every key and value of the default family, as "key=value", in key order.</summary>
    internal static List<string> Scan(SolariumDb db)
    {
        using Iterator iterator = db.NewIterator();
        List<string> entries = [];
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            entries.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        return entries;
    }

    /// <summary>The numbers of the SST files in <paramref name="directory"/>.</summary>
    internal static List<long> TablesOnDisk(string directory)
    {
        return [.. Directory.GetFiles(directory, "*.sst").Select(p => long.Parse(Path.GetFileNameWithoutExtension(p)))];
    }

    /// <summary>True when <paramref name="path"/> is an SST file whose number is not in <paramref name="existing"/>.</summary>
    internal static bool IsNewTable(string path, HashSet<long> existing)
    {
        return path.EndsWith(".sst", StringComparison.Ordinal) && !existing.Contains(long.Parse(Path.GetFileNameWithoutExtension(path)));
    }

    /// <summary>Copies what a process crash leaves: every file as the OS holds it, without the lock.</summary>
    internal static void CopyDatabase(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string file in Directory.GetFiles(from))
            if (Path.GetFileName(file) != "LOCK")
                File.Copy(file, Path.Combine(to, Path.GetFileName(file)));
    }
}
