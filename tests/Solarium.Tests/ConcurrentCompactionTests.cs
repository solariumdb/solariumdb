using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// More than one compaction at a time: compactions of two families side by side, a merge of level 0
/// beside a merge of the level below it, the files and key ranges a running compaction keeps from the
/// others, manual compactions that run alone, a failure while another compaction runs, Dispose while
/// several merge, and many writers on several families checked against a model.
/// </summary>
public sealed class ConcurrentCompactionTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_held_merge_of_one_family_does_not_hold_back_the_merge_of_another(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using ManualResetEventSlim held = new(), gate = new();
        DbOptions options = TwoFamilies(scope) with
        {
            CompactionStarting = compaction =>
            {
                if (compaction.Family.Name == "a")
                {
                    held.Set();
                    gate.Wait();
                }
            },
        };

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["a", "b"]))
        {
            try
            {
                ColumnFamily a = db.GetColumnFamily("a"), b = db.GetColumnFamily("b");
                OverlappingFlushes(db, a, "a");
                Assert.True(held.Wait(Timeout));

                // The second thread compacts b while a's compaction waits: b's older table overlaps
                // nothing in level 6, so it moves there, and the newer one, which overlaps it, stays.
                OverlappingFlushes(db, b, "b");
                Assert.True(SpinWait.SpinUntil(() => db.FamilyCounts(b).Moves == 1, Timeout), "b did not compact beside a");
                Assert.Single(b.Version.Level0);
                Assert.Single(b.Version.Level6.Tables);
                Assert.Equal(2, a.Version.Level0.Length);
                Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-running-compactions") == "1", Timeout));
                Assert.Equal(2, db.CompactionConcurrency().MostRunning);
            }
            finally
            {
                gate.Set();
            }

            // The count goes up when the compaction installs, and the compaction leaves the running ones after.
            Assert.True(SpinWait.SpinUntil(() => db.FamilyCounts(db.GetColumnFamily("a")).Moves == 1 && db.GetProperty("rocksdb.num-running-compactions") == "0", Timeout),
                "a did not compact");
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, TwoFamilies(scope), ["a", "b"]))
        {
            Assert.Equal("a-2", S(db.Get(B("k025"), db.GetColumnFamily("a"))));
            Assert.Equal("b-2", S(db.Get(B("k049"), db.GetColumnFamily("b"))));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_picker_passes_over_the_files_and_ranges_that_running_compactions_hold(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions manual = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { DisableAutoCompactions = true } };
        WriteLayout(manual, largeB: false);

        using SolariumDb db = SolariumDb.Open(DbPath, manual);
        ColumnFamily family = db.DefaultColumnFamily;
        FamilyVersion version = family.Version;
        LevelTargets targets = LevelTargets.Compute(version, family.Options);
        Assert.Equal(5, targets.BaseLevel);
        TableReader a = version.Levels[5].Tables[0], b = version.Levels[5].Tables[1];

        // The oldest level 0 table overlaps a, so the merge into level 5 takes it.
        Compaction level0 = CompactionPicker.Pick(family, 0, targets, [])!;
        Assert.Equal([a], level0.OutputLevelInputs);

        // One level 0 compaction at a time; level 5 passes over a and takes b.
        Assert.Null(CompactionPicker.Pick(family, 0, targets, [level0]));
        Compaction besideIt = CompactionPicker.Pick(family, 5, targets, [level0])!;
        Assert.Equal([b], besideIt.Inputs);

        // Alone, kMinOverlappingRatio picks a; while that runs, level 0 waits, since a is in its range.
        Compaction ofA = CompactionPicker.Pick(family, 5, targets, [])!;
        Assert.Equal([a], ofA.Inputs);
        Assert.Null(CompactionPicker.Pick(family, 0, targets, [ofA]));

        // A running compaction into level 6 over a's range keeps a's merge out of it, so b goes; with
        // b's merge running too, level 5 has nothing left to give.
        Compaction intoA = new(family, 0, [version.Level0[0]], 6, [], isMove: true, []);
        Assert.Equal([b], CompactionPicker.Pick(family, 5, targets, [intoA])!.Inputs);
        Assert.Null(CompactionPicker.Pick(family, 5, targets, [intoA, besideIt]));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_merge_of_level_0_runs_beside_a_merge_of_the_level_below_outside_its_range(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        ColumnFamilyOptions levels = new() { Level0FileNumCompactionTrigger = 2, MaxBytesForLevelBase = 4 * 1024 };
        DbOptions manual = scope.Options() with { DefaultColumnFamilyOptions = levels with { DisableAutoCompactions = true } };
        WriteLayout(manual, largeB: true);

        // Level 0 needs a compaction most, and level 5 needs one too once level 0's merge holds a.
        using (SolariumDb db = SolariumDb.Open(DbPath, manual))
        {
            ColumnFamily family = db.DefaultColumnFamily;
            LevelTargets targets = LevelTargets.Compute(family.Version, family.Options);
            Assert.Equal(5, targets.BaseLevel);
            Dictionary<int, double> scores = CompactionPicker.Scores(family, targets, []).ToDictionary(s => s.Level, s => s.Score);
            Assert.True(scores[0] > scores[5], $"level 0 scores {scores[0]}, level 5 {scores[5]}");
            Compaction level0 = CompactionPicker.Pick(family, 0, targets, [])!;
            Assert.True(CompactionPicker.Scores(family, targets, [level0]).Single(s => s.Level == 5).Score >= 1);
        }

        using ManualResetEventSlim held = new(), gate = new();
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = levels,
            MaxBackgroundCompactions = 2,
            CompactionStarting = compaction =>
            {
                if (compaction.StartLevel == 0 && !held.IsSet)
                {
                    held.Set();
                    gate.Wait();
                }
            },
        };

        List<string> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, manual))
            expected = LevelTests.Scan(db);

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            try
            {
                Assert.True(held.Wait(Timeout));

                // b leaves level 5 while level 0's merge, which holds a, waits.
                Assert.True(SpinWait.SpinUntil(() => db.DefaultColumnFamily.Version.Levels[5].Tables.Length == 1, Timeout), "b did not move down");
                Assert.Equal("a010", S(db.DefaultColumnFamily.Version.Levels[5].Tables[0].Properties.Smallest));
                Assert.Equal(2, db.CompactionConcurrency().MostRunning);
                Assert.Equal(expected, LevelTests.Scan(db));
            }
            finally
            {
                gate.Set();
            }

            Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.compaction-pending") == "0" && db.GetProperty("rocksdb.num-running-compactions") == "0", Timeout));
            Assert.Equal(expected, LevelTests.Scan(db));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, manual))
            Assert.Equal(expected, LevelTests.Scan(db));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_manual_compaction_waits_for_the_running_ones_and_holds_back_new_ones(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using ManualResetEventSlim held = new(), gate = new();
        DbOptions options = TwoFamilies(scope) with
        {
            CompactionStarting = compaction =>
            {
                if (compaction.Family.Name == "a")
                {
                    held.Set();
                    gate.Wait();
                }
            },
        };

        using SolariumDb db = SolariumDb.Open(DbPath, options, ["a", "b"]);
        ColumnFamily a = db.GetColumnFamily("a"), b = db.GetColumnFamily("b");
        Task manual;
        try
        {
            OverlappingFlushes(db, a, "a");
            Assert.True(held.Wait(Timeout));

            // Queued while a's merge runs: it waits, and b's level 0, past its trigger, waits with it.
            manual = Task.Run(() => db.CompactRange(null, null, b));
            Assert.True(SpinWait.SpinUntil(() => db.QueuedManualCompactions == 1, Timeout));
            OverlappingFlushes(db, b, "b");
            Assert.NotSame(manual, await Task.WhenAny(manual, Task.Delay(300)));
            Assert.Equal(2, b.Version.Level0.Length);
            Assert.Equal(1, db.CompactionConcurrency().MostRunning);
        }
        finally
        {
            gate.Set();
        }

        await manual.WaitAsync(Timeout);
        Assert.Empty(b.Version.Level0);
        Assert.Equal(1, db.CompactionConcurrency().MostRunning);
        Assert.Equal("b-2", S(db.Get(B("k010"), b)));
    }

    public static TheoryData<string, bool> EnvironmentsAndMerges()
    {
        TheoryData<string, bool> data = [];
        foreach (string name in IoUring.IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
        {
            data.Add(name, false);
            data.Add(name, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndMerges))]
    public void A_compaction_that_fails_beside_another_stops_writes_and_the_other_installs_nothing(string environment, bool merge)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using ManualResetEventSlim held = new(), gate = new();
        DbOptions options = TwoFamilies(scope) with
        {
            CompactionStarting = compaction =>
            {
                if (compaction.Family.Name == "b")
                    throw new IOException("injected");

                // The move of a's first table, when the held compaction is a merge, goes through.
                if (compaction.IsMove == merge)
                    return;

                held.Set();
                gate.Wait();
            },
        };

        long[] before;
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["a", "b"]))
        {
            ColumnFamily a = db.GetColumnFamily("a"), b = db.GetColumnFamily("b");
            try
            {
                // Two flushes move a's older table to level 6; a third overlaps it, so the second
                // compaction merges.
                OverlappingFlushes(db, a, "a");
                if (merge)
                {
                    Assert.True(SpinWait.SpinUntil(() => db.FamilyCounts(a).Moves == 1, Timeout));
                    for (int i = 0; i < 50; i++)
                        db.Put(B($"k{i:D3}"), B("a-2"), a);

                    db.Flush(a);
                }

                Assert.True(held.Wait(Timeout));
                Assert.Equal(merge, db.GetLiveFilesMetaData().Any(f => f.ColumnFamilyName == "a" && f.Level == 6));
                OverlappingFlushes(db, b, "b");
                Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-running-compactions") == "1" && Throws(() => db.Put(B("z"), B("1"), a)), Timeout),
                    "the failure did not stop writes");
                before = [.. db.GetLiveFilesMetaData().Select(f => f.FileNumber)];
            }
            finally
            {
                gate.Set();
            }

            // a's merge runs after the failure and installs nothing: the manifest keeps a's level 0
            // tables, and the merge's outputs are gone.
            Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-running-compactions") == "0", Timeout));
            Assert.Equal(before, db.GetLiveFilesMetaData().Select(f => f.FileNumber));
            Assert.Equal(2, a.Version.Level0.Length);
            Assert.Throws<SolariumException>(() => db.Put(B("z"), B("1")));
        }

        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!;
        HashSet<long> listed = [.. manifest.ColumnFamilies.SelectMany(f => f.Tables).Select(t => t.Number)];
        Assert.All(Directory.GetFiles(DbPath, "*.sst"), file => Assert.Contains(long.Parse(Path.GetFileNameWithoutExtension(file)), listed));

        using (SolariumDb db = SolariumDb.Open(DbPath, TwoFamilies(scope), ["a", "b"]))
        {
            Assert.Equal("a-2", S(db.Get(B("k025"), db.GetColumnFamily("a"))));
            Assert.Equal("b-2", S(db.Get(B("k025"), db.GetColumnFamily("b"))));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Dispose_stops_every_merge_that_runs_and_leaves_no_file_of_theirs(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using ManualResetEventSlim gate = new();
        using CountdownEvent started = new(2);
        DbOptions options = TwoFamilies(scope) with
        {
            CompactionStarting = _ =>
            {
                started.Signal();
                gate.Wait();
            },
        };

        Task disposing;
        SolariumDb db = SolariumDb.Open(DbPath, options, ["a", "b"]);
        try
        {
            // Enough keys that each merge reaches its check for a stop before it ends.
            foreach (string name in new[] { "a", "b" })
                OverlappingFlushes(db, db.GetColumnFamily(name), name, 20000);

            Assert.True(started.Wait(Timeout));
            disposing = Task.Run(db.Dispose);
            Assert.NotSame(disposing, await Task.WhenAny(disposing, Task.Delay(200)));
        }
        finally
        {
            gate.Set();
        }

        await disposing.WaitAsync(Timeout);

        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!;
        HashSet<long> listed = [.. manifest.ColumnFamilies.SelectMany(f => f.Tables).Select(t => t.Number)];
        Assert.All(Directory.GetFiles(DbPath, "*.sst"), file => Assert.Contains(long.Parse(Path.GetFileNameWithoutExtension(file)), listed));

        using SolariumDb reopened = SolariumDb.Open(DbPath, TwoFamilies(scope), ["a", "b"]);
        Assert.Equal("a-2", S(reopened.Get(B("k19999"), reopened.GetColumnFamily("a"))));
        Assert.Equal("b-2", S(reopened.Get(B("k00000"), reopened.GetColumnFamily("b"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Many_writers_on_several_families_agree_with_a_model_while_four_compactions_run(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string[] names = ["f0", "f1", "f2", "f3"];
        DbOptions options = scope.Options(createMissingColumnFamilies: true) with
        {
            MaxBackgroundCompactions = 4,
            DefaultColumnFamilyOptions = LevelTests.SmallLevels() with { WriteBufferSize = 16 * 1024 },
        };

        // Each writer owns its keys, so each keeps its own model.
        const int Writers = 8;
        SortedDictionary<string, string>[] models = [.. Enumerable.Range(0, Writers).Select(_ => new SortedDictionary<string, string>(StringComparer.Ordinal))];
        using (SolariumDb db = SolariumDb.Open(DbPath, options, names))
        {
            await Task.WhenAll(Enumerable.Range(0, Writers).Select(w => Task.Run(() =>
            {
                Random random = new(w);
                ColumnFamily family = db.GetColumnFamily(names[w % names.Length]);
                for (int i = 0; i < 3000; i++)
                {
                    string key = $"w{w}-{random.Next(800):D4}";
                    if (random.Next(10) == 0)
                    {
                        db.Delete(B(key), family);
                        models[w].Remove(key);
                    }
                    else if (random.Next(200) == 0)
                    {
                        string end = $"w{w}-{random.Next(800):D4}";
                        (string begin, string last) = string.CompareOrdinal(key, end) <= 0 ? (key, end) : (end, key);
                        db.DeleteRange(B(begin), B(last), family);
                        foreach (string doomed in models[w].Keys.Where(k => string.CompareOrdinal(k, begin) >= 0 && string.CompareOrdinal(k, last) < 0).ToList())
                            models[w].Remove(doomed);
                    }
                    else
                    {
                        string value = $"{i}:" + new string((char)('a' + w), random.Next(20, 160));
                        db.Put(B(key), B(value), family);
                        models[w][key] = value;
                    }
                }
            }))).WaitAsync(TimeSpan.FromSeconds(120));

            Assert.True(SpinWait.SpinUntil(() => names.All(n => db.GetProperty("rocksdb.compaction-pending", db.GetColumnFamily(n)) == "0")
                && db.GetProperty("rocksdb.num-running-compactions") == "0", Timeout), "the compactions did not finish");
            Assert.True(db.CompactionConcurrency().MostRunning >= 2, $"at most {db.CompactionConcurrency().MostRunning} compactions ran at once");
            Assert.Contains(db.GetLiveFilesMetaData(), f => f.Level is > 0 and < 6);
            Check(db, names, models);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, names))
            Check(db, names, models);
    }

    public void Dispose() => _directory.Dispose();

    private static DbOptions TwoFamilies(EnvironmentScope scope) => scope.Options(createMissingColumnFamilies: true) with
    {
        MaxBackgroundCompactions = 2,
        DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 },
    };

    // Two flushes of the same keys, so the two level 0 tables overlap and merge rather than move.
    private static void OverlappingFlushes(SolariumDb db, ColumnFamily family, string name, int keys = 50)
    {
        string format = keys > 1000 ? "D5" : "D3";
        for (int flush = 1; flush <= 2; flush++)
        {
            for (int i = 0; i < keys; i++)
                db.Put(B("k" + i.ToString(format, System.Globalization.CultureInfo.InvariantCulture)), B($"{name}-{flush}"), family);

            db.Flush(family);
        }
    }

    // Level 6: a000-a099 and x000-x099; level 5: a (a010-a019) and b (x010-x019, or 50 larger values);
    // level 0, oldest first: a011, a012, a013, a014, each a table of its own that overlaps a.
    private void WriteLayout(DbOptions options, bool largeB)
    {
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            LevelTests.Write(db, [.. Enumerable.Range(0, 100).Select(i => ($"a{i:D3}", "6" + new string('s', 100)))]);
            LevelTests.Write(db, [.. Enumerable.Range(0, 100).Select(i => ($"x{i:D3}", "6" + new string('s', 100)))]);
            LevelTests.Write(db, [.. Enumerable.Range(10, 10).Select(i => ($"a{i:D3}", "5" + new string('m', 100)))]);
            LevelTests.Write(db, [.. Enumerable.Range(10, largeB ? 50 : 10).Select(i => ($"x{i:D3}", "5" + new string('m', 100)))]);
            for (int i = 11; i <= 14; i++)
                LevelTests.Write(db, ($"a{i:D3}", "0"));
        }

        LevelTests.Place(DbPath, 6, 6, 5, 5, 0, 0, 0, 0);
    }

    private static bool Throws(Action action)
    {
        try
        {
            action();
            return false;
        }
        catch (SolariumException)
        {
            return true;
        }
    }

    private static void Check(SolariumDb db, string[] names, SortedDictionary<string, string>[] models)
    {
        for (int f = 0; f < names.Length; f++)
        {
            ColumnFamily family = db.GetColumnFamily(names[f]);
            List<string> expected = [.. Enumerable.Range(0, models.Length).Where(w => w % names.Length == f)
                .SelectMany(w => models[w]).OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => $"{pair.Key}={pair.Value}")];
            List<string> scanned = [];
            using (Iterator iterator = db.NewIterator(family))
                for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                    scanned.Add($"{S(iterator.Key())}={S(iterator.Value())}");

            Assert.Equal(expected, scanned);
        }
    }
}
