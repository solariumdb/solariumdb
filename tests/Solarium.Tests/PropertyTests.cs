using static Solarium.Tests.CompactionTests;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// RocksDB properties through <see cref="SolariumDb.GetProperty"/>, with the values a RocksDB probe
/// gave for the same steps, and the live SSTables through <see cref="SolariumDb.GetLiveFilesMetaData"/>.
/// </summary>
public sealed class PropertyTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();
    private readonly ManualResetEventSlim _held = new();
    private readonly ManualResetEventSlim _release = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_active_memtable_counts_every_put_delete_range_delete_and_overwrite_until_a_flush(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options(createMissingColumnFamilies: true), ["other"]);
        ColumnFamily other = db.GetColumnFamily("other");
        Assert.Equal("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));
        Assert.Equal("0", db.GetProperty("rocksdb.cur-size-all-mem-tables"));

        for (int i = 0; i < 10; i++)
            db.Put(B($"k{i:D2}"), B("v"));

        db.Delete(B("k03"));
        db.DeleteRange(B("k05"), B("k08"));
        db.Put(B("k01"), B("v2"));

        // RocksDB gives 13 for the same writes: a range delete and an overwrite are entries too.
        Assert.Equal("13", db.GetProperty("rocksdb.num-entries-active-mem-table"));
        Assert.Equal("13", db.GetProperty("rocksdb.num-entries-active-mem-table", db.DefaultColumnFamily));
        Assert.Equal("0", db.GetProperty("rocksdb.num-entries-active-mem-table", other));
        Assert.True(long.Parse(db.GetProperty("rocksdb.cur-size-all-mem-tables")!) > 0);
        Assert.Equal("0", db.GetProperty("rocksdb.cur-size-all-mem-tables", other));
        Assert.Equal("0", db.GetProperty("rocksdb.num-immutable-mem-table"));

        db.Flush();
        Assert.Equal("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));
        Assert.Equal("0", db.GetProperty("rocksdb.cur-size-all-mem-tables"));
        Assert.Equal("0", db.GetProperty("rocksdb.num-immutable-mem-table"));
        Assert.Equal("0", db.GetProperty("rocksdb.mem-table-flush-pending"));
        Assert.Equal("0", db.GetProperty("rocksdb.num-running-flushes"));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Table_properties_follow_flushes_moves_and_drops_and_survive_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options(createMissingColumnFamilies: true));
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            Assert.Equal("0", db.GetProperty("rocksdb.live-sst-files-size"));
            Assert.Equal("0", db.GetProperty("rocksdb.estimate-table-readers-mem"));

            PutRange(db, 0, 10, "a");
            db.Flush();
            Assert.Equal("1", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.Equal("0", db.GetProperty("rocksdb.num-files-at-level6"));
            Assert.Equal(TableBytesOnDisk(), long.Parse(db.GetProperty("rocksdb.live-sst-files-size")!));
            Assert.True(long.Parse(db.GetProperty("rocksdb.estimate-table-readers-mem")!) > 0);
            Assert.Equal("0", db.GetProperty("rocksdb.live-sst-files-size", other));
            Assert.Equal("0", db.GetProperty("rocksdb.compaction-pending"));

            // Automatic compactions are off, and the trigger of 4 still makes one pending, as in RocksDB.
            for (int i = 1; i < 4; i++)
            {
                PutRange(db, i * 10, (i * 10) + 10, "a");
                db.Flush();
            }

            Assert.Equal("4", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.Equal("1", db.GetProperty("rocksdb.compaction-pending"));
            Assert.Equal("0", db.GetProperty("rocksdb.compaction-pending", other));

            // The four files do not overlap, so they move to level 6 as they are.
            long tableBytes = TableBytesOnDisk();
            long readerBytes = long.Parse(db.GetProperty("rocksdb.estimate-table-readers-mem")!);
            db.CompactRange(null, null);
            Assert.Equal("0", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.Equal("4", db.GetProperty("rocksdb.num-files-at-level6"));
            for (int level = 1; level <= 5; level++)
                Assert.Equal("0", db.GetProperty($"rocksdb.num-files-at-level{level}"));

            Assert.Equal("0", db.GetProperty("rocksdb.compaction-pending"));
            Assert.Equal(tableBytes, long.Parse(db.GetProperty("rocksdb.live-sst-files-size")!));
            Assert.Equal(readerBytes.ToString(), db.GetProperty("rocksdb.estimate-table-readers-mem"));

            // The two lowest files lie wholly in the range; the drop leaves the other two.
            db.DeleteFilesInRange(B("k000"), B("k019"));
            Assert.Equal("2", db.GetProperty("rocksdb.num-files-at-level6"));
            Assert.True(long.Parse(db.GetProperty("rocksdb.live-sst-files-size")!) < tableBytes);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            Assert.Equal("0", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.Equal("2", db.GetProperty("rocksdb.num-files-at-level6"));
            Assert.Equal(TableBytesOnDisk(), long.Parse(db.GetProperty("rocksdb.live-sst-files-size")!));
            Assert.True(long.Parse(db.GetProperty("rocksdb.estimate-table-readers-mem")!) > 0);

            db.DeleteFilesInRange(null, null);
            Assert.Equal("0", db.GetProperty("rocksdb.num-files-at-level6"));
            Assert.Equal("0", db.GetProperty("rocksdb.live-sst-files-size"));
            Assert.Equal("0", db.GetProperty("rocksdb.estimate-table-readers-mem"));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_held_flush_runs_and_the_memtable_behind_it_is_pending(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.EndsWith(".sst", StringComparison.Ordinal) };
        DbOptions options = scope.Options() with
        {
            Environment = hooks,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { MaxWriteBufferNumber = 3 },
        };

        using SolariumDb db = SolariumDb.Open(DbPath, options);
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        hooks.FlushGate.Reset();

        db.Put(B("a"), B("1"));
        db.Flush(wait: false);
        Assert.True(hooks.FlushStarted.Wait(Timeout), "no flush started");
        Assert.Equal("1", db.GetProperty("rocksdb.num-running-flushes"));
        Assert.Equal("1", db.GetProperty("rocksdb.num-immutable-mem-table"));
        Assert.Equal("0", db.GetProperty("rocksdb.mem-table-flush-pending"));

        db.Put(B("b"), B("1"));
        db.Flush(wait: false);
        Assert.Equal("2", db.GetProperty("rocksdb.num-immutable-mem-table"));
        Assert.Equal("1", db.GetProperty("rocksdb.mem-table-flush-pending"));
        Assert.True(long.Parse(db.GetProperty("rocksdb.cur-size-all-mem-tables")!) > 0);

        hooks.FlushGate.Set();
        Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-immutable-mem-table") == "0", Timeout), "the flushes did not end");
        Assert.Equal("0", db.GetProperty("rocksdb.num-running-flushes"));
        Assert.Equal("0", db.GetProperty("rocksdb.mem-table-flush-pending"));
        Assert.Equal("2", db.GetProperty("rocksdb.num-files-at-level0"));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_held_compaction_runs_and_its_inputs_do_not_count_toward_a_pending_one(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 },
            BeforeCompaction = () =>
            {
                _held.Set();
                _release.Wait();
            },
        };

        using SolariumDb db = SolariumDb.Open(DbPath, options);
        try
        {
            foreach (string key in new[] { "a", "b" })
            {
                db.Put(B(key), B("1"));
                db.Flush();
            }

            Assert.True(_held.Wait(Timeout), "no compaction started");
            Assert.Equal("1", db.GetProperty("rocksdb.num-running-compactions"));
            Assert.Equal("2", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.Equal("0", db.GetProperty("rocksdb.compaction-pending"));

            foreach (string key in new[] { "c", "d" })
            {
                db.Put(B(key), B("1"));
                db.Flush();
            }

            Assert.Equal("1", db.GetProperty("rocksdb.compaction-pending"));
        }
        finally
        {
            _release.Set();
        }

        Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-files-at-level0") == "0", Timeout), "level 0 did not empty");
        Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-running-compactions") == "0", Timeout), "the compaction did not end");
        Assert.Equal("0", db.GetProperty("rocksdb.compaction-pending"));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_stall_properties_give_the_delayed_write_rate_and_the_stop(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options(createMissingColumnFamilies: true) with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                Level0FileNumCompactionTrigger = 1,
                Level0SlowdownWritesTrigger = 2,
                Level0StopWritesTrigger = 3,
            },
            DelayedWriteRate = 64 * 1024,
            BeforeCompaction = () =>
            {
                _held.Set();
                _release.Wait();
            },
        };

        using SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]);
        ColumnFamily other = db.GetColumnFamily("other");
        try
        {
            Assert.Equal("0", db.GetProperty("rocksdb.is-write-stopped"));
            Assert.Equal("0", db.GetProperty("rocksdb.actual-delayed-write-rate"));

            db.Put(B("a"), B("1"));
            db.Flush();
            Assert.True(_held.Wait(Timeout), "no compaction started");
            db.Put(B("b"), B("1"));
            db.Flush();

            // The stall belongs to the database: every family reports it.
            Assert.Equal("65536", db.GetProperty("rocksdb.actual-delayed-write-rate"));
            Assert.Equal("65536", db.GetProperty("rocksdb.actual-delayed-write-rate", other));
            Assert.Equal("0", db.GetProperty("rocksdb.is-write-stopped"));

            db.Put(B("c"), B("1"));
            db.Flush();
            Assert.Equal("1", db.GetProperty("rocksdb.is-write-stopped", other));
            Assert.Equal("0", db.GetProperty("rocksdb.actual-delayed-write-rate"));
        }
        finally
        {
            _release.Set();
        }

        // A compaction installs its version, then sets the stall condition from it, so a reader can see level 0
        // empty before the stall clears.
        Assert.True(SpinWait.SpinUntil(() => db.GetProperty("rocksdb.num-files-at-level0") == "0" && db.GetProperty("rocksdb.is-write-stopped") == "0"
            && db.GetProperty("rocksdb.actual-delayed-write-rate") == "0", Timeout), "level 0 did not empty, or the stall did not clear");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Unknown_names_levels_past_6_and_the_block_cache_return_null_and_a_disposed_database_throws(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SolariumDb db = SolariumDb.Open(DbPath, scope.Options());
        string[] unsupported =
        [
            "rocksdb.no-such-property", "rocksdb.stats", "num-entries-active-mem-table", "rocksdb.num-files-at-level7",
            "rocksdb.num-files-at-level", "rocksdb.num-files-at-levelx", "rocksdb.num-files-at-level-1",
            "rocksdb.block-cache-capacity", "rocksdb.block-cache-usage", "rocksdb.block-cache-pinned-usage", "",
        ];
        foreach (string name in unsupported)
            Assert.Null(db.GetProperty(name));

        using (TempDirectory elsewhere = new())
        using (SolariumDb second = SolariumDb.Open(elsewhere.Combine("db"), scope.Options()))
            Assert.Throws<ArgumentException>(() => db.GetProperty("rocksdb.num-files-at-level0", second.DefaultColumnFamily));

        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.GetProperty("rocksdb.num-files-at-level0"));
        Assert.Throws<ObjectDisposedException>(() => db.GetLiveFilesMetaData());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Live_file_metadata_names_each_table_on_disk_with_its_family_level_keys_and_sequence_numbers(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options(createMissingColumnFamilies: true));
        List<LiveFileMetaData> before;
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            Assert.Empty(db.GetLiveFilesMetaData());

            db.Put(B("k1"), B("v"));
            db.Put(B("k2"), B("v"));
            db.Delete(B("k3"));
            db.Flush();
            db.Put(B("x"), B("v"), other);
            db.Flush(other);

            LiveFileMetaData table = Assert.Single(db.GetLiveFilesMetaData(), f => f.ColumnFamilyName == ColumnFamily.DefaultName);
            Assert.Equal(0, table.Level);
            Assert.Equal($"/{table.FileNumber:D6}.sst", table.Name);
            Assert.Equal(new FileInfo(DbPath + table.Name).Length, table.Size);
            Assert.Equal("k1", S(table.SmallestKey));
            Assert.Equal("k3", S(table.LargestKey));
            Assert.Equal((1L, 3L, 3L), (table.SmallestSequenceNumber, table.LargestSequenceNumber, table.EntryCount));
            Assert.Single(db.GetLiveFilesMetaData(), f => f.ColumnFamilyName == "other" && S(f.SmallestKey) == "x");

            // The keys are copies: changing them changes nothing the next call returns.
            table.SmallestKey[0] = (byte)'z';
            Assert.Equal("k1", S(db.GetLiveFilesMetaData().Single(f => f.FileNumber == table.FileNumber).SmallestKey));

            // A move keeps the file and its number; only the level changes.
            db.CompactRange(null, null);
            LiveFileMetaData moved = Assert.Single(db.GetLiveFilesMetaData(), f => f.ColumnFamilyName == ColumnFamily.DefaultName);
            Assert.Equal((table.FileNumber, 6), (moved.FileNumber, moved.Level));
            before = db.GetLiveFilesMetaData();
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            List<LiveFileMetaData> after = db.GetLiveFilesMetaData();
            Assert.Equal(before.Select(f => (f.ColumnFamilyName, f.Level, f.Name, f.Size, S(f.SmallestKey), S(f.LargestKey))),
                after.Select(f => (f.ColumnFamilyName, f.Level, f.Name, f.Size, S(f.SmallestKey), S(f.LargestKey))));

            db.DeleteFilesInRange(null, null);
            LiveFileMetaData left = Assert.Single(db.GetLiveFilesMetaData());
            Assert.Equal("other", left.ColumnFamilyName);
        }
    }

    /// <summary>Releases a held compaction and deletes the temporary directory.</summary>
    public void Dispose()
    {
        _release.Set();
        _held.Dispose();
        _release.Dispose();
        _directory.Dispose();
    }

    private long TableBytesOnDisk() => Directory.GetFiles(DbPath, "*.sst").Sum(path => new FileInfo(path).Length);
}
