using System.Buffers;
using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.CompactionTests;
using static Solarium.Tests.DeleteFilesInRangeTests;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>Flush to SSTables, WAL retention, recovery from crashes and damage, and format migration.</summary>
public sealed class FlushTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    private static readonly ColumnFamilyOptions Small = new() { WriteBufferSize = 16 * 1024, MaxWriteBufferNumber = 2 };

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_flush_moves_data_to_an_sstable_and_a_reopen_keeps_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            for (int i = 0; i < 100; i++)
                db.Put(B($"key{i:D3}"), B($"value{i}"));

            db.Flush();
            Assert.Equal(1, db.FlushCount);
            string table = Assert.Single(Directory.GetFiles(DbPath, "*.sst"));
            Assert.Equal(new FileInfo(table).Length, db.FlushBytesWritten);

            // The next write group rotates the WAL; the flushed file then goes.
            db.Put(B("after"), B("flush"));
            Assert.Single(db.WalFileNumbers);

            Assert.Equal("value42", S(db.Get(B("key042"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal(101, db.LatestSequenceNumber);
            Assert.Equal("value42", S(db.Get(B("key042"))));
            Assert.Equal("flush", S(db.Get(B("after"))));
            Assert.Equal(101, CountKeys(db));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Tombstones_hide_data_across_memtables_and_sstables(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            foreach (string key in new[] { "a", "b", "c", "d", "e" })
                db.Put(B(key), B(key + "1"));

            db.Flush();
            db.Delete(B("b"));
            db.DeleteRange(B("c"), B("e"));
            db.Flush();
            db.Put(B("c"), B("c2"));

            AssertState(db);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            AssertState(db);
        }

        static void AssertState(SolariumDb db)
        {
            Assert.Equal(["a1", null, "c2", null, "e1"], db.MultiGet([B("a"), B("b"), B("c"), B("d"), B("e")]).Select(S));

            using Iterator iterator = db.NewIterator();
            List<string> forward = new();
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                forward.Add(S(iterator.Key()));

            Assert.Equal(["a", "c", "e"], forward);
            iterator.SeekToLast();
            Assert.Equal("e", S(iterator.Key()));
            iterator.Prev();
            Assert.Equal("c", S(iterator.Key()));
        }
    }

    [Fact]
    public void An_iterator_keeps_its_snapshot_across_a_flush()
    {
        using SolariumDb db = SolariumDb.Open(DbPath);
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("1"));

        using Iterator iterator = db.NewIterator();
        db.Flush();
        db.Put(B("a"), B("2"));
        db.Delete(B("b"));
        db.Flush();

        iterator.SeekToFirst();
        Assert.Equal(("a", "1"), (S(iterator.Key()), S(iterator.Value())));
        iterator.Next();
        Assert.Equal(("b", "1"), (S(iterator.Key()), S(iterator.Value())));
        iterator.Next();
        Assert.False(iterator.Valid());

        Assert.Equal("2", S(db.Get(B("a"))));
        Assert.Null(db.Get(B("b")));
    }

    [Fact]
    public void A_full_memtable_flushes_by_itself()
    {
        DbOptions options = new DbOptions { DefaultColumnFamilyOptions = Small };
        byte[] value = new byte[1000];

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            for (int i = 0; i < 500; i++)
                db.Put(B($"key{i:D4}"), value);

            Assert.True(SpinWait.SpinUntil(() => db.FlushCount >= 10, TimeSpan.FromSeconds(10)), $"{db.FlushCount} flushes");
            Assert.True(db.WalFileNumbers.Length <= 3, $"{db.WalFileNumbers.Length} live WAL files");
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal(500, CountKeys(db));
            Assert.Equal(500, db.LatestSequenceNumber);
        }
    }

    [Fact]
    public void A_quiet_column_family_does_not_keep_wal_files_alive()
    {
        DbOptions options = new DbOptions
        {
            CreateMissingColumnFamilies = true,
            MaxTotalWalSize = 64 * 1024,
            ColumnFamilyOptions = new Dictionary<string, ColumnFamilyOptions> { ["busy"] = Small },
        };

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["busy", "quiet"]))
        {
            ColumnFamily busy = db.GetColumnFamily("busy");
            ColumnFamily quiet = db.GetColumnFamily("quiet");
            db.Put(B("only"), B("write"), quiet);

            for (int i = 0; i < 1000; i++)
                db.Put(B($"key{i:D4}"), new byte[500], busy);

            Assert.True(SpinWait.SpinUntil(() => quiet.Version.Level0.Length == 1, TimeSpan.FromSeconds(10)), "the quiet family was not flushed");
            Assert.True(db.WalFileNumbers.Length <= 4, $"{db.WalFileNumbers.Length} live WAL files");
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal("write", S(db.Get(B("only"), db.GetColumnFamily("quiet"))));
            Assert.Equal(1000, CountKeys(db, db.GetColumnFamily("busy")));
        }
    }

    [Fact]
    public async Task Writes_wait_when_every_memtable_is_full()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance) { GatedPaths = path => path.EndsWith(".sst", StringComparison.Ordinal) };
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks, DefaultColumnFamilyOptions = Small });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();

        hooks.FlushGate.Reset();
        Task writer = Task.Run(() =>
        {
            for (int i = 0; i < 100; i++)
                db.Put(B($"key{i:D3}"), new byte[1000]);
        });

        // The first memtable fills and its flush holds; the second fills and the writer waits.
        Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));
        await Task.Delay(300);
        Assert.False(writer.IsCompleted);

        hooks.FlushGate.Set();
        await writer.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(100, CountKeys(db));
    }

    [Fact]
    public void A_flush_without_wait_returns_before_the_table_is_written()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance) { GatedPaths = path => path.EndsWith(".sst", StringComparison.Ordinal) };
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        db.Put(B("k"), B("v"));

        hooks.FlushGate.Reset();
        db.Flush(wait: false);
        Assert.Equal(0, db.FlushCount);
        Assert.Equal("v", S(db.Get(B("k"))));

        hooks.FlushGate.Set();
        Assert.True(SpinWait.SpinUntil(() => db.FlushCount == 1, TimeSpan.FromSeconds(10)));
        Assert.Equal("v", S(db.Get(B("k"))));
    }

    [Fact]
    public void A_flush_failure_stops_writes_but_not_reads()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance) { FailingPaths = path => path.EndsWith(".sst", StringComparison.Ordinal) };
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        db.Put(B("k"), B("v"));

        hooks.FailWrites = true;
        SolariumException failed = Assert.Throws<SolariumException>(() => db.Flush());
        Assert.IsType<IOException>(failed.InnerException);
        Assert.Contains("no more writes", Assert.Throws<SolariumException>(() => db.Put(B("k2"), B("v"))).Message);

        Assert.Equal("v", S(db.Get(B("k"))));
        Assert.Empty(Directory.GetFiles(DbPath, "*.sst"));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_flush_writes_the_newest_version_of_each_key_and_drops_what_a_newer_tombstone_of_its_memtable_covers(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options());

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("k1"), B("1"));
            db.Put(B("k1"), B("2"));
            db.Put(B("k1"), B("3"));
            db.Put(B("k2"), B("1"));
            db.Delete(B("k2"));
            foreach (string key in new[] { "r1", "r2", "r3", "r4", "r5" })
                db.Put(B(key), B("old"));

            db.Delete(B("r3"));
            db.DeleteRange(B("r1"), B("r4"));
            db.Put(B("r2"), B("new"));
            db.Flush();

            // k1 = 3, the delete of k2, r2 = new, r4 and r5; the tombstone stays for older tables.
            TableReader table = Assert.Single(db.DefaultColumnFamily.Version.Level0);
            Assert.Equal(5, table.Properties.EntryCount);
            Assert.Single(table.Tombstones);
            AssertReads(db);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
            AssertReads(db);

        static void AssertReads(SolariumDb db)
        {
            Assert.Equal("3", S(db.Get(B("k1"))));
            Assert.Null(db.Get(B("k2")));
            Assert.Equal(["k1=3", "r2=new", "r4=old", "r5=old"], Scan(db));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_delete_that_no_tombstone_covers_reaches_its_table_and_hides_the_older_table(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options());

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("1"));
            db.Flush();
            db.Put(B("a"), B("2"));
            db.Delete(B("a"));
            db.Flush();

            TableReader newest = db.DefaultColumnFamily.Version.Level0[0];
            Assert.Equal(1, newest.Properties.EntryCount);
            Assert.Null(db.Get(B("a")));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Null(db.Get(B("a")));
            Assert.Equal(["b=1"], Scan(db));
        }
    }

    // A flush keeps only the newest version of a key, so a table that held an entry no reader could
    // see yet would leave a reader of the new version with no version of that key at all.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Readers_beside_four_writers_never_miss_a_key_across_flushes_that_keep_only_its_newest_version(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 8 * 1024, MaxWriteBufferNumber = 3 },
        };

        const int Writers = 4;
        const int KeysPerWriter = 50;
        const int Rounds = 100;
        using SolariumDb db = SolariumDb.Open(DbPath, options);
        for (int i = 0; i < Writers * KeysPerWriter; i++)
            db.Put(B($"k{i:D3}"), B($"0:{i}"));

        int done = 0;
        Exception? failure = null;
        Thread[] readers = new Thread[4];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r;
            readers[r] = new Thread(() =>
            {
                try
                {
                    // Each key only moves to a later round, and every key exists from the start.
                    Random random = new(seed);
                    int[] seen = new int[Writers * KeysPerWriter];
                    while (Volatile.Read(ref done) == 0)
                    {
                        int i = random.Next(seen.Length);
                        byte[]? value = db.Get(B($"k{i:D3}"));
                        Assert.True(value is not null, $"key {i} is missing");
                        int round = int.Parse(S(value)!.Split(':')[0]);
                        Assert.True(round >= seen[i], $"key {i}: round {round} after round {seen[i]}");
                        seen[i] = round;
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            });
            readers[r].Start();
        }

        Thread[] writers = new Thread[Writers];
        for (int w = 0; w < Writers; w++)
        {
            int first = w * KeysPerWriter;
            writers[w] = new Thread(() =>
            {
                for (int round = 1; round <= Rounds; round++)
                    for (int i = first; i < first + KeysPerWriter; i++)
                        db.Put(B($"k{i:D3}"), B($"{round}:{i}:" + new string('v', 40)));
            });
            writers[w].Start();
        }

        foreach (Thread writer in writers)
            writer.Join();

        Volatile.Write(ref done, 1);
        foreach (Thread reader in readers)
            reader.Join();

        Assert.Null(failure);
        Assert.True(db.FlushCount > 20, $"only {db.FlushCount} flushes");
        for (int i = 0; i < Writers * KeysPerWriter; i++)
            Assert.StartsWith($"{Rounds}:", S(db.Get(B($"k{i:D3}"))));
    }

    // Kommander's floor pass writes the floor and a range tombstone from the active memtable's first
    // row to the floor in one batch, so the rows that die in memory are dropped by the flush.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_log_rows_below_a_floor_that_a_tombstone_of_their_memtable_covers_never_reach_a_table(string environment)
    {
        const int Partition = 1;
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Manual(scope.Options(createMissingColumnFamilies: true));

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["metadata"]))
        {
            ColumnFamily metadata = db.GetColumnFamily("metadata");
            for (long id = 1; id <= 100; id++)
                db.Put(LogKey(Partition, id), B($"row{id}"));

            db.Write(new WriteBatch()
                .Put(B("raft_compaction_floor_p1"), B("91"), metadata)
                .DeleteRange(LogKey(Partition, 1), LogKey(Partition, 91)));
            db.Flush();

            LiveFileMetaData file = Assert.Single(db.GetLiveFilesMetaData(), f => f.ColumnFamilyName == ColumnFamily.DefaultName);
            Assert.Equal(10, file.EntryCount);
            Assert.Equal(LogKey(Partition, 1), file.SmallestKey);
            Assert.Equal(LogKey(Partition, 100), file.LargestKey);
            AssertRows(db);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["metadata"]))
        {
            AssertRows(db);
            Assert.Equal("91", S(db.Get(B("raft_compaction_floor_p1"), db.GetColumnFamily("metadata"))));
        }

        static void AssertRows(SolariumDb db)
        {
            Assert.Null(db.Get(LogKey(Partition, 90)));
            Assert.Equal("row91", S(db.Get(LogKey(Partition, 91))));
            Assert.Equal(10, Scan(db).Count);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_flush_whose_manifest_save_fails_after_the_replace_keeps_its_table(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        int fail = 0;
        DbOptions options = scope.Options() with
        {
            AfterManifestSave = () =>
            {
                if (Interlocked.Exchange(ref fail, 0) == 1)
                    throw new IOException("injected directory flush failure");
            },
        };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("k"), B("v"));
            Volatile.Write(ref fail, 1);
            Assert.Throws<SolariumException>(() => db.Flush());
            Assert.Throws<SolariumException>(() => db.Put(B("k2"), B("v")));
            Assert.Equal("v", S(db.Get(B("k"))));
        }

        // The manifest on disk lists the table and a flush point past k, so the table must be there.
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal("v", S(db.Get(B("k"))));
            LiveFileMetaData table = Assert.Single(db.GetLiveFilesMetaData());
            Assert.Equal([table.FileNumber], Directory.GetFiles(DbPath, "*.sst").Select(p => long.Parse(Path.GetFileNameWithoutExtension(p))));
        }
    }

    [Fact]
    public void Leftovers_of_a_crashed_flush_are_deleted_at_open()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
        }

        // A table that was written but never reached the manifest, and a half-written manifest.
        File.WriteAllBytes(Path.Combine(DbPath, "000999.sst"), new byte[100]);
        File.WriteAllBytes(Path.Combine(DbPath, "MANIFEST.tmp"), new byte[10]);

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal("v", S(db.Get(B("k"))));
        }

        Assert.False(File.Exists(Path.Combine(DbPath, "000999.sst")));
        Assert.False(File.Exists(Path.Combine(DbPath, "MANIFEST.tmp")));
    }

    [Fact]
    public void A_wal_file_that_outlives_the_flush_of_its_data_is_not_applied_twice()
    {
        string backup = _directory.Combine("backup");
        Directory.CreateDirectory(backup);

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            db.Put(B("a"), B("1"));
            db.DeleteRange(B("b"), B("c"));
            db.Put(B("b2"), B("1"));
            foreach (string wal in Directory.GetFiles(DbPath, "*.log"))
                File.Copy(wal, Path.Combine(backup, Path.GetFileName(wal)));

            db.Flush();
            db.Put(B("a"), B("2"));
            db.Put(B("b1"), B("2"));
        }

        // A crash after the manifest save and before the file deletion leaves the old WAL file behind.
        foreach (string wal in Directory.GetFiles(backup))
            File.Copy(wal, Path.Combine(DbPath, Path.GetFileName(wal)), overwrite: true);

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            // Replaying the old range tombstone again would hide b1.
            Assert.Equal(["2", "2", "1"], db.MultiGet([B("a"), B("b1"), B("b2")]).Select(S));
            Assert.Equal(3, CountKeys(db));
            Assert.Equal(5, db.LatestSequenceNumber);
        }
    }

    [Fact]
    public void A_missing_wal_file_with_unflushed_data_refuses_to_open()
    {
        string oldest;
        DbOptions options = new DbOptions { CreateMissingColumnFamilies = true };
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["quiet"]))
        {
            db.Put(B("unflushed"), B("1"), db.GetColumnFamily("quiet"));
            oldest = Directory.GetFiles(DbPath, "*.log").Single();
            db.Put(B("k"), B("1"));
            db.Flush();
            db.Put(B("k"), B("2"));
            Assert.Equal(2, db.WalFileNumbers.Length);
        }

        File.Delete(oldest);

        SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains("is missing", e.Message);
    }

    [Fact]
    public void Damage_to_a_wal_file_that_a_newer_one_follows_refuses_to_open()
    {
        string oldest;
        DbOptions options = new DbOptions { CreateMissingColumnFamilies = true };
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["quiet"]))
        {
            db.Put(B("unflushed"), B("1"), db.GetColumnFamily("quiet"));
            oldest = Directory.GetFiles(DbPath, "*.log").Single();
            db.Put(B("k"), B("1"));
            db.Flush();
            db.Put(B("k"), B("2"));
        }

        byte[] bytes = File.ReadAllBytes(oldest);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(oldest, bytes);

        SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains("a newer WAL file follows", e.Message);
    }

    [Fact]
    public void A_damaged_sstable_refuses_to_open()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
        }

        string table = Directory.GetFiles(DbPath, "*.sst").Single();
        byte[] bytes = File.ReadAllBytes(table);
        bytes[^20] ^= 0xFF;
        File.WriteAllBytes(table, bytes);

        SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains(Path.GetFileName(table), e.Message);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_rotation_whose_new_file_fails_stops_writes_and_flushes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { FailingPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        RotateWithFailure(hooks, () => hooks.FailWrites = true, () => hooks.FailWrites = false);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_rotation_whose_directory_flush_fails_stops_writes_and_flushes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        RotateWithFailure(hooks, () => hooks.FailDirectoryFlushes = true, () => hooks.FailDirectoryFlushes = false);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_flush_that_fails_at_the_close_is_reported_after_the_close_completes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { FailingPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { Environment = hooks });
        db.Put(B("a"), B("1"));

        hooks.FailFlushes = true;
        SolariumException e = Assert.Throws<SolariumException>(db.Dispose);
        Assert.Contains("flush of the WAL at the close", e.Message);
        hooks.FailFlushes = false;

        // The lock is released and the write, which the OS holds, is there.
        using SolariumDb reopened = SolariumDb.Open(DbPath, scope.Options());
        Assert.Equal("1", S(reopened.Get(B("a"))));
    }

    [Fact]
    public void A_first_open_that_a_crash_cut_short_after_its_temporary_format_file_opens_again()
    {
        Directory.CreateDirectory(DbPath);
        File.WriteAllText(Path.Combine(DbPath, "FORMAT.tmp"), "solarium-for");

        using (SolariumDb db = SolariumDb.Open(DbPath))
            db.Put(B("a"), B("1"));

        Assert.False(File.Exists(Path.Combine(DbPath, "FORMAT.tmp")));
        Assert.Equal("solarium-format 5", File.ReadAllText(Path.Combine(DbPath, "FORMAT")));
        using SolariumDb reopened = SolariumDb.Open(DbPath);
        Assert.Equal("1", S(reopened.Get(B("a"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_larger_than_the_write_buffer_replays_into_tables_not_into_one_memtable(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions large = scope.Options(createMissingColumnFamilies: true) with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 4 * 1024 * 1024, DisableAutoCompactions = true },
        };
        DbOptions small = large with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, DisableAutoCompactions = true },
        };

        using (SolariumDb db = SolariumDb.Open(DbPath, large, ["other"]))
        {
            for (int i = 0; i < 2000; i++)
                db.Write(new WriteBatch().Put(B($"k{i:D4}"), B(new string('v', 300))).Put(B($"o{i:D4}"), B("1"), db.GetColumnFamily("other")));

            Assert.Equal("0", db.GetProperty("rocksdb.num-files-at-level0"));
        }

        for (int open = 0; open < 2; open++)
        {
            using SolariumDb db = SolariumDb.Open(DbPath, small, ["other"]);
            Assert.True(long.Parse(db.GetProperty("rocksdb.num-files-at-level0")!) >= 8, $"{db.GetProperty("rocksdb.num-files-at-level0")} tables");
            Assert.True(long.Parse(db.GetProperty("rocksdb.cur-size-all-mem-tables")!) < 64 * 1024 + 1024);
            Assert.Equal(4000, db.LatestSequenceNumber);
            Assert.Equal(2000, Scan(db).Count);
            Assert.Equal("1", S(db.Get(B("o1999"), db.GetColumnFamily("other"))));
        }
    }

    [Fact]
    public void A_format_2_directory_replays_its_version_1_wal_and_moves_to_the_current_format()
    {
        // A database whose WAL file is empty, then that file in version 1, as format 2 wrote it.
        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
        }

        string wal = Assert.Single(Directory.GetFiles(DbPath, "*.log"));
        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, 1, [new WalOperation(OperationKind.Put, 0, B("a"), B("1"))], version: 1);
        WriteAheadLog.EncodeBatch(records, 2, [new WalOperation(OperationKind.Put, 0, B("b"), B("2"))], version: 1);
        File.WriteAllBytes(wal, records.WrittenSpan.ToArray());
        File.WriteAllText(Path.Combine(DbPath, "FORMAT"), "solarium-format 2");

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal(["a=1", "b=2"], Scan(db));
            Assert.Equal(2, db.LatestSequenceNumber);
            db.Put(B("c"), B("3"));
        }

        Assert.Equal("solarium-format 5", File.ReadAllText(Path.Combine(DbPath, "FORMAT")));
        using SolariumDb reopened = SolariumDb.Open(DbPath);
        Assert.Equal(["a=1", "b=2", "c=3"], Scan(reopened));
    }

    [Fact]
    public void A_format_1_directory_is_migrated()
    {
        Directory.CreateDirectory(DbPath);
        File.WriteAllText(Path.Combine(DbPath, "FORMAT"), "solarium-format 1");
        File.WriteAllText(Path.Combine(DbPath, "COLUMN_FAMILIES"), "0\tdefault\n1\tother\n");

        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, 1, [new WalOperation(OperationKind.Put, 0, B("a"), B("1")), new WalOperation(OperationKind.Put, 1, B("x"), B("9"))], version: 1);
        File.WriteAllBytes(Path.Combine(DbPath, "wal.log"), records.WrittenSpan.ToArray());

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal("1", S(db.Get(B("a"))));
            Assert.Equal("9", S(db.Get(B("x"), db.GetColumnFamily("other"))));
            Assert.Equal(2, db.LatestSequenceNumber);
            db.Put(B("a"), B("2"));
        }

        Assert.Equal("solarium-format 5", File.ReadAllText(Path.Combine(DbPath, "FORMAT")));
        Assert.False(File.Exists(Path.Combine(DbPath, "COLUMN_FAMILIES")));
        Assert.False(File.Exists(Path.Combine(DbPath, "wal.log")));

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal("2", S(db.Get(B("a"))));
            Assert.Equal("9", S(db.Get(B("x"), db.GetColumnFamily("other"))));
        }
    }

    [Fact]
    public void A_damaged_manifest_refuses_to_open()
    {
        using (SolariumDb.Open(DbPath))
        {
        }

        string manifest = Path.Combine(DbPath, "MANIFEST");
        byte[] bytes = File.ReadAllBytes(manifest);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(manifest, bytes);

        SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains("checksum", e.Message);
    }

    [Fact]
    public void Invalid_column_family_options_are_refused()
    {
        DbOptions options = new DbOptions { DefaultColumnFamilyOptions = new ColumnFamilyOptions { MaxWriteBufferNumber = 1 } };
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, options));
    }

    private static int CountKeys(SolariumDb db, ColumnFamily? family = null)
    {
        using Iterator iterator = db.NewIterator(family);
        int count = 0;
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            count++;

        return count;
    }

    // A flush switches the memtable, so the next write group rotates the WAL; fail sets a failure for
    // that rotation, and heal clears it. The rotation's group fails, every later write and flush
    // refuses, synced or not, and a reopen keeps what was written before.
    private void RotateWithFailure(HookedEnvironment hooks, Action fail, Action heal)
    {
        using (SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks }))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
            db.Put(B("b"), B("2"));
            db.Flush();

            fail();
            SolariumException rotation = Assert.Throws<SolariumException>(() => db.Put(B("c"), B("3")));
            Assert.Contains("WAL rotation", rotation.Message);
            heal();

            Assert.Contains("no more writes", Assert.Throws<SolariumException>(() => db.Put(B("d"), B("4"), options: WriteOptions.Synced)).Message);
            Assert.Contains("no more writes", Assert.Throws<SolariumException>(() => db.Put(B("e"), B("5"))).Message);
            Assert.Contains("no more writes", Assert.Throws<SolariumException>(() => db.Flush()).Message);
            Assert.Equal("2", S(db.Get(B("b"))));
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        Assert.Equal(["a=1", "b=2"], Scan(reopened));
        reopened.Put(B("f"), B("6"));
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();
}
