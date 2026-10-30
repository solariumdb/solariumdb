using Solarium.Engine;
using Solarium.IoUring;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>End-to-end tests of <see cref="SolariumDb"/>: open, write, read, group commit, failures, and reopen.</summary>
public sealed class SolariumDbTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Put_get_delete_and_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Null(db.Get(B("missing")));

            db.Put(B("k"), B("v1"));
            db.Put(B("k"), B("v2"), options: WriteOptions.Synced);
            db.Put(B("gone"), B("x"));
            db.Delete(B("gone"));

            Assert.Equal("v2", S(db.Get(B("k"))));
            Assert.Null(db.Get(B("gone")));
            Assert.Equal(4, db.LatestSequenceNumber);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal("v2", S(db.Get(B("k"))));
            Assert.Null(db.Get(B("gone")));
            Assert.Equal(4, db.LatestSequenceNumber);

            db.Put(B("after"), B("reopen"));
            Assert.Equal(5, db.LatestSequenceNumber);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Column_families_are_separate_key_spaces_and_persist(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options(createMissingColumnFamilies: true), ["metadata", "shard0"]))
        {
            ColumnFamily metadata = db.GetColumnFamily("metadata");
            ColumnFamily shard = db.GetColumnFamily("shard0");

            db.Write(new WriteBatch()
                .Put(B("k"), B("default"))
                .Put(B("k"), B("metadata"), metadata)
                .Put(B("k"), B("shard"), shard));

            Assert.Equal("default", S(db.Get(B("k"))));
            Assert.Equal("metadata", S(db.Get(B("k"), metadata)));
            Assert.Equal("shard", S(db.Get(B("k"), shard)));
        }

        // Every existing family opens, even when the caller does not name it.
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal(["default", "metadata", "shard0"], db.ColumnFamilies.Select(f => f.Name).Order());
            Assert.Equal("shard", S(db.Get(B("k"), db.GetColumnFamily("shard0"))));
        }
    }

    [Fact]
    public void Open_errors()
    {
        Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath, new DbOptions { CreateIfMissing = false }));

        SolariumException missingFamily = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath, columnFamilies: ["nope"]));
        Assert.Contains("CreateMissingColumnFamilies", missingFamily.Message);

        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, new DbOptions { CreateMissingColumnFamilies = true }, ["bad\tname"]));

        using (SolariumDb.Open(DbPath))
        {
            SolariumException locked = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
            Assert.Contains("in use", locked.Message);
        }

        string foreign = _directory.Combine("foreign");
        Directory.CreateDirectory(foreign);
        File.WriteAllText(Path.Combine(foreign, "SHARDS"), "4");
        SolariumException notOurs = Assert.Throws<SolariumException>(() => SolariumDb.Open(foreign));
        Assert.Contains("not a Solarium database", notOurs.Message);
    }

    [Fact]
    public void Misuse_errors()
    {
        using SolariumDb other = SolariumDb.Open(_directory.Combine("other"));
        SolariumDb db = SolariumDb.Open(DbPath);

        Assert.Throws<ArgumentException>(() => db.Put(B("k"), B("v"), other.DefaultColumnFamily));
        Assert.Throws<KeyNotFoundException>(() => db.GetColumnFamily("nope"));

        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.Get(B("k")));
        Assert.Throws<ObjectDisposedException>(() => db.Put(B("k"), B("v")));
        db.Dispose();
    }

    [Fact]
    public void Delete_range_hides_keys_in_the_range_until_they_are_written_again()
    {
        using SolariumDb db = SolariumDb.Open(DbPath);
        foreach (string key in new[] { "a", "b", "c", "d" })
            db.Put(B(key), B(key));

        db.DeleteRange(B("b"), B("d"));

        Assert.Equal("a", S(db.Get(B("a"))));
        Assert.Null(db.Get(B("b")));
        Assert.Null(db.Get(B("c")));
        Assert.Equal("d", S(db.Get(B("d"))));

        db.Put(B("c"), B("again"));
        Assert.Equal("again", S(db.Get(B("c"))));
        Assert.Equal([null, "again"], db.MultiGet([B("b"), B("c")]).Select(S));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_range_delete_whose_end_comes_before_its_begin_is_refused_and_an_empty_one_writes_nothing(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            foreach (string key in new[] { "a", "b", "c" })
                db.Put(B(key), B(key));

            long before = db.LatestSequenceNumber;
            Assert.Throws<ArgumentException>(() => db.DeleteRange(B("c"), B("a")));
            WriteBatch batch = new WriteBatch().Put(B("d"), B("d"));
            Assert.Throws<ArgumentException>(() => batch.DeleteRange(B("c"), B("a")));
            Assert.Equal(1, batch.Count);

            db.DeleteRange(B("b"), B("b"));
            Assert.Equal(before, db.LatestSequenceNumber);
            Assert.Equal("3", db.GetProperty("rocksdb.num-entries-active-mem-table"));

            // A flushed table and a compaction see only real ranges.
            db.DeleteRange(B("a"), B("b"));
            db.Flush();
            db.CompactRange(null, null);
            Assert.Null(db.Get(B("a")));
            Assert.Equal("b", S(db.Get(B("b"))));
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, scope.Options());
        Assert.Equal(["b=b", "c=c"], CompactionTests.Scan(reopened));
    }

    [Fact]
    public void A_torn_batch_is_lost_whole_in_every_column_family()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { CreateMissingColumnFamilies = true }, ["other"]))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            db.Write(new WriteBatch().Put(B("a"), B("1")).Put(B("x"), B("1"), other), WriteOptions.Synced);
            db.Write(new WriteBatch().Put(B("a"), B("2")).Put(B("x"), B("2"), other), WriteOptions.Synced);
        }

        string wal = Directory.GetFiles(DbPath, "*.log").Order().Last();
        using (FileStream stream = new(wal, FileMode.Open))
        {
            stream.SetLength(stream.Length - 5);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal("1", S(db.Get(B("a"))));
            Assert.Equal("1", S(db.Get(B("x"), db.GetColumnFamily("other"))));
            Assert.Equal(2, db.LatestSequenceNumber);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Wal_damage_that_later_records_show_was_flushed_refuses_to_open_and_damage_to_unflushed_records_is_cut_off(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (bool synced in new[] { true, false })
        {
            string path = _directory.Combine($"db-{synced}");
            using (SolariumDb db = SolariumDb.Open(path, scope.Options()))
            {
                for (int i = 0; i < 4; i++)
                    db.Put(B($"key{i}"), B("value"), options: synced ? WriteOptions.Synced : null);
            }

            // A byte of the second record changes, as damage to the disk, or a power loss, leaves it.
            string wal = Directory.GetFiles(path, "*.log").Order().Last();
            byte[] bytes = File.ReadAllBytes(wal);
            bytes[bytes.AsSpan().IndexOf(B("key1"))] ^= 0xFF;
            File.WriteAllBytes(wal, bytes);

            if (synced)
            {
                // The third record was appended after the flush that made the second one durable. The
                // refused open names the file and leaves no file open.
                HookedEnvironment hooks = new(scope.Environment);
                SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(path, scope.Options() with { Environment = hooks }));
                Assert.Contains("shows had been flushed", e.Message);
                Assert.Contains(wal, e.Message);
                Assert.Equal(0, hooks.OpenFileCount);
                continue;
            }

            using SolariumDb reopened = SolariumDb.Open(path, scope.Options());
            Assert.Equal(["key0=value"], CompactionTests.Scan(reopened));
            Assert.Equal(1, reopened.LatestSequenceNumber);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Readers_never_see_part_of_a_batch(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options());
        db.Write(new WriteBatch().Put(B("a"), B("0")).Put(B("b"), B("0")));

        using CancellationTokenSource stop = new();
        using ManualResetEventSlim readerRunning = new();
        int violations = 0;
        int checks = 0;

        Task reader = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                byte[]?[] pair = db.MultiGet([B("a"), B("b")]);
                if (S(pair[0]) != S(pair[1]))
                    Interlocked.Increment(ref violations);

                using Iterator iterator = db.NewIterator();
                iterator.SeekToFirst();
                string first = S(iterator.Value());
                iterator.Next();
                if (first != S(iterator.Value()))
                    Interlocked.Increment(ref violations);

                Interlocked.Increment(ref checks);
                readerRunning.Set();
            }
        });

        // On a machine with few cores the reader can start after every write is done, and then it
        // checks nothing. Start writing only when it runs, and require checks during the writes.
        Assert.True(readerRunning.Wait(TimeSpan.FromSeconds(30)), "The reader did not start.");
        int checksBeforeWrites = Volatile.Read(ref checks);
        int checksAtPause = checksBeforeWrites;

        for (int i = 1; i <= 3000; i++)
        {
            db.Write(new WriteBatch().Put(B("a"), B($"{i}")).Put(B("noise"), B($"{i}"), null).Put(B("b"), B($"{i}")));

            // With one CPU, the writer can keep it for every write, about 50 ms, and the reader
            // then checks nothing while they run. So the writer lets the reader check at least
            // once in each 250 writes.
            if (i % 250 == 0)
            {
                Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref checks) > checksAtPause, TimeSpan.FromSeconds(30)), "The reader stopped.");
                checksAtPause = Volatile.Read(ref checks);
            }
        }

        int checksDuringWrites = Volatile.Read(ref checks) - checksBeforeWrites;
        stop.Cancel();
        await reader;

        Assert.True(checksDuringWrites > 0, "No read overlapped the writes.");
        Assert.Equal(0, violations);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Concurrent_writers_from_many_threads_all_land(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Parallel.For(0, 16, new ParallelOptions { MaxDegreeOfParallelism = 16 }, t =>
            {
                for (int i = 0; i < 100; i++)
                    db.Put(B($"t{t:D2}:{i:D3}"), B($"{t}/{i}"), options: i % 2 == 0 ? WriteOptions.Synced : WriteOptions.Default);
            });

            Assert.Equal(1600, db.LatestSequenceNumber);
            Assert.True(db.WriteGroupCount <= 1600);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            for (int t = 0; t < 16; t++)
                for (int i = 0; i < 100; i++)
                    Assert.Equal($"{t}/{i}", S(db.Get(B($"t{t:D2}:{i:D3}"))));
        }
    }

    [Fact]
    public async Task Writers_that_queue_during_a_flush_commit_as_one_group()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();

        hooks.FlushGate.Reset();
        hooks.FlushStarted.Reset();
        Task leader = Task.Run(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced));
        Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

        // Dedicated threads: every follower blocks until the leader is done, and the pool adds threads slowly.
        Task[] followers = [.. Enumerable.Range(0, 10).Select(i =>
            Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1"), options: WriteOptions.Synced), TaskCreationOptions.LongRunning))];
        Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == 10, TimeSpan.FromSeconds(10)));

        hooks.FlushGate.Set();
        await Task.WhenAll([leader, .. followers]);

        Assert.Equal(2, db.WriteGroupCount);
        Assert.Equal(11, db.LatestSequenceNumber);
    }

    [Fact]
    public async Task Write_group_sizes_count_each_group_by_its_writers()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        int followerCount = SolariumDb.MaxCountedGroupSize + 6;

        hooks.FlushGate.Reset();
        hooks.FlushStarted.Reset();
        Task leader = Task.Run(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced));
        Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

        // Dedicated threads: every follower blocks until the leader is done, and the pool adds threads slowly.
        Task[] followers = [.. Enumerable.Range(0, followerCount).Select(i =>
            Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1")), TaskCreationOptions.LongRunning))];
        Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == followerCount, TimeSpan.FromSeconds(10)));

        hooks.FlushGate.Set();
        await Task.WhenAll([leader, .. followers]);
        db.Put(B("alone"), B("1"));

        long[] sizes = db.WriteGroupSizes();
        Assert.Equal(0, sizes[0]);
        Assert.Equal(2, sizes[1]);   // the held leader, then the last Put
        Assert.Equal(1, sizes[SolariumDb.MaxCountedGroupSize]);   // the followers, past the last size counted alone
        Assert.Equal(3, sizes.Sum());
        Assert.Equal(db.WriteGroupCount, sizes.Sum());
    }

    [Fact]
    public async Task Writers_behind_a_held_leader_block_once_and_commit_as_the_next_group()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        const int followerCount = 8;

        hooks.FlushGate.Reset();
        hooks.FlushStarted.Reset();
        Task leader = Task.Run(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced));
        Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

        // Dedicated threads: every follower blocks until the leader is done, and the pool adds threads slowly.
        Task[] followers = [.. Enumerable.Range(0, followerCount).Select(i =>
            Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1")), TaskCreationOptions.LongRunning))];

        // The leader's group syncs, so every follower blocks without a spin.
        Assert.True(SpinWait.SpinUntil(() => db.WriterWaits() == (followerCount, followerCount, followerCount, 0), TimeSpan.FromSeconds(10)));

        // The held leader offers leadership to the oldest follower, which takes the others with it.
        // No other writer runs, so nothing takes the queue before it.
        hooks.FlushGate.Set();
        await Task.WhenAll([leader, .. followers]);
        db.Put(B("alone"), B("1"));   // no other writer, so no wait

        Assert.Equal((followerCount, followerCount, followerCount, 0), db.WriterWaits());
        Assert.Equal(3, db.WriteGroupCount);
        Assert.Equal(followerCount + 2, db.LatestSequenceNumber);
    }

    [Fact]
    public async Task Writers_that_wait_for_a_wal_flush_block_without_a_spin()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        // A spin this long would hold every follower past the test's timeouts.
        using SolariumDb db = SolariumDb.Open(DbPath, FixedSpin(hooks, TimeSpan.FromMinutes(1)));
        const int followerCount = 4;

        hooks.FlushGate.Reset();
        hooks.FlushStarted.Reset();
        Task leader = Task.Run(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced));
        try
        {
            Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

            Task[] followers = [.. Enumerable.Range(0, followerCount).Select(i =>
                Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1")), TaskCreationOptions.LongRunning))];
            Assert.True(SpinWait.SpinUntil(() => db.WriterWaits() == (followerCount, followerCount, followerCount, 0), TimeSpan.FromSeconds(10)));

            hooks.FlushGate.Set();
            await Task.WhenAll([leader, .. followers]);
        }
        finally
        {
            hooks.FlushGate.Set();
        }

        Assert.Equal(followerCount + 1, db.LatestSequenceNumber);
        Assert.Equal(0, db.SpinWaitTimes().Sum());   // no wait spun
        Assert.Equal((0, 0, 0, WriterSpinCredit.Start), db.WriterSpinTurns());   // and none was a probe or a short spin
    }

    [Fact]
    public async Task Only_the_writers_of_a_next_group_that_syncs_skip_the_spin()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, FixedSpin(hooks, TimeSpan.FromMinutes(1)));

        // An unsynced leader, held in its WAL write.
        hooks.WriteGate.Reset();
        hooks.WriteStarted.Reset();
        Task leader = Task.Run(() => db.Put(B("leader"), B("1")));
        try
        {
            Assert.True(hooks.WriteStarted.Wait(TimeSpan.FromSeconds(10)));

            // The oldest writer leads the next group, so it waits only for the unsynced group: it spins.
            Task synced = Task.Factory.StartNew(() => db.Put(B("synced"), B("1"), options: WriteOptions.Synced), TaskCreationOptions.LongRunning);
            Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == 1, TimeSpan.FromSeconds(10)));

            // The next writer is in that synced group, so it blocks at once.
            Task unsynced = Task.Factory.StartNew(() => db.Put(B("unsynced"), B("1")), TaskCreationOptions.LongRunning);
            Assert.True(SpinWait.SpinUntil(() => db.WriterWaits() == (2, 1, 1, 0), TimeSpan.FromSeconds(10)));

            hooks.WriteGate.Set();
            await Task.WhenAll(leader, synced, unsynced);
        }
        finally
        {
            hooks.WriteGate.Set();
        }

        Assert.Equal((2, 1, 1, 0), db.WriterWaits());   // the spinning writer took its offer without a block
        Assert.Equal(2, db.WriteGroupCount);
        Assert.Equal(3, db.LatestSequenceNumber);
    }

    [Fact]
    public async Task Probes_that_block_make_the_next_turns_spin_short_and_probes_go_on_in_windows()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        // A probe every second turn, the first one included, and windows of two long turns while
        // the spin is short. The long spin is short enough that the long turns behind held leaders
        // end in blocks within the test's timeouts.
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions
        {
            Environment = hooks,
            WriterSpin = TimeSpan.FromMilliseconds(10),
            WriterSpinProbeInterval = 2,
        });
        long blocked = await MakeTheSpinShort(db, hooks);

        // Then a window: a long turn that lets the short spins end, and a probe.
        await FollowBehindAHeldLeader(db, hooks, "window", waitsBlocked: ++blocked);
        Assert.Equal((16, 16, 1), Turns(db));
        await FollowBehindAHeldLeader(db, hooks, "window probe", waitsBlocked: ++blocked);
        Assert.Equal((17, 17, 1), Turns(db));

        // The probe blocked too, so the spin stays short.
        await FollowBehindAHeldLeader(db, hooks, "short again", waitsBlocked: ++blocked);
        Assert.Equal((17, 17, 2), Turns(db));

        Assert.Equal((blocked, 0, blocked, 0), db.WriterWaits());
        Assert.Equal(blocked, db.SpinWaitTimes().Sum());   // every turn spun, the short ones too
        Assert.Equal("1", S(db.Get(B("short again"))));
    }

    [Fact]
    public async Task A_window_that_brings_the_credit_back_resumes_the_probes_within_an_interval()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        // As above, with a long spin that leaves room for a turn to end in time on a busy host.
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions
        {
            Environment = hooks,
            WriterSpin = TimeSpan.FromMilliseconds(50),
            WriterSpinProbeInterval = 2,
        });
        long blocked = await MakeTheSpinShort(db, hooks);

        // A window whose probe ends in time: the credit is back at 0 or more.
        await FollowBehindAHeldLeader(db, hooks, "window", waitsBlocked: ++blocked);
        await FollowBehindAHeldLeader(db, hooks, "window probe", waitsBlocked: null);
        Assert.Equal((17, 16, 1), Turns(db));
        Assert.True(db.WriterSpinTurns().Credit >= 0);

        // The next turns spin long, and a probe follows within an interval of the first of them,
        // not at the end of the count to the next window.
        await FollowBehindAHeldLeader(db, hooks, "long after the window", waitsBlocked: null);
        await FollowBehindAHeldLeader(db, hooks, "long again", waitsBlocked: null);
        Assert.Equal((17, 16, 1), Turns(db));
        await FollowBehindAHeldLeader(db, hooks, "probe after the window", waitsBlocked: null);
        Assert.Equal((18, 16, 1), Turns(db));
        Assert.Equal("1", S(db.Get(B("probe after the window"))));
    }

    [Fact]
    public async Task Probes_that_end_in_time_keep_the_long_spin()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        // A spin of a minute, so each follower gets its turn in the spin and never blocks.
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions
        {
            Environment = hooks,
            WriterSpin = TimeSpan.FromMinutes(1),
            WriterSpinProbeInterval = 2,
        });

        await FollowBehindAHeldLeader(db, hooks, "probe", waitsBlocked: null);
        await FollowBehindAHeldLeader(db, hooks, "long", waitsBlocked: null);
        await FollowBehindAHeldLeader(db, hooks, "probe again", waitsBlocked: null);

        Assert.Equal((2, 0, 0), Turns(db));
        Assert.True(db.WriterSpinTurns().Credit > WriterSpinCredit.Start);
        Assert.Equal((3, 0, 0, 0), db.WriterWaits());
        Assert.Equal("1", S(db.Get(B("long"))));
    }

    [Fact]
    public async Task Writers_queued_when_the_database_is_disposed_fail_instead_of_hanging()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        const int followerCount = 8;
        Task[] followers;

        using (SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks }))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            hooks.FlushGate.Reset();
            hooks.FlushStarted.Reset();
            Task leader = Task.Run(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced));
            Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

            followers = [.. Enumerable.Range(0, followerCount).Select(i =>
                Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1")), TaskCreationOptions.LongRunning))];
            Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == followerCount, TimeSpan.FromSeconds(10)));

            // Dispose marks the database closed, then waits for the held leader. Nothing else holds
            // the write lock, so a Dispose thread that is blocked is in that wait.
            Thread dispose = new(db.Dispose);
            dispose.Start();
            Assert.True(SpinWait.SpinUntil(() => dispose.ThreadState.HasFlag(ThreadState.WaitSleepJoin), TimeSpan.FromSeconds(10)));

            hooks.FlushGate.Set();
            await leader;   // the write in progress completes
            Assert.True(dispose.Join(TimeSpan.FromSeconds(10)));
        }

        foreach (Task follower in followers)
            await Assert.ThrowsAsync<ObjectDisposedException>(() => follower);

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal("1", S(db.Get(B("leader"))));
            Assert.Null(db.Get(B("f0")));
            Assert.Equal(1, db.LatestSequenceNumber);
        }
    }

    [Fact]
    public async Task Writers_queued_behind_a_failed_wal_flush_fail_instead_of_hanging()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        const int followerCount = 8;

        hooks.FlushGate.Reset();
        hooks.FlushStarted.Reset();
        Task leader = Task.Run(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced));
        Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

        Task[] followers = [.. Enumerable.Range(0, followerCount).Select(i =>
            Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1")), TaskCreationOptions.LongRunning))];
        Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == followerCount, TimeSpan.FromSeconds(10)));

        hooks.FailFlushes = true;
        hooks.FlushGate.Set();

        SolariumException failed = await Assert.ThrowsAsync<SolariumException>(() => leader);
        Assert.IsType<IOException>(failed.InnerException);
        foreach (Task follower in followers)
            await Assert.ThrowsAsync<SolariumException>(() => follower);

        Assert.Equal(0, db.WriteGroupCount);   // a group counts only after its WAL append succeeds
        Assert.Equal(0, db.LatestSequenceNumber);
        Assert.Throws<SolariumException>(() => db.Put(B("after"), B("1")));
    }

    [Theory]
    [InlineData(0, 256)]
    [InlineData(1, 256)]
    [InlineData(20, 256)]
    [InlineData(1000, 256)]
    [InlineData(1000, 1)]   // every turn a probe
    [InlineData(1000, 0)]   // a fixed spin
    public void Concurrent_writers_lose_no_write_with_any_writer_spin(int spinMicroseconds, int probeInterval)
    {
        DbOptions options = new DbOptions
        {
            WriterSpin = TimeSpan.FromMicroseconds(spinMicroseconds),
            WriterSpinProbeInterval = probeInterval,
        };
        const int threads = 16;
        const int puts = 500;

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
            {
                for (int i = 0; i < puts; i++)
                    db.Put(B($"t{t:D2}:{i:D3}"), B($"{t}/{i}"));
            });

            Assert.Equal(threads * puts, db.LatestSequenceNumber);
            // A writer blocks at most twice: before an offer, and after a running writer overtook it.
            // No write syncs, so every waiting writer spins first.
            (long waits, long flushWaits, long blocked, long overtaken) = db.WriterWaits();
            Assert.Equal(0, flushWaits);
            Assert.InRange(overtaken, 0, waits);
            Assert.InRange(blocked, 0, waits + overtaken);
            // Every wait spins, once more after each overtaken offer; with no spin, none is counted.
            long turns = spinMicroseconds > 0 ? waits + overtaken : 0;
            Assert.Equal(turns, db.SpinWaitTimes().Sum());

            // The first turn that spins is a probe. How many follow depends on how the probes end,
            // and a probe is never a short spin.
            (long probes, long probeBlocks, long shortSpins, _) = db.WriterSpinTurns();
            if (probeInterval == 0 || turns == 0)
            {
                Assert.Equal(0, probes);
                Assert.Equal(0, shortSpins);
            }
            else
            {
                Assert.InRange(probes, 1, turns);
                Assert.InRange(shortSpins, 0, turns - probes);
            }

            Assert.InRange(probeBlocks, 0, Math.Min(probes, blocked));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            for (int t = 0; t < threads; t++)
                for (int i = 0; i < puts; i++)
                    Assert.Equal($"{t}/{i}", S(db.Get(B($"t{t:D2}:{i:D3}"))));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void The_stage_timers_of_a_group_count_only_when_they_are_on(bool measure)
    {
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { MeasureGroupStages = measure });

        db.Put(B("first"), B("1"));
        (TimeSpan prepare, TimeSpan encode, TimeSpan append, TimeSpan memTable, _, TimeSpan gap, _) = db.GroupStages();
        Assert.Equal(measure, prepare > TimeSpan.Zero && encode > TimeSpan.Zero && append > TimeSpan.Zero && memTable > TimeSpan.Zero);
        Assert.Equal(TimeSpan.Zero, gap);   // no group before the first

        // A hand-off takes less than the 100 ns of a TimeSpan tick, so it shows only over many groups.
        for (int i = 0; i < 1000; i++)
            db.Put(B($"k{i}"), B("1"));
        (_, _, _, _, TimeSpan handOff, gap, _) = db.GroupStages();
        Assert.Equal(measure, handOff > TimeSpan.Zero && gap > TimeSpan.Zero);
        if (!measure)
            Assert.Equal(default, db.GroupStages());
        Assert.Equal("1", S(db.Get(B("k999"))));
    }

    [Fact]
    public async Task A_held_wal_write_counts_in_the_append_stage_of_its_group()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks, MeasureGroupStages = true });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        TimeSpan held = TimeSpan.FromMilliseconds(100);

        hooks.WriteGate.Reset();
        hooks.WriteStarted.Reset();
        Task writer = Task.Run(() => db.Put(B("k"), B("1")));
        Assert.True(hooks.WriteStarted.Wait(TimeSpan.FromSeconds(10)));
        Thread.Sleep(held);
        hooks.WriteGate.Set();
        await writer;

        (TimeSpan prepare, TimeSpan encode, TimeSpan append, TimeSpan memTable, TimeSpan handOff, _, _) = db.GroupStages();
        Assert.True(append >= held, $"The append took {append}.");
        Assert.True(prepare + encode + memTable + handOff < held, "The other stages took the held time.");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Concurrent_batches_across_column_families_read_back_the_same_before_and_after_replay(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { CreateMissingColumnFamilies = true };
        const int threads = 8;
        const int batches = 300;

        // Each thread writes its own keys, so the state each thread leaves does not depend on how
        // the groups interleave: every batch puts a key in all three families, deletes the key before
        // it in "a", and deletes a range of the thread's earlier keys in "b".
        static string Key(int thread, int i) => $"t{thread}:{i:D4}";
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["a", "b"]))
        {
            ColumnFamily a = db.GetColumnFamily("a");
            ColumnFamily b = db.GetColumnFamily("b");
            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
            {
                for (int i = 0; i < batches; i++)
                {
                    WriteBatch batch = new WriteBatch()
                        .Put(B(Key(t, i)), B($"d{i}"))
                        .Put(B(Key(t, i)), B($"a{i}"), a);
                    if (i > 0)
                        batch.Delete(B(Key(t, i - 1)), a);
                    if (i % 10 == 9)
                        batch.DeleteRange(B(Key(t, i - 9)), B(Key(t, i - 4)), b);
                    batch.Put(B(Key(t, i)), B($"b{i}"), b);
                    db.Write(batch);
                }
            });

            Check(db);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["a", "b"]))
            Check(db);

        void Check(SolariumDb db)
        {
            // 3 operations a batch, 4 after the first, and 1 more in every tenth.
            Assert.Equal(threads * ((4 * batches) - 1 + (batches / 10)), db.LatestSequenceNumber);
            ColumnFamily a = db.GetColumnFamily("a");
            ColumnFamily b = db.GetColumnFamily("b");
            for (int t = 0; t < threads; t++)
            {
                for (int i = 0; i < batches; i++)
                {
                    Assert.Equal($"d{i}", S(db.Get(B(Key(t, i)))));
                    Assert.Equal(i == batches - 1 ? $"a{i}" : null, S(db.Get(B(Key(t, i)), a)));

                    // A range deletion comes before the put of its batch and after the puts of the
                    // keys it covers, which are earlier batches of the same thread.
                    bool deleted = i % 10 < 5;
                    Assert.Equal(deleted ? null : $"b{i}", S(db.Get(B(Key(t, i)), b)));
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public async Task A_write_group_stops_at_the_size_cap_and_the_writers_left_lead_the_next(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        DbOptions options = scope.Options() with { Environment = hooks, MaxWriteBatchGroupSizeBytes = 4096, EnablePipelinedWrite = pipelined };
        using SolariumDb db = SolariumDb.Open(DbPath, options);
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        byte[] value = new byte[1000];

        // Nine writers queue behind a leader whose WAL write is held. Their records, about 1 KB, are
        // above an eighth of the cap, so the queue goes in groups of 3, whatever the scheduling. The
        // writers block, so each has a thread of its own: on a thread pool that other tests keep busy,
        // they could start too late to queue.
        hooks.WriteGate.Reset();
        Task first = Task.Factory.StartNew(() => db.Put(B("first"), value), TaskCreationOptions.LongRunning);
        Assert.True(hooks.WriteStarted.Wait(TimeSpan.FromSeconds(30)), "the first write did not start");
        Task[] queued = [.. Enumerable.Range(0, 9).Select(i => Task.Factory.StartNew(() => db.Put(B($"q{i}"), value), TaskCreationOptions.LongRunning))];
        Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == 9, TimeSpan.FromSeconds(30)), "the writers did not queue");
        hooks.WriteGate.Set();
        await Task.WhenAll([first, .. queued]).WaitAsync(TimeSpan.FromSeconds(30));
        long[] held = db.WriteGroupSizes();
        Assert.Equal(3, held[3]);
        Assert.All(held.Skip(4), count => Assert.Equal(0, count));

        // Free-running writers, then a batch larger than the cap, which is a group of its own.
        await Task.WhenAll(Enumerable.Range(0, 16).Select(t => Task.Factory.StartNew(() =>
        {
            for (int i = 0; i < 200; i++)
                db.Put(B($"t{t:D2}-{i:D3}"), value);
        }, TaskCreationOptions.LongRunning))).WaitAsync(TimeSpan.FromSeconds(60));
        db.Put(B("large"), new byte[64 * 1024]);

        Assert.All(db.WriteGroupSizes().Skip(4), count => Assert.Equal(0, count));
        Assert.Equal(3211, db.LatestSequenceNumber);
        using Iterator iterator = db.NewIterator();
        int keys = 0;
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            keys++;

        Assert.Equal(3211, keys);
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(_directory.Combine("other"), new DbOptions { MaxWriteBatchGroupSizeBytes = 0 }));
    }

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public async Task Writers_that_a_capped_group_leaves_at_the_front_of_the_queue_never_lead_beside_another_leader(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { MaxWriteBatchGroupSizeBytes = 2048, EnablePipelinedWrite = pipelined };

        // Small writers arrive while a large one, which a small leader's group cannot take, heads the
        // queue with an offer of leadership; a second leader would interleave the WAL.
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            await Task.WhenAll(Enumerable.Range(0, 16).Select(t => Task.Run(() =>
            {
                for (int i = 0; i < 1500; i++)
                    db.Put(B($"t{t:D2}-{i:D4}"), new byte[i % 2 == 0 ? 8 : 1800]);
            }))).WaitAsync(TimeSpan.FromSeconds(120));

            Assert.Equal(24000, db.LatestSequenceNumber);
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, options);
        Assert.Equal(24000, reopened.LatestSequenceNumber);
        for (int t = 0; t < 16; t++)
            Assert.Equal(1800, reopened.Get(B($"t{t:D2}-1499"))!.Length);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_late_wake_of_a_leadership_offer_never_reaches_a_writer_that_led_a_group_since(string environment)
    {
        TimeSpan timeout = TimeSpan.FromSeconds(30);
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        using ManualResetEventSlim stageHeld = new(), stageGate = new(), wakeHeld = new(), wakeGate = new(), smallLed = new(), largeLed = new();
        DbOptions options = scope.Options() with
        {
            Environment = hooks,
            EnablePipelinedWrite = true,
            MaxWriteBatchGroupSizeBytes = 2048,
            BeforeMemTableStage = first =>
            {
                if (first == 1)
                {
                    stageHeld.Set();
                    stageGate.Wait();
                }
            },
            AfterWalLeadership = first =>
            {
                if (first == 2)
                {
                    wakeHeld.Set();
                    wakeGate.Wait();
                }
                else if (first == 3)
                    smallLed.Set();
                else if (first == 4)
                    largeLed.Set();
            },
        };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            // The first group's memtable stage waits, so every later group joins the memtable queue.
            Task first = Task.Run(() => db.Put(B("a"), B("1")));
            Assert.True(stageHeld.Wait(timeout));

            // The next leader's append waits while a writer too large for a small leader's group queues.
            hooks.WriteGate.Reset();
            hooks.WriteStarted.Reset();
            Task leader = Task.Run(() => db.Put(B("b"), B("2")));
            Assert.True(hooks.WriteStarted.Wait(timeout));
            Task large = Task.Run(() => db.Put(B("w"), new byte[1800]));
            Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == 1, timeout));

            // The leader offers leadership to the large writer and waits before it wakes it. A small
            // writer leads meanwhile, and its group leaves the large writer at the front of the queue.
            hooks.WriteGate.Set();
            Assert.True(wakeHeld.Wait(timeout));
            Task small = Task.Run(() => db.Put(B("r"), B("3")));
            Assert.True(smallLed.Wait(timeout));

            // Whether the large writer led its group now or leads it at the wake, a wake that comes
            // after it must not read as the offer of a memtable stage.
            largeLed.Wait(TimeSpan.FromMilliseconds(500));
            wakeGate.Set();
            stageGate.Set();
            await Task.WhenAll(first, leader, large, small).WaitAsync(timeout);

            Assert.Equal(4, db.LatestSequenceNumber);
            Assert.Equal(["1", "2", "3"], new[] { "a", "b", "r" }.Select(key => S(db.Get(B(key)))));
            Assert.Equal(1800, db.Get(B("w"))!.Length);
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, scope.Options());
        Assert.Equal(4, reopened.LatestSequenceNumber);
        Assert.Equal(1800, reopened.Get(B("w"))!.Length);
    }

    [Fact]
    public void A_thread_keeps_its_record_buffer_only_for_batches_up_to_64_kb()
    {
        using SolariumDb db = SolariumDb.Open(DbPath);
        List<int> kept = [];

        // On a thread of its own, so the buffer of another test's writes is not in the way.
        Thread writer = new(() =>
        {
            db.Put(B("small"), B("1"));
            kept.Add(SolariumDb.KeptRecordBufferLength);
            db.Put(B("large"), new byte[100 * 1024]);
            kept.Add(SolariumDb.KeptRecordBufferLength);
            db.Put(B("medium"), new byte[10 * 1024]);
            kept.Add(SolariumDb.KeptRecordBufferLength);
        });
        writer.Start();
        writer.Join();

        Assert.Equal([256, 256, 16 * 1024], kept);
        Assert.Equal(100 * 1024, db.Get(B("large"))!.Length);
        Assert.Equal(10 * 1024, db.Get(B("medium"))!.Length);
    }

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void A_large_batch_is_appended_from_its_own_record_without_a_copy_and_replays(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { EnablePipelinedWrite = pipelined };
        const int count = 4096;

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("before"), B("1"));

            // About 4 MiB: a record of its own, which the leader completes in place.
            WriteBatch large = new();
            for (int i = 0; i < count; i++)
                large.Put(B($"large/{i:D4}"), Enumerable.Repeat((byte)i, 1024).ToArray());
            db.Write(large, WriteOptions.Synced);

            db.Put(B("after"), B("2"), options: WriteOptions.Synced);

            Assert.InRange(db.GroupBufferCapacity, 0, 64 * 1024);
            Assert.Equal(count + 2, db.LatestSequenceNumber);
            Assert.Equal("2", S(db.Get(B("after"))));
        }

        // Replay checks each record's CRC, so a record completed wrongly in place would stop it here.
        using SolariumDb reopened = SolariumDb.Open(DbPath, options);
        Assert.Equal(count + 2, reopened.LatestSequenceNumber);
        Assert.Equal("1", S(reopened.Get(B("before"))));
        Assert.Equal("2", S(reopened.Get(B("after"))));
        for (int i = 0; i < count; i++)
            Assert.Equal(Enumerable.Repeat((byte)i, 1024).ToArray(), reopened.Get(B($"large/{i:D4}")));
    }

    [Fact]
    public void A_full_memtable_switches_when_its_family_is_not_the_first_of_the_batch()
    {
        const long bufferSize = 4 * 1024;
        DbOptions options = new DbOptions
        {
            CreateMissingColumnFamilies = true,
            ColumnFamilyOptions = new Dictionary<string, ColumnFamilyOptions> { ["small"] = new ColumnFamilyOptions { WriteBufferSize = bufferSize } },
        };
        using SolariumDb db = SolariumDb.Open(DbPath, options, ["small"]);
        ColumnFamily small = db.GetColumnFamily("small");

        // Each batch writes the default family first, so only the batch's second family is "small".
        for (int i = 0; i < 40; i++)
        {
            db.Write(new WriteBatch().Put(B($"k{i}"), B("1")).Put(B($"k{i}"), new byte[1024], small));

            // A group switches a full memtable before it writes, so the active one holds at most
            // one batch more than the buffer.
            Assert.InRange(small.Version.Active.ApproximateBytes, 0, bufferSize + 2048);
        }

        db.Flush(small);
        Assert.Equal(1024, db.Get(B("k39"), small)!.Length);
    }

    [Fact]
    public void A_wal_write_failure_stops_writes_but_not_reads()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks });
        db.Put(B("k"), B("before"));

        hooks.FailWrites = true;
        SolariumException failed = Assert.Throws<SolariumException>(() => db.Put(B("k"), B("lost")));
        Assert.IsType<IOException>(failed.InnerException);

        hooks.FailWrites = false;
        SolariumException stopped = Assert.Throws<SolariumException>(() => db.Put(B("k"), B("later")));
        Assert.Contains("no more writes", stopped.Message);

        Assert.Equal("before", S(db.Get(B("k"))));
    }

    [Fact]
    public void A_returned_value_is_the_callers_copy()
    {
        using SolariumDb db = SolariumDb.Open(DbPath);
        db.Put(B("k"), B("original"));

        byte[] value = db.Get(B("k"))!;
        value[0] = (byte)'X';
        db.MultiGet([B("k")])[0]![1] = (byte)'X';

        Assert.Equal("original", S(db.Get(B("k"))));
    }

    // A database whose MANIFEST is lost (a restore that missed it, a deleted file) is refused before
    // anything in it changes: starting it over would delete its tables as orphans. Three shapes:
    // a checkpoint, which has tables and no WAL; a database with flushed tables and WAL files in
    // two families; and one with WAL files only.
    [Theory]
    [MemberData(nameof(LostManifestCases))]
    public void A_database_whose_manifest_is_lost_is_refused_and_left_as_it_was(string environment, string shape)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options(createMissingColumnFamilies: true);
        string path = DbPath;
        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["locks"]))
        {
            ColumnFamily locks = db.GetColumnFamily("locks");
            db.Put(B("key"), B("durable"), options: WriteOptions.Synced);
            db.Put(B("lock"), B("held"), locks, WriteOptions.Synced);
            if (shape != "wal")
            {
                db.Flush();
                db.Flush(locks);
                db.Put(B("after"), B("flush"), options: WriteOptions.Synced);
            }

            if (shape == "checkpoint")
            {
                path = _directory.Combine("checkpoint");
                db.CreateCheckpoint(path);
            }
        }

        string manifest = Path.Combine(path, "MANIFEST");
        byte[] saved = File.ReadAllBytes(manifest);
        File.Delete(manifest);
        Dictionary<string, byte[]> before = Contents(path);
        Assert.Contains(before.Keys, name => name.EndsWith(shape == "wal" ? ".log" : ".sst", StringComparison.Ordinal));

        SolariumException refusal = Assert.Throws<SolariumException>(() => SolariumDb.Open(path, options, ["locks"]));
        Assert.Contains("no MANIFEST", refusal.Message);
        Assert.Contains(path, refusal.Message);
        Dictionary<string, byte[]> after = Contents(path);
        Assert.Equal(before.Keys.Order(StringComparer.Ordinal), after.Keys.Order(StringComparer.Ordinal));
        foreach ((string name, byte[] bytes) in before)
            Assert.True(bytes.AsSpan().SequenceEqual(after[name]), $"{name} changed");

        File.WriteAllBytes(manifest, saved);
        using SolariumDb reopened = SolariumDb.Open(path, options, ["locks"]);
        Assert.Equal("durable", S(reopened.Get(B("key"))));
        Assert.Equal("held", S(reopened.Get(B("lock"), reopened.GetColumnFamily("locks"))));
        if (shape != "wal")
            Assert.Equal("flush", S(reopened.Get(B("after"))));

        // Every file but LOCK, which Open takes and the refusal releases.
        static Dictionary<string, byte[]> Contents(string directory) =>
            Directory.GetFiles(directory).Where(file => Path.GetFileName(file) != "LOCK").ToDictionary(file => Path.GetFileName(file)!, File.ReadAllBytes);
    }

    /// <summary>Each environment with each shape of <see cref="A_database_whose_manifest_is_lost_is_refused_and_left_as_it_was"/>.</summary>
    public static TheoryData<string, string> LostManifestCases()
    {
        TheoryData<string, string> cases = new();
        foreach (string environment in IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
            foreach (string shape in new[] { "checkpoint", "flushed", "wal" })
                cases.Add(environment, shape);

        return cases;
    }

    // A first open writes FORMAT, then MANIFEST through MANIFEST.tmp; a crash between them leaves a
    // directory that opens as a new database.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_first_open_cut_short_before_its_manifest_opens_as_a_new_database(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb.Open(DbPath, scope.Options()))
        {
        }

        foreach (string file in Directory.GetFiles(DbPath))
            if (Path.GetFileName(file) is not ("FORMAT" or "LOCK"))
                File.Delete(file);

        File.WriteAllBytes(Path.Combine(DbPath, "MANIFEST.tmp"), [1, 2, 3]);
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Null(db.Get(B("key")));
            db.Put(B("key"), B("value"));
        }

        Assert.False(File.Exists(Path.Combine(DbPath, "MANIFEST.tmp")));
        using SolariumDb reopened = SolariumDb.Open(DbPath, scope.Options());
        Assert.Equal("value", S(reopened.Get(B("key"))));
    }

    // A data block whose checksum is valid but whose entry runs out of bytes before its kind: every
    // read path reports the table as corrupt.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_table_entry_cut_short_under_a_valid_checksum_fails_every_read_as_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            db.Put(B("a"), B("b"));
            db.Flush();
        }

        // The first data block starts the file: [key length 1]["a"][sequence][kind][value length 1]["b"],
        // then its type (raw) and its CRC, which covers the type. A key length of 7 takes the "a" and 6
        // bytes of the sequence, so the sequence takes the rest, and the kind is past the end.
        string table = Assert.Single(Directory.GetFiles(DbPath, "*.sst"));
        byte[] bytes = File.ReadAllBytes(table);
        Assert.Equal(1, BitConverter.ToInt32(bytes, 0));
        const int Payload = 4 + 1 + 8 + 1 + 4 + 1;
        Assert.Equal(0, bytes[Payload]);
        BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), 7);
        BitConverter.TryWriteBytes(bytes.AsSpan(Payload + 1, 4), System.IO.Hashing.Crc32.HashToUInt32(bytes.AsSpan(0, Payload + 1)));
        File.WriteAllBytes(table, bytes);

        using SolariumDb damaged = SolariumDb.Open(DbPath, scope.Options());
        Assert.Contains("does not decode", Assert.Throws<SolariumException>(() => damaged.Get(B("a"))).Message);
        Assert.Contains("does not decode", Assert.Throws<SolariumException>(() => damaged.MultiGet([B("a")])).Message);
        using Iterator forward = damaged.NewIterator();
        Assert.Contains("does not decode", Assert.Throws<SolariumException>(forward.SeekToFirst).Message);
        using Iterator backward = damaged.NewIterator();
        Assert.Contains("does not decode", Assert.Throws<SolariumException>(backward.SeekToLast).Message);
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // A spin that does not adapt, so its length does not depend on how earlier waits ended.
    private static DbOptions FixedSpin(IStorageEnvironment environment, TimeSpan spin) =>
        new DbOptions { Environment = environment, WriterSpin = spin, WriterSpinProbeInterval = 0 };

    private static (long Probes, long ProbeBlocks, long ShortSpins) Turns(SolariumDb db)
    {
        (long probes, long probeBlocks, long shortSpins, _) = db.WriterSpinTurns();
        return (probes, probeBlocks, shortSpins);
    }

    // Probes every second turn, each behind a held leader and blocked, until the credit stops the
    // long spin, and then one short turn. A new database has a margin of 16 blocked probes, so the
    // long spin lasts until the 16th probe. Returns the waits that blocked.
    private static async Task<long> MakeTheSpinShort(SolariumDb db, HookedEnvironment hooks)
    {
        long blocked = 0;
        for (int i = 1; i <= 16; i++)
        {
            await FollowBehindAHeldLeader(db, hooks, $"probe {i}", waitsBlocked: ++blocked);
            Assert.Equal((i, i, 0), Turns(db));
            Assert.Equal(i < 16, db.WriterSpinTurns().Credit >= 0);
            if (i < 16)
                await FollowBehindAHeldLeader(db, hooks, $"long {i}", waitsBlocked: ++blocked);
        }

        // The next turn is not a probe, and the credit stops the long spin.
        await FollowBehindAHeldLeader(db, hooks, "short", waitsBlocked: ++blocked);
        Assert.Equal((16, 16, 1), Turns(db));
        return blocked;
    }

    // Hold an unsynced leader in its WAL write, so one follower waits for it with a spin. With
    // waitsBlocked, release the leader only when that many waits have blocked; without it, release
    // the leader as soon as the follower is in the queue.
    private static async Task FollowBehindAHeldLeader(SolariumDb db, HookedEnvironment hooks, string key, long? waitsBlocked)
    {
        hooks.WriteGate.Reset();
        hooks.WriteStarted.Reset();
        Task leader = Task.Run(() => db.Put(B($"leader of {key}"), B("1")));
        try
        {
            Assert.True(hooks.WriteStarted.Wait(TimeSpan.FromSeconds(10)));

            Task follower = Task.Factory.StartNew(() => db.Put(B(key), B("1")), TaskCreationOptions.LongRunning);
            if (waitsBlocked is long blocked)
                Assert.True(SpinWait.SpinUntil(() => db.WriterWaits().Blocked == blocked, TimeSpan.FromSeconds(10)));
            else
                Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == 1, TimeSpan.FromSeconds(10)));

            hooks.WriteGate.Set();
            await Task.WhenAll(leader, follower);
        }
        finally
        {
            hooks.WriteGate.Set();
        }
    }
}
