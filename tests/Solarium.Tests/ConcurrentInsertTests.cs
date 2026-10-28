using Solarium.IoUring;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Write groups whose writers insert their own batches into the memtables at once, against the
/// leader that inserts the whole group: the same contents, atomic batches, and the same failure paths.
/// A writer that a test holds runs on a thread of its own: on a thread pool that other tests keep busy,
/// it could start later than the test waits for it.
/// </summary>
public sealed class ConcurrentInsertTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    /// <summary>Every available storage environment, with concurrent inserts on and off, and pipelined commits on and off.</summary>
    public static TheoryData<string, bool, bool> EnvironmentsAndInsertPaths()
    {
        TheoryData<string, bool, bool> cases = new();
        foreach (string environment in IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
            foreach (bool concurrent in new[] { true, false })
                foreach (bool pipelined in new[] { false, true })
                    cases.Add(environment, concurrent, pipelined);

        return cases;
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndInsertPaths))]
    public async Task Readers_never_see_part_of_a_batch_from_many_writers(string environment, bool concurrent, bool pipelined)
    {
        const int writers = 4;
        const int writes = 1500;
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { AllowConcurrentMemtableWrite = concurrent, EnablePipelinedWrite = pipelined });
        byte[][] keys = [.. Enumerable.Range(0, writers).SelectMany(t => new[] { B($"a{t}"), B($"b{t}") })];
        WriteBatch initial = new();
        foreach (byte[] key in keys)
            initial.Put(key, B("0"));
        db.Write(initial);

        using CancellationTokenSource stop = new();
        using ManualResetEventSlim readerRunning = new();
        int violations = 0;
        int checks = 0;

        Task reader = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                byte[]?[] values = db.MultiGet(keys);
                for (int t = 0; t < writers; t++)
                    if (S(values[2 * t]) != S(values[(2 * t) + 1]))
                        Interlocked.Increment(ref violations);

                Dictionary<string, string> seen = new(StringComparer.Ordinal);
                using (Iterator iterator = db.NewIterator())
                {
                    for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                        seen[S(iterator.Key())] = S(iterator.Value());
                }

                for (int t = 0; t < writers; t++)
                    if (seen[$"a{t}"] != seen[$"b{t}"])
                        Interlocked.Increment(ref violations);

                Interlocked.Increment(ref checks);
                readerRunning.Set();
            }
        });

        Assert.True(readerRunning.Wait(TimeSpan.FromSeconds(30)), "The reader did not start.");
        int checksBeforeWrites = Volatile.Read(ref checks);

        await Task.WhenAll(Enumerable.Range(0, writers).Select(t => Task.Factory.StartNew(() =>
        {
            int checksAtPause = checksBeforeWrites;
            for (int i = 1; i <= writes; i++)
            {
                db.Write(new WriteBatch().Put(B($"a{t}"), B($"{i}")).Put(B($"noise{t}"), B($"{i}")).Put(B($"b{t}"), B($"{i}")));

                // With few CPUs the writers can keep them all, and the reader then checks nothing
                // while they run.
                if (i % 250 == 0)
                {
                    Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref checks) > checksAtPause, TimeSpan.FromSeconds(30)), "The reader stopped.");
                    checksAtPause = Volatile.Read(ref checks);
                }
            }
        }, TaskCreationOptions.LongRunning)));

        int checksDuringWrites = Volatile.Read(ref checks) - checksBeforeWrites;
        stop.Cancel();
        await reader;

        Assert.True(checksDuringWrites > 0, "No read overlapped the writes.");
        Assert.Equal(0, violations);
        Assert.Equal(keys.Length + ((long)writers * writes * 3), db.LatestSequenceNumber);
        if (!concurrent)
            Assert.Equal(0, db.ConcurrentInserts().ParallelGroups);
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndInsertPaths))]
    public void Concurrent_writers_from_many_threads_all_land_on_either_insert_path(string environment, bool concurrent, bool pipelined)
    {
        const int threads = 16;
        const int puts = 200;
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { AllowConcurrentMemtableWrite = concurrent, EnablePipelinedWrite = pipelined };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
            {
                for (int i = 0; i < puts; i++)
                    db.Put(B($"t{t:D2}:{i:D3}"), B($"{t}/{i}"), options: i % 10 == 0 ? WriteOptions.Synced : WriteOptions.Default);
            });

            Assert.Equal(threads * puts, db.LatestSequenceNumber);
            (long parallelGroups, _, _, _, _) = db.ConcurrentInserts();
            Assert.InRange(parallelGroups, 0, concurrent ? db.WriteGroupCount : 0);
            for (int t = 0; t < threads; t++)
                for (int i = 0; i < puts; i++)
                    Assert.Equal($"{t}/{i}", S(db.Get(B($"t{t:D2}:{i:D3}"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal(threads * puts, db.LatestSequenceNumber);
            for (int t = 0; t < threads; t++)
                for (int i = 0; i < puts; i++)
                    Assert.Equal($"{t}/{i}", S(db.Get(B($"t{t:D2}:{i:D3}"))));
        }
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndInsertPaths))]
    public async Task A_group_of_several_writers_gives_the_same_contents_on_either_insert_path(string environment, bool concurrent, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        ColumnFamilyOptions small = new() { WriteBufferSize = 4 * 1024 };
        DbOptions options = scope.Options(createMissingColumnFamilies: true) with
        {
            Environment = hooks,
            DefaultColumnFamilyOptions = small,
            AllowConcurrentMemtableWrite = concurrent,
            EnablePipelinedWrite = pipelined,
            WriterSpin = HeldSpin,
            WriterSpinProbeInterval = 0,
        };
        const int followerCount = 8;

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["cf"]))
        {
            ColumnFamily cf = db.GetColumnFamily("cf");
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            WriteBatch initial = new();
            for (int i = 0; i < 20; i++)
                initial.Put(B($"r{i:D2}"), B("old")).Put(B($"r{i:D2}"), B("old"), cf);
            db.Write(initial);

            // The held leader fills the default family's memtable, so the next group switches it
            // before its inserts.
            hooks.WriteGate.Reset();
            hooks.WriteStarted.Reset();
            Task leader = Task.Factory.StartNew(() => db.Put(B("leader"), new byte[8 * 1024]), TaskCreationOptions.LongRunning);
            Assert.True(hooks.WriteStarted.Wait(TimeSpan.FromSeconds(10)));

            // Each batch spans both families. The writers touch different keys, so the contents do
            // not depend on their order in the group.
            Task[] followers = QueueFollowers(db, [.. Enumerable.Range(0, followerCount).Select(i => i switch
            {
                0 => new WriteBatch().DeleteRange(B("r05"), B("r10")).DeleteRange(B("r05"), B("r10"), cf),
                1 => new WriteBatch().Delete(B("r00")).Delete(B("r00"), cf),
                _ => new WriteBatch().Put(B($"f{i}"), B($"new {i}")).Put(B($"f{i}"), B($"new {i}"), cf),
            })]);

            hooks.WriteGate.Set();
            await Task.WhenAll([leader, .. followers]);

            // The held leader's group, and the followers as one group, inserted at once when on.
            Assert.Equal(3, db.WriteGroupCount);
            CheckMixedGroup(db, concurrent, followerCount);

            // A group decides its switches from the memtables as they are. With pipelined commits
            // the followers' group can prepare before the held leader's inserts are in, and then the
            // switch comes with the next group, as in RocksDB.
            db.Put(B("after"), B("1"));
            Assert.True(SpinWait.SpinUntil(() => db.FlushCount == 1, TimeSpan.FromSeconds(10)), "The full memtable was not switched and flushed.");
            CheckContents(db, cf);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["cf"]))
            CheckContents(db, db.GetColumnFamily("cf"));

        static void CheckContents(SolariumDb db, ColumnFamily cf)
        {
            Assert.Equal(40 + 1 + (2 * followerCount) + 1, db.LatestSequenceNumber);
            Assert.Equal(8 * 1024, db.Get(B("leader"))!.Length);
            foreach (ColumnFamily family in new[] { db.DefaultColumnFamily, cf })
            {
                for (int i = 0; i < 20; i++)
                {
                    bool deleted = i == 0 || (i >= 5 && i < 10);
                    Assert.Equal(deleted ? null : "old", S(db.Get(B($"r{i:D2}"), family)));
                }

                for (int i = 2; i < followerCount; i++)
                    Assert.Equal($"new {i}", S(db.Get(B($"f{i}"), family)));
            }
        }
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndInsertPaths))]
    public async Task Flushes_during_the_inserts_of_groups_lose_no_entry(string environment, bool concurrent, bool pipelined)
    {
        const int threads = 8;
        const int puts = 300;
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 8 * 1024 },
            AllowConcurrentMemtableWrite = concurrent,
            EnablePipelinedWrite = pipelined,
        };
        byte[] value = new byte[100];

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            using CancellationTokenSource stop = new();
            Task flusher = Task.Run(() =>
            {
                for (int i = 0; !stop.IsCancellationRequested; i++)
                    db.Flush(wait: i % 2 == 0);
            });

            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
            {
                for (int i = 0; i < puts; i++)
                    db.Put(B($"t{t}:{i:D3}"), value);
            });

            stop.Cancel();
            await flusher;
            Assert.True(db.FlushCount > 0);
            Assert.Equal(threads * puts, CountKeys(db));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Equal(threads * puts, db.LatestSequenceNumber);
            Assert.Equal(threads * puts, CountKeys(db));
        }
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task A_wal_write_failure_of_a_group_fails_every_writer_and_launches_no_insert(bool concurrent, bool pipelined)
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        const int followerCount = 8;

        using (SolariumDb db = SolariumDb.Open(DbPath, new DbOptions { Environment = hooks, AllowConcurrentMemtableWrite = concurrent, EnablePipelinedWrite = pipelined }))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            // The leader's write is done and its flush is held; the followers queue behind it, and
            // their group's write fails.
            hooks.FlushGate.Reset();
            hooks.FlushStarted.Reset();
            Task leader = Task.Factory.StartNew(() => db.Put(B("leader"), B("1"), options: WriteOptions.Synced), TaskCreationOptions.LongRunning);
            Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

            Task[] followers = [.. Enumerable.Range(0, followerCount).Select(i =>
                Task.Factory.StartNew(() => db.Put(B($"f{i}"), B("1")), TaskCreationOptions.LongRunning))];
            Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == followerCount, TimeSpan.FromSeconds(10)));

            hooks.FailWrites = true;
            hooks.FlushGate.Set();
            await leader;

            foreach (Task follower in followers)
            {
                SolariumException failed = await Assert.ThrowsAsync<SolariumException>(() => follower);
                Assert.IsType<IOException>(failed.InnerException);
            }

            Assert.Equal(0, db.ConcurrentInserts().ParallelGroups);
            Assert.Equal(1, db.LatestSequenceNumber);
            Assert.Null(db.Get(B("f0")));
            hooks.FailWrites = false;
            Assert.Throws<SolariumException>(() => db.Put(B("after"), B("1")));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal(1, db.LatestSequenceNumber);
            Assert.Equal("1", S(db.Get(B("leader"))));
            Assert.Null(db.Get(B("f0")));
        }
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task Dispose_during_a_group_of_several_writers_keeps_every_acknowledged_write(bool concurrent, bool pipelined)
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance);
        const int followerCount = 8;

        DbOptions options = new()
        {
            Environment = hooks,
            AllowConcurrentMemtableWrite = concurrent,
            EnablePipelinedWrite = pipelined,
            WriterSpin = HeldSpin,
            WriterSpinProbeInterval = 0,
        };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            hooks.WriteGate.Reset();
            hooks.WriteStarted.Reset();
            Task leader = Task.Factory.StartNew(() => db.Put(B("leader"), B("1")), TaskCreationOptions.LongRunning);
            Assert.True(hooks.WriteStarted.Wait(TimeSpan.FromSeconds(10)));

            Task[] followers = QueueFollowers(db, [.. Enumerable.Range(0, followerCount).Select(i => new WriteBatch().Put(B($"f{i}"), B("1")))]);

            // The followers' group syncs: hold its WAL flush, so its leader is active when Dispose starts.
            hooks.FlushGate.Reset();
            hooks.FlushStarted.Reset();
            hooks.WriteGate.Set();
            Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

            Thread dispose = new(db.Dispose);
            dispose.Start();
            Assert.True(SpinWait.SpinUntil(() => dispose.ThreadState.HasFlag(ThreadState.WaitSleepJoin), TimeSpan.FromSeconds(10)));

            hooks.FlushGate.Set();
            await Task.WhenAll([leader, .. followers]);
            Assert.True(dispose.Join(TimeSpan.FromSeconds(10)));
            CheckMixedGroup(db, concurrent, followerCount);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal(followerCount + 1, db.LatestSequenceNumber);
            Assert.Equal("1", S(db.Get(B("leader"))));
            for (int i = 0; i < followerCount; i++)
                Assert.Equal("1", S(db.Get(B($"f{i}"))));
        }
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // A spin that outlasts any held leader, so a writer that spins is still spinning at the launch.
    private static readonly TimeSpan HeldSpin = TimeSpan.FromSeconds(30);

    // The writers of QueueFollowers that do not sync, the first of them the next leader.
    private const int SpinningFollowers = 4;

    // Queue the batches behind a held leader, one at a time, so each joins in order. The first
    // SpinningFollowers do not sync, so they spin for HeldSpin; the others sync, so they and every
    // writer after them block at once. Returns when the blocked ones are blocked: the next leader
    // launches the inserts of its spinning members and inserts the batches of the blocked ones.
    private static Task[] QueueFollowers(SolariumDb db, WriteBatch[] batches)
    {
        Task[] followers = new Task[batches.Length];
        for (int i = 0; i < batches.Length; i++)
        {
            WriteBatch batch = batches[i];
            WriteOptions options = i < SpinningFollowers ? WriteOptions.Default : WriteOptions.Synced;
            followers[i] = Task.Factory.StartNew(() => db.Write(batch, options), TaskCreationOptions.LongRunning);
            Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == i + 1, TimeSpan.FromSeconds(10)));
        }

        int blocked = batches.Length - SpinningFollowers;
        Assert.True(SpinWait.SpinUntil(() => db.WriterWaits().Blocked == blocked, TimeSpan.FromSeconds(10)));
        return followers;
    }

    // The group of QueueFollowers: with concurrent inserts on, its leader launched the spinning
    // members and kept the blocked ones; off, it inserted every batch.
    private static void CheckMixedGroup(SolariumDb db, bool concurrent, int followerCount)
    {
        (long parallelGroups, long launched, long kept, _, _) = db.ConcurrentInserts();
        Assert.Equal(concurrent ? 1 : 0, parallelGroups);
        Assert.Equal(concurrent ? SpinningFollowers - 1 : 0, launched);
        Assert.Equal(concurrent ? followerCount - SpinningFollowers : 0, kept);
    }

    private static int CountKeys(SolariumDb db)
    {
        int count = 0;
        using Iterator iterator = db.NewIterator();
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            count++;

        return count;
    }
}
