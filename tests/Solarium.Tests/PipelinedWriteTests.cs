using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// The pipelined commit of write groups: a group appends to the WAL, joins the memtable queue and
/// gives up its leadership, and one memtable stage at a time inserts and publishes the queue. Held
/// stages show the order of publication, the groups that one stage takes, flushes and WAL rotations
/// between the stages, insert failures, and Dispose.
/// </summary>
public sealed class PipelinedWriteTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_group_held_between_its_stages_keeps_the_groups_after_it_unpublished(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Hold hold = new(at: 2);

        using (SolariumDb db = SolariumDb.Open(DbPath, Pipelined(scope.Options(), hold)))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            // The held group gave up its leadership after its append, so the next group appends too,
            // and then waits in the memtable queue.
            Task next = Task.Factory.StartNew(() => db.Put(B("next"), B("3")), TaskCreationOptions.LongRunning);
            Assert.True(SpinWait.SpinUntil(() => db.WriteGroupCount == 3, TimeSpan.FromSeconds(10)));
            await Task.Delay(100);

            Assert.False(next.IsCompleted);
            Assert.Equal(1, db.LatestSequenceNumber);
            Assert.Null(db.Get(B("held")));
            Assert.Null(db.Get(B("next")));

            hold.Release.Set();
            await Task.WhenAll(held, next);
            Assert.Equal(3, db.LatestSequenceNumber);
            Assert.Equal("3", S(db.Get(B("next"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal(3, db.LatestSequenceNumber);
            Assert.Equal("2", S(db.Get(B("held"))));
            Assert.Equal("3", S(db.Get(B("next"))));
        }
    }

    [Fact]
    public async Task A_flush_between_the_stages_of_a_group_leaves_the_group_to_the_next_memtable()
    {
        Hold hold = new(at: 2);
        using (SolariumDb db = SolariumDb.Open(DbPath, Pipelined(new DbOptions(), hold)))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            // The held group is in no memtable yet, so the flush neither waits for it nor takes it.
            await Task.Run(() => db.Flush()).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(db.DefaultColumnFamily.Version.Active.IsEmpty);
            Assert.Equal(1, db.LatestSequenceNumber);

            hold.Release.Set();
            await held;
            Assert.False(db.DefaultColumnFamily.Version.Active.IsEmpty);
            Assert.Equal("2", S(db.Get(B("held"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal("1", S(db.Get(B("first"))));
            Assert.Equal("2", S(db.Get(B("held"))));
        }
    }

    [Fact]
    public async Task A_flush_waits_for_a_group_in_its_memtable_stage_so_the_group_stays_in_one_memtable()
    {
        Hold hold = new(at: 2);
        DbOptions options = new DbOptions { EnablePipelinedWrite = true, BeforeMemTableInsert = hold.BeforeMemTableStage };
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Write(new WriteBatch().Put(B("held a"), B("2")).Put(B("held b"), B("3"))), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            // The group's turn has come, so the switch waits until all of its entries are in the
            // memtable that the flush takes.
            Task flush = Task.Run(() => db.Flush());
            await Task.Delay(200);
            Assert.False(flush.IsCompleted);

            hold.Release.Set();
            await Task.WhenAll(held, flush);
            Assert.True(db.DefaultColumnFamily.Version.Active.IsEmpty);
            Assert.Equal("3", S(db.Get(B("held b"))));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
            Assert.Equal("2", S(db.Get(B("held a"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_wal_rotation_waits_for_the_groups_appended_to_the_file_it_closes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Hold hold = new(at: 2);

        using (SolariumDb db = SolariumDb.Open(DbPath, Pipelined(scope.Options(), hold)))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            // The flush makes "first" durable in an SSTable, and the next group rotates the WAL.
            // With no memtable holding data from the current file, a rotation that did not wait
            // would delete the file that holds the held group.
            db.Flush();
            Task next = Task.Factory.StartNew(() => db.Put(B("next"), B("3")), TaskCreationOptions.LongRunning);
            await Task.Delay(200);
            Assert.False(next.IsCompleted);
            Assert.Equal(2, db.WriteGroupCount);

            hold.Release.Set();
            await Task.WhenAll(held, next);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal(3, db.LatestSequenceNumber);
            Assert.Equal("1", S(db.Get(B("first"))));
            Assert.Equal("2", S(db.Get(B("held"))));
            Assert.Equal("3", S(db.Get(B("next"))));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task An_insert_failure_fails_its_group_and_every_group_appended_after_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Hold hold = new(at: 2, fail: true);

        using (SolariumDb db = SolariumDb.Open(DbPath, Pipelined(scope.Options(), hold)))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));
            Task next = Task.Factory.StartNew(() => db.Put(B("next"), B("3")), TaskCreationOptions.LongRunning);
            Assert.True(SpinWait.SpinUntil(() => db.WriteGroupCount == 3, TimeSpan.FromSeconds(10)));

            hold.Release.Set();
            SolariumException failed = await Assert.ThrowsAsync<SolariumException>(() => held);
            Assert.IsType<InvalidOperationException>(failed.InnerException);
            await Assert.ThrowsAsync<SolariumException>(() => next);

            Assert.Equal(1, db.LatestSequenceNumber);
            Assert.Null(db.Get(B("held")));
            Assert.Null(db.Get(B("next")));
            Assert.Throws<SolariumException>(() => db.Put(B("later"), B("4")));
        }

        // Both failed groups are in the WAL, so an open replays them, as after any insert failure.
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal(3, db.LatestSequenceNumber);
            Assert.Equal("2", S(db.Get(B("held"))));
            Assert.Equal("3", S(db.Get(B("next"))));
        }
    }

    [Fact]
    public async Task An_insert_failure_fails_the_groups_that_join_the_memtable_queue_after_its_stage_took_it()
    {
        Hold hold = new(at: 2, fail: true);
        DbOptions options = new DbOptions { EnablePipelinedWrite = true, BeforeMemTableInsert = hold.BeforeMemTableStage };
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            // The held stage has its group already, so the next group waits for the stage after it.
            Task next = Task.Factory.StartNew(() => db.Put(B("next"), B("3")), TaskCreationOptions.LongRunning);
            Assert.True(SpinWait.SpinUntil(() => db.WriteGroupCount == 3, TimeSpan.FromSeconds(10)));

            hold.Release.Set();
            await Assert.ThrowsAsync<SolariumException>(() => held);
            await Assert.ThrowsAsync<SolariumException>(() => next).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(3, db.MemTableStages().Stages);
            Assert.Equal(1, db.LatestSequenceNumber);
            Assert.Null(db.Get(B("next")));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
            Assert.Equal("3", S(db.Get(B("next"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task One_memtable_stage_publishes_every_group_that_joined_the_queue_while_a_stage_ran(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Hold hold = new(at: 2);
        DbOptions options = scope.Options() with { EnablePipelinedWrite = true, BeforeMemTableInsert = hold.BeforeMemTableStage };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            // Each write appends as a group of its own, and joins the queue behind the held stage.
            List<Task> queued = [];
            for (int i = 0; i < 3; i++)
            {
                string key = $"queued {i}";
                queued.Add(Task.Factory.StartNew(() => db.Put(B(key), B("3")), TaskCreationOptions.LongRunning));
                long groups = 3 + i;
                Assert.True(SpinWait.SpinUntil(() => db.WriteGroupCount == groups, TimeSpan.FromSeconds(10)));
            }

            await Task.Delay(100);
            Assert.All(queued, task => Assert.False(task.IsCompleted));
            Assert.Equal(1, db.LatestSequenceNumber);

            hold.Release.Set();
            await Task.WhenAll([held, .. queued]).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(5, db.LatestSequenceNumber);
            Assert.Equal(5, db.WriteGroupCount);
            Assert.Equal(3, db.MemTableStages().Stages);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Equal(5, db.LatestSequenceNumber);
            for (int i = 0; i < 3; i++)
                Assert.Equal("3", S(db.Get(B($"queued {i}"))));
        }
    }

    [Fact]
    public async Task Dispose_waits_for_a_group_between_its_stages_and_keeps_its_write()
    {
        Hold hold = new(at: 2);
        using (SolariumDb db = SolariumDb.Open(DbPath, Pipelined(new DbOptions(), hold)))
        {
            db.Put(B("first"), B("1"));
            Task held = Task.Factory.StartNew(() => db.Put(B("held"), B("2")), TaskCreationOptions.LongRunning);
            Assert.True(hold.Reached.Wait(TimeSpan.FromSeconds(10)));

            Thread dispose = new(db.Dispose);
            dispose.Start();
            Assert.True(SpinWait.SpinUntil(() => dispose.ThreadState.HasFlag(ThreadState.WaitSleepJoin), TimeSpan.FromSeconds(10)));
            await Task.Delay(100);
            Assert.True(dispose.IsAlive);

            hold.Release.Set();
            await held;   // acknowledged
            Assert.True(dispose.Join(TimeSpan.FromSeconds(10)));
        }

        using (SolariumDb db = SolariumDb.Open(DbPath))
            Assert.Equal("2", S(db.Get(B("held"))));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_snapshot_of_any_published_sequence_number_holds_every_write_up_to_it(bool concurrentInserts)
    {
        const int writers = 8;
        const int puts = 2000;
        DbOptions options = new DbOptions
        {
            EnablePipelinedWrite = true,
            AllowConcurrentMemtableWrite = concurrentInserts,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30 },
        };
        using SolariumDb db = SolariumDb.Open(DbPath, options);

        // Every write is a new key in one memtable, so a snapshot at sequence number s holds exactly
        // s keys unless a group was published before a group with lower sequence numbers.
        int done = 0;
        int snapshots = 0;
        (long Snapshot, long Count)? gap = null;
        Thread reader = new(() =>
        {
            while (Volatile.Read(ref done) < writers && gap is null)
            {
                FamilyVersion version = db.DefaultColumnFamily.EnterVersion(out int slot);
                long snapshot = db.LatestSequenceNumber;
                using Iterator iterator = new(version, slot, snapshot, null, fillCache: true);
                long count = 0;
                for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                    count++;

                if (count != snapshot)
                    gap = (snapshot, count);
                snapshots++;
            }
        });
        reader.Start();

        // Threads of their own that start together: Parallel.For on a busy thread pool can run the
        // writers on fewer threads than writers, and then their writes never overlap.
        using Barrier start = new(writers);
        Thread[] threads = new Thread[writers];
        for (int w = 0; w < writers; w++)
        {
            int t = w;
            threads[w] = new Thread(() =>
            {
                start.SignalAndWait();
                for (int i = 0; i < puts; i++)
                    db.Put(B($"t{t}:{i:D5}"), B("1"));
                Interlocked.Increment(ref done);
            });
            threads[w].Start();
        }

        foreach (Thread thread in threads)
            thread.Join();
        reader.Join();

        Assert.Null(gap);
        Assert.Equal(writers * puts, db.LatestSequenceNumber);
        Assert.True(snapshots > 0);
        // The writes overlapped: a WAL group of several writers, or a memtable stage of several groups.
        Assert.True(db.MemTableStages().Stages < writers * puts, "No memtable stage took more than one writer.");
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    private static DbOptions Pipelined(DbOptions options, Hold hold) =>
        options with { EnablePipelinedWrite = true, BeforeMemTableStage = hold.BeforeMemTableStage };

    // Holds the memtable stage of the group whose first sequence number is `at` until Release is set,
    // and then lets it go on, or fails it as an insert failure would.
    private sealed class Hold(long at, bool fail = false)
    {
        public ManualResetEventSlim Reached { get; } = new();

        public ManualResetEventSlim Release { get; } = new();

        public void BeforeMemTableStage(long first)
        {
            if (first != at)
                return;

            Reached.Set();
            if (!Release.Wait(TimeSpan.FromSeconds(30)))
                throw new TimeoutException($"The group at sequence number {at} was never released.");
            if (fail)
                throw new InvalidOperationException($"An insert failure of the group at sequence number {at}.");
        }
    }
}
