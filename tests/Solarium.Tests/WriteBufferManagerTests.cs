using System.Globalization;
using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// One cap on the memtables of several databases: the flushes it causes in each database, the family it
/// picks, what it counts across replay, flush, close and a failed open, and many writers on two
/// databases that share it.
/// </summary>
public sealed class WriteBufferManagerTests : IDisposable
{
    private const int Kilobyte = 1024;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Two_databases_that_share_a_manager_flush_to_keep_their_memtables_under_its_cap(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteBufferManager manager = new(64 * Kilobyte);
        DbOptions options = LargeMemTables(scope) with { WriteBufferManager = manager };

        using (SolariumDb one = SolariumDb.Open(_directory.Combine("one"), options))
        using (SolariumDb two = SolariumDb.Open(_directory.Combine("two"), options))
        {
            for (int i = 0; i < 400; i++)
            {
                one.Put(B($"k{i:D4}"), Value(i));
                two.Put(B($"k{i:D4}"), Value(i));

                // Each write checks the cap before its group, so the active memtables pass 7/8 of it
                // by at most the one group that each database wrote since.
                Assert.InRange(manager.MutableMemoryUsage, 0, (64 * Kilobyte * 7 / 8) + (2 * EntryBytes));
            }

            // 800 KB of writes against a cap of 64 KB; the memtables of each database are far under
            // their own WriteBufferSize, so only the manager flushed them.
            Assert.True(one.BufferManagerSwitches > 5, $"the first database switched {one.BufferManagerSwitches} times");
            Assert.True(two.BufferManagerSwitches > 5, $"the second database switched {two.BufferManagerSwitches} times");
            WaitForFlushes(one);
            WaitForFlushes(two);
            Assert.Equal(MemTableBytes(one) + MemTableBytes(two), manager.MemoryUsage);

            one.Flush();
            two.Flush();
            Assert.Equal(0, manager.MemoryUsage);
            Assert.Equal(0, manager.MutableMemoryUsage);
        }

        using (SolariumDb one = SolariumDb.Open(_directory.Combine("one"), options))
            Assert.Equal(Value(399), one.Get(B("k0399")));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_writing_database_flushes_its_family_with_the_oldest_memtable_and_never_another_database(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteBufferManager manager = new(32 * Kilobyte);
        DbOptions options = LargeMemTables(scope) with { WriteBufferManager = manager, CreateMissingColumnFamilies = true };

        using SolariumDb idle = SolariumDb.Open(_directory.Combine("idle"), options);
        using SolariumDb busy = SolariumDb.Open(_directory.Combine("busy"), options, ["a", "b"]);
        ColumnFamily a = busy.GetColumnFamily("a"), b = busy.GetColumnFamily("b");

        // A quarter of the cap in a database that takes no more writes.
        for (int i = 0; i < 8; i++)
            idle.Put(B($"k{i}"), Value(i));
        long idleBytes = MemTableBytes(idle);

        // a holds the oldest data of the busy database; the writes to b pass the cap.
        busy.Put(B("a0"), Value(0), a);
        for (int i = 0; i < 24 && busy.BufferManagerSwitches == 0; i++)
            busy.Put(B($"b{i:D2}"), Value(i), b);

        Assert.Equal(1, busy.BufferManagerSwitches);
        Assert.True(SpinWait.SpinUntil(() => a.FlushCount == 1, Timeout), "a was not flushed");
        Assert.Equal(0, b.FlushCount);
        Assert.Equal(0, idle.DefaultColumnFamily.FlushCount);
        Assert.Equal(idleBytes, MemTableBytes(idle));

        // The next one is b's, the oldest that is left.
        for (int i = 24; i < 64 && busy.BufferManagerSwitches == 1; i++)
            busy.Put(B($"b{i:D2}"), Value(i), b);

        Assert.Equal(2, busy.BufferManagerSwitches);
        Assert.True(SpinWait.SpinUntil(() => b.FlushCount == 1, Timeout), "b was not flushed");
        Assert.Equal(0, idle.DefaultColumnFamily.FlushCount);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Replay_charges_the_manager_and_the_first_write_past_the_cap_flushes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string path = _directory.Combine("db");
        using (SolariumDb db = SolariumDb.Open(path, LargeMemTables(scope)))
        {
            for (int i = 0; i < 40; i++)
                db.Put(B($"k{i:D2}"), Value(i));
        }

        WriteBufferManager manager = new(16 * Kilobyte);
        using (SolariumDb db = SolariumDb.Open(path, LargeMemTables(scope) with { WriteBufferManager = manager }))
        {
            // The WAL holds about 40 KB, past the cap; replay flushes only at WriteBufferSize, as
            // RocksDB's recovery, so all of it is in the memtable and charged.
            Assert.Equal(MemTableBytes(db), manager.MemoryUsage);
            Assert.True(manager.MemoryUsage > 16 * Kilobyte);
            Assert.Equal(0, db.DefaultColumnFamily.FlushCount);

            db.Put(B("after"), Value(40));
            Assert.Equal(1, db.BufferManagerSwitches);
            Assert.True(SpinWait.SpinUntil(() => manager.MemoryUsage == MemTableBytes(db) && manager.MemoryUsage < 16 * Kilobyte, Timeout),
                $"the manager holds {manager.MemoryUsage} bytes");
            Assert.Equal(Value(0), db.Get(B("k00")));
        }

        Assert.Equal(0, manager.MemoryUsage);

        // A replay that flushes at WriteBufferSize charges the memtables it flushes and releases them.
        using (SolariumDb db = SolariumDb.Open(_directory.Combine("replay-flush"), LargeMemTables(scope)))
        {
            for (int i = 0; i < 40; i++)
                db.Put(B($"k{i:D2}"), Value(i));
        }

        DbOptions small = scope.Options() with
        {
            WriteBufferManager = manager,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 16 * Kilobyte },
        };
        using (SolariumDb db = SolariumDb.Open(_directory.Combine("replay-flush"), small))
        {
            Assert.True(db.DefaultColumnFamily.FlushCount >= 2);
            Assert.Equal(MemTableBytes(db), manager.MemoryUsage);
        }

        Assert.Equal(0, manager.MemoryUsage);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_closed_database_and_a_failed_open_leave_nothing_charged(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        // Only table flushes wait at the gate: the WAL rotation after a switch flushes the WAL.
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.EndsWith(".sst", StringComparison.Ordinal) };
        WriteBufferManager manager = new(1L << 30);
        DbOptions options = LargeMemTables(scope) with { Environment = hooks, WriteBufferManager = manager, CreateMissingColumnFamilies = true };
        string path = _directory.Combine("db");

        using (SolariumDb db = SolariumDb.Open(path, options, ["a"]))
        using (hooks.OpenGatesAtEnd())
        {
            // Data in an active memtable, an immutable one held by a gated flush, and a flushed one.
            db.Put(B("flushed"), Value(0));
            db.Flush();
            hooks.FlushGate.Reset();
            db.Put(B("immutable"), Value(1), db.GetColumnFamily("a"));
            db.Flush(db.GetColumnFamily("a"), wait: false);
            db.Put(B("active"), Value(2));

            Assert.Equal(MemTableBytes(db), manager.MemoryUsage);
            Assert.Equal(MemTableBytes(db, "default"), manager.MutableMemoryUsage);
        }

        Assert.Equal(0, manager.MemoryUsage);
        Assert.Equal(0, manager.MutableMemoryUsage);

        // Replay charges the memtables again, and the open fails after it, at its manifest save.
        hooks.FailDirectoryFlushes = true;
        Assert.ThrowsAny<Exception>(() => SolariumDb.Open(path, options, ["a"]));
        Assert.Equal(0, manager.MemoryUsage);
        Assert.Equal(0, manager.MutableMemoryUsage);

        hooks.FailDirectoryFlushes = false;
        using (SolariumDb db = SolariumDb.Open(path, options, ["a"]))
        {
            Assert.Equal(Value(2), db.Get(B("active")));
            Assert.Equal(Value(1), db.Get(B("immutable"), db.GetColumnFamily("a")));
            Assert.True(manager.MemoryUsage > 0);
        }

        Assert.Equal(0, manager.MemoryUsage);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void DbWriteBufferSize_caps_one_database_across_its_families_and_a_manager_overrides_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = LargeMemTables(scope) with { DbWriteBufferSize = 16 * Kilobyte, CreateMissingColumnFamilies = true };
        string[] families = ["a", "b", "c"];

        using (SolariumDb db = SolariumDb.Open(_directory.Combine("own"), options, families))
        {
            for (int i = 0; i < 60; i++)
                db.Put(B($"k{i:D2}"), Value(i), db.GetColumnFamily(families[i % 3]));

            Assert.True(db.BufferManagerSwitches >= 3, $"{db.BufferManagerSwitches} switches");
            Assert.True(SpinWait.SpinUntil(() => families.All(f => db.GetColumnFamily(f).FlushCount > 0), Timeout), "a family was never flushed");
        }

        WriteBufferManager manager = new(1L << 30);
        using (SolariumDb db = SolariumDb.Open(_directory.Combine("shared"), options with { WriteBufferManager = manager }, families))
        {
            for (int i = 0; i < 60; i++)
                db.Put(B($"k{i:D2}"), Value(i), db.GetColumnFamily(families[i % 3]));

            Assert.Equal(0, db.BufferManagerSwitches);
            Assert.Equal(MemTableBytes(db), manager.MemoryUsage);
        }

        Assert.Throws<ArgumentException>(() => SolariumDb.Open(_directory.Combine("bad"), new DbOptions { DbWriteBufferSize = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new WriteBufferManager(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => manager.SetBufferSize(0));
    }

    [Fact]
    public void A_manager_asks_for_a_flush_at_seven_eighths_active_or_at_its_cap_with_half_active()
    {
        WriteBufferManager manager = new(800);
        manager.Reserve(700);
        Assert.False(manager.ShouldFlush());
        manager.Reserve(1);
        Assert.True(manager.ShouldFlush());

        // 701 in all, 301 active: under the cap.
        manager.ScheduleFree(400);
        Assert.False(manager.ShouldFlush());

        // 801 in all, 401 active: at the cap, with half of it active.
        manager.Reserve(100);
        Assert.True(manager.ShouldFlush());
        manager.SetBufferSize(2000);
        Assert.False(manager.ShouldFlush());
    }

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void Writers_on_two_databases_that_share_a_manager_keep_every_write_and_leave_nothing_charged(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteBufferManager manager = new(48 * Kilobyte);
        DbOptions options = LargeMemTables(scope) with
        {
            WriteBufferManager = manager,
            EnablePipelinedWrite = pipelined,
            CreateMissingColumnFamilies = true,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 20, MaxWriteBufferNumber = 3 },
        };

        string[] paths = [_directory.Combine("one"), _directory.Combine("two")];
        SolariumDb[] dbs = [SolariumDb.Open(paths[0], options, ["x"]), SolariumDb.Open(paths[1], options, ["x"])];
        const int Threads = 8, Batches = 300;
        try
        {
            // Four writers on each database, each on keys of its own, with batches that span both families.
            Thread[] threads = new Thread[Threads];
            for (int t = 0; t < Threads; t++)
            {
                int thread = t;
                SolariumDb db = dbs[thread % 2];
                threads[t] = new Thread(() =>
                {
                    Random random = new(thread);
                    for (int i = 0; i < Batches; i++)
                    {
                        WriteBatch batch = new();
                        batch.Put(B($"t{thread}-{i:D3}"), Value(i));
                        if (random.Next(3) == 0)
                            batch.Put(B($"t{thread}-{i:D3}"), Value(i), db.GetColumnFamily("x"));
                        db.Write(batch);
                    }
                });
                threads[t].Start();
            }

            foreach (Thread thread in threads)
                Assert.True(thread.Join(Timeout), "a writer did not finish");

            Assert.True(dbs[0].BufferManagerSwitches > 0 && dbs[1].BufferManagerSwitches > 0);
            foreach (SolariumDb db in dbs)
            {
                db.Flush();
                db.Flush(db.GetColumnFamily("x"));
            }

            Assert.Equal(0, manager.MemoryUsage);
            Assert.Equal(0, manager.MutableMemoryUsage);
        }
        finally
        {
            foreach (SolariumDb db in dbs)
                db.Dispose();
        }

        Assert.Equal(0, manager.MemoryUsage);
        for (int t = 0; t < Threads; t++)
        {
            using SolariumDb db = SolariumDb.Open(paths[t % 2], options, ["x"]);
            for (int i = 0; i < Batches; i++)
                Assert.Equal(Value(i), db.Get(B($"t{t}-{i:D3}")));
        }

        Assert.Equal(0, manager.MemoryUsage);
    }

    public void Dispose() => _directory.Dispose();

    // The bytes MemTable counts for one entry of Value(i) under a five-byte key.
    private const long EntryBytes = 5 + 1000 + 32;

    // Memtables that flush only when the manager asks: far larger than any test writes.
    private static DbOptions LargeMemTables(EnvironmentScope scope) => scope.Options() with
    {
        DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30 },
    };

    private static byte[] Value(int i) => B(i.ToString("D4", CultureInfo.InvariantCulture) + new string('v', 996));

    // Until no flush is pending or running, so the memtables and the manager's count hold still.
    private static void WaitForFlushes(SolariumDb db) =>
        Assert.True(SpinWait.SpinUntil(() => db.ColumnFamilies.All(f => db.GetProperty("rocksdb.num-immutable-mem-table", f) == "0"), Timeout), "a flush did not end");

    private static long MemTableBytes(SolariumDb db) => db.ColumnFamilies.Sum(f => long.Parse(db.GetProperty("rocksdb.cur-size-all-mem-tables", f)!, CultureInfo.InvariantCulture));

    private static long MemTableBytes(SolariumDb db, string family) =>
        long.Parse(db.GetProperty("rocksdb.cur-size-all-mem-tables", db.GetColumnFamily(family))!, CultureInfo.InvariantCulture);
}
