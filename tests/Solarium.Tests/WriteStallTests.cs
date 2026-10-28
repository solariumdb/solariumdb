using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Write stalls on too many level 0 SSTables: writes slow down at the slowdown trigger, wait at the
/// stop trigger, and run again when compaction catches up; Dispose and failures end a stop.
/// </summary>
public sealed class WriteStallTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();
    private readonly ManualResetEventSlim _held = new();
    private readonly ManualResetEventSlim _release = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Writes_slow_down_at_the_slowdown_trigger_stop_at_the_stop_trigger_and_run_again_when_compaction_catches_up(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Stalling(scope.Options(createMissingColumnFamilies: true), slowdown: 2, stop: 3) with { DelayedWriteRate = 64 * 1024 };

        using (SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]))
        {
            try
            {
                // The first table starts a compaction, which the hook holds while level 0 grows.
                db.Put(B("a"), B("1"));
                db.Flush();
                Assert.True(_held.Wait(Timeout), "no compaction started");
                Assert.Equal((false, 0L), Now(db));

                db.Put(B("b"), B("1"));
                db.Flush();
                Assert.Equal((false, 64L * 1024), Now(db));

                // A group pays for the bytes of the group before it: 16 KiB at 64 KiB/s is 250 ms.
                db.Put(B("big"), new byte[16 * 1024]);
                long delayedBefore = db.WriteStalls().DelayedGroups;
                DateTime start = DateTime.UtcNow;
                db.Put(B("after"), B("1"));
                Assert.True(DateTime.UtcNow - start >= TimeSpan.FromMilliseconds(200), $"the write took {DateTime.UtcNow - start}");
                Assert.True(db.WriteStalls().DelayedGroups > delayedBefore);

                // A stop holds back the writes to every family, not only to the one past its trigger.
                db.Put(B("c"), B("1"));
                db.Flush();
                Assert.Equal((true, 0L), Now(db));

                ColumnFamily other = db.GetColumnFamily("other");
                Task stopped = Task.Run(() => db.Put(B("o"), B("1"), other));
                Assert.True(SpinWait.SpinUntil(() => db.WriteStalls().StoppedGroups == 1, Timeout), "the write did not stop");
                await Task.Delay(100);
                Assert.False(stopped.IsCompleted);
                Assert.Null(db.Get(B("o"), other));

                _release.Set();
                await stopped.WaitAsync(Timeout);
                Assert.Equal("1", S(db.Get(B("o"), other)));
                Assert.True(SpinWait.SpinUntil(() => db.DefaultColumnFamily.Version.Level0.Length == 0, Timeout), "level 0 did not empty");
                Assert.Equal((false, 0L), Now(db));
                Assert.True(db.WriteStalls().Stalled > TimeSpan.Zero);

                db.Put(B("d"), B("1"));
            }
            finally
            {
                _release.Set();
            }
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options(), ["other"]))
        {
            foreach (string key in new[] { "a", "b", "big", "after", "c", "d" })
                Assert.NotNull(db.Get(B(key)));

            Assert.Equal("1", S(db.Get(B("o"), db.GetColumnFamily("other"))));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Dispose_fails_the_writes_that_a_stop_holds_without_waiting_for_compaction(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Stalling(scope.Options(), slowdown: 2, stop: 2);

        SolariumDb db = SolariumDb.Open(DbPath, options);
        try
        {
            db.Put(B("a"), B("1"));
            db.Flush();
            Assert.True(_held.Wait(Timeout), "no compaction started");
            db.Put(B("b"), B("1"));
            db.Flush();
            Assert.True(db.WriteStalls().Stopped);

            // One writer leads and waits for the stop to end; the others wait in the queue behind it.
            Task[] writers = [.. Enumerable.Range(0, 3).Select(i => Task.Run(() => db.Put(B($"w{i}"), B("1"))))];
            Assert.True(SpinWait.SpinUntil(() => db.WriteStalls().StoppedGroups == 1 && db.QueuedWriterCount == 2, Timeout), "the writes did not stop");

            // The compaction stays held: the writers fail on Dispose alone.
            Task dispose = Task.Run(db.Dispose);
            foreach (Task writer in writers)
                await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.WaitAsync(Timeout));

            Assert.False(dispose.IsCompleted);
            _release.Set();
            await dispose.WaitAsync(Timeout);
        }
        finally
        {
            _release.Set();
            db.Dispose();
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, scope.Options());
        Assert.Equal("1", S(reopened.Get(B("b"))));
        for (int i = 0; i < 3; i++)
            Assert.Null(reopened.Get(B($"w{i}")));
    }

    [Fact]
    public async Task A_flush_failure_fails_the_writes_that_a_stop_holds()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance) { FailingPaths = path => path.EndsWith(".sst", StringComparison.Ordinal) };
        DbOptions options = Stalling(new DbOptions { Environment = hooks, CreateMissingColumnFamilies = true }, slowdown: 2, stop: 2);

        using SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]);
        try
        {
            ColumnFamily other = db.GetColumnFamily("other");
            db.Put(B("a"), B("1"));
            db.Flush();
            Assert.True(_held.Wait(Timeout), "no compaction started");
            db.Put(B("o"), B("1"), other);
            db.Put(B("b"), B("1"));
            db.Flush();

            Task stopped = Task.Run(() => db.Put(B("c"), B("1")));
            Assert.True(SpinWait.SpinUntil(() => db.WriteStalls().StoppedGroups == 1, Timeout), "the write did not stop");

            // A flush needs no writer, so it runs during the stop; its failure ends the stop.
            hooks.FailWrites = true;
            Assert.Throws<SolariumException>(() => db.Flush(other));
            SolariumException failed = await Assert.ThrowsAsync<SolariumException>(() => stopped.WaitAsync(Timeout));
            Assert.Contains("no more writes", failed.Message);
            Assert.Equal("1", S(db.Get(B("b"))));
        }
        finally
        {
            _release.Set();
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_database_that_opens_with_level_0_at_the_stop_trigger_stops_writes_until_compaction_catches_up(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, CompactionTests.Manual(scope.Options())))
        {
            foreach (string key in new[] { "a", "b", "c" })
            {
                db.Put(B(key), B("1"));
                db.Flush();
            }
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, Stalling(scope.Options(), slowdown: 2, stop: 3)))
        {
            try
            {
                Assert.True(db.WriteStalls().Stopped);
                Task stopped = Task.Run(() => db.Put(B("d"), B("1")));
                Assert.True(SpinWait.SpinUntil(() => db.WriteStalls().StoppedGroups == 1, Timeout), "the write did not stop");

                _release.Set();
                await stopped.WaitAsync(Timeout);
                Assert.True(SpinWait.SpinUntil(() => db.DefaultColumnFamily.Version.Level0.Length == 0, Timeout), "level 0 did not empty");
                Assert.Equal((false, 0L), Now(db));
            }
            finally
            {
                _release.Set();
            }
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, scope.Options());
        string[] expected = ["a=1", "b=1", "c=1", "d=1"];
        Assert.Equal(expected, CompactionTests.Scan(reopened));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_family_without_automatic_compactions_slows_and_stops_no_writes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                DisableAutoCompactions = true,
                Level0FileNumCompactionTrigger = 1,
                Level0SlowdownWritesTrigger = 1,
                Level0StopWritesTrigger = 1,
            },
        };

        // On a task, so a write that stops fails the test instead of hanging it; Dispose ends the stop.
        using SolariumDb db = SolariumDb.Open(DbPath, options);
        await Task.Run(() =>
        {
            for (int i = 0; i < 3; i++)
            {
                db.Put(B($"k{i}"), B("1"));
                db.Flush();
            }

            db.Put(B("last"), B("1"));
        }).WaitAsync(Timeout);

        Assert.Equal(3, db.DefaultColumnFamily.Version.Level0.Length);
        (bool stopped, long rate, long delayed, long stoppedGroups, TimeSpan _) = db.WriteStalls();
        Assert.Equal((false, 0L, 0L, 0L), (stopped, rate, delayed, stoppedGroups));
    }

    [Fact]
    public void Write_stall_triggers_out_of_order_or_a_delayed_write_rate_below_1_are_refused()
    {
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, new DbOptions
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 8, Level0SlowdownWritesTrigger = 4 },
        }));
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, new DbOptions
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0SlowdownWritesTrigger = 30, Level0StopWritesTrigger = 29 },
        }));
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, new DbOptions { DelayedWriteRate = 0 }));

        // Equal triggers are allowed.
        using SolariumDb db = SolariumDb.Open(DbPath, new DbOptions
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 2 },
        });
    }

    /// <summary>Releases a held compaction and deletes the temporary directory.</summary>
    public void Dispose()
    {
        _release.Set();
        _held.Dispose();
        _release.Dispose();
        _directory.Dispose();
    }

    // Options where the first level 0 table starts a compaction, and the hook holds every compaction
    // until the test sets _release.
    private DbOptions Stalling(DbOptions options, int slowdown, int stop)
    {
        return options with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                Level0FileNumCompactionTrigger = 1,
                Level0SlowdownWritesTrigger = slowdown,
                Level0StopWritesTrigger = stop,
            },
            BeforeCompaction = () =>
            {
                _held.Set();
                _release.Wait();
            },
        };
    }

    // The stall now: whether writes stop, and the delayed write rate (0 when writes are not delayed).
    private static (bool Stopped, long DelayedWriteRate) Now(SolariumDb db)
    {
        (bool stopped, long rate, long _, long _, TimeSpan _) = db.WriteStalls();
        return (stopped, rate);
    }
}
