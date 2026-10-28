using System.Diagnostics;
using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// The pacing of background writes: the rate limiter's rate, priorities, fairness and tuning, the table
/// writes through a megabyte buffer, the writeback of each megabyte, and the failures of both.
/// </summary>
public sealed class PacingTests : IDisposable
{
    private const int Megabyte = 1 << 20;

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Fact]
    public void A_limiter_hands_out_its_rate_one_period_at_a_time()
    {
        RateLimiter limiter = new(1_000_000, TimeSpan.FromMilliseconds(10));
        Assert.Equal(10_000, limiter.SingleBurstBytes);

        // The first period's budget is there at once; the other 29 pieces wait for their refills.
        Stopwatch elapsed = Stopwatch.StartNew();
        limiter.Request(300_000, high: false);
        Assert.InRange(elapsed.Elapsed.TotalSeconds, 0.25, 2);
        Assert.Equal(300_000, limiter.TotalBytesThrough);
        Assert.Equal(30, limiter.TotalRequests);
        Assert.True(limiter.TotalWait > TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public async Task Flushes_go_before_compactions_and_compactions_still_progress()
    {
        RateLimiter limiter = new(100_000, TimeSpan.FromMilliseconds(10));

        // A compaction that wants 100 periods' worth waits behind a flush that comes later. Threads of
        // their own, and the flush starts once the compaction has its first period's bytes and waits for
        // the rest, not after a delay: a busy thread pool could resume a delay seconds late.
        Stopwatch elapsed = Stopwatch.StartNew();
        TimeSpan lowDone = TimeSpan.Zero, highDone = TimeSpan.Zero;
        Thread low = new(() =>
        {
            limiter.Request(100_000, high: false);
            lowDone = elapsed.Elapsed;
        });

        Thread high = new(() =>
        {
            limiter.Request(5_000, high: true);
            highDone = elapsed.Elapsed;
        });

        low.Start();
        SpinWait.SpinUntil(() => limiter.TotalRequests > 1);
        high.Start();
        low.Join();
        high.Join();
        Assert.True(highDone < lowDone, $"the flush ended at {highDone}, the compaction at {lowDone}");

        // Flushes that never stop asking still leave the compactions one refill in ten. Threads of their own,
        // so a busy thread pool cannot hold the compaction back.
        using CancellationTokenSource stop = new();
        Task[] flushes = [.. Enumerable.Range(0, 2).Select(_ => Task.Factory.StartNew(() =>
        {
            while (!stop.IsCancellationRequested)
                limiter.Request(1_000, high: true);
        }, TaskCreationOptions.LongRunning))];

        Task compaction = Task.Factory.StartNew(() => limiter.Request(3_000, high: false), TaskCreationOptions.LongRunning);
        await compaction.WaitAsync(TimeSpan.FromSeconds(10));
        stop.Cancel();
        await Task.WhenAll(flushes);
    }

    [Fact]
    public async Task An_auto_tuned_limiter_falls_when_idle_and_climbs_under_steady_demand_within_its_range()
    {
        RateLimiter limiter = new(10_000_000, TimeSpan.FromMilliseconds(1), autoTuned: true);
        Assert.Equal(5_000_000, limiter.BytesPerSecond);

        // More than 100 idle periods: the next request tunes it to the bottom of its range.
        await Task.Delay(200);
        limiter.Request(1, high: false);
        Assert.Equal(500_000, limiter.BytesPerSecond);

        // Writes that always wait raise it 5% a tune, and never past its maximum.
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < TimeSpan.FromSeconds(2))
            limiter.Request(limiter.SingleBurstBytes, high: false);

        Assert.InRange(limiter.BytesPerSecond, 550_000, 10_000_000);

        limiter.SetBytesPerSecond(400_000);
        Assert.True(limiter.BytesPerSecond <= 400_000);
    }

    [Fact]
    public void A_limiter_refuses_rates_and_periods_it_cannot_keep()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(1000, TimeSpan.FromTicks(10)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(1000, fairness: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(1000).SetBytesPerSecond(-1));
        Assert.Throws<ArgumentException>(() => SolariumDb.Open(DbPath, new DbOptions { BytesPerSync = -1 }));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_flush_writes_its_table_a_megabyte_at_a_time_and_starts_the_writeback_of_each(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { CountedPaths = IsTable };
        DbOptions options = scope.Options() with { Environment = hooks, BytesPerSync = Megabyte };

        long size;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            PutValues(db, 2500);
            db.Flush();
            size = db.GetLiveFilesMetaData().Single().Size;
        }

        // About 2.5 MB: two full buffers and the rest, and the writeback of each megabyte written.
        Assert.InRange(size, 2 * Megabyte + 1, 3 * Megabyte);
        Assert.Equal(3, hooks.WriteCount);
        List<(string Path, long Offset, long Length)> ranges = hooks.SyncRanges;
        Assert.Equal([(0L, (long)Megabyte), (Megabyte, Megabyte)], ranges.Select(r => (r.Offset, r.Length)));
        Assert.All(ranges, range => Assert.True(IsTable(range.Path)));

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
            Assert.Equal(new string('v', 1000), S(db.Get(B("k01234"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_rate_limiter_paces_flushes_and_compactions_and_brings_the_writeback_with_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { CountedPaths = IsTable };
        RateLimiter limiter = new(2 * Megabyte, TimeSpan.FromMilliseconds(10));
        DbOptions options = scope.Options() with { Environment = hooks, RateLimiter = limiter };

        using SolariumDb db = SolariumDb.Open(DbPath, options);
        PutValues(db, 3000);
        Stopwatch elapsed = Stopwatch.StartNew();
        db.Flush();
        TimeSpan flush = elapsed.Elapsed;
        long flushed = db.GetLiveFilesMetaData().Single().Size;

        // About 3 MB at 2 MB/s, with the first period's budget at once.
        Assert.True(flush > TimeSpan.FromSeconds(1), $"the flush took {flush}");
        Assert.Equal(flushed, limiter.TotalBytesThrough);

        // BytesPerSync is 1 MB once a limiter is set.
        Assert.NotEmpty(hooks.SyncRanges);

        PutValues(db, 3000);
        db.Flush();
        long before = limiter.TotalBytesThrough;
        db.CompactRange(null, null);
        long compacted = db.GetLiveFilesMetaData().Sum(f => f.Size);
        Assert.Equal(compacted, limiter.TotalBytesThrough - before);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void One_limiter_paces_two_databases(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        RateLimiter limiter = new(1L << 30);
        DbOptions options = scope.Options() with { RateLimiter = limiter };

        long sizes = 0;
        foreach (string name in new[] { "one", "two" })
        {
            using SolariumDb db = SolariumDb.Open(_directory.Combine(name), options);
            PutValues(db, 200);
            db.Flush();
            sizes += db.GetLiveFilesMetaData().Single().Size;
        }

        Assert.Equal(sizes, limiter.TotalBytesThrough);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_failed_writeback_fails_the_flush_as_a_failed_write_would(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { FailingPaths = IsTable };
        DbOptions options = scope.Options() with { Environment = hooks, BytesPerSync = Megabyte };

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            PutValues(db, 2500);
            hooks.FailSyncRanges = true;
            Assert.Throws<SolariumException>(() => db.Flush());
            Assert.Throws<SolariumException>(() => db.Put(B("after"), B("1")));
        }

        hooks.FailSyncRanges = false;
        using (SolariumDb db = SolariumDb.Open(DbPath, options))
            Assert.Equal(new string('v', 1000), S(db.Get(B("k02499"))));
    }

    public void Dispose() => _directory.Dispose();

    private static bool IsTable(string path) => path.EndsWith(".sst", StringComparison.Ordinal);

    // Rows of 1,000 bytes that do not compress away.
    private static void PutValues(SolariumDb db, int count)
    {
        for (int i = 0; i < count; i++)
            db.Put(B($"k{i:D5}"), B(new string('v', 1000)));
    }
}
