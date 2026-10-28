using System.Diagnostics;
using System.Globalization;

namespace Solarium.Benchmarks;

/// <summary>
/// Synced batches while compactions catch up, as a Kahuna store takes them: a load of random keys with
/// 1 KiB values and no sync, then synced batches of 1,024 rows over the same keys for a while, with
/// one line a second of what the writes waited for: memtable switches that waited for a flush, write
/// stalls, the WAL append with its sync, and the compactions that ran.
/// </summary>
/// <remarks>
/// <c>synced [compactions=N] [rows=N] [seconds=N] [rate=MB/s] [auto=true] [bytespersync=KB] [dir=path]</c>:
/// <c>rate</c> gives the database a <see cref="RateLimiter"/> of that many MiB per second, auto-tuned with
/// <c>auto</c>, and <c>bytespersync</c> sets <see cref="DbOptions.BytesPerSync"/>. The data directory is
/// deleted at the end.
/// </remarks>
internal static class SyncedLoad
{
    private const int Batch = 1024;
    private const int ValueSize = 1024;

    public static void Run(string[] args)
    {
        Dictionary<string, string> settings = args.Select(a => a.Split('=', 2)).ToDictionary(p => p[0], p => p[1]);
        int compactions = int.Parse(settings.GetValueOrDefault("compactions", "1"), CultureInfo.InvariantCulture);
        long rows = long.Parse(settings.GetValueOrDefault("rows", "2000000"), CultureInfo.InvariantCulture);
        int seconds = int.Parse(settings.GetValueOrDefault("seconds", "40"), CultureInfo.InvariantCulture);
        string directory = Path.Combine(settings.GetValueOrDefault("dir", Path.GetTempPath()), $"solarium-synced-{Guid.NewGuid():N}");
        long rate = long.Parse(settings.GetValueOrDefault("rate", "0"), CultureInfo.InvariantCulture) * 1024 * 1024;
        bool auto = bool.Parse(settings.GetValueOrDefault("auto", "false"));
        long bytesPerSync = long.Parse(settings.GetValueOrDefault("bytespersync", "0"), CultureInfo.InvariantCulture) * 1024;
        // The load and its drain run with the limiter out of the way; the synced batches run at the rate.
        RateLimiter? limiter = rate > 0 ? new RateLimiter(1L << 40, autoTuned: auto) : null;

        DbOptions options = new()
        {
            MaxBackgroundCompactions = compactions,
            MeasureGroupStages = true,
            RateLimiter = limiter,
            BytesPerSync = bytesPerSync,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = 64L * 1024 * 1024,
                MaxWriteBufferNumber = 3,
                Compression = CompressionType.Lz4,
            },
        };

        try
        {
            using SolariumDb db = SolariumDb.Open(directory, options);
            Random random = new(1);
            byte[] value = new byte[ValueSize];
            Stopwatch load = Stopwatch.StartNew();
            for (long written = 0; written < rows; written += Batch)
            {
                WriteBatch batch = new();
                for (int i = 0; i < Batch; i++)
                {
                    random.NextBytes(value);
                    batch.Put(Key(random, rows), value);
                }

                db.Write(batch);
            }

            Console.WriteLine($"// synced compactions={compactions}: loaded {rows:N0} rows in {load.Elapsed.TotalSeconds:F1} s");
            Stopwatch drain = Stopwatch.StartNew();
            while (db.GetProperty("rocksdb.compaction-pending") != "0" || db.GetProperty("rocksdb.num-running-compactions") != "0")
                Thread.Sleep(100);

            Console.WriteLine($"// synced compactions={compactions}: drained in {drain.Elapsed.TotalSeconds:F1} s, files per level {Levels(db)}");

            limiter?.SetBytesPerSecond(rate);
            WriteOptions sync = new() { Sync = true };
            long total = 0;
            long second = 0;
            (long Waits, TimeSpan Waited) memTable = db.MemTableWaits();
            TimeSpan stalled = db.WriteStalls().Stalled;
            TimeSpan append = db.GroupStages().Append;
            Stopwatch run = Stopwatch.StartNew();
            Stopwatch window = Stopwatch.StartNew();
            while (run.Elapsed.TotalSeconds < seconds)
            {
                WriteBatch batch = new();
                for (int i = 0; i < Batch; i++)
                {
                    random.NextBytes(value);
                    batch.Put(Key(random, rows), value);
                }

                db.Write(batch, sync);
                second += Batch;
                if (window.Elapsed.TotalSeconds < 1)
                    continue;

                (long Waits, TimeSpan Waited) nowMemTable = db.MemTableWaits();
                TimeSpan nowStalled = db.WriteStalls().Stalled;
                TimeSpan nowAppend = db.GroupStages().Append;
                Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"t={run.Elapsed.TotalSeconds,5:F1} rows/s {second / window.Elapsed.TotalSeconds,8:N0}  memtable waits {nowMemTable.Waits - memTable.Waits} ({(nowMemTable.Waited - memTable.Waited).TotalMilliseconds,6:F0} ms)  stalled {(nowStalled - stalled).TotalMilliseconds,6:F0} ms  append {(nowAppend - append).TotalMilliseconds,6:F0} ms  running {db.GetProperty("rocksdb.num-running-compactions")}  levels {Levels(db)}"));
                total += second;
                second = 0;
                memTable = nowMemTable;
                stalled = nowStalled;
                append = nowAppend;
                window.Restart();
            }

            total += second;
            Console.WriteLine($"// synced compactions={compactions} rate={rate >> 20} auto={auto} bytespersync={bytesPerSync >> 10}: {total / run.Elapsed.TotalSeconds:N0} rows/s over {run.Elapsed.TotalSeconds:F1} s, {db.CompactionCounts()}, limiter at {limiter?.BytesPerSecond >> 20} MiB/s, waited {limiter?.TotalWait}");
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static byte[] Key(Random random, long rows) => System.Text.Encoding.ASCII.GetBytes(random.NextInt64(rows).ToString("D16", CultureInfo.InvariantCulture));

    private static string Levels(SolariumDb db) => string.Join(",", Enumerable.Range(0, 7).Select(l => db.GetProperty($"rocksdb.num-files-at-level{l}")));
}
