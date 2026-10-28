using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;
using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.IoUring;
using Solarium.Storage;

namespace Solarium.Kommander.Benchmarks;

/// <summary>
/// Measures Kommander's write path on its WAL backends: <c>RocksDbWAL</c> as Kommander ships it, and
/// <see cref="SolariumWAL"/> on the default and the io_uring environment, both with their default
/// tuning. Each writer thread writes its own partition, as Raft groups do. A case runs a warmup, then
/// measures for a fixed time; each run opens a new WAL in a new directory and deletes it after.
/// </summary>
/// <remarks>
/// <para>The engines of a case run one after the other, in an order that rotates with the case and
/// the repeat: the disk and the page cache change over a long run, and a fixed order favors whichever
/// engine runs first. The summary takes the median of the repeats.</para>
/// <para>Latency is that of one operation of the workload: one <c>Write</c> for an append or a batch,
/// the append and its commit marker together for the commit and mixed workloads. It is kept in a
/// histogram of 64 buckets per power of two (1.6% precision), not as samples, so the unsynced cases do
/// not allocate per operation to measure themselves.</para>
/// </remarks>
internal static class WalBenchmark
{
    private const int BatchSize = 64;
    private const int ReadWindow = 64;

    private static readonly string[] AllEngines = ["rocksdb", "solarium", "solarium-io_uring"];

    /// <summary>Runs the cases that <paramref name="args"/> select and prints each run, a summary, and optionally a CSV.</summary>
    public static void Run(string[] args)
    {
        Dictionary<string, string> options = ParseOptions(args);
        string set = options.GetValueOrDefault("cases", "full");
        List<WalCase> cases = set == "quick" ? QuickCases() : FullCases();
        if (options.TryGetValue("only", out string? only))
            cases = [.. cases.Where(c => c.ToString().Contains(only, StringComparison.Ordinal))];

        string[] engines = options.TryGetValue("engines", out string? list) ? list.Split(',') : AllEngines;
        int repeats = int.Parse(options.GetValueOrDefault("repeats", set == "quick" ? "1" : "3"), CultureInfo.InvariantCulture);
        TimeSpan warmup = TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("warmup", set == "quick" ? "0.5" : "2"), CultureInfo.InvariantCulture));
        TimeSpan measured = TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("seconds", set == "quick" ? "1" : "8"), CultureInfo.InvariantCulture));
        string root = options.GetValueOrDefault("dir", Path.GetTempPath());
        int? readers = options.TryGetValue("readers", out string? readerText) ? int.Parse(readerText, CultureInfo.InvariantCulture) : null;
        int checkpointEvery = int.Parse(options.GetValueOrDefault("checkpoint-every", "1000"), CultureInfo.InvariantCulture);
        double rate = double.Parse(options.GetValueOrDefault("rate", "0"), CultureInfo.InvariantCulture);
        TimeSpan settle = TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("settle", set == "quick" ? "0" : "2"), CultureInfo.InvariantCulture));

        Console.WriteLine($"// {cases.Count} cases x {engines.Length} engines x {repeats} repeats, {warmup.TotalSeconds:0.#} s warmup + {measured.TotalSeconds:0.#} s measured, in {root}");
        Console.WriteLine($"// {Environment.ProcessorCount} CPUs, server GC: {System.Runtime.GCSettings.IsServerGC}, ioxide {IoxideVersion()}");

        List<WalRun> runs = [];
        for (int repeat = 0; repeat < repeats; repeat++)
        {
            for (int index = 0; index < cases.Count; index++)
            {
                for (int turn = 0; turn < engines.Length; turn++)
                {
                    string engine = engines[(turn + index + repeat) % engines.Length];
                    Settle(settle);
                    WalRun run = RunOne(cases[index], engine, repeat, root, warmup, measured, readers, checkpointEvery, rate);
                    runs.Add(run);
                    Console.WriteLine(
                        $"{run.Case,-26} {engine,-18} r{repeat}  {run.EntriesPerSecond,12:N0} entries/s  p50 {run.P50Microseconds,9:N1} µs  p99 {run.P99Microseconds,9:N1} µs  " +
                        $"p99.9 {run.P999Microseconds,9:N1} µs  max {run.MaxMicroseconds,9:N0} µs  cpu {run.CpuMicrosecondsPerEntry,7:N2} µs/entry  alloc {run.AllocatedBytesPerEntry,8:N0} B/entry" +
                        (run.ReadsPerSecond > 0 ? $"  reads {run.ReadsPerSecond:N0}/s" : "") +
                        $"  gc {run.GcPauseMilliseconds:N0} ms, {run.Gen2Collections} gen2" + run.EngineNotes);
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine(Summary(cases, engines, runs));
        if (options.TryGetValue("csv", out string? csv))
            File.WriteAllText(csv, Csv(runs));
    }

    // ── Cases ────────────────────────────────────────────────────────────────────────────────────

    private static List<WalCase> FullCases() =>
    [
        new(WalWorkload.Append, 1, true, 1024),
        new(WalWorkload.Append, 4, true, 1024),
        new(WalWorkload.Append, 16, true, 1024),
        new(WalWorkload.Append, 64, true, 1024),
        new(WalWorkload.Append, 1, false, 1024),
        new(WalWorkload.Append, 4, false, 1024),
        new(WalWorkload.Append, 16, false, 1024),
        new(WalWorkload.Append, 16, true, 128),
        new(WalWorkload.Append, 16, true, 4096),
        new(WalWorkload.Append, 4, false, 128),
        new(WalWorkload.Append, 4, false, 4096),
        new(WalWorkload.Commit, 1, true, 1024),
        new(WalWorkload.Commit, 16, true, 1024),
        new(WalWorkload.Commit, 4, false, 1024),
        new(WalWorkload.Batch, 1, true, 1024),
        new(WalWorkload.Batch, 16, true, 1024),
        new(WalWorkload.Batch, 4, false, 1024),
        new(WalWorkload.Mixed, 16, true, 1024),
        new(WalWorkload.Mixed, 4, false, 1024),
    ];

    private static List<WalCase> QuickCases() =>
    [
        new(WalWorkload.Append, 1, true, 1024),
        new(WalWorkload.Append, 16, true, 1024),
        new(WalWorkload.Append, 4, false, 1024),
        new(WalWorkload.Commit, 16, true, 1024),
        new(WalWorkload.Batch, 1, true, 1024),
        new(WalWorkload.Mixed, 16, true, 1024),
    ];

    // ── One run ──────────────────────────────────────────────────────────────────────────────────

    private static WalRun RunOne(WalCase walCase, string engine, int repeat, string root, TimeSpan warmup, TimeSpan measured, int? readers, int checkpointEvery, double rate)
    {
        string path = Path.Combine(root, $"solarium-wal-bench-{Guid.NewGuid():N}");
        IStorageEnvironment? environment = engine == "solarium-io_uring" ? new IoUringStorageEnvironment() : null;
        try
        {
            using IWAL wal = Open(engine, path, walCase.Sync, environment);
            return Measure(wal, walCase, engine, repeat, warmup, measured, readers, checkpointEvery, rate);
        }
        finally
        {
            (environment as IDisposable)?.Dispose();
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
    }

    // An unsynced run leaves dirty pages that the kernel writes back later, and a run that starts
    // while that writeback is still due is throttled by it: it measures the run before. A sync and
    // a pause drain it.
    private static void Settle(TimeSpan settle)
    {
        if (settle <= TimeSpan.Zero)
            return;

        using (Process sync = Process.Start(new ProcessStartInfo("sync") { UseShellExecute = false })!)
            sync.WaitForExit();

        Thread.Sleep(settle);
    }

    private static IWAL Open(string engine, string path, bool sync, IStorageEnvironment? environment) => engine switch
    {
        "rocksdb" => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: sync),
        "solarium" or "solarium-io_uring" => new SolariumWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: sync,
            new SolariumWalOptions { Environment = environment ?? DefaultStorageEnvironment.Instance }),
        _ => throw new ArgumentException($"Unknown engine '{engine}'; the engines are {string.Join(", ", AllEngines)}."),
    };

    private static WalRun Measure(IWAL wal, WalCase walCase, string engine, int repeat, TimeSpan warmup, TimeSpan measured, int? readerCount, int checkpointEvery, double rate)
    {
        int readers = walCase.Workload == WalWorkload.Mixed ? readerCount ?? Math.Max(1, walCase.Threads / 4) : 0;
        long[] lastIds = new long[walCase.Threads];
        long[][] histograms = new long[walCase.Threads][];
        long[] entries = new long[walCase.Threads];
        long[] reads = new long[Math.Max(readers, 1)];
        Exception? failure = null;
        int phase = 0;   // 0 warmup, 1 measured, 2 stop

        using Barrier start = new(walCase.Threads + readers + 1);
        List<Thread> threads = [];
        for (int t = 0; t < walCase.Threads; t++)
        {
            int writer = t;
            threads.Add(new Thread(() =>
            {
                try
                {
                    histograms[writer] = Write(wal, walCase, writer, checkpointEvery, rate, lastIds, ref entries[writer], ref phase, start);
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                    Volatile.Write(ref phase, 2);
                }
            }) { Name = $"wal-writer-{writer}", IsBackground = true });
        }

        for (int r = 0; r < readers; r++)
        {
            int reader = r;
            threads.Add(new Thread(() =>
            {
                try
                {
                    Read(wal, walCase, reader, lastIds, ref reads[reader], ref phase, start);
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                    Volatile.Write(ref phase, 2);
                }
            }) { Name = $"wal-reader-{reader}", IsBackground = true });
        }

        foreach (Thread thread in threads)
            thread.Start();

        start.SignalAndWait();
        Thread.Sleep(warmup);

        using Process process = Process.GetCurrentProcess();
        process.Refresh();
        TimeSpan cpuBefore = process.TotalProcessorTime;
        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: false);
        TimeSpan pauseBefore = GC.GetTotalPauseDuration();
        int gen2Before = GC.CollectionCount(2);
        (long Waits, TimeSpan Waited, long Flushes) engineBefore = EngineCounts(wal);
        long startTicks = Stopwatch.GetTimestamp();
        Volatile.Write(ref phase, 1);

        Thread.Sleep(measured);

        Volatile.Write(ref phase, 2);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(startTicks);
        process.Refresh();
        TimeSpan cpu = process.TotalProcessorTime - cpuBefore;
        long allocated = GC.GetTotalAllocatedBytes(precise: false) - allocatedBefore;
        TimeSpan pause = GC.GetTotalPauseDuration() - pauseBefore;
        int gen2 = GC.CollectionCount(2) - gen2Before;
        (long Waits, TimeSpan Waited, long Flushes) engineAfter = EngineCounts(wal);
        string notes = wal is SolariumWAL
            ? $"  memtable waits {engineAfter.Waits - engineBefore.Waits} ({(engineAfter.Waited - engineBefore.Waited).TotalMilliseconds:N0} ms), flushes {engineAfter.Flushes - engineBefore.Flushes}"
            : "";

        foreach (Thread thread in threads)
            thread.Join();

        if (failure is not null)
            throw new InvalidOperationException($"{walCase} on {engine} failed.", failure);

        // The log must hold what the writers acknowledged.
        long expected = Volatile.Read(ref lastIds[0]);
        long max = wal.GetMaxLog(0);
        if (max != expected)
            throw new InvalidOperationException($"{walCase} on {engine}: partition 0 reports max log {max}, but the writer acknowledged {expected}.");

        long[] merged = new long[Histogram.Buckets];
        foreach (long[] histogram in histograms)
            for (int i = 0; i < merged.Length; i++)
                merged[i] += histogram[i];

        long totalEntries = entries.Sum();
        return new WalRun(
            walCase,
            engine,
            repeat,
            totalEntries / elapsed.TotalSeconds,
            Histogram.Percentile(merged, 0.50) / 1000.0,
            Histogram.Percentile(merged, 0.99) / 1000.0,
            Histogram.Percentile(merged, 0.999) / 1000.0,
            Histogram.Percentile(merged, 1.0) / 1000.0,
            totalEntries == 0 ? 0 : cpu.TotalMicroseconds / totalEntries,
            totalEntries == 0 ? 0 : (double)allocated / totalEntries,
            reads.Sum() / elapsed.TotalSeconds,
            pause.TotalMilliseconds,
            gen2,
            notes);
    }

    // Solarium's memtable-limit waits and flushes since open; zeros for RocksDB, which has no such counters here.
    private static (long Waits, TimeSpan Waited, long Flushes) EngineCounts(IWAL wal)
    {
        if (wal is not SolariumWAL solarium)
            return default;

        SolariumDb db = solarium.DatabaseForTesting;
        (long waits, TimeSpan waited) = db.MemTableWaits();
        return (waits, waited, db.FlushCount);
    }

    // One writer: its own partition, ids from 1 up. Returns its latency histogram, in nanoseconds.
    private static long[] Write(IWAL wal, WalCase walCase, int partition, int checkpointEvery, double rate, long[] lastIds, ref long entries, ref int phase, Barrier start)
    {
        // With a rate, each writer paces its operations to it, as a node whose clients send that much.
        long ticksPerOperation = rate > 0 ? (long)(Stopwatch.Frequency / rate) : 0;
        long due = Stopwatch.GetTimestamp();
        long[] histogram = new long[Histogram.Buckets];
        byte[] payload = new byte[walCase.Payload];
        new Random(partition).NextBytes(payload);
        long id = 0;
        long lastCheckpoint = 0;
        start.SignalAndWait();

        while (Volatile.Read(ref phase) != 2)
        {
            if (ticksPerOperation > 0)
            {
                due += ticksPerOperation;
                long wait = due - Stopwatch.GetTimestamp();
                if (wait > 0)
                    Thread.Sleep(TimeSpan.FromTicks(wait * TimeSpan.TicksPerSecond / Stopwatch.Frequency));
            }

            long began = Stopwatch.GetTimestamp();
            int written;
            switch (walCase.Workload)
            {
                case WalWorkload.Append:
                    id++;
                    Check(wal.Write([(partition, [Entry(id, RaftLogType.Proposed, payload)])], walCase.Sync));
                    written = 1;
                    break;

                case WalWorkload.Batch:
                    List<RaftLog> batch = new(BatchSize);
                    for (int i = 0; i < BatchSize; i++)
                        batch.Add(Entry(++id, RaftLogType.Proposed, payload));

                    Check(wal.Write([(partition, batch)], walCase.Sync));
                    written = BatchSize;
                    break;

                default:
                    id++;
                    Check(wal.Write([(partition, [Entry(id, RaftLogType.Proposed, payload)])], walCase.Sync));
                    Check(wal.Write([(partition, [Entry(id, RaftLogType.Committed, null)])], walCase.Sync));
                    written = 1;
                    if (walCase.Workload == WalWorkload.Mixed && checkpointEvery > 0 && id - lastCheckpoint >= checkpointEvery)
                    {
                        id++;
                        Check(wal.Write([(partition, [Entry(id, RaftLogType.CommittedCheckpoint, null)])], walCase.Sync));
                        if (lastCheckpoint > 0)
                            Check(wal.CompactLogsOlderThan(partition, lastCheckpoint, checkpointEvery).Status);

                        lastCheckpoint = id;
                    }

                    break;
            }

            long nanoseconds = (long)((Stopwatch.GetTimestamp() - began) * (1e9 / Stopwatch.Frequency));
            Volatile.Write(ref lastIds[partition], id);
            if (Volatile.Read(ref phase) == 1)
            {
                histogram[Histogram.Bucket(nanoseconds)]++;
                entries += written;
            }
        }

        return histogram;
    }

    // A reader of the mixed workload: the last entries of a random partition, as a follower's catch-up reads them.
    private static void Read(IWAL wal, WalCase walCase, int reader, long[] lastIds, ref long reads, ref int phase, Barrier start)
    {
        Random random = new(1000 + reader);
        start.SignalAndWait();
        while (Volatile.Read(ref phase) != 2)
        {
            int partition = random.Next(walCase.Threads);
            long last = Volatile.Read(ref lastIds[partition]);
            if (last <= ReadWindow)
            {
                Thread.Yield();
                continue;
            }

            wal.ReadLogsRange(partition, last - ReadWindow + 1, ReadWindow);
            if (Volatile.Read(ref phase) == 1)
                reads++;
        }
    }

    private static RaftLog Entry(long id, RaftLogType type, byte[]? payload) =>
        new() { Id = id, Term = 1, Type = type, LogType = "bench", LogData = payload };

    private static void Check(RaftOperationStatus status)
    {
        if (status != RaftOperationStatus.Success)
            throw new InvalidOperationException($"A WAL operation returned {status}.");
    }

    // ── Output ───────────────────────────────────────────────────────────────────────────────────

    private static string Summary(List<WalCase> cases, string[] engines, List<WalRun> runs)
    {
        StringBuilder text = new();
        text.AppendLine("Entries per second (median of repeats); the ratio is each Solarium engine against RocksDB.");
        text.AppendLine();
        text.Append("| Case |");
        foreach (string engine in engines)
            text.Append($" {engine} |");

        foreach (string engine in engines.Where(e => e != "rocksdb" && engines.Contains("rocksdb")))
            text.Append($" {engine} / rocksdb |");

        text.AppendLine();
        text.Append("| --- |");
        for (int i = 0; i < engines.Length + engines.Count(e => e != "rocksdb" && engines.Contains("rocksdb")); i++)
            text.Append(" ---: |");

        text.AppendLine();
        foreach (WalCase walCase in cases)
        {
            text.Append($"| {walCase} |");
            foreach (string engine in engines)
                text.Append($" {Median(runs, walCase, engine, r => r.EntriesPerSecond):N0} |");

            if (engines.Contains("rocksdb"))
            {
                double baseline = Median(runs, walCase, "rocksdb", r => r.EntriesPerSecond);
                foreach (string engine in engines.Where(e => e != "rocksdb"))
                    text.Append($" {Median(runs, walCase, engine, r => r.EntriesPerSecond) / baseline:0.00}x |");
            }

            text.AppendLine();
        }

        text.AppendLine();
        text.AppendLine("Latency of one operation, p50 / p99 / p99.9 in µs, and CPU µs and allocated bytes per entry (medians of repeats).");
        text.AppendLine();
        text.Append("| Case |");
        foreach (string engine in engines)
            text.Append($" {engine} |");

        text.AppendLine();
        text.Append("| --- |");
        foreach (string _ in engines)
            text.Append(" --- |");

        text.AppendLine();
        foreach (WalCase walCase in cases)
        {
            text.Append($"| {walCase} |");
            foreach (string engine in engines)
                text.Append($" {Median(runs, walCase, engine, r => r.P50Microseconds):N0} / {Median(runs, walCase, engine, r => r.P99Microseconds):N0} / {Median(runs, walCase, engine, r => r.P999Microseconds):N0}; " +
                            $"{Median(runs, walCase, engine, r => r.CpuMicrosecondsPerEntry):N1} µs, {Median(runs, walCase, engine, r => r.AllocatedBytesPerEntry):N0} B |");

            text.AppendLine();
        }

        return text.ToString();
    }

    private static double Median(List<WalRun> runs, WalCase walCase, string engine, Func<WalRun, double> value)
    {
        double[] values = [.. runs.Where(r => r.Case == walCase && r.Engine == engine).Select(value).Order()];
        if (values.Length == 0)
            return double.NaN;

        return values.Length % 2 == 1 ? values[values.Length / 2] : (values[(values.Length / 2) - 1] + values[values.Length / 2]) / 2;
    }

    private static string Csv(List<WalRun> runs)
    {
        StringBuilder text = new("workload,threads,sync,payload,engine,repeat,entries_per_second,p50_us,p99_us,p999_us,cpu_us_per_entry,alloc_bytes_per_entry,reads_per_second\n");
        foreach (WalRun run in runs)
            text.AppendLine(string.Create(CultureInfo.InvariantCulture,
                $"{run.Case.Workload},{run.Case.Threads},{run.Case.Sync},{run.Case.Payload},{run.Engine},{run.Repeat},{run.EntriesPerSecond:0.0},{run.P50Microseconds:0.0},{run.P99Microseconds:0.0},{run.P999Microseconds:0.0},{run.CpuMicrosecondsPerEntry:0.00},{run.AllocatedBytesPerEntry:0},{run.ReadsPerSecond:0.0}"));

        return text.ToString();
    }

    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        Dictionary<string, string> options = new(StringComparer.Ordinal);
        for (int i = 0; i + 1 < args.Length; i += 2)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Expected an option at '{args[i]}'; options are --cases quick|full, --only <text>, --engines a,b, --repeats n, --warmup s, --seconds s, --dir path, --settle s, --readers n, --checkpoint-every n (0: none), --rate ops/s per writer, --csv file.");

            options[args[i][2..]] = args[i + 1];
        }

        return options;
    }

    private static string IoxideVersion() =>
        typeof(IoUringStorageEnvironment).Assembly.GetReferencedAssemblies().FirstOrDefault(a => a.Name == "ioxide")?.Version?.ToString() ?? "?";

    /// <summary>A latency histogram in nanoseconds: exact below 128, then 64 buckets per power of two.</summary>
    private static class Histogram
    {
        /// <summary>Buckets up to about 2^40 ns (18 minutes).</summary>
        public const int Buckets = 64 * 36;

        /// <summary>The bucket of <paramref name="nanoseconds"/>.</summary>
        public static int Bucket(long nanoseconds)
        {
            if (nanoseconds < 128)
                return (int)Math.Max(nanoseconds, 0);

            int shift = 63 - BitOperations.LeadingZeroCount((ulong)nanoseconds) - 6;
            return Math.Min(((shift + 1) * 64) + (int)((nanoseconds >> shift) - 64), Buckets - 1);
        }

        /// <summary>The lower bound, in nanoseconds, of the bucket that holds the <paramref name="quantile"/> of <paramref name="histogram"/>.</summary>
        public static long Percentile(long[] histogram, double quantile)
        {
            long total = histogram.Sum();
            if (total == 0)
                return 0;

            long rank = (long)Math.Ceiling(total * quantile);
            long seen = 0;
            for (int i = 0; i < histogram.Length; i++)
            {
                seen += histogram[i];
                if (seen >= rank)
                    return i < 128 ? i : (long)((i % 64) + 64) << ((i / 64) - 1);
            }

            return 0;
        }
    }
}
