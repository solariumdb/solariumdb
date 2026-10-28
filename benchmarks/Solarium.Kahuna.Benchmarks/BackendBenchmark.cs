using System.Diagnostics;
using System.Globalization;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Solarium.Kahuna.Benchmarks;

/// <summary>
/// Measures Kahuna's persistence backends through <see cref="IPersistenceBackend"/>, as a Kahuna node calls
/// them: <c>RocksDbPersistenceBackend</c> and <c>SolariumPersistenceBackend</c>, opened through their providers
/// (see <see cref="BackendEngine"/>). A session loads a dataset into a new store, reopens it, waits for the
/// compactions the load left, then measures point reads, batched reads, scans, synced writes, reads under
/// writes, checkpoints (after the compactions the writes left) and, for a dataset with history, a revision
/// prune; then it deletes the store.
/// </summary>
/// <remarks>
/// <para>The engines of a dataset run one after the other, in an order that rotates with the dataset and the
/// repeat: the disk and the page cache change over a long run, and a fixed order favors whichever engine runs
/// first. The summary takes the median of the repeats.</para>
/// <para>Writes come from one thread, as from Kahuna's background writer, which hands the backend up to 1,024
/// rows per call; every call is synced, as both backends sync <c>StoreKeyValues</c>. Reads come from 1 to n
/// threads, as from the key-value actors that miss their in-memory store. Latency is that of one backend call,
/// kept in a histogram with 1.6% precision.</para>
/// </remarks>
internal static class BackendBenchmark
{
    private const int LoadBatch = 1024;
    private const int ReadBatch = 16;
    private const int ScanLimit = 100;
    private const int VerifySample = 1000;
    private const int PruneBatch = 10_000;

    // A drain ends at the first window in which the process read and wrote less than this in all.
    private const long DrainQuietBytes = 1024 * 1024;
    private static readonly TimeSpan DrainWindow = TimeSpan.FromSeconds(2);

    private static readonly Dataset[] Datasets =
    [
        new("small", 200_000, 256, 1),
        new("large", 2_000_000, 1024, 1),
        new("history", 10_000, 256, 8),
    ];

    /// <summary>Runs the sessions that <paramref name="args"/> select and prints each case, a summary, and optionally a CSV.</summary>
    public static void Run(string[] args)
    {
        Dictionary<string, string> options = ParseOptions(args);
        bool quick = options.GetValueOrDefault("cases", "full") == "quick";
        double scale = double.Parse(options.GetValueOrDefault("scale", quick ? "0.02" : "1"), CultureInfo.InvariantCulture);
        string[] datasetNames = options.GetValueOrDefault("datasets", "small,large,history").Split(',');
        List<Dataset> datasets = [.. datasetNames.Select(name => (Datasets.FirstOrDefault(d => d.Name == name)
            ?? throw new ArgumentException($"Unknown dataset '{name}'; the datasets are {string.Join(", ", Datasets.Select(d => d.Name))}.")).Scaled(scale))];

        string[] engines = options.TryGetValue("engines", out string? list) ? list.Split(',') : BackendEngine.All;
        int repeats = int.Parse(options.GetValueOrDefault("repeats", quick ? "1" : "3"), CultureInfo.InvariantCulture);
        Settings settings = new(
            Warmup: TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("warmup", quick ? "0.5" : "2"), CultureInfo.InvariantCulture)),
            Measured: TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("seconds", quick ? "1" : "8"), CultureInfo.InvariantCulture)),
            Settle: TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("settle", quick ? "0" : "2"), CultureInfo.InvariantCulture)),
            DrainLimit: TimeSpan.FromSeconds(double.Parse(options.GetValueOrDefault("drain", quick ? "10" : "900"), CultureInfo.InvariantCulture)),
            ReaderThreads: [.. options.GetValueOrDefault("threads", "1,8").Split(',').Select(t => int.Parse(t, CultureInfo.InvariantCulture))],
            MixedReaders: int.Parse(options.GetValueOrDefault("mixed-readers", "8"), CultureInfo.InvariantCulture),
            MixedRate: double.Parse(options.GetValueOrDefault("mixed-rate", "20000"), CultureInfo.InvariantCulture),
            Only: options.GetValueOrDefault("only"));

        if (!options.TryGetValue("dir", out string? root))
            throw new ArgumentException("--dir is required: a directory on the disk to measure. A tmpfs directory measures memory, not storage.");

        Directory.CreateDirectory(root);

        Console.WriteLine($"// {datasets.Count} datasets x {engines.Length} engines x {repeats} repeats, {settings.Warmup.TotalSeconds:0.#} s warmup + {settings.Measured.TotalSeconds:0.#} s measured per case, in {root}");
        Console.WriteLine($"// {Environment.ProcessorCount} CPUs, server GC: {System.Runtime.GCSettings.IsServerGC}, memory limit {MemoryLimit()}");

        List<BackendRun> runs = [];
        for (int repeat = 0; repeat < repeats; repeat++)
        {
            for (int index = 0; index < datasets.Count; index++)
            {
                for (int turn = 0; turn < engines.Length; turn++)
                {
                    string engine = engines[(turn + index + repeat) % engines.Length];
                    Settle(settings.Settle);
                    RunSession(datasets[index], engine, repeat, root, settings, run =>
                    {
                        runs.Add(run);
                        Console.WriteLine(Line(run));
                    });
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine(Summary(datasets, engines, runs));
        if (options.TryGetValue("csv", out string? csv))
            File.WriteAllText(csv, Csv(runs));
    }

    // ── One session ──────────────────────────────────────────────────────────────────────────────

    private static void RunSession(Dataset dataset, string engineName, int repeat, string root, Settings settings, Action<BackendRun> report)
    {
        using BackendEngine engine = BackendEngine.Create(engineName, Path.Combine(root, $"solarium-kahuna-bench-{Guid.NewGuid():N}"));
        engine.Open();
        Writer writer = new(dataset);

        report(Load(engine, writer, dataset, repeat));
        HLCTimestamp loaded = writer.Now;

        Usage beforeReopen = Usage.Now();
        long opened = Stopwatch.GetTimestamp();
        engine.Close();
        engine.Open();
        TimeSpan reopen = Stopwatch.GetElapsedTime(opened);
        Usage reopenUsed = Usage.Now() - beforeReopen;
        report(Once(dataset, "reopen", engine.Name, repeat, reopen, reopenUsed,
            $"  storage read {reopenUsed.StorageRead / (1024.0 * 1024):N0} MiB, written {reopenUsed.StorageWritten / (1024.0 * 1024):N0} MiB"));
        writer.Verify(engine.Backend!, VerifySample);
        report(Drain(dataset, "drain after load", engine, repeat, settings));
        Settle(settings.Settle);

        IPersistenceBackend backend = engine.Backend!;
        int keys = dataset.Keys;
        foreach (int threads in settings.ReaderThreads)
        {
            Timed(dataset, $"get t{threads}", engine.Name, repeat, settings, report,
                new Lane(threads, _ => random => Get(backend, random.Next(keys)), 0));

            Timed(dataset, $"get-miss t{threads}", engine.Name, repeat, settings, report,
                new Lane(threads, _ => random => Miss(backend, random.Next(keys)), 0));

            Timed(dataset, $"get-batch{ReadBatch} t{threads}", engine.Name, repeat, settings, report,
                new Lane(threads, _ =>
                {
                    string[] names = new string[ReadBatch];
                    return random => GetBatch(backend, names, random, keys);
                }, 0));

            Timed(dataset, $"prefix{Dataset.GroupSize} t{threads}", engine.Name, repeat, settings, report,
                new Lane(threads, _ => random => Prefix(backend, random.Next(keys)), 0));

            Timed(dataset, $"range{ScanLimit} t{threads}", engine.Name, repeat, settings, report,
                new Lane(threads, _ => random => Range(backend, random.Next(Math.Max(1, keys - ScanLimit))), 0));
        }

        foreach (int batch in (int[])[1, 64, LoadBatch])
            Timed(dataset, $"store b{batch}", engine.Name, repeat, settings, report,
                new Lane(1, _ => random => writer.Store(backend, random, batch), 0));

        int mixedBatch = 64;
        Timed(dataset, $"mixed t{settings.MixedReaders} @{settings.MixedRate:0}/s", engine.Name, repeat, settings, report,
            new Lane(settings.MixedReaders, _ => random => Get(backend, random.Next(keys)), 0),
            new Lane(1, _ => random => writer.Store(backend, random, mixedBatch), settings.MixedRate / mixedBatch));

        if (Selected(settings, "checkpoint"))
        {
            report(Drain(dataset, "drain after writes", engine, repeat, settings));
            report(Checkpoint(dataset, "checkpoint", engine, repeat, root,
                destination => backend.CreateCheckpoint(destination, 1, writer.Now)));

            if (backend.SupportsExactAsOfCheckpoint)
                report(Checkpoint(dataset, "checkpoint-asof", engine, repeat, root,
                    destination => backend.CreateCheckpointAsOf(destination, 1, loaded)));
        }

        if (dataset.Revisions > 1 && Selected(settings, "prune"))
            report(Prune(dataset, engine, repeat));

        writer.Verify(backend, VerifySample);
    }

    private static bool Selected(Settings settings, string caseName) =>
        settings.Only is null || caseName.Contains(settings.Only, StringComparison.Ordinal);

    // The dataset, written by one writer in batches of LoadBatch rows: every key once per revision, in a
    // shuffled order, as writes to independent keys arrive.
    private static BackendRun Load(BackendEngine engine, Writer writer, Dataset dataset, int repeat)
    {
        int[] order = [.. Enumerable.Range(0, dataset.Keys)];
        long[] histogram = new long[Histogram.Buckets];
        Usage before = Usage.Now();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        long started = Stopwatch.GetTimestamp();
        long rows = 0;
        for (int revision = 0; revision < dataset.Revisions; revision++)
        {
            new Random(revision).Shuffle(order);
            for (int offset = 0; offset < order.Length; offset += LoadBatch)
            {
                int count = Math.Min(LoadBatch, order.Length - offset);
                long began = Stopwatch.GetTimestamp();
                writer.StoreKeys(engine.Backend!, order.AsSpan(offset, count));
                histogram[Histogram.Bucket(Nanoseconds(began))]++;
                rows += count;
            }
        }

        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        Usage used = Usage.Now() - before;
        using Process process = Process.GetCurrentProcess();
        string notes = $"  disk {engine.BytesOnDisk() / (1024.0 * 1024):N0} MiB, managed heap {GC.GetTotalMemory(false) / (1024.0 * 1024):N0} MiB, working set {process.WorkingSet64 / (1024.0 * 1024):N0} MiB";
        return Result(dataset, $"load b{LoadBatch}", engine.Name, repeat, rows, elapsed, histogram, allocated, used, notes);
    }

    // Waits until the process has stopped reading and writing the storage device, so the flushes and
    // compactions that writes leave behind do not run under the next cases: a read case then measures
    // reads, and a checkpoint measures the checkpoint. The time it waited is how long the backlog lasted.
    private static BackendRun Drain(Dataset dataset, string caseName, BackendEngine engine, int repeat, Settings settings)
    {
        Usage before = Usage.Now();
        long started = Stopwatch.GetTimestamp();
        Usage last = before;
        while (Stopwatch.GetElapsedTime(started) < settings.DrainLimit)
        {
            Thread.Sleep(DrainWindow);
            Usage now = Usage.Now();
            Usage window = now - last;
            last = now;
            if (window.StorageRead + window.StorageWritten < DrainQuietBytes)
                break;
        }

        // The last window was quiet, so the backlog ended at most one window before it.
        TimeSpan elapsed = Stopwatch.GetElapsedTime(started) - DrainWindow;
        if (elapsed < TimeSpan.Zero)
            elapsed = TimeSpan.Zero;

        Usage used = last - before;
        return Once(dataset, caseName, engine.Name, repeat, elapsed, used,
            $"  disk {engine.BytesOnDisk() / (1024.0 * 1024):N0} MiB, storage read {used.StorageRead / (1024.0 * 1024):N0} MiB, written {used.StorageWritten / (1024.0 * 1024):N0} MiB");
    }

    private static BackendRun Checkpoint(Dataset dataset, string caseName, BackendEngine engine, int repeat, string root, Func<string, CheckpointResult> create)
    {
        string destination = Path.Combine(root, $"solarium-kahuna-checkpoint-{Guid.NewGuid():N}");
        try
        {
            Usage before = Usage.Now();
            long started = Stopwatch.GetTimestamp();
            CheckpointResult result = create(destination);
            TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
            Usage used = Usage.Now() - before;
            long bytes = 0;
            foreach (FileInfo file in new DirectoryInfo(result.CheckpointPath).EnumerateFiles("*", SearchOption.AllDirectories))
                bytes += file.Length;

            return Once(dataset, caseName, engine.Name, repeat, elapsed, used,
                $"  {bytes / (1024.0 * 1024):N0} MiB, storage read {used.StorageRead / (1024.0 * 1024):N0} MiB, written {used.StorageWritten / (1024.0 * 1024):N0} MiB");
        }
        finally
        {
            if (Directory.Exists(destination))
                Directory.Delete(destination, recursive: true);
        }
    }

    // A backend-wide sweep that keeps the newest revision of every key, in passes of PruneBatch deletes,
    // until a pass deletes nothing.
    private static BackendRun Prune(Dataset dataset, BackendEngine engine, int repeat)
    {
        Usage before = Usage.Now();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        long started = Stopwatch.GetTimestamp();
        long deleted = 0;
        int passes = 0;
        while (true)
        {
            passes++;
            if (!engine.Backend!.PruneKeyValueRevisions(null, 1, TimeSpan.Zero, PruneBatch, HLCTimestamp.Zero, out RevisionPruneResult result))
                throw new InvalidOperationException($"The prune pass {passes} on {engine.Name} failed.");

            deleted += result.RevisionsDeleted;
            if (result.RevisionsDeleted == 0 && !result.BatchLimitReached)
                break;

            if (passes > 100_000)
                throw new InvalidOperationException($"The prune on {engine.Name} did not finish in {passes} passes.");
        }

        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        Usage used = Usage.Now() - before;
        return new BackendRun(dataset, "prune", engine.Name, repeat, deleted / elapsed.TotalSeconds, double.NaN, double.NaN, double.NaN, double.NaN,
            elapsed.TotalSeconds, PerOperation(used.Cpu.TotalMicroseconds, deleted), PerOperation(allocated, deleted),
            PerOperation(used.StorageRead, deleted), PerOperation(used.StorageWritten, deleted),
            used.GcPause.TotalMilliseconds, used.Gen2, $"  {deleted:N0} revisions deleted in {passes} passes");
    }

    // ── Operations ───────────────────────────────────────────────────────────────────────────────

    private static int Get(IPersistenceBackend backend, int index)
    {
        if (backend.GetKeyValue(Dataset.Key(index)) is null)
            throw new InvalidOperationException($"The key '{Dataset.Key(index)}' was written but reads as missing.");

        return 1;
    }

    private static int Miss(IPersistenceBackend backend, int index)
    {
        if (backend.GetKeyValue(Dataset.MissingKey(index)) is not null)
            throw new InvalidOperationException($"The key '{Dataset.MissingKey(index)}' was never written but reads as present.");

        return 1;
    }

    private static int GetBatch(IPersistenceBackend backend, string[] names, Random random, int keys)
    {
        for (int i = 0; i < names.Length; i++)
            names[i] = Dataset.Key(random.Next(keys));

        ReadOnlyKeyValueEntry?[] entries = backend.GetKeyValues(names);
        for (int i = 0; i < entries.Length; i++)
            if (entries[i] is null)
                throw new InvalidOperationException($"The key '{names[i]}' was written but reads as missing in a batch.");

        return names.Length;
    }

    private static int Prefix(IPersistenceBackend backend, int index)
    {
        int count = backend.GetKeyValueByPrefix(Dataset.Prefix(index)).Count;
        if (count != Dataset.GroupSize)
            throw new InvalidOperationException($"The prefix '{Dataset.Prefix(index)}' holds {Dataset.GroupSize} keys, but the scan returned {count}.");

        return count;
    }

    private static int Range(IPersistenceBackend backend, int index)
    {
        int count = backend.GetKeyValueByRange("bench/", Dataset.Key(index), ScanLimit).Count;
        if (count == 0)
            throw new InvalidOperationException($"The range from '{Dataset.Key(index)}' returned no key.");

        return count;
    }

    // ── Measurement ──────────────────────────────────────────────────────────────────────────────

    // A case of one or more lanes of threads running at once. The first lane is the case's result; the
    // others (a mixed case's writer) are reported in its notes.
    private static void Timed(Dataset dataset, string caseName, string engine, int repeat, Settings settings, Action<BackendRun> report, params Lane[] lanes)
    {
        if (!Selected(settings, caseName))
            return;

        Exception? failure = null;
        int phase = 0;   // 0 warmup, 1 measured, 2 stop
        int threadCount = lanes.Sum(l => l.Threads);
        long[][] histograms = new long[threadCount][];
        long[] operations = new long[threadCount];
        long[] allocations = new long[threadCount];
        int[] laneOf = new int[threadCount];
        using Barrier start = new(threadCount + 1);
        List<Thread> threads = new(threadCount);
        int slot = 0;
        for (int l = 0; l < lanes.Length; l++)
        {
            for (int t = 0; t < lanes[l].Threads; t++)
            {
                int thread = slot++;
                Lane lane = lanes[l];
                laneOf[thread] = l;
                histograms[thread] = new long[Histogram.Buckets];
                Operation operation = lane.Create(thread);
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        Loop(operation, lane.RatePerThread, new Random(1000 * repeat + thread), histograms[thread], ref operations[thread], ref allocations[thread], ref phase, start);
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                        Volatile.Write(ref phase, 2);
                    }
                }) { Name = $"bench-{caseName}-{thread}", IsBackground = true });
            }
        }

        foreach (Thread thread in threads)
            thread.Start();

        start.SignalAndWait();
        Thread.Sleep(settings.Warmup);
        Usage before = Usage.Now();
        long started = Stopwatch.GetTimestamp();
        Volatile.Write(ref phase, 1);
        Thread.Sleep(settings.Measured);
        Volatile.Write(ref phase, 2);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
        Usage used = Usage.Now() - before;
        foreach (Thread thread in threads)
            thread.Join();

        if (failure is not null)
            throw new InvalidOperationException($"{dataset} {caseName} on {engine} failed.", failure);

        long LaneOperations(int lane) => Enumerable.Range(0, threadCount).Where(t => laneOf[t] == lane).Sum(t => operations[t]);
        long LaneAllocations(int lane) => Enumerable.Range(0, threadCount).Where(t => laneOf[t] == lane).Sum(t => allocations[t]);
        long[] LaneHistogram(int lane) => Histogram.Merge(Enumerable.Range(0, threadCount).Where(t => laneOf[t] == lane).Select(t => histograms[t]));

        StringBuilder notes = new();
        for (int l = 1; l < lanes.Length; l++)
        {
            long[] histogram = LaneHistogram(l);
            notes.Append($"  writer {LaneOperations(l) / elapsed.TotalSeconds:N0} rows/s, p99 {Histogram.Percentile(histogram, 0.99) / 1000.0:N0} µs, max {Histogram.Percentile(histogram, 1.0) / 1000.0:N0} µs");
        }

        // CPU and storage I/O are the whole process's, so a mixed case charges the writer's to the reads; the
        // allocations are the reading threads' own.
        report(Result(dataset, caseName, engine, repeat, LaneOperations(0), elapsed, LaneHistogram(0), LaneAllocations(0), used, notes.ToString()));
    }

    // Runs the operation until the phase is 2, and counts the calls, their latency and the bytes this thread
    // allocated during phase 1 only. The allocations are the calling thread's: the process-wide count is not
    // exact under the server GC, and the backends' background threads show in the GC pause instead.
    private static void Loop(Operation operation, double rate, Random random, long[] histogram, ref long operations, ref long allocated, ref int phase, Barrier start)
    {
        long allocatedBase = -1;
        // With a rate, the thread paces its calls to it, as a node whose clients send that much.
        long ticksPerCall = rate > 0 ? (long)(Stopwatch.Frequency / rate) : 0;
        start.SignalAndWait();
        long due = Stopwatch.GetTimestamp();
        while (Volatile.Read(ref phase) != 2)
        {
            if (ticksPerCall > 0)
            {
                due += ticksPerCall;
                long wait = due - Stopwatch.GetTimestamp();
                if (wait > 0)
                    Thread.Sleep(TimeSpan.FromTicks(wait * TimeSpan.TicksPerSecond / Stopwatch.Frequency));
            }

            long began = Stopwatch.GetTimestamp();
            int done = operation(random);
            long nanoseconds = Nanoseconds(began);
            if (Volatile.Read(ref phase) == 1)
            {
                if (allocatedBase < 0)
                    allocatedBase = GC.GetAllocatedBytesForCurrentThread();

                histogram[Histogram.Bucket(nanoseconds)]++;
                operations += done;
            }
        }

        if (allocatedBase >= 0)
            allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBase;
    }

    private static long Nanoseconds(long began) => (long)((Stopwatch.GetTimestamp() - began) * (1e9 / Stopwatch.Frequency));

    private static double PerOperation(double total, long operations) => operations == 0 ? 0 : total / operations;

    private static BackendRun Result(Dataset dataset, string caseName, string engine, int repeat, long operations, TimeSpan elapsed, long[] histogram, long allocated, Usage used, string notes) =>
        new(dataset, caseName, engine, repeat,
            operations / elapsed.TotalSeconds,
            Histogram.Percentile(histogram, 0.50) / 1000.0,
            Histogram.Percentile(histogram, 0.99) / 1000.0,
            Histogram.Percentile(histogram, 0.999) / 1000.0,
            Histogram.Percentile(histogram, 1.0) / 1000.0,
            elapsed.TotalSeconds,
            PerOperation(used.Cpu.TotalMicroseconds, operations),
            PerOperation(allocated, operations),
            PerOperation(used.StorageRead, operations),
            PerOperation(used.StorageWritten, operations),
            used.GcPause.TotalMilliseconds,
            used.Gen2,
            notes);

    private static BackendRun Once(Dataset dataset, string caseName, string engine, int repeat, TimeSpan elapsed, Usage used, string notes) =>
        new(dataset, caseName, engine, repeat, double.NaN, double.NaN, double.NaN, double.NaN, double.NaN, elapsed.TotalSeconds, double.NaN, double.NaN,
            double.NaN, double.NaN, used.GcPause.TotalMilliseconds, used.Gen2, notes);

    // An unsynced page cache or a compaction left by the run before slows the next one: a sync and a pause
    // drain the writeback.
    private static void Settle(TimeSpan settle)
    {
        if (settle <= TimeSpan.Zero)
            return;

        using (Process sync = Process.Start(new ProcessStartInfo("sync") { UseShellExecute = false })!)
            sync.WaitForExit();

        Thread.Sleep(settle);
    }

    // The memory.max of this process's cgroup, which bounds the page cache it can fill, or "none".
    private static string MemoryLimit()
    {
        try
        {
            string cgroup = File.ReadAllText("/proc/self/cgroup").Trim().Split("::").Last();
            string limit = File.ReadAllText(Path.Combine("/sys/fs/cgroup" + cgroup, "memory.max")).Trim();
            return limit == "max" ? "none" : $"{long.Parse(limit, CultureInfo.InvariantCulture) / (1024 * 1024):N0} MiB";
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or FormatException)
        {
            return "unknown";
        }
    }

    // ── Output ───────────────────────────────────────────────────────────────────────────────────

    private static string Line(BackendRun run)
    {
        if (double.IsNaN(run.P50Microseconds))
            return $"{run.Dataset,-24} {run.Case,-20} {run.Engine,-18} r{run.Repeat}  " +
                   (double.IsNaN(run.OperationsPerSecond) ? "" : $"{run.OperationsPerSecond,12:N0} ops/s  ") +
                   $"{run.Seconds,8:N2} s  gc {run.GcPauseMilliseconds:N0} ms" + run.Notes;

        return $"{run.Dataset,-24} {run.Case,-20} {run.Engine,-18} r{run.Repeat}  {run.OperationsPerSecond,12:N0} ops/s  p50 {run.P50Microseconds,9:N1} µs  p99 {run.P99Microseconds,9:N1} µs  " +
               $"p99.9 {run.P999Microseconds,9:N1} µs  max {run.MaxMicroseconds,9:N0} µs  cpu {run.CpuMicrosecondsPerOperation,7:N2} µs/op  alloc {run.AllocatedBytesPerOperation,8:N0} B/op  " +
               $"storage r {run.StorageReadBytesPerOperation,8:N0} w {run.StorageWrittenBytesPerOperation,8:N0} B/op  " +
               $"gc {run.GcPauseMilliseconds:N0} ms, {run.Gen2Collections} gen2" + run.Notes;
    }

    private static string Summary(List<Dataset> datasets, string[] engines, List<BackendRun> runs)
    {
        StringBuilder text = new();
        bool baseline = engines.Contains("rocksdb");
        string[] others = [.. engines.Where(e => e != "rocksdb")];
        foreach (Dataset dataset in datasets)
        {
            List<string> cases = [.. runs.Where(r => r.Dataset == dataset).Select(r => r.Case).Distinct()];
            text.AppendLine($"{dataset}: operations per second, or seconds for the cases that run once (median of repeats); the ratios are against rocksdb, higher is better.");
            text.AppendLine();
            text.Append("| Case |");
            foreach (string engine in engines)
                text.Append($" {engine} |");

            if (baseline)
                foreach (string engine in others)
                    text.Append($" {engine} / rocksdb |");

            text.AppendLine();
            text.Append("| --- |");
            for (int i = 0; i < engines.Length + (baseline ? others.Length : 0); i++)
                text.Append(" ---: |");

            text.AppendLine();
            foreach (string caseName in cases)
            {
                bool once = double.IsNaN(Median(runs, dataset, caseName, engines[0], r => r.P50Microseconds)) && caseName != "prune";
                Func<BackendRun, double> value = once ? r => r.Seconds : r => r.OperationsPerSecond;
                text.Append($"| {caseName} |");
                foreach (string engine in engines)
                    text.Append(once ? $" {Median(runs, dataset, caseName, engine, value):N2} s |" : $" {Median(runs, dataset, caseName, engine, value):N0} |");

                if (baseline)
                {
                    double reference = Median(runs, dataset, caseName, "rocksdb", value);
                    foreach (string engine in others)
                    {
                        double measured = Median(runs, dataset, caseName, engine, value);
                        double ratio = measured / reference;

                        // A drain shorter than its window reads zero, and a ratio of zero says nothing.
                        if (once && (measured < 0.01 || reference < 0.01))
                            text.Append(" - |");
                        else
                            text.Append($" {(once ? 1 / ratio : ratio):0.00}x |");
                    }
                }

                text.AppendLine();
            }

            text.AppendLine();
            text.AppendLine($"{dataset}: latency of one call, p50 / p99 / p99.9 in µs; CPU µs and allocated bytes per operation (medians of repeats).");
            text.AppendLine();
            text.Append("| Case |");
            foreach (string engine in engines)
                text.Append($" {engine} |");

            text.AppendLine();
            text.Append("| --- |");
            foreach (string _ in engines)
                text.Append(" --- |");

            text.AppendLine();
            foreach (string caseName in cases.Where(c => !double.IsNaN(Median(runs, dataset, c, engines[0], r => r.P50Microseconds))))
            {
                text.Append($"| {caseName} |");
                foreach (string engine in engines)
                    text.Append($" {Median(runs, dataset, caseName, engine, r => r.P50Microseconds):N1} / {Median(runs, dataset, caseName, engine, r => r.P99Microseconds):N1} / {Median(runs, dataset, caseName, engine, r => r.P999Microseconds):N1}; " +
                                $"{Median(runs, dataset, caseName, engine, r => r.CpuMicrosecondsPerOperation):N1} µs, {Median(runs, dataset, caseName, engine, r => r.AllocatedBytesPerOperation):N0} B |");

                text.AppendLine();
            }

            text.AppendLine();
            text.AppendLine($"{dataset}: bytes read from / written to the storage device per operation, by the whole process (medians of repeats).");
            text.AppendLine();
            text.Append("| Case |");
            foreach (string engine in engines)
                text.Append($" {engine} |");

            text.AppendLine();
            text.Append("| --- |");
            foreach (string _ in engines)
                text.Append(" ---: |");

            text.AppendLine();
            foreach (string caseName in cases.Where(c => !double.IsNaN(Median(runs, dataset, c, engines[0], r => r.StorageWrittenBytesPerOperation))))
            {
                text.Append($"| {caseName} |");
                foreach (string engine in engines)
                    text.Append($" {Median(runs, dataset, caseName, engine, r => r.StorageReadBytesPerOperation):N0} / {Median(runs, dataset, caseName, engine, r => r.StorageWrittenBytesPerOperation):N0} |");

                text.AppendLine();
            }

            text.AppendLine();
        }

        return text.ToString();
    }

    private static double Median(List<BackendRun> runs, Dataset dataset, string caseName, string engine, Func<BackendRun, double> value)
    {
        double[] values = [.. runs.Where(r => r.Dataset == dataset && r.Case == caseName && r.Engine == engine).Select(value).Order()];
        if (values.Length == 0)
            return double.NaN;

        return values.Length % 2 == 1 ? values[values.Length / 2] : (values[(values.Length / 2) - 1] + values[values.Length / 2]) / 2;
    }

    private static string Csv(List<BackendRun> runs)
    {
        StringBuilder text = new("dataset,keys,value_size,revisions,case,engine,repeat,ops_per_second,p50_us,p99_us,p999_us,max_us,seconds,cpu_us_per_op,alloc_bytes_per_op,storage_read_bytes_per_op,storage_written_bytes_per_op,gc_pause_ms,gen2,notes\n");
        foreach (BackendRun run in runs)
            text.AppendLine(string.Create(CultureInfo.InvariantCulture,
                $"{run.Dataset.Name},{run.Dataset.Keys},{run.Dataset.ValueSize},{run.Dataset.Revisions},{run.Case},{run.Engine},{run.Repeat},{run.OperationsPerSecond:0.0},{run.P50Microseconds:0.0},{run.P99Microseconds:0.0},{run.P999Microseconds:0.0},{run.MaxMicroseconds:0},{run.Seconds:0.000},{run.CpuMicrosecondsPerOperation:0.00},{run.AllocatedBytesPerOperation:0},{run.StorageReadBytesPerOperation:0},{run.StorageWrittenBytesPerOperation:0},{run.GcPauseMilliseconds:0},{run.Gen2Collections},\"{run.Notes.Trim()}\""));

        return text.ToString();
    }

    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        Dictionary<string, string> options = new(StringComparer.Ordinal);
        for (int i = 0; i + 1 < args.Length; i += 2)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Expected an option at '{args[i]}'; options are --dir path (required), --cases quick|full, --datasets small,large,history, --scale x, --engines a,b, " +
                                            "--repeats n, --warmup s, --seconds s, --settle s, --drain s, --threads 1,8, --mixed-readers n, --mixed-rate rows/s, --only <text>, --csv file.");

            options[args[i][2..]] = args[i + 1];
        }

        return options;
    }

    // ── Types ────────────────────────────────────────────────────────────────────────────────────

    private delegate int Operation(Random random);

    // Threads that run the operation each Create returns, one per thread, at a rate in calls per second per
    // thread (0: as fast as they can).
    private sealed record Lane(int Threads, Func<int, Operation> Create, double RatePerThread);

    private sealed record Settings(TimeSpan Warmup, TimeSpan Measured, TimeSpan Settle, TimeSpan DrainLimit, int[] ReaderThreads, int MixedReaders, double MixedRate, string? Only);

    // What the whole process spent, its native and background threads included: CPU, GC pauses, gen 2
    // collections, and the bytes it read from and wrote to the storage device (Linux's /proc/self/io, which
    // counts a write when the page is dirtied, and a read only when the page cache missed).
    private readonly record struct Usage(TimeSpan Cpu, TimeSpan GcPause, int Gen2, long StorageRead, long StorageWritten)
    {
        public static Usage Now()
        {
            using Process process = Process.GetCurrentProcess();
            (long read, long written) = StorageBytes();
            return new(process.TotalProcessorTime, GC.GetTotalPauseDuration(), GC.CollectionCount(2), read, written);
        }

        public static Usage operator -(Usage after, Usage before) =>
            new(after.Cpu - before.Cpu, after.GcPause - before.GcPause, after.Gen2 - before.Gen2,
                after.StorageRead - before.StorageRead, after.StorageWritten - before.StorageWritten);

        private static (long Read, long Written) StorageBytes()
        {
            long read = 0;
            long written = 0;
            try
            {
                foreach (string line in File.ReadLines("/proc/self/io"))
                {
                    if (line.StartsWith("read_bytes: ", StringComparison.Ordinal))
                        read = long.Parse(line.AsSpan(12), CultureInfo.InvariantCulture);
                    else if (line.StartsWith("write_bytes: ", StringComparison.Ordinal))
                        written = long.Parse(line.AsSpan(13), CultureInfo.InvariantCulture);
                }
            }
            catch (IOException)
            {
                // Not Linux: the storage columns read zero.
            }

            return (read, written);
        }
    }

    // The session's only writer: it assigns each row its next revision and an HLC time that only grows, and
    // remembers the revisions it wrote so a read can be checked against them. One thread at a time writes.
    private sealed class Writer(Dataset dataset)
    {
        private readonly long[] _revisions = new long[dataset.Keys];
        private readonly byte[] _pool = RandomPool();
        private readonly Random _random = new(42);
        private readonly long _epoch = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        private readonly long _started = Stopwatch.GetTimestamp();
        private long _lastPhysical;
        private uint _counter;

        public HLCTimestamp Now => new(1, _lastPhysical, _counter);

        public void StoreKeys(IPersistenceBackend backend, ReadOnlySpan<int> indexes)
        {
            List<PersistenceRequestItem> items = new(indexes.Length);
            foreach (int index in indexes)
                items.Add(Item(index));

            if (!backend.StoreKeyValues(items))
                throw new InvalidOperationException($"StoreKeyValues of {items.Count} rows failed.");
        }

        public int Store(IPersistenceBackend backend, Random random, int count)
        {
            Span<int> indexes = count <= 1024 ? stackalloc int[count] : new int[count];
            for (int i = 0; i < count; i++)
                indexes[i] = random.Next(dataset.Keys);

            StoreKeys(backend, indexes);
            return count;
        }

        // Reads a sample of keys and checks each holds the newest revision written to it.
        public void Verify(IPersistenceBackend backend, int sample)
        {
            Random random = new(7);
            for (int i = 0; i < sample; i++)
            {
                int index = random.Next(dataset.Keys);
                ReadOnlyKeyValueEntry? entry = backend.GetKeyValue(Dataset.Key(index));
                long expected = _revisions[index] - 1;
                if (entry is null || entry.Revision != expected || entry.Value?.Length != dataset.ValueSize)
                    throw new InvalidOperationException(
                        $"The key '{Dataset.Key(index)}' should hold revision {expected} with {dataset.ValueSize} bytes, but reads as " +
                        (entry is null ? "missing." : $"revision {entry.Revision} with {entry.Value?.Length ?? 0} bytes."));
            }
        }

        private PersistenceRequestItem Item(int index)
        {
            long physical = _epoch + (long)Stopwatch.GetElapsedTime(_started).TotalMilliseconds;
            if (physical > _lastPhysical)
            {
                _lastPhysical = physical;
                _counter = 0;
            }
            else
                _counter++;

            long revision = _revisions[index]++;
            return new PersistenceRequestItem(Dataset.Key(index), Value(), revision, 0, 0, 0,
                1, _lastPhysical, _counter, 1, _lastPhysical, _counter, (int)KeyValueState.Set);
        }

        // Each row its own bytes, a slice of a random pool at a random offset: one value repeated in every row
        // would let RocksDB's block compression shrink what real rows would not.
        private byte[] Value() => _pool.AsSpan(_random.Next(_pool.Length - dataset.ValueSize), dataset.ValueSize).ToArray();

        private static byte[] RandomPool()
        {
            byte[] pool = new byte[16 << 20];
            new Random(42).NextBytes(pool);
            return pool;
        }
    }
}
