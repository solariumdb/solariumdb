using System.Diagnostics;
using System.Globalization;

namespace Solarium.Benchmarks;

/// <summary>
/// Compaction scenarios, run to the end rather than sampled, because what they measure is the work
/// of the whole run: <c>dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- compaction
/// own|shared|random solarium|rocksdb [name=value ...]</c> (the names are those of
/// <see cref="CompactionSettings.With"/>).
/// </summary>
/// <remarks>
/// <para><c>own</c> is Kommander's append-then-die log with one partition per shard family: each
/// partition appends rows in batches and, every <c>pass</c> rows, raises its floor to keep
/// <c>window</c> rows, writes a tombstone confined to its active memtable, and drops the tables below
/// the floor. Its files do not overlap, so they should move to the sorted level and be dropped whole,
/// with no merge. <c>shared</c> puts several partitions in each family, as Kommander does past eight
/// partitions: the tombstones cover the whole dead range, and every compaction merges. <c>random</c>
/// writes uniformly random keys to one family: the worst case for one sorted level, where every
/// merge rewrites the level.</para>
/// <para>The report gives the bytes the writes carried (keys and values), the bytes flushes and
/// compactions wrote and their ratio to it (the write amplification, without the WAL), the level 0
/// files that moved without a rewrite, the most level 0 files a family held and the time writes
/// stalled, and the live SSTable bytes against the rows the floors keep. The counts include the
/// compactions that ran after the writes until the engine settled.</para>
/// </remarks>
internal static class CompactionLoad
{
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan SampleInterval = TimeSpan.FromMilliseconds(20);

    /// <summary>Runs the scenario and the store that <paramref name="args"/> name, and prints its report.</summary>
    public static void Run(string[] args)
    {
        if (args.Length < 2 || args[0] is not ("own" or "shared" or "random"))
            throw new ArgumentException("Usage: compaction own|shared|random solarium|rocksdb [name=value ...]");

        string scenario = args[0];
        CompactionSettings defaults = scenario switch
        {
            "shared" => new CompactionSettings { Partitions = 16, Families = 8, Rows = 1_000_000, Window = 250_000 },
            "random" => new CompactionSettings { Families = 1 },
            _ => new CompactionSettings(),
        };
        CompactionSettings settings = defaults.With(args.Skip(2));
        string directory = Path.Combine(Path.GetTempPath(), $"solarium-compaction-{Guid.NewGuid():N}");
        try
        {
            using CompactionStore store = CompactionStore.Create(args[1], directory, settings, settings.FamilyNames());
            Run(scenario, args[1], store, settings);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static void Run(string scenario, string storeName, CompactionStore store, CompactionSettings settings)
    {
        byte[] value = new byte[settings.Value];
        new Random(42).NextBytes(value);

        int maxLevel0 = 0;
        long samples = 0;
        long samplesAtSlowdown = 0;
        bool sampling = true;
        Thread sampler = new(() =>
        {
            while (Volatile.Read(ref sampling))
            {
                int level0 = store.MaxLevel0Files();
                maxLevel0 = Math.Max(maxLevel0, level0);
                samples++;
                if (level0 >= settings.Level0Slowdown)
                    samplesAtSlowdown++;

                Thread.Sleep(SampleInterval);
            }
        }) { Name = "level0-sampler" };
        sampler.Start();

        Stopwatch writes = Stopwatch.StartNew();
        long[] floors = scenario == "random" ? [] : Append(store, settings, value);
        long rows = scenario == "random" ? RandomPuts(store, settings, value) : settings.Partitions * settings.Rows;
        writes.Stop();
        long liveAtEnd = store.LiveSstBytes();

        Stopwatch settle = Stopwatch.StartNew();
        bool settled = store.Settle(SettleTimeout);
        for (int p = 0; p < floors.Length; p++)
            store.DropBelow(p % settings.Families, p, floors[p]);

        settle.Stop();
        Volatile.Write(ref sampling, false);
        sampler.Join();

        int keyWidth = scenario == "random" ? CompactionSettings.RandomKeyWidth : CompactionSettings.LogKeyWidth;
        long userBytes = rows * (keyWidth + settings.Value);
        // Random keys: the expected distinct keys of that many uniform draws.
        long keptBytes = scenario == "random"
            ? (long)(settings.Keys * (1 - Math.Exp(-(double)rows / settings.Keys))) * (keyWidth + settings.Value)
            : settings.Partitions * Math.Min(settings.Window, settings.Rows) * (keyWidth + settings.Value);
        (long flushBytes, long compactionRead, long compactionWritten, long? movedFiles, TimeSpan stalled) = store.Counts();
        long live = store.LiveSstBytes();

        CultureInfo c = CultureInfo.InvariantCulture;
        string label = $"// compaction {scenario} {storeName}";
        Console.WriteLine(string.Create(c, $"{label}: {settings}"));
        Console.WriteLine(string.Create(c, $"{label}: {rows:N0} rows in {writes.Elapsed.TotalSeconds:F1} s, {rows / writes.Elapsed.TotalSeconds:N0} rows/s; settled {(settled ? "in" : "NOT in")} {settle.Elapsed.TotalSeconds:F1} s"));
        Console.WriteLine(string.Create(c, $"{label}: user {Mb(userBytes)}, flushed {Mb(flushBytes)}, compaction read {Mb(compactionRead)} and wrote {Mb(compactionWritten)}, moved files {(movedFiles is long moved ? moved.ToString("N0", c) : "-")}"));
        Console.WriteLine(string.Create(c, $"{label}: write amplification {(double)(flushBytes + compactionWritten) / userBytes:F2}, compaction written over flushed {(double)compactionWritten / Math.Max(1, flushBytes):F3}"));
        Console.WriteLine(string.Create(c, $"{label}: level 0 at most {maxLevel0} files, at or past the slowdown trigger ({settings.Level0Slowdown}) in {samplesAtSlowdown} of {samples} samples, writes stalled {stalled.TotalSeconds:F1} s"));
        Console.WriteLine(string.Create(c, $"{label}: live SSTables {Mb(liveAtEnd)} at the end of the writes, {Mb(live)} settled, against {Mb(keptBytes)} kept ({(double)live / keptBytes:F2}x)"));
    }

    // One writer per partition; returns each partition's floor at the end.
    private static long[] Append(CompactionStore store, CompactionSettings settings, byte[] value)
    {
        int[] partitionsPerFamily = new int[settings.Families];
        for (int p = 0; p < settings.Partitions; p++)
            partitionsPerFamily[p % settings.Families]++;

        long[] floors = new long[settings.Partitions];
        Exception? failure = null;
        Thread[] writers = new Thread[settings.Partitions];
        for (int p = 0; p < settings.Partitions; p++)
        {
            int partition = p;
            int family = p % settings.Families;

            // Kommander confines the tombstone only when the partition owns its family.
            bool confined = partitionsPerFamily[family] == 1;
            writers[p] = new Thread(() =>
            {
                try
                {
                    floors[partition] = AppendPartition(store, settings, value, family, partition, confined);
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            }) { Name = $"append-{p}" };
            writers[p].Start();
        }

        foreach (Thread writer in writers)
            writer.Join();

        if (failure is not null)
            throw new InvalidOperationException("A writer failed.", failure);

        return floors;
    }

    private static long AppendPartition(CompactionStore store, CompactionSettings settings, byte[] value, int family, int partition, bool confined)
    {
        long floor = 0;
        long sincePass = 0;
        for (long id = 1; id <= settings.Rows; id += settings.Batch)
        {
            int count = (int)Math.Min(settings.Batch, settings.Rows - id + 1);
            store.Append(family, partition, id, count, value);
            sincePass += count;
            if (sincePass < settings.Pass)
                continue;

            sincePass = 0;
            long head = id + count - 1;
            long newFloor = head + 1 - settings.Window;
            if (newFloor > floor)
            {
                store.FloorPass(family, partition, floor, newFloor, head, confined);
                floor = newFloor;
            }
            else
            {
                store.DropBelow(family, partition, floor);
            }
        }

        return floor;
    }

    // Uniformly random keys from the key space, split over the writer threads; returns the Puts.
    private static long RandomPuts(CompactionStore store, CompactionSettings settings, byte[] value)
    {
        long perThread = settings.Puts / settings.Threads;
        Exception? failure = null;
        Thread[] writers = new Thread[settings.Threads];
        for (int t = 0; t < settings.Threads; t++)
        {
            int seed = t + 1;
            writers[t] = new Thread(() =>
            {
                try
                {
                    Random random = new(seed);
                    Span<byte> key = stackalloc byte[CompactionSettings.RandomKeyWidth];
                    for (long i = 0; i < perThread; i++)
                    {
                        random.NextInt64(settings.Keys).TryFormat(key, out _, "D16", CultureInfo.InvariantCulture);
                        store.Put(key, value);
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            }) { Name = $"put-{t}" };
            writers[t].Start();
        }

        foreach (Thread writer in writers)
            writer.Join();

        if (failure is not null)
            throw new InvalidOperationException("A writer failed.", failure);

        return perThread * settings.Threads;
    }

    private static string Mb(long bytes) => string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024):N0} MiB");
}
