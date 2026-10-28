using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Unicode;
using BenchmarkDotNet.Attributes;

namespace Solarium.Benchmarks;

/// <summary>
/// Puts from several threads at once, so group commit forms groups. Each thread writes its own
/// partition in Kommander's key layout, with log ids that only grow, as a Raft log does. The time
/// reported is per Put: the whole invocation divided by the number of Puts in it, across all threads.
/// </summary>
/// <remarks>
/// <para><see cref="Store"/> names the engine: Solarium on each storage environment, and RocksDB with
/// its defaults and with changed write path options (see <see cref="RocksDbPutStore"/>). Both engines
/// get the same workload, the same write buffer size, and the same fixed job.</para>
/// <para>Every iteration opens a new, empty database and closes it after. The cost of a Put depends
/// on what the database and the page cache already hold, so a database that lives for the whole run
/// makes later iterations slower than early ones. Each subclass also fixes the number of invocations,
/// so every case writes the same number of Puts.</para>
/// <para>At the end of each case, the benchmark process prints the write groups it formed on lines
/// that start with <c>// Write groups</c>: by their number of writers for Solarium, and in total for
/// RocksDB. For Solarium, they also give the writers that waited for a leader, those of them that
/// waited for a WAL flush without a spin, the times they blocked, the offers of leadership that a
/// running writer overtook, and the spin; and the turns that probed the long spin, those of the
/// probes that blocked, the turns that spun short, and the credit at the end. The counts
/// cover every iteration of the case, warmup included, because each iteration does the same work.
/// Another line gives the waits that spun by the time until the writer's turn came. With
/// <c>SOLARIUM_MEASURE_GROUP_STAGES=1</c>, a line gives the time of each stage of a group. The process CPU
/// time during the writes, over the same iterations, is on a line that starts with
/// <c>// CPU time</c>. On Linux, a line that starts with <c>// Threads</c> gives the time that the
/// writers, the readers and the io_uring reactor spent on a CPU and waiting for one, over the whole
/// case.</para>
/// <para>A subclass can add reader threads that call <c>Get</c> on keys already written while the
/// writers run, and stop before the invocation ends. Their reads per second are printed on a line
/// that starts with <c>// Reads</c>.</para>
/// <para>The benchmark process runs the server GC, as the hosts that embed Solarium do: Kahuna's and
/// Kommander's servers. Under BenchmarkDotNet's default, the workstation GC, collections of memtable
/// entries took 20% to 29% of each Put, against 7% to 9% under the server GC, so a change measured
/// under one GC can lose under the other.</para>
/// </remarks>
[MemoryDiagnoser]
[GcServer(true)]
public abstract class PutBenchmarkBase
{
    private const int ValueSize = 128;
    private const int ReadCounterStride = 8;
    private const string ThreadCountsVariable = "SOLARIUM_BENCH_THREADS";

    // Thread names as Linux keeps them, cut to 15 bytes: "solarium-io_uring" becomes "solarium-io_uri".
    private static readonly (string Label, string NamePrefix)[] ScheduleGroups =
    [
        ("writers", "put-"),
        ("readers", "get-"),
        ("io_uring reactor", "solarium-io_uri"),
    ];

    private PutStore _store = null!;
    private string _directory = null!;
    private Thread[] _workers = null!;
    private Barrier _barrier = null!;
    private Thread[] _readers = null!;
    private Barrier _readerBarrier = null!;
    private long[] _reads = null!;
    private volatile bool _reading;
    private long _readTicks;
    private TimeSpan _userTime;
    private TimeSpan _systemTime;
    private ThreadSchedule? _scheduleAtSetup;
    private long[] _nextLogId = null!;
    private volatile bool _stop;
    private int _putsPerThread;
    private long _puts;
    private long _minGroupsPerInvocation = long.MaxValue;
    private long _maxGroupsPerInvocation;
    private int _iterations;
    private int _tables;
    private bool _synced;
    private Exception? _failure;

    [ParamsSource(nameof(StoreNames))]
    public string Store { get; set; } = "solarium-default";

    // A [ParamsSource], so the case names list it after Store: BenchmarkDotNet puts [Params] first.
    [ParamsSource(nameof(ThreadCounts))]
    public int Threads { get; set; }

    public static IEnumerable<string> StoreNames() => PutStore.Names();

    /// <summary>
    /// 1, 2, 4 and 16 writer threads; 2 give exactly one hand-off between writers per group. The
    /// environment variable <c>SOLARIUM_BENCH_THREADS</c>, a comma-separated list, replaces them, so
    /// a pinned run can add counts without more cases in every run.
    /// </summary>
    public static IEnumerable<int> ThreadCounts()
    {
        string? counts = System.Environment.GetEnvironmentVariable(ThreadCountsVariable);
        if (string.IsNullOrWhiteSpace(counts))
            return [1, 2, 4, 16];

        return [.. counts.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(count => int.Parse(count, CultureInfo.InvariantCulture))];
    }

    /// <summary>The label of the case in the <c>// Write groups</c> lines.</summary>
    protected abstract string Kind { get; }

    /// <summary>The memtable size, or null for the engine's default.</summary>
    protected virtual long? WriteBufferSize => null;

    /// <summary>The reader threads that run beside the writers.</summary>
    protected virtual int ReaderCount => 0;

    [GlobalSetup]
    public void Setup()
    {
        _store = PutStore.Create(Store, WriteBufferSize);
        _nextLogId = new long[Threads];
        _barrier = new Barrier(Threads + 1);
        _workers = new Thread[Threads];
        for (int i = 0; i < Threads; i++)
        {
            int partition = i;
            _workers[i] = new Thread(() => Work(partition)) { Name = $"put-{partition}", IsBackground = true };
            _workers[i].Start();
        }

        _readerBarrier = new Barrier(ReaderCount + 1);
        _readers = new Thread[ReaderCount];
        // One cache line per counter, so the readers do not share one.
        _reads = new long[ReaderCount * ReadCounterStride];
        for (int i = 0; i < ReaderCount; i++)
        {
            int reader = i;
            _readers[i] = new Thread(() => Read(reader)) { Name = $"get-{reader}", IsBackground = true };
            _readers[i].Start();
        }

        _scheduleAtSetup = ThreadSchedule.Read();
    }

    [IterationSetup]
    public void OpenDatabase()
    {
        _directory = Directory.CreateTempSubdirectory("solarium-bench-").FullName;
        _store.Open(_directory);
        Array.Clear(_nextLogId);
    }

    [IterationCleanup]
    public void CloseDatabase()
    {
        // After the close, so a flush that was still running has written its table.
        _store.Close();
        _tables += Directory.GetFiles(_directory, "*.sst").Length;
        _iterations++;
        Directory.Delete(_directory, recursive: true);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        // Before the stop, so the writer and reader threads still exist.
        ThreadSchedule? schedule = ThreadSchedule.Read();

        _stop = true;
        _barrier.SignalAndWait();
        _readerBarrier.SignalAndWait();
        foreach (Thread worker in _workers)
            worker.Join();
        foreach (Thread reader in _readers)
            reader.Join();

        ReportGroups();
        ReportCpuTime();
        ReportReads();
        if (schedule is not null && _scheduleAtSetup is not null)
            Console.WriteLine($"// Threads, {Kind}, {Store}, {Threads} threads, {ReaderCount} readers: {schedule.Since(_scheduleAtSetup, ScheduleGroups)}");
        _store.CheckGroups(_puts);

        _barrier.Dispose();
        _readerBarrier.Dispose();
        _store.Dispose();
    }

    protected void Run(int puts, bool synced)
    {
        _putsPerThread = puts / Threads;
        _synced = synced;
        _puts += puts;
        long groupsBefore = _store.WriteGroupCount;

        // The first phase releases the workers, the second waits until every one has finished.
        // The readers run only between the two, so none reads a database that is opened or closed.
        System.Environment.ProcessCpuUsage cpuBefore = System.Environment.CpuUsage;
        _reading = true;
        _readerBarrier.SignalAndWait();
        long start = Stopwatch.GetTimestamp();
        _barrier.SignalAndWait();
        _barrier.SignalAndWait();
        _readTicks += Stopwatch.GetTimestamp() - start;
        _reading = false;
        _readerBarrier.SignalAndWait();
        System.Environment.ProcessCpuUsage cpuAfter = System.Environment.CpuUsage;
        _userTime += cpuAfter.UserTime - cpuBefore.UserTime;
        _systemTime += cpuAfter.PrivilegedTime - cpuBefore.PrivilegedTime;

        if (_failure is not null)
        {
            throw new InvalidOperationException("A writer failed.", _failure);
        }

        long groups = _store.WriteGroupCount - groupsBefore;
        _minGroupsPerInvocation = Math.Min(_minGroupsPerInvocation, groups);
        _maxGroupsPerInvocation = Math.Max(_maxGroupsPerInvocation, groups);
    }

    private void ReportGroups()
    {
        long groups = _store.TotalWriteGroups;
        string sizes = _store.GroupSizes ?? " not counted by this engine";

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"// Write groups, {Kind}, {Store}, {Threads} threads: {_puts} Puts in {groups} {_store.GroupName}, {(double)_puts / groups:F2} Puts per group"));
        Console.WriteLine($"// Write groups by size (writers:groups):{sizes}");
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"// Write groups per invocation: min {_minGroupsPerInvocation}, max {_maxGroupsPerInvocation}; {_tables} SSTables flushed in {_iterations} iterations"));
        if (_store.WriterWaits is (long waits, long flushWaits, long blocked, long overtaken))
        {
            (TimeSpan spin, TimeSpan shortSpin, int probeInterval) = _store.WriterSpin!.Value;
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"// Write groups, writer waits: {waits} waits, {flushWaits} of them for a WAL flush without a spin, {blocked} blocks, {overtaken} offers overtaken; per group {(double)waits / groups:F2} waits, {(double)blocked / groups:F2} blocks, {(double)overtaken / groups:F2} overtaken; spin {spin.TotalMicroseconds} us, short spin {shortSpin.TotalMicroseconds} us, a probe every {probeInterval} turns"));
        }

        if (_store.WriterSpinTurns is (long probes, long probeBlocks, long shortSpins, int credit))
        {
            // The turns that spin: every wait but those for a WAL flush, and once more after each overtaken offer.
            (long allWaits, long waitsForFlush, _, long overtakenOffers) = _store.WriterWaits!.Value;
            long turns = allWaits - waitsForFlush + overtakenOffers;
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"// Write groups, writer spin: {probes} probes, {probeBlocks} of them blocked ({100.0 * probeBlocks / Math.Max(probes, 1):F1}%); {shortSpins} short spins ({100.0 * shortSpins / Math.Max(turns, 1):F1}% of about {turns} turns); credit at the end {credit}"));
        }

        if (_store.ConcurrentInserts is (bool on, long parallelGroups, long launchedInserts, long keptInserts, long insertRetries, long blocksAfterInsert))
        {
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"// Write groups, concurrent inserts {(on ? "on" : "off")}: {parallelGroups} groups inserted in parallel ({100.0 * parallelGroups / Math.Max(groups, 1):F1}%), {launchedInserts} inserts launched, {keptInserts} kept by the leader for blocked writers, {insertRetries} links searched again, {blocksAfterInsert} blocks after an insert"));
        }

        if (_store.MemTableStages is (long memTableStages, long queueBlocks))
        {
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"// Write groups, memtable stages: {memTableStages} stages, {(double)groups / Math.Max(memTableStages, 1):F2} groups per stage, {queueBlocks} blocks of a group's leader in the memtable queue"));
        }

        if (_store.GroupStages is (TimeSpan prepare, TimeSpan encode, TimeSpan append, TimeSpan memTable, TimeSpan handOff, TimeSpan gap, TimeSpan turn))
        {
            // Per group and per Put, in microseconds; the Puts of every iteration, warmup included, as the groups.
            string Stage(string name, TimeSpan time) =>
                string.Create(CultureInfo.InvariantCulture, $"{name} {time.TotalMicroseconds / groups:F3} us ({time.TotalMicroseconds / _puts:F3} per Put)");
            Console.WriteLine($"// Write groups, stages per group: {Stage("prepare", prepare)}, {Stage("encode", encode)}, {Stage("append", append)}, {Stage("memtable", memTable)}, {Stage("hand-off", handOff)}, {Stage("gap", gap)}, {Stage("turn", turn)}{(_store.PipelinedWrite == true ? "; pipelined" : "")}");
        }

        if (_store.SpinWaitTimes is long[] times)
        {
            long total = times.Sum();
            StringBuilder buckets = new();
            for (int i = 0; i < times.Length; i++)
            {
                string label = i < SolariumDb.SpinWaitTimeLimits.Length
                    ? $"up to {SolariumDb.SpinWaitTimeLimits[i].TotalMicroseconds} us"
                    : "longer";
                buckets.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : ", ")}{label}: {times[i]} ({100.0 * times[i] / Math.Max(total, 1):F1}%)");
            }

            Console.WriteLine($"// Write groups, spin wait times until the turn came: {total} waits; {buckets}");
        }
    }

    // Only the time of the writes, so the build of the job, the start of the process, and the open
    // and close of each database do not count. The readers' time counts, as theirs is work the
    // writers' waits compete with.
    private void ReportCpuTime()
    {
        double seconds = (double)_readTicks / Stopwatch.Frequency;
        double total = (_userTime + _systemTime).TotalMicroseconds;
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"// CPU time, {Kind}, {Store}, {Threads} threads, {ReaderCount} readers: {_userTime.TotalSeconds:F3} s user, {_systemTime.TotalSeconds:F3} s system in {seconds:F2} s of writes; per Put {total / _puts:F2} us, {_systemTime.TotalMicroseconds / _puts:F2} us of it system; {total / 1e6 / seconds:F2} CPUs busy"));
    }

    private void ReportReads()
    {
        if (ReaderCount == 0)
            return;

        long reads = 0;
        for (int i = 0; i < ReaderCount; i++)
            reads += _reads[i * ReadCounterStride];

        double seconds = (double)_readTicks / Stopwatch.Frequency;
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"// Reads, {Kind}, {Store}, {Threads} threads, {ReaderCount} readers: {reads} Gets in {seconds:F2} s of writes, {reads / seconds:F0} Gets per second, {(double)reads / _puts:F3} Gets per Put"));
    }

    // Reads keys that a writer has already written in this iteration. A writer counts a log id
    // before its Put, so the newest id of a partition can still be in flight; every older one is in.
    private void Read(int reader)
    {
        Span<byte> key = stackalloc byte[31];
        Random random = new(reader);

        while (true)
        {
            _readerBarrier.SignalAndWait();
            if (_stop)
                return;

            long reads = 0;
            try
            {
                while (_reading)
                {
                    int partition = random.Next(Threads);
                    long written = Volatile.Read(ref _nextLogId[partition]) - 1;
                    if (written <= 0)
                        continue;

                    long logId = random.NextInt64(written);
                    Utf8.TryWrite(key, $"{partition:D10}:{logId:D20}", out _);
                    if (!_store.Get(key))
                        throw new InvalidOperationException($"Key {partition}:{logId} was written but not found.");
                    reads++;
                }
            }
            catch (Exception e)
            {
                _failure = e;
            }

            _reads[reader * ReadCounterStride] += reads;
            _readerBarrier.SignalAndWait();
        }
    }

    private void Work(int partition)
    {
        Span<byte> key = stackalloc byte[31];
        byte[] value = new byte[ValueSize];
        Random.Shared.NextBytes(value);

        while (true)
        {
            _barrier.SignalAndWait();
            if (_stop)
            {
                return;
            }

            try
            {
                for (int i = 0; i < _putsPerThread; i++)
                {
                    long logId = _nextLogId[partition]++;
                    Utf8.TryWrite(key, $"{partition:D10}:{logId:D20}", out _);
                    _store.Put(key, value, _synced);
                }
            }
            catch (Exception e)
            {
                _failure = e;
            }

            _barrier.SignalAndWait();
        }
    }
}

/// <summary>Synced Puts: each group of writers waits for one WAL flush.</summary>
/// <remarks>4 invocations of 192 Puts per iteration: about 3.5 s per iteration at 1 thread.</remarks>
[SimpleJob(warmupCount: 2, iterationCount: 15, invocationCount: 4)]
public class SyncedPutBenchmarks : PutBenchmarkBase
{
    // Divisible by every thread count, so each thread does the same share.
    private const int Puts = 192;

    protected override string Kind => "synced";

    [Benchmark(OperationsPerInvoke = Puts)]
    public void SyncedPut() => Run(Puts, synced: true);
}

/// <summary>Unsynced Puts, with memtable flushes and without them.</summary>
/// <remarks>
/// 96 invocations of 9,600 Puts per iteration: 921,600 Puts, about 147 MB of keys and values. With
/// the default 64 MB write buffer, the memtable switches and flushes about twice in each iteration.
/// With <see cref="NoFlush"/>, the buffer holds the whole iteration, and no flush runs.
/// </remarks>
[SimpleJob(warmupCount: 3, iterationCount: 15, invocationCount: 96)]
public class UnsyncedPutBenchmarks : PutBenchmarkBase
{
    // Divisible by every thread count, so each thread does the same share.
    private const int Puts = 9_600;
    private const long NoFlushWriteBufferSize = 1L << 30;

    [Params(false, true)]
    public bool NoFlush { get; set; }

    protected override string Kind => NoFlush ? "unsynced, no flush" : "unsynced";

    protected override long? WriteBufferSize => NoFlush ? NoFlushWriteBufferSize : null;

    [Benchmark(OperationsPerInvoke = Puts)]
    public void UnsyncedPut() => Run(Puts, synced: false);
}

/// <summary>
/// Unsynced Puts, without memtable flushes, with reader threads that call <c>Get</c> during the
/// writes. The readers use the CPU time that the writers leave, so their reads per second show what
/// the writers' waits cost other work on the host. Compare two values of
/// <c>SOLARIUM_WRITER_SPIN_US</c> or <c>SOLARIUM_WRITER_SPIN_PROBE_INTERVAL</c> by their reads per
/// second as well as by their time per Put.
/// </summary>
/// <remarks>The same job and data size as <see cref="UnsyncedPutBenchmarks"/> with <c>NoFlush</c>.</remarks>
[SimpleJob(warmupCount: 3, iterationCount: 15, invocationCount: 96)]
public class UnsyncedPutWithReadersBenchmarks : PutBenchmarkBase
{
    // Divisible by every thread count, so each thread does the same share.
    private const int Puts = 9_600;
    private const long NoFlushWriteBufferSize = 1L << 30;

    [Params(4, 16)]
    public int Readers { get; set; }

    protected override string Kind => "unsynced, no flush, with readers";

    protected override long? WriteBufferSize => NoFlushWriteBufferSize;

    protected override int ReaderCount => Readers;

    [Benchmark(OperationsPerInvoke = Puts)]
    public void UnsyncedPutWithReaders() => Run(Puts, synced: false);
}
