using System.Globalization;
using System.Text;
using Solarium.Storage;
using RocksDbHandle = RocksDbSharp.RocksDb;
using RocksDbNative = RocksDbSharp.Native;
using RocksDbOptions = RocksDbSharp.DbOptions;
using RocksDbStatisticsLevel = RocksDbSharp.StatisticsLevel;
using RocksDbWriteOptions = RocksDbSharp.WriteOptions;

namespace Solarium.Benchmarks;

/// <summary>
/// A database that the Put benchmarks write to, by the name it takes as a parameter: Solarium on one
/// of its storage environments, or RocksDB, the reference behavior. The benchmark opens a new
/// database in an empty directory for each iteration and closes it after.
/// </summary>
internal abstract class PutStore : IDisposable
{
    private const string SolariumPrefix = "solarium-";

    public static IEnumerable<string> Names()
    {
        foreach (string environment in Environments.Names())
        {
            yield return SolariumPrefix + environment;
        }

        foreach (string name in RocksDbPutStore.Variants.Keys)
        {
            yield return name;
        }
    }

    /// <param name="name">One of <see cref="Names"/>.</param>
    /// <param name="writeBufferSize">The memtable size, or null for the default (64 MB in both engines).</param>
    public static PutStore Create(string name, long? writeBufferSize)
    {
        return name switch
        {
            _ when RocksDbPutStore.Variants.TryGetValue(name, out RocksDbPutStore.Variant variant) =>
                new RocksDbPutStore(variant, writeBufferSize),
            _ when name.StartsWith(SolariumPrefix, StringComparison.Ordinal) =>
                new SolariumPutStore(Environments.Create(name[SolariumPrefix.Length..]), writeBufferSize),
            _ => throw new ArgumentException($"Unknown store '{name}'.", nameof(name)),
        };
    }

    public abstract void Open(string directory);

    /// <summary>Safe to call from any thread while the database is open.</summary>
    public abstract void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool synced);

    /// <summary>Reads <paramref name="key"/> and returns true when it is found. Safe to call from any thread while the database is open.</summary>
    public abstract bool Get(ReadOnlySpan<byte> key);

    /// <summary>The write groups formed so far. Only the difference between two reads is meaningful.</summary>
    public abstract long WriteGroupCount { get; }

    public abstract void Close();

    /// <summary>The write groups of every database this store has opened.</summary>
    public abstract long TotalWriteGroups { get; }

    /// <summary>What a write group is for this store, in the <c>// Write groups</c> lines.</summary>
    public virtual string GroupName => "groups";

    /// <summary>
    /// Checks the group counts against the <paramref name="puts"/> that every database this store
    /// opened received, and throws if they disagree.
    /// </summary>
    public virtual void CheckGroups(long puts)
    {
    }

    /// <summary>
    /// The write groups of every database this store has opened, as <c>writers:groups</c> pairs, or
    /// null when the engine does not count groups by size.
    /// </summary>
    public virtual string? GroupSizes => null;

    /// <summary>
    /// The writers that waited for a leader, those of them that waited for a WAL flush without a
    /// spin, the times they blocked, and the offers of leadership that a running writer overtook, of
    /// every database this store has opened, or null when the engine does not count them.
    /// </summary>
    public virtual (long Waits, long FlushWaits, long Blocked, long Overtaken)? WriterWaits => null;

    /// <summary>
    /// The turns of waiting writers that probed the long spin, those of the probes that blocked, and
    /// the turns that spun short, of every database this store has opened, with the credit of the
    /// last one when it closed, or null when the engine does not adapt its spin.
    /// </summary>
    public virtual (long Probes, long ProbeBlocks, long ShortSpins, int LastCredit)? WriterSpinTurns => null;

    /// <summary>
    /// The waits that spun first, of every database this store has opened, by the time until the
    /// writer's turn came, in the buckets of <see cref="SolariumDb.SpinWaitTimeLimits"/>, or null
    /// when the engine does not count them.
    /// </summary>
    public virtual long[]? SpinWaitTimes => null;

    /// <summary>
    /// How long a waiting writer spins before it blocks while the long spin pays, how long it spins
    /// when it does not, and one turn in how many probes the long spin (0: none, a fixed spin), or
    /// null when the engine does not say.
    /// </summary>
    public virtual (TimeSpan Spin, TimeSpan ShortSpin, int ProbeInterval)? WriterSpin => null;

    /// <summary>
    /// Whether the writers of a group insert their own batches at once, the groups that did, the
    /// members whose inserts their leader launched, the members whose batches their leader inserted
    /// because they had blocked, the times a concurrent insert searched a level again, and the times
    /// a writer blocked after its insert, of every database this store has opened, or null when the
    /// engine does not say.
    /// </summary>
    public virtual (bool On, long ParallelGroups, long LaunchedInserts, long KeptInserts, long InsertRetries, long BlocksAfterInsert)? ConcurrentInserts => null;

    /// <summary>Whether the engine pipelines the commit of write groups, or null when it does not say.</summary>
    public virtual bool? PipelinedWrite => null;

    // Solarium with pipelined commits: the memtable stages, and the blocks of group leaders in the memtable queue.
    public virtual (long Stages, long QueueBlocks)? MemTableStages => null;

    /// <summary>
    /// The time the leaders spent in each stage of their groups, of every database this store has
    /// opened, or null when the engine does not time them or the timers are off.
    /// </summary>
    public virtual (TimeSpan Prepare, TimeSpan Encode, TimeSpan Append, TimeSpan MemTable, TimeSpan HandOff, TimeSpan Gap, TimeSpan Turn)? GroupStages => null;

    public abstract void Dispose();
}

/// <summary>
/// Solarium with its defaults, on the given storage environment. The environment variables
/// <c>SOLARIUM_WRITER_SPIN_US</c> and <c>SOLARIUM_WRITER_SHORT_SPIN_US</c>, in microseconds, replace
/// the defaults of a waiting writer: the long spin and the short spin. <c>SOLARIUM_WRITER_SPIN_PROBE_INTERVAL</c>
/// replaces the interval of the probes; 0 gives a fixed spin of <c>SOLARIUM_WRITER_SPIN_US</c>.
/// <c>SOLARIUM_ALLOW_CONCURRENT_MEMTABLE_WRITE=0</c> makes the leader insert every batch of its group.
/// <c>SOLARIUM_MEASURE_GROUP_STAGES=1</c> times the stages of each write group.
/// <c>SOLARIUM_ENABLE_PIPELINED_WRITE=1</c> pipelines the commit of write groups.
/// </summary>
internal sealed class SolariumPutStore(IStorageEnvironment environment, long? writeBufferSize) : PutStore
{
    private const string WriterSpinVariable = "SOLARIUM_WRITER_SPIN_US";
    private const string WriterShortSpinVariable = "SOLARIUM_WRITER_SHORT_SPIN_US";
    private const string WriterSpinProbeIntervalVariable = "SOLARIUM_WRITER_SPIN_PROBE_INTERVAL";
    private const string ConcurrentInsertsVariable = "SOLARIUM_ALLOW_CONCURRENT_MEMTABLE_WRITE";
    private const string MeasureGroupStagesVariable = "SOLARIUM_MEASURE_GROUP_STAGES";
    private const string PipelinedWriteVariable = "SOLARIUM_ENABLE_PIPELINED_WRITE";

    private readonly long[] _groupSizes = new long[SolariumDb.MaxCountedGroupSize + 1];
    private readonly DbOptions _options = Options(environment, writeBufferSize);
    private long _waits;
    private long _flushWaits;
    private long _blockedWaits;
    private long _overtakenOffers;
    private long _spinProbes;
    private long _probeBlocks;
    private long _shortSpins;
    private int _lastCredit;
    private long _parallelGroups;
    private long _launchedInserts;
    private long _keptInserts;
    private long _insertRetries;
    private long _blocksAfterInsert;
    private long _memTableStages;
    private long _memTableQueueBlocks;
    private (TimeSpan Prepare, TimeSpan Encode, TimeSpan Append, TimeSpan MemTable, TimeSpan HandOff, TimeSpan Gap, TimeSpan Turn) _stages;
    private readonly long[] _spinWaitTimes = new long[SolariumDb.SpinWaitTimeLimits.Length + 1];
    private SolariumDb _db = null!;

    public override void Open(string directory) => _db = SolariumDb.Open(directory, _options);

    public override void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool synced)
    {
        _db.Put(key, value, options: synced ? WriteOptions.Synced : WriteOptions.Default);
    }

    public override bool Get(ReadOnlySpan<byte> key) => _db.Get(key) is not null;

    public override long WriteGroupCount => _db.WriteGroupCount;

    public override void Close()
    {
        long[] sizes = _db.WriteGroupSizes();
        for (int i = 0; i < sizes.Length; i++)
        {
            _groupSizes[i] += sizes[i];
        }

        (long waits, long flushWaits, long blocked, long overtaken) = _db.WriterWaits();
        _waits += waits;
        _flushWaits += flushWaits;
        _blockedWaits += blocked;
        _overtakenOffers += overtaken;

        (long probes, long probeBlocks, long shortSpins, int credit) = _db.WriterSpinTurns();
        _spinProbes += probes;
        _probeBlocks += probeBlocks;
        _shortSpins += shortSpins;
        _lastCredit = credit;

        long[] times = _db.SpinWaitTimes();
        for (int i = 0; i < times.Length; i++)
            _spinWaitTimes[i] += times[i];

        (long parallelGroups, long launchedInserts, long keptInserts, long insertRetries, long blocksAfterInsert) = _db.ConcurrentInserts();
        _parallelGroups += parallelGroups;
        _launchedInserts += launchedInserts;
        _keptInserts += keptInserts;
        _insertRetries += insertRetries;
        _blocksAfterInsert += blocksAfterInsert;

        (long memTableStages, long memTableQueueBlocks) = _db.MemTableStages();
        _memTableStages += memTableStages;
        _memTableQueueBlocks += memTableQueueBlocks;

        (TimeSpan Prepare, TimeSpan Encode, TimeSpan Append, TimeSpan MemTable, TimeSpan HandOff, TimeSpan Gap, TimeSpan Turn) stages = _db.GroupStages();
        _stages = (_stages.Prepare + stages.Prepare, _stages.Encode + stages.Encode, _stages.Append + stages.Append,
            _stages.MemTable + stages.MemTable, _stages.HandOff + stages.HandOff, _stages.Gap + stages.Gap, _stages.Turn + stages.Turn);

        _db.Dispose();
    }

    public override long TotalWriteGroups
    {
        get
        {
            long groups = 0;
            for (int size = 1; size < _groupSizes.Length; size++)
            {
                groups += _groupSizes[size];
            }

            return groups;
        }
    }

    public override string GroupSizes
    {
        get
        {
            var histogram = new StringBuilder();
            for (int size = 1; size < _groupSizes.Length; size++)
            {
                if (_groupSizes[size] > 0)
                {
                    string label = size == SolariumDb.MaxCountedGroupSize ? $"{size}+" : $"{size}";
                    histogram.Append(CultureInfo.InvariantCulture, $" {label}:{_groupSizes[size]}");
                }
            }

            return histogram.ToString();
        }
    }

    public override (long Waits, long FlushWaits, long Blocked, long Overtaken)? WriterWaits =>
        (_waits, _flushWaits, _blockedWaits, _overtakenOffers);

    public override (long Probes, long ProbeBlocks, long ShortSpins, int LastCredit)? WriterSpinTurns =>
        (_spinProbes, _probeBlocks, _shortSpins, _lastCredit);

    public override long[] SpinWaitTimes => _spinWaitTimes;

    public override (TimeSpan Spin, TimeSpan ShortSpin, int ProbeInterval)? WriterSpin =>
        (_options.WriterSpin, _options.WriterShortSpin, _options.WriterSpinProbeInterval);

    public override (bool On, long ParallelGroups, long LaunchedInserts, long KeptInserts, long InsertRetries, long BlocksAfterInsert)? ConcurrentInserts =>
        (_options.AllowConcurrentMemtableWrite, _parallelGroups, _launchedInserts, _keptInserts, _insertRetries, _blocksAfterInsert);

    public override (TimeSpan Prepare, TimeSpan Encode, TimeSpan Append, TimeSpan MemTable, TimeSpan HandOff, TimeSpan Gap, TimeSpan Turn)? GroupStages => _options.MeasureGroupStages ? _stages : null;

    public override bool? PipelinedWrite => _options.EnablePipelinedWrite;

    public override (long Stages, long QueueBlocks)? MemTableStages =>
        _options.EnablePipelinedWrite ? (_memTableStages, _memTableQueueBlocks) : null;

    public override void CheckGroups(long puts)
    {
        // Each Put is one writer, so the writers of all groups add up to the Puts. Groups of the
        // last size counted can be larger, so the check needs none of them.
        if (_groupSizes[SolariumDb.MaxCountedGroupSize] > 0)
        {
            return;
        }

        long writers = 0;
        for (int size = 1; size < _groupSizes.Length; size++)
        {
            writers += size * _groupSizes[size];
        }

        if (writers != puts)
        {
            throw new InvalidOperationException($"The write groups hold {writers} writers, but the store received {puts} Puts.");
        }
    }

    public override void Dispose() => Environments.Dispose(environment);

    private static DbOptions Options(IStorageEnvironment environment, long? writeBufferSize)
    {
        var options = new DbOptions { Environment = environment };
        if (writeBufferSize is long size)
        {
            options = options with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = size } };
        }

        string? spin = System.Environment.GetEnvironmentVariable(WriterSpinVariable);
        if (spin is not null)
        {
            options = options with { WriterSpin = TimeSpan.FromMicroseconds(double.Parse(spin, CultureInfo.InvariantCulture)) };
        }

        string? concurrentInserts = System.Environment.GetEnvironmentVariable(ConcurrentInsertsVariable);
        if (concurrentInserts is not null)
        {
            options = options with { AllowConcurrentMemtableWrite = concurrentInserts != "0" };
        }

        string? pipelined = System.Environment.GetEnvironmentVariable(PipelinedWriteVariable);
        if (pipelined is not null)
        {
            options = options with { EnablePipelinedWrite = pipelined != "0" };
        }

        string? measureStages = System.Environment.GetEnvironmentVariable(MeasureGroupStagesVariable);
        if (measureStages is not null)
        {
            options = options with { MeasureGroupStages = measureStages != "0" };
        }

        string? shortSpin = System.Environment.GetEnvironmentVariable(WriterShortSpinVariable);
        if (shortSpin is not null)
        {
            options = options with { WriterShortSpin = TimeSpan.FromMicroseconds(double.Parse(shortSpin, CultureInfo.InvariantCulture)) };
        }

        string? probeInterval = System.Environment.GetEnvironmentVariable(WriterSpinProbeIntervalVariable);
        if (probeInterval is not null)
        {
            options = options with { WriterSpinProbeInterval = int.Parse(probeInterval, CultureInfo.InvariantCulture) };
        }

        return options;
    }
}

/// <summary>
/// RocksDB through <c>Kommander.RocksDB</c>, the binding Kommander uses. <c>rocksdb</c> has RocksDB's
/// defaults: concurrent memtable writes and adaptive yield in the write thread are on, pipelined
/// writes are off. The other variants change one or two of those options, to tell which part of
/// RocksDB's write path keeps its throughput up when threads are added.
/// </summary>
/// <remarks>
/// <para>Statistics are on at the level that keeps tickers but no histograms or timers. A group is
/// what the ticker <c>rocksdb.write.self</c> counts: one write done by a leader, which writes the WAL
/// for the others, counted by <c>rocksdb.write.other</c>. With pipelined writes, that is a WAL group;
/// memtable groups form separately. The statistics belong to the options, so they add up over every
/// database the store opens.</para>
/// <para>With adaptive yield off, a waiting writer still spins for about a microsecond before it
/// blocks; RocksDB has no option for that spin.</para>
/// </remarks>
internal sealed class RocksDbPutStore : PutStore
{
    private const string WriteDoneBySelf = "rocksdb.write.self";
    private const string WriteDoneByOther = "rocksdb.write.other";

    /// <summary>The RocksDB stores by name, with the write path options each one sets.</summary>
    public static readonly IReadOnlyDictionary<string, Variant> Variants = new Dictionary<string, Variant>(StringComparer.Ordinal)
    {
        ["rocksdb"] = new(PipelinedWrite: false, ConcurrentMemtableWrite: true, AdaptiveYield: true),
        ["rocksdb-pipelined"] = new(PipelinedWrite: true, ConcurrentMemtableWrite: true, AdaptiveYield: true),
        ["rocksdb-serial"] = new(PipelinedWrite: false, ConcurrentMemtableWrite: false, AdaptiveYield: true),
        ["rocksdb-block"] = new(PipelinedWrite: false, ConcurrentMemtableWrite: true, AdaptiveYield: false),
        ["rocksdb-serial-block"] = new(PipelinedWrite: false, ConcurrentMemtableWrite: false, AdaptiveYield: false),
    };

    private readonly Variant _variant;
    private readonly RocksDbOptions _options;
    private readonly RocksDbWriteOptions _unsynced = new();
    private readonly RocksDbWriteOptions _synced = new RocksDbWriteOptions().SetSync(true);
    private readonly uint _writeDoneBySelf;
    private readonly uint _writeDoneByOther;
    private RocksDbHandle _db = null!;

    public RocksDbPutStore(Variant variant, long? writeBufferSize)
    {
        _variant = variant;
        _options = new RocksDbOptions()
            .SetCreateIfMissing(true)
            .EnableStatistics()
            .SetAllowConcurrentMemtableWrite(variant.ConcurrentMemtableWrite)
            .SetEnableWriteThreadAdaptiveYield(variant.AdaptiveYield);
        RocksDbNative.Instance.rocksdb_options_set_statistics_level(_options.Handle, (int)RocksDbStatisticsLevel.ExceptHistogramOrTimers);
        RocksDbNative.Instance.rocksdb_options_set_enable_pipelined_write(_options.Handle, variant.PipelinedWrite ? (byte)1 : (byte)0);
        if (writeBufferSize is long size)
        {
            _options.SetWriteBufferSize((ulong)size);
        }

        _writeDoneBySelf = TickerId(WriteDoneBySelf);
        _writeDoneByOther = TickerId(WriteDoneByOther);
    }

    public override void Open(string directory) => _db = RocksDbHandle.Open(_options, directory);

    public override void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool synced)
    {
        _db.Put(key, value, null, synced ? _synced : _unsynced);
    }

    public override bool Get(ReadOnlySpan<byte> key) => _db.Get(key) is not null;

    public override long WriteGroupCount => Ticker(_writeDoneBySelf);

    public override void Close() => _db.Dispose();

    public override long TotalWriteGroups => WriteGroupCount;

    public override string GroupName => _variant.PipelinedWrite ? "WAL groups" : "groups";

    public override void CheckGroups(long puts)
    {
        // The ids came from the order of the statistics string. If that order ever stops matching
        // the ticker enum, the counts would silently be of other tickers.
        long self = Ticker(_writeDoneBySelf);
        long other = Ticker(_writeDoneByOther);
        long reportedSelf = TickerValue(WriteDoneBySelf);
        long reportedOther = TickerValue(WriteDoneByOther);
        if (self != reportedSelf || other != reportedOther)
        {
            throw new InvalidOperationException(
                $"Tickers {_writeDoneBySelf} and {_writeDoneByOther} read {self} and {other}, but the statistics string reports {WriteDoneBySelf} = {reportedSelf} and {WriteDoneByOther} = {reportedOther}.");
        }

        // Every Put is done either by a leader for itself or by a leader for another writer.
        if (self + other != puts)
        {
            throw new InvalidOperationException(
                $"{WriteDoneBySelf} ({self}) + {WriteDoneByOther} ({other}) = {self + other}, but the store received {puts} Puts.");
        }
    }

    public override void Dispose()
    {
    }

    private long Ticker(uint id) => (long)RocksDbNative.Instance.rocksdb_options_statistics_get_ticker_count(_options.Handle, id);

    // The C API reads a ticker by its enum value, and the binding has no enum. The statistics
    // string lists every ticker in enum order, as "name COUNT : value"; histograms follow, with
    // percentiles on the same line.
    private uint TickerId(string name)
    {
        string[] tickers = Tickers();
        for (int i = 0; i < tickers.Length; i++)
        {
            if (tickers[i].StartsWith(name + " COUNT : ", StringComparison.Ordinal))
            {
                return (uint)i;
            }
        }

        throw new InvalidOperationException($"The RocksDB statistics have no ticker {name}.");
    }

    private long TickerValue(string name)
    {
        string line = Tickers()[(int)TickerId(name)];
        return long.Parse(line.AsSpan((name + " COUNT : ").Length), CultureInfo.InvariantCulture);
    }

    private string[] Tickers()
    {
        return _options.GetStatisticsString()
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(line => line.Contains(" COUNT : ", StringComparison.Ordinal) && !line.Contains(" P50 : ", StringComparison.Ordinal))
            .ToArray();
    }

    /// <summary>The write path options of one RocksDB store.</summary>
    public readonly record struct Variant(bool PipelinedWrite, bool ConcurrentMemtableWrite, bool AdaptiveYield);
}
