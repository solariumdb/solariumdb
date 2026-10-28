using System.Diagnostics.Metrics;

namespace Solarium.Kommander;

/// <summary>
/// The meter of <see cref="SolariumWAL"/>: the storage-engine instruments that Kommander publishes for
/// its RocksDB WAL on its own meter, with the same names, units and meanings, so dashboards and alerts
/// built on them read a Solarium WAL too once the host also listens to <see cref="MeterName"/>.
/// Kommander's other <c>raft.wal.*</c> instruments (batches, queue depth, write duration, compaction
/// passes) come from Kommander itself, whatever the WAL.
/// </summary>
/// <remarks>
/// Each open <see cref="SolariumWAL"/> reports one measurement per gauge, without tags, as each
/// <c>RocksDbWAL</c> does on Kommander's meter; a WAL stops reporting at <see cref="SolariumWAL.Dispose"/>.
/// <c>raft.wal.block_cache_usage</c> and <c>raft.wal.block_cache_pinned_usage</c> come only from a WAL with a
/// <see cref="SolariumWalOptions.BlockCache"/>: one without has no cache to report, where every
/// <c>RocksDbWAL</c> has RocksDB's default one. The instruments cost nothing while no listener is attached.
/// </remarks>
public static class SolariumWalMetrics
{
    /// <summary>
    /// The meter's name, to subscribe to beside Kommander's <c>"Kommander"</c>: OpenTelemetry's
    /// <c>AddMeter</c>, or <c>dotnet-counters monitor --counters</c>.
    /// </summary>
    public const string MeterName = "Solarium.Kommander";

    private static readonly Meter Meter = new(MeterName, typeof(SolariumWalMetrics).Assembly.GetName().Version?.ToString());

    private static readonly Counter<long> EngineReopens = Meter.CreateCounter<long>(
        "raft.wal.engine_reopens_total",
        description: "Solarium WAL engine reopens performed to clear a storage failure, by outcome.");

    private static readonly KeyValuePair<string, object?> ReopenSucceeded = new("outcome", "success");
    private static readonly KeyValuePair<string, object?> ReopenFailed = new("outcome", "failed");

    // The WALs that report. Observations read them under the lock, and a WAL leaves under it before its
    // database closes, so no observation reads a closing database. The references are weak so a WAL that
    // is never disposed does not stay alive for its gauges.
    private static readonly Lock Gate = new();
    private static readonly List<WeakReference<SolariumWAL>> Wals = [];

    static SolariumWalMetrics()
    {
        Meter.CreateObservableGauge(
            "raft.wal.delayed_write_rate",
            static () => Measure(static wal => wal.GetActualDelayedWriteRate()),
            unit: "By/s",
            description: "Bytes/second the Solarium WAL engine is throttling writers to (0 = no slowdown). Non-zero means the storage engine is stalling the Raft write path.");

        Meter.CreateObservableGauge(
            "raft.wal.write_stalled",
            static () => Measure(static wal => wal.GetIsWriteStopped()),
            description: "1 while the Solarium WAL engine has stopped writes entirely (hard stall), else 0.");

        Meter.CreateObservableGauge(
            "raft.wal.shard_live_sst_bytes",
            static () => Measure(static wal => wal.GetShardLiveSstBytes()),
            unit: "By",
            description: "Live SSTable bytes across the Raft-log shard column families. Far above the retained window means dead files are not being reclaimed.");

        Meter.CreateObservableGauge(
            "raft.wal.shard_l6_files",
            static () => Measure(static wal => wal.GetShardLevel6FileCount()),
            description: "SSTable count in level 6, the bottom level, across the Raft-log shard column families; a shard family below 256 MB keeps all its sorted files there. A flat, high value signals whole-file drops are not landing.");

        Meter.CreateObservableGauge(
            "raft.wal.shard_l0_files",
            static () => Measure(static wal => wal.GetShardLevel0FileCount()),
            description: "SSTable count in level 0 across the Raft-log shard column families; should stay below the level 0 compaction trigger, since log files that do not overlap move out of level 0 without a rewrite.");

        Meter.CreateObservableGauge(
            "raft.wal.table_readers_memory",
            static () => Measure(static wal => wal.GetTableReadersMemoryBytes()),
            unit: "By",
            description: "Bytes the open SSTables of the Raft-log engine keep in memory: range tombstones, and the index and filter blocks unless SolariumWalOptions.BlockCache holds them.");

        Meter.CreateObservableGauge(
            "raft.wal.block_cache_usage",
            static () => MeasureCache(static cache => cache.Usage),
            unit: "By",
            description: "Bytes in the block cache of the Raft-log engine: index and filter blocks, and the memtable memory of a WriteBufferManager created with the cache. A cache shared with other databases reports the whole cache, not this engine's share of it.");

        Meter.CreateObservableGauge(
            "raft.wal.block_cache_pinned_usage",
            static () => MeasureCache(static cache => cache.PinnedUsage),
            unit: "By",
            description: "Bytes in the block cache of the Raft-log engine that cannot be evicted: level 0 index and filter blocks, and memtable charges. Approaching the cache's capacity means the cache cannot make room and lookups read indexes from the files again.");

        Meter.CreateObservableGauge(
            "raft.wal.memtable_memory",
            static () => Measure(static wal => wal.GetMemtableMemoryBytes()),
            unit: "By",
            description: "Bytes of memtable memory across the Raft-log engine's column families, including memtables awaiting flush.");

        Meter.CreateObservableGauge(
            "raft.wal.alive_log_bytes",
            static () => Measure(static wal => wal.GetAliveWriteAheadLogBytes()),
            unit: "By",
            description: "Bytes of write-ahead .log files alive in the Raft-log engine directory, which a restart replays. Bounded by SolariumWalOptions.MaxTotalWalSizeFlushUnits.");
    }

    /// <summary>Adds <paramref name="wal"/> to the WALs the gauges read; the constructor calls it once the WAL is open.</summary>
    internal static void Register(SolariumWAL wal)
    {
        lock (Gate)
            Wals.Add(new WeakReference<SolariumWAL>(wal));
    }

    /// <summary>
    /// Removes <paramref name="wal"/> from the WALs the gauges read. Returns after any observation that
    /// reads it, so the caller may then close its database.
    /// </summary>
    internal static void Unregister(SolariumWAL wal)
    {
        lock (Gate)
            Wals.RemoveAll(reference => !reference.TryGetTarget(out SolariumWAL? target) || ReferenceEquals(target, wal));
    }

    /// <summary>Counts one reopen of a WAL's database, tagged by whether it opened.</summary>
    internal static void RecordEngineReopen(bool succeeded) => EngineReopens.Add(1, succeeded ? ReopenSucceeded : ReopenFailed);

    // One measurement per open WAL with a block cache.
    private static List<Measurement<long>> MeasureCache(Func<Solarium.Cache, long> read)
    {
        lock (Gate)
        {
            List<Measurement<long>> measurements = new(Wals.Count);
            Wals.RemoveAll(reference => !reference.TryGetTarget(out _));
            foreach (WeakReference<SolariumWAL> reference in Wals)
                if (reference.TryGetTarget(out SolariumWAL? wal) && wal.BlockCache is { } cache)
                    measurements.Add(new Measurement<long>(read(cache)));

            return measurements;
        }
    }

    private static List<Measurement<long>> Measure(Func<SolariumWAL, long> read)
    {
        lock (Gate)
        {
            List<Measurement<long>> measurements = new(Wals.Count);
            Wals.RemoveAll(reference => !reference.TryGetTarget(out _));
            foreach (WeakReference<SolariumWAL> reference in Wals)
                if (reference.TryGetTarget(out SolariumWAL? wal))
                    measurements.Add(new Measurement<long>(read(wal)));

            return measurements;
        }
    }
}
