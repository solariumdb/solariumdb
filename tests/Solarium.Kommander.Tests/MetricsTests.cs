using System.Diagnostics.Metrics;
using Kommander.Data;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The instruments of <see cref="SolariumWalMetrics"/>, read through a <see cref="MeterListener"/> as a
/// host's telemetry reads them: an open WAL reports what its engine holds, through the same readers the
/// WAL has, until it is disposed, and reopens count by outcome.
/// </summary>
[Collection(SerialCollection.Name)]
public sealed class MetricsTests : IDisposable
{
    private const int Partition = 1;

    private static readonly string[] Gauges =
    [
        "raft.wal.alive_log_bytes",
        "raft.wal.delayed_write_rate",
        "raft.wal.memtable_memory",
        "raft.wal.shard_l0_files",
        "raft.wal.shard_l6_files",
        "raft.wal.shard_live_sst_bytes",
        "raft.wal.table_readers_memory",
        "raft.wal.write_stalled",
    ];

    private static readonly SolariumWalOptions Small = new() { ShardWriteBufferSizeBytes = 64 * 1024 };

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_open_wal_reports_what_its_engine_holds_until_it_is_disposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using Recorder recorder = new();

        // A WAL that another test dropped without disposing it reports until it is collected.
        GC.Collect();
        GC.WaitForPendingFinalizers();

        SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, options: Small);
        Dictionary<string, List<long>> observed;
        try
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Logs.CommittedRange(1, 40, Logs.Incompressible(1024)))]));
            wal.FlushMemTablesForTesting(wait: true);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Logs.CommittedRange(41, 50, Logs.Incompressible(1024)))]));

            observed = recorder.Observe();
            Assert.Equal(Gauges, observed.Keys.Where(name => Gauges.Contains(name)).Order(StringComparer.Ordinal));
            foreach (string name in Gauges)
                Assert.Single(observed[name]);

            Assert.Equal(wal.GetAliveWriteAheadLogBytes(), observed["raft.wal.alive_log_bytes"][0]);
            Assert.Equal(wal.GetActualDelayedWriteRate(), observed["raft.wal.delayed_write_rate"][0]);
            Assert.Equal(wal.GetMemtableMemoryBytes(), observed["raft.wal.memtable_memory"][0]);
            Assert.Equal(wal.GetShardLevel0FileCount(), observed["raft.wal.shard_l0_files"][0]);
            Assert.Equal(wal.GetShardLevel6FileCount(), observed["raft.wal.shard_l6_files"][0]);
            Assert.Equal(wal.GetShardLiveSstBytes(), observed["raft.wal.shard_live_sst_bytes"][0]);
            Assert.Equal(wal.GetTableReadersMemoryBytes(), observed["raft.wal.table_readers_memory"][0]);
            Assert.Equal(wal.GetIsWriteStopped(), observed["raft.wal.write_stalled"][0]);

            // The readers report the engine, not zeros: the flushed rows are in an SSTable, the rest in a memtable and the WAL file.
            Assert.True(observed["raft.wal.shard_live_sst_bytes"][0] > 40 * 1024);
            Assert.Equal(1, observed["raft.wal.shard_l0_files"][0]);
            Assert.True(observed["raft.wal.memtable_memory"][0] > 10 * 1024);
            Assert.True(observed["raft.wal.table_readers_memory"][0] > 0);
            Assert.True(observed["raft.wal.alive_log_bytes"][0] > 10 * 1024);
            Assert.Equal(0, observed["raft.wal.write_stalled"][0]);
        }
        finally
        {
            wal.Dispose();
        }

        observed = recorder.Observe();
        foreach (string name in Gauges)
            Assert.Empty(observed.GetValueOrDefault(name) ?? []);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Only_a_wal_with_a_block_cache_reports_its_usage_and_pinned_usage(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using Recorder recorder = new();
        GC.Collect();
        GC.WaitForPendingFinalizers();

        Cache cache = new(8L * 1024 * 1024);
        using SolariumWAL plain = Wals.Open(scope.Environment, _directory.Combine("plain"), options: Small);
        using SolariumWAL cached = Wals.Open(scope.Environment, _directory.Combine("cached"), options: Small with { BlockCache = cache });
        foreach (SolariumWAL wal in new[] { plain, cached })
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Logs.CommittedRange(1, 40, Logs.Incompressible(1024)))]));
            wal.FlushMemTablesForTesting(wait: true);
        }

        // The flushed table is in level 0, so its blocks are pinned.
        Dictionary<string, List<long>> observed = recorder.Observe();
        Assert.Equal(cache.Usage, Assert.Single(observed["raft.wal.block_cache_usage"]));
        Assert.Equal(cache.PinnedUsage, Assert.Single(observed["raft.wal.block_cache_pinned_usage"]));
        Assert.True(cache.PinnedUsage > 0);
    }

    // As in StorageFailureTests: with only the WAL files refusing writes, the first reopen fails, and
    // the reopen after they take writes again succeeds.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Reopens_count_by_outcome(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using Recorder recorder = new();
        HookedEnvironment hooks = new(scope.Environment) { FailingPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        using SolariumWAL wal = Wals.Open(hooks, _directory.Path, syncWrites: true);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Logs.CommittedRange(1, 5))]));
        hooks.FailWrites = true;
        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Logs.Committed(6)])]));
        Assert.Equal(1, recorder.Reopens("failed"));
        Assert.Equal(0, recorder.Reopens("success"));

        hooks.FailWrites = false;
        Wals.WaitUntil(() => wal.Write([(Partition, [Logs.Committed(6)])]), status => status == RaftOperationStatus.Success, TimeSpan.FromSeconds(30));
        Assert.Equal(1, recorder.Reopens("failed"));
        Assert.Equal(1, recorder.Reopens("success"));
    }

    /// <summary>Listens to the instruments of <see cref="SolariumWalMetrics.MeterName"/>.</summary>
    private sealed class Recorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, List<long>> _measurements = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _reopens = new(StringComparer.Ordinal);

        /// <summary>Starts listening.</summary>
        public Recorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == SolariumWalMetrics.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>(Record);
            _listener.Start();
        }

        /// <summary>Observes the gauges once and returns their measurements by instrument name.</summary>
        public Dictionary<string, List<long>> Observe()
        {
            lock (_measurements)
                _measurements.Clear();

            _listener.RecordObservableInstruments();
            lock (_measurements)
                return _measurements.ToDictionary(pair => pair.Key, pair => pair.Value.ToList(), StringComparer.Ordinal);
        }

        /// <summary>The reopens counted with <paramref name="outcome"/> since the recorder started.</summary>
        public long Reopens(string outcome)
        {
            lock (_measurements)
                return _reopens.GetValueOrDefault(outcome);
        }

        /// <summary>Stops listening.</summary>
        public void Dispose() => _listener.Dispose();

        private void Record(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            lock (_measurements)
            {
                if (instrument.Name == "raft.wal.engine_reopens_total")
                {
                    foreach (KeyValuePair<string, object?> tag in tags)
                        if (tag.Key == "outcome")
                            _reopens[(string)tag.Value!] = _reopens.GetValueOrDefault((string)tag.Value!) + value;

                    return;
                }

                if (!_measurements.TryGetValue(instrument.Name, out List<long>? values))
                    _measurements[instrument.Name] = values = [];

                values.Add(value);
            }
        }
    }
}
