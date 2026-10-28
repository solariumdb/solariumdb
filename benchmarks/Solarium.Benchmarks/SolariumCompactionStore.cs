using System.Diagnostics;
using System.Globalization;

namespace Solarium.Benchmarks;

/// <summary>Solarium on the default storage environment, for the compaction scenarios.</summary>
internal sealed class SolariumCompactionStore : CompactionStore
{
    private readonly SolariumDb _db;
    private readonly ColumnFamily[] _families;
    private readonly ColumnFamily _metadata;

    /// <summary>Opens a new database in <paramref name="directory"/> with <paramref name="families"/> and <c>metadata</c>.</summary>
    public SolariumCompactionStore(string directory, CompactionSettings settings, string[] families)
    {
        DbOptions options = new()
        {
            CreateMissingColumnFamilies = true,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = settings.WriteBufferSize,
                MaxWriteBufferNumber = settings.MaxWriteBufferNumber,
                Level0FileNumCompactionTrigger = settings.Level0Trigger,
                Level0SlowdownWritesTrigger = settings.Level0Slowdown,
                Level0StopWritesTrigger = settings.Level0Stop,
            },

            // Kommander's cap of two flush units, so the metadata family, which never fills a
            // memtable, does not keep every WAL file.
            MaxTotalWalSize = 2 * settings.WriteBufferSize,
            MaxBackgroundCompactions = settings.Compactions,
        };

        _db = SolariumDb.Open(directory, options, [.. families, "metadata"]);
        _families = [.. families.Select(_db.GetColumnFamily)];
        _metadata = _db.GetColumnFamily("metadata");
    }

    /// <inheritdoc/>
    public override void Append(int family, int partition, long firstId, int count, byte[] value)
    {
        WriteBatch batch = new();
        Span<byte> key = stackalloc byte[CompactionSettings.LogKeyWidth];
        for (int i = 0; i < count; i++)
        {
            CompactionSettings.LogKey(key, partition, firstId + i);
            batch.Put(key, value, _families[family]);
        }

        _db.Write(batch);
    }

    /// <inheritdoc/>
    public override void Put(ReadOnlySpan<byte> key, byte[] value) => _db.Put(key, value, _families[0]);

    /// <inheritdoc/>
    public override int MaxLevel0Files() => (int)_families.Max(f => Property("rocksdb.num-files-at-level0", f));

    /// <inheritdoc/>
    public override long LiveSstBytes() => _families.Append(_metadata).Sum(f => Property("rocksdb.live-sst-files-size", f));

    /// <inheritdoc/>
    public override bool Settle(TimeSpan timeout)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < timeout)
        {
            if (Property("rocksdb.num-running-compactions", _metadata) == 0 && _families.All(f => Property("rocksdb.compaction-pending", f) == 0
                && Property("rocksdb.num-immutable-mem-table", f) == 0))
                return true;

            Thread.Sleep(500);
        }

        return false;
    }

    /// <inheritdoc/>
    public override (long FlushBytes, long CompactionRead, long CompactionWritten, long? MovedFiles, TimeSpan Stalled) Counts()
    {
        (long _, long movedFiles, long _, long bytesRead, long bytesWritten) = _db.CompactionCounts();
        return (_db.FlushBytesWritten, bytesRead, bytesWritten, movedFiles, _db.WriteStalls().Stalled);
    }

    /// <inheritdoc/>
    public override void DropBelow(int family, int partition, long floor)
    {
        if (floor >= 2)
            _db.DeleteFilesInRange(CompactionSettings.LogKey(partition, 0), CompactionSettings.LogKey(partition, floor - 1), _families[family]);
    }

    /// <inheritdoc/>
    public override void Dispose() => _db.Dispose();

    /// <inheritdoc/>
    protected override long ActiveEntries(int family) => Property("rocksdb.num-entries-active-mem-table", _families[family]);

    /// <inheritdoc/>
    protected override void WriteFloor(int family, int partition, long newFloor, long tombstoneStart)
    {
        WriteBatch batch = new();
        batch.Put(CompactionSettings.FloorKey(partition), BitConverter.GetBytes(newFloor), _metadata);
        if (tombstoneStart >= 0)
            batch.DeleteRange(CompactionSettings.LogKey(partition, tombstoneStart), CompactionSettings.LogKey(partition, newFloor), _families[family]);

        _db.Write(batch);
    }

    private long Property(string name, ColumnFamily family) => long.Parse(_db.GetProperty(name, family)!, CultureInfo.InvariantCulture);
}
