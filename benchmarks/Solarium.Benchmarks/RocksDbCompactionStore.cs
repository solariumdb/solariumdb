using System.Diagnostics;
using System.Globalization;
using RocksDbSharp;
using RocksDbHandle = RocksDbSharp.RocksDb;
using RocksDbNative = RocksDbSharp.Native;
using RocksDbOptions = RocksDbSharp.DbOptions;
using RocksDbColumnFamilyOptions = RocksDbSharp.ColumnFamilyOptions;
using RocksDbWriteBatch = RocksDbSharp.WriteBatch;
using RocksDbWriteOptions = RocksDbSharp.WriteOptions;

namespace Solarium.Benchmarks;

/// <summary>
/// RocksDB for the compaction scenarios, with Kommander's shard options: its background jobs (half
/// the processors, from 2 to 8), the L0 triggers, and the WAL cap of two flush units. Unlike
/// Kommander, one memtable makes a flush (Solarium merges none) and nothing is compressed.
/// </summary>
internal sealed class RocksDbCompactionStore : CompactionStore
{
    private readonly RocksDbOptions _options;
    private readonly RocksDbHandle _db;
    private readonly ColumnFamilyHandle[] _families;
    private readonly ColumnFamilyHandle _metadata;
    private readonly RocksDbWriteOptions _unsynced = new();
    private readonly Dictionary<string, uint> _tickers;

    /// <summary>Opens a new database in <paramref name="directory"/> with <paramref name="families"/> and <c>metadata</c>.</summary>
    public RocksDbCompactionStore(string directory, CompactionSettings settings, string[] families)
    {
        _options = new RocksDbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetAllowConcurrentMemtableWrite(true)
            .SetMaxTotalWalSize((ulong)(2 * settings.WriteBufferSize))
            .EnableStatistics();
        RocksDbNative.Instance.rocksdb_options_set_max_background_jobs(_options.Handle, Math.Clamp(System.Environment.ProcessorCount / 2, 2, 8));

        ColumnFamilies columnFamilies = new(Options(settings));
        foreach (string name in families.Append("metadata"))
            columnFamilies.Add(name, Options(settings));

        _db = RocksDbHandle.Open(_options, directory, columnFamilies);
        _families = [.. families.Select(_db.GetColumnFamily)];
        _metadata = _db.GetColumnFamily("metadata");
        _tickers = TickerIds();
    }

    /// <inheritdoc/>
    public override void Append(int family, int partition, long firstId, int count, byte[] value)
    {
        using RocksDbWriteBatch batch = new();
        byte[] key = new byte[CompactionSettings.LogKeyWidth];
        for (int i = 0; i < count; i++)
        {
            CompactionSettings.LogKey(key, partition, firstId + i);
            batch.Put(key, value, _families[family]);
        }

        _db.Write(batch, _unsynced);
    }

    /// <inheritdoc/>
    public override void Put(ReadOnlySpan<byte> key, byte[] value) => _db.Put(key, value, _families[0], _unsynced);

    /// <inheritdoc/>
    public override int MaxLevel0Files() => _families.Max(f => (int)Property("rocksdb.num-files-at-level0", f));

    /// <inheritdoc/>
    public override long LiveSstBytes() => _families.Append(_metadata).Sum(f => Property("rocksdb.live-sst-files-size", f));

    /// <inheritdoc/>
    public override bool Settle(TimeSpan timeout)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < timeout)
        {
            if (_families.All(f => Property("rocksdb.compaction-pending", f) == 0 && Property("rocksdb.num-running-compactions", f) == 0
                && Property("rocksdb.num-immutable-mem-table", f) == 0))
                return true;

            Thread.Sleep(500);
        }

        return false;
    }

    /// <inheritdoc/>
    public override (long FlushBytes, long CompactionRead, long CompactionWritten, long? MovedFiles, TimeSpan Stalled) Counts()
    {
        return (Ticker("rocksdb.flush.write.bytes"), Ticker("rocksdb.compact.read.bytes"), Ticker("rocksdb.compact.write.bytes"), null,
            TimeSpan.FromMicroseconds(Ticker("rocksdb.stall.micros")));
    }

    /// <inheritdoc/>
    public override void DropBelow(int family, int partition, long floor)
    {
        if (floor < 2)
            return;

        byte[] begin = CompactionSettings.LogKey(partition, 0);
        byte[] end = CompactionSettings.LogKey(partition, floor - 1);
        RocksDbNative.Instance.rocksdb_delete_file_in_range_cf(_db.Handle, _families[family].Handle, begin, (UIntPtr)begin.Length, end, (UIntPtr)end.Length, out IntPtr error);
        if (error != IntPtr.Zero)
        {
            string message = System.Runtime.InteropServices.Marshal.PtrToStringAnsi(error) ?? "";
            RocksDbNative.Instance.rocksdb_free(error);
            throw new InvalidOperationException($"DeleteFilesInRange failed: {message}");
        }
    }

    /// <inheritdoc/>
    public override void Dispose() => _db.Dispose();

    /// <inheritdoc/>
    protected override long ActiveEntries(int family) => Property("rocksdb.num-entries-active-mem-table", _families[family]);

    /// <inheritdoc/>
    protected override void WriteFloor(int family, int partition, long newFloor, long tombstoneStart)
    {
        using RocksDbWriteBatch batch = new();
        batch.Put(CompactionSettings.FloorKey(partition), BitConverter.GetBytes(newFloor), _metadata);
        if (tombstoneStart >= 0)
        {
            byte[] begin = CompactionSettings.LogKey(partition, tombstoneStart);
            byte[] end = CompactionSettings.LogKey(partition, newFloor);
            batch.DeleteRange(begin, (ulong)begin.Length, end, (ulong)end.Length, _families[family]);
        }

        _db.Write(batch, _unsynced);
    }

    private static RocksDbColumnFamilyOptions Options(CompactionSettings settings)
    {
        return new RocksDbColumnFamilyOptions()
            .SetWriteBufferSize((ulong)settings.WriteBufferSize)
            .SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
            .SetMinWriteBufferNumberToMerge(1)
            .SetLevel0FileNumCompactionTrigger(settings.Level0Trigger)
            .SetLevel0SlowdownWritesTrigger(settings.Level0Slowdown)
            .SetLevel0StopWritesTrigger(settings.Level0Stop)
            .SetCompression(Compression.No);
    }

    private long Property(string name, ColumnFamilyHandle family) => long.Parse(_db.GetProperty(name, family), CultureInfo.InvariantCulture);

    private long Ticker(string name) => (long)RocksDbNative.Instance.rocksdb_options_statistics_get_ticker_count(_options.Handle, _tickers[name]);

    // The C API reads a ticker by its enum value, and the binding has no enum. The statistics string
    // lists every ticker in enum order, as "name COUNT : value"; histograms follow.
    private Dictionary<string, uint> TickerIds()
    {
        string[] tickers = [.. _options.GetStatisticsString()
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(line => line.Contains(" COUNT : ", StringComparison.Ordinal) && !line.Contains(" P50 : ", StringComparison.Ordinal))];
        Dictionary<string, uint> ids = new(StringComparer.Ordinal);
        for (int i = 0; i < tickers.Length; i++)
            ids[tickers[i][..tickers[i].IndexOf(" COUNT : ", StringComparison.Ordinal)]] = (uint)i;

        return ids;
    }
}
