using System.Text.Unicode;
using BenchmarkDotNet.Attributes;

namespace Solarium.Benchmarks;

/// <summary>
/// Reads of live keys beside many range tombstones, the shape Kommander's compaction floor leaves:
/// each pass deletes a range of dead log ids below the live ones. The tombstones do not cover the keys
/// read, so every read must check them and find none. With <see cref="Source"/> "memtable", the rows
/// and the tombstones are in the active memtable; with "table", one flush has written them to an SSTable.
/// </summary>
/// <remarks>The time reported is per <c>Get</c> or per iterator step.</remarks>
[MemoryDiagnoser]
[GcServer(true)]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class RangeTombstoneReadBenchmarks
{
    private const int LiveRows = 4096;
    private const long FirstLiveId = 1_000_000;
    private const int ValueSize = 128;

    private string _directory = null!;
    private SolariumDb _db = null!;
    private byte[][] _lookups = null!;
    private byte[] _first = null!;

    /// <summary>The range tombstones beside the live rows.</summary>
    [Params(0, 16, 128, 1024, 8192)]
    public int Tombstones { get; set; }

    /// <summary>Where the rows and tombstones are: "memtable" or "table".</summary>
    [Params("memtable", "table")]
    public string Source { get; set; } = "memtable";

    [GlobalSetup]
    public void Setup()
    {
        _directory = Directory.CreateTempSubdirectory("solarium-tombstone-bench-").FullName;
        DbOptions options = new()
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30, DisableAutoCompactions = true },
        };
        _db = SolariumDb.Open(_directory, options);

        byte[] value = new byte[ValueSize];
        Random.Shared.NextBytes(value);
        for (int i = 0; i < LiveRows; i++)
            _db.Put(Key(FirstLiveId + i), value);

        // Ranges of dead ids that do not overlap, each a compaction pass of 100 ids.
        for (int i = 0; i < Tombstones; i++)
            _db.DeleteRange(Key(i * 100L), Key((i * 100L) + 100));

        if (Source == "table")
            _db.Flush();

        Random random = new(42);
        _lookups = new byte[LiveRows][];
        for (int i = 0; i < LiveRows; i++)
            _lookups[i] = Key(FirstLiveId + random.Next(LiveRows));

        _first = Key(FirstLiveId);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark(OperationsPerInvoke = LiveRows)]
    public long Get()
    {
        long bytes = 0;
        foreach (byte[] key in _lookups)
            bytes += _db.Get(key)!.Length;

        return bytes;
    }

    [Benchmark(OperationsPerInvoke = LiveRows)]
    public long IteratorNext()
    {
        using Iterator iterator = _db.NewIterator();
        long bytes = 0;
        iterator.Seek(_first);
        for (int i = 0; i < LiveRows && iterator.Valid(); i++, iterator.Next())
            bytes += iterator.Value().Length;

        return bytes;
    }

    // Kommander's key layout for partition 1.
    private static byte[] Key(long logId)
    {
        byte[] key = new byte[31];
        Utf8.TryWrite(key, $"{1:D10}:{logId:D20}", out _);
        return key;
    }
}
