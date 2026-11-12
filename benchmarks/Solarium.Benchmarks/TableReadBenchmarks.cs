using System.Text.Unicode;
using BenchmarkDotNet.Attributes;

namespace Solarium.Benchmarks;

/// <summary>
/// Reads from one SSTable: <c>Get</c> of keys that are there, <c>Get</c> of keys that are not, which the
/// Bloom filter answers, and a forward scan. The table holds what <see cref="MemTableReadBenchmarks"/> holds in
/// its memtable (921,600 entries of a 31-byte key and a 128-byte value, in 16 partitions), written by one flush,
/// and the memtable is empty, so the reads measure the table's index, filter and data blocks.
/// </summary>
/// <remarks>
/// The time reported is per <c>Get</c> or per step. With <see cref="Blocks"/> "file", each read takes its data
/// block from the file, which the OS page cache holds after the warmup; with "cached", from a block cache
/// that holds the whole table. The process runs the server GC, as the <c>Put</c> benchmarks do.
/// </remarks>
[MemoryDiagnoser]
[GcServer(true)]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class TableReadBenchmarks
{
    private const int Entries = 921_600;
    private const int Partitions = 16;
    private const int ValueSize = 128;
    private const int Lookups = 4096;
    private const int Steps = 65_536;

    private string _directory = null!;
    private SolariumDb _db = null!;
    private byte[][] _lookups = null!;
    private byte[][] _missing = null!;
    private byte[] _middle = null!;

    /// <summary>Where a read takes its data blocks from: "file" or "cached".</summary>
    [Params("file", "cached")]
    public string Blocks { get; set; } = "file";

    [GlobalSetup]
    public void Setup()
    {
        _directory = Directory.CreateTempSubdirectory("solarium-table-bench-").FullName;
        bool cached = Blocks == "cached";
        DbOptions options = new()
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = 1L << 30,
                DisableAutoCompactions = true,
                BlockCache = cached ? new Cache(1L << 30) : null,
                CacheDataBlocks = cached,
            },
        };
        _db = SolariumDb.Open(_directory, options);

        byte[] value = new byte[ValueSize];
        Random.Shared.NextBytes(value);
        for (int i = 0; i < Entries; i++)
            _db.Put(Key(i % Partitions, i / Partitions), value);

        _db.Flush();

        // Keys in a random order, so each lookup reads another block. A missing key is past the last log id of
        // its partition: inside the table's key range, so only the filter can rule it out without a block.
        Random random = new(42);
        _lookups = new byte[Lookups][];
        _missing = new byte[Lookups][];
        for (int i = 0; i < Lookups; i++)
        {
            _lookups[i] = Key(random.Next(Partitions), random.Next(Entries / Partitions));
            _missing[i] = Key(random.Next(Partitions - 1), (Entries / Partitions) + random.Next(Entries));
        }

        _middle = Key(Partitions / 2, 0);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark(OperationsPerInvoke = Lookups)]
    public long Get()
    {
        long bytes = 0;
        foreach (byte[] key in _lookups)
            bytes += _db.Get(key)!.Length;

        return bytes;
    }

    [Benchmark(OperationsPerInvoke = Lookups)]
    public int GetMissing()
    {
        int found = 0;
        foreach (byte[] key in _missing)
            if (_db.Get(key) is not null)
                found++;

        return found;
    }

    [Benchmark(OperationsPerInvoke = Steps)]
    public long IteratorNext()
    {
        using Iterator iterator = _db.NewIterator();
        long bytes = 0;
        iterator.Seek(_middle);
        for (int i = 0; i < Steps && iterator.Valid(); i++, iterator.Next())
            bytes += iterator.Value().Length;

        return bytes;
    }

    // Kommander's key layout, as the Put benchmarks write it.
    private static byte[] Key(int partition, long logId)
    {
        byte[] key = new byte[31];
        Utf8.TryWrite(key, $"{partition:D10}:{logId:D20}", out _);
        return key;
    }
}
