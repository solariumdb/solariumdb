using System.Text.Unicode;
using BenchmarkDotNet.Attributes;

namespace Solarium.Benchmarks;

/// <summary>
/// Reads from a full memtable: <c>Get</c> of keys that are there, and iterator steps forward and
/// backward. The memtable holds one iteration of the unsynced <c>Put</c> benchmark (921,600 entries of
/// a 31-byte key and a 128-byte value, in 16 partitions written in turn), and no flush runs, so the
/// reads measure the memtable's layout, not SSTables.
/// </summary>
/// <remarks>The time reported is per <c>Get</c> or per step. The process runs the server GC, as the
/// <c>Put</c> benchmarks do.</remarks>
[MemoryDiagnoser]
[GcServer(true)]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class MemTableReadBenchmarks
{
    private const int Entries = 921_600;
    private const int Partitions = 16;
    private const int ValueSize = 128;
    private const int Lookups = 4096;
    private const int Steps = 4096;

    private string _directory = null!;
    private SolariumDb _db = null!;
    private byte[][] _lookups = null!;
    private byte[] _middle = null!;

    [GlobalSetup]
    public void Setup()
    {
        _directory = Directory.CreateTempSubdirectory("solarium-read-bench-").FullName;
        DbOptions options = new() { DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30 } };
        _db = SolariumDb.Open(_directory, options);

        byte[] value = new byte[ValueSize];
        Random.Shared.NextBytes(value);
        for (int i = 0; i < Entries; i++)
            _db.Put(Key(i % Partitions, i / Partitions), value);

        // Keys in a random order, so each lookup starts cold, as in a log that many readers share.
        Random random = new(42);
        _lookups = new byte[Lookups][];
        for (int i = 0; i < Lookups; i++)
            _lookups[i] = Key(random.Next(Partitions), random.Next(Entries / Partitions));

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

    [Benchmark(OperationsPerInvoke = Steps)]
    public long IteratorPrev()
    {
        using Iterator iterator = _db.NewIterator();
        long bytes = 0;
        iterator.Seek(_middle);
        for (int i = 0; i < Steps && iterator.Valid(); i++, iterator.Prev())
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
