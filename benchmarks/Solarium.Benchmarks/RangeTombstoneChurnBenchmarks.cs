using System.Text.Unicode;
using BenchmarkDotNet.Attributes;

namespace Solarium.Benchmarks;

/// <summary>
/// Range deletes that alternate with reads, in the active memtable: each operation is one
/// <c>DeleteRange</c> of a range of dead ids, which does not overlap the others, then one read of a
/// live key outside every range, as a <c>Get</c> or as the <c>Seek</c> of a new iterator. Each delete
/// changes the memtable's tombstones, so the read after it pays for whatever the memtable must
/// rebuild to search them; a read with no delete before it is in <see cref="RangeTombstoneReadBenchmarks"/>.
/// <see cref="Readers"/> threads read live keys meanwhile, and race the measured thread to that work.
/// </summary>
/// <remarks>
/// The time reported is per pair of a delete and a read. Each iteration starts from a new database
/// with <see cref="Tombstones"/> range tombstones and adds <see cref="Pairs"/> more. With readers,
/// the allocations include theirs.
/// </remarks>
[MemoryDiagnoser]
[GcServer(true)]
[SimpleJob(warmupCount: 3, iterationCount: 15, invocationCount: 1)]
public class RangeTombstoneChurnBenchmarks
{
    private const int Pairs = 256;
    private const int LiveRows = 4096;
    private const long FirstLiveId = 1_000_000;
    private const int ValueSize = 128;

    private string _directory = null!;
    private SolariumDb _db = null!;
    private byte[][] _lookups = null!;
    private Thread[] _readers = [];
    private volatile bool _stop;
    private long _next;

    /// <summary>The range tombstones in the memtable before the first pair.</summary>
    [Params(0, 8, 1024, 8192)]
    public int Tombstones { get; set; }

    /// <summary>Threads that read live keys while the pairs run.</summary>
    [Params(0, 4)]
    public int Readers { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _directory = Directory.CreateTempSubdirectory("solarium-tombstone-churn-").FullName;
        DbOptions options = new()
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30, DisableAutoCompactions = true },
        };
        _db = SolariumDb.Open(_directory, options);

        byte[] value = new byte[ValueSize];
        Random.Shared.NextBytes(value);
        for (int i = 0; i < LiveRows; i++)
            _db.Put(Key(FirstLiveId + i), value);

        for (_next = 0; _next < Tombstones; _next++)
            _db.DeleteRange(Key(_next * 100), Key((_next * 100) + 100));

        Random random = new(42);
        _lookups = new byte[Pairs][];
        for (int i = 0; i < Pairs; i++)
            _lookups[i] = Key(FirstLiveId + random.Next(LiveRows));

        // One read first, so the pairs start from a memtable that a reader has searched.
        _db.Get(_lookups[0]);

        _stop = false;
        _readers = new Thread[Readers];
        for (int r = 0; r < Readers; r++)
        {
            int seed = r;
            _readers[r] = new Thread(() => Read(seed)) { IsBackground = true };
            _readers[r].Start();
        }
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _stop = true;
        foreach (Thread reader in _readers)
            reader.Join();

        _db.Dispose();
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark(OperationsPerInvoke = Pairs)]
    public long DeleteRangeThenGet()
    {
        long bytes = 0;
        foreach (byte[] key in _lookups)
        {
            _db.DeleteRange(Key(_next * 100), Key((_next * 100) + 100));
            _next++;
            bytes += _db.Get(key)!.Length;
        }

        return bytes;
    }

    [Benchmark(OperationsPerInvoke = Pairs)]
    public long DeleteRangeThenSeek()
    {
        long bytes = 0;
        foreach (byte[] key in _lookups)
        {
            _db.DeleteRange(Key(_next * 100), Key((_next * 100) + 100));
            _next++;
            using Iterator iterator = _db.NewIterator();
            iterator.Seek(key);
            bytes += iterator.Value().Length;
        }

        return bytes;
    }

    private void Read(int seed)
    {
        Random random = new(seed);
        while (!_stop)
            _db.Get(Key(FirstLiveId + random.Next(LiveRows)));
    }

    // Kommander's key layout for partition 1.
    private static byte[] Key(long logId)
    {
        byte[] key = new byte[31];
        Utf8.TryWrite(key, $"{1:D10}:{logId:D20}", out _);
        return key;
    }
}
