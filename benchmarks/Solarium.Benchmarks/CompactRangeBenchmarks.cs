using System.Text.Unicode;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace Solarium.Benchmarks;

/// <summary>
/// One manual compaction that merges level 0 tables of overlapping keys into one sorted run, and the flush that
/// wrote one of those tables. Each iteration builds its own store outside the measurement: 1,000,000
/// <c>Put</c>s of a 31-byte key and a 128-byte value, with keys drawn at random from 500,000, flushed as four
/// tables, so the merge reads every entry of every table and drops the older versions.
/// </summary>
/// <remarks>
/// The time reported is of the whole <c>CompactRange</c> or the whole <c>Flush</c>, and <c>Allocated</c> of the
/// managed memory it took. The files are in the OS page cache, so the reads cost their system calls and copies,
/// not the drive.
/// </remarks>
[MemoryDiagnoser]
[SimpleJob(RunStrategy.Monitoring, warmupCount: 2, iterationCount: 10, invocationCount: 1)]
public class CompactRangeBenchmarks
{
    private const int Entries = 1_000_000;
    private const int Keys = 500_000;
    private const int Tables = 4;
    private const int ValueSize = 128;

    private string _directory = null!;
    private SolariumDb _db = null!;

    /// <summary>How the tables store their data blocks.</summary>
    [Params(CompressionType.None, CompressionType.Lz4)]
    public CompressionType Compression { get; set; }

    [IterationSetup(Target = nameof(CompactRange))]
    public void SetupTables() => Load(flushLast: true);

    /// <summary>Leaves the last quarter of the entries in the memtable, for <see cref="Flush"/> to write.</summary>
    [IterationSetup(Target = nameof(Flush))]
    public void SetupMemTable() => Load(flushLast: false);

    [IterationCleanup]
    public void Cleanup()
    {
        _db.Dispose();
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark]
    public void CompactRange() => _db.CompactRange(null, null);

    [Benchmark]
    public void Flush() => _db.Flush();

    private void Load(bool flushLast)
    {
        _directory = Directory.CreateTempSubdirectory("solarium-compact-bench-").FullName;
        DbOptions options = new()
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30, DisableAutoCompactions = true, Compression = Compression },
        };
        _db = SolariumDb.Open(_directory, options);

        // Half of each value repeats one byte, so a block compresses, as rows with text do.
        Random random = new(42);
        byte[] value = new byte[ValueSize];
        random.NextBytes(value.AsSpan(0, ValueSize / 2));
        int perTable = Entries / Tables;
        WriteBatch batch = new();
        for (int table = 0; table < Tables; table++)
        {
            for (int i = 0; i < perTable; i++)
            {
                batch.Put(Key(random.Next(Keys)), value);
                if (batch.Count == 100)
                {
                    _db.Write(batch);
                    batch.Clear();
                }
            }

            _db.Write(batch);
            batch.Clear();
            if (flushLast || table < Tables - 1)
                _db.Flush();
        }
    }

    // Kommander's key layout for partition 1.
    private static byte[] Key(long logId)
    {
        byte[] key = new byte[31];
        Utf8.TryWrite(key, $"{1:D10}:{logId:D20}", out _);
        return key;
    }
}
