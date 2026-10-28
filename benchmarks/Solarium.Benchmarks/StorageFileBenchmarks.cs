using BenchmarkDotNet.Attributes;
using Solarium.Storage;

namespace Solarium.Benchmarks;

/// <summary>
/// Raw <see cref="IStorageFile"/> reads from one thread, with no engine above them: the cost of each
/// environment's path to the kernel. The file is in the page cache.
/// </summary>
[MemoryDiagnoser]
public class StorageFileBenchmarks
{
    private const int ReadsPerInvoke = 64;
    private const long ReadFileSize = 64L * 1024 * 1024;

    private IStorageEnvironment _environment = null!;
    private string _directory = null!;
    private IStorageFile _readFile = null!;
    private byte[] _buffer = null!;
    private long[] _readOffsets = null!;
    private int _nextRead;

    [ParamsSource(nameof(EnvironmentNames))]
    public string Environment { get; set; } = "default";

    [Params(4096, 65536)]
    public int Size { get; set; }

    public static IEnumerable<string> EnvironmentNames() => Environments.Names();

    [GlobalSetup]
    public void Setup()
    {
        _environment = Environments.Create(Environment);
        _directory = Directory.CreateTempSubdirectory("solarium-bench-").FullName;
        _buffer = new byte[Size];
        Random.Shared.NextBytes(_buffer);

        // Fill the read file, so every read finds data in the page cache.
        _readFile = _environment.OpenFile(Path.Combine(_directory, "read.dat"));
        for (long offset = 0; offset < ReadFileSize; offset += _buffer.Length)
        {
            _readFile.Write(_buffer, offset);
        }

        // Block-aligned offsets in a fixed pseudo-random order, the same for both environments.
        var random = new Random(42);
        long blocks = ReadFileSize / Size;
        _readOffsets = new long[4096];
        for (int i = 0; i < _readOffsets.Length; i++)
        {
            _readOffsets[i] = random.NextInt64(blocks) * Size;
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _readFile.Dispose();
        Environments.Dispose(_environment);
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark(OperationsPerInvoke = ReadsPerInvoke)]
    public int Read()
    {
        int total = 0;
        for (int i = 0; i < ReadsPerInvoke; i++)
        {
            total += _readFile.Read(_buffer, _readOffsets[_nextRead]);
            _nextRead = (_nextRead + 1) & (_readOffsets.Length - 1);
        }

        return total;
    }
}
