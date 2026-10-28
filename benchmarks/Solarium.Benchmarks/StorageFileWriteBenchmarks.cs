using BenchmarkDotNet.Attributes;
using Solarium.Storage;

namespace Solarium.Benchmarks;

/// <summary>
/// Raw <see cref="IStorageFile.Write"/> calls from one thread at increasing offsets, as an append
/// does, with no engine above them. No flush, so the writes land in the page cache.
/// </summary>
/// <remarks>
/// Each iteration starts with an empty file and no dirty pages of its own: the iteration setup
/// truncates the file and flushes it, outside the measurement. An iteration writes 64 invocations of
/// 64 writes, 16 MB at 4 KB and 256 MB at 64 KB, far below the kernel's background writeback
/// threshold on the benchmark host.
/// </remarks>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 20, invocationCount: 64)]
public class StorageFileWriteBenchmarks
{
    private const int WritesPerInvoke = 64;

    private IStorageEnvironment _environment = null!;
    private string _directory = null!;
    private IStorageFile _file = null!;
    private byte[] _buffer = null!;
    private long _offset;

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
        _file = _environment.OpenFile(Path.Combine(_directory, "write.dat"));
    }

    [IterationSetup]
    public void EmptyFile()
    {
        _file.SetLength(0);
        _file.Flush();
        _offset = 0;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _file.Dispose();
        Environments.Dispose(_environment);
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark(OperationsPerInvoke = WritesPerInvoke)]
    public void Write()
    {
        for (int i = 0; i < WritesPerInvoke; i++)
        {
            _file.Write(_buffer, _offset);
            _offset += Size;
        }
    }
}
