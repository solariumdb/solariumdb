using Solarium.IoUring;
using Solarium.Storage;

namespace Solarium.Benchmarks;

/// <summary>The storage environments a benchmark compares, by the name it takes as a parameter.</summary>
public static class Environments
{
    public static IEnumerable<string> Names()
    {
        yield return "default";

        // Without io_uring the comparison is meaningless; fail loudly instead of reporting the
        // default environment alone.
        if (!IoUringStorageEnvironment.IsSupported())
        {
            throw new InvalidOperationException(
                "io_uring is not available. In Docker, run with --security-opt seccomp=unconfined --ulimit memlock=-1.");
        }

        yield return "io_uring";
    }

    public static IStorageEnvironment Create(string name)
    {
        return name == "io_uring" ? new IoUringStorageEnvironment() : DefaultStorageEnvironment.Instance;
    }

    public static void Dispose(IStorageEnvironment environment) => (environment as IDisposable)?.Dispose();
}
