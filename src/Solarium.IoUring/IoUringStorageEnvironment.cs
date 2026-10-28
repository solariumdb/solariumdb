using System.Runtime.InteropServices;
using ioxide;
using Solarium.Storage;

namespace Solarium.IoUring;

/// <summary>
/// An <see cref="IStorageEnvironment"/> on Linux io_uring, through one ioxide reactor that opens no
/// listeners. Writes are IORING_OP_WRITE on that reactor's ring, and the calling thread blocks until
/// the completion arrives. A read is pread on the calling thread; a batch read (<see cref="MultiRead"/>)
/// sends what the page cache misses to the ring together.
/// </summary>
/// <remarks>
/// <para>ioxide's <see cref="IRingHost"/> has no fsync operation, so <see cref="IStorageFile.Flush"/>
/// calls fdatasync directly on the calling thread.</para>
/// <para>A write that the ring completes with <c>ECANCELED</c>, which io-wq reports for work it never
/// ran, is done again with pwrite on the calling thread, and counted in
/// <see cref="CanceledOperations"/>.</para>
/// <para>Each write is a hand-off to the reactor thread and back, which costs more than the pwrite it
/// replaces. Reads skip the ring: a table read is one blocking read, usually from the page cache, and
/// through the ring every caller's reads would run one at a time on the reactor thread.</para>
/// <para>A fault that stops the reactor after it started leaves in-flight and later operations
/// waiting forever. The fault is written to standard error.</para>
/// <para>Create one environment per process, and dispose it after every database that uses it is
/// closed.</para>
/// </remarks>
public sealed class IoUringStorageEnvironment : IStorageEnvironment, IDisposable
{
    private readonly Reactor _reactor;
    private readonly Thread _thread;
    private readonly Action _canceled;
    private long _canceledOperations;
    private int _disposed;

    /// <summary>Starts the reactor thread and waits for it; throws <see cref="IOException"/> when the reactor cannot start.</summary>
    /// <param name="ringEntries">The size of the io_uring submission queue.</param>
    public IoUringStorageEnvironment(uint ringEntries = 256)
    {
        ServerConfig config = new()
        {
            ReactorCount = 1,
            RingEntries = ringEntries,

            // No sockets at all; the reactor still registers a receive-buffer ring, so keep it tiny.
            RecvSlots = 16,
            RecvBufferSize = 4096,
            Tcp = null,
            Udp = null,
            Quic = null,
        };

        _reactor = new Reactor(0, config);
        _canceled = NoteCanceled;

        // Not disposed: OnFault can still run on the reactor thread after the constructor returns.
        ManualResetEventSlim started = new();
        Exception? fault = null;
        _reactor.OnStart = _ => started.Set();
        _reactor.OnFault = (_, e) =>
        {
            fault = e;
            Console.Error.WriteLine($"solarium: the io_uring reactor stopped after a fault; file I/O through it will not complete: {e}");
            started.Set();
        };

        _thread = new Thread(_reactor.Run) { Name = "solarium-io_uring", IsBackground = true };
        _thread.Start();
        started.Wait();

        if (fault is not null)
            throw new IOException("The io_uring reactor did not start.", fault);
    }

    /// <inheritdoc />
    public string Name => $"io_uring (ioxide {IoxideRuntime.Version})";

    /// <summary>
    /// The writes that the ring completed with <c>ECANCELED</c> and that were done again with pwrite
    /// on the calling thread, since the environment started.
    /// </summary>
    public long CanceledOperations => Interlocked.Read(ref _canceledOperations);

    /// <summary>The reactor that the files submit to; tests wrap it.</summary>
    internal IRingHost RingHost => _reactor;

    /// <summary>Counts one operation in <see cref="CanceledOperations"/>; the files call it.</summary>
    internal void NoteCanceled() => Interlocked.Increment(ref _canceledOperations);

    /// <summary>True on Linux when the kernel lets this process create an io_uring ring.</summary>
    public static bool IsSupported()
    {
        if (!OperatingSystem.IsLinux())
            return false;

        try
        {
            Ring.Create(8).Dispose();
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public IStorageFile OpenFile(string path)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return IoUringStorageFile.Open(_reactor, path, _canceled);
    }

    /// <summary>
    /// Reads a batch: the reads that the page cache holds on the calling thread (preadv2 with RWF_NOWAIT), and the
    /// others in the ring together, so the drive serves them at once; the call returns when all are done. A request
    /// on a file this environment did not open is read through that file's <see cref="IStorageFile.Read"/>.
    /// </summary>
    public void MultiRead(Span<ReadRequest> requests) => IoUringStorageFile.MultiRead(requests);

    /// <summary>Stops the reactor and joins its thread. Safe to call more than once.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _reactor.Stop();
            _thread.Join();
        }
    }
}
