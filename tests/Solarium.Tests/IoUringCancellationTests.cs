using ioxide;
using Solarium.IoUring;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// What the io_uring environment does with a write, or a read of a batch, that the ring completes with
/// <c>-ECANCELED</c>, as io-wq does for work it never ran: it does it again with pwrite or pread on the
/// calling thread, and counts it. A single read never goes to the ring, and a batch read only for what the
/// page cache does not hold. A wrapper of the reactor cancels the operations a test picks, since the kernel
/// does it too rarely to wait for.
/// </summary>
public sealed class IoUringCancellationTests
{
    private const int ECANCELED = 125;

    [Fact]
    public void A_write_that_the_ring_cancels_completes_on_the_calling_thread_and_reads_skip_the_ring()
    {
        // macOS and Windows have no io_uring.
        if (!IoUringStorageEnvironment.IsSupported())
            return;

        using IoUringStorageEnvironment environment = new();
        using TempDirectory directory = new();
        CancelingRing ring = new(environment.RingHost);
        string path = directory.Combine("file.dat");
        byte[] data = new byte[64 * 1024];
        new Random(5).NextBytes(data);

        using (IStorageFile file = IoUringStorageFile.Open(ring, path, environment.NoteCanceled))
        {
            ring.Fail(-ECANCELED);
            file.Write(data, 4096);
            Assert.Equal(1, environment.CanceledOperations);

            // The next write goes to the ring again; the reads do not go to it at all.
            int submitted = ring.Submitted;
            file.Write(B("tail"), 4096 + data.Length);
            Assert.Equal(submitted + 1, ring.Submitted);

            byte[] back = new byte[data.Length];
            Assert.Equal(data.Length, file.Read(back, 4096));
            Assert.Equal(data, back);
            Assert.Equal("tail", S(back.AsSpan(0, file.Read(back.AsSpan(0, 4), 4096 + data.Length))));
            Assert.Equal(submitted + 1, ring.Submitted);
            Assert.Equal(1, environment.CanceledOperations);
        }

        byte[] onDisk = File.ReadAllBytes(path);
        Assert.Equal(data, onDisk.AsSpan(4096, data.Length).ToArray());
        Assert.Equal("tail", S(onDisk.AsSpan(4096 + data.Length)));
    }

    // Any other failure is still an IOException with its errno, and the thread's next write completes.
    [Fact]
    public void A_write_that_the_ring_fails_otherwise_throws_its_errno_and_the_next_write_completes()
    {
        if (!IoUringStorageEnvironment.IsSupported())
            return;

        using IoUringStorageEnvironment environment = new();
        using TempDirectory directory = new();
        CancelingRing ring = new(environment.RingHost);
        using IStorageFile file = IoUringStorageFile.Open(ring, directory.Combine("file.dat"), environment.NoteCanceled);

        ring.Fail(-5);
        Assert.Contains("errno 5", Assert.Throws<IOException>(() => file.Write(B("data"), 0)).Message);
        Assert.Equal(0, environment.CanceledOperations);

        file.Write(B("next"), 0);
        byte[] back = new byte[4];
        Assert.Equal(4, file.Read(back, 0));
        Assert.Equal("next", S(back));
    }

    // A batch reads what the page cache holds on the calling thread (with RWF_NOWAIT, or with pread on a file system
    // without it, as tmpfs); with that turned off, as for reads the page cache misses, the reads go to the ring together. A read the ring cancels is done again with pread, and any
    // other failure throws its errno, after every read of the batch has completed.
    [Fact]
    public void A_batch_read_goes_to_the_ring_only_for_what_the_page_cache_misses_and_survives_a_cancel()
    {
        if (!IoUringStorageEnvironment.IsSupported())
            return;

        using IoUringStorageEnvironment environment = new();
        using TempDirectory directory = new();
        CancelingRing ring = new(environment.RingHost);
        using IoUringStorageFile file = IoUringStorageFile.Open(ring, directory.Combine("file.dat"), environment.NoteCanceled);
        for (int i = 0; i < 16; i++)
            file.Write(Enumerable.Repeat((byte)i, 4096).ToArray(), i * 4096L);

        file.Write(B("tail"), 16 * 4096);

        // Just written, so in the page cache: nothing goes to the ring.
        int submitted = ring.Submitted;
        AssertBatch(file);
        Assert.Equal(submitted, ring.Submitted);

        file.PageCacheFirst = false;
        AssertBatch(file);
        Assert.Equal(submitted + 17, ring.Submitted);

        ring.Fail(-ECANCELED);
        AssertBatch(file);
        Assert.Equal(1, environment.CanceledOperations);

        ring.Fail(-5);
        Assert.Contains("errno 5", Assert.Throws<IOException>(() => AssertBatch(file)).Message);
        AssertBatch(file);
    }

    // The failure that was seen: a cancelled write in Open, there of FORMAT. Here every fifth
    // write of a database's life is cancelled, through open, writes, a flush, a compaction and a
    // reopen, and every read returns what was written.
    [Fact]
    public void A_database_whose_writes_the_ring_cancels_reads_back_what_it_wrote_after_a_reopen()
    {
        if (!IoUringStorageEnvironment.IsSupported())
            return;

        using IoUringStorageEnvironment environment = new();
        using TempDirectory directory = new();
        CancelingRing ring = new(environment.RingHost) { CancelEvery = 5 };
        DbOptions options = new() { Environment = new RingEnvironment(ring, environment) };
        string path = directory.Combine("db");

        using (SolariumDb db = SolariumDb.Open(path, options))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 200; i++)
                    db.Put(B($"k{i:D3}"), B($"v{round}-{i}"), options: WriteOptions.Synced);

                db.Flush();
            }

            db.CompactRange(null, null);
        }

        using (SolariumDb db = SolariumDb.Open(path, options))
        {
            for (int i = 0; i < 200; i++)
                Assert.Equal($"v2-{i}", S(db.Get(B($"k{i:D3}"))));
        }

        Assert.True(environment.CanceledOperations > 10, $"{environment.CanceledOperations} operations were cancelled");
    }

    // Sixteen pages of their own byte and a tail of 4 bytes, read in one batch: the last request reaches past the end.
    private static void AssertBatch(IStorageFile file)
    {
        byte[][] buffers = [.. Enumerable.Range(0, 17).Select(_ => new byte[4096])];
        ReadRequest[] requests = [.. buffers.Select((buffer, i) => new ReadRequest(file, buffer, i * 4096L))];
        IoUringStorageFile.MultiRead(requests);

        for (int i = 0; i < 16; i++)
        {
            Assert.Equal(4096, requests[i].BytesRead);
            Assert.All(buffers[i], b => Assert.Equal((byte)i, b));
        }

        Assert.Equal(4, requests[16].BytesRead);
        Assert.Equal("tail", S(buffers[16].AsSpan(0, 4)));
    }

    /// <summary>Forwards to a reactor, but completes the operations a test picks with an error instead of submitting them.</summary>
    private sealed class CancelingRing(IRingHost inner) : IRingHost
    {
        private int _failNext;
        private int _operations;
        private int _submitted;

        /// <summary>When positive, every this-many-th read or write submitted completes with <c>-ECANCELED</c>.</summary>
        public int CancelEvery { get; init; }

        /// <summary>The reads and writes passed to the reactor.</summary>
        public int Submitted => Volatile.Read(ref _submitted);

        /// <summary>Completes the next read or write with <paramref name="result"/>, a negated errno.</summary>
        public void Fail(int result) => _failNext = result;

        public void SubmitRead(int fd, nint buffer, int length, long offset, IRingCompletion completion)
        {
            if (!Failed(completion))
            {
                Interlocked.Increment(ref _submitted);
                inner.SubmitRead(fd, buffer, length, offset, completion);
            }
        }

        public void SubmitWrite(int fd, nint buffer, int length, long offset, IRingCompletion completion)
        {
            if (!Failed(completion))
            {
                Interlocked.Increment(ref _submitted);
                inner.SubmitWrite(fd, buffer, length, offset, completion);
            }
        }

        public void SubmitConnect(int fd, nint sockaddr, int length, IRingCompletion completion) => inner.SubmitConnect(fd, sockaddr, length, completion);

        public void SubmitSend(int fd, nint buffer, int length, IRingCompletion completion) => inner.SubmitSend(fd, buffer, length, completion);

        public void SubmitRecv(int fd, nint buffer, int length, IRingCompletion completion) => inner.SubmitRecv(fd, buffer, length, completion);

        public void SubmitTimeout(long nanoseconds, IRingCompletion completion) => inner.SubmitTimeout(nanoseconds, completion);

        // Callers on several threads (writers, the flush and compaction threads) share the count.
        private bool Failed(IRingCompletion completion)
        {
            int failure = Interlocked.Exchange(ref _failNext, 0);
            if (failure == 0 && CancelEvery > 0 && Interlocked.Increment(ref _operations) % CancelEvery == 0)
                failure = -ECANCELED;

            if (failure == 0)
                return false;

            completion.Complete(failure);
            return true;
        }
    }

    /// <summary>The io_uring environment, with its files on a wrapped ring.</summary>
    private sealed class RingEnvironment(IRingHost ring, IoUringStorageEnvironment environment) : IStorageEnvironment
    {
        public string Name => $"canceling {environment.Name}";

        public IStorageFile OpenFile(string path) => IoUringStorageFile.Open(ring, path, environment.NoteCanceled);
    }
}
