using System.Buffers;
using System.Runtime.InteropServices;
using ioxide;
using Solarium.Storage;

namespace Solarium.IoUring;

/// <summary>
/// An <see cref="IStorageFile"/> that performs its writes on an io_uring reactor and blocks the caller until
/// each completes, and reads with pread on the calling thread.
/// </summary>
/// <remarks>
/// A write that completes with <c>-ECANCELED</c> is done again with pwrite on the calling thread. The
/// kernel fails a request with it when io-wq cancels work it never ran, as when it cannot start its first
/// worker; positional I/O is idempotent, so writing the same bytes at the same offset again is safe even
/// if part of it ran. A synchronous retry cannot be cancelled the
/// same way, and it does not depend on the reactor. <see cref="IoUringStorageEnvironment.CanceledOperations"/>
/// counts them.
/// </remarks>
internal sealed unsafe partial class IoUringStorageFile : IStorageFile
{
    // Linux open(2) flags and errno values; the same on x86-64 and arm64.
    private const int O_RDWR = 0x2;
    private const int O_CREAT = 0x40;
    private const int Mode0644 = 420;
    private const int EINTR = 4;
    private const int EAGAIN = 11;
    private const int EOPNOTSUPP = 95;
    private const int ECANCELED = 125;
    private const int RWF_NOWAIT = 0x8;
    private const uint SyncFileRangeWrite = 2;

    private readonly IRingHost _ring;
    private readonly int _fd;
    private readonly Action _canceled;
    private volatile bool _noWaitRefused;   // the file system does not support RWF_NOWAIT
    private volatile bool _pageCacheFirst = true;
    private int _disposed;

    private IoUringStorageFile(IRingHost ring, int fd, Action canceled)
    {
        _ring = ring;
        _fd = fd;
        _canceled = canceled;
    }

    /// <summary>
    /// Opens or creates <paramref name="path"/> for reading and writing; throws <see cref="IOException"/> with the
    /// errno on failure. <paramref name="canceled"/> runs on the calling thread for each operation that the
    /// ring cancelled and the file did again synchronously.
    /// </summary>
    public static IoUringStorageFile Open(IRingHost ring, string path, Action canceled)
    {
        int fd = Native.open(path, O_RDWR | O_CREAT | Native.O_CLOEXEC, Mode0644);
        return fd >= 0 ? new IoUringStorageFile(ring, fd, canceled) : throw Errno($"open '{path}'", Marshal.GetLastPInvokeError());
    }

    /// <inheritdoc />
    public long GetLength() => Native.FileLength(_fd);

    /// <summary>
    /// True, the default, while a batch read (<see cref="MultiRead"/>) first tries the page cache on the calling
    /// thread; false sends every read of a batch to the ring, as for reads the page cache misses. Tests set it.
    /// </summary>
    internal bool PageCacheFirst
    {
        get => _pageCacheFirst;
        set => _pageCacheFirst = value;
    }

    /// <summary>
    /// Reads with pread on the calling thread, not through the ring. A table read is one blocking read whose
    /// bytes are usually in the page cache, where the kernel completes it inline: a hand-off to the reactor
    /// thread and back costs several times the read, and the reactor runs every caller's reads one at a time.
    /// </summary>
    public int Read(Span<byte> buffer, long offset)
    {
        fixed (byte* pointer = buffer)
        {
            int result = Synchronously(write: false, pointer, buffer.Length, offset);
            return result >= 0 ? result : throw Errno("read", -result);
        }
    }

    /// <summary>
    /// Reads a batch of requests, the work of <see cref="IoUringStorageEnvironment.MultiRead"/>. A read that the page
    /// cache holds is done on the calling thread, by preadv2 with RWF_NOWAIT, which fails with EAGAIN rather than wait
    /// for the drive. The others go to the ring together, so the drive serves them at once, and the call waits for
    /// all of them. A read that the ring cancels, or that it completes short of the end of the file, is finished with
    /// pread. A request on a file of another environment is read through its own <see cref="IStorageFile.Read"/>.
    /// </summary>
    /// <exception cref="IOException">A read failed; every read that went to the ring has completed.</exception>
    public static void MultiRead(Span<ReadRequest> requests)
    {
        ReadBatch batch = ReadBatch.ForThisThread(requests.Length);
        try
        {
            for (int i = 0; i < requests.Length; i++)
            {
                ref ReadRequest request = ref requests[i];
                if (request.File is not IoUringStorageFile file)
                {
                    request.BytesRead = ReadFully(request.File, request.Buffer.Span, request.Offset);
                    continue;
                }

                // What the page cache holds on this thread, and the rest from the ring.
                int cached = file.ReadCached(request.Buffer.Span, request.Offset, out bool finished);
                if (finished)
                    request.BytesRead = cached;
                else
                    batch.Submit(file, i, request.Buffer, cached, request.Offset);
            }
        }
        finally
        {
            // Every read the ring holds must complete before its buffer is unpinned, even when a read above threw.
            batch.WaitAll();
        }

        for (int s = 0; s < batch.Count; s++)
        {
            ReadSlot slot = batch.Slots[s];
            ref ReadRequest request = ref requests[slot.Request];
            request.BytesRead = slot.File.FinishRead(request.Buffer.Span, request.Offset, slot.Done, slot.Result);
        }
    }

    /// <inheritdoc />
    public void Write(ReadOnlySpan<byte> buffer, long offset)
    {
        fixed (byte* pointer = buffer)
        {
            int written = 0;
            while (written < buffer.Length)
            {
                BlockingCompletion op = BlockingCompletion.ForThisThread();
                _ring.SubmitWrite(_fd, (nint)(pointer + written), buffer.Length - written, offset + written, op);
                int result = op.Wait();
                if (result == -ECANCELED)
                {
                    _canceled();
                    result = Synchronously(write: true, pointer + written, buffer.Length - written, offset + written);
                }

                if (result <= 0)
                    throw result == 0 ? new IOException("write wrote 0 bytes") : Errno("write", -result);

                written += result;
            }
        }
    }

    /// <summary>Calls fdatasync on the calling thread, because the reactor has no fsync operation.</summary>
    public void Flush()
    {
        if (fdatasync(_fd) != 0)
            throw Errno("fdatasync", Marshal.GetLastPInvokeError());
    }

    /// <summary>
    /// Calls <c>sync_file_range(SYNC_FILE_RANGE_WRITE)</c> on the calling thread, because the reactor
    /// has no such operation: the writeback of the range starts, and the call does not wait for it.
    /// </summary>
    public void SyncRange(long offset, long length)
    {
        if (sync_file_range(_fd, offset, length, SyncFileRangeWrite) != 0)
            throw Errno("sync_file_range", Marshal.GetLastPInvokeError());
    }

    /// <inheritdoc />
    public void SetLength(long length)
    {
        if (ftruncate(_fd, length) != 0)
            throw Errno("ftruncate", Marshal.GetLastPInvokeError());
    }

    /// <summary>Closes the file descriptor. Safe to call more than once.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            Native.close(_fd);
    }

    // The bytes the page cache holds from offset on, read on this thread by preadv2 with RWF_NOWAIT, which fails
    // with EAGAIN rather than wait for the drive. finished is true when nothing is left to read: the buffer is full or
    // the file ended. A file system that does not support RWF_NOWAIT, as tmpfs, is read with pread on this thread,
    // as a single read is: through the ring, reads that never wait for the drive would run one at a time on the
    // reactor thread.
    private int ReadCached(Span<byte> buffer, long offset, out bool finished)
    {
        finished = false;
        if (!_pageCacheFirst)
            return 0;

        fixed (byte* pointer = buffer)
        {
            int total = 0;
            while (total < buffer.Length)
            {
                nint result;
                if (_noWaitRefused)
                {
                    result = Synchronously(write: false, pointer + total, buffer.Length - total, offset + total);
                    if (result < 0)
                        throw Errno("read", (int)-result);
                }
                else
                {
                    Iovec vector = new() { Base = pointer + total, Length = (nuint)(buffer.Length - total) };
                    result = preadv2(_fd, &vector, 1, offset + total, RWF_NOWAIT);
                    if (result < 0)
                    {
                        int errno = Marshal.GetLastPInvokeError();
                        if (errno == EAGAIN)
                            return total;

                        if (errno == EOPNOTSUPP)
                            _noWaitRefused = true;
                        else if (errno != EINTR)
                            throw Errno("read", errno);

                        continue;
                    }
                }

                if (result == 0)
                    break;

                total += (int)result;
            }

            finished = true;
            return total;
        }
    }

    // The bytes a batch read ends with, from what was read before the ring (done) and the ring's result: a
    // cancelled read, or one the ring completed short, goes on with pread until the buffer is full or the file ends.
    private int FinishRead(Span<byte> buffer, long offset, int done, int result)
    {
        if (result == -ECANCELED)
        {
            _canceled();
            result = 0;
        }
        else if (result < 0)
        {
            throw Errno("read", -result);
        }
        else if (result == 0)
        {
            return done;
        }

        int total = done + result;
        fixed (byte* pointer = buffer)
        {
            while (total < buffer.Length)
            {
                int read = Synchronously(write: false, pointer + total, buffer.Length - total, offset + total);
                if (read < 0)
                    throw Errno("read", -read);

                if (read == 0)
                    break;

                total += read;
            }
        }

        return total;
    }

    // Read until the buffer is full or the file ends, through the file's own Read.
    private static int ReadFully(IStorageFile file, Span<byte> buffer, long offset)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = file.Read(buffer[total..], offset + total);
            if (read == 0)
                break;

            total += read;
        }

        return total;
    }

    // One pread or pwrite on this thread, again after EINTR; the count, or the negated errno as a ring reports it.
    private int Synchronously(bool write, byte* buffer, int length, long offset)
    {
        while (true)
        {
            nint result = write ? pwrite(_fd, buffer, (nuint)length, offset) : pread(_fd, buffer, (nuint)length, offset);
            if (result >= 0)
                return (int)result;

            int errno = Marshal.GetLastPInvokeError();
            if (errno != EINTR)
                return -errno;
        }
    }

    private static IOException Errno(string operation, int errno)
    {
        return new IOException($"{operation} failed: {Marshal.GetPInvokeErrorMessage(errno)} (errno {errno})");
    }

    [LibraryImport("libc", SetLastError = true)]
    private static partial int fdatasync(int fd);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int ftruncate(int fd, long length);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int sync_file_range(int fd, long offset, long length, uint flags);

    [LibraryImport("libc", SetLastError = true)]
    private static partial nint pread(int fd, byte* buffer, nuint count, long offset);

    [LibraryImport("libc", SetLastError = true)]
    private static partial nint pwrite(int fd, byte* buffer, nuint count, long offset);

    [LibraryImport("libc", SetLastError = true)]
    private static partial nint preadv2(int fd, Iovec* vectors, int count, long offset, int flags);

    // struct iovec.
    private struct Iovec
    {
        public byte* Base;
        public nuint Length;
    }

    // One read of a batch in the ring: the request it serves, the bytes read before it, and its result.
    private sealed class ReadSlot(ReadBatch batch) : IRingCompletion
    {
        private MemoryHandle _pin;

        public IoUringStorageFile File { get; private set; } = null!;

        public int Request { get; private set; }

        public int Done { get; private set; }

        public int Result { get; private set; }

        // Pins the buffer until Release and returns where the read writes, past the bytes already read.
        public byte* Arm(IoUringStorageFile file, int request, Memory<byte> buffer, int done)
        {
            File = file;
            Request = request;
            Done = done;
            Result = 0;
            _pin = buffer.Pin();
            return (byte*)_pin.Pointer + done;
        }

        public void Release() => _pin.Dispose();

        public void Complete(int result)
        {
            // The result is written before the batch's count falls, whose barrier publishes it to the waiter.
            Result = result;
            batch.One();
        }
    }

    // The reads one thread has in the ring for a batch, and the event it waits on until they all complete. A thread
    // reads one batch at a time, so each thread reuses one batch and its slots.
    private sealed class ReadBatch
    {
        [ThreadStatic]
        private static ReadBatch? t_current;

        private readonly ManualResetEventSlim _done = new();
        private ReadSlot[] _slots = [];
        private int _remaining;   // Interlocked: the reads in the ring, plus one until WaitAll

        public ReadSlot[] Slots => _slots;

        public int Count { get; private set; }

        public static ReadBatch ForThisThread(int capacity)
        {
            ReadBatch batch = t_current ??= new ReadBatch();
            if (batch._slots.Length < capacity)
            {
                ReadSlot[] slots = new ReadSlot[Math.Max(capacity, batch._slots.Length * 2)];
                batch._slots.CopyTo(slots, 0);
                for (int i = batch._slots.Length; i < slots.Length; i++)
                    slots[i] = new ReadSlot(batch);

                batch._slots = slots;
            }

            batch.Count = 0;
            batch._remaining = 1;
            batch._done.Reset();
            return batch;
        }

        // Sends the rest of request's read, past the done bytes, to the file's ring.
        public void Submit(IoUringStorageFile file, int request, Memory<byte> buffer, int done, long offset)
        {
            ReadSlot slot = _slots[Count];
            byte* target = slot.Arm(file, request, buffer, done);
            Count++;
            Interlocked.Increment(ref _remaining);
            try
            {
                file._ring.SubmitRead(file._fd, (nint)target, buffer.Length - done, offset + done, slot);
            }
            catch
            {
                // Never submitted, so no completion will count it down.
                Count--;
                slot.Release();
                One();
                throw;
            }
        }

        public void One()
        {
            if (Interlocked.Decrement(ref _remaining) == 0)
                _done.Set();
        }

        // Waits for every read in the ring, then unpins their buffers.
        public void WaitAll()
        {
            One();
            _done.Wait();
            for (int s = 0; s < Count; s++)
                _slots[s].Release();
        }
    }

    // Completed on the reactor thread; waited for on the caller's. A thread has at most one operation in
    // flight, since it blocks until it completes, so each thread reuses one completion and its event rather
    // than allocating both for every write.
    private sealed class BlockingCompletion : IRingCompletion
    {
        [ThreadStatic]
        private static BlockingCompletion? t_current;

        private readonly ManualResetEventSlim _done = new();
        private int _result;

        public static BlockingCompletion ForThisThread() => t_current ??= new BlockingCompletion();

        public void Complete(int result)
        {
            // The result is written before Set, whose barrier publishes it to the waiter.
            _result = result;
            _done.Set();
        }

        public int Wait()
        {
            _done.Wait();
            _done.Reset();
            return _result;
        }
    }
}
