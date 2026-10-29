using System.Runtime.InteropServices;

namespace Solarium.Engine;

/// <summary>
/// Chunks of native memory of <see cref="ChunkSize"/> bytes for the arenas of memtables (<see cref="Arena"/>), reused
/// from one memtable to the next. The garbage collector never sees them: they count against no heap limit, and no
/// collection copies, marks or compacts them. A freed chunk is kept for the next memtable while the chunks held, in
/// arenas and here, stay within <see cref="RetainLimit"/>, and given back to the operating system past it, so a pool
/// whose limit is the memtable budget never holds more than the budget. A kept chunk's pages are mapped already,
/// so a memtable that fills it takes no page faults. Safe from any thread.
/// </summary>
/// <remarks>
/// A <see cref="WriteBufferManager"/> owns the pool of the databases that share it, with its cap as the limit. The
/// databases without one share <see cref="Shared"/>, whose limit is the sum of their families' memtable bytes: each
/// adds its own at the open and takes it away at the close, so a database opened after another closed reuses its
/// chunks. A pool that is never disposed gives its kept chunks back when the garbage collector finalizes it, which
/// is only after every arena that took a chunk from it is unreachable.
/// </remarks>
internal sealed unsafe class ArenaChunkPool : IDisposable
{
    /// <summary>The size of a chunk: small enough that a small memtable wastes little, large enough that a chunk holds hundreds of entries.</summary>
    public const int ChunkSize = 64 * 1024;

    private readonly object _lock = new();
    private readonly Stack<nint> _kept = new();   // under _lock
    private long _retainLimit;   // under _lock
    private long _rented;        // under _lock: chunks in arenas
    private long _allocated;     // Interlocked: chunks taken from the operating system and not given back, for tests
    private bool _disposed;      // under _lock

    /// <summary>A pool that keeps freed chunks while the chunks held stay within <paramref name="retainLimit"/> bytes.</summary>
    public ArenaChunkPool(long retainLimit)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retainLimit);
        _retainLimit = retainLimit;
    }

    ~ArenaChunkPool() => Release();

    /// <summary>
    /// The pool of the databases without a <see cref="WriteBufferManager"/>, and of the memtables that tests build
    /// alone; its limit is what the open databases added (<see cref="AddToRetainLimit"/>).
    /// </summary>
    public static ArenaChunkPool Shared { get; } = new(0);

    /// <summary>The bytes the pool may hold, in arenas and kept, before it gives a freed chunk back.</summary>
    public long RetainLimit
    {
        get
        {
            lock (_lock)
                return _retainLimit;
        }

        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            SetRetainLimit(_ => value);
        }
    }

    /// <summary>Adds <paramref name="bytes"/>, which may be negative, to <see cref="RetainLimit"/>, as a database that opens or closes does.</summary>
    public void AddToRetainLimit(long bytes) =>
        SetRetainLimit(limit => bytes > 0 && limit > long.MaxValue - bytes ? long.MaxValue : Math.Max(0, limit + bytes));

    /// <summary>The bytes of the chunks that arenas hold.</summary>
    public long RentedBytes
    {
        get
        {
            lock (_lock)
                return _rented * ChunkSize;
        }
    }

    /// <summary>The bytes of the freed chunks the pool keeps for the next arena.</summary>
    public long KeptBytes
    {
        get
        {
            lock (_lock)
                return (long)_kept.Count * ChunkSize;
        }
    }

    /// <summary>The bytes of the chunks taken from the operating system and not given back, held or kept.</summary>
    internal long AllocatedBytes => Interlocked.Read(ref _allocated) * ChunkSize;

    /// <summary>A chunk of <see cref="ChunkSize"/> bytes, uninitialized: a kept one, or a new one.</summary>
    /// <exception cref="OutOfMemoryException">The operating system has no memory for a new chunk.</exception>
    public nint Rent()
    {
        lock (_lock)
        {
            _rented++;
            if (_kept.Count > 0)
                return _kept.Pop();
        }

        try
        {
            nint chunk = (nint)NativeMemory.Alloc(ChunkSize);
            Interlocked.Increment(ref _allocated);
            return chunk;
        }
        catch
        {
            lock (_lock)
                _rented--;

            throw;
        }
    }

    /// <summary>
    /// Takes back a chunk that <see cref="Rent"/> gave, which no reader can reach any more: it is kept for the next
    /// arena, or given back to the operating system when the pool is at its limit or disposed.
    /// </summary>
    public void Return(nint chunk)
    {
#if DEBUG
        // A read of a freed entry finds this pattern instead of the entry it expected.
        new Span<byte>((void*)chunk, ChunkSize).Fill(0xDD);
#endif
        lock (_lock)
        {
            _rented--;
            if (!_disposed && (_rented + _kept.Count + 1) * (long)ChunkSize <= _retainLimit)
            {
                _kept.Push(chunk);
                return;
            }
        }

        FreeChunk(chunk);
    }

    /// <summary>Gives the kept chunks back to the operating system; a chunk returned after this is given back at once.</summary>
    public void Dispose()
    {
        Release();
        GC.SuppressFinalize(this);
    }

    // A new limit from the old one, and the kept chunks past it given back.
    private void SetRetainLimit(Func<long, long> limit)
    {
        List<nint> surplus = [];
        lock (_lock)
        {
            _retainLimit = limit(_retainLimit);
            while (_kept.Count > 0 && (_rented + _kept.Count) * (long)ChunkSize > _retainLimit)
                surplus.Add(_kept.Pop());
        }

        foreach (nint chunk in surplus)
            FreeChunk(chunk);
    }

    private void Release()
    {
        nint[] kept;
        lock (_lock)
        {
            _disposed = true;
            kept = [.. _kept];
            _kept.Clear();
        }

        foreach (nint chunk in kept)
            FreeChunk(chunk);
    }

    private void FreeChunk(nint chunk)
    {
        NativeMemory.Free((void*)chunk);
        Interlocked.Decrement(ref _allocated);
    }
}
