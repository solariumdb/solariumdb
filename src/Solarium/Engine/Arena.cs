using System.Runtime.InteropServices;

namespace Solarium.Engine;

/// <summary>
/// The native memory of one memtable, as RocksDB's <c>ConcurrentArena</c>: allocations are carved from chunks of
/// an <see cref="ArenaChunkPool"/> and never freed one by one; <see cref="Free"/> gives every chunk back at once,
/// when no reader can reach the memtable. An allocation never moves, so its address is a stable link.
/// </summary>
/// <remarks>
/// <para>One writer at a time allocates with <see cref="Allocate"/>, from the first shard with no atomic operation;
/// several writers at once allocate with <see cref="AllocateConcurrently"/>, each from the shard of its processor,
/// with one atomic add, so writers on different processors fill different chunks. A shard whose chunk is full
/// takes a new one under the shard's lock. Never both at once: the database separates the two with the full
/// fences of its write groups.</para>
/// <para>An allocation larger than a quarter of a chunk gets a block of its own, as RocksDB's
/// <c>Arena::AllocateFallback</c>, so a large value wastes no chunk.</para>
/// <para>The arena gives its memory back when the garbage collector finalizes it, if <see cref="Free"/> was never
/// called: the memtable that holds it was unreachable, so no reader could reach its memory either. A method that
/// reads the memory keeps its memtable reachable until it is done (<see cref="GC.KeepAlive"/>).</para>
/// </remarks>
internal sealed unsafe class Arena
{
    /// <summary>The largest allocation carved from a chunk; a larger one gets a block of its own.</summary>
    public const int LargeAllocation = ArenaChunkPool.ChunkSize / 4;

    private static readonly int ShardCount = Math.Clamp(Environment.ProcessorCount, 1, 64);

    private readonly ArenaChunkPool _pool;
    private readonly Shard[] _shards = new Shard[ShardCount];
    private readonly object _lock = new();
    private readonly List<nint> _chunks = [];   // under _lock
    private readonly List<nint> _large = [];    // under _lock
    private long _allocatedBytes;               // Interlocked: chunks and large blocks held
    private int _freed;                         // Interlocked

    /// <summary>An empty arena whose chunks come from <paramref name="pool"/>.</summary>
    public Arena(ArenaChunkPool pool)
    {
        _pool = pool;
        for (int i = 0; i < _shards.Length; i++)
            _shards[i] = new Shard();
    }

    ~Arena() => Release();

    /// <summary>The bytes of the chunks and large blocks the arena holds, used or not.</summary>
    public long AllocatedBytes => Interlocked.Read(ref _allocatedBytes);

    /// <summary>True once <see cref="Free"/> gave the memory back.</summary>
    public bool IsFreed => Volatile.Read(ref _freed) != 0;

    /// <summary>
    /// <paramref name="size"/> bytes, a multiple of 8, aligned to 8 and uninitialized. One writer at a time, with no
    /// concurrent one.
    /// </summary>
    public byte* Allocate(int size)
    {
        if (size > LargeAllocation)
            return AllocateLarge(size);

        Shard shard = _shards[0];
        Chunk? chunk = shard.Current;
        if (chunk is null || chunk.Used + size > ArenaChunkPool.ChunkSize)
        {
            chunk = new Chunk(NewChunk());
            shard.Current = chunk;
        }

        byte* allocation = (byte*)chunk.Start + chunk.Used;
        chunk.Used += size;
        return allocation;
    }

    /// <summary><paramref name="size"/> bytes as <see cref="Allocate"/> gives them, while other threads allocate too.</summary>
    public byte* AllocateConcurrently(int size)
    {
        if (size > LargeAllocation)
            return AllocateLarge(size);

        Shard shard = _shards[Thread.GetCurrentProcessorId() % _shards.Length];
        while (true)
        {
            // The add claims the bytes of the chunk this thread read, whichever chunk the shard holds by then:
            // the count belongs to the chunk, not to the shard. Past the end, the chunk is full for everyone.
            Chunk? chunk = Volatile.Read(ref shard.Current);
            if (chunk is not null)
            {
                int end = Interlocked.Add(ref chunk.Used, size);
                if (end <= ArenaChunkPool.ChunkSize)
                    return (byte*)chunk.Start + end - size;
            }

            lock (shard)
            {
                if (ReferenceEquals(shard.Current, chunk))
                    Volatile.Write(ref shard.Current, new Chunk(NewChunk()));
            }
        }
    }

    /// <summary>
    /// Gives every chunk back to the pool and frees every large block, once: no reader can reach the memory any more.
    /// </summary>
    public void Free()
    {
        Release();
        GC.SuppressFinalize(this);
    }

    private void Release()
    {
        if (Interlocked.Exchange(ref _freed, 1) != 0)
            return;

        nint[] chunks;
        nint[] large;
        lock (_lock)
        {
            chunks = [.. _chunks];
            large = [.. _large];
            _chunks.Clear();
            _large.Clear();
        }

        foreach (nint chunk in chunks)
            _pool.Return(chunk);

        foreach (nint block in large)
            NativeMemory.Free((void*)block);

        Interlocked.Exchange(ref _allocatedBytes, 0);
    }

    private nint NewChunk()
    {
        nint chunk = _pool.Rent();
        lock (_lock)
            _chunks.Add(chunk);

        Interlocked.Add(ref _allocatedBytes, ArenaChunkPool.ChunkSize);
        return chunk;
    }

    private byte* AllocateLarge(int size)
    {
        nint block = (nint)NativeMemory.Alloc((nuint)size);
        lock (_lock)
            _large.Add(block);

        Interlocked.Add(ref _allocatedBytes, size);
        return (byte*)block;
    }

    private sealed class Shard
    {
        public Chunk? Current;
    }

    private sealed class Chunk(nint start)
    {
        public readonly nint Start = start;
        public int Used;   // Interlocked while writers allocate at once; past ChunkSize once full
    }
}
