using System.Diagnostics.CodeAnalysis;
using Solarium.Engine;

namespace Solarium;

/// <summary>
/// An LRU cache with a capacity in bytes, as RocksDB's <c>NewLRUCache</c>, for the blocks of SSTables
/// (<see cref="ColumnFamilyOptions.BlockCache"/>: the index and filter blocks with
/// <see cref="ColumnFamilyOptions.CacheIndexAndFilterBlocks"/>, and the data blocks with
/// <see cref="ColumnFamilyOptions.CacheDataBlocks"/>) and, through a <see cref="WriteBufferManager"/>
/// created with it, for the memory of memtables, so one capacity bounds all of them. The host creates it and may
/// share it across databases and column families; it holds no resource to dispose.
/// </summary>
/// <remarks>
/// The capacity is soft, as RocksDB's default <c>strict_capacity_limit = false</c>: an insert evicts the least
/// recently used entries that nothing pins, and when only pinned entries are left the cache stays over its
/// capacity instead of failing a read. The cache is split into shards by key, each with its own lock and an
/// equal part of the capacity. As in RocksDB, a part of each shard, <see cref="HighPriorityPoolRatio"/>, is a
/// high-priority pool for the index and filter blocks and for the entries read more than once: an eviction
/// takes the other entries first, so the blocks of a long scan, read once, cannot push them out.
/// </remarks>
public sealed class Cache
{
    // RocksDB's default sharding: a shard of at least 512 KiB, and at most 64 shards.
    private const long MinShardBytes = 512 * 1024;
    private const int MaxDefaultShardBits = 6;
    private const int MaxShardBits = 20;

    private static long s_lastOwner;

    private readonly CacheShard[] _shards;
    private readonly double _highPriorityPoolRatio;
    private readonly int _shardShift;
    private long _capacity;

    /// <summary>
    /// A cache of <paramref name="capacity"/> bytes in 2^<paramref name="shardBits"/> shards; -1, the default,
    /// picks RocksDB's count: as many shards as keep each at 512 KiB or more, up to 64.
    /// <paramref name="highPriorityPoolRatio"/> is the share of each shard that the high-priority pool may hold,
    /// as RocksDB's <c>high_pri_pool_ratio</c> and with its default, 0.5; 0 makes the cache one plain LRU list.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="capacity"/> is negative, <paramref name="shardBits"/> is below -1 or above 20, or
    /// <paramref name="highPriorityPoolRatio"/> is outside 0 to 1.
    /// </exception>
    public Cache(long capacity, int shardBits = -1, double highPriorityPoolRatio = 0.5)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        ArgumentOutOfRangeException.ThrowIfLessThan(shardBits, -1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(shardBits, MaxShardBits);
        if (!(highPriorityPoolRatio >= 0 && highPriorityPoolRatio <= 1))
            throw new ArgumentOutOfRangeException(nameof(highPriorityPoolRatio), highPriorityPoolRatio, "The high-priority pool's share must be from 0 to 1.");

        if (shardBits == -1)
            shardBits = DefaultShardBits(capacity);

        _capacity = capacity;
        _highPriorityPoolRatio = highPriorityPoolRatio;
        _shards = new CacheShard[1 << shardBits];
        _shardShift = 64 - shardBits;
        for (int i = 0; i < _shards.Length; i++)
            _shards[i] = new CacheShard(ShardCapacity(capacity, _shards.Length), highPriorityPoolRatio);
    }

    /// <summary>The capacity in bytes, as RocksDB's <c>GetCapacity</c>.</summary>
    public long Capacity => Interlocked.Read(ref _capacity);

    /// <summary>The bytes the cached entries are charged, pinned ones included, as RocksDB's <c>GetUsage</c>.</summary>
    public long Usage
    {
        get
        {
            long usage = 0;
            foreach (CacheShard shard in _shards)
                usage += shard.Usage;

            return usage;
        }
    }

    /// <summary>
    /// The bytes of the entries that cannot be evicted, as RocksDB's <c>GetPinnedUsage</c>: the index and filter
    /// blocks of level 0 tables under <see cref="ColumnFamilyOptions.PinL0FilterAndIndexBlocksInCache"/>, and
    /// the memtable memory a <see cref="WriteBufferManager"/> charges.
    /// </summary>
    public long PinnedUsage
    {
        get
        {
            long usage = 0;
            foreach (CacheShard shard in _shards)
                usage += shard.PinnedUsage;

            return usage;
        }
    }

    /// <summary>The share of each shard that the high-priority pool may hold, as RocksDB's <c>high_pri_pool_ratio</c>.</summary>
    public double HighPriorityPoolRatio => _highPriorityPoolRatio;

    /// <summary>The bytes of the entries in the high-priority pool: unpinned, and inserted at high priority or read again.</summary>
    internal long HighPriorityPoolUsage
    {
        get
        {
            long usage = 0;
            foreach (CacheShard shard in _shards)
                usage += shard.HighPoolUsage;

            return usage;
        }
    }

    /// <summary>The entries inserted so far, pinned ones included: a diagnostic that a miss read a table's blocks into the cache.</summary>
    internal long InsertCount
    {
        get
        {
            long inserts = 0;
            foreach (CacheShard shard in _shards)
                inserts += shard.InsertCount;

            return inserts;
        }
    }

    /// <summary>The number of shards.</summary>
    public int ShardCount => _shards.Length;

    /// <summary>Changes the capacity, as RocksDB's <c>SetCapacity</c>; a smaller one evicts at once.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
    public void SetCapacity(long capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        Interlocked.Exchange(ref _capacity, capacity);
        foreach (CacheShard shard in _shards)
            shard.SetCapacity(ShardCapacity(capacity, _shards.Length));
    }

    /// <summary>A number no other database or memtable budget of the process has, to key its entries by.</summary>
    internal static long NewOwner() => Interlocked.Increment(ref s_lastOwner);

    /// <summary>
    /// The value cached under <paramref name="key"/>, now the most recently used, in the high-priority pool; false on a
    /// miss. A <see cref="NativeBlock"/> comes with a reference that the caller releases.
    /// </summary>
    internal bool TryGet(CacheKey key, [NotNullWhen(true)] out object? value) => Shard(key).TryGet(key, out value);

    /// <summary>Caches <paramref name="value"/>, or returns the value already cached under <paramref name="key"/>; see <see cref="CacheShard.Insert"/>.</summary>
    internal object Insert(CacheKey key, object value, long charge, bool highPriority = false) => Shard(key).Insert(key, value, charge, highPriority);

    /// <summary>
    /// A native copy of a data block's <paramref name="entries"/> to cache under <paramref name="key"/> with
    /// <see cref="Insert"/>, from the memory of the key's shard, with one reference, the caller's.
    /// </summary>
    internal NativeBlock NewBlock(CacheKey key, ReadOnlySpan<byte> entries) => Shard(key).Memory.Allocate(entries);

    /// <summary>The native blocks not freed yet, cached or held by readers, and their bytes; for tests.</summary>
    internal (long Count, long Bytes) NativeBlocks
    {
        get
        {
            long count = 0;
            long bytes = 0;
            foreach (CacheShard shard in _shards)
            {
                count += shard.Memory.Count;
                bytes += shard.Memory.Bytes;
            }

            return (count, bytes);
        }
    }

    /// <summary>Caches and pins <paramref name="value"/>, or pins the entry already cached; see <see cref="CacheShard.InsertPinned"/>.</summary>
    internal CacheEntry InsertPinned(CacheKey key, object value, long charge, bool highPriority = false) => Shard(key).InsertPinned(key, value, charge, highPriority);

    /// <summary>Drops one pin of an entry that <see cref="InsertPinned"/> returned.</summary>
    internal void Unpin(CacheEntry entry) => Shard(entry.Key).Unpin(entry);

    /// <summary>Removes the entry of <paramref name="key"/>, if any, pinned or not.</summary>
    internal void Erase(CacheKey key) => Shard(key).Erase(key);

    /// <summary>
    /// Removes every entry of <paramref name="owner"/>, pinned or not, as a database that closes does with the
    /// blocks of its tables, so a cache shared across databases holds nothing of a closed one. It takes each
    /// shard's lock in turn, for one pass over that shard's entries.
    /// </summary>
    internal void EraseOwner(long owner)
    {
        foreach (CacheShard shard in _shards)
            shard.EraseOwner(owner);
    }

    private CacheShard Shard(CacheKey key)
    {
        if (_shards.Length == 1)
            return _shards[0];

        // A 64-bit mix of every part, so the tables of one database and the partitions of one table spread over
        // every shard.
        ulong hash = ((ulong)key.Owner * 0x9E3779B97F4A7C15UL) ^ ((ulong)key.Number * 0xC2B2AE3D27D4EB4FUL) ^ (ulong)key.Part;
        hash ^= hash >> 31;
        hash *= 0xBF58476D1CE4E5B9UL;
        hash ^= hash >> 29;
        return _shards[(int)(hash >> _shardShift)];
    }

    private static int DefaultShardBits(long capacity)
    {
        int bits = 0;
        while (bits < MaxDefaultShardBits && capacity / (1L << (bits + 1)) >= MinShardBytes)
            bits++;

        return bits;
    }

    private static long ShardCapacity(long capacity, int shards) => (capacity + shards - 1) / shards;
}
