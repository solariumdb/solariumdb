using System.Diagnostics.CodeAnalysis;

namespace Solarium.Engine;

/// <summary>
/// One shard of a <see cref="Cache"/>: a table of entries and an LRU order of the ones no holder pins, under one
/// lock. The capacity is soft, as RocksDB's LRU cache with <c>strict_capacity_limit = false</c>: an insert over it
/// evicts unpinned entries from the least recently used end, and when only pinned entries are left the shard
/// stays over its capacity rather than fail the insert.
/// </summary>
/// <remarks>
/// The LRU order is two lists, as RocksDB's LRU cache with a <c>high_pri_pool_ratio</c>: the high-priority pool,
/// up to its share of the capacity, holds the entries inserted at high priority and every entry looked up again
/// since its insert, and the low-priority pool the rest. An entry that enters the LRU order goes to the newest
/// end of its pool. When the high-priority pool is over its share, its oldest entries move to the newest end of
/// the low-priority pool, so the two lists together stay one LRU order, and an eviction takes the oldest entry
/// of the low-priority pool first. Entries read once, such as the blocks of a long scan, therefore pass through
/// the low-priority pool without evicting the entries that are used again. With a share of 0, every entry is in
/// the low-priority pool: one plain LRU list.
/// <para>A value that is a <see cref="NativeBlock"/> is counted: the shard holds a reference while the entry is
/// cached and releases it when the entry leaves, and a lookup or an insert that returns the block takes one for
/// its caller, under the lock, which the caller releases.</para>
/// </remarks>
internal sealed class CacheShard
{
    private readonly object _lock = new();
    private readonly Dictionary<CacheKey, CacheEntry> _entries = [];
    private readonly double _highPoolRatio;
    private CacheEntry? _highNewest;   // the most recently used entry of the high-priority pool
    private CacheEntry? _highOldest;   // the next to move to the low-priority pool
    private CacheEntry? _lowNewest;    // the most recently used entry of the low-priority pool
    private CacheEntry? _lowOldest;    // the next to evict
    private long _capacity;
    private long _highPoolCapacity;
    private long _usage;
    private long _pinnedUsage;
    private long _highPoolUsage;
    private long _inserts;

    /// <summary>
    /// A shard that holds up to <paramref name="capacity"/> bytes of unpinned entries, of which up to
    /// <paramref name="highPoolRatio"/> (0 to 1) in the high-priority pool.
    /// </summary>
    public CacheShard(long capacity, double highPoolRatio)
    {
        Memory = new BlockMemory();
        _highPoolRatio = highPoolRatio;
        _capacity = capacity;
        _highPoolCapacity = HighPoolCapacity(capacity);
    }

    /// <summary>The native blocks of the shard's entries, and those that readers still hold after their eviction.</summary>
    public BlockMemory Memory { get; }

    /// <summary>The shard's capacity in bytes.</summary>
    public long Capacity
    {
        get
        {
            lock (_lock)
                return _capacity;
        }
    }

    /// <summary>The charges of the entries in the shard, pinned or not.</summary>
    public long Usage
    {
        get
        {
            lock (_lock)
                return _usage;
        }
    }

    /// <summary>The charges of the pinned entries in the shard.</summary>
    public long PinnedUsage
    {
        get
        {
            lock (_lock)
                return _pinnedUsage;
        }
    }

    /// <summary>The charges of the entries in the high-priority pool.</summary>
    public long HighPoolUsage
    {
        get
        {
            lock (_lock)
                return _highPoolUsage;
        }
    }

    /// <summary>The entries inserted into the shard so far, a key cached already not counted.</summary>
    public long InsertCount
    {
        get
        {
            lock (_lock)
                return _inserts;
        }
    }

    /// <summary>Sets the capacity, and the high-priority pool's share of it, and evicts down to it.</summary>
    public void SetCapacity(long capacity)
    {
        lock (_lock)
        {
            _capacity = capacity;
            _highPoolCapacity = HighPoolCapacity(capacity);
            MaintainHighPool();
            EvictToCapacity();
        }
    }

    /// <summary>
    /// The value of <paramref name="key"/>, which becomes the most recently used entry, in the high-priority pool;
    /// false when it is not cached. A <see cref="NativeBlock"/> comes with a reference that the caller releases.
    /// </summary>
    public bool TryGet(CacheKey key, [NotNullWhen(true)] out object? value)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(key, out CacheEntry? entry))
            {
                value = null;
                return false;
            }

            Touch(entry);
            value = Acquire(entry.Value);
            return true;
        }
    }

    /// <summary>
    /// Caches <paramref name="value"/> under <paramref name="key"/> and evicts down to the capacity, which can
    /// evict the new entry itself when it is larger than the room left. When the key is cached already, as
    /// when two readers missed at once, the cached value stays, counts as looked up, and is returned; otherwise
    /// <paramref name="value"/> is. <paramref name="highPriority"/> puts the new entry in the high-priority pool.
    /// A <see cref="NativeBlock"/> returned comes with a reference that the caller releases; the caller keeps its own
    /// reference on <paramref name="value"/> too, and the cache takes one of its own when it caches it.
    /// </summary>
    public object Insert(CacheKey key, object value, long charge, bool highPriority)
    {
        lock (_lock)
        {
            if (_entries.TryGetValue(key, out CacheEntry? existing))
            {
                Touch(existing);
                return Acquire(existing.Value);
            }

            // The cache's reference, and the one returned with the value.
            Acquire(value);
            Acquire(value);
            CacheEntry entry = new(key, value, charge, highPriority);
            _entries.Add(key, entry);
            _usage += charge;
            _inserts++;
            LinkNewest(entry);
            EvictToCapacity();
            return value;
        }
    }

    /// <summary>
    /// Caches <paramref name="value"/> under <paramref name="key"/>, or takes the entry cached there, and pins it:
    /// it stays until <see cref="Unpin"/> or <see cref="Erase"/>. Inserting evicts unpinned entries down to the
    /// capacity. <paramref name="highPriority"/> is the new entry's priority once it is unpinned.
    /// </summary>
    public CacheEntry InsertPinned(CacheKey key, object value, long charge, bool highPriority)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(key, out CacheEntry? entry))
            {
                Acquire(value);
                entry = new CacheEntry(key, value, charge, highPriority);
                _entries.Add(key, entry);
                _usage += charge;
                _inserts++;
            }
            else if (entry.Pins == 0)
            {
                Unlink(entry);
            }

            if (entry.Pins++ == 0)
                _pinnedUsage += entry.Charge;

            EvictToCapacity();
            return entry;
        }
    }

    /// <summary>
    /// Drops one pin of <paramref name="entry"/>. With the last pin gone, an entry still in the cache becomes the
    /// most recently used of its pool, and the shard evicts down to its capacity.
    /// </summary>
    public void Unpin(CacheEntry entry)
    {
        lock (_lock)
        {
            if (--entry.Pins > 0 || !entry.InCache)
                return;

            _pinnedUsage -= entry.Charge;
            LinkNewest(entry);
            EvictToCapacity();
        }
    }

    /// <summary>Removes <paramref name="key"/> and its charge, pinned or not. A pinned entry's holders keep its value.</summary>
    public void Erase(CacheKey key)
    {
        lock (_lock)
        {
            if (!_entries.Remove(key, out CacheEntry? entry))
                return;

            entry.InCache = false;
            _usage -= entry.Charge;
            if (entry.Pins > 0)
                _pinnedUsage -= entry.Charge;
            else
                Unlink(entry);

            Left(entry);
        }
    }

    /// <summary>
    /// Removes every entry whose key has <paramref name="owner"/>, pinned or not, as <see cref="Erase"/> does each,
    /// in one pass over the shard under its lock.
    /// </summary>
    public void EraseOwner(long owner)
    {
        lock (_lock)
        {
            // Removing from a dictionary does not end an enumeration of it.
            foreach (KeyValuePair<CacheKey, CacheEntry> pair in _entries)
            {
                if (pair.Key.Owner != owner)
                    continue;

                CacheEntry entry = pair.Value;
                _entries.Remove(pair.Key);
                entry.InCache = false;
                _usage -= entry.Charge;
                if (entry.Pins > 0)
                    _pinnedUsage -= entry.Charge;
                else
                    Unlink(entry);

                Left(entry);
            }
        }
    }

    private long HighPoolCapacity(long capacity) => (long)(capacity * _highPoolRatio);

    // A lookup of an entry: it now counts as used again, and an unpinned one moves to the newest end of the
    // high-priority pool.
    private void Touch(CacheEntry entry)
    {
        entry.Hit = true;
        if (entry.Pins > 0)
            return;

        Unlink(entry);
        LinkNewest(entry);
    }

    private void EvictToCapacity()
    {
        while (_usage > _capacity && (_lowOldest ?? _highOldest) is CacheEntry victim)
        {
            Unlink(victim);
            _entries.Remove(victim.Key);
            victim.InCache = false;
            _usage -= victim.Charge;
            Left(victim);
        }
    }

    // The reference that a lookup or an insert hands its caller with a native block, under the lock, while the
    // cache's own reference holds the block.
    private static object Acquire(object value)
    {
        (value as NativeBlock)?.AddReference();
        return value;
    }

    // An entry that left the cache drops the cache's reference on its native block; a reader that holds the block
    // keeps it until its own release.
    private static void Left(CacheEntry entry) => (entry.Value as NativeBlock)?.Release();

    // Moves the oldest entries of the high-priority pool to the newest end of the low-priority pool until the
    // pool is within its share.
    private void MaintainHighPool()
    {
        while (_highPoolUsage > _highPoolCapacity && _highOldest is CacheEntry oldest)
        {
            Unlink(oldest);
            LinkLowNewest(oldest);
        }
    }

    // Links an unpinned entry at the newest end of the pool it belongs to.
    private void LinkNewest(CacheEntry entry)
    {
        if (_highPoolRatio > 0 && (entry.HighPriority || entry.Hit))
        {
            entry.InHighPool = true;
            entry.Older = _highNewest;
            entry.Newer = null;
            if (_highNewest is not null)
                _highNewest.Newer = entry;

            _highNewest = entry;
            _highOldest ??= entry;
            _highPoolUsage += entry.Charge;
            MaintainHighPool();
            return;
        }

        LinkLowNewest(entry);
    }

    private void LinkLowNewest(CacheEntry entry)
    {
        entry.InHighPool = false;
        entry.Older = _lowNewest;
        entry.Newer = null;
        if (_lowNewest is not null)
            _lowNewest.Newer = entry;

        _lowNewest = entry;
        _lowOldest ??= entry;
    }

    private void Unlink(CacheEntry entry)
    {
        if (entry.InHighPool)
        {
            if (entry.Newer is not null)
                entry.Newer.Older = entry.Older;
            else
                _highNewest = entry.Older;

            if (entry.Older is not null)
                entry.Older.Newer = entry.Newer;
            else
                _highOldest = entry.Newer;

            _highPoolUsage -= entry.Charge;
            entry.InHighPool = false;
        }
        else
        {
            if (entry.Newer is not null)
                entry.Newer.Older = entry.Older;
            else
                _lowNewest = entry.Older;

            if (entry.Older is not null)
                entry.Older.Newer = entry.Newer;
            else
                _lowOldest = entry.Newer;
        }

        entry.Newer = null;
        entry.Older = null;
    }
}
