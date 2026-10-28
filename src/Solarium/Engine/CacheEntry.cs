namespace Solarium.Engine;

/// <summary>
/// One entry of a <see cref="CacheShard"/>. Every field but <see cref="Key"/>, <see cref="Value"/>,
/// <see cref="Charge"/> and <see cref="HighPriority"/> is read and written under the shard's lock only.
/// </summary>
internal sealed class CacheEntry(CacheKey key, object value, long charge, bool highPriority)
{
    /// <summary>The entry's key.</summary>
    public CacheKey Key { get; } = key;

    /// <summary>
    /// The cached object, never changed. A reader that took it keeps it alive after an eviction or an erase:
    /// the cache gives up its reference and its charge, and the garbage collector frees the object after the
    /// last reader.
    /// </summary>
    public object Value { get; } = value;

    /// <summary>The bytes the entry counts against the cache's capacity.</summary>
    public long Charge { get; } = charge;

    /// <summary>
    /// True for an entry inserted at high priority, as RocksDB's <c>Cache::Priority::HIGH</c>: it joins the shard's
    /// high-priority pool whenever it enters the LRU list.
    /// </summary>
    public bool HighPriority { get; } = highPriority;

    /// <summary>True once the entry is looked up after its insert: from then on it joins the high-priority pool too.</summary>
    public bool Hit { get; set; }

    /// <summary>True while the entry is in the shard's high-priority pool, the newer part of its LRU order.</summary>
    public bool InHighPool { get; set; }

    /// <summary>The holders that keep the entry from eviction; while above 0 the entry is not in the LRU list.</summary>
    public int Pins { get; set; }

    /// <summary>False once the entry is evicted or erased: its charge has left the cache.</summary>
    public bool InCache { get; set; } = true;

    /// <summary>The next entry towards the most recently used end of its pool's list, or null.</summary>
    public CacheEntry? Newer { get; set; }

    /// <summary>The next entry towards the least recently used end of its pool's list, or null.</summary>
    public CacheEntry? Older { get; set; }
}
