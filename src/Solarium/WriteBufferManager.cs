using Solarium.Engine;

namespace Solarium;

/// <summary>
/// One cap on the memtable memory of every database that shares it, as RocksDB's
/// <c>WriteBufferManager</c>. Give it to <see cref="DbOptions.WriteBufferManager"/>; the host owns it,
/// and one manager can cap several databases. When the memtables of all of them pass the cap, the next
/// database to write switches its column family with the oldest memtable and flushes it. A database
/// only ever flushes its own memtables, so the memory of one that takes no writes stays until it does.
/// </summary>
/// <remarks>
/// <para>Memory counts as each database's <c>rocksdb.cur-size-all-mem-tables</c> counts it: the keys and
/// values, 32 bytes per entry, and the fragments of range tombstones. RocksDB counts the arena blocks
/// its memtables allocate.</para>
/// <para>Unlike RocksDB's, the manager never stalls writes (<c>allow_stall</c>): going over the cap only
/// makes the writing database flush.</para>
/// <para>With a <see cref="Solarium.Cache"/>, the manager charges the memtable memory to it as well, as
/// RocksDB's <c>WriteBufferManager(buffer_size, cache)</c>: pinned entries of 256 KiB, as many as cover
/// <see cref="MemoryUsage"/>, which the cache's index and filter blocks make room for. One capacity then
/// bounds both, as Kommander's <c>RocksDbSharedResources.CreateWithUnifiedBudget</c>.</para>
/// </remarks>
public sealed class WriteBufferManager
{
    /// <summary>The size of one entry the manager charges to its cache, as RocksDB's <c>kSizeDummyEntry</c>.</summary>
    public const long CacheChargeUnit = 256 * 1024;

    // The value of every charge entry: the cache holds only its charge.
    private static readonly object s_charge = new();

    private readonly Cache? _cache;
    private readonly long _cacheOwner;
    private readonly object _cacheLock = new();
    private readonly Stack<CacheEntry> _charges = new();   // under _cacheLock
    private long _nextCharge;      // under _cacheLock
    private long _chargedUnits;    // written under _cacheLock with Interlocked; read without it
    private long _bufferSize;
    private long _memoryUsage;    // Interlocked
    private long _mutableUsage;   // Interlocked

    /// <summary>
    /// A manager whose cap is <paramref name="bufferSize"/> bytes, which charges the memtable memory to
    /// <paramref name="cache"/> too when one is given.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bufferSize"/> is not positive.</exception>
    public WriteBufferManager(long bufferSize, Cache? cache = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bufferSize, 1);
        _bufferSize = bufferSize;
        _cache = cache;
        _cacheOwner = Cache.NewOwner();
    }

    /// <summary>The cache the manager charges the memtable memory to, or null.</summary>
    public Cache? Cache => _cache;

    /// <summary>The cap, in bytes, as RocksDB's <c>buffer_size</c>.</summary>
    public long BufferSize => Volatile.Read(ref _bufferSize);

    /// <summary>
    /// The memory of every memtable of the databases that share the manager, active and immutable, as
    /// RocksDB's <c>memory_usage</c>. A memtable leaves it when its flush installs its table.
    /// </summary>
    public long MemoryUsage => Interlocked.Read(ref _memoryUsage);

    /// <summary>
    /// The part of <see cref="MemoryUsage"/> in active memtables, the ones that take writes, as RocksDB's
    /// <c>mutable_memtable_memory_usage</c>.
    /// </summary>
    public long MutableMemoryUsage => Interlocked.Read(ref _mutableUsage);

    /// <summary>Change the cap, as RocksDB's <c>SetBufferSize</c>. The next write of each database checks the new one.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bufferSize"/> is not positive.</exception>
    public void SetBufferSize(long bufferSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bufferSize, 1);
        Volatile.Write(ref _bufferSize, bufferSize);
    }

    /// <summary>
    /// True when the writing database should flush, as RocksDB's <c>ShouldFlush</c>: the active memtables
    /// hold more than 7/8 of the cap, or all memtables reach the cap and the active ones hold half of it.
    /// </summary>
    internal bool ShouldFlush()
    {
        long size = BufferSize;
        long mutable = MutableMemoryUsage;
        return mutable > size * 7 / 8 || (MemoryUsage >= size && mutable >= size / 2);
    }

    /// <summary>Add bytes that an active memtable took, as RocksDB's <c>ReserveMem</c>.</summary>
    internal void Reserve(long bytes)
    {
        Interlocked.Add(ref _memoryUsage, bytes);
        Interlocked.Add(ref _mutableUsage, bytes);
        if (_cache is not null)
            ChargeCache();
    }

    /// <summary>Take the bytes of a memtable that stopped taking writes out of the active ones, as RocksDB's <c>ScheduleFreeMem</c>.</summary>
    internal void ScheduleFree(long bytes) => Interlocked.Add(ref _mutableUsage, -bytes);

    /// <summary>Take the bytes of a memtable that the database no longer holds out of the total, as RocksDB's <c>FreeMem</c>.</summary>
    internal void Free(long bytes)
    {
        Interlocked.Add(ref _memoryUsage, -bytes);
        if (_cache is not null)
            ChargeCache();
    }

    // Makes the cache's charge entries cover MemoryUsage, in whole units. Most calls change the usage within a
    // unit and see that the count is right without the lock. A caller that skips the lock read the count after
    // its own change to the usage; the lock holder sets the count, a full fence, before it reads the usage again,
    // so either the holder sees that change or the caller sees the holder's count and takes the lock itself.
    private void ChargeCache()
    {
        if (UnitsFor(MemoryUsage) == Interlocked.Read(ref _chargedUnits))
            return;

        lock (_cacheLock)
        {
            long units;
            do
            {
                units = UnitsFor(MemoryUsage);
                while (_charges.Count < units)
                    _charges.Push(_cache!.InsertPinned(new CacheKey(_cacheOwner, ++_nextCharge), s_charge, CacheChargeUnit));

                while (_charges.Count > units)
                {
                    CacheEntry charge = _charges.Pop();
                    _cache!.Erase(charge.Key);
                }

                Interlocked.Exchange(ref _chargedUnits, units);
            }
            while (UnitsFor(MemoryUsage) != units);
        }
    }

    private static long UnitsFor(long bytes) => bytes <= 0 ? 0 : (bytes + CacheChargeUnit - 1) / CacheChargeUnit;
}
