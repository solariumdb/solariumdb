using Kahuna.Server.Persistence.Providers;

namespace Solarium.Kahuna;

/// <summary>
/// The Solarium objects of a node's shared memory (<see cref="SharedStorageMemory"/>), which the node's Solarium
/// WAL and Solarium store both use: a <see cref="Solarium.Cache"/> for the index and filter blocks of their tables,
/// and a <see cref="Solarium.WriteBufferManager"/> for their memtables, charged into the cache when there is one,
/// as the RocksDB pair's unified budget. The first of the two providers to open creates them.
/// </summary>
internal sealed class SolariumSharedMemory
{
    private SolariumSharedMemory(SharedStorageMemory memory)
    {
        Cache = memory.CacheBytes > 0 ? new Cache(memory.CacheBytes) : null;
        WriteBufferManager = memory.MemtableBudgetBytes > 0 ? new WriteBufferManager(memory.MemtableBudgetBytes, Cache) : null;
    }

    /// <summary>The cache the WAL and the store share, or null when the node shares none.</summary>
    public Cache? Cache { get; }

    /// <summary>The memtable budget the WAL and the store share, or null when the node shares none.</summary>
    public WriteBufferManager? WriteBufferManager { get; }

    /// <summary>The Solarium objects of <paramref name="memory"/>, the same for every caller; null when the node shares no memory.</summary>
    public static SolariumSharedMemory? For(SharedStorageMemory? memory) =>
        memory?.GetOrAdd(static shared => new SolariumSharedMemory(shared));
}
