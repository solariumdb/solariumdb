using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Providers;
using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The node's shared memory: a Solarium WAL and a Solarium store of one node charge one
/// <see cref="WriteBufferManager"/>, the store flushes early when they pass it, and both release what they
/// charged when they close. A node without a budget gives the store none.
/// </summary>
public sealed class TestSharedMemory : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_wal_and_the_store_of_a_node_charge_one_budget_and_release_it_when_they_close(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SharedStorageMemory budget = new(64L * 1024 * 1024, 0);

        IWAL wal = OpenWal(scope.Environment, budget);
        SolariumPersistenceBackend store = OpenStore(scope.Environment, budget);

        WriteBufferManager manager = SolariumSharedMemory.For(budget)!.WriteBufferManager!;
        Assert.Same(manager, store.MemTableBudget);
        Assert.Equal(64L * 1024 * 1024, manager.BufferSize);

        long beforeStore = manager.MemoryUsage;
        Assert.True(store.StoreKeyValues([.. Enumerable.Range(0, 32).Select(i => Item($"shared/{i:D3}", 1, 1000))]));
        long afterStore = manager.MemoryUsage;
        Assert.True(afterStore >= beforeStore + 32 * 1000, $"The store charged {afterStore - beforeStore} bytes for 32 rows of 1000 bytes.");

        for (long id = 1; id <= 16; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Log(id, 1000)])]));
        Assert.True(manager.MemoryUsage >= afterStore + 16 * 1000, $"The WAL charged {manager.MemoryUsage - afterStore} bytes for 16 entries of 1000 bytes.");

        ((IDisposable)wal).Dispose();
        store.Dispose();
        Assert.Equal(0, manager.MemoryUsage);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_store_past_the_shared_budget_flushes_early_and_keeps_every_row_through_the_recovery_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SharedStorageMemory budget = new(256 * 1024, 0);
        string storeDirectory = Path.Combine(_directory.Path, "store", "v1");

        using (SolariumPersistenceBackend store = OpenStore(scope.Environment, budget))
        {
            WriteBufferManager manager = store.MemTableBudget!;

            // 1 MB of rows: the kv memtable holds 64 MB, so only the budget can flush it.
            for (int batch = 0; batch < 50; batch++)
                Assert.True(store.StoreKeyValues([.. Enumerable.Range(batch * 20, 20).Select(i => Item($"early/{i:D4}", 1, 1000))]));

            WaitUntil(() => Directory.GetFiles(storeDirectory, "*.sst").Length > 0 && manager.MemoryUsage <= manager.BufferSize);

            Assert.True(store.TryRecoverFromStorageFailure());
            Assert.Same(manager, store.MemTableBudget);
            Assert.True(manager.MemoryUsage <= manager.BufferSize, $"{manager.MemoryUsage} bytes charged after the reopen, over the budget of {manager.BufferSize}.");
            Assert.True(store.StoreKeyValues([Item("early/after", 2, 1000)]));

            for (int i = 0; i < 1000; i++)
                Assert.NotNull(store.GetKeyValue($"early/{i:D4}"));
            Assert.NotNull(store.GetKeyValue("early/after"));
        }

        Assert.Equal(0, SolariumSharedMemory.For(budget)!.WriteBufferManager!.MemoryUsage);
    }

    [Fact]
    public void A_node_without_shared_memory_gives_the_store_no_budget_and_the_process_cache()
    {
        using SolariumPersistenceBackend store = OpenStore(DefaultStorageEnvironment.Instance, budget: null);

        Assert.Null(store.MemTableBudget);
        Assert.Same(SolariumPersistenceBackend.SharedBlockCache, store.BlockCache);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_shared_cache_holds_the_stores_tables_and_the_memtables_of_both_and_empties_when_they_close(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SharedStorageMemory memory = new(memtableBudgetBytes: 16L * 1024 * 1024, cacheBytes: 64L * 1024 * 1024);
        SolariumSharedMemory shared = SolariumSharedMemory.For(memory)!;
        Cache cache = shared.Cache!;
        Assert.Equal(64L * 1024 * 1024, cache.Capacity);
        Assert.Same(cache, shared.WriteBufferManager!.Cache);

        IWAL wal = OpenWal(scope.Environment, memory);
        SolariumPersistenceBackend store = OpenStore(scope.Environment, memory);
        Assert.Same(cache, store.BlockCache);
        Assert.Same(shared.WriteBufferManager, store.MemTableBudget);

        // The memtables of both, charged into the cache in whole units, all pinned.
        Assert.True(store.StoreKeyValues([.. Enumerable.Range(0, 500).Select(i => Item($"shared/{i:D4}", 1, 1000))]));
        for (long id = 1; id <= 200; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Log(id, 1000)])]));

        WriteBufferManager manager = shared.WriteBufferManager;
        Assert.True(manager.MemoryUsage >= 700 * 1000);
        Assert.Equal(Charged(manager), cache.PinnedUsage);

        // The store's flushed tables join them, pinned at level 0, and the store's memtable charge leaves.
        Assert.InRange(store.FlushAndMeasureTableMemoryForTesting(), 1, 1024);   // the top-level indexes alone
        Assert.True(cache.PinnedUsage > Charged(manager), "the store's level 0 tables are not pinned in the shared cache");
        Assert.NotNull(store.GetKeyValue("shared/0007"));

        ((IDisposable)wal).Dispose();
        store.Dispose();
        Assert.Equal(0, cache.Usage);
        Assert.Equal(0, manager.MemoryUsage);
    }

    public void Dispose() => _directory.Dispose();

    // What a manager charges its cache: its memory in whole units.
    private static long Charged(WriteBufferManager manager) =>
        (manager.MemoryUsage + WriteBufferManager.CacheChargeUnit - 1) / WriteBufferManager.CacheChargeUnit * WriteBufferManager.CacheChargeUnit;

    // The WAL and the store opened as a Kahuna node opens them: through the providers, with the node's budget.
    private IWAL OpenWal(IStorageEnvironment environment, SharedStorageMemory? budget) =>
        new SolariumWalProvider(environment).Open(new WalContext
        {
            Path = Path.Combine(_directory.Path, "wal"),
            Revision = "v1",
            SyncWrites = false,
            Logger = NullLogger<IRaft>.Instance,
            SharedMemory = budget,
        });

    private SolariumPersistenceBackend OpenStore(IStorageEnvironment environment, SharedStorageMemory? budget) =>
        (SolariumPersistenceBackend)new SolariumPersistenceBackendProvider(environment).Open(new PersistenceBackendContext
        {
            StoragePath = Path.Combine(_directory.Path, "store"),
            StorageRevision = "v1",
            Configuration = new KahunaConfiguration(),
            Logger = NullLogger.Instance,
            SharedMemory = budget,
        });

    private static void WaitUntil(Func<bool> condition)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition did not hold within 30 seconds.");
            Thread.Sleep(10);
        }
    }

    private static RaftLog Log(long id, int payloadBytes) =>
        new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "op", LogData = new byte[payloadBytes] };

    private static PersistenceRequestItem Item(string key, long revision, int valueBytes) =>
        new(key,
            Encoding.UTF8.GetBytes(new string('v', valueBytes)),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);
}
