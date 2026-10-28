using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Providers;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The store's tables keep their index and filter blocks in a cache, as the RocksDB backend's do: the stores a
/// node opens share one 256 MiB cache, a store releases its entries when it closes, and a cache too small for
/// the tables still reads every row.
/// </summary>
public sealed class TestBlockCache : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Fact]
    public void Stores_that_the_provider_opens_share_one_cache_of_256_MiB()
    {
        using SolariumPersistenceBackend one = Open("one");
        using SolariumPersistenceBackend two = Open("two");

        Assert.Same(SolariumPersistenceBackend.SharedBlockCache, one.BlockCache);
        Assert.Same(one.BlockCache, two.BlockCache);
        Assert.Equal(256L * 1024 * 1024, one.BlockCache.Capacity);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_tables_index_and_filter_go_to_the_cache_through_a_recovery_reopen_and_leave_it_at_close(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64L * 1024 * 1024);

        using (SolariumPersistenceBackend store = new(_directory.Combine("store"), "v1", NullLogger.Instance, scope.Environment, blockCache: cache))
        {
            Assert.True(store.StoreKeyValues([.. Enumerable.Range(0, 2000).Select(i => Item($"cached/{i:D4}"))]));
            Assert.InRange(store.FlushAndMeasureTableMemoryForTesting(), 1, 1024);   // the top-level indexes alone
            long usage = cache.Usage;
            Assert.True(usage > 0);
            Assert.Equal(usage, cache.PinnedUsage);   // the flushed tables are in level 0

            Assert.True(store.TryRecoverFromStorageFailure());
            Assert.Equal(usage, cache.Usage);
            for (int i = 0; i < 2000; i += 37)
                Assert.NotNull(store.GetKeyValue($"cached/{i:D4}"));
        }

        Assert.Equal(0, cache.Usage);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_cache_too_small_for_one_table_still_reads_every_row(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        using SolariumPersistenceBackend store = new(_directory.Combine("store"), "v1", NullLogger.Instance, scope.Environment, blockCache: new Cache(1));
        Assert.True(store.StoreKeyValues([.. Enumerable.Range(0, 2000).Select(i => Item($"tiny/{i:D4}"))]));
        store.FlushAndMeasureTableMemoryForTesting();

        for (int i = 0; i < 2000; i += 37)
            Assert.NotNull(store.GetKeyValue($"tiny/{i:D4}"));
    }

    public void Dispose() => _directory.Dispose();

    // A store opened as a Kahuna node opens it: through the provider.
    private SolariumPersistenceBackend Open(string name) =>
        (SolariumPersistenceBackend)new SolariumPersistenceBackendProvider().Open(new PersistenceBackendContext
        {
            StoragePath = _directory.Combine(name),
            StorageRevision = "v1",
            Configuration = new KahunaConfiguration(),
            Logger = NullLogger.Instance,
        });

    private static PersistenceRequestItem Item(string key) =>
        new(key,
            Encoding.UTF8.GetBytes("value of " + key),
            revision: 1,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 1,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);
}
