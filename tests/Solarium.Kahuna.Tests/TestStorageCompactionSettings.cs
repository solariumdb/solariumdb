using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Providers;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The node's storage settings pace the store's compactions: a compaction rate gives the store a rate
/// limiter and RocksDB's compaction count, a count of its own wins, and neither keeps one compaction at a
/// time with no limiter. The limiter outlives the in-place recovery reopen.
/// </summary>
public sealed class TestStorageCompactionSettings : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Theory]
    [InlineData(0, 0, 1, false)]
    [InlineData(50_000_000, 0, -1, true)]
    [InlineData(50_000_000, 3, 3, true)]
    [InlineData(0, 5, 5, false)]
    public void The_storage_settings_set_the_compaction_threads_and_the_rate_limiter(long rate, int count, int threads, bool limited)
    {
        using SolariumPersistenceBackend backend = Open(rate, count);

        Assert.Equal(threads < 0 ? Math.Max(2, Environment.ProcessorCount / 2) : threads, backend.CompactionThreads);
        Assert.Equal(limited, backend.CompactionRateLimiter is not null);
        if (limited)
            Assert.Equal(rate, backend.CompactionRateLimiter!.BytesPerSecond);
    }

    [Fact]
    public void A_limited_store_writes_reads_and_keeps_its_limiter_through_the_recovery_reopen()
    {
        using SolariumPersistenceBackend backend = Open(50_000_000, 0);
        RateLimiter limiter = backend.CompactionRateLimiter!;

        Assert.True(backend.StoreKeyValues([.. Enumerable.Range(0, 32).Select(i => Item($"paced/{i:D3}", 1))]));
        Assert.True(backend.TryRecoverFromStorageFailure());
        Assert.Same(limiter, backend.CompactionRateLimiter);
        Assert.True(backend.StoreKeyValues([Item("paced/after", 2)]));

        Assert.NotNull(backend.GetKeyValue("paced/000"));
        Assert.NotNull(backend.GetKeyValue("paced/after"));
    }

    public void Dispose() => _directory.Dispose();

    // A store opened as a Kahuna node opens it: through the provider, with the node's configuration.
    private SolariumPersistenceBackend Open(long rate, int count) =>
        (SolariumPersistenceBackend)new SolariumPersistenceBackendProvider().Open(new PersistenceBackendContext
        {
            StoragePath = _directory.Path,
            StorageRevision = "v1",
            Configuration = new KahunaConfiguration { StorageCompactionRateBytesPerSec = rate, StorageMaxBackgroundCompactions = count },
            Logger = NullLogger.Instance,
        });

    private static PersistenceRequestItem Item(string key, long revision) =>
        new(key,
            Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);
}
