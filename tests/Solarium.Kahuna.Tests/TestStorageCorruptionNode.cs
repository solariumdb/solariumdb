using System.Security.Cryptography;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Providers;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// Storage damage seen through a whole Kahuna node on Solarium, the store and the Raft log both. A node whose store
/// table was damaged on disk answers the first read that reaches the damage with the terminal <c>StorageCorrupt</c>,
/// never <c>MustRetry</c> and never a short page, then takes itself out of consensus for the partitions it hosts:
/// candidacy withheld, leadership gone, the storage health naming the damage. A single node whose store directory
/// was lost while its Raft log survived starts, reports the store as replaced, and rebuilds it from the log. Ported
/// from Kahuna's <c>TestStorageCorruptionNode</c>.
/// </summary>
public sealed class TestStorageCorruptionNode : IDisposable
{
    private const string Revision = "solarium-corruption";
    private const string Prefix = "corrupt/";
    private const int Keys = 256;
    private const int ValueBytes = 2048;

    private readonly TempDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    private string StoragePath => _directory.Combine("store");

    private string WalPath => _directory.Combine("wal");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_damaged_block_answers_the_first_read_with_storage_corrupt_and_the_node_stops_serving(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        byte[][] values = new byte[Keys][];
        for (int i = 0; i < Keys; i++)
        {
            values[i] = new byte[ValueBytes];
            RandomNumberGenerator.Fill(values[i]);
        }

        await using (EmbeddedKahunaNode first = new(Options(scope.Environment, withWal: false)))
        {
            await first.StartAsync(TestContext.Current.CancellationToken);
            for (int i = 0; i < Keys; i++)
            {
                (KeyValueResponseType response, _, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, Key(i), values[i], null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, response);
            }

            await first.FlushAsync();
        }

        // Every row to a table, then one byte of a data block flipped on disk.
        using (SolariumPersistenceBackend closed = new(StoragePath, Revision, NullLogger.Instance, scope.Environment))
            closed.FlushAndMeasureTableMemoryForTesting();

        FlipByteAtMiddle(LargestTable());

        await using EmbeddedKahunaNode second = new(Options(scope.Environment, withWal: false));
        await second.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(second.Kahuna.GetStorageHealth().IsHealthy);

        // A scan over every key reaches the damaged block: the answer is terminal, and nothing is served from it.
        KeyValueGetByRangeResult scan = await second.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, Prefix, null, true, null, true, Keys * 2, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.StorageCorrupt, scan.Type);
        Assert.Empty(scan.Items);

        KahunaStorageHealth storage = second.Kahuna.GetStorageHealth();
        Assert.NotNull(storage.StoreFailStopReason);
        Assert.Contains("bad checksum", storage.StoreFailStopReason, StringComparison.Ordinal);

        // Candidacy is withheld and leadership gone for every hosted data partition.
        List<int> hosted = [.. second.Raft.GetPartitionMap()
            .Where(range => range.PartitionId != RaftSystemConfig.SystemPartition && second.Raft.HostsPartition(range.PartitionId))
            .Select(range => range.PartitionId)];
        Assert.NotEmpty(hosted);
        await WaitUntil(() => Task.FromResult(hosted.All(second.Raft.IsCandidacyWithheld)));
        await WaitUntil(async () =>
        {
            foreach (int partition in hosted)
                if (await second.Raft.AmILeader(partition, TestContext.Current.CancellationToken))
                    return false;

            return true;
        });

        // A read of an intact key serves nothing either; without a leader the locator may answer MustRetry instead.
        (KeyValueResponseType later, ReadOnlyKeyValueEntry? entry) = await second.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, Key(0), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.True(later is KeyValueResponseType.StorageCorrupt or KeyValueResponseType.MustRetry or KeyValueResponseType.Errored, $"a damaged store served a read with {later}");
        Assert.True(entry?.Value is null, "a damaged store served a value");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_lost_store_directory_is_reported_as_replaced_and_rebuilt_from_the_log(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        const int keys = 16;
        await using (EmbeddedKahunaNode first = new(Options(scope.Environment, withWal: true)))
        {
            await first.StartAsync(TestContext.Current.CancellationToken);
            for (int i = 0; i < keys; i++)
            {
                (KeyValueResponseType response, _, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"wiped/k{i:D3}", Encoding.UTF8.GetBytes("v" + i), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, response);
            }

            await first.FlushAsync();
            Assert.Null(first.Kahuna.GetStorageHealth().StoreReplacedNotice);
        }

        // The store directory is gone; the Raft log is not.
        Directory.Delete(Path.Combine(StoragePath, Revision), recursive: true);

        await using EmbeddedKahunaNode second = new(Options(scope.Environment, withWal: true));

        // Found at construction, before the log is replayed.
        KahunaStorageHealth storage = second.Kahuna.GetStorageHealth();
        Assert.NotNull(storage.StoreReplacedNotice);
        Assert.Contains("lost or replaced", storage.StoreReplacedNotice, StringComparison.Ordinal);

        await second.StartAsync(TestContext.Current.CancellationToken);

        // The log was never compacted, so replay rebuilds every row and reads are whole again.
        for (int i = 0; i < keys; i++)
        {
            (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await second.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"wiped/k{i:D3}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, response);
            Assert.NotNull(entry);
            Assert.Equal(Encoding.UTF8.GetBytes("v" + i), entry.Value);
        }

        await second.FlushAsync();
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    private static string Key(int i) => $"{Prefix}k{i:D4}";

    // One node on a Solarium store, and on a Solarium WAL when withWal, else an in-memory one.
    private EmbeddedKahunaOptions Options(IStorageEnvironment environment, bool withWal)
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        providers.Register(new SolariumPersistenceBackendProvider(environment));
        providers.Register(new SolariumWalProvider(environment));
        return new EmbeddedKahunaOptions
        {
            Storage = SolariumPersistenceBackendProvider.TypeName,
            StoragePath = StoragePath,
            StorageRevision = Revision,
            StorageProviders = providers,
            WalStorage = withWal ? SolariumPersistenceBackendProvider.TypeName : "memory",
            WalPath = withWal ? WalPath : "",
            WalRevision = withWal ? "wal" : "",
            InitialPartitions = 1,

            // A short writer tick, so the writer's observation of the latch re-asserts the containment quickly; the
            // revision sweep stays off, so no background scan reaches the damage before a read does.
            DirtyObjectsWriterDelay = 200,
            CheckpointInterval = TimeSpan.FromHours(1),
        };
    }

    private string LargestTable()
    {
        string[] files = Directory.GetFiles(Path.Combine(StoragePath, Revision), "*.sst");
        Assert.NotEmpty(files);
        return files.OrderByDescending(f => new FileInfo(f).Length).First();
    }

    private static void FlipByteAtMiddle(string table)
    {
        byte[] bytes = File.ReadAllBytes(table);
        bytes[bytes.Length / 2] ^= 0xFF;
        File.WriteAllBytes(table, bytes);
    }

    private static async Task WaitUntil(Func<Task<bool>> condition)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(15);
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met in time.");
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }
}
