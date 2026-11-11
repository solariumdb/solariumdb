using System.Diagnostics;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Providers;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// A deleted key through a whole node on Solarium: once the retention age passes the delete, the node
/// removes every row of the key except its current row, on its own schedule and with no further write.
/// The key still reads as absent, at the latest time and at the time of the delete, and a write that
/// creates it again continues its revisions, also on a node that has to load the key from disk first.
/// Ported from Kahuna's <c>TestDeletedKeyDiskRelease</c>.
/// </summary>
public sealed class TestDeletedKeyDiskRelease : IDisposable
{
    private const string Revision = "deleted-key-release";

    private readonly TempDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private EmbeddedKahunaOptions Options(IPersistenceBackendProvider provider)
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        providers.Register(provider);
        return new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = SolariumPersistenceBackendProvider.TypeName,
            StoragePath = _directory.Path,
            StorageRevision = Revision,
            StorageProviders = providers,
            WalStorage = "memory",
            InitialPartitions = 1,
            DirtyObjectsWriterDelay = 100,
            PersistentRevisionRetentionAge = TimeSpan.FromSeconds(1),
            PersistentRevisionCleanupInterval = TimeSpan.FromMilliseconds(100)
        };
    }

    private static bool HasAnyHistoryRow(IPersistenceBackend backend, string key, long upToRevision)
    {
        for (long revision = 0; revision <= upToRevision; revision++)
        {
            if (backend.GetKeyValueRevision(key, revision) is not null)
                return true;
        }

        return false;
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task DeletedKey_IsReleasedOnDisk_AndALaterWriteContinuesItsRevisions(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        CancellationToken ct = TestContext.Current.CancellationToken;

        string key = "release/" + Guid.NewGuid().ToString("N")[..8];
        string neighbour = key + "-live";

        long deleteRevision;
        HLCTimestamp deleteTime;

        RecordingProvider recording = new(new SolariumPersistenceBackendProvider(scope.Environment));
        await using (EmbeddedKahunaNode node = new(Options(recording)))
        {
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync(key, ct);

            IKahuna kahuna = node.Kahuna;

            foreach (string value in (string[])["v1", "v2"])
            {
                (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, B(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, set);
            }

            (KeyValueResponseType neighbourSet, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, neighbour, B("stays"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, neighbourSet);

            (KeyValueResponseType deleted, deleteRevision, _) =
                await kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Deleted, deleted);
            Assert.Equal(2, deleteRevision);

            await kahuna.FlushPersistenceAsync();

            // The node opens its store through the provider, which keeps what it opened.
            IPersistenceBackend backend = Assert.IsType<SolariumPersistenceBackend>(recording.Opened);

            // No write follows the delete. The node must release the key by itself once the retention
            // age has passed the delete record.
            Stopwatch waited = Stopwatch.StartNew();
            while (HasAnyHistoryRow(backend, key, deleteRevision))
            {
                Assert.True(waited.Elapsed < TimeSpan.FromSeconds(30),
                    "the deleted key still has history rows on disk 30 s after its delete, with a retention age of 1 s");
                await Task.Delay(100, ct);
            }

            ReadOnlyKeyValueEntry? current = backend.GetKeyValue(key);
            Assert.NotNull(current);
            Assert.Equal(KeyValueState.Deleted, current.State);
            Assert.Equal(deleteRevision, current.Revision);

            // The commit time of the delete, as the store recorded it.
            deleteTime = current.LastModified;
            Assert.NotEqual(HLCTimestamp.Zero, deleteTime);

            // The key next to it is not a deleted key and keeps its row.
            Assert.NotNull(backend.GetKeyValueRevision(neighbour, 0));

            (KeyValueResponseType latest, _) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, latest);

            (KeyValueResponseType atDelete, _) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, deleteTime, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, atDelete);
        }

        // A node with nothing in memory (the log is not kept across the restart): what it knows of the
        // key is the row the release left on disk.
        await using EmbeddedKahunaNode restarted = new(Options(new SolariumPersistenceBackendProvider(scope.Environment)));
        await restarted.StartAsync(ct);
        await restarted.WaitForLeaderForKeyAsync(key, ct);

        (KeyValueResponseType absent, _) = await restarted.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, absent);

        (KeyValueResponseType recreated, long recreatedRevision, _) = await restarted.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, B("v3"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, recreated);
        Assert.Equal(deleteRevision + 1, recreatedRevision);

        await restarted.Kahuna.FlushPersistenceAsync();

        (KeyValueResponseType got, ReadOnlyKeyValueEntry? entry) = await restarted.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, got);
        Assert.Equal("v3", Encoding.UTF8.GetString(entry!.Value!));

        // Between the delete and the new value the key did not exist, and the history says so although
        // the delete record's own row is gone.
        (KeyValueResponseType betweenDeleteAndRecreate, _) = await restarted.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, deleteTime, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, betweenDeleteAndRecreate);
    }

    /// <summary>
    /// The Solarium provider, keeping the store the node opens: the node's own reference to it is
    /// internal to Kahuna.
    /// </summary>
    private sealed class RecordingProvider(SolariumPersistenceBackendProvider inner) : IPersistenceBackendProvider
    {
        private readonly IPersistenceBackendProvider _inner = inner;

        /// <summary>The store that <see cref="Open"/> opened last, or null before the first open.</summary>
        public IPersistenceBackend? Opened { get; private set; }

        public string StorageType => _inner.StorageType;

        public bool KeepsDataOnDisk => _inner.KeepsDataOnDisk;

        public bool StagesLiveCheckpointsInStore => _inner.StagesLiveCheckpointsInStore;

        public IPersistenceBackend Open(PersistenceBackendContext context) => Opened = _inner.Open(context);

        public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) => _inner.OpenAt(directory, revision, logger);

        public string DataDirectory(string storageRoot, string revision) => _inner.DataDirectory(storageRoot, revision);

        public bool HasData(string storageRoot, string revision) => _inner.HasData(storageRoot, revision);

        public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
            _inner.MergeCheckpointInto(targetDirectory, targetRevision, sourceDirectory, ct);

        public void AdoptRevision(string checkpointDirectory, string revision) => _inner.AdoptRevision(checkpointDirectory, revision);
    }
}
