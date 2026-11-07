using System.Security.Cryptography;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Providers;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Kommander;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// Storage damage seen through a whole Kahuna node on Solarium, the store and the Raft log both. A node whose store
/// table was damaged on disk answers the first read that reaches the damage with the terminal <c>StorageCorrupt</c>,
/// never <c>MustRetry</c> and never a short page, then takes itself out of consensus for the partitions it hosts:
/// candidacy withheld, leadership gone, the storage health naming the damage. A single node whose store directory
/// was lost while its Raft log survived starts, reports the store as replaced, and rebuilds it from the log. Ported
/// from Kahuna's <c>TestStorageCorruptionNode</c>. A store lost under a log compacted below a checkpoint cannot be
/// rebuilt from that log: a single node refuses the replay and never leads, and a follower of three is rebuilt from
/// its leader's snapshot.
/// </summary>
public sealed class TestStorageCorruptionNode : IDisposable
{
    private const string Revision = "solarium-corruption";
    private const string Prefix = "corrupt/";
    private const int Keys = 256;
    private const int ValueBytes = 2048;
    private const int DataPartition = 1;

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

    // A single node whose Raft log was compacted below a checkpoint, then whose store was lost: replaying the log
    // suffix would leave every row below the compaction floor missing, so a read of such a key would answer that it
    // does not exist. The store answers floor 0 (it keeps floors, and holds none), Kommander finds that floor below
    // the compaction floor, refuses the replay, and the partition never leads, so nothing missing is served. The same
    // compacted log over the intact store replays and serves every key, which shows the log's compaction is not
    // what refuses.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_lost_store_under_a_compacted_log_refuses_the_replay_and_serves_nothing(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        const int keys = 16;
        await using (EmbeddedKahunaNode first = new(Options(scope.Environment, withWal: true)))
        {
            await first.StartAsync(TestContext.Current.CancellationToken);
            for (int i = 0; i < keys; i++)
            {
                (KeyValueResponseType response, _, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, CompactedKey(i), Encoding.UTF8.GetBytes("v" + i), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, response);
            }

            await first.FlushAsync();
        }

        int partition = CompactDataPartitionOfTheLog(scope.Environment, WalPath, writeCheckpoint: true);

        // The intact store holds a floor at or above the boundary: the replay runs and every key reads back.
        await using (EmbeddedKahunaNode intact = new(Options(scope.Environment, withWal: true)))
        {
            Assert.True(intact.Kahuna.GetStorageHealth().IsHealthy);
            await intact.StartAsync(TestContext.Current.CancellationToken);
            for (int i = 0; i < keys; i++)
            {
                (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await intact.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, CompactedKey(i), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Get, response);
                Assert.Equal(Encoding.UTF8.GetBytes("v" + i), entry?.Value);
            }

            Assert.Equal(RaftPartitionStorageState.Healthy, intact.Raft.GetPartitionStorageHealth(partition)?.State);
        }

        Directory.Delete(Path.Combine(StoragePath, Revision), recursive: true);

        await using EmbeddedKahunaNode wiped = new(Options(scope.Environment, withWal: true));
        Assert.NotNull(wiped.Kahuna.GetStorageHealth().StoreReplacedNotice);
        await StartWithin(wiped, TimeSpan.FromSeconds(5));

        await WaitUntil(() => Task.FromResult(
            wiped.Raft.GetPartitionStorageHealth(partition)?.State == RaftPartitionStorageState.ApplicationFloorBelowCompaction));

        await Task.Delay(1500, TestContext.Current.CancellationToken);
        Assert.False(await wiped.Raft.AmILeader(partition, TestContext.Current.CancellationToken));

        // One read: with no leader, each one waits out the locator's bound before it answers.
        (KeyValueResponseType wipedResponse, ReadOnlyKeyValueEntry? wipedEntry) = await wiped.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, CompactedKey(0), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.True(wipedResponse is not (KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist),
            $"a store that lost the rows below the compaction floor answered {wipedResponse} for {CompactedKey(0)}");
        Assert.Null(wipedEntry?.Value);
    }

    // The same loss on one replica of three: the follower whose store was lost under its compacted log refuses its
    // replay and asks the leader for a snapshot, and the snapshot rebuilds its store whole. The two intact replicas
    // keep serving every key meanwhile, and the follower never leads from the store it lost.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_follower_that_lost_its_store_under_a_compacted_log_is_rebuilt_from_the_leaders_snapshot(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        const int keys = 16;
        EmbeddedKahunaOptions options = Options(scope.Environment, withWal: true);
        options.NodeName = "node";
        options.ReplicationFactor = 3;

        int follower;
        await using (EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, options, cancellationToken: TestContext.Current.CancellationToken))
        {
            EmbeddedKahunaNode entry = cluster.GetNode(0);
            for (int i = 0; i < keys; i++)
            {
                (KeyValueResponseType response, _, _) = await entry.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, CompactedKey(i), Encoding.UTF8.GetBytes("v" + i), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, response);
            }

            int leader = await cluster.GetLeaderIndexAsync(DataPartition, TestContext.Current.CancellationToken);
            follower = (leader + 1) % cluster.NodeCount;

            // Every committed entry applied on the follower, so its log holds them all before it stops.
            long committed = cluster.GetNode(leader).Raft.WalAdapter.GetMaxLog(DataPartition);
            await WaitUntil(() => Task.FromResult(cluster.GetNode(follower).Raft.WalAdapter.GetMaxLog(DataPartition) >= committed));
            for (int i = 0; i < cluster.NodeCount; i++)
                await cluster.GetNode(i).FlushAsync();

            await cluster.StopNodeAsync(follower);
            CompactDataPartitionOfTheLog(scope.Environment, MemberWalPath(follower), writeCheckpoint: false);
            Directory.Delete(Path.Combine(MemberStoragePath(follower), Revision), recursive: true);

            await cluster.RestartNodeAsync(follower, TestContext.Current.CancellationToken);
            EmbeddedKahunaNode rebuilt = cluster.GetNode(follower);
            Assert.NotNull(rebuilt.Kahuna.GetStorageHealth().StoreReplacedNotice);

            // Until the snapshot lands the follower must not lead; once it lands the partition is healthy again.
            await WaitUntil(async () =>
            {
                RaftPartitionStorageState? state = rebuilt.Raft.GetPartitionStorageHealth(DataPartition)?.State;
                if (state != RaftPartitionStorageState.Healthy)
                {
                    Assert.False(await rebuilt.Raft.AmILeader(DataPartition, TestContext.Current.CancellationToken),
                        $"the follower led partition {DataPartition} in storage state {state}");
                    return false;
                }

                return true;
            });

            for (int i = 0; i < keys; i++)
            {
                (KeyValueResponseType response, ReadOnlyKeyValueEntry? read) = await entry.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, CompactedKey(i), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Get, response);
                Assert.Equal(Encoding.UTF8.GetBytes("v" + i), read?.Value);
            }

            await rebuilt.FlushAsync();
        }

        // The follower's own store holds every row again, from the snapshot alone: its log no longer has them.
        using SolariumPersistenceBackend store = new(MemberStoragePath(follower), Revision, NullLogger.Instance, scope.Environment);
        for (int i = 0; i < keys; i++)
            Assert.Equal(Encoding.UTF8.GetBytes("v" + i), store.GetKeyValue(CompactedKey(i))?.Value);
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    private static string Key(int i) => $"{Prefix}k{i:D4}";

    private static string CompactedKey(int i) => $"compacted/k{i:D3}";

    // A cluster member keeps its store and its log under its own directory, named after the member (node-1, ...).
    private string MemberStoragePath(int index) => Path.Combine(StoragePath, $"node-{index + 1}");

    private string MemberWalPath(int index) => Path.Combine(WalPath, $"node-{index + 1}");

    // With the node stopped, compacts the data partition's log in walDirectory/wal as the node's own checkpoint and
    // compaction would, and returns the partition. With a checkpoint, the floor is a checkpoint row written one past
    // the last row, so no row is left above it; without one, the floor is the last row, which stays, so a follower's
    // log remains a prefix of its leader's.
    private static int CompactDataPartitionOfTheLog(IStorageEnvironment environment, string walDirectory, bool writeCheckpoint)
    {
        using SolariumWAL wal = new(walDirectory, "wal", NullLogger<IRaft>.Instance, syncWrites: true, SolariumWalOptions.Default with { Environment = environment });
        long last = wal.GetMaxLog(DataPartition);
        Assert.True(last > 2, $"partition {DataPartition} holds too short a log to compact ({last})");

        long floor = last;
        if (writeCheckpoint)
        {
            floor = last + 1;
            RaftLog row = new() { Id = floor, Term = wal.GetCurrentTerm(DataPartition), Type = RaftLogType.CommittedCheckpoint, LogType = "checkpoint", LogData = [] };
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(DataPartition, [row])]));
        }

        Assert.Equal(RaftOperationStatus.Success, wal.CompactLogsOlderThan(DataPartition, floor, 100_000, 100_000).Status);
        Assert.Equal(floor, ((IWAL)wal).GetCompactionFloor(DataPartition));
        return DataPartition;
    }

    // A start that may not finish: a partition that refuses its replay never elects a leader, and the start may
    // wait for one. The verdict is readable either way.
    private static async Task StartWithin(EmbeddedKahunaNode node, TimeSpan bound)
    {
        using CancellationTokenSource cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancel.CancelAfter(bound);
        try
        {
            await node.StartAsync(cancel.Token);
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
        }
    }

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
