using System.Diagnostics;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// Kommander clusters of three nodes in one process, each on its own <see cref="SolariumWAL"/>, over
/// Kommander's in-memory transport: replication with checkpoints across partitions, a restart of the
/// whole cluster on its directories, and a follower that restarts after it missed writes. The WALs
/// have small memtables, so the logs the nodes read back come from SSTables as well as memtables.
/// </summary>
[Collection(SerialCollection.Name)]
public sealed class ClusterTests : IDisposable
{
    private const int Nodes = 3;
    private const int PayloadSize = 600;

    private static readonly TimeSpan Convergence = TimeSpan.FromSeconds(30);

    private static readonly SolariumWalOptions Small = new()
    {
        ShardWriteBufferSizeBytes = 16 * 1024,
        ShardMaxWriteBufferNumber = 3,
        ShardLevel0FileNumCompactionTrigger = 2,
        MaxTotalWalSizeFlushUnits = 2,
    };

    private readonly TempDirectory _directory = new();
    private readonly InMemoryCommunication _communication = new();
    private readonly RaftManager?[] _nodes = new RaftManager?[Nodes];
    private readonly SolariumWAL?[] _wals = new SolariumWAL?[Nodes];

    /// <summary>Stops the nodes still running and deletes the test's directory.</summary>
    public void Dispose()
    {
        StopAll();
        _directory.Dispose();
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Every_node_of_a_three_node_cluster_holds_the_same_committed_log(string environment)
    {
        const int partitions = 3;
        const int entries = 40;
        const int checkpointEvery = 10;
        using EnvironmentScope scope = Environments.Create(environment);
        try
        {
            for (int i = 0; i < Nodes; i++)
                Start(i, scope.Environment, partitions);

            await JoinAll();
            for (int partition = 1; partition <= partitions; partition++)
            {
                RaftManager leader = await WaitForLeader(partition);
                long expected = 1;
                for (int n = 1; n <= entries; n++)
                {
                    await Replicate(leader, partition, expected);
                    expected++;
                    if (n % checkpointEvery != 0)
                        continue;

                    RaftReplicationResult checkpoint = await leader.ReplicateCheckpoint(partition, TestContext.Current.CancellationToken);
                    Assert.Equal(RaftOperationStatus.Success, checkpoint.Status);
                    Assert.Equal(expected++, checkpoint.LogIndex);
                }

                List<RaftLog> log = await WaitForSameCommittedLog(partition, expected - 1);
                Assert.Equal(entries / checkpointEvery, log.Count(l => l.Type == RaftLogType.CommittedCheckpoint));
                foreach (RaftLog entry in log)
                    if (entry.Type == RaftLogType.Committed)
                        Assert.Equal(Payload(partition, entry.Id), entry.LogData);
            }

            // Each partition wrote more than a memtable holds, so what every node read came in part from SSTables.
            for (int i = 0; i < Nodes; i++)
                for (int partition = 1; partition <= partitions; partition++)
                    Wals.WaitUntil(() => Wals.ShardCounts(_wals[i]!, partition).Flushes, flushes => flushes > 0, Convergence);
        }
        finally
        {
            StopAll();
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_cluster_restarted_on_its_directories_restores_every_committed_entry_and_keeps_numbering(string environment)
    {
        const int partition = 1;
        const int entries = 30;
        using EnvironmentScope scope = Environments.Create(environment);
        try
        {
            for (int i = 0; i < Nodes; i++)
                Start(i, scope.Environment, partitions: 1);

            await JoinAll();
            RaftManager leader = await WaitForLeader(partition);
            for (int id = 1; id <= entries; id++)
                await Replicate(leader, partition, id);

            List<string> before = Describe(await WaitForSameCommittedLog(partition, entries));
            StopAll();

            List<RaftLog>[] restored = new List<RaftLog>[Nodes];
            for (int i = 0; i < Nodes; i++)
            {
                RaftManager node = Start(i, scope.Environment, partitions: 1);
                Assert.Equal(before, Describe(ReadAll(_wals[i]!, partition)));
                restored[i] = Record(node, partition);
            }

            // Kommander catches what a restore callback throws, so the entries are checked here, not in it.
            await JoinAll();
            for (int i = 0; i < Nodes; i++)
            {
                List<RaftLog> delivered = restored[i];
                Wals.WaitUntil(() => Count(delivered), count => count >= entries, Convergence);
                lock (delivered)
                {
                    Assert.Equal(Enumerable.Range(1, entries).Select(id => (long)id), delivered.Select(l => l.Id));
                    foreach (RaftLog log in delivered)
                        Assert.Equal(Payload(partition, log.Id), log.LogData);
                }
            }

            // A leadership change may add an entry of its own, so the next index is past the restored log, not at it.
            leader = await WaitForLeader(partition);
            RaftReplicationResult next = await leader.ReplicateLogs(partition, "entry", Payload(partition, 0), cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, next.Status);
            Assert.True(next.LogIndex > entries, $"The first entry after the restart took index {next.LogIndex}, inside the restored log.");

            List<string> after = Describe(await WaitForSameCommittedLog(partition, next.LogIndex));
            Assert.Equal(before, after.Take(entries));
        }
        finally
        {
            StopAll();
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_follower_that_restarts_on_its_directory_catches_up_with_the_entries_it_missed(string environment)
    {
        const int partition = 1;
        const int beforeCrash = 15;
        const int entries = 40;
        using EnvironmentScope scope = Environments.Create(environment);
        try
        {
            for (int i = 0; i < Nodes; i++)
                Start(i, scope.Environment, partitions: 1);

            await JoinAll();
            RaftManager leader = await WaitForLeader(partition);
            for (int id = 1; id <= beforeCrash; id++)
                await Replicate(leader, partition, id);

            await WaitForSameCommittedLog(partition, beforeCrash);

            // Dispose stops the node without leaving the cluster, so the leader still counts it as a peer, as after a crash.
            int follower = Array.FindIndex(_nodes, node => !ReferenceEquals(node, leader));
            _nodes[follower]!.Dispose();
            _nodes[follower] = null;
            _wals[follower] = null;
            _communication.SetNodes(Network());

            for (int id = beforeCrash + 1; id <= entries; id++)
                await Replicate(leader, partition, id);

            RaftManager restarted = Start(follower, scope.Environment, partitions: 1);
            Assert.Equal(beforeCrash, _wals[follower]!.GetMaxLog(partition));

            List<RaftLog> restored = Record(restarted, partition);
            _communication.SetNodes(Network());
            await restarted.UpdateNodes();
            await restarted.JoinCluster(TestContext.Current.CancellationToken);

            List<RaftLog> log = await WaitForSameCommittedLog(partition, entries);
            for (int id = 1; id <= entries; id++)
                Assert.Equal(Payload(partition, id), log[id - 1].LogData);

            lock (restored)
                Assert.Equal(Enumerable.Range(1, beforeCrash).Select(id => (long)id), restored.Select(l => l.Id));
        }
        finally
        {
            StopAll();
        }
    }

    // Opens node index's WAL in its directory and builds the node; it joins with JoinAll or JoinCluster.
    private RaftManager Start(int index, IStorageEnvironment environment, int partitions)
    {
        SolariumWAL wal = new(Path.Combine(_directory.Path, $"node{index + 1}"), "wal", NullLogger<IRaft>.Instance, syncWrites: true, Small with { Environment = environment });
        RaftConfiguration config = new()
        {
            NodeName = $"node{index + 1}",
            NodeId = index + 1,
            Host = "localhost",
            Port = 8001 + index,
            InitialPartitions = partitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            // Long election timeouts keep one leader while the test writes; the heartbeats that carry
            // replication and backfill stay fast.
            StartElectionTimeout = 1000,
            EndElectionTimeout = 2000,
            EnableQuiescence = false,
        };

        List<RaftNode> peers = [];
        for (int i = 0; i < Nodes; i++)
            if (i != index)
                peers.Add(new RaftNode(Endpoint(i)));

        RaftManager node = new(config, new StaticDiscovery(peers), wal, _communication, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        _nodes[index] = node;
        _wals[index] = wal;
        return node;
    }

    // Disposing a node disposes its WAL, which closes the database and releases its directory.
    private void StopAll()
    {
        for (int i = 0; i < Nodes; i++)
        {
            _nodes[i]?.Dispose();
            _nodes[i] = null;
            _wals[i] = null;
        }
    }

    private async Task JoinAll()
    {
        _communication.SetNodes(Network());
        foreach (RaftManager? node in _nodes)
            await node!.UpdateNodes();

        await Task.WhenAll(_nodes.Select(node => node!.JoinCluster(TestContext.Current.CancellationToken)));
    }

    private Dictionary<string, IRaft> Network()
    {
        Dictionary<string, IRaft> network = new(StringComparer.Ordinal);
        for (int i = 0; i < Nodes; i++)
            if (_nodes[i] is RaftManager node)
                network[Endpoint(i)] = node;

        return network;
    }

    private async Task<RaftManager> WaitForLeader(int partition)
    {
        Stopwatch clock = Stopwatch.StartNew();
        while (clock.Elapsed < Convergence)
        {
            foreach (RaftManager? node in _nodes)
                if (node is not null && await node.AmILeaderQuick(partition))
                    return node;

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"No leader for partition {partition} within {Convergence}.");
        return null!;
    }

    // Waits until every running node holds the same log of partition, ending at lastId and committed throughout, and returns it.
    private async Task<List<RaftLog>> WaitForSameCommittedLog(int partition, long lastId)
    {
        Stopwatch clock = Stopwatch.StartNew();
        while (true)
        {
            List<List<RaftLog>> logs = [];
            foreach (SolariumWAL? wal in _wals)
                if (wal is not null)
                    logs.Add(ReadAll(wal, partition));

            List<string> first = Describe(logs[0]);
            bool same = logs.All(log => Describe(log).SequenceEqual(first, StringComparer.Ordinal));
            bool complete = logs[0].Count > 0 && logs[0][^1].Id == lastId
                && logs[0].All(l => l.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint);
            if (same && complete)
                return logs[0];

            if (clock.Elapsed > Convergence)
                Assert.Fail($"Partition {partition} did not converge to a committed log ending at {lastId} within {Convergence}: "
                    + string.Join("; ", logs.Select(log => $"{log.Count} entries, last {(log.Count == 0 ? 0 : log[^1].Id)}, {log.Count(l => l.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)} committed")));

            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }

    // The entries of partition that node hands to the application on restore, in the order it hands them; lock the list to read it.
    private static List<RaftLog> Record(RaftManager node, int partition)
    {
        List<RaftLog> restored = [];
        node.OnLogRestored += (restoredPartition, log) =>
        {
            if (restoredPartition == partition)
                lock (restored)
                    restored.Add(log);

            return Task.FromResult(true);
        };

        return restored;
    }

    private static int Count(List<RaftLog> log)
    {
        lock (log)
            return log.Count;
    }

    private static async Task Replicate(RaftManager leader, int partition, long expectedId)
    {
        RaftReplicationResult result = await leader.ReplicateLogs(partition, "entry", Payload(partition, expectedId), cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Success, result.Status);
        Assert.Equal(expectedId, result.LogIndex);
    }

    // The whole log of partition: ReadLogs starts at the last checkpoint.
    private static List<RaftLog> ReadAll(SolariumWAL wal, int partition) => wal.ReadLogsRange(partition, 1);

    // What a log entry is, apart from its timestamp: the fields that must match on every node.
    private static List<string> Describe(List<RaftLog> log) =>
        log.Select(l => $"{l.Id} {l.Type} {l.Term} {l.LogType} {Convert.ToHexString(l.LogData ?? [])}").ToList();

    // The payload of entry id: distinct per entry, so an entry read back in the wrong place does not match.
    private static byte[] Payload(int partition, long id)
    {
        byte[] payload = new byte[PayloadSize];
        new Random((partition * 1_000_003) + (int)id).NextBytes(payload);
        return payload;
    }

    private static string Endpoint(int index) => $"localhost:{8001 + index}";
}
