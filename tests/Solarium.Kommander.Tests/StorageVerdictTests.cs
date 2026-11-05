using System.Collections.Concurrent;
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
/// What a Kommander node on <see cref="SolariumWAL"/> does with the WAL's storage verdicts, through a real
/// <see cref="RaftManager"/>. A node whose WAL file was cut after its fsync restores every partition quarantined
/// (<see cref="RaftPartitionStorageState.LostAcknowledgedEntries"/>): as the only voter it never leads from the
/// stale log, across restarts, until the operator accepts the loss; as a follower it is repaired by its leader and
/// lifts the quarantine. A node whose WAL meets damage fail-stops it: as the only voter whose restore reads the damage
/// it never leads, and as a leader it reports <see cref="RaftPartitionStorageState.EngineFailed"/> and steps down
/// while another node takes over. Ported from
/// Kommander's <c>TestStorageVerdictRestore</c> and <c>TestLostAcknowledgedEntriesCluster</c> at commit e2763bc.
/// </summary>
[Collection(SerialCollection.Name)]
public sealed class StorageVerdictTests : IDisposable
{
    private const int UserPartition = 1;
    private const int Nodes = 3;

    private static readonly TimeSpan Convergence = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();
    private readonly InMemoryCommunication _communication = new();
    private readonly RaftManager?[] _nodes = new RaftManager?[Nodes];

    /// <summary>Stops the nodes still running and deletes the test's directory.</summary>
    public void Dispose()
    {
        StopAll();
        _directory.Dispose();
    }

    private void StopAll()
    {
        for (int i = 0; i < Nodes; i++)
        {
            _nodes[i]?.Dispose();
            _nodes[i] = null;
        }
    }

    // ── A single voter ──────────────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_single_voter_whose_wal_was_cut_after_its_fsync_never_leads_until_the_loss_is_accepted(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        const int total = 24;
        long acknowledged;
        using (RaftManager healthy = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.Quarantine))
        {
            await Join(healthy);
            await WaitUntil(() => healthy.AmILeaderQuick(UserPartition));
            for (int i = 1; i <= total; i++)
                Assert.True((await healthy.ReplicateLogs(UserPartition, "Greeting", "Hello"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken)).Success);

            acknowledged = healthy.WalAdapter.GetMaxLog(UserPartition);
        }

        CutNewestWalFile(Path.Combine(_directory.Path, "single", "wal"));

        using (RaftManager victim = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.Quarantine))
        {
            Assert.True(victim.WalAdapter.GetMaxLog(UserPartition) < acknowledged, "the cut removed no acknowledged row; the fault was not reproduced");
            await TryJoin(victim);
            RaftPartitionStorageHealth health = await WaitForVerdict(victim, UserPartition);
            Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, health.State);
            Assert.Contains("lost synced writes", health.Reason);

            await Task.Delay(1500, TestContext.Current.CancellationToken);
            Assert.False(await victim.AmILeaderQuick(UserPartition));
            Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, victim.GetPartitionStorageHealth(UserPartition)?.State);
        }

        // The verdict persists: a plain restart quarantines again.
        using (RaftManager again = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.Quarantine))
        {
            await TryJoin(again);
            Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, (await WaitForVerdict(again, UserPartition)).State);
        }

        // The operator accepts the loss: the node runs and records the reconciliation, so the next restart is healthy.
        using (RaftManager accepting = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.AcceptAndLog))
        {
            await Join(accepting);
            await WaitUntil(() => accepting.AmILeaderQuick(UserPartition));
            Assert.Equal(RaftPartitionStorageState.Healthy, accepting.GetPartitionStorageHealth(UserPartition)?.State);
        }

        using RaftManager strict = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.Quarantine);
        await Join(strict);
        await WaitUntil(() => strict.AmILeaderQuick(UserPartition));
        Assert.Equal(RaftPartitionStorageState.Healthy, strict.GetPartitionStorageHealth(UserPartition)?.State);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_single_voter_whose_wal_meets_damage_at_its_restore_never_leads(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string engine = Path.Combine(_directory.Path, "single", "wal");
        using (RaftManager healthy = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.Quarantine))
        {
            await Join(healthy);
            await WaitUntil(() => healthy.AmILeaderQuick(UserPartition));
            for (int i = 1; i <= 24; i++)
                Assert.True((await healthy.ReplicateLogs(UserPartition, "Greeting", Logs.Incompressible(512), cancellationToken: TestContext.Current.CancellationToken)).Success);

            ((SolariumWAL)healthy.WalAdapter).FlushMemTablesForTesting(wait: true);
        }

        // The partition's rows are in a table of its shard; the restore reads them and meets the damage.
        string table = ShardTable(scope.Environment, engine);
        byte[] bytes = File.ReadAllBytes(table);
        bytes[8] ^= 0xFF;
        File.WriteAllBytes(table, bytes);

        using RaftManager victim = SingleNode(scope.Environment, LostAcknowledgedEntriesPolicy.Quarantine);
        await TryJoin(victim);
        // The restore's own read meets the damage, so Kommander reports the restore failed; either verdict withholds
        // candidacy, and the WAL is fail-stopped either way.
        RaftPartitionStorageHealth health = await WaitForVerdict(victim, UserPartition);
        Assert.True(health.State is RaftPartitionStorageState.RestoreFailed or RaftPartitionStorageState.EngineFailed, $"the verdict is {health}");
        Assert.True(health.WithholdsCandidacy);
        Assert.Contains("bad checksum", health.Reason);
        Assert.Contains("bad checksum", victim.WalAdapter.GetFailStopReason());

        await Task.Delay(1500, TestContext.Current.CancellationToken);
        Assert.False(await victim.AmILeaderQuick(UserPartition));
        Assert.Equal(bytes, File.ReadAllBytes(table));
    }

    // ── Three nodes ─────────────────────────────────────────────────────────────────────────

    // The shape the kommander-jepsen lost-tail fault leaves: a follower's WAL file cut back into the user writes after
    // their fsync. The watermark goes with the rows, so the verdict comes from the sync ledger; heartbeats are held so
    // it can be seen before the repair, then the leader replicates past the loss and the quarantine lifts. Kommander
    // syncs every write here (WalSingleFsyncCommit off): with its default, a batch of commit markers is written
    // unsynced, and a follower that first meets an entry already committed can write the row in such a batch, so a
    // cut may take only writes that were never synced, which no ledger can tell from a torn tail.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_follower_whose_wal_was_cut_after_its_fsync_is_quarantined_then_repaired_by_its_leader(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        try
        {
            const int total = 20;
            for (int i = 0; i < Nodes; i++)
                ClusterNode(i, scope.Environment, singleFsyncCommit: false);

            await JoinAll();
            RaftManager leader = await WaitForLeader(UserPartition);
            long[] before = new long[Nodes];
            for (int i = 0; i < Nodes; i++)
                before[i] = NewestWalFile(EngineDirectory(i)).Length;

            for (int i = 1; i <= total; i++)
                Assert.True((await leader.ReplicateLogs(UserPartition, "Greeting", "Hello World"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken)).Success);

            long leaderMax = leader.WalAdapter.GetMaxLog(UserPartition);
            await WaitUntil(() => ValueTask.FromResult(_nodes.All(n => n!.WalAdapter.GetMaxLog(UserPartition) >= leaderMax)));

            int victim = -1;
            for (int i = 0; i < Nodes && victim < 0; i++)
                if (!ReferenceEquals(_nodes[i], leader) && !await _nodes[i]!.AmILeaderQuick(UserPartition))
                    victim = i;

            Assert.InRange(victim, 0, Nodes - 1);
            long victimMax = _nodes[victim]!.WalAdapter.GetMaxLog(UserPartition);
            await WaitUntil(() => ValueTask.FromResult(_nodes[victim]!.WalAdapter.GetAcknowledgedPresence(UserPartition) >= victimMax));
            Assert.Equal(victimMax, _nodes[victim]!.WalAdapter.GetAcknowledgedPresence(UserPartition));

            // Crash the follower and cut its WAL file back into the user writes.
            _nodes[victim]!.Dispose();
            _nodes[victim] = null;
            FileInfo log = NewestWalFile(EngineDirectory(victim));
            Assert.True(log.Length > before[victim], "the user writes did not reach the follower's WAL file");
            using (FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write))
                stream.SetLength(before[victim] + ((stream.Length - before[victim]) / 2));

            Assert.Equal(RaftOperationStatus.Success, await leader.SuspendHeartbeatsAsync(UserPartition, TestContext.Current.CancellationToken));
            RaftManager restarted = ClusterNode(victim, scope.Environment, singleFsyncCommit: false);
            ConcurrentBag<long> applied = [];
            restarted.OnLogRestored += (partition, entry) =>
            {
                if (partition == UserPartition)
                    applied.Add(entry.Id);

                return Task.FromResult(true);
            };
            restarted.OnReplicationReceived += (partition, entry) =>
            {
                if (partition == UserPartition)
                    applied.Add(entry.Id);

                return Task.FromResult(true);
            };

            _communication.SetNodes(Network());
            await JoinWithin(restarted);

            RaftPartitionStorageHealth health = await WaitForVerdict(restarted, UserPartition);
            Assert.True(health.State == RaftPartitionStorageState.LostAcknowledgedEntries,
                $"the verdict is {health}; the WAL holds {restarted.WalAdapter.GetMaxLog(UserPartition)} of {victimMax}, and reports {restarted.WalAdapter.GetLostSyncedWritesReason(UserPartition) ?? "no loss"}");
            Assert.Contains("lost synced writes", health.Reason);
            Assert.True(health.PresentThrough < victimMax, "the cut removed no user row");
            Assert.True(restarted.WalAdapter.GetAcknowledgedPresence(UserPartition) <= health.PresentThrough);

            // Resumed, the leader backfills past the loss and the quarantine lifts by itself.
            Assert.Equal(RaftOperationStatus.Success, await leader.ResumeHeartbeatsAsync(UserPartition, TestContext.Current.CancellationToken));
            await WaitUntil(() => ValueTask.FromResult(restarted.GetPartitionStorageHealth(UserPartition)?.State == RaftPartitionStorageState.Healthy));
            await WaitUntil(() => ValueTask.FromResult(restarted.WalAdapter.GetMaxLog(UserPartition) >= victimMax));
            HashSet<long> userIds = [.. leader.WalAdapter.ReadLogs(UserPartition).Where(l => l.LogType == "Greeting").Select(l => l.Id)];
            Assert.Equal(total, userIds.Count);
            await WaitUntil(() => ValueTask.FromResult(userIds.All(applied.Contains)));

            // The system partition shared the WAL and was quarantined too; the leader's heartbeat repairs it.
            await WaitUntil(() => ValueTask.FromResult(restarted.GetPartitionStorageHealth(0)?.State == RaftPartitionStorageState.Healthy));

            // The reconciliation was recorded: a restart after the repair is healthy at once.
            _nodes[victim]!.Dispose();
            _nodes[victim] = null;
            RaftManager recovered = ClusterNode(victim, scope.Environment, singleFsyncCommit: false);
            _communication.SetNodes(Network());
            await JoinWithin(recovered);
            Assert.Equal(RaftPartitionStorageState.Healthy, (await WaitForVerdict(recovered, UserPartition)).State);
            Assert.Equal(RaftPartitionStorageState.Healthy, (await WaitForVerdict(recovered, 0)).State);
        }
        finally
        {
            // Before the environment closes: a node still writing would wait on it forever.
            StopAll();
        }
    }

    // A leader whose WAL meets damage fail-stops: on its next tick it steps down and reports the engine failed, and
    // another node leads and takes writes.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_leader_whose_wal_meets_damage_steps_down_and_another_node_leads(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        try
        {
            for (int i = 0; i < Nodes; i++)
                ClusterNode(i, scope.Environment);

            await JoinAll();
            RaftManager leader = await WaitForLeader(UserPartition);
            for (int i = 1; i <= 24; i++)
                Assert.True((await leader.ReplicateLogs(UserPartition, "Greeting", Logs.Incompressible(512), cancellationToken: TestContext.Current.CancellationToken)).Success);

            SolariumWAL wal = (SolariumWAL)leader.WalAdapter;
            wal.FlushMemTablesForTesting(wait: true);
            (string name, _, _, _, _) = wal.GetShardLiveFilesForTesting(UserPartition).OrderBy(f => f.SmallestId).First();
            string table = Path.Combine(EngineDirectory(Array.IndexOf(_nodes, leader)), name.TrimStart('/'));
            byte[] bytes = File.ReadAllBytes(table);
            bytes[8] ^= 0xFF;
            File.WriteAllBytes(table, bytes);

            // A read of the log, as a backfill of a lagging follower makes, reaches the damaged block.
            Assert.True(Assert.Throws<SolariumException>(() => leader.WalAdapter.ReadLogsRange(UserPartition, 1, 100)).IsCorruption);
            await WaitUntil(async () => !await leader.AmILeaderQuick(UserPartition));
            Assert.Equal(RaftPartitionStorageState.EngineFailed, leader.GetPartitionStorageHealth(UserPartition)?.State);

            RaftManager next = await WaitForLeader(UserPartition, except: leader);
            Assert.True((await next.ReplicateLogs(UserPartition, "Greeting", "after"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken)).Success);
            await Task.Delay(1000, TestContext.Current.CancellationToken);
            Assert.False(await leader.AmILeaderQuick(UserPartition));
        }
        finally
        {
            // Before the environment closes: a node still writing would wait on it forever.
            StopAll();
        }
    }

    // ── Scaffolding ─────────────────────────────────────────────────────────────────────────

    private static RaftConfiguration Configuration(string name, int id, int port, LostAcknowledgedEntriesPolicy policy, bool singleFsyncCommit = true) => new()
    {
        WalSingleFsyncCommit = singleFsyncCommit,
        NodeName = name,
        NodeId = id,
        Host = "localhost",
        Port = port,
        InitialPartitions = 1,
        HeartbeatInterval = TimeSpan.FromMilliseconds(50),
        RecentHeartbeat = TimeSpan.FromMilliseconds(25),
        VotingTimeout = TimeSpan.FromMilliseconds(250),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
        TimerInitialDelay = TimeSpan.FromMilliseconds(25),
        StartElectionTimeout = 100,
        EndElectionTimeout = 250,
        EnableQuiescence = false,
        EnableCheckQuorum = false,
        BackfillThreshold = 1,
        LostAcknowledgedEntriesPolicy = policy,
    };

    private RaftManager SingleNode(IStorageEnvironment environment, LostAcknowledgedEntriesPolicy policy) =>
        new(Configuration("single", 1, 8001, policy), new StaticDiscovery([]),
            Wals.Open(environment, Path.Combine(_directory.Path, "single"), syncWrites: true), new InMemoryCommunication(), new HybridLogicalClock(), NullLogger<IRaft>.Instance);

    private RaftManager ClusterNode(int index, IStorageEnvironment environment, bool singleFsyncCommit = true)
    {
        List<RaftNode> peers = [];
        for (int i = 0; i < Nodes; i++)
            if (i != index)
                peers.Add(new RaftNode(Endpoint(i)));

        RaftManager node = new(Configuration($"node{index + 1}", index + 1, 8001 + index, LostAcknowledgedEntriesPolicy.Quarantine, singleFsyncCommit), new StaticDiscovery(peers),
            Wals.Open(environment, Path.Combine(_directory.Path, $"node{index + 1}"), syncWrites: true), _communication, new HybridLogicalClock(), NullLogger<IRaft>.Instance);
        _nodes[index] = node;
        return node;
    }

    private string EngineDirectory(int index) => Path.Combine(_directory.Path, $"node{index + 1}", "wal");

    private static string Endpoint(int index) => $"localhost:{8001 + index}";

    private Dictionary<string, IRaft> Network()
    {
        Dictionary<string, IRaft> network = new(StringComparer.Ordinal);
        for (int i = 0; i < Nodes; i++)
            if (_nodes[i] is RaftManager node)
                network[Endpoint(i)] = node;

        return network;
    }

    private async Task JoinAll()
    {
        _communication.SetNodes(Network());
        foreach (RaftManager? node in _nodes)
            await node!.UpdateNodes();

        await Task.WhenAll(_nodes.Select(node => node!.JoinCluster(TestContext.Current.CancellationToken)));
    }

    private static async Task Join(RaftManager node)
    {
        await node.UpdateNodes();
        await node.JoinCluster(TestContext.Current.CancellationToken);
    }

    // A join of a restarted node, which fails the test instead of waiting past the convergence bound.
    private static async Task JoinWithin(RaftManager node)
    {
        using CancellationTokenSource cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancel.CancelAfter(Convergence);
        await node.UpdateNodes();
        await node.JoinCluster(cancel.Token);
    }

    // A join that may not finish: with every partition quarantined, a single voter elects no system leader, and the
    // join waits for one. The verdict is readable either way.
    private static async Task TryJoin(RaftManager node)
    {
        using CancellationTokenSource cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancel.CancelAfter(TimeSpan.FromSeconds(5));
        await node.UpdateNodes();
        try
        {
            await node.JoinCluster(cancel.Token);
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task<RaftPartitionStorageHealth> WaitForVerdict(RaftManager node, int partition)
    {
        RaftPartitionStorageHealth? health = null;
        await WaitUntil(() =>
        {
            health = node.GetPartitionStorageHealth(partition);
            return ValueTask.FromResult(health is not null && health.State != RaftPartitionStorageState.Restoring);
        });

        return health!;
    }

    private async Task<RaftManager> WaitForLeader(int partition, RaftManager? except = null)
    {
        RaftManager? found = null;
        await WaitUntil(async () =>
        {
            foreach (RaftManager? node in _nodes)
                if (node is not null && !ReferenceEquals(node, except) && await node.AmILeaderQuick(partition))
                {
                    found = node;
                    return true;
                }

            return false;
        });

        return found!;
    }

    private static async Task WaitUntil(Func<ValueTask<bool>> condition)
    {
        Stopwatch clock = Stopwatch.StartNew();
        while (!await condition())
        {
            Assert.True(clock.Elapsed < Convergence, $"The condition was not met within {Convergence}.");
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }
    }

    private static FileInfo NewestWalFile(string directory) =>
        new DirectoryInfo(directory).GetFiles("*.log").OrderByDescending(f => f.Name, StringComparer.Ordinal).First();

    private static void CutNewestWalFile(string directory)
    {
        FileInfo log = NewestWalFile(directory);
        using FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write);
        stream.SetLength(stream.Length / 2);
    }

    // The first table of the user partition's shard, read from a WAL opened for it alone.
    private static string ShardTable(IStorageEnvironment environment, string engine)
    {
        using SolariumWAL wal = Wals.Open(environment, Path.GetDirectoryName(engine)!, syncWrites: true);
        (string name, _, _, _, _) = wal.GetShardLiveFilesForTesting(UserPartition).OrderBy(f => f.SmallestId).First();
        return Path.Combine(engine, name.TrimStart('/'));
    }
}
