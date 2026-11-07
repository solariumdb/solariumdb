using Kommander.Data;
using Kommander.WAL;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The sync ledger's cost under group commit: the writers of one WAL write group attest the group's last
/// sequence (<see cref="WriteBatch.GroupLastSequence"/>), so a group costs the ledger at most one flush
/// (<see cref="SolariumSyncLedger.FlushCount"/>), and the ledger still ends at what the database holds.
/// </summary>
public sealed class SyncLedgerAttestationTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    // The leader's WAL flush is held while the followers queue behind it, so each round is two write groups,
    // the leader's and the followers', and the ledger flushes at most twice: a ledger that each writer flushed
    // for its own batch would flush up to seventeen times.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_writers_of_one_write_group_flush_the_sync_ledger_at_most_once(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        using SolariumWAL wal = Wals.Open(hooks, _directory.Path, syncWrites: true);
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        SolariumDb db = wal.DatabaseForTesting;
        const int followerCount = 16;
        const int rounds = 5;
        for (long round = 1; round <= rounds; round++)
        {
            long groupsBefore = db.WriteGroupCount;
            long flushesBefore = wal.GetSyncLedgerFlushes();
            hooks.FlushGate.Reset();
            hooks.FlushStarted.Reset();

            RaftOperationStatus[] statuses = new RaftOperationStatus[followerCount + 1];
            long id = round;
            Thread leader = new(() => statuses[0] = wal.Write([(1, [Logs.Proposed(id)])], true, null));
            leader.Start();
            Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));

            Thread[] followers = new Thread[followerCount];
            for (int i = 0; i < followerCount; i++)
            {
                int partition = i + 2;
                int slot = i + 1;
                followers[i] = new Thread(() => statuses[slot] = wal.Write([(partition, [Logs.Proposed(id)])], true, null));
                followers[i].Start();
            }

            Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == followerCount, TimeSpan.FromSeconds(10)));
            hooks.FlushGate.Set();
            leader.Join();
            foreach (Thread follower in followers)
                follower.Join();

            Assert.All(statuses, status => Assert.Equal(RaftOperationStatus.Success, status));
            long groups = db.WriteGroupCount - groupsBefore;
            long flushes = wal.GetSyncLedgerFlushes() - flushesBefore;
            Assert.Equal(2, groups);
            Assert.InRange(flushes, 1, groups);
            Assert.Equal(db.LatestSequenceNumber, wal.SyncLedgerAttestedSequence);
        }
    }
}
