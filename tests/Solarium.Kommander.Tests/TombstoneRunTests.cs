using Kommander.Data;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// <see cref="SolariumWAL"/> after a truncating boundary: the state of a node restarted under load
/// once its leader sends it a snapshot. The boundary deletes the suffix above it row by row, and the
/// commit-frontier walk, the checkpoint check and the max-log read must still decide correctly
/// around that run of deletes. Ported from Kommander's <c>TestSnapshotInstallTombstoneRun</c> at
/// commit 1bee500, without its counts of skipped tombstones: RocksDB's perf context has no
/// equivalent in Solarium, so these tests check what the probes decide, not how far they read.
/// </summary>
public sealed class TombstoneRunTests : IDisposable
{
    private const int Partition = 1;
    private const long CommittedThrough = 2_000;
    private const long Boundary = 10_000;
    private const long SuffixEnd = 40_000;
    private const long BoundaryTerm = 2;

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Commit_markers_above_the_boundary_land_as_committed_rows(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = SeedAndInstall(scope);

        for (int round = 0; round < 3; round++)
        {
            long first = Boundary + 5_000 + (round * 100);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(first, first + 49, RaftLogType.Committed))]));
        }

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 9_000, RaftLogType.Committed)])]));

        // No Proposed row existed for them, so nothing was absorbed: they are full rows.
        List<RaftLog> written = wal.ReadLogsRange(Partition, Boundary + 5_000, 50);
        Assert.Equal(50, written.Count);
        Assert.All(written, log => Assert.Equal(RaftLogType.Committed, log.Type));
        Assert.Equal(RaftLogType.Committed, Assert.Single(wal.ReadLogsRange(Partition, Boundary + 9_000, 1)).Type);
    }

    // The walk hops the certified absent span, crosses the boundary row, and absorbs a contiguous
    // marker right above it; a Proposed row inside the span still stops it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_frontier_catches_up_across_the_boundary_and_absorbs_the_next_marker(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = SeedAndInstall(scope);
        long absorbedBefore = wal.CommitMarkersAbsorbed;

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 1, RaftLogType.Proposed)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 1, RaftLogType.Committed)])]));
        Assert.Equal(absorbedBefore + 1, wal.CommitMarkersAbsorbed);
        Assert.Equal(RaftLogType.Committed, Assert.Single(wal.ReadLogsRange(Partition, Boundary + 1, 1)).Type);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 2, RaftLogType.Proposed), Row(Boundary + 4, RaftLogType.Proposed)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 4, RaftLogType.Committed)])]));
        Assert.Equal(absorbedBefore + 1, wal.CommitMarkersAbsorbed);
        Assert.Equal(RaftLogType.Proposed, Assert.Single(wal.ReadLogsRange(Partition, Boundary + 2, 1)).Type);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_above_the_boundary_verifies_its_prefix_and_moves_the_last_checkpoint(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = SeedAndInstall(scope);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(Boundary + 1, Boundary + 4, RaftLogType.Proposed))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 5, RaftLogType.CommittedCheckpoint)])]));

        Assert.Equal(Boundary + 5, wal.GetLastCheckpoint(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_max_log_stays_exact_through_appends_sweeps_truncations_and_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = SeedAndInstall(scope))
        {
            Assert.Equal(Boundary, wal.GetMaxLog(Partition));
            Assert.Equal(Boundary, wal.GetMaxLog(Partition));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(Boundary + 200, Boundary + 209, RaftLogType.Proposed))]));
            Assert.Equal(Boundary + 209, wal.GetMaxLog(Partition));

            // A sweep that removes the top rows and leaves none above the cut.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, Boundary + 204));
            Assert.Equal(Boundary + 204, wal.GetMaxLog(Partition));

            // A sweep with a resolved survivor above the cut keeps the survivor as the max.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 300, RaftLogType.Committed)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(Boundary + 301, Boundary + 303, RaftLogType.Proposed))]));
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, Boundary + 250));
            Assert.Equal(Boundary + 300, wal.GetMaxLog(Partition));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, Boundary + 202));
            Assert.Equal(Boundary + 202, wal.GetMaxLog(Partition));

            // A boundary above the tail that truncates nothing raises the max to its row.
            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, Boundary + 500, -1, sync: false);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
            Assert.Equal(Boundary + 500, wal.GetMaxLog(Partition));
        }

        // The cache belongs to the open WAL: a reopen reads the max from disk.
        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        Assert.Equal(Boundary + 500, reopened.GetMaxLog(Partition));
        Assert.Equal(RaftOperationStatus.Success, reopened.DeletePartitionWAL(Partition));
        Assert.Equal(0, reopened.GetMaxLog(Partition));
    }

    // A committed prefix, an absent span, then a long Proposed suffix that a boundary with a term
    // matching nothing truncates into a run of deletes.
    private SolariumWAL SeedAndInstall(EnvironmentScope scope)
    {
        SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);
        for (long id = 1; id <= CommittedThrough; id += 500)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Range(id, Math.Min(id + 499, CommittedThrough), 1, RaftLogType.Proposed, [1, 2, 3, 4]))]));

        for (long id = 1; id <= CommittedThrough; id += 500)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Range(id, Math.Min(id + 499, CommittedThrough), 1, RaftLogType.Committed, [1, 2, 3, 4]))]));

        Assert.Equal(CommittedThrough, wal.CommitMarkersAbsorbed);

        for (long id = Boundary + 1; id <= SuffixEnd; id += 500)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Range(id, Math.Min(id + 499, SuffixEnd), 1, RaftLogType.Proposed, [1, 2, 3, 4]))]));

        Assert.Equal(SuffixEnd, wal.GetMaxLog(Partition));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, Boundary, BoundaryTerm, sync: false);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated);
        return wal;
    }

    private static RaftLog Row(long id, RaftLogType type) =>
        new() { Id = id, Term = BoundaryTerm, Type = type, LogType = "t", LogData = [1, 2, 3, 4] };

    private static List<RaftLog> Rows(long first, long last, RaftLogType type) => Range(first, last, BoundaryTerm, type, [1, 2, 3, 4]);
}
