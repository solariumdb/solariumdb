using Kommander.Data;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The edges of <see cref="SolariumWAL"/>'s maintenance operations: a boundary at or past the end
/// of the log, the same boundary twice, operations on partitions never written, and operations on
/// two partitions that share a shard. Maintenance runs while appends and other partitions are live,
/// so each edge must be defined, idempotent, and leave its neighbours alone. Ported from
/// Kommander's <c>TestRocksDbWALMaintenanceEdges</c> at commit 1bee500.
/// </summary>
public sealed class MaintenanceTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    // Nothing lies above the last entry, so nothing is truncated, yet the boundary row is rewritten
    // with the snapshot's term and the checkpoint type.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_at_the_last_entry_with_another_term_rewrites_the_row_without_a_truncation(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 5, 1, RaftLogType.Committed))]));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 5, lastIncludedTerm: 9, sync: true);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.False(truncated);

        Assert.Equal(9, wal.GetTermAt(1, 5));
        RaftLog boundary = wal.ReadLogsRange(1, 5).Single();
        Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
        Assert.Equal(9, boundary.Term);
        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(wal.ReadLogsRange(1, 1)));
        Assert.Equal(5, wal.GetMaxLog(1));
        Assert.Equal(5, wal.GetLastCheckpoint(1));
    }

    // A snapshot is delivered again on a retry, so a second install may neither delete again nor
    // bring anything back.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Installing_the_same_boundary_twice_changes_nothing(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 5, 1, RaftLogType.Committed))]));
        for (int attempt = 0; attempt < 2; attempt++)
        {
            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 1, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
        }

        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(wal.ReadLogsRange(1, 1)));
        Assert.Equal(5, wal.GetMaxLog(1));
        Assert.Equal(3, wal.GetLastCheckpoint(1));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(2, Range(1, 4, 1, RaftLogType.Committed))]));
        (RaftOperationStatus first, bool firstTruncated) = wal.InstallSnapshotBoundary(2, snapshotIndex: 2, lastIncludedTerm: 7, sync: true);
        Assert.Equal(RaftOperationStatus.Success, first);
        Assert.True(firstTruncated);
        Assert.Equal([1L, 2L], Ids(wal.ReadLogsRange(2, 1)));

        (RaftOperationStatus second, bool secondTruncated) = wal.InstallSnapshotBoundary(2, snapshotIndex: 2, lastIncludedTerm: 7, sync: true);
        Assert.Equal(RaftOperationStatus.Success, second);
        Assert.False(secondTruncated);
        Assert.Equal([1L, 2L], Ids(wal.ReadLogsRange(2, 1)));
        Assert.Equal(7, wal.GetTermAt(2, 2));
        Assert.Equal(2, wal.GetMaxLog(2));
        Assert.Equal(2, wal.GetLastCheckpoint(2));
    }

    // The boundary is the one maintenance operation that writes on a fresh partition: its row and
    // the last checkpoint. A partition delete then removes both.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Maintenance_on_partitions_never_written_succeeds_and_leaves_nothing(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(30, afterLogId: 5));
        Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(30, afterLogId: 5));
        Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(30));
        Assert.Equal(0, wal.GetMaxLog(30));
        Assert.Empty(wal.ReadLogs(30));
        Assert.Equal(-1, wal.GetLastCheckpoint(30));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(31, snapshotIndex: 7, lastIncludedTerm: 3, sync: true);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.False(truncated);

        RaftLog boundary = Assert.Single(wal.ReadLogsRange(31, 1));
        Assert.Equal(7, boundary.Id);
        Assert.Equal(3, boundary.Term);
        Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
        Assert.Equal(7, wal.GetMaxLog(31));
        Assert.Equal(7, wal.GetLastCheckpoint(31));

        Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(31));
        Assert.Equal(0, wal.GetMaxLog(31));
        Assert.Empty(wal.ReadLogsRange(31, 1));
        Assert.Equal(-1, wal.GetLastCheckpoint(31));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Maintenance_on_a_shard_shared_by_two_partitions_leaves_the_neighbour_alone(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(0, Range(1, 5, 1, RaftLogType.Committed))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(8, Range(1, 5, 1, RaftLogType.Committed))]));

        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(0, afterLogId: 2));
        Assert.Equal([1L, 2L], Ids(wal.ReadLogsRange(0, 1)));
        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(wal.ReadLogsRange(8, 1)));
        Assert.Equal(5, wal.GetMaxLog(8));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(0, snapshotIndex: 1, lastIncludedTerm: 99, sync: true);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated);
        Assert.Equal([1L], Ids(wal.ReadLogsRange(0, 1)));
        Assert.Equal(99, wal.GetTermAt(0, 1));
        Assert.Equal(1, wal.GetLastCheckpoint(0));
        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(wal.ReadLogsRange(8, 1)));
        Assert.Equal(5, wal.GetMaxLog(8));
        Assert.Equal(1, wal.GetTermAt(8, 5));
        Assert.Equal(-1, wal.GetLastCheckpoint(8));

        // The partition delete is a range delete bounded at partition 0's own keys.
        Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(0));
        Assert.Equal(0, wal.GetMaxLog(0));
        Assert.Equal(-1, wal.GetLastCheckpoint(0));
        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(wal.ReadLogsRange(8, 1)));
        Assert.Equal(5, wal.GetMaxLog(8));
    }

    // The Raft layer issues a truncation again on a retry, so the same cut twice must be a no-op.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_repeated_truncation_is_a_no_op_and_one_below_the_first_entry_empties_the_partition(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 5, 1, RaftLogType.Committed))]));
        for (int attempt = 0; attempt < 2; attempt++)
        {
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 3));
            Assert.Equal([1L, 2L, 3L], Ids(wal.ReadLogsRange(1, 1)));
            Assert.Equal(3, wal.GetMaxLog(1));
        }

        for (int attempt = 0; attempt < 2; attempt++)
        {
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 0));
            Assert.Empty(wal.ReadLogsRange(1, 1));
            Assert.Equal(0, wal.GetMaxLog(1));
            Assert.Equal(0, wal.GetCurrentTerm(1));
        }
    }
}
