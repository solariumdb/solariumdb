using Kommander.Data;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The persisted last checkpoint and the snapshot boundary of <see cref="SolariumWAL"/>, across a
/// reopen and against a racing append. Ported from Kommander's <c>TestLastCheckpointReopen</c>,
/// <c>TestCheckpointOverGapFloor</c>, <c>TestSnapshotBoundaryReopen</c>,
/// <c>RocksDbSnapshotBoundaryConcurrencyTests</c> and one test of
/// <c>TestRocksDbWALFrontierClampsAndAttribution</c> at commit 1bee500.
/// </summary>
public sealed class CheckpointTests : IDisposable
{
    private const int Partition = 3;

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_last_checkpoint_survives_a_reopen_and_sets_where_replay_starts(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true))
        {
            wal.Write([(Partition, [Committed(1), Checkpoint(2), Committed(3)])]);
            Assert.Equal(2, wal.GetLastCheckpoint(Partition));
            Assert.Equal([2L, 3L], Ids(wal.ReadLogs(Partition)));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);
        Assert.Equal(2, reopened.GetLastCheckpoint(Partition));
        Assert.Equal([2L, 3L], Ids(reopened.ReadLogs(Partition)));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void With_no_checkpoint_a_reopen_replays_the_whole_log(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true))
        {
            wal.Write([(Partition, [Committed(1), Committed(2)])]);
            Assert.Equal(-1, wal.GetLastCheckpoint(Partition));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);
        Assert.Equal(-1, reopened.GetLastCheckpoint(Partition));
        Assert.Equal([1L, 2L], Ids(reopened.ReadLogs(Partition)));
    }

    // The last checkpoint certifies its whole prefix as applied and compactable, so a checkpoint row
    // that lands over a replication gap is stored but moves nothing until a later checkpoint finds
    // the gap filled.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_over_a_gap_moves_the_last_checkpoint_only_once_the_gap_is_filled(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);

        wal.Write([(Partition, [Committed(1), Committed(2), Committed(3)])]);

        wal.Write([(Partition, [Checkpoint(10)])]);
        Assert.Equal(-1, wal.GetLastCheckpoint(Partition));

        wal.Write([(Partition, Range(4, 9, 1, RaftLogType.Committed))]);
        Assert.Equal(-1, wal.GetLastCheckpoint(Partition));

        wal.Write([(Partition, [Checkpoint(11)])]);
        Assert.Equal(11, wal.GetLastCheckpoint(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_over_a_contiguous_prefix_moves_the_last_checkpoint_and_a_lower_one_never_lowers_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);

        // The prefix includes rows staged in the same batch.
        wal.Write([(Partition, [Committed(1), Committed(2), Committed(3), Checkpoint(4)])]);
        Assert.Equal(4, wal.GetLastCheckpoint(Partition));

        wal.Write([(Partition, [Checkpoint(2)])]);
        Assert.Equal(4, wal.GetLastCheckpoint(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_with_a_gap_inside_its_batch_does_not_move_the_last_checkpoint(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);

        wal.Write([(Partition, [Committed(1), Committed(2), Checkpoint(5)])]);

        Assert.Equal(-1, wal.GetLastCheckpoint(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_that_keeps_the_suffix_survives_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true))
        {
            wal.Write([(1, Range(1, 5, 2, RaftLogType.Committed))]);
            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 2, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);
        Assert.Equal(3, reopened.GetLastCheckpoint(1));
        Assert.Equal(2, reopened.GetTermAt(1, 3));
        Assert.Equal(5, reopened.GetMaxLog(1));
        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(reopened.ReadLogsRange(1, 1)));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_that_truncates_the_suffix_survives_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true))
        {
            wal.Write([(1, Range(1, 5, 2, RaftLogType.Committed))]);
            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 9, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(truncated);
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);
        Assert.Equal(3, reopened.GetLastCheckpoint(1));
        Assert.Equal(9, reopened.GetTermAt(1, 3));
        Assert.Equal(3, reopened.GetMaxLog(1));
        Assert.Equal([1L, 2L, 3L], Ids(reopened.ReadLogsRange(1, 1)));
    }

    // A boundary written unsynced still reaches the WAL before the call returns, so it survives a
    // clean close; only a power loss can take it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_unsynced_boundary_survives_a_clean_close(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Committed(1), Committed(2), Committed(3)])]));

            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 9, sync: false);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
            Assert.Equal(9, wal.GetTermAt(1, 3));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        Assert.Equal(9, reopened.GetTermAt(1, 3));
        Assert.Equal(3, reopened.GetLastCheckpoint(1));
        Assert.Equal(3, reopened.GetMaxLog(1));
    }

    // The boundary scans the suffix and then deletes it in one batch. An append that landed between
    // the two would survive the truncation, so the boundary holds the exclusive write guard across
    // both, and the append waits for it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_append_cannot_land_between_the_boundary_scan_and_its_batch(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        bool appendFinishedInside = true;
        Thread? append = null;
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true))
        {
            wal.Write([(1, Range(1, 5, 1, RaftLogType.Committed))]);

            wal.OnAfterBoundaryScanForTesting = () =>
            {
                append = new Thread(() => wal.Write([(1, [Committed(6)])])) { IsBackground = true };
                append.Start();
                appendFinishedInside = append.Join(TimeSpan.FromMilliseconds(500));
            };

            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 5, lastIncludedTerm: 2, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
            Assert.True(append!.Join(TimeSpan.FromSeconds(10)), "the append did not finish after the boundary released the guard");
        }

        Assert.False(appendFinishedInside, "the append finished while the boundary held the write guard");

        // The append landed after the boundary, so it is a row above it, not a survivor of its cut.
        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);
        Assert.Equal(5, reopened.GetLastCheckpoint(1));
        Assert.Equal(2, reopened.GetTermAt(1, 5));
        Assert.Equal(6, reopened.GetMaxLog(1));
    }
}
