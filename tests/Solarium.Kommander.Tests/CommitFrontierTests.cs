using Kommander.Data;
using Solarium.Storage;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The persisted commit frontier of <see cref="SolariumWAL"/>: a contiguous <c>Committed</c> marker
/// becomes one small metadata put instead of a rewrite of its row, reads derive the type from the
/// frontier, and every path that removes rows (truncation, the boundary, a partition delete) clamps
/// it. Ported from Kommander's <c>TestRocksDbCommitFrontier</c> and
/// <c>TestRocksDbWALFrontierClampsAndAttribution</c> at commit 1bee500.
/// </summary>
public sealed class CommitFrontierTests : IDisposable
{
    private const int Partition = 1;

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Contiguous_commits_are_absorbed_into_the_frontier_and_read_committed_after_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        byte[] payload = [1, 2, 3, 4];
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5, payload), Proposed(2, 5, payload), Proposed(3, 5, payload)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));
            Assert.Equal(3, wal.CommitMarkersAbsorbed);

            AssertCommitted(wal.ReadLogs(Partition));
        }

        // The commits survive the reopen through the frontier, not through rewritten rows.
        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        AssertCommitted(reopened.ReadLogs(Partition));

        void AssertCommitted(List<RaftLog> logs)
        {
            Assert.Equal(3, logs.Count);
            Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
            Assert.All(logs, log => Assert.Equal(payload, log.LogData));
            Assert.All(logs, log => Assert.Equal(5, log.Term));
        }
    }

    // A Proposed row has no type field (proto3 leaves out a zero), so the header reader reaches the
    // log type and the payload. With log type "op", a reader that skipped a length-delimited field one
    // byte short would read the payload's length, 32, as the tag of the type field, and the payload's
    // first byte as the type: Committed. The proposed-tail sweep would then keep an uncommitted entry.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_proposed_row_whose_payload_looks_like_a_type_field_still_reads_as_proposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        byte[] payload = new byte[32];
        payload[0] = (byte)RaftLogType.Committed;
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5, payload), Proposed(2, 5, payload), Proposed(3, 5, payload)])]));

        Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, 0));

        Assert.Empty(wal.ReadLogs(Partition));
        Assert.Equal(0, wal.GetMaxLog(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_duplicate_commit_below_the_frontier_succeeds_and_writes_nothing(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5)])]));
        Assert.Equal(1, wal.CommitMarkersAbsorbed);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5)])]));

        Assert.Equal(RaftLogType.Committed, Assert.Single(wal.ReadLogs(Partition)).Type);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_commit_over_a_gap_writes_its_row_and_the_frontier_catches_up_over_it_later(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        byte[] payload = [9, 9];
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5, payload), Proposed(2, 5, payload), Proposed(3, 5, payload)])]));

            // Id 1 is unresolved, so the commit of 2 cannot chain from the frontier: a full row.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(2, 5, payload)])]));
            Assert.Equal(0, wal.CommitMarkersAbsorbed);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5, payload)])]));
            Assert.Equal(1, wal.CommitMarkersAbsorbed);

            // Id 2 is resolved on disk by its full row, so the frontier walks over it to absorb 3.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(3, 5, payload)])]));
            Assert.Equal(2, wal.CommitMarkersAbsorbed);
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        List<RaftLog> logs = reopened.ReadLogs(Partition);
        Assert.Equal([1L, 2L, 3L], Ids(logs));
        Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_commit_with_another_term_rewrites_the_row(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 7)])]));
        Assert.Equal(0, wal.CommitMarkersAbsorbed);

        RaftLog log = Assert.Single(wal.ReadLogs(Partition));
        Assert.Equal(RaftLogType.Committed, log.Type);
        Assert.Equal(7, log.Term);
    }

    // A row written later at a truncated id must not read as Committed through a stale frontier.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_truncation_clamps_the_frontier_so_a_new_row_at_a_cut_id_reads_proposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5), Proposed(2, 5), Proposed(3, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 1));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(2, 9)])]));

            List<RaftLog> logs = wal.ReadLogs(Partition);
            Assert.Equal([1L, 2L], Ids(logs));
            Assert.Equal(RaftLogType.Committed, logs[0].Type);
            Assert.Equal(RaftLogType.Proposed, logs[1].Type);
            Assert.Equal(9, logs[1].Term);
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        List<RaftLog> reread = reopened.ReadLogs(Partition);
        Assert.Equal(RaftLogType.Committed, reread[0].Type);
        Assert.Equal(RaftLogType.Proposed, reread[1].Type);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_proposed_tail_sweep_keeps_the_rows_the_frontier_commits(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5), Proposed(2, 5), Proposed(3, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));
        Assert.Equal(3, wal.CommitMarkersAbsorbed);

        // The rows are still Proposed on disk; the frontier certifies them.
        Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, 0));

        List<RaftLog> logs = wal.ReadLogs(Partition);
        Assert.Equal(3, logs.Count);
        Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void After_a_backfill_of_committed_rows_the_frontier_catches_up_and_absorbs_the_next_commit(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        // Rows that arrive already committed are first writes: there is no frontier to chain from.
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Range(1, 5, 5, RaftLogType.Committed))]));
        Assert.Equal(0, wal.CommitMarkersAbsorbed);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(6, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(6, 5)])]));
        Assert.Equal(1, wal.CommitMarkersAbsorbed);

        Assert.Equal(RaftLogType.Committed, wal.ReadLogs(Partition).Single(log => log.Id == 6).Type);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_frontier_hops_a_compacted_prefix_that_the_checkpoint_certifies(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        List<RaftLog> logs = Range(1, 9, 5, RaftLogType.Committed);
        logs.Add(Checkpoint(10, 5));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, logs)]));
        Assert.Equal(10, wal.GetLastCheckpoint(Partition));

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, 10, 100);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(9, removed);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(11, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(11, 5)])]));
        Assert.Equal(1, wal.CommitMarkersAbsorbed);

        Assert.Equal(RaftLogType.Committed, wal.ReadLogs(Partition).Single(log => log.Id == 11).Type);
    }

    // The frontier never certifies a row it did not verify against its own commit marker.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_proposed_row_below_a_checkpoint_still_reads_proposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5), Committed(2, 5), Checkpoint(3, 5)])]));

        List<RaftLog> logs = wal.ReadLogsRange(Partition, 1);
        Assert.Equal(RaftLogType.Proposed, logs.Single(log => log.Id == 1).Type);
        Assert.Equal(RaftLogType.Committed, logs.Single(log => log.Id == 2).Type);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_with_a_conflicting_term_clamps_the_frontier_to_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5), Proposed(2, 5), Proposed(3, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 2, lastIncludedTerm: 99, sync: false);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(3, 100)])]));

        List<RaftLog> logs = wal.ReadLogs(Partition);
        RaftLog boundary = logs.Single(log => log.Id == 2);
        Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
        Assert.Equal(99, boundary.Term);

        RaftLog refilled = logs.Single(log => log.Id == 3);
        Assert.Equal(RaftLogType.Proposed, refilled.Type);
        Assert.Equal(100, refilled.Term);
    }

    // A truncating boundary deletes rows the frontier certified; a fresh Proposed row at one of
    // those ids would otherwise read as Committed, an entry no quorum agreed on.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_truncating_boundary_clamps_a_stale_frontier_so_fresh_rows_stay_unresolved(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 5, 1, RaftLogType.Proposed))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 5, 1, RaftLogType.Committed))]));
        Assert.Equal(5, wal.CommitMarkersAbsorbed);
        Assert.Equal(RaftLogType.Committed, wal.ReadLogsRange(1, 5, maxEntries: 1).Single().Type);

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 2, lastIncludedTerm: 9, sync: true);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Proposed(4, 9)])]));

        RaftLog fresh = wal.ReadLogsRange(1, 4, maxEntries: 1).Single();
        Assert.Equal(RaftLogType.Proposed, fresh.Type);
        Assert.Equal(9, fresh.Term);
        Assert.Equal([1L, 2L, 4L], Ids(wal.ReadLogsRange(1, 1)));
        Assert.Equal(2, wal.GetLastCheckpoint(1));
        Assert.Equal(9, wal.GetTermAt(1, 2));
        Assert.Equal(4, wal.GetMaxLog(1));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Deleting_a_partition_clears_its_frontier_for_a_reused_partition_id(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5), Proposed(2, 5)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Committed(2, 5)])]));

        Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(Partition));
        Assert.Empty(wal.ReadLogs(Partition));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 8)])]));

        RaftLog log = Assert.Single(wal.ReadLogs(Partition));
        Assert.Equal(RaftLogType.Proposed, log.Type);
        Assert.Equal(8, log.Term);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_compaction_keeps_the_checkpoint_row_and_the_tail_across_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            List<RaftLog> logs = Range(1, 9, 5, RaftLogType.Committed, [1]);
            logs.Add(Checkpoint(10, 5));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, logs)]));
            Assert.Equal(10, wal.GetLastCheckpoint(Partition));

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 10, compactNumberEntries: 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(9, removed);
            Assert.Equal(1, wal.LastCompactionWriteCount);

            Assert.Equal(10, Assert.Single(wal.ReadLogs(Partition)).Id);
            Assert.Equal(1, wal.CountPersistedLogs(Partition));
            Assert.Equal(10, wal.GetMaxLog(Partition));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        RaftLog survivor = Assert.Single(reopened.ReadLogs(Partition));
        Assert.Equal(10, survivor.Id);
        Assert.Equal(RaftLogType.CommittedCheckpoint, survivor.Type);
    }

    // More markers than one MultiGet takes, so the prefetch reads the target headers in several
    // chunks, mixed with the other probe outcomes; the decisions must match the point probes'.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Many_markers_in_one_batch_decide_through_the_prefetch_as_point_probes_would(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        byte[] payload = [7, 7, 7];
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            // Id 150 holds a stale term: its marker must not be absorbed over it.
            List<RaftLog> proposals = [];
            for (long id = 1; id <= 300; id++)
                proposals.Add(Proposed(id, id == 150 ? 4 : 5, payload));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, proposals)]));

            //   150: another term -> a full row, and the frontier stops below it;
            //   301: no row       -> a full row (a backfill's first write);
            //   302: its proposal is staged earlier in the batch -> absorbed from the staged set.
            List<RaftLog> batch = [];
            for (long id = 1; id <= 301; id++)
                batch.Add(Committed(id, 5, payload));

            batch.Add(Proposed(302, 5, payload));
            batch.Add(Committed(302, 5, payload));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, batch)]));

            // Absorbed: 1..149, 151..300 (the walk passes 150 on its staged full row), and 302.
            Assert.Equal(149 + 150 + 1, wal.CommitMarkersAbsorbed);
            AssertAllCommitted(wal.ReadLogs(Partition));
            Assert.Equal(301, wal.MarkerHeadersPrefetched);
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        AssertAllCommitted(reopened.ReadLogs(Partition));

        void AssertAllCommitted(List<RaftLog> logs)
        {
            Assert.Equal(Enumerable.Range(1, 302).Select(i => (long)i), Ids(logs));
            Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
            Assert.All(logs, log => Assert.Equal(5, log.Term));
            Assert.All(logs, log => Assert.Equal(payload, log.LogData));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Markers_of_rows_already_present_are_prefetched_and_absorbed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 4, 1, RaftLogType.Proposed))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Committed(1), Committed(2)])]));

        Assert.Equal(2, wal.CommitMarkersAbsorbed);
        Assert.True(wal.MarkerHeadersPrefetched >= 2, "the batched header prefetch did not run for the markers");
        Assert.Equal([RaftLogType.Committed, RaftLogType.Committed, RaftLogType.Proposed, RaftLogType.Proposed],
            wal.ReadLogsRange(1, 1).Select(log => log.Type));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Bounded_scans_stop_at_their_partition_past_a_truncated_suffix_on_a_shared_shard(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        // Partitions 1 and 9 share shard 1, so 9's rows follow 1's truncated suffix in the family.
        const int Sibling = 9;
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Range(1, 20, 5, RaftLogType.Committed)), (Sibling, Range(1, 20, 5, RaftLogType.Committed))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(21, 5)])]));

        // Deletes for 22..40 above the checkpoint: the run each partition scan must not walk into.
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Range(22, 40, 5, RaftLogType.Proposed))]));
        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 21));

        Assert.Equal(Enumerable.Range(1, 21).Select(i => (long)i), Ids(wal.ReadLogsRange(Partition, 1)));
        Assert.Equal([21L], Ids(wal.ReadLogs(Partition)));
        Assert.Equal(21, wal.CountPersistedLogs(Partition));
        Assert.Equal(20, wal.CountRemovableLogs(Partition));

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 21, compactNumberEntries: 100);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(20, removed);
        Assert.Equal(1, wal.CountPersistedLogs(Partition));
        Assert.Equal(0, wal.CountRemovableLogs(Partition));

        Assert.Equal(20, wal.CountPersistedLogs(Sibling));
        Assert.Equal(Enumerable.Range(1, 20).Select(i => (long)i), Ids(wal.ReadLogsRange(Sibling, 1)));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_cut_between_checkpoints_restores_the_lower_one_and_returns_the_surviving_max(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1,
        [
            Committed(1), Committed(2), Checkpoint(3), Committed(4), Committed(5),
            Committed(6), Checkpoint(7), Committed(8), Committed(9),
        ])]));
        Assert.Equal(7, wal.GetLastCheckpoint(1));

        Assert.Equal((RaftOperationStatus.Success, 5L), wal.TruncateLogsAfterAndGetMax(1, afterLogId: 5));

        Assert.Equal(3, wal.GetLastCheckpoint(1));
        Assert.Equal(5, wal.GetMaxLog(1));
        Assert.Equal([3L, 4L, 5L], Ids(wal.ReadLogs(1)));
    }

    // With the only checkpoint gone, a recorded one would certify a prefix whose anchor row is gone.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_cut_that_removes_the_only_checkpoint_clears_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        List<RaftLog> logs = Range(1, 6, 1, RaftLogType.Committed);
        logs.Add(Checkpoint(7));
        logs.Add(Committed(8));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, logs)]));
        Assert.Equal(7, wal.GetLastCheckpoint(1));

        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 5));

        Assert.Equal(-1, wal.GetLastCheckpoint(1));
        Assert.Equal([1L, 2L, 3L, 4L, 5L], Ids(wal.ReadLogsRange(1, 1)));
        Assert.Equal(5, wal.GetMaxLog(1));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_that_keeps_the_suffix_keeps_a_higher_checkpoint(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        List<RaftLog> logs = Range(1, 5, 1, RaftLogType.Committed);
        logs.Add(Checkpoint(6));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, logs)]));
        Assert.Equal(6, wal.GetLastCheckpoint(1));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 2, lastIncludedTerm: 1, sync: true);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.False(truncated);

        Assert.Equal(6, wal.GetLastCheckpoint(1));
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], Ids(wal.ReadLogsRange(1, 1)));
        Assert.Equal(6, wal.GetMaxLog(1));
    }

    // Row 1 is Committed and row 2 Proposed, and the frontier is 0. A duplicate commit of 1 reads the
    // frontier and waits in its probe of row 1's table; a commit of 2 then walks over row 1 and
    // absorbs 2. Without one lock per partition, the commit of 2 lands while the commit of 1 waits,
    // and the commit of 1 then puts frontier 1 over 2: both succeed with sync on, and after a reopen
    // row 2 reads Proposed. With it, the commit of 2 waits for the commit of 1, and reads its frontier.
    [Theory]
    [MemberData(nameof(FrontierRaceCases))]
    public async Task A_lower_commit_that_lands_last_does_not_lower_the_persisted_frontier(string environment, bool lowerBatched, bool higherBatched)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        ReadGateEnvironment gated = new(scope.Environment);
        using (SolariumWAL wal = Wals.Open(gated, _directory.Path, syncWrites: true))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Proposed(2, 5)])]));
            wal.FlushMemTablesForTesting(wait: true);
            Assert.Equal([RaftLogType.Committed, RaftLogType.Proposed], wal.ReadLogs(Partition).Select(log => log.Type));
            Assert.Equal(0, wal.CommitMarkersAbsorbed);

            CancellationToken cancel = TestContext.Current.CancellationToken;
            Task<RaftOperationStatus> lower = Task.Run(() =>
            {
                ReadGateEnvironment.GateNextTableReadOnThisThread();
                return wal.Write(Marker(1, lowerBatched));
            }, cancel);
            Assert.True(gated.Started.Wait(TimeSpan.FromSeconds(30), cancel));
            Task<RaftOperationStatus> higher = Task.Run(() => wal.Write(Marker(2, higherBatched)), cancel);
            await Task.WhenAny(higher, Task.Delay(500, cancel));
            gated.Release();

            Assert.Equal(RaftOperationStatus.Success, await lower.WaitAsync(TimeSpan.FromSeconds(30), cancel));
            Assert.Equal(RaftOperationStatus.Success, await higher.WaitAsync(TimeSpan.FromSeconds(30), cancel));
            Assert.All(wal.ReadLogs(Partition), log => Assert.Equal(RaftLogType.Committed, log.Type));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);
        List<RaftLog> logs = reopened.ReadLogs(Partition);
        Assert.Equal([1L, 2L], Ids(logs));
        Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));

        // A batch adds an append to another partition, so the write takes the multi-entry path.
        static List<(int, List<RaftLog>)> Marker(long id, bool batched) =>
            batched ? [(Partition, [Committed(id, 5)]), (Partition + 1, [Proposed(id, 5)])] : [(Partition, [Committed(id, 5)])];
    }

    // A commit reads frontier 0 and waits in its probe while the partition is deleted. Unless the
    // delete waits for it, the commit then puts frontier 1 for a partition with no rows, and a reused
    // partition id's first Proposed row reads Committed.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_commit_that_overlaps_a_partition_delete_leaves_no_frontier_for_the_reused_id(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        ReadGateEnvironment gated = new(scope.Environment);
        using SolariumWAL wal = Wals.Open(gated, _directory.Path, syncWrites: true);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5), Proposed(2, 5)])]));
        wal.FlushMemTablesForTesting(wait: true);
        Assert.Equal(2, wal.ReadLogs(Partition).Count);

        CancellationToken cancel = TestContext.Current.CancellationToken;
        Task<RaftOperationStatus> commit = Task.Run(() =>
        {
            ReadGateEnvironment.GateNextTableReadOnThisThread();
            return wal.Write([(Partition, [Committed(1, 5)])]);
        }, cancel);
        Assert.True(gated.Started.Wait(TimeSpan.FromSeconds(30), cancel));
        Task<RaftOperationStatus> delete = Task.Run(() => wal.DeletePartitionWAL(Partition), cancel);
        await Task.WhenAny(delete, Task.Delay(500, cancel));
        gated.Release();

        Assert.Equal(RaftOperationStatus.Success, await commit.WaitAsync(TimeSpan.FromSeconds(30), cancel));
        Assert.Equal(RaftOperationStatus.Success, await delete.WaitAsync(TimeSpan.FromSeconds(30), cancel));
        Assert.Empty(wal.ReadLogs(Partition));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 6)])]));
        Assert.Equal(RaftLogType.Proposed, Assert.Single(wal.ReadLogs(Partition)).Type);
    }

    /// <summary>Each environment with a single entry and a batch for each of the two commits.</summary>
    public static TheoryData<string, bool, bool> FrontierRaceCases()
    {
        TheoryData<string, bool, bool> cases = new();
        foreach (string environment in Solarium.IoUring.IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
            foreach (bool lowerBatched in new[] { false, true })
                foreach (bool higherBatched in new[] { false, true })
                    cases.Add(environment, lowerBatched, higherBatched);

        return cases;
    }

    /// <summary>
    /// Forwards to another environment, and holds the first SSTable read of a thread that asked for
    /// it with <see cref="GateNextTableReadOnThisThread"/> until <see cref="Release"/>.
    /// </summary>
    private sealed class ReadGateEnvironment(IStorageEnvironment inner) : IStorageEnvironment
    {
        [ThreadStatic]
        private static bool t_gateNextRead;

        private readonly ManualResetEventSlim _gate = new();

        /// <summary>Set when the held read starts to wait.</summary>
        public ManualResetEventSlim Started { get; } = new();

        /// <summary>Holds the next SSTable read of the calling thread.</summary>
        public static void GateNextTableReadOnThisThread() => t_gateNextRead = true;

        /// <summary>Lets the held read go on.</summary>
        public void Release() => _gate.Set();

        /// <inheritdoc />
        public string Name => $"read-gated {inner.Name}";

        /// <inheritdoc />
        public IStorageFile OpenFile(string path)
        {
            IStorageFile file = inner.OpenFile(path);
            return path.EndsWith(".sst", StringComparison.Ordinal) ? new GatedFile(file, this) : file;
        }

        /// <inheritdoc />
        public bool FileExists(string path) => inner.FileExists(path);

        /// <inheritdoc />
        public string[] GetChildren(string directory) => inner.GetChildren(directory);

        /// <inheritdoc />
        public void DeleteFile(string path) => inner.DeleteFile(path);

        /// <inheritdoc />
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);

        /// <inheritdoc />
        public bool TryLinkFile(string source, string target) => inner.TryLinkFile(source, target);

        /// <inheritdoc />
        public void CreateDirectory(string directory) => inner.CreateDirectory(directory);

        /// <inheritdoc />
        public void FlushDirectory(string directory) => inner.FlushDirectory(directory);

        private sealed class GatedFile(IStorageFile file, ReadGateEnvironment owner) : IStorageFile
        {
            public long GetLength() => file.GetLength();

            public int Read(Span<byte> buffer, long offset)
            {
                if (t_gateNextRead)
                {
                    t_gateNextRead = false;
                    owner.Started.Set();
                    Assert.True(owner._gate.Wait(TimeSpan.FromSeconds(30)));
                }

                return file.Read(buffer, offset);
            }

            public void Write(ReadOnlySpan<byte> buffer, long offset) => file.Write(buffer, offset);

            public void Flush() => file.Flush();

            public void SyncRange(long offset, long length) => file.SyncRange(offset, length);

            public void SetLength(long length) => file.SetLength(length);

            public void Dispose() => file.Dispose();
        }
    }
}
