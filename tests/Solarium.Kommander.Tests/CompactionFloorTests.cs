using Kommander.Data;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The persisted compaction floor of <see cref="SolariumWAL"/>: compaction deletes the log's dead
/// prefix logically, by a per-partition floor, and physically by whole files, so no log row is ever
/// rewritten. These tests pin the reads (the same as a physical delete) and the file layout that
/// makes the reclaim free: files that do not overlap, moves out of level 0, whole-file drops below
/// the floor, and tombstones confined to the memtable. Ported from Kommander's
/// <c>TestRocksDbCompactionFloor</c> and the whole-file drop test of <c>TestRocksDbCommitFrontier</c>
/// at commit 1bee500. Where those read RocksDB's LOG for moves, merges and flush reasons, these read
/// Solarium's per-family counts.
/// </summary>
public sealed class CompactionFloorTests : IDisposable
{
    private const int Partition = 1;

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    // ── Reads ────────────────────────────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Compaction_hides_the_prefix_below_the_floor_and_the_floor_survives_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 10))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11, 5)])]));

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 6, compactNumberEntries: 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(5, removed);
            Assert.Equal(1, wal.LastCompactionWriteCount);
            Assert.Equal(6, wal.GetCompactionFloorForTesting(Partition));

            // Rows 1 to 5 are still in the memtable, yet every read path reports them gone.
            Assert.Equal([6L, 7L, 8L, 9L, 10L, 11L], Ids(wal.ReadLogsRange(Partition, 0)));
            Assert.Equal([6L, 7L, 8L, 9L, 10L, 11L], Ids(wal.ReadLogsRange(Partition, 3)));
            Assert.Equal(6, wal.CountPersistedLogs(Partition));
            Assert.Equal(5, wal.CountRemovableLogs(Partition));
            Assert.Equal(11, wal.GetMaxLog(Partition));
            Assert.Equal(-1, wal.GetTermAt(Partition, 5));
            Assert.Equal(5, wal.GetTermAt(Partition, 6));

            // A second pass to the same floor has nothing to do and writes nothing.
            (status, removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 6, compactNumberEntries: 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(0, removed);
            Assert.Equal(0, wal.LastCompactionWriteCount);
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        Assert.Equal(6, reopened.GetCompactionFloorForTesting(Partition));
        Assert.Equal([6L, 7L, 8L, 9L, 10L, 11L], Ids(reopened.ReadLogsRange(Partition, 0)));
        Assert.Equal(6, reopened.CountPersistedLogs(Partition));
        Assert.Equal([11L], Ids(reopened.ReadLogs(Partition)));
    }

    // The cap bounds the counting scan, not the deletion: the floor goes to the caller's floor in
    // one pass, and the count past the cap is completed by arithmetic.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_capped_pass_moves_the_floor_all_the_way_and_counts_exactly(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 20))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(21, 5)])]));

        (_, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 21, compactNumberEntries: 4, maxTotalEntries: 8);
        Assert.Equal(20, removed);
        Assert.Equal(21, wal.GetCompactionFloorForTesting(Partition));
        Assert.Equal([21L], Ids(wal.ReadLogsRange(Partition, 0)));
        Assert.Equal(0, wal.CountRemovableLogs(Partition));

        (_, removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 21, compactNumberEntries: 100, maxTotalEntries: 100);
        Assert.Equal(0, removed);
        Assert.Equal(0, wal.LastCompactionWriteCount);
        Assert.Equal(21, wal.GetCompactionFloorForTesting(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_truncation_below_the_floor_clamps_it_so_the_rewritten_tail_reads(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 10))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11, 5)])]));
        wal.CompactLogsOlderThan(Partition, lastCheckpoint: 8, compactNumberEntries: 100);
        Assert.Equal(8, wal.GetCompactionFloorForTesting(Partition));

        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 3));
        Assert.Equal(4, wal.GetCompactionFloorForTesting(Partition));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(4, 7, [1, 2, 3]), Proposed(5, 7, [1, 2, 3])])]));
        Assert.Equal([4L, 5L], Ids(wal.ReadLogsRange(Partition, 0)));
        Assert.Equal(5, wal.GetMaxLog(Partition));
        Assert.Equal(7, wal.GetCurrentTerm(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_below_the_floor_clamps_it_so_the_boundary_row_reads(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 10))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11, 5)])]));
        wal.CompactLogsOlderThan(Partition, lastCheckpoint: 9, compactNumberEntries: 100);
        Assert.Equal(9, wal.GetCompactionFloorForTesting(Partition));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 5, lastIncludedTerm: 9, sync: false);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated);
        Assert.Equal(5, wal.GetCompactionFloorForTesting(Partition));

        RaftLog boundary = Assert.Single(wal.ReadLogsRange(Partition, 0));
        Assert.Equal(5, boundary.Id);
        Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
        Assert.Equal(5, wal.GetLastCheckpoint(Partition));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_boundary_below_a_floor_set_by_compaction_clamps_the_floor_and_truncates_above_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        List<RaftLog> logs = Range(1, 9, 1, RaftLogType.Committed);
        logs.Add(Checkpoint(10));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, logs)]));

        Assert.Equal((RaftOperationStatus.Success, 7), wal.CompactLogsOlderThan(1, lastCheckpoint: 8, compactNumberEntries: 100));
        Assert.Equal(8, wal.GetCompactionFloorForTesting(1));

        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 42, sync: true);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated, "the rows above the boundary (8, 9, 10) had to go");

        Assert.Equal(3, wal.GetCompactionFloorForTesting(1));
        Assert.Equal(42, wal.GetTermAt(1, 3));
        RaftLog boundary = Assert.Single(wal.ReadLogsRange(1, 1));
        Assert.Equal(3, boundary.Id);
        Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
        Assert.Equal(3, wal.GetLastCheckpoint(1));
        Assert.Equal(3, wal.GetMaxLog(1));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Deleting_a_partition_resets_its_floor_so_reused_ids_read(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 10))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11, 5)])]));
        wal.CompactLogsOlderThan(Partition, lastCheckpoint: 11, compactNumberEntries: 100);
        Assert.Equal(11, wal.GetCompactionFloorForTesting(Partition));

        Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(Partition));
        Assert.Equal(0, wal.GetCompactionFloorForTesting(Partition));
        Assert.Empty(wal.ReadLogsRange(Partition, 0));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 3))]));
        Assert.Equal([1L, 2L, 3L], Ids(wal.ReadLogsRange(Partition, 0)));
    }

    // Rows 1 to 5 are dead but still present; the marker for 7 must cross them as a compacted span
    // below the certified checkpoint, pass the checkpoint row at 6, and be absorbed.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_frontier_crosses_a_dead_but_present_prefix_as_compacted(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 5))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(6, 5)])]));
        wal.CompactLogsOlderThan(Partition, lastCheckpoint: 6, compactNumberEntries: 100);
        Assert.Equal(6, wal.GetCompactionFloorForTesting(Partition));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(7, 5, [1, 2, 3])])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(7, 5)])]));
        Assert.Equal(1, wal.CommitMarkersAbsorbed);

        Assert.Equal(RaftLogType.Committed, Assert.Single(wal.ReadLogsRange(Partition, 7)).Type);
    }

    // ── File layout ──────────────────────────────────────────────────────────────────────────────

    // The flushed files of an append-only partition do not overlap, so they leave level 0 by moves
    // under the same file numbers, and the drop below the floor removes exactly the dead ones.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Flushed_files_move_without_a_rewrite_and_the_files_below_the_floor_are_dropped_whole(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, options: new SolariumWalOptions { ShardLevel0FileNumCompactionTrigger = 2 });

        byte[] payload = Incompressible(4096);
        for (int chunk = 0; chunk < 4; chunk++)
        {
            long first = (chunk * 300) + 1;
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(first, first + 299, payload))]));
            wal.FlushMemTablesForTesting(wait: true);
        }

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(1201, 5)])]));

        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = Wals.WaitUntil(
            () => wal.GetShardLiveFilesForTesting(Partition),
            f => f.Count == 4 && f.All(x => x.Level == 6),
            Timeout);
        AssertNonOverlapping(files);

        (long flushes, long moves, long merges) = Wals.ShardCounts(wal, Partition);
        Assert.Equal(4, flushes);
        Assert.True(moves > 0, "nothing moved out of level 0");
        Assert.Equal(0, merges);

        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> dead = [.. files.Where(f => f.LargestId < 699)];
        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> live = [.. files.Where(f => f.LargestId >= 699)];
        Assert.Equal(2, dead.Count);
        Assert.Equal(2, live.Count);
        long deadBytes = dead.Sum(f => f.SizeBytes);
        long before = wal.GetShardLiveSstBytes();
        Assert.Equal(files.Sum(f => f.SizeBytes), before);

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 700, compactNumberEntries: 100, maxTotalEntries: 1000);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(699, removed);

        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> after = wal.GetShardLiveFilesForTesting(Partition);
        foreach ((string name, _, _, _, _) in dead)
            Assert.DoesNotContain(after, f => f.Name == name);

        foreach ((string name, _, _, _, long sizeBytes) in live)
            Assert.Contains(after, f => f.Name == name && f.SizeBytes == sizeBytes);

        Assert.Equal(deadBytes, before - wal.GetShardLiveSstBytes());

        List<RaftLog> visible = wal.ReadLogsRange(Partition, 0);
        Assert.Equal(700, visible[0].Id);
        Assert.Equal(1201, visible[^1].Id);
        Assert.Equal(502, visible.Count);
        Assert.All(visible.Where(l => l.Id <= 1200), l => Assert.Equal(payload, l.LogData));
    }

    // One capped pass moves the floor to the checkpoint and drops the whole dead prefix once it is
    // at the bottom level; the count past the cap is completed by arithmetic.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void One_capped_pass_reclaims_a_dead_prefix_at_the_bottom_level(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        byte[] payload = Incompressible(4096);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 2000, payload))]));
        wal.FlushMemTablesForTesting(wait: true);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(2001, 5)])]));
        wal.FlushMemTablesForTesting(wait: true);

        // The two tables overlap nothing, so the compaction moves them to the bottom level as they are.
        wal.CompactShardToBottomForTesting(Partition);
        Assert.Equal(0, wal.GetShardLevel0FileCount());
        Assert.Equal((2L, 1L, 0L), Wals.ShardCounts(wal, Partition));
        long before = wal.GetShardLiveSstBytes();
        Assert.True(before > 2000 * 4096, $"expected the payload prefix on disk, saw {before} bytes");

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 2001, compactNumberEntries: 100, maxTotalEntries: 500);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(2000, removed);

        long after = wal.GetShardLiveSstBytes();
        Assert.True(after < before / 100, $"the whole-file drop did not reclaim the dead prefix: {before} -> {after} bytes");
        Assert.Equal(2001, Assert.Single(wal.ReadLogs(Partition)).Id);
    }

    // Rows that die in the active memtable never reach disk, and because the tombstone starts at or
    // above the memtable's own first row, it does not widen the flushed file's range below it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_confined_tombstone_drops_memtable_rows_at_flush_and_keeps_the_files_apart(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        byte[] payload = Incompressible(4096);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 2000, payload))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(2001, 5)])]));
        (_, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 1500, compactNumberEntries: 100, maxTotalEntries: 2000);
        Assert.Equal(1499, removed);
        Assert.Equal(1, wal.LastCompactionWriteCount);

        wal.FlushMemTablesForTesting(wait: true);

        long flushed = wal.GetShardLiveSstBytes();
        Assert.True(flushed < 3_500_000, $"the confined tombstone should have dropped about 1,400 of 2,000 rows at flush; {flushed} bytes reached disk");

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(2002, 4000, payload))]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(4001, 5)])]));
        (_, removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 3500, compactNumberEntries: 100, maxTotalEntries: 5000);
        Assert.Equal(2000, removed);

        wal.FlushMemTablesForTesting(wait: true);

        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = wal.GetShardLiveFilesForTesting(Partition);
        Assert.True(files.Count >= 2, $"expected two flushed files, saw {files.Count}");
        AssertNonOverlapping(files);
        Assert.Contains(files, f => f.SmallestId >= 2001);
        Assert.Equal(3500, wal.ReadLogsRange(Partition, 0)[0].Id);
        Assert.Equal(4001, wal.GetMaxLog(Partition));
    }

    // Partitions 1 and 9 share a shard, so its files hold both and cannot be dropped for one: the
    // pass falls back to a tombstone over the whole dead range, which a merge applies.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_shared_shard_uses_a_full_tombstone_that_a_merge_applies(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);
        const int Other = Partition + 8;

        byte[] payload = Incompressible(2048);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(1, 500, payload)), (Other, CommittedRange(1, 500, payload))]));
        wal.FlushMemTablesForTesting(wait: true);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(501, 5)]), (Other, [Checkpoint(501, 5)])]));

        (_, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 401, compactNumberEntries: 100, maxTotalEntries: 1000);
        Assert.Equal(400, removed);
        Assert.Equal(1, wal.LastCompactionWriteCount);

        wal.FlushMemTablesForTesting(wait: true);
        wal.CompactShardToBottomForTesting(Partition);

        // The merge met the tombstone: partition 1's first row on disk is its first live one.
        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = wal.GetShardLiveFilesForTesting(Partition);
        Assert.NotEmpty(files);
        Assert.Equal(1, Wals.ShardCounts(wal, Partition).Merges);
        Assert.All(files, f => Assert.True(f.SmallestId >= 401, $"dead row {f.SmallestId} survived the merge"));

        Assert.Equal(401, wal.ReadLogsRange(Partition, 0)[0].Id);
        Assert.Equal(501, wal.ReadLogsRange(Other, 0).Count);
        Assert.Equal(0, wal.GetCompactionFloorForTesting(Other));
    }

    // Sustained append-then-compact with small memtables: every file that leaves level 0 does so by
    // a move, and the live bytes track the retained window, not the bytes ingested.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Sustained_ingest_never_rewrites_a_log_file_and_the_footprint_tracks_the_retained_window(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SolariumWalOptions options = new()
        {
            ShardWriteBufferSizeBytes = 2L * 1024 * 1024,
            ShardMaxWriteBufferNumber = 3,
            ShardLevel0FileNumCompactionTrigger = 4,
        };
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, options: options);

        byte[] payload = Incompressible(512);
        const long Total = 60_000;
        const long Lag = 3_000;
        long ingested = 0;
        for (long id = 1; id <= Total; id += 100)
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(id, id + 99, payload))]));
            ingested += 100 * payload.Length;
            if (id % 5_000 == 1 && id > Lag)
            {
                (RaftOperationStatus status, _) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: id + 99 - Lag, compactNumberEntries: 100, maxTotalEntries: 10_000);
                Assert.Equal(RaftOperationStatus.Success, status);
            }
        }

        wal.FlushMemTablesForTesting(wait: true);
        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = Wals.WaitUntil(
            () => wal.GetShardLiveFilesForTesting(Partition),
            f => f.Count(x => x.Level == 0) < options.ShardLevel0FileNumCompactionTrigger,
            Timeout);
        AssertNonOverlapping(files);

        (long flushes, long moves, long merges) = Wals.ShardCounts(wal, Partition);
        Assert.True(flushes > 5, $"only {flushes} flushes; the memtables should have filled many times");
        Assert.True(moves > 0, "nothing moved out of level 0");
        Assert.Equal(0, merges);

        long live = wal.GetShardLiveSstBytes();
        Assert.True(live < ingested / 2, $"the live SSTables hold {live} bytes; they should track the retained window, not the {ingested} bytes ingested");
    }

    // The metadata family (frontier and floor puts) never fills its memtable, and a WAL file stays
    // until every family with data in it has flushed, so without a cadence of its own it would keep
    // every WAL file since its last flush. The WAL flushes it once per flush unit of shard rows.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_metadata_flush_cadence_releases_wal_files(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SolariumWalOptions options = new() { ShardWriteBufferSizeBytes = 1L * 1024 * 1024, ShardMaxWriteBufferNumber = 3 };
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, options: options);

        byte[] payload = Incompressible(1024);
        const long Total = 40_000;
        for (long id = 1; id <= Total; id += 100)
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, CommittedRange(id, id + 99, payload))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(id + 99, 5)])]));
        }

        long walBytes = Wals.WaitUntil(wal.GetAliveWriteAheadLogBytes, bytes => bytes < 6L * 1024 * 1024, Timeout);
        Assert.True(walBytes < 6L * 1024 * 1024, $"{walBytes >> 20} MiB of WAL files for about 40 MiB written at a 1 MiB flush unit");
        Assert.True(wal.DatabaseForTesting.FamilyCounts(wal.DatabaseForTesting.GetColumnFamily("metadata")).Flushes > 0, "the metadata family never flushed");

        Assert.Equal(Total, wal.GetMaxLog(Partition));
        Assert.Equal(RaftLogType.Committed, wal.ReadLogsRange(Partition, Total, 1)[0].Type);
    }

    // A shard that only sees a trickle never fills its memtable and keeps every WAL file written after
    // its first unflushed row. The bound is the WAL size cap: crossing it flushes the families that
    // keep the oldest WAL file. The run without the cap is the control that makes the bound a claim.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_wal_size_cap_flushes_a_trickle_shard_and_releases_wal_files(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SolariumWalOptions options = new() { ShardWriteBufferSizeBytes = 2L * 1024 * 1024, ShardMaxWriteBufferNumber = 4 };
        long flushUnit = options.ShardWriteBufferSizeBytes;
        const long Ingested = 40L * 1024 * 1024;

        (long uncappedWalBytes, long uncappedTrickle, long uncappedBusy) = TrickleAndBulk(scope, "uncapped", options with { MaxTotalWalSizeFlushUnits = 0 }, Ingested);
        Assert.Equal(0, uncappedTrickle);
        Assert.True(uncappedWalBytes > Ingested / 2, $"without the cap the trickle shard should keep the WAL files, but only {uncappedWalBytes >> 20} MiB of {Ingested >> 20} MiB is alive");

        (long cappedWalBytes, long cappedTrickle, long cappedBusy) = TrickleAndBulk(scope, "capped", options, Ingested);
        Assert.True(cappedTrickle > 0, "the cap did not flush the trickle shard");
        Assert.True(cappedWalBytes < 4 * flushUnit, $"{cappedWalBytes >> 20} MiB of WAL files under the cap, for {Ingested >> 20} MiB written at a {flushUnit >> 20} MiB flush unit");

        // The busy shard flushes each flush unit on its own; the cap must not become its cadence.
        Assert.True(cappedBusy <= uncappedBusy + 2, $"the cap added flushes to the busy shard: {cappedBusy} with it, {uncappedBusy} without");
    }

    // Writes Ingested bytes of 1 KiB rows to partition 1 (shard1), one small row to partition 0
    // (shard0) per 1,000 of them, and a metadata put per batch, waits for the flushes the cap starts,
    // and returns the WAL bytes alive and the flushes of each shard.
    private (long WalBytes, long TrickleFlushes, long BusyFlushes) TrickleAndBulk(EnvironmentScope scope, string name, SolariumWalOptions options, long ingested)
    {
        const int Trickle = 0;
        const int Busy = 1;
        using SolariumWAL wal = Wals.Open(scope.Environment, Path.Combine(_directory.Path, name), options: options);

        byte[] payload = Incompressible(1024);
        long rows = ingested / payload.Length / 100 * 100;
        long trickleId = 1;
        for (long id = 1; id <= rows; id += 100)
        {
            if ((id - 1) % 1000 == 0)
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Trickle, [Committed(trickleId++, 5, [1, 2, 3])])]));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Busy, CommittedRange(id, id + 99, payload))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Busy, [Committed(id + 99, 5)])]));
        }

        long alive = Wals.WaitUntil(
            wal.GetAliveWriteAheadLogBytes,
            bytes => bytes < 4 * options.ShardWriteBufferSizeBytes,
            options.MaxTotalWalSizeFlushUnits > 0 ? Timeout : TimeSpan.FromMilliseconds(200),
            failOnTimeout: false);

        Assert.Equal(rows, wal.GetMaxLog(Busy));
        Assert.Equal(trickleId - 1, wal.GetMaxLog(Trickle));
        Assert.Equal(RaftLogType.Committed, wal.ReadLogsRange(Trickle, 1, 1)[0].Type);
        return (alive, Wals.ShardCounts(wal, Trickle).Flushes, Wals.ShardCounts(wal, Busy).Flushes);
    }

    private static void AssertNonOverlapping(List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files)
    {
        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> ordered = [.. files.OrderBy(f => f.SmallestId)];
        for (int i = 1; i < ordered.Count; i++)
            Assert.True(ordered[i - 1].LargestId < ordered[i].SmallestId,
                $"files overlap: {ordered[i - 1].Name} [{ordered[i - 1].SmallestId}, {ordered[i - 1].LargestId}] and {ordered[i].Name} [{ordered[i].SmallestId}, {ordered[i].LargestId}]");
    }
}
