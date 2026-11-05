using Kommander.Data;
using Kommander.WAL;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The storage-damage contract of <see cref="SolariumWAL"/> against real files: the Raft term and vote are flushed
/// before <see cref="IWAL.PersistHardState"/> reports them durable; the acknowledged-presence watermark lands with the
/// batch that carries it, survives a reopen, and is clamped by every path that removes rows; and damage to the
/// database's files fail-stops the WAL. Fail-stopped, every write is refused without a reopen, reads of intact data
/// still serve, a scan over the damage throws instead of ending short, and a damaged table refuses the open. Ported
/// from Kommander's <c>TestStorageCorruptionAndLostWrites</c> at commit e2763bc.
/// </summary>
public sealed class StorageDamageTests : IDisposable
{
    private const int Partition = 1;

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    private string Root => _directory.Path;

    // ── Hard state ──────────────────────────────────────────────────────────────────────────

    // A vote reply leaves after PersistHardState returns: with syncWrites the vote must be on the device by then, so
    // a WAL that cannot flush it reports it not durable. With syncWrites off the node already accepts losing writes
    // the system has not flushed, and the hard state follows the log.
    [Theory]
    [MemberData(nameof(SyncCases))]
    public void A_vote_is_reported_durable_only_after_its_flush(string environment, bool syncWrites)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumWAL wal = Wals.Open(hooks, Root, syncWrites);
        IWAL hardState = wal;

        long before = wal.SyncMetadataWrites;
        Assert.True(hardState.PersistHardState(Partition, currentTerm: 3, votedFor: "node-b"));
        Assert.True(hardState.PersistHardState(Partition, currentTerm: 4, votedFor: null));
        Assert.Equal(syncWrites ? before + 2 : before, wal.SyncMetadataWrites);
        Assert.True(hardState.TryGetHardState(Partition, out long term, out string? votedFor));
        Assert.Equal(4, term);
        Assert.Null(votedFor);

        hooks.FailingPaths = path => path.EndsWith(".log", StringComparison.Ordinal);
        hooks.FailFlushes = true;
        Assert.Equal(!syncWrites, hardState.PersistHardState(Partition, currentTerm: 5, votedFor: "node-c"));

        // The close flushes the WAL too.
        hooks.FailFlushes = false;
    }

    /// <summary>Each environment, with syncWrites on and off.</summary>
    public static TheoryData<string, bool> SyncCases()
    {
        TheoryData<string, bool> cases = new();
        foreach (string environment in Environments.Available().Select(row => (string)row.Data))
        {
            cases.Add(environment, true);
            cases.Add(environment, false);
        }

        return cases;
    }

    // ── Acknowledged presence ───────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_watermark_lands_with_its_batch_survives_a_reopen_and_is_clamped_by_every_row_removal(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            IWAL asInterface = wal;
            Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition));

            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Logs.Range(1, 10, 1, RaftLogType.Committed))], sync: true, Watermark(10)));
            Assert.Equal(10, wal.GetAcknowledgedPresence(Partition));

            // A batch without a watermark leaves it alone; a batch with no rows for the partition (the correction
            // after a failed write) assigns it, lower included.
            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Logs.Range(11, 11, 1, RaftLogType.Committed))], sync: true));
            Assert.Equal(10, wal.GetAcknowledgedPresence(Partition));
            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([], sync: true, Watermark(9)));
            Assert.Equal(9, wal.GetAcknowledgedPresence(Partition));
            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([], sync: true, Watermark(11)));
            Assert.Equal(11, wal.GetAcknowledgedPresence(Partition));
        }

        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            IWAL asInterface = wal;
            Assert.Equal(11, wal.GetAcknowledgedPresence(Partition));

            // A truncation clamps it to the cut, in the batch of the deletes.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 5));
            Assert.Equal(5, wal.GetAcknowledgedPresence(Partition));
            Assert.Equal(5, wal.GetMaxLog(Partition));

            // A sweep of Proposed rows that removes one inside the acknowledged prefix clamps it too.
            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Logs.Range(6, 9, 1, RaftLogType.Proposed))], sync: true, Watermark(9)));
            Assert.Equal(9, wal.GetAcknowledgedPresence(Partition));
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, 7));
            Assert.Equal(7, wal.GetAcknowledgedPresence(Partition));

            // A snapshot boundary above the log raises it to the boundary, whose checkpoint certifies the prefix...
            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 12, lastIncludedTerm: 3, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
            Assert.Equal(12, wal.GetAcknowledgedPresence(Partition));

            // ...and a truncating install, a term mismatch at the boundary, clamps it to the boundary.
            (status, truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 4, lastIncludedTerm: 9, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(truncated);
            Assert.Equal(4, wal.GetAcknowledgedPresence(Partition));

            // The assignment from outside a batch, and its removal.
            Assert.True(wal.SetAcknowledgedPresence(Partition, 2));
            Assert.Equal(2, wal.GetAcknowledgedPresence(Partition));
            Assert.True(wal.SetAcknowledgedPresence(Partition, -1));
            Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition));

            Assert.True(wal.SetAcknowledgedPresence(Partition, 3));
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(Partition));
            Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition));
            Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition + 1));
        }

        // The clamps and the removal are on disk, not only in the cache.
        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Equal(-1, reopened.GetAcknowledgedPresence(Partition));
    }

    // ── Fail-stop on damage ─────────────────────────────────────────────────────────────────

    // A read that reaches a damaged block throws the database's corruption and fail-stops the WAL: from then on every
    // write is refused as Errored without a reopen, the hard state cannot be persisted, and reads of intact data
    // still serve.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Damage_found_by_a_read_fail_stops_writes_without_a_reopen_and_intact_reads_still_serve(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteFlushedPartition(scope);
        FlipByte(table, 8);
        byte[] damaged = File.ReadAllBytes(table);

        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        IWAL asInterface = wal;
        Assert.Null(wal.GetFailStopReason());

        SolariumException read = Assert.Throws<SolariumException>(() => wal.ReadLogs(Partition));
        Assert.True(read.IsCorruption);
        Assert.Contains("bad checksum", wal.GetFailStopReason(), StringComparison.Ordinal);
        Assert.Equal(read.Message, asInterface.GetFailStopReason());

        for (int attempt = 0; attempt < 3; attempt++)
            Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Logs.Proposed(100)])]));

        Assert.False(asInterface.PersistHardState(Partition, currentTerm: 9, votedFor: "node-c"));
        Assert.Equal(RaftOperationStatus.Errored, wal.TruncateLogsAfter(Partition, 1));
        Assert.False(wal.SetAcknowledgedPresence(Partition, 1));
        Assert.Equal(RaftOperationStatus.Errored, wal.CompactLogsOlderThan(Partition, 10, 100).Status);
        Assert.Equal(0, wal.EngineReopenCount);
        Assert.Equal(damaged, File.ReadAllBytes(table));

        // The metadata family is intact and still readable.
        Assert.Equal("1", wal.GetMetaData("intact-marker"));
    }

    // Every scan that reaches the damage throws; none returns a short result as if the log ended there, which would
    // restore an empty or a shorter log as the partition's own.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_scan_over_a_damaged_block_throws_instead_of_ending_short(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        FlipByte(WriteFlushedPartition(scope), 8);

        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.True(Assert.Throws<SolariumException>(() => wal.ReadLogs(Partition)).IsCorruption);
        Assert.True(Assert.Throws<SolariumException>(() => wal.ReadLogsRange(Partition, 1, 100)).IsCorruption);
        Assert.True(Assert.Throws<SolariumException>(() => wal.CountPersistedLogs(Partition)).IsCorruption);
        Assert.True(Assert.Throws<SolariumException>(() => wal.GetTermAt(Partition, 1)).IsCorruption);
        Assert.NotNull(wal.GetFailStopReason());
    }

    // The write path meets the damage itself: a commit marker over a damaged row reads the row's header, the read
    // throws, and the write is Errored with the WAL fail-stopped, with no probe and no reopen.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Damage_found_by_a_write_fail_stops_without_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        FlipByte(WriteFlushedPartition(scope), 8);

        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Logs.Committed(5)])]));
        Assert.Contains("bad checksum", wal.GetFailStopReason(), StringComparison.Ordinal);
        Assert.Equal(0, wal.EngineReopenCount);
    }

    // A compaction that merges the damaged table fails in the background and stops the database's writes; the next
    // write meets the refusal, which carries the damage, and the WAL fail-stops instead of reopening. The rows are
    // written again over the same ids, so the new tables overlap the damaged one and the compaction must read it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Damage_found_by_a_compaction_fail_stops_without_a_reopen_and_leaves_the_files(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteFlushedPartition(scope);
        FlipByte(table, 8);

        SolariumWalOptions options = SolariumWalOptions.Default with { ShardLevel0FileNumCompactionTrigger = 2 };
        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true, options);
        RaftOperationStatus last = RaftOperationStatus.Success;
        for (int round = 0; round < 6 && last == RaftOperationStatus.Success; round++)
        {
            last = wal.Write([(Partition, Logs.Range(1, 40, 1, RaftLogType.Proposed, Logs.Incompressible(64)))]);
            if (last != RaftOperationStatus.Success)
                break;

            // The forced flush meets the stopped writes as soon as the compaction has failed.
            try
            {
                wal.FlushMemTablesForTesting(wait: true);
            }
            catch (SolariumException e) when (e.IsCorruption)
            {
                break;
            }
        }

        long id = 1000;
        Wals.WaitUntil(() => wal.Write([(Partition, [Logs.Proposed(id++)])]), status => status == RaftOperationStatus.Errored, TimeSpan.FromSeconds(30));
        string reason = Assert.IsType<string>(wal.GetFailStopReason());
        Assert.Contains("accepts no more writes after a WAL, flush or compaction failure", reason);
        Assert.Contains("bad checksum", reason);
        Assert.Equal(0, wal.EngineReopenCount);
        Assert.True(File.Exists(table));
    }

    // A table shorter than the manifest records refuses the open: the node does not start on partial data.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_truncated_table_refuses_the_open(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string table = WriteFlushedPartition(scope);
        byte[] bytes = File.ReadAllBytes(table);
        File.WriteAllBytes(table, bytes[..^16]);

        SolariumException open = Assert.Throws<SolariumException>(() => Wals.Open(scope.Environment, Root, syncWrites: true));
        Assert.True(open.IsCorruption);
        Assert.Contains(Path.GetFileName(table), open.Message);
    }

    // ── Scaffolding ─────────────────────────────────────────────────────────────────────────

    private static Dictionary<int, long> Watermark(long value) => new() { [Partition] = value };

    // Rows 1 to 40 of the partition, flushed to a table of its shard, and a marker in the metadata family; returns the
    // table's path, with the WAL closed.
    private string WriteFlushedPartition(EnvironmentScope scope)
    {
        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Logs.Range(1, 40, 1, RaftLogType.Proposed, Logs.Incompressible(64)))]));
        Assert.True(wal.SetMetaData("intact-marker", "1"));
        wal.FlushMemTablesForTesting(wait: true);
        (string name, _, _, _, _) = Assert.Single(wal.GetShardLiveFilesForTesting(Partition));
        string table = Path.Combine(Root, "wal", name.TrimStart('/'));
        Assert.True(File.Exists(table), $"no table at {table}");
        return table;
    }

    // Data blocks come first in a table, and byte 8 is inside the first one's entries.
    private static void FlipByte(string path, int offset)
    {
        byte[] bytes = File.ReadAllBytes(path);
        bytes[offset] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
    }
}
