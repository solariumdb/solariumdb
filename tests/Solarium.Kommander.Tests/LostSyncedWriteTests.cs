using Kommander.Data;
using Kommander.WAL;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The sync ledger (<see cref="SolariumSyncLedger"/>) and the hard-state ledger (<see cref="SolariumHardStateLedger"/>)
/// against real file damage. A WAL file cut after its fsync opens short and clean, because the acknowledged-presence
/// watermark and the hard state go with the rows they describe; each such cut now opens with a persisted
/// lost-synced-writes verdict that every partition carries until it reconciles, the database keeps accepting writes
/// so a leader can repair it, and a forgotten vote is written back from the hard-state ledger. The shapes that are
/// not a loss must not be reported as one: a torn unsynced record, a clean reopen, a ledger copied from another
/// directory, a torn or unreadable ledger, a directory from before the ledgers, and any loss with
/// <c>syncWrites</c> off. Ported from Kommander's <c>TestRocksDbSyncLedger</c> at commit e2763bc.
/// </summary>
/// <remarks>
/// The damage is staged after a clean close: Solarium flushes its WAL at <see cref="SolariumDb.Dispose"/> but not its
/// memtables, so the rows are in the newest WAL file alone, and a cut removes them as after a kill. Each damage test
/// asserts that rows went missing, so none passes without the fault.
/// </remarks>
public sealed class LostSyncedWriteTests : IDisposable
{
    private const int Partition = 1;
    private const int Total = 40;

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    private string Root => _directory.Path;

    private string EngineDirectory => Path.Combine(Root, "wal");

    // ── Damage the ledger must find ─────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_file_cut_to_half_after_its_fsync_quarantines_every_partition_until_each_reconciles(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        long attested = WriteSyncedRows(scope);
        CutNewestWalFile(length => length / 2);

        long surviving;
        using (SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            IWAL wal = reopened;
            surviving = reopened.GetMaxLog(Partition);
            Assert.True(surviving < Total, "the cut removed no row; the fault was not reproduced");

            // The watermark alone says nothing: it went with the rows.
            Assert.Equal(surviving, reopened.GetAcknowledgedPresence(Partition));

            // The verdict names the sequences and covers every partition.
            string reason = Assert.IsType<string>(wal.GetLostSyncedWritesReason(Partition));
            Assert.Contains("lost synced writes", reason);
            Assert.Contains($"attests sequence {attested}", reason);
            Assert.Equal(reason, wal.GetLostSyncedWritesReason(Partition + 1));
            Assert.Equal(1, reopened.LostSyncedWritesVerdict);

            // Not a fail-stop: the database is consistent, only stale, so it serves reads and takes the writes a
            // repair needs, and the ledger follows it again.
            Assert.Null(reopened.GetFailStopReason());
            Assert.Equal(Enumerable.Range(1, (int)surviving).Select(i => (long)i), reopened.ReadLogs(Partition).Select(l => l.Id));
            Assert.Equal(RaftOperationStatus.Success, reopened.Write([(Partition, Rows(surviving + 1, surviving + 1))]));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);

            // One partition reconciles; the other still carries the verdict.
            Assert.True(wal.MarkLostSyncedWritesReconciled(Partition));
            Assert.Null(wal.GetLostSyncedWritesReason(Partition));
            Assert.NotNull(wal.GetLostSyncedWritesReason(Partition + 1));
        }

        // The verdict and the reconciliation persist: a restart quarantines the other partition again, and only it.
        using (SolariumWAL again = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            IWAL wal = again;
            Assert.Equal(1, again.LostSyncedWritesVerdict);
            Assert.Null(wal.GetLostSyncedWritesReason(Partition));
            Assert.NotNull(wal.GetLostSyncedWritesReason(Partition + 1));
            Assert.True(wal.MarkLostSyncedWritesReconciled(Partition + 1));
            WriteSyncedRows(again, surviving + 2, surviving + 20);
        }

        // Damage again after a full reconciliation: a new verdict, which the old reconciliations do not cover.
        CutNewestWalFile(length => length / 2);
        using SolariumWAL damagedAgain = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Equal(2, damagedAgain.LostSyncedWritesVerdict);
        Assert.NotNull(((IWAL)damagedAgain).GetLostSyncedWritesReason(Partition));
        Assert.NotNull(((IWAL)damagedAgain).GetLostSyncedWritesReason(Partition + 1));
    }

    // The smallest cut: one acknowledged entry lost.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_file_cut_by_one_record_after_its_fsync_is_a_loss(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope);
        CutNewestWalFile(length => length - 16);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Equal(Total - 1, reopened.GetMaxLog(Partition));
        Assert.Contains("lost synced writes", ((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Null(reopened.GetFailStopReason());
    }

    // The flush that releases WAL files moves the watermark through 20 into a table midway; the rows after it are
    // in the WAL file in use, which only the ledger covers.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_wal_file_cut_after_a_metadata_flush_is_a_loss(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope, flushMetadataAfter: 20);
        CutNewestWalFile(length => length / 2);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.InRange(reopened.GetMaxLog(Partition), 0, Total - 1);
        Assert.Contains("lost synced writes", ((IWAL)reopened).GetLostSyncedWritesReason(Partition));
    }

    // Solarium's manifest lists every live WAL file, so a deleted one is found by the database itself, which refuses
    // to open rather than open short: corruption, not a loss to repair, where RocksDB opens on its tables alone.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_deleted_wal_file_refuses_the_open(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            WriteSyncedRows(wal, 1, 20);
            wal.FlushMemTablesForTesting(wait: true);
            WriteSyncedRows(wal, 21, Total);
        }

        File.Delete(NewestWalFile().FullName);
        SolariumException refused = Assert.Throws<SolariumException>(() => Wals.Open(scope.Environment, Root, syncWrites: true));
        Assert.True(refused.IsCorruption);
    }

    // Every database file gone and the ledgers left, as a shell's "rm -rf dir/*" leaves them: the database opens new,
    // and the ledger proves the directory held synced writes. Before the ledger this opened as a fresh WAL.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_directory_whose_database_files_were_erased_under_the_ledgers_is_a_loss(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope);
        foreach (string file in Directory.GetFiles(EngineDirectory))
            if (!Path.GetFileName(file).StartsWith('.'))
                File.Delete(file);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Equal(0, reopened.GetMaxLog(Partition));
        string reason = Assert.IsType<string>(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Contains("lost synced writes", reason);
        Assert.Contains("format version", reason);
        Assert.Equal("2.0.0", reopened.GetMetaData("version"));
        Assert.True(Guid.TryParseExact(reopened.GetMetaData("raft_engine_identity"), "N", out _));
        Assert.Null(reopened.GetFailStopReason());
    }

    // The hard state is a synced metadata put in the same WAL file. A cut that takes it leaves an older term and no
    // vote, which would let the node vote twice in one term; the hard-state ledger writes it back at open.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_vote_cut_off_the_wal_is_restored_from_the_hard_state_ledger(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            WriteSyncedRows(wal, 1, 10);
            Assert.True(((IWAL)wal).PersistHardState(Partition, currentTerm: 7, votedFor: "node-b"));
        }

        CutNewestWalFile(length => length - 16);
        using (SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            IWAL wal = reopened;
            Assert.True(wal.TryGetHardState(Partition, out long term, out string? votedFor));
            Assert.Equal(7, term);
            Assert.Equal("node-b", votedFor);
            Assert.Equal(10, reopened.GetMaxLog(Partition));
            Assert.Contains("lost synced writes", wal.GetLostSyncedWritesReason(Partition));

            // Later hard state goes on through the database and the ledger.
            Assert.True(wal.PersistHardState(Partition, currentTerm: 8, votedFor: null));
        }

        using SolariumWAL again = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.True(((IWAL)again).TryGetHardState(Partition, out long laterTerm, out string? laterVote));
        Assert.Equal(8, laterTerm);
        Assert.Null(laterVote);
    }

    // ── Shapes that are not a loss ──────────────────────────────────────────────────────────

    // A record appended without a sync and then cut, the residue of a kill: the ledger never attested it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_torn_unsynced_record_reopens_healthy(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            WriteSyncedRows(wal, 1, 10);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(11, 11))], sync: false));
        }

        CutNewestWalFile(length => length - 4);
        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Null(reopened.GetFailStopReason());
        Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Equal(10, reopened.GetMaxLog(Partition));
        Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
        Assert.Equal(RaftOperationStatus.Success, reopened.Write([(Partition, Rows(11, 11))]));
        Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_clean_reopen_is_healthy_and_the_ledger_follows_the_database(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Null(reopened.GetFailStopReason());
        Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Equal(-1, reopened.LostSyncedWritesVerdict);
        Assert.Equal(Total, reopened.GetMaxLog(Partition));
        Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
        Assert.True(File.Exists(reopened.SyncLedgerPath));
        Assert.True(File.Exists(reopened.HardStateLedgerPath));
    }

    // Every synced write is attested before it returns: the one-row path, an absorbed commit marker, the hard state,
    // the watermark rewrite and a truncation. An unsynced write is not, and the next synced one covers it.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_synced_write_is_attested_before_it_returns(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        IWAL asInterface = wal;

        void AssertAttested() => Assert.Equal(wal.EngineLatestSequence, wal.SyncLedgerAttestedSequence);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1, 1))]));
        AssertAttested();
        Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Rows(2, 5))], sync: true, new Dictionary<int, long> { [Partition] = 5 }));
        AssertAttested();
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1, 1, RaftLogType.Committed))]));
        Assert.Equal(1, wal.CommitMarkersAbsorbed);
        AssertAttested();
        Assert.True(asInterface.PersistHardState(Partition, 3, "node-b"));
        AssertAttested();
        Assert.True(wal.SetAcknowledgedPresence(Partition, 4));
        AssertAttested();
        Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 3));
        AssertAttested();

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(4, 4))], sync: false));
        Assert.True(wal.SyncLedgerAttestedSequence < wal.EngineLatestSequence);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(5, 5))], sync: true));
        AssertAttested();
    }

    // Concurrent synced and unsynced writers: the ledger never attests past what the database holds (an unsynced batch
    // right after a synced one must not count), and a last synced write brings it level.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Concurrent_writers_never_attest_past_the_database(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        Parallel.For(0, 400, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i =>
        {
            int partition = 1 + (i % 4);
            long id = 1 + (i / 4);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(partition, Rows(id, id))], sync: i % 3 != 0));
            Assert.True(wal.SyncLedgerAttestedSequence <= wal.EngineLatestSequence);
        });

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1000, 1000))], sync: true));
        Assert.Equal(wal.EngineLatestSequence, wal.SyncLedgerAttestedSequence);
        Assert.Null(wal.GetFailStopReason());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_ledger_copied_from_another_directory_is_reset_and_not_believed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope);
        string other = _directory.Combine("other");
        using (SolariumWAL small = Wals.Open(scope.Environment, other, syncWrites: true))
            WriteSyncedRows(small, 1, 5);

        File.Copy(Path.Combine(EngineDirectory, SolariumSyncLedger.FileName), Path.Combine(other, "wal", SolariumSyncLedger.FileName), overwrite: true);
        using (SolariumWAL reopened = Wals.Open(scope.Environment, other, syncWrites: true))
        {
            Assert.Null(reopened.GetFailStopReason());
            Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Equal(5, reopened.GetMaxLog(Partition));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
        }

        // The reset outranks the copied record: the next open adopts it.
        using SolariumWAL again = Wals.Open(scope.Environment, other, syncWrites: true);
        Assert.Null(((IWAL)again).GetLostSyncedWritesReason(Partition));
    }

    // A torn slot is skipped; the other slot's older attestation is never above the database.
    [Theory]
    [MemberData(nameof(SlotCases))]
    public void A_torn_ledger_slot_falls_back_to_the_other_slot(string environment, int slot)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope);
        FlipLedgerByte((slot * 512) + 20);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Equal(Total, reopened.GetMaxLog(Partition));
        Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
    }

    /// <summary>Each environment with each of the ledger's two slots.</summary>
    public static TheoryData<string, int> SlotCases()
    {
        TheoryData<string, int> cases = new();
        foreach (string environment in Environments.Available().Select(row => (string)row.Data))
        {
            cases.Add(environment, 0);
            cases.Add(environment, 1);
        }

        return cases;
    }

    // Both slots unreadable is no shape a crash leaves: it is treated as no ledger, with a warning, not as a loss.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_unreadable_ledger_is_started_over_and_not_reported_as_a_loss(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteSyncedRows(scope);
        FlipLedgerByte(20);
        FlipLedgerByte(512 + 20);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
    }

    // A directory from before the ledgers (no ledger, no usable identity) is upgraded and healthy, and protected from
    // that open on. A deleted ledger is the same case.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_directory_from_before_the_ledgers_is_upgraded_and_healthy(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            WriteSyncedRows(wal, 1, 10);
            Assert.True(wal.SetMetaData("raft_engine_identity", "not-a-guid"));
        }

        File.Delete(Path.Combine(EngineDirectory, SolariumSyncLedger.FileName));
        File.Delete(Path.Combine(EngineDirectory, SolariumHardStateLedger.FileName));
        using (SolariumWAL upgraded = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            Assert.Null(((IWAL)upgraded).GetLostSyncedWritesReason(Partition));
            Assert.True(Guid.TryParseExact(upgraded.GetMetaData("raft_engine_identity"), "N", out _));
            Assert.True(File.Exists(upgraded.SyncLedgerPath));
            Assert.Equal(upgraded.EngineLatestSequence, upgraded.SyncLedgerAttestedSequence);
            WriteSyncedRows(upgraded, 11, 20);
        }

        CutNewestWalFile(length => length / 2);
        using SolariumWAL damaged = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.Contains("lost synced writes", ((IWAL)damaged).GetLostSyncedWritesReason(Partition));
    }

    // With syncWrites off nothing is promised durable, so nothing is attested past the open: a lost unsynced tail is
    // not damage. The open's own writes, unsynced too, are made durable by the mark the ledger is bound at.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void With_sync_writes_off_a_lost_tail_is_not_a_loss(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: false))
        {
            WriteSyncedRows(wal, 1, Total);
            Assert.True(wal.SyncLedgerAttestedSequence < wal.EngineLatestSequence);
        }

        CutNewestWalFile(length => length / 2);
        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: false);
        Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.True(reopened.GetMaxLog(Partition) < Total);
    }

    // A torn record at the hard-state ledger's tail, a crash during an append, is cut at open and never restored over
    // the database's hard state; later appends land after the cut.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_torn_hard_state_ledger_record_is_cut_and_ignored(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string ledger;
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            Assert.True(((IWAL)wal).PersistHardState(Partition, 3, "node-a"));
            Assert.True(((IWAL)wal).PersistHardState(Partition, 4, "node-b"));
            ledger = wal.HardStateLedgerPath;
        }

        long whole = new FileInfo(ledger).Length;
        using (FileStream stream = File.Open(ledger, FileMode.Open, FileAccess.Write))
        {
            stream.Seek(0, SeekOrigin.End);
            stream.Write([0x48, 0x44, 0x53, 0x54, 1, 0, 0, 0, 99]);
        }

        using (SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: true))
        {
            Assert.Equal(whole, new FileInfo(ledger).Length);
            Assert.True(((IWAL)reopened).TryGetHardState(Partition, out long term, out string? votedFor));
            Assert.Equal(4, term);
            Assert.Equal("node-b", votedFor);
            Assert.True(((IWAL)reopened).PersistHardState(Partition, 5, null));
        }

        using SolariumWAL again = Wals.Open(scope.Environment, Root, syncWrites: true);
        Assert.True(((IWAL)again).TryGetHardState(Partition, out long laterTerm, out _));
        Assert.Equal(5, laterTerm);
    }

    // Past its size bound the hard-state ledger is rewritten with one record per partition, and keeps every
    // partition's last term and vote across the rewrite and a reopen.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_hard_state_ledger_compacts_without_losing_a_vote(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string ledger;
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: false))
        {
            ledger = wal.HardStateLedgerPath;
            long term = 0;
            while (new FileInfo(ledger).Length <= SolariumHardStateLedger.CompactAboveBytes)
            {
                term++;
                for (int partition = 1; partition <= 4; partition++)
                    Assert.True(((IWAL)wal).PersistHardState(partition, term, $"node-{partition}-{term}"));
            }

            Assert.True(((IWAL)wal).PersistHardState(1, term + 1, "node-last"));
            Assert.True(new FileInfo(ledger).Length < 4096);
        }

        // The database lost every hard state; the compacted ledger still holds each one.
        using (SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: false))
            for (int partition = 1; partition <= 4; partition++)
                Assert.True(wal.SetMetaData($"raft_hardstate_p{partition}", "0"));

        using SolariumWAL reopened = Wals.Open(scope.Environment, Root, syncWrites: false);
        Assert.True(((IWAL)reopened).TryGetHardState(1, out _, out string? vote));
        Assert.Equal("node-last", vote);
        Assert.True(((IWAL)reopened).TryGetHardState(4, out long fourth, out string? fourthVote));
        Assert.Equal($"node-4-{fourth}", fourthVote);
    }

    // A flush failure of either ledger fails the write that needed it: a promise the ledger does not cover is not made.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_write_whose_ledger_cannot_be_flushed_is_not_reported_durable(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumWAL wal = Wals.Open(hooks, Root, syncWrites: true);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1, 1))]));

        hooks.FailingPaths = path => Path.GetFileName(path) == SolariumSyncLedger.FileName;
        hooks.FailFlushes = true;
        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, Rows(2, 2))]));
        Assert.False(((IWAL)wal).PersistHardState(Partition, 2, "node-a"));

        hooks.FailingPaths = path => Path.GetFileName(path) == SolariumHardStateLedger.FileName;
        Assert.False(((IWAL)wal).PersistHardState(Partition, 2, "node-a"));
        Assert.Null(wal.GetFailStopReason());
    }

    // ── Scaffolding ─────────────────────────────────────────────────────────────────────────

    // The rows 1 to 40 in one life of the WAL; returns what the ledger attested at its end.
    private long WriteSyncedRows(EnvironmentScope scope, int flushMetadataAfter = 0)
    {
        using SolariumWAL wal = Wals.Open(scope.Environment, Root, syncWrites: true);
        WriteSyncedRows(wal, 1, Total, flushMetadataAfter);
        return wal.SyncLedgerAttestedSequence;
    }

    // One synced batch per id, each with its watermark: the shape of production writes.
    private static void WriteSyncedRows(SolariumWAL wal, long from, long to, int flushMetadataAfter = 0)
    {
        IWAL asInterface = wal;
        for (long id = from; id <= to; id++)
        {
            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Rows(id, id))], sync: true, new Dictionary<int, long> { [Partition] = id }));
            if (id == flushMetadataAfter)
                wal.FlushMetadataForTesting();
        }
    }

    private static List<RaftLog> Rows(long from, long to, RaftLogType type = RaftLogType.Proposed) =>
        Logs.Range(from, to, 1, type, new byte[256]);

    private FileInfo NewestWalFile() =>
        new DirectoryInfo(EngineDirectory).GetFiles("*.log").OrderByDescending(f => f.Name, StringComparer.Ordinal).First();

    private void CutNewestWalFile(Func<long, long> newLength)
    {
        FileInfo log = NewestWalFile();
        using FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write);
        long target = newLength(stream.Length);
        Assert.InRange(target, 0, stream.Length - 1);
        stream.SetLength(target);
    }

    private void FlipLedgerByte(int offset)
    {
        using FileStream stream = File.Open(Path.Combine(EngineDirectory, SolariumSyncLedger.FileName), FileMode.Open, FileAccess.ReadWrite);
        stream.Position = offset;
        int value = stream.ReadByte();
        stream.Position = offset;
        stream.WriteByte((byte)(value ^ 0xFF));
    }
}
