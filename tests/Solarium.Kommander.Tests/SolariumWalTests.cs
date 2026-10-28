using Kommander.Data;
using Solarium.Storage;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// Reads, writes, tuning and the format guard of <see cref="SolariumWAL"/>. Ported from Kommander's
/// <c>TestRocksDbWAL</c>, <c>TestRocksDbWALFormatGuards</c> and the attribution test of
/// <c>TestRocksDbWALFrontierClampsAndAttribution</c> at commit 1bee500.
/// </summary>
public sealed class SolariumWalTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_shared_memtable_budget_caps_the_wal_beside_another_database_and_a_small_one_is_warned_about(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteBufferManager manager = new(64 * 1024);
        CapturingLogger logger = new();
        SolariumWalOptions options = new() { WriteBufferManager = manager, Environment = scope.Environment };

        using SolariumWAL wal = new(_directory.Combine("raft"), "wal", logger, syncWrites: false, options);
        Assert.Contains(logger.Warnings, warning => warning.Contains("shared memtable budget (0 MB)", StringComparison.Ordinal));

        // A store beside the WAL holds most of the budget, and takes no more writes.
        using SolariumDb store = SolariumDb.Open(_directory.Combine("store"), new DbOptions
        {
            Environment = scope.Environment,
            WriteBufferManager = manager,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 1L << 30 },
        });
        for (int i = 0; i < 40; i++)
            store.Put(BitConverter.GetBytes(i), Incompressible(1000));

        // The WAL's appends pass the cap, and the WAL flushes its own shard, never the store.
        for (long id = 1; id <= 40; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, [Committed(id, payload: Incompressible(1000))])]));

        Assert.True(wal.DatabaseForTesting.BufferManagerSwitches > 0);
        Wals.WaitUntil(() => Wals.ShardCounts(wal, 3).Flushes, flushes => flushes > 0, TimeSpan.FromSeconds(30));
        Assert.Equal(0, store.FlushCount);
        Assert.Equal(40, wal.ReadLogs(3).Count);

        // A budget of a shard memtable and one more is not warned about.
        CapturingLogger quiet = new();
        using SolariumWAL roomy = new(_directory.Combine("roomy"), "wal", quiet, syncWrites: false,
            options with { WriteBufferManager = new WriteBufferManager(2 * options.ShardWriteBufferSizeBytes) });
        Assert.Empty(quiet.Warnings);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_block_cache_holds_the_index_and_filter_of_every_family_with_level_0_pinned_and_the_log_reads_back(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(16L * 1024 * 1024);
        SolariumWalOptions options = new() { BlockCache = cache, Environment = scope.Environment, ShardWriteBufferSizeBytes = 64 * 1024 };
        string path = _directory.Combine("raft");

        using (SolariumWAL wal = new(path, "wal", new CapturingLogger(), syncWrites: false, options))
        {
            SolariumDb db = wal.DatabaseForTesting;
            foreach (ColumnFamily family in db.ColumnFamilies)
                Assert.True(family.Options is { BlockCache: not null, CacheIndexAndFilterBlocks: true, PinL0FilterAndIndexBlocksInCache: true }, family.Name);

            for (long id = 1; id <= 400; id++)
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, [Committed(id, payload: new byte[1000])])]));

            Wals.WaitUntil(() => Wals.ShardCounts(wal, 3).Flushes, flushes => flushes > 0, TimeSpan.FromSeconds(30));
            // The readers keep only their top-level indexes; the partitions are in the cache.
            Assert.True(long.Parse(db.GetProperty("rocksdb.estimate-table-readers-mem", db.GetColumnFamily("shard3"))!, System.Globalization.CultureInfo.InvariantCulture) < cache.Usage);
            Assert.True(cache.Usage > 0);
            Assert.Equal(400, wal.ReadLogs(3).Count);
        }

        Assert.Equal(0, cache.Usage);
        using (SolariumWAL reopened = new(path, "wal", new CapturingLogger(), syncWrites: false, options))
            Assert.Equal(400, reopened.ReadLogs(3).Count);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_compaction_pass_is_one_write_however_many_entries_it_removes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        List<RaftLog> logs = [];
        for (long id = 1; id <= 9; id++)
            logs.Add(Committed(id, term: id));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(4, logs)]));

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(4, lastCheckpoint: 10, compactNumberEntries: 2, maxTotalEntries: 9);

        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(9, removed);
        Assert.Equal(1, wal.LastCompactionWriteCount);
        Assert.Empty(wal.ReadLogsRange(4, 1));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Writes_can_be_unsynced(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: false);

        Assert.False(wal.SyncWritesEnabled);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(2, [Committed(1, term: 20)])]));

        RaftLog log = Assert.Single(wal.ReadLogs(2));
        Assert.Equal(1, log.Id);
        Assert.Equal(20, log.Term);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Partitions_that_share_a_shard_keep_their_own_rows_for_the_same_ids(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);

        // Nine partitions on eight shards: 0 and 8 share shard 0.
        for (int partitionId = 0; partitionId < 9; partitionId++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, [Named(partitionId, 1, 100 + partitionId)])]));

        for (int partitionId = 0; partitionId < 9; partitionId++)
        {
            RaftLog log = Assert.Single(wal.ReadLogs(partitionId));
            Assert.Equal(1, log.Id);
            Assert.Equal(100 + partitionId, log.Term);
            Assert.Equal($"partition-{partitionId}", log.LogType);

            RaftLog ranged = Assert.Single(wal.ReadLogsRange(partitionId, 1));
            Assert.Equal(100 + partitionId, ranged.Term);
            Assert.Equal($"partition-{partitionId}", ranged.LogType);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Scans_stay_inside_their_partition(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, syncWrites: true);

        Assert.Equal(RaftOperationStatus.Success, wal.Write(
        [
            (0, [Named(0, 1, 10), Named(0, 3, 13, RaftLogType.CommittedCheckpoint)]),
            (8, [Named(8, 1, 80), Named(8, 2, 82), Named(8, 3, 83)]),
        ]));

        Assert.Equal(3, wal.GetMaxLog(0));
        Assert.Equal(13, wal.GetCurrentTerm(0));

        // Partition 0 has a hole at id 2, so its checkpoint row at 3 certifies nothing.
        Assert.Equal(-1, wal.GetLastCheckpoint(0));
        Assert.Equal(3, wal.GetMaxLog(8));
        Assert.Equal(83, wal.GetCurrentTerm(8));
        Assert.Equal(-1, wal.GetLastCheckpoint(8));

        (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(8, lastCheckpoint: 3, compactNumberEntries: 10);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(2, removed);

        Assert.Equal([1L, 3L], Ids(wal.ReadLogsRange(0, 1)));
        RaftLog remaining = Assert.Single(wal.ReadLogsRange(8, 1));
        Assert.Equal(3, remaining.Id);
        Assert.Equal(83, remaining.Term);
    }

    // The row encoder leaves out a null log type, and the decoder reads an absent one as the empty
    // string, as the generated protobuf parser does; callers rely on a non-null result.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_null_log_type_reads_back_as_an_empty_string(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(5, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = null }])]));

        Assert.Equal("", Assert.Single(wal.ReadLogs(5)).LogType);
        Assert.Equal("", Assert.Single(wal.ReadLogsRange(5, 1)).LogType);
    }

    // The shard tuning applies when the database opens and leaves nothing on disk, so a database
    // written under one tuning reads back the same under another.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_database_written_with_one_tuning_reads_back_under_another(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SolariumWalOptions small = new()
        {
            ShardWriteBufferSizeBytes = 8 * 1024 * 1024,
            ShardMaxWriteBufferNumber = 2,
            ShardLevel0FileNumCompactionTrigger = 4,
            ShardLevel0SlowdownWritesTrigger = 20,
            ShardLevel0StopWritesTrigger = 36,
        };

        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path, options: small))
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, [Committed(1, term: 7), Committed(2, term: 7)])]));
            Assert.Equal([1L, 2L], Ids(wal.ReadLogs(3)));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        List<RaftLog> logs = reopened.ReadLogs(3);
        Assert.Equal([1L, 2L], Ids(logs));
        Assert.All(logs, log => Assert.Equal(7, log.Term));
    }

    // A shard needs a memtable to write into while its full one flushes, or every switch waits.
    [Fact]
    public void A_shard_with_fewer_than_two_memtables_is_rejected()
    {
        SolariumWalOptions options = new() { ShardMaxWriteBufferNumber = 1 };

        Assert.Throws<ArgumentOutOfRangeException>(() => Wals.Open(DefaultStorageEnvironment.Instance, _directory.Path, options: options));
    }

    [Fact]
    public void A_negative_wal_cap_is_rejected_before_the_database_opens()
    {
        SolariumWalOptions options = new() { MaxTotalWalSizeFlushUnits = -1 };

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(
            () => Wals.Open(DefaultStorageEnvironment.Instance, _directory.Path, options: options));

        Assert.Contains("MaxTotalWalSizeFlushUnits", error.Message);
        Assert.False(Directory.Exists(Path.Combine(_directory.Path, "wal")));
    }

    // Opening a directory of another format version would misread every row, so the constructor
    // refuses it, and closes the database it opened for the check: a second attempt meets the
    // same refusal, not a held lock.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_directory_of_another_format_version_is_refused_and_left_unlocked(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            Assert.Equal("2.0.0", wal.GetMetaData("version"));
            Assert.True(wal.SetMetaData("version", "1.0.0"));
        }

        for (int attempt = 0; attempt < 2; attempt++)
        {
            InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Wals.Open(scope.Environment, _directory.Path));
            Assert.Contains("1.0.0", error.Message);
            Assert.Contains("2.0.0", error.Message);
        }
    }

    // A host may create the directory before the first open, and a crash during the first open can
    // leave it without the version: a WAL whose database holds nothing opens as new.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_directory_that_exists_before_the_first_open_opens_as_a_new_wal(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Directory.CreateDirectory(Path.Combine(_directory.Path, "wal"));

        using (SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path))
        {
            Assert.Equal("2.0.0", wal.GetMetaData("version"));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Committed(1)])]));
        }

        using SolariumWAL reopened = Wals.Open(scope.Environment, _directory.Path);
        Assert.Equal([1L], Ids(reopened.ReadLogs(1)));
    }

    // Rows with no version are another layout, which the WAL must not read as its own.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_database_that_holds_data_but_no_version_is_refused(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string path = Path.Combine(_directory.Path, "wal");
        using (SolariumDb db = SolariumDb.Open(path, scope.Options(createMissingColumnFamilies: true), ["metadata", "shard1"]))
            db.Put("0000000001:00000000000000000001"u8, "row"u8, db.GetColumnFamily("shard1"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Wals.Open(scope.Environment, _directory.Path));
        Assert.Contains("<missing>", error.Message);
    }

    // The diagnostics readers report live figures while the database holds data, and 0 once it is
    // closed, except the WAL bytes, which are the files on disk: what a reopen would replay. Solarium
    // starts a new WAL file at the first write after a flush, not at the flush as RocksDB does, so
    // the flushed batch stays in the current file and the close leaves it there.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_diagnostics_report_live_figures_while_open_and_nothing_after_close(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        SolariumWAL wal = Wals.Open(scope.Environment, _directory.Path);
        long walBytesAtClose;
        try
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, Range(1, 300, 1, RaftLogType.Proposed, new byte[2048]))]));

            Assert.True(wal.GetAliveWriteAheadLogBytes() > 300 * 2048, "an unflushed batch must keep its WAL file");
            Assert.True(wal.GetMemtableMemoryBytes() > 300 * 2048, "an unflushed batch must occupy memtable memory");

            wal.FlushMemTablesForTesting(wait: true);

            Assert.True(wal.GetShardLiveSstBytes() > 300 * 2048, "the flushed rows must occupy shard SSTables");
            Assert.True(wal.GetTableReadersMemoryBytes() > 0, "open tables must hold their index in memory");
            Assert.Equal(1, wal.GetShardLevel0FileCount());
            Assert.Equal(0, wal.GetShardLevel6FileCount());
            Assert.Equal(0, wal.GetActualDelayedWriteRate());
            Assert.Equal(0, wal.GetIsWriteStopped());
            walBytesAtClose = wal.GetAliveWriteAheadLogBytes();
        }
        finally
        {
            wal.Dispose();
        }

        Assert.True(walBytesAtClose > 300 * 2048, "the current WAL file should still hold the flushed batch");
        Assert.Equal(walBytesAtClose, wal.GetAliveWriteAheadLogBytes());
        Assert.Equal(0, wal.GetMemtableMemoryBytes());
        Assert.Equal(0, wal.GetTableReadersMemoryBytes());
        Assert.Equal(0, wal.GetShardLiveSstBytes());
        Assert.Equal(0, wal.GetShardLevel6FileCount());
        Assert.Equal(0, wal.GetShardLevel0FileCount());
        Assert.Equal(0, wal.GetActualDelayedWriteRate());
        Assert.Equal(0, wal.GetIsWriteStopped());
        Assert.Throws<ObjectDisposedException>(() => wal.ReadLogs(1));
    }

    private static RaftLog Named(int partitionId, long id, long term, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = $"partition-{partitionId}" };
}
