using Kommander.Data;
using Kommander.WAL;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// The recovery of <see cref="SolariumWAL"/> after a failed write. Solarium refuses every write after
/// one fails, until the database is opened again, so the WAL asks whether the volume takes a small
/// durable write (the probe) and whether the database does (the canary), and reopens only when the
/// volume does and the database does not. Ported from Kommander's
/// <c>TestRocksDbWalStorageFailureGate</c> and the WAL test of <c>TestRocksDbWalEnospcRecovery</c>
/// at commit 1bee500; where those fill a real volume, these fail writes through
/// <see cref="HookedEnvironment"/>, which the probe meets as well, since it writes through the
/// database's environment.
/// </summary>
public sealed class StorageFailureTests : IDisposable
{
    private const int Partition = 1;
    private const int Entries = 25;

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void While_writes_fail_the_wal_never_reopens_and_once_they_succeed_it_reopens_and_keeps_every_entry(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumWAL wal = Wals.Open(hooks, _directory.Path, syncWrites: true);
        IWAL hardState = wal;

        for (long id = 1; id <= Entries; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Entry(id)])]));

        Assert.True(hardState.PersistHardState(Partition, currentTerm: 2, votedFor: "node-a"));

        hooks.FailWrites = true;

        // Every failure reports Errored, never an exception, and none reopens: the probe fails too.
        for (int attempt = 0; attempt < 3; attempt++)
            Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Entry(Entries + 1)])]));

        Assert.Equal(0, wal.EngineReopenCount);
        Assert.NotNull(wal.LastStorageFailureMessage);
        Assert.False(hardState.PersistHardState(Partition, currentTerm: 3, votedFor: "node-a"));

        // Reads do not take the write path: every acknowledged entry is still readable.
        Assert.Equal(Entries, wal.ReadLogs(Partition).Count);

        hooks.FailWrites = false;

        // The retries a node makes on its own cadence; the probe runs at most once a second.
        RaftOperationStatus status = RetryUntilSuccess(() => wal.Write([(Partition, [Entry(Entries + 1)])]));
        Assert.Equal(RaftOperationStatus.Success, status);

        // Solarium latches every write failure, so recovery can only go through a reopen.
        Assert.Equal(1, wal.EngineReopenCount);

        List<RaftLog> logs = wal.ReadLogs(Partition);
        Assert.Equal(Enumerable.Range(1, Entries + 1).Select(i => (long)i), Logs.Ids(logs));
        Assert.Equal(Entries + 1, wal.GetMaxLog(Partition));

        // Hard state persists again, and the vote from before the failures survived it.
        Assert.True(hardState.TryGetHardState(Partition, out long term, out string? votedFor));
        Assert.Equal(2, term);
        Assert.Equal("node-a", votedFor);
        Assert.True(hardState.PersistHardState(Partition, currentTerm: 4, votedFor: "node-b"));
        Assert.True(hardState.TryGetHardState(Partition, out term, out votedFor));
        Assert.Equal(4, term);
        Assert.Equal("node-b", votedFor);
    }

    // When only the WAL files refuse writes, the probe passes and the canary fails, so the WAL closes
    // the database to reopen it, and the reopen fails too: the WAL stays closed, reads report it, and
    // a later failed write tries the reopen again once the files take writes.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_reopen_that_fails_leaves_the_wal_closed_until_a_later_write_reopens_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { FailingPaths = path => path.EndsWith(".log", StringComparison.Ordinal) };
        using SolariumWAL wal = Wals.Open(hooks, _directory.Path, syncWrites: true);

        for (long id = 1; id <= Entries; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Entry(id)])]));

        hooks.FailWrites = true;
        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Entry(Entries + 1)])]));
        Assert.Equal(0, wal.EngineReopenCount);

        // The first failure probed, closed the database, and failed to open it: a new WAL file needs a header.
        Assert.Throws<global::Kommander.RaftException>(() => wal.ReadLogs(Partition));
        Assert.Equal(0, wal.GetShardLiveSstBytes());

        hooks.FailWrites = false;
        RaftOperationStatus status = RetryUntilSuccess(() => wal.Write([(Partition, [Entry(Entries + 1)])]));
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(1, wal.EngineReopenCount);

        Assert.Equal(Enumerable.Range(1, Entries + 1).Select(i => (long)i), Logs.Ids(wal.ReadLogs(Partition)));
    }

    // A failure that is not a storage fault (here, a write to a database the host closed) is not
    // something a reopen can fix, so it reports Errored without probing or reopening.
    [Fact]
    public void A_write_after_dispose_throws_and_never_reopens()
    {
        SolariumWAL wal = Wals.Open(DefaultStorageEnvironment.Instance, _directory.Path);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Entry(1)])]));
        wal.Dispose();

        Assert.Throws<ObjectDisposedException>(() => wal.Write([(Partition, [Entry(2)])]));
        Assert.Equal(0, wal.EngineReopenCount);
    }

    private static RaftOperationStatus RetryUntilSuccess(Func<RaftOperationStatus> write)
    {
        return Wals.WaitUntil(write, status => status == RaftOperationStatus.Success, Timeout, failOnTimeout: false);
    }

    private static RaftLog Entry(long id) => new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "Greeting", LogData = new byte[4096] };
}
