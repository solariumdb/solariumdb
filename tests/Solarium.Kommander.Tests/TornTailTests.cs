using Kommander.Data;
using Kommander.WAL;
using Solarium.Tests;
using static Solarium.Kommander.Tests.Logs;

namespace Solarium.Kommander.Tests;

/// <summary>
/// A node killed mid-write must open its WAL again. A kill leaves the last WAL record short, the
/// normal residue of an unclean stop, so a reopen keeps the synced prefix and drops that record;
/// damage with whole records after it is still refused. The kill is staged on disk: the directory is
/// copied while the WAL is open, then the copy's newest WAL file is cut or damaged. The torn record
/// is an unsynced one, the only kind a kill can tear: a synced record cut short is a loss the sync
/// ledger reports (<see cref="LostSyncedWriteTests"/>). Ported from Kommander's
/// <c>TestTornWalTailReopen</c> at commit e2763bc.
/// </summary>
public sealed class TornTailTests : IDisposable
{
    private const int Partition = 1;

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    private string Live => _directory.Combine("live");

    private string Crashed => _directory.Combine("crashed");

    // A synced write is acknowledged after its flush, and the torn record was not synced, so it was
    // never acknowledged: the reopen drops it and reports no loss.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_torn_last_record_is_dropped_and_the_synced_prefix_survives(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteTwoRecordsAndCopy(scope);

        FileInfo log = NewestWalFile(Path.Combine(Crashed, "wal"));
        using (FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write))
            stream.SetLength(log.Length - 4);

        using SolariumWAL reopened = Wals.Open(scope.Environment, Crashed, syncWrites: true);
        Assert.Equal([1L, 2L, 3L, 4L], Ids(reopened.ReadLogs(Partition)));
        Assert.Equal(4, reopened.GetMaxLog(Partition));
        Assert.Equal(4, reopened.GetCurrentTerm(Partition));
        Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        Assert.Null(reopened.GetFailStopReason());
    }

    // Only the last record can be short, so tolerating a torn tail costs nothing; a bad checksum with
    // whole records after it is corruption, and replaying past it would leave a hole in the log.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Damage_in_the_middle_of_the_wal_is_refused(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteTwoRecordsAndCopy(scope);

        // The file header is 8 bytes and a record header 8 more, so byte 24 is inside the payload of
        // the first record, the one that set the format version.
        FileInfo log = NewestWalFile(Path.Combine(Crashed, "wal"));
        using (FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Seek(24, SeekOrigin.Begin);
            int current = stream.ReadByte();
            stream.Seek(24, SeekOrigin.Begin);
            stream.WriteByte((byte)(current ^ 0xFF));
        }

        SolariumException error = Assert.Throws<SolariumException>(() => Wals.Open(scope.Environment, Crashed, syncWrites: true));
        Assert.Contains(log.Name, error.Message);
    }

    // A synced write and an unsynced one, so the WAL file's last two records are theirs, then a copy
    // of the directory, the ledgers with it, taken while the WAL is still open: the files a kill
    // leaves behind.
    private void WriteTwoRecordsAndCopy(EnvironmentScope scope)
    {
        using SolariumWAL wal = Wals.Open(scope.Environment, Live, syncWrites: true);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(1), Row(2), Row(3), Row(4)])]));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(5)])], sync: false));
        CopyDirectory(Path.Combine(Live, "wal"), Path.Combine(Crashed, "wal"));
    }

    private static RaftLog Row(long id) => new() { Id = id, Term = id, Type = RaftLogType.Committed, LogType = "torn-tail" };

    private static FileInfo NewestWalFile(string directory)
    {
        FileInfo? log = new DirectoryInfo(directory).GetFiles("*.log").OrderByDescending(f => f.Name, StringComparer.Ordinal).FirstOrDefault();
        Assert.NotNull(log);
        Assert.True(log.Length > 24, $"WAL file {log.Name} is only {log.Length} bytes");
        return log;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source))
        {
            // The copy opens with its own lock.
            if (Path.GetFileName(file) == "LOCK")
                continue;

            using FileStream from = new(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using FileStream to = new(Path.Combine(destination, Path.GetFileName(file)), FileMode.CreateNew, FileAccess.Write);
            from.CopyTo(to);
        }
    }
}
