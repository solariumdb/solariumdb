using Kommander.Data;

namespace Solarium.Kommander.Tests;

/// <summary>Builds the Raft log entries the tests write.</summary>
internal static class Logs
{
    /// <summary>A <c>Proposed</c> entry, the shape a leader appends before the commit.</summary>
    public static RaftLog Proposed(long id, long term = 1, byte[]? payload = null) =>
        new() { Id = id, Term = term, Type = RaftLogType.Proposed, LogType = "op", LogData = payload };

    /// <summary>A <c>Committed</c> entry: a commit marker, or a backfilled row.</summary>
    public static RaftLog Committed(long id, long term = 1, byte[]? payload = null) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "op", LogData = payload };

    /// <summary>A <c>CommittedCheckpoint</c> entry with no payload.</summary>
    public static RaftLog Checkpoint(long id, long term = 1) =>
        new() { Id = id, Term = term, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" };

    /// <summary>Entries <paramref name="first"/> to <paramref name="last"/> inclusive, all of <paramref name="type"/>.</summary>
    public static List<RaftLog> Range(long first, long last, long term, RaftLogType type, byte[]? payload = null)
    {
        List<RaftLog> logs = new((int)(last - first + 1));
        for (long id = first; id <= last; id++)
            logs.Add(new RaftLog { Id = id, Term = term, Type = type, LogType = "op", LogData = payload });

        return logs;
    }

    /// <summary><c>Committed</c> entries <paramref name="first"/> to <paramref name="last"/> at term 5, with a 3-byte payload when none is given.</summary>
    public static List<RaftLog> CommittedRange(long first, long last, byte[]? payload = null) =>
        Range(first, last, 5, RaftLogType.Committed, payload ?? [1, 2, 3]);

    /// <summary>A payload of <paramref name="size"/> bytes with no runs, so its size on disk is its real size.</summary>
    public static byte[] Incompressible(int size)
    {
        byte[] payload = new byte[size];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)((i * 2654435761L) >> 13);

        return payload;
    }

    /// <summary>The ids of <paramref name="logs"/>, in order.</summary>
    public static long[] Ids(List<RaftLog> logs) => [.. logs.Select(log => log.Id)];
}
