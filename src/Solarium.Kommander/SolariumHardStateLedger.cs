// Ported from Kommander's RocksDbHardStateLedger (https://github.com/kahunakv/kommander,
// Kommander/WAL/RocksDbHardStateLedger.cs at commit e2763bc), MIT License, Copyright (c) 2025 Andres Gutierrez. The
// file name and the record layout are the same; the file goes through the WAL's storage environment.

using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Solarium.Storage;

namespace Solarium.Kommander;

/// <summary>
/// The hard-state ledger: an append-only file beside the database of a <see cref="SolariumWAL"/> that holds,
/// outside the database's WAL, the last term and vote persisted for each partition.
/// </summary>
/// <remarks>
/// <para>Why it exists. <see cref="SolariumWAL.PersistHardState"/> is a synced metadata put, which makes the
/// vote durable, inside the same WAL file as the rows. A WAL file truncated after its fsync takes the put with
/// it, and the node restarts with an older term and no vote: it then grants a second candidate a term it
/// already voted in. The sync ledger (<see cref="SolariumSyncLedger"/>) detects that synced writes were lost,
/// but not which vote is missing. This file can: at open, a partition whose entry here is ahead of the
/// database's gets its term and vote written back before the Raft layer reads them.</para>
/// <para>Ordering. Appended after the database's put returned, never before. A crash between the two leaves
/// the ledger behind, which the merge ignores; the ledger is ahead only when the database lost the put. A
/// record is one write and one flush; elections are rare.</para>
/// <para>Format. Records of <c>[u32 magic][i32 partition][i64 term][u16 vote length][vote UTF-8][u32 CRC-32 of
/// what precedes it]</c>, little-endian, replayed in order at open; the last record of a partition wins. A
/// torn tail record (the crash shape) fails its checksum, and the file is cut back to the last whole record so
/// later appends start at a record boundary. Past <see cref="CompactAboveBytes"/> the file is rewritten with
/// one record per partition.</para>
/// <para>Thread-safety. <see cref="Append"/> and <see cref="Snapshot"/> take a lock; <see cref="Open"/> runs on
/// the WAL's constructor.</para>
/// </remarks>
internal sealed class SolariumHardStateLedger : IDisposable
{
    /// <summary>The ledger's file name in the database directory; the database ignores files whose names start with '.'.</summary>
    public const string FileName = ".kommander-hard-state";

    /// <summary>The ledger is rewritten with one record per partition once it grows past this.</summary>
    internal const long CompactAboveBytes = 256 * 1024;

    private const uint Magic = 0x54534448;   // "HDST"

    // magic 4, partition 4, term 8, vote length 2; the vote bytes and the CRC-32 follow.
    private const int FixedHeaderSize = 18;
    private const int MaxVoteBytes = 1024;

    private readonly IStorageEnvironment _environment;
    private readonly string _directory;
    private readonly Lock _gate = new();
    private readonly Dictionary<int, Entry> _latest = [];
    private IStorageFile _file;
    private long _length;
    private bool _disposed;

    private SolariumHardStateLedger(IStorageEnvironment environment, string directory, IStorageFile file)
    {
        _environment = environment;
        _directory = directory;
        _file = file;
        FilePath = Path.Combine(directory, FileName);
    }

    /// <summary>One partition's last persisted term and vote.</summary>
    /// <param name="Term">The term.</param>
    /// <param name="VotedFor">The endpoint voted for in it, or null for none.</param>
    public readonly record struct Entry(long Term, string? VotedFor);

    /// <summary>The ledger file's full path, for log lines.</summary>
    public string FilePath { get; }

    /// <summary>
    /// Opens the ledger in <paramref name="directory"/> through <paramref name="environment"/>, creating it when
    /// missing, replays every whole record, and cuts a torn tail record off.
    /// </summary>
    /// <exception cref="IOException">The file could not be opened, read, cut or flushed.</exception>
    public static SolariumHardStateLedger Open(IStorageEnvironment environment, string directory)
    {
        string filePath = Path.Combine(directory, FileName);
        bool existed = environment.FileExists(filePath);
        SolariumHardStateLedger ledger = new(environment, directory, environment.OpenFile(filePath));
        try
        {
            ledger.Replay();
            if (!existed)
                environment.FlushDirectory(directory);
        }
        catch
        {
            ledger.Dispose();
            throw;
        }

        return ledger;
    }

    /// <summary>A copy of the last entry of each partition: the replayed ones and every append since.</summary>
    public Dictionary<int, Entry> Snapshot()
    {
        lock (_gate)
            return new Dictionary<int, Entry>(_latest);
    }

    /// <summary>Appends one record and flushes it.</summary>
    /// <exception cref="IOException">
    /// The write or the flush failed, or the vote is longer than the record allows. The caller must then report
    /// the hard-state write as not durable, since the database's copy alone is what this ledger backs up.
    /// </exception>
    public void Append(int partitionId, long term, string? votedFor)
    {
        byte[] record = Encode(partitionId, term, votedFor);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_length > CompactAboveBytes)
                Compact();

            _file.Write(record, _length);
            _file.Flush();
            _length += record.Length;
            _latest[partitionId] = new Entry(term, string.IsNullOrEmpty(votedFor) ? null : votedFor);
        }
    }

    private static byte[] Encode(int partitionId, long term, string? votedFor)
    {
        int voteBytes = string.IsNullOrEmpty(votedFor) ? 0 : Encoding.UTF8.GetByteCount(votedFor);
        if (voteBytes > MaxVoteBytes)
            throw new IOException($"The hard-state ledger cannot record a vote of {voteBytes} bytes; the bound is {MaxVoteBytes}.");

        byte[] record = new byte[FixedHeaderSize + voteBytes + 4];
        Span<byte> span = record;
        BinaryPrimitives.WriteUInt32LittleEndian(span, Magic);
        BinaryPrimitives.WriteInt32LittleEndian(span[4..], partitionId);
        BinaryPrimitives.WriteInt64LittleEndian(span[8..], term);
        BinaryPrimitives.WriteUInt16LittleEndian(span[16..], (ushort)voteBytes);
        if (voteBytes > 0)
            Encoding.UTF8.GetBytes(votedFor, span.Slice(FixedHeaderSize, voteBytes));

        int payload = FixedHeaderSize + voteBytes;
        BinaryPrimitives.WriteUInt32LittleEndian(span[payload..], Crc32.HashToUInt32(span[..payload]));
        return record;
    }

    // Reads every whole record into _latest; a torn tail is cut and the cut flushed.
    private void Replay()
    {
        long length = _file.GetLength();
        if (length == 0)
            return;

        byte[] bytes = new byte[length];
        int read = 0;
        while (read < bytes.Length)
        {
            int n = _file.Read(bytes.AsSpan(read), read);
            if (n == 0)
                break;

            read += n;
        }

        int offset = 0;
        while (TryDecode(bytes.AsSpan(0, read), offset, out int partitionId, out Entry entry, out int size))
        {
            _latest[partitionId] = entry;
            offset += size;
        }

        // Records are not self-synchronising, so nothing after a bad one can be trusted, and nothing after it
        // was ever reported durable: a crash during an append leaves exactly this.
        if (offset < length)
        {
            _file.SetLength(offset);
            _file.Flush();
        }

        _length = offset;
    }

    private static bool TryDecode(ReadOnlySpan<byte> bytes, int offset, out int partitionId, out Entry entry, out int size)
    {
        partitionId = 0;
        entry = default;
        size = 0;
        if (bytes.Length - offset < FixedHeaderSize + 4)
            return false;

        ReadOnlySpan<byte> span = bytes[offset..];
        if (BinaryPrimitives.ReadUInt32LittleEndian(span) != Magic)
            return false;

        int voteBytes = BinaryPrimitives.ReadUInt16LittleEndian(span[16..]);
        if (voteBytes > MaxVoteBytes || span.Length < FixedHeaderSize + voteBytes + 4)
            return false;

        int payload = FixedHeaderSize + voteBytes;
        if (BinaryPrimitives.ReadUInt32LittleEndian(span[payload..]) != Crc32.HashToUInt32(span[..payload]))
            return false;

        partitionId = BinaryPrimitives.ReadInt32LittleEndian(span[4..]);
        string? votedFor = voteBytes == 0 ? null : Encoding.UTF8.GetString(span.Slice(FixedHeaderSize, voteBytes));
        entry = new Entry(BinaryPrimitives.ReadInt64LittleEndian(span[8..]), votedFor);
        size = payload + 4;
        return true;
    }

    // The file rewritten with one record per partition: a sibling file written and flushed, then renamed over the
    // ledger and the directory flushed, so a crash leaves the old file or the new one, each complete. Under the gate.
    private void Compact()
    {
        string temporary = FilePath + ".tmp";
        long length = 0;
        using (IStorageFile fresh = _environment.OpenFile(temporary))
        {
            fresh.SetLength(0);
            foreach ((int partitionId, Entry entry) in _latest)
            {
                byte[] record = Encode(partitionId, entry.Term, entry.VotedFor);
                fresh.Write(record, length);
                length += record.Length;
            }

            fresh.Flush();
        }

        // Reopened whatever the rename did: the ledger it names, the old one or the new, is complete either way.
        _file.Dispose();
        long kept = _length;
        try
        {
            _environment.RenameFile(temporary, FilePath);
            kept = length;
            _environment.FlushDirectory(_directory);
        }
        finally
        {
            _file = _environment.OpenFile(FilePath);
            _length = kept;
        }
    }

    /// <summary>Closes the file. A later <see cref="Append"/> throws <see cref="ObjectDisposedException"/>.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;

            _disposed = true;
            _file.Dispose();
        }
    }
}
