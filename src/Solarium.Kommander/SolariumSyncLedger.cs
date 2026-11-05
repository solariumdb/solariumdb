// Ported from Kommander's RocksDbSyncLedger (https://github.com/kahunakv/kommander, Kommander/WAL/RocksDbSyncLedger.cs
// at commit e2763bc), MIT License, Copyright (c) 2025 Andres Gutierrez. The file name and the record layout are the
// same; the file goes through the WAL's storage environment.

using System.Buffers.Binary;
using System.IO.Hashing;
using Solarium.Storage;

namespace Solarium.Kommander;

/// <summary>
/// The sync ledger: a small file beside the database of a <see cref="SolariumWAL"/> that attests, outside the
/// database's own WAL, the highest sequence number a synced write of this process completed.
/// </summary>
/// <remarks>
/// <para>Why it exists. Solarium cuts the newest WAL file at its first damage unless an intact later record
/// proves the damage was flushed, and a file truncated after its fsync leaves no later record: the database
/// opens as if the lost records had never been written. Every proof kept inside that WAL (the
/// acknowledged-presence watermark, the term and the vote) is cut with the rows it protects, so what survives
/// is consistent and the loss is invisible. The proof has to live in a file the truncation cannot reach.</para>
/// <para>What it records. One number: the last sequence of a batch, after the database's synced write of
/// that batch returned (<see cref="WriteBatch.Sequence"/>). A synced write makes every sequence at or below
/// its batch's last durable, so at open the WAL compares the attested sequence with the one the database
/// replayed (<see cref="SolariumDb.LatestSequenceNumber"/>), and a replay that stops short lost synced writes.
/// One number covers the rows of every partition, the watermarks, the hard state, the frontiers and the
/// floors.</para>
/// <para>No false positive. The ledger is written only after the synced write returned, with that batch's
/// own sequence, never the database's latest, which a concurrent unsynced write may have raised. A crash
/// between the two leaves the ledger behind, which is harmless; it is never ahead of what was flushed.</para>
/// <para>Its own durability. Two 512-byte slots, written alternately and each flushed before the next write
/// begins, so at most one slot is ever torn and the other holds the previous attestation. A torn slot (bad
/// checksum) is skipped; both unreadable is treated as no ledger, with a warning, because a crash cannot
/// leave that shape and a loss is reported only on evidence. Each record carries the database's identity, so
/// a ledger copied from another directory is reset instead of believed. The file is preallocated and written
/// in place: an attestation is one sector write and one flush, and a full disk cannot refuse it.</para>
/// <para>Cost. One flush per synced write, grouped: concurrent writers raise a shared target, and the first
/// one through the gate writes it for all of them.</para>
/// <para>Thread-safety. <see cref="Attest"/> is safe from any thread. <see cref="Open"/>, <see cref="Adopt"/>
/// and <see cref="Initialize"/> run on the WAL's constructor before any writer exists.</para>
/// </remarks>
internal sealed class SolariumSyncLedger : IDisposable
{
    /// <summary>The ledger's file name in the database directory; the database ignores files whose names start with '.'.</summary>
    public const string FileName = ".kommander-sync-ledger";

    private const uint Magic = 0x4C59534B;   // "KSYL"
    private const uint FormatVersion = 1;
    private const int SlotSize = 512;
    private const int SlotCount = 2;

    // magic 4, version 4, generation 8, sequence 8, identity 16, CRC-32 4.
    private const int RecordSize = 44;

    private readonly IStorageFile _file;
    private readonly byte[] _slot = new byte[SlotSize];
    private readonly Lock _gate = new();
    private Guid _identity;
    private long _generation;
    private long _attested = -1;
    private long _pending = -1;
    private bool _disposed;

    private SolariumSyncLedger(IStorageFile file, string filePath)
    {
        _file = file;
        FilePath = filePath;
    }

    /// <summary>What <see cref="Open"/> found in the file.</summary>
    public enum ReadStatus
    {
        /// <summary>No file, or no slot was ever written.</summary>
        Absent,

        /// <summary>At least one slot holds a verified record; the newest is reported.</summary>
        Valid,

        /// <summary>Every written slot fails its checksum. No crash leaves this; it is treated as absent, with a warning.</summary>
        Unreadable,
    }

    /// <summary>One attestation as a slot holds it.</summary>
    /// <param name="Identity">The identity of the database the ledger was written for.</param>
    /// <param name="Sequence">The attested sequence.</param>
    /// <param name="Generation">The record's number; the higher of the two slots is the newest.</param>
    public readonly record struct Record(Guid Identity, long Sequence, long Generation);

    /// <summary>The ledger file's full path, for log lines.</summary>
    public string FilePath { get; }

    /// <summary>The highest sequence this ledger attests as durable; -1 before the first attestation.</summary>
    public long AttestedSequence => Volatile.Read(ref _attested);

    /// <summary>
    /// Opens the ledger in <paramref name="directory"/> through <paramref name="environment"/>, creating and
    /// preallocating it when it is missing, and reads the newest verified record. The caller decides what the
    /// record means against the database, then calls <see cref="Adopt"/> or <see cref="Initialize"/> before the
    /// first <see cref="Attest"/>.
    /// </summary>
    /// <exception cref="IOException">The file could not be opened, read, preallocated or flushed.</exception>
    public static SolariumSyncLedger Open(IStorageEnvironment environment, string directory, out ReadStatus status, out Record record)
    {
        string filePath = Path.Combine(directory, FileName);
        bool existed = environment.FileExists(filePath);
        SolariumSyncLedger ledger = new(environment.OpenFile(filePath), filePath);
        try
        {
            (status, record) = ledger.ReadNewest();

            // Both slots preallocated, so every later attestation overwrites allocated blocks in place.
            if (ledger._file.GetLength() < SlotSize * SlotCount)
            {
                ledger._file.Write(new byte[SlotSize * SlotCount], 0);
                ledger._file.Flush();
            }

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

    /// <summary>Continues a ledger of this database: later attestations alternate slots from the record's generation and never lower its sequence.</summary>
    public void Adopt(in Record record)
    {
        _identity = record.Identity;
        _generation = record.Generation;
        Volatile.Write(ref _attested, record.Sequence);
        Volatile.Write(ref _pending, record.Sequence);
    }

    /// <summary>
    /// Starts the ledger over for <paramref name="databaseIdentity"/> at <paramref name="sequence"/>: a new
    /// directory, one that predates the ledger, or a ledger of another database. <paramref name="afterGeneration"/>
    /// is the generation of the record being superseded, so the new record outranks it at the next read. The
    /// record is durable when this returns.
    /// </summary>
    /// <exception cref="IOException">The write or the flush failed.</exception>
    public void Initialize(Guid databaseIdentity, long sequence, long afterGeneration = 0)
    {
        lock (_gate)
        {
            _identity = databaseIdentity;
            _generation = Math.Max(afterGeneration, 0);
            long start = Math.Max(sequence, 0);
            WriteRecord(start);
            Volatile.Write(ref _attested, start);
            Volatile.Write(ref _pending, start);
        }
    }

    /// <summary>
    /// Makes <paramref name="sequence"/> part of the ledger's durable attestation; returns once a record at or
    /// above it is on the device. Grouped: concurrent callers raise one target, which the first through the gate writes.
    /// </summary>
    /// <exception cref="IOException">The write or the flush failed. The caller must then report its own write as failed: a promise the ledger does not cover must not be made.</exception>
    public void Attest(long sequence)
    {
        if (sequence <= Volatile.Read(ref _attested))
            return;

        long seen = Volatile.Read(ref _pending);
        while (seen < sequence)
        {
            long prior = Interlocked.CompareExchange(ref _pending, sequence, seen);
            if (prior == seen)
                break;

            seen = prior;
        }

        lock (_gate)
        {
            if (sequence <= _attested)
                return;

            long target = Volatile.Read(ref _pending);
            WriteRecord(target);
            Volatile.Write(ref _attested, target);
        }
    }

    // One record with the next generation, into the other slot, flushed. Under the gate.
    private void WriteRecord(long sequence)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        long next = _generation + 1;
        int slot = (int)(next % SlotCount);
        Span<byte> record = _slot;
        record.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(record, Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(record[4..], FormatVersion);
        BinaryPrimitives.WriteInt64LittleEndian(record[8..], next);
        BinaryPrimitives.WriteInt64LittleEndian(record[16..], sequence);
        _identity.TryWriteBytes(record.Slice(24, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(record[40..], Crc32.HashToUInt32(record[..40]));

        _file.Write(_slot, (long)slot * SlotSize);
        _file.Flush();
        _generation = next;
    }

    private (ReadStatus Status, Record Record) ReadNewest()
    {
        ReadStatus status = ReadStatus.Absent;
        Record newest = default;
        byte[] buffer = new byte[SlotSize];
        for (int slot = 0; slot < SlotCount; slot++)
        {
            Array.Clear(buffer);
            int read = 0;
            while (read < RecordSize)
            {
                int n = _file.Read(buffer.AsSpan(read), (long)slot * SlotSize + read);
                if (n == 0)
                    break;

                read += n;
            }

            if (read < RecordSize)
                continue;

            ReadOnlySpan<byte> record = buffer;
            uint magic = BinaryPrimitives.ReadUInt32LittleEndian(record);
            if (magic == 0)
                continue;

            bool valid = magic == Magic
                && BinaryPrimitives.ReadUInt32LittleEndian(record[4..]) == FormatVersion
                && BinaryPrimitives.ReadUInt32LittleEndian(record[40..]) == Crc32.HashToUInt32(record[..40]);
            if (!valid)
            {
                if (status != ReadStatus.Valid)
                    status = ReadStatus.Unreadable;

                continue;
            }

            Record candidate = new(new Guid(record.Slice(24, 16)), BinaryPrimitives.ReadInt64LittleEndian(record[16..]), BinaryPrimitives.ReadInt64LittleEndian(record[8..]));
            if (status != ReadStatus.Valid || candidate.Generation > newest.Generation)
                newest = candidate;

            status = ReadStatus.Valid;
        }

        return (status, newest);
    }

    /// <summary>Closes the file. A later <see cref="Attest"/> throws <see cref="ObjectDisposedException"/>.</summary>
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
