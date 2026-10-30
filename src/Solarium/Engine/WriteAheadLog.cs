using System.Buffers;
using System.IO.Hashing;
using Solarium.Storage;
using static System.Buffers.Binary.BinaryPrimitives;

namespace Solarium.Engine;

/// <summary>
/// The write-ahead log of a database, shared by all its column families. One record holds one
/// whole <see cref="WriteBatch"/>, so a batch is replayed all together or not at all.
/// </summary>
/// <remarks>
/// File and record layout, little-endian:
/// <code>
///   file      = [u32 magic "SWAL"][u32 version = 2][record]...
///   record    = [u32 payload length][u32 CRC-32 of payload][payload]
///   payload   = [u64 first sequence number][u64 durable offset][u32 operation count][operation]...
///   operation = [u8 kind][u32 column family id][u32 key length][key]                       delete
///             | [u8 kind][u32 column family id][u32 key length][key][u32 value length][value]  put, delete range
/// </code>
/// Operation i of a batch has sequence number <c>first + i</c>. The durable offset is the length of
/// the file that a completed flush had made durable when the record was appended, so a record after
/// damage tells whether the damaged bytes had been flushed. A file of version 1, written before the
/// header existed, starts with its first record, and its payload has no durable offset.
/// </remarks>
internal sealed class WriteAheadLog : IDisposable
{
    private const int HeaderSize = 8;
    private const int FileHeaderSize = 8;
    private const uint Magic = 0x4C415753;   // "SWAL": as a version 1 record length it exceeds MaxPayload
    private const int Version = 2;
    private const int MaxPayload = 256 * 1024 * 1024;
    private const int MinOperationSize = 9;   // a delete of an empty key: kind, family id, key length

    private readonly IStorageFile _file;
    private readonly int _version;
    private long _end;       // written by the appending thread only
    private long _durable;   // raised by any thread that flushed the file

    private WriteAheadLog(IStorageFile file, int version, long end, long durable)
    {
        _file = file;
        _version = version;
        _end = end;
        _durable = durable;
    }

    /// <summary>Bytes of the file up to the end of its last intact record.</summary>
    public long Length => Volatile.Read(ref _end);

    /// <summary>The length of the file that a completed flush made durable, for <see cref="CopyRecord"/>.</summary>
    public long DurableLength => Volatile.Read(ref _durable);

    /// <summary>
    /// Start a log in <paramref name="file"/>, which must be empty: its header is written and flushed
    /// before any record, so a crash never leaves a record without it.
    /// </summary>
    public static WriteAheadLog Create(IStorageFile file)
    {
        Span<byte> header = stackalloc byte[FileHeaderSize];
        WriteUInt32LittleEndian(header, Magic);
        WriteInt32LittleEndian(header[4..], Version);
        file.Write(header, 0);
        file.Flush();
        return new WriteAheadLog(file, Version, FileHeaderSize, FileHeaderSize);
    }

    /// <summary>
    /// Replay every intact batch, in order, through <paramref name="replay"/>, and cut off what a
    /// crash left after the last one. Damage that a crash cannot leave fails the open instead of
    /// dropping the data after it.
    /// </summary>
    /// <remarks>
    /// A process crash can only cut the last write short. A power loss can also lose any part of
    /// what was written after the last flush, out of order, and leave intact records after the
    /// damage. So in a version 2 file, damage is cut off with everything after it unless an intact
    /// record after it shows, by its durable offset, that the damaged bytes had been flushed: then
    /// no crash explains it, and the open fails. A version 1 file has no durable offsets; any
    /// damage with data after it fails the open, unless the rest is the end of a record or zeros.
    /// </remarks>
    /// <param name="tolerateTornTail">
    /// False for a WAL file that is not the newest: it was flushed before the next file started, so
    /// a crash cannot damage it, and any damage fails the open.
    /// </param>
    public static WriteAheadLog Open(IStorageFile file, Action<long, List<WalOperation>> replay, bool tolerateTornTail = true)
    {
        long fileLength = file.GetLength();
        int version = ReadVersion(file, fileLength);
        long first = version == Version ? Math.Min(FileHeaderSize, fileLength) : 0;
        byte[] buffer = new byte[1 << 20];
        long bufferOffset = first;   // file offset of buffer[0]
        int start = 0;               // first unparsed byte in buffer
        int filled = file.Read(buffer, first);
        bool endOfFile = filled == 0;
        long lastSequence = 0;

        while (true)
        {
            long recordOffset = bufferOffset + start;
            ParseResult result = TryParse(buffer.AsSpan(start, filled - start), version, out int size, out ReadOnlySpan<byte> payload);

            if (result == ParseResult.Record)
            {
                (long firstSequence, _, List<WalOperation> operations) = DecodeBatch(payload, version, recordOffset);
                replay(firstSequence, operations);
                lastSequence = firstSequence + operations.Count - 1;
                start += size;
                continue;
            }

            if (result == ParseResult.NeedMore && !endOfFile)
            {
                // Move the partial record to the front, grow the buffer if it cannot fit, read more.
                int partial = filled - start;
                if (size > buffer.Length)
                {
                    byte[] larger = new byte[size];
                    buffer.AsSpan(start, partial).CopyTo(larger);
                    buffer = larger;
                }
                else
                    buffer.AsSpan(start, partial).CopyTo(buffer);

                bufferOffset += start;
                start = 0;
                filled = partial;

                int read = file.Read(buffer.AsSpan(filled), bufferOffset + filled);
                endOfFile = read == 0;
                filled += read;
                continue;
            }

            bool cleanEnd = result == ParseResult.NeedMore && recordOffset == fileLength;
            if (!cleanEnd)
            {
                if (!tolerateTornTail)
                    throw new SolariumException(
                        $"The WAL is corrupt at offset {recordOffset}, in a file that a newer WAL file follows. " +
                        "That file was flushed before the next one started, so this is not the result of a crash.");

                if (version == Version)
                {
                    long proof = DurableOffsetAfter(file, recordOffset, fileLength, lastSequence);
                    if (proof >= 0)
                        throw new SolariumException(
                            $"The WAL is corrupt at offset {recordOffset}, which the intact record at offset {proof} shows had been flushed. " +
                            "No crash explains this; the database refuses to open rather than drop the data after it.");
                }
                else if (!IsTornTail(file, result, recordOffset, size, fileLength))
                    throw new SolariumException(
                        $"The WAL is corrupt at offset {recordOffset}, with intact data after it, so the database refuses to open rather than drop that data. " +
                        "This file was written before WAL records said what was flushed: a power loss can leave this when the device wrote the unsynced end out of order, " +
                        "but so can damage to flushed data.");
            }

            if (fileLength > recordOffset)
            {
                file.SetLength(recordOffset);
                file.Flush();
            }

            return new WriteAheadLog(file, version, recordOffset, recordOffset);
        }
    }

    /// <summary>
    /// The length of the record that holds <paramref name="operations"/>, for
    /// <see cref="EncodeRecord"/>. Called before a batch joins a write group, so an oversized batch
    /// fails alone.
    /// </summary>
    /// <exception cref="ArgumentException">The operations do not fit in one record.</exception>
    public static int CheckSize(List<BatchOperation> operations)
    {
        long payloadLength = BatchHeaderSize(Version);
        foreach (BatchOperation operation in operations)
            payloadLength += OperationSize(operation.Kind, operation.Key, operation.Value);

        if (payloadLength > MaxPayload)
            throw new ArgumentException($"A write batch is limited to {MaxPayload} bytes of encoded operations; this one has {payloadLength}.");

        return HeaderSize + (int)payloadLength;
    }

    /// <summary>
    /// Encode <paramref name="operations"/> into <paramref name="record"/>, which is exactly
    /// <see cref="CheckSize"/> bytes long, all but the first sequence number, the durable offset and
    /// the CRC: the writer of a batch encodes it before it joins a write group, and the group's
    /// leader completes it in <see cref="CopyRecord"/>. An operation with no family is in
    /// <paramref name="defaultFamilyId"/>.
    /// </summary>
    public static void EncodeRecord(Span<byte> record, List<BatchOperation> operations, int defaultFamilyId)
    {
        Span<byte> payload = record[HeaderSize..];
        WriteUInt32LittleEndian(record, (uint)payload.Length);
        WriteInt32LittleEndian(payload[16..], operations.Count);

        int position = BatchHeaderSize(Version);
        foreach (BatchOperation operation in operations)
            position = WriteOperation(payload, position, operation.Kind, operation.Family?.Id ?? defaultFamilyId, operation.Key, operation.Value);
    }

    /// <summary>
    /// Append a copy of a record of <see cref="EncodeRecord"/> to <paramref name="output"/>, with
    /// <paramref name="firstSequence"/>, the log's <paramref name="durableOffset"/> (its
    /// <see cref="DurableLength"/>), and the CRC of its payload. <paramref name="record"/> is not
    /// changed, so it can belong to another writer.
    /// </summary>
    public static void CopyRecord(IBufferWriter<byte> output, ReadOnlySpan<byte> record, long firstSequence, long durableOffset)
    {
        Span<byte> copy = output.GetSpan(record.Length)[..record.Length];
        record.CopyTo(copy);
        CompleteRecord(copy, firstSequence, durableOffset);
        output.Advance(record.Length);
    }

    /// <summary>
    /// Complete a record of <see cref="EncodeRecord"/> in place, as <see cref="CopyRecord"/> completes
    /// its copy: with <paramref name="firstSequence"/>, the log's <paramref name="durableOffset"/>, and
    /// the CRC of its payload. Only for a record that nothing else reads.
    /// </summary>
    public static void CompleteRecord(Span<byte> record, long firstSequence, long durableOffset)
    {
        Span<byte> payload = record[HeaderSize..];
        WriteInt64LittleEndian(payload, firstSequence);
        WriteInt64LittleEndian(payload[8..], durableOffset);
        WriteUInt32LittleEndian(record[4..], Crc32.HashToUInt32(payload));
    }

    /// <summary>
    /// Encode one whole batch record from operations that already name their family by id, as a
    /// test that writes a WAL by hand has them: of <paramref name="version"/> 2 with
    /// <paramref name="durableOffset"/>, or of version 1, which has none. The engine encodes with
    /// <see cref="EncodeRecord"/> and <see cref="CopyRecord"/>.
    /// </summary>
    public static void EncodeBatch(IBufferWriter<byte> output, long firstSequence, List<WalOperation> operations, int version = Version, long durableOffset = 0)
    {
        long payloadLength = BatchHeaderSize(version);
        foreach (WalOperation operation in operations)
            payloadLength += OperationSize(operation.Kind, operation.Key, operation.Value);

        int recordLength = HeaderSize + (int)payloadLength;
        Span<byte> record = output.GetSpan(recordLength)[..recordLength];
        Span<byte> payload = record[HeaderSize..];

        WriteInt64LittleEndian(payload, firstSequence);
        if (version == Version)
            WriteInt64LittleEndian(payload[8..], durableOffset);

        WriteInt32LittleEndian(payload[(BatchHeaderSize(version) - 4)..], operations.Count);
        int position = BatchHeaderSize(version);

        foreach (WalOperation operation in operations)
            position = WriteOperation(payload, position, operation.Kind, operation.FamilyId, operation.Key, operation.Value);

        WriteUInt32LittleEndian(record, (uint)payloadLength);
        WriteUInt32LittleEndian(record[4..], Crc32.HashToUInt32(payload));
        output.Advance(recordLength);
    }

    /// <summary>Make every append so far durable.</summary>
    public void Flush()
    {
        long end = Length;
        _file.Flush();
        NoteDurable(end);
    }

    /// <summary>
    /// Record that a flush through another handle of the file, which started after the first
    /// <paramref name="length"/> bytes were written, made them durable. Any thread.
    /// </summary>
    public void NoteDurable(long length)
    {
        long seen = Volatile.Read(ref _durable);
        while (length > seen)
        {
            long prior = Interlocked.CompareExchange(ref _durable, length, seen);
            if (prior == seen)
                return;

            seen = prior;
        }
    }

    /// <summary>
    /// Write <paramref name="records"/> after the last record; flush them when <paramref name="sync"/>
    /// is set. One thread at a time, on a log from <see cref="Create"/>.
    /// </summary>
    public void Append(ReadOnlySpan<byte> records, bool sync)
    {
        if (_version != Version)
            throw new InvalidOperationException("A WAL file of an older version is only replayed, never appended to.");

        long end = _end + records.Length;
        _file.Write(records, _end);
        if (sync)
        {
            _file.Flush();
            NoteDurable(end);
        }

        Volatile.Write(ref _end, end);
    }

    /// <summary>Closes the log file.</summary>
    public void Dispose() => _file.Dispose();

    // The fields before the first operation: the first sequence number, the durable offset (version
    // 2), and the operation count.
    private static int BatchHeaderSize(int version) => version == Version ? 20 : 12;

    // 2 for a file that starts with the header, 1 for one written before it existed. An empty file,
    // or one cut short before its header was flushed, holds no record of either.
    private static int ReadVersion(IStorageFile file, long fileLength)
    {
        if (fileLength < FileHeaderSize)
            return Version;

        Span<byte> header = stackalloc byte[FileHeaderSize];
        if (ReadFully(file, header, 0) == FileHeaderSize && ReadUInt32LittleEndian(header) == Magic)
        {
            int version = ReadInt32LittleEndian(header[4..]);
            return version == Version
                ? version
                : throw new SolariumException($"The WAL file has version {version}; this version of Solarium reads {Version} and 1.");
        }

        // The header is flushed before any record is written, so a file whose first bytes are zeros
        // was cut short at its creation.
        return header.ContainsAnyExcept((byte)0) ? 1 : Version;
    }

    private static long OperationSize(OperationKind kind, byte[] key, byte[]? value)
    {
        return 9 + key.Length + (kind == OperationKind.Delete ? 0 : 4 + value!.Length);
    }

    private static int WriteOperation(Span<byte> payload, int position, OperationKind kind, int familyId, byte[] key, byte[]? value)
    {
        payload[position] = (byte)kind;
        WriteInt32LittleEndian(payload[(position + 1)..], familyId);
        position = WriteBytes(payload, position + 5, key);

        if (kind != OperationKind.Delete)
            position = WriteBytes(payload, position, value!);
        return position;
    }

    private static int WriteBytes(Span<byte> destination, int position, byte[] bytes)
    {
        WriteInt32LittleEndian(destination[position..], bytes.Length);
        bytes.CopyTo(destination[(position + 4)..]);
        return position + 4 + bytes.Length;
    }

    private enum ParseResult { Record, NeedMore, BadLength, BadChecksum }

    // On NeedMore and BadChecksum, size is the byte count of the whole record.
    private static ParseResult TryParse(ReadOnlySpan<byte> data, int version, out int size, out ReadOnlySpan<byte> payload)
    {
        payload = default;
        size = HeaderSize;

        if (data.Length < HeaderSize)
            return ParseResult.NeedMore;

        uint payloadLength = ReadUInt32LittleEndian(data);
        if (payloadLength < BatchHeaderSize(version) || payloadLength > MaxPayload)
            return ParseResult.BadLength;

        size = HeaderSize + (int)payloadLength;
        if (data.Length < size)
            return ParseResult.NeedMore;

        payload = data.Slice(HeaderSize, (int)payloadLength);
        return Crc32.HashToUInt32(payload) == ReadUInt32LittleEndian(data[4..]) ? ParseResult.Record : ParseResult.BadChecksum;
    }

    // The checksum matched, so a payload that does not decode is a bug or a format mismatch.
    private static (long FirstSequence, long DurableOffset, List<WalOperation> Operations) DecodeBatch(ReadOnlySpan<byte> payload, int version, long recordOffset)
    {
        try
        {
            long firstSequence = ReadInt64LittleEndian(payload);
            long durableOffset = version == Version ? ReadInt64LittleEndian(payload[8..]) : 0;
            int position = BatchHeaderSize(version);
            int count = ReadInt32LittleEndian(payload[(position - 4)..]);
            if (count < 0 || count > (payload.Length - position) / MinOperationSize)
                throw new InvalidDataException($"an operation count of {count} does not fit in the record");

            List<WalOperation> operations = new(count);

            for (int i = 0; i < count; i++)
            {
                // The count check bounds the operations by their minimum size, not by the lengths
                // of the ones before, so each one's fixed fields are checked against what is left.
                if (payload.Length - position < 5)
                    throw new InvalidDataException($"operation {i} of {count} starts at {position}, past the end of the {payload.Length}-byte payload");

                OperationKind kind = (OperationKind)payload[position];
                int familyId = ReadInt32LittleEndian(payload[(position + 1)..]);
                byte[] key = ReadBytes(payload, ref position, position + 5);
                byte[]? value = kind == OperationKind.Delete ? null : ReadBytes(payload, ref position, position);

                if (kind is not (OperationKind.Put or OperationKind.Delete or OperationKind.DeleteRange))
                    throw new InvalidDataException($"unknown operation kind {(byte)kind}");

                operations.Add(new WalOperation(kind, familyId, key, value));
            }

            if (position != payload.Length)
                throw new InvalidDataException("trailing bytes after the last operation");

            return (firstSequence, durableOffset, operations);
        }
        catch (Exception e) when (e is InvalidDataException or ArgumentOutOfRangeException)
        {
            throw new SolariumException($"The WAL record at offset {recordOffset} has a valid checksum but does not decode: {e.Message}", e);
        }
    }

    private static byte[] ReadBytes(ReadOnlySpan<byte> payload, ref int position, int lengthAt)
    {
        int length = ReadInt32LittleEndian(payload[lengthAt..]);
        byte[] bytes = payload.Slice(lengthAt + 4, length).ToArray();
        position = lengthAt + 4 + length;
        return bytes;
    }

    // Version 1: damage that a crash can leave is the end of the last record or zeros to the end.
    private static bool IsTornTail(IStorageFile file, ParseResult result, long recordOffset, int size, long fileLength)
    {
        return result switch
        {
            ParseResult.NeedMore => true,                                       // cut short by the end of the file
            ParseResult.BadChecksum => recordOffset + size >= fileLength,       // the last record
            _ => IsZeroToEnd(file, recordOffset, fileLength),                   // a file extended with zeros
        };
    }

    // Version 2: the offset of an intact record after the damage at damaged whose durable offset is
    // past it, which proves the damaged bytes had been flushed; or -1 when there is none. Records
    // start anywhere, so every offset is tried; a candidate must be a later batch, with a durable
    // offset between the damage and its own start, before its checksum is computed.
    private static long DurableOffsetAfter(IStorageFile file, long damaged, long fileLength, long lastSequence)
    {
        const int Window = 1 << 20;
        int prefix = HeaderSize + BatchHeaderSize(Version);
        byte[] chunk = new byte[Window + prefix];
        for (long chunkOffset = damaged + 1; chunkOffset + prefix <= fileLength; chunkOffset += Window)
        {
            int read = ReadFully(file, chunk.AsSpan(0, (int)Math.Min(chunk.Length, fileLength - chunkOffset)), chunkOffset);
            for (int i = 0; i + prefix <= read && i < Window; i++)
            {
                long offset = chunkOffset + i;
                ReadOnlySpan<byte> candidate = chunk.AsSpan(i);
                uint payloadLength = ReadUInt32LittleEndian(candidate);
                if (payloadLength < BatchHeaderSize(Version) || payloadLength > MaxPayload || offset + HeaderSize + payloadLength > fileLength)
                    continue;

                long firstSequence = ReadInt64LittleEndian(candidate[HeaderSize..]);
                long durableOffset = ReadInt64LittleEndian(candidate[(HeaderSize + 8)..]);
                if (firstSequence <= lastSequence || durableOffset <= damaged || durableOffset > offset)
                    continue;

                byte[] payload = new byte[payloadLength];
                if (ReadFully(file, payload, offset + HeaderSize) == payload.Length
                    && Crc32.HashToUInt32(payload) == ReadUInt32LittleEndian(candidate[4..]))
                    return offset;
            }
        }

        return -1;
    }

    private static int ReadFully(IStorageFile file, Span<byte> buffer, long offset)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = file.Read(buffer[total..], offset + total);
            if (read == 0)
                break;

            total += read;
        }

        return total;
    }

    private static bool IsZeroToEnd(IStorageFile file, long offset, long fileLength)
    {
        Span<byte> chunk = new byte[64 * 1024];
        while (offset < fileLength)
        {
            int read = file.Read(chunk, offset);
            if (read == 0)
                return true;

            if (chunk[..read].ContainsAnyExcept((byte)0))
                return false;

            offset += read;
        }

        return true;
    }
}
