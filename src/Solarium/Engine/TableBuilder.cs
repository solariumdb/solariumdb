using System.Buffers;
using System.IO.Hashing;
using K4os.Compression.LZ4;
using Solarium.Storage;
using static System.Buffers.Binary.BinaryPrimitives;

namespace Solarium.Engine;

/// <summary>
/// Writes an SSTable from entries that arrive in internal key order: all at once with
/// <see cref="Build"/>, or one entry at a time with <see cref="Add"/> and then <see cref="Finish"/>.
/// Dispose it when it does not finish, so its write buffer goes back to the pool.
/// </summary>
/// <remarks>
/// The bytes reach the file through a buffer of <see cref="WriteBufferSize"/>, as RocksDB's
/// <c>WritableFileWriter</c> with its default <c>writable_file_max_buffer_size</c>, so a table takes one
/// write for each megabyte instead of two for each block, and its <see cref="TablePacing"/> asks the rate
/// limiter once for each.
/// </remarks>
internal sealed class TableBuilder : IDisposable
{
    /// <summary>The bytes the builder gathers before it writes them to the file.</summary>
    public const int WriteBufferSize = 1 << 20;

    private readonly IStorageFile _file;
    private readonly CompressionType _compression;
    private readonly TablePacing _pacing;
    private byte[]? _buffer = ArrayPool<byte>.Shared.Rent(WriteBufferSize);
    private int _buffered;
    private long _written;   // the bytes of the file written, below _offset by the buffered ones
    private long _synced;    // the bytes of the file whose writeback has started
    private readonly Crc32 _crc = new();
    private readonly ArrayBufferWriter<byte> _block = new(TableFormat.BlockSize * 2);
    private readonly int _partitionSize;
    private readonly ArrayBufferWriter<byte> _partitionIndex = new();   // the index entries of the open partition
    private readonly List<ulong> _partitionHashes = [];                // the user key hashes of the open partition
    private readonly List<Partition> _partitions = [];                 // the partitions cut, written at Finish
    private long _offset;
    private int _partitionBlocks;
    private long _entryCount;
    private long _minSequence = long.MaxValue;
    private long _maxSequence;
    private byte[]? _smallest;
    private byte[]? _largest;
    private Entry _lastInBlock;
    private byte[] _compressed = [];   // the size prefix and the LZ4 bytes of the last data block, reused

    /// <summary>
    /// Starts a table at the beginning of <paramref name="file"/>, which the caller owns, whose data
    /// blocks are stored as <paramref name="compression"/> says, and whose writes are paced as
    /// <paramref name="pacing"/> says.
    /// </summary>
    public TableBuilder(IStorageFile file, CompressionType compression = CompressionType.None, TablePacing pacing = default)
        : this(file, compression, pacing, TableFormat.PartitionSize)
    {
    }

    /// <summary>A builder that cuts an index partition at <paramref name="partitionSize"/> bytes of index entries; for tests.</summary>
    internal TableBuilder(IStorageFile file, CompressionType compression, TablePacing pacing, int partitionSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionSize, 1);
        _file = file;
        _compression = compression;
        _pacing = pacing;
        _partitionSize = partitionSize;
    }

    /// <summary>The entries added so far.</summary>
    public long EntryCount => _entryCount;

    /// <summary>The bytes added so far, written or buffered, plus the data block not yet closed: the size of the table without its metadata blocks.</summary>
    public long EstimatedSize => _offset + _block.WrittenCount;

    /// <summary>
    /// Write a whole table, its data blocks stored as <paramref name="compression"/> says and its writes
    /// paced as <paramref name="pacing"/> says, and flush the file.
    /// </summary>
    public static TableProperties Build(IStorageFile file, IEnumerable<Entry> entries, IReadOnlyList<RangeTombstone> tombstones,
        CompressionType compression = CompressionType.None, TablePacing pacing = default)
    {
        using TableBuilder builder = new(file, compression, pacing);
        foreach (Entry entry in entries)
            builder.Add(entry);

        return builder.Finish(tombstones);
    }

    /// <summary>Add an entry; it must come after every entry added before it, in internal key order.</summary>
    public void Add(Entry entry)
    {
        // Each partition's filter holds every user key in its blocks: a key whose versions continue into the
        // next partition goes into its filter too, as the first key there.
        if (_largest is null || _partitionHashes.Count == 0 || !entry.Key.AsSpan().SequenceEqual(_largest))
            _partitionHashes.Add(XxHash3.HashToUInt64(entry.Key));

        _smallest ??= entry.Key;
        _largest = entry.Key;
        NoteSequence(entry.Sequence);
        _entryCount++;

        int valueLength = entry.Value?.Length ?? 0;
        Span<byte> span = _block.GetSpan(4 + entry.Key.Length + 13 + valueLength);
        WriteInt32LittleEndian(span, entry.Key.Length);
        entry.Key.CopyTo(span[4..]);
        int position = 4 + entry.Key.Length;
        WriteInt64LittleEndian(span[position..], entry.Sequence);
        span[position + 8] = (byte)entry.Kind;
        WriteInt32LittleEndian(span[(position + 9)..], valueLength);
        entry.Value?.CopyTo(span[(position + 13)..]);
        _block.Advance(position + 13 + valueLength);
        _lastInBlock = entry;

        if (_block.WrittenCount >= TableFormat.BlockSize)
            FinishDataBlock();
    }

    /// <summary>
    /// Write the last data block, <paramref name="tombstones"/>, the metadata blocks and the footer,
    /// flush the file, and return what the manifest records about the table.
    /// </summary>
    public TableProperties Finish(IReadOnlyList<RangeTombstone> tombstones)
    {
        if (_block.WrittenCount > 0)
            FinishDataBlock();

        if (_partitionBlocks > 0)
            CutPartition();

        // Range tombstone block. The last entry's key, before a tombstone's end can raise the largest bound.
        byte[]? lastKey = _largest;
        foreach (RangeTombstone tombstone in tombstones)
        {
            NoteSequence(tombstone.Sequence);
            if (_smallest is null || tombstone.Begin.AsSpan().SequenceCompareTo(_smallest) < 0)
                _smallest = tombstone.Begin;

            if (_largest is null || tombstone.End.AsSpan().SequenceCompareTo(_largest) > 0)
                _largest = tombstone.End;
        }

        ArrayBufferWriter<byte> payload = new();
        WriteInt32(payload, tombstones.Count);
        foreach (RangeTombstone tombstone in tombstones)
        {
            WriteBytes(payload, tombstone.Begin);
            WriteBytes(payload, tombstone.End);
            WriteInt64(payload, tombstone.Sequence);
        }

        (long, int) rangeHandle = WriteBlock(payload.WrittenSpan);

        // Each partition's filter, then its index, next to each other: a lookup that misses a cache reads both.
        ArrayBufferWriter<byte> topLevel = new();
        WriteInt32(topLevel, _partitions.Count);
        foreach (Partition partition in _partitions)
        {
            (long filterOffset, int filterSize) = WriteBlock(partition.Filter);
            (long indexOffset, int indexSize) = WriteBlock(partition.Index);
            WriteBytes(topLevel, partition.LastKey);
            WriteInt64(topLevel, partition.LastSequence);
            WriteInt64(topLevel, indexOffset);
            WriteInt32(topLevel, indexSize);
            WriteInt64(topLevel, filterOffset);
            WriteInt32(topLevel, filterSize);
        }

        // Each partition names its filter, so the footer's filter handle is empty.
        (long, int) filterHandle = (0, 0);

        // Properties block.
        payload.ResetWrittenCount();
        WriteInt64(payload, _entryCount);
        WriteInt64(payload, _minSequence == long.MaxValue ? 0 : _minSequence);
        WriteInt64(payload, _maxSequence);
        WriteBytes(payload, _smallest ?? []);
        WriteBytes(payload, _largest ?? []);
        (long, int) propertiesHandle = WriteBlock(payload.WrittenSpan);

        (long, int) indexHandle = WriteBlock(topLevel.WrittenSpan);

        Span<byte> footer = stackalloc byte[TableFormat.FooterSize];
        int at = 0;
        foreach ((long offset, int size) in new[] { rangeHandle, filterHandle, propertiesHandle, indexHandle })
        {
            WriteInt64LittleEndian(footer[at..], offset);
            WriteInt32LittleEndian(footer[(at + 8)..], size);
            at += 12;
        }

        WriteUInt32LittleEndian(footer[at..], TableFormat.Version);
        WriteUInt32LittleEndian(footer[(at + 4)..], Crc32.HashToUInt32(footer[..(at + 4)]));
        WriteUInt64LittleEndian(footer[(at + 8)..], TableFormat.Magic);
        Append(footer);
        WriteBuffer();
        Dispose();
        _file.Flush();

        return new TableProperties(_offset, _entryCount, _minSequence == long.MaxValue ? 0 : _minSequence, _maxSequence, _smallest ?? [], _largest ?? [],
            TableProperties.IsExclusive(lastKey, _largest ?? []));
    }

    private void FinishDataBlock()
    {
        (long offset, int size) = WriteDataBlock(_block.WrittenSpan);
        _block.ResetWrittenCount();

        WriteBytes(_partitionIndex, _lastInBlock.Key);
        WriteInt64(_partitionIndex, _lastInBlock.Sequence);
        WriteInt64(_partitionIndex, offset);
        WriteInt32(_partitionIndex, size);
        _partitionBlocks++;
        if (_partitionIndex.WrittenCount >= _partitionSize)
            CutPartition();
    }

    // Closes the open partition after its last data block: its index block's payload and its filter, kept
    // until Finish writes them after the data blocks.
    private void CutPartition()
    {
        byte[] index = new byte[sizeof(int) + _partitionIndex.WrittenCount];
        WriteInt32LittleEndian(index, _partitionBlocks);
        _partitionIndex.WrittenSpan.CopyTo(index.AsSpan(sizeof(int)));
        _partitions.Add(new Partition(_lastInBlock.Key, _lastInBlock.Sequence, index, BloomFilter.Build(_partitionHashes)));

        _partitionIndex.ResetWrittenCount();
        _partitionHashes.Clear();
        _partitionBlocks = 0;
    }

    // A data block: the entries, or their LZ4 block when it is worth it, then the type and the CRC-32
    // of both. Kept raw unless compression saves an eighth, as RocksDB's GoodCompressionRatio: a
    // block that barely shrinks would cost a decompression on every read for almost nothing.
    private (long Offset, int Size) WriteDataBlock(ReadOnlySpan<byte> entries)
    {
        ReadOnlySpan<byte> payload = entries;
        byte type = TableFormat.RawBlock;
        if (_compression == CompressionType.Lz4)
        {
            int bound = sizeof(int) + LZ4Codec.MaximumOutputSize(entries.Length);
            if (_compressed.Length < bound)
                _compressed = new byte[bound];

            int encoded = LZ4Codec.Encode(entries, _compressed.AsSpan(sizeof(int)));
            if (encoded > 0 && sizeof(int) + encoded < entries.Length - (entries.Length / 8))
            {
                WriteInt32LittleEndian(_compressed, entries.Length);
                payload = _compressed.AsSpan(0, sizeof(int) + encoded);
                type = TableFormat.Lz4Block;
            }
        }

        long offset = _offset;
        Append(payload);

        Span<byte> trailer = stackalloc byte[5];
        trailer[0] = type;
        _crc.Reset();
        _crc.Append(payload);
        _crc.Append(trailer[..1]);
        WriteUInt32LittleEndian(trailer[1..], _crc.GetCurrentHashAsUInt32());
        Append(trailer);

        return (offset, payload.Length + trailer.Length);
    }

    private (long Offset, int Size) WriteBlock(ReadOnlySpan<byte> payload)
    {
        long offset = _offset;
        Append(payload);

        Span<byte> checksum = stackalloc byte[4];
        WriteUInt32LittleEndian(checksum, Crc32.HashToUInt32(payload));
        Append(checksum);

        return (offset, payload.Length + 4);
    }

    /// <summary>Return the write buffer to the pool. A builder that has not finished writes nothing more.</summary>
    public void Dispose()
    {
        if (_buffer is not null)
            ArrayPool<byte>.Shared.Return(_buffer);

        _buffer = null;
    }

    // The bytes at the end of the table, through the buffer.
    private void Append(ReadOnlySpan<byte> bytes)
    {
        byte[] buffer = _buffer ?? throw new ObjectDisposedException(nameof(TableBuilder));
        while (!bytes.IsEmpty)
        {
            if (_buffered == WriteBufferSize)
                WriteBuffer();

            int count = Math.Min(bytes.Length, WriteBufferSize - _buffered);
            bytes[..count].CopyTo(buffer.AsSpan(_buffered));
            _buffered += count;
            _offset += count;
            bytes = bytes[count..];
        }
    }

    // The buffered bytes to the file, once the rate limiter grants them, and the writeback of what the
    // file holds once another BytesPerSync of it is written. A failure of either fails the table.
    private void WriteBuffer()
    {
        if (_buffered == 0)
            return;

        byte[] buffer = _buffer ?? throw new ObjectDisposedException(nameof(TableBuilder));
        _pacing.Limiter?.Request(_buffered, _pacing.High);
        _file.Write(buffer.AsSpan(0, _buffered), _written);
        _written += _buffered;
        _buffered = 0;
        if (_pacing.BytesPerSync > 0 && _written - _synced >= _pacing.BytesPerSync)
        {
            _file.SyncRange(_synced, _written - _synced);
            _synced = _written;
        }
    }

    private void NoteSequence(long sequence)
    {
        _minSequence = Math.Min(_minSequence, sequence);
        _maxSequence = Math.Max(_maxSequence, sequence);
    }

    private static void WriteInt32(IBufferWriter<byte> output, int value)
    {
        WriteInt32LittleEndian(output.GetSpan(4), value);
        output.Advance(4);
    }

    private static void WriteInt64(IBufferWriter<byte> output, long value)
    {
        WriteInt64LittleEndian(output.GetSpan(8), value);
        output.Advance(8);
    }

    private static void WriteBytes(IBufferWriter<byte> output, byte[] bytes)
    {
        WriteInt32(output, bytes.Length);
        output.Write(bytes);
    }

    // A partition cut and not yet written: the last internal key of its last block, its index block's
    // payload, and its filter block's payload.
    private readonly record struct Partition(byte[] LastKey, long LastSequence, byte[] Index, byte[] Filter);
}
