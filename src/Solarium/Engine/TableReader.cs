using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using K4os.Compression.LZ4;
using Solarium.Storage;
using static System.Buffers.Binary.BinaryPrimitives;

namespace Solarium.Engine;

/// <summary>
/// Reads an SSTable. The range tombstones and the top-level index stay in memory. The partitions of the index
/// and filter stay in memory too, or each in a <see cref="Cache"/> entry (<see cref="TableCaching"/>), which reads
/// it again from the file after an eviction. A data block is read from the file when a lookup or a cursor needs
/// it, unless the table caches data blocks and its cache holds the block's verified and decompressed entries.
/// Safe from any thread.
/// </summary>
internal sealed class TableReader : IEntrySource, IDisposable
{
    // The last sequence number, offset and size that the index keeps beside each last key.
    private const int IndexHandleBytes = sizeof(long) + sizeof(long) + sizeof(int);

    // The fewest bytes of an index entry (an empty last key), of a top-level entry, and of a range tombstone (empty keys).
    private const int IndexEntrySize = sizeof(int) + IndexHandleBytes;
    private const int TopLevelEntrySize = IndexEntrySize + sizeof(long) + sizeof(int);
    private const int TombstoneSize = sizeof(int) + sizeof(int) + sizeof(long);

    // The footer's handles of the filter and the index: in version 4, (0, 0) and the top-level index.
    private const int FilterHandle = 1;
    private const int IndexHandle = 3;

    /// <summary>The bytes that the first read ahead of a cursor takes, as RocksDB's initial auto readahead size.</summary>
    public const int InitialReadahead = 8 * 1024;

    /// <summary>The most bytes that a cursor reads ahead by itself, as RocksDB's <c>max_auto_readahead_size</c>.</summary>
    public const int MaxReadahead = 256 * 1024;

    /// <summary>
    /// The bytes that each read of a compaction's cursor takes, from its first read. RocksDB's
    /// <c>compaction_readahead_size</c> is 2 MB; a window of that size, one for each input, is larger than a
    /// processor's cache, and a merge of tables in the OS page cache took a quarter longer with it than with this one.
    /// </summary>
    public const int CompactionReadahead = 256 * 1024;

    private readonly IStorageFile _file;
    private readonly long _blocksEnd;   // where the footer starts: no block handle reaches past it
    private readonly ulong _id;         // the table id in the footer, bound into every block's checksum; 0 before version 5
    private readonly TablePartitions _partitions;
    private readonly byte[]? _lastKey;   // the last key of the last block, the table's last key with a version
    private readonly IndexAndFilter[]? _resident;   // null when a cache holds the partitions
    private readonly Cache? _cache;   // the partitions unless they are resident
    private readonly Cache? _blockCache;   // the data blocks, when they are cached
    private readonly long _cacheOwner;
    private readonly bool _highPriority;   // the partitions go into the cache at high priority
    private CacheEntry[]? _pinned;   // Interlocked: the pinned cache entries, one per partition, until Unpin or Dispose
    private readonly RangeTombstone[] _tombstones;
    private readonly FragmentedTombstones? _fragments;   // past MemTable.LinearTombstoneLimit tombstones
    private readonly bool _typedBlocks;   // format version 3 and later: a data block ends with a type byte before its CRC-32
    private readonly bool _lastEntryOffsets;   // format version 5: a data block's trailer holds the offset of its last entry
    private int _references;   // Interlocked: the versions that list the table and are not closed
    private int _disposed;     // Interlocked

    private TableReader(long number, IStorageFile file, long blocksEnd, TablePartitions partitions, IndexAndFilter[] blocks, TableCaching caching,
        RangeTombstone[] tombstones, TableProperties properties, uint version)
    {
        Number = number;
        _typedBlocks = version != TableFormat.Version2;
        _lastEntryOffsets = version == TableFormat.Version;
        _file = file;
        _blocksEnd = blocksEnd;
        _id = properties.Id;
        _partitions = partitions;
        _lastKey = partitions.Count == 0 ? null : partitions.Index.LastKey(partitions.Count - 1).ToArray();
        _tombstones = tombstones;
        _fragments = tombstones.Length > MemTable.LinearTombstoneLimit ? FragmentedTombstones.Build(tombstones) : null;
        Properties = properties;

        // What the reader keeps in memory for its life: the tombstones with their fragments, the top-level index,
        // and the partitions unless a cache holds them, as RocksDB counts it with cache_index_and_filter_blocks.
        long bytes = (_fragments?.MemoryBytes ?? 0) + partitions.Bytes;
        foreach (RangeTombstone tombstone in tombstones)
            bytes += tombstone.Begin.Length + tombstone.End.Length + sizeof(long);

        _cache = caching.Cache;
        _blockCache = caching.DataBlocks ? caching.Cache : null;
        _cacheOwner = caching.Owner;
        _highPriority = caching.HighPriority;
        if (_cache is null || !caching.IndexAndFilter)
        {
            _resident = blocks;
            foreach (IndexAndFilter partition in blocks)
                bytes += partition.Index.KeyAndHandleBytes + partition.Filter.ByteCount;
        }
        else
        {
            // Prefetched into the cache at open, as RocksDB's prefetch_index_and_filter_in_cache.
            CacheEntry[]? pinned = caching.Pin ? new CacheEntry[blocks.Length] : null;
            for (int partition = 0; partition < blocks.Length; partition++)
            {
                if (pinned is not null)
                    pinned[partition] = _cache.InsertPinned(PartitionKey(partition), blocks[partition], blocks[partition].Charge, _highPriority);
                else
                    _cache.Insert(PartitionKey(partition), blocks[partition], blocks[partition].Charge, _highPriority);
            }

            _pinned = pinned;
        }

        MemoryBytes = bytes;
    }

    /// <summary>The file number of the table.</summary>
    public long Number { get; }

    /// <summary>What the manifest records about the table, decoded from its properties block.</summary>
    public TableProperties Properties { get; }

    /// <summary>
    /// The bytes the reader holds in memory while it is open, as RocksDB's
    /// <c>rocksdb.estimate-table-readers-mem</c>: the range tombstones, the top-level index, and the index keys
    /// and their handles and the filter bits of every partition unless a cache holds them. Computed from their
    /// sizes, without object headers.
    /// </summary>
    public long MemoryBytes { get; }

    /// <summary>True while the reader holds its partitions pinned in its cache.</summary>
    public bool IsPinned => Volatile.Read(ref _pinned) is not null;

    /// <summary>The number of partitions of the index and filter; one for a table of format version 3 or 2.</summary>
    public int PartitionCount => _partitions.Count;

    /// <summary>The offsets of partition <paramref name="partition"/>'s index and filter blocks; for tests.</summary>
    internal (long IndexOffset, long FilterOffset) PartitionOffsets(int partition) =>
        (_partitions.Index.Offset(partition), _partitions.FilterOffset(partition));

    /// <summary>Open a table with no cache, which keeps its index and filter resident. The reader owns <paramref name="file"/>.</summary>
    public static TableReader Open(long number, IStorageFile file) => Open(number, file, TableCaching.Resident);

    /// <summary>
    /// Open a table, with its index and filter partitions and its data blocks where <paramref name="caching"/> says.
    /// The reader owns <paramref name="file"/>. Every block but the data blocks is read and verified here.
    /// </summary>
    public static TableReader Open(long number, IStorageFile file, TableCaching caching)
    {
        try
        {
            long length = file.GetLength();
            if (length < TableFormat.FooterSize4)
                throw Corrupt(number, "the file is shorter than its footer");

            // The magic and the version are at the same place from the end in every version, and the version
            // says how long the footer is: a version 5 footer has the table id before the handles.
            int footerRead = (int)Math.Min(length, TableFormat.FooterSize);
            byte[] footer = new byte[footerRead];
            ReadExactly(file, footer, length - footerRead, number);
            byte[] handles = footer[(footerRead - TableFormat.FooterSize4)..];
            if (ReadUInt64LittleEndian(handles.AsSpan(56)) != TableFormat.Magic)
                throw Corrupt(number, "the magic number does not match");

            uint version = ReadUInt32LittleEndian(handles.AsSpan(48));
            ulong id = 0;
            int footerSize = TableFormat.FooterSize4;
            ReadOnlySpan<byte> checksummed = handles.AsSpan(0, 52);
            if (version == TableFormat.Version)
            {
                if (footerRead < TableFormat.FooterSize)
                    throw Corrupt(number, "the file is shorter than its footer");

                id = ReadUInt64LittleEndian(footer);
                footerSize = TableFormat.FooterSize;
                checksummed = footer.AsSpan(0, TableFormat.FooterSize - 12);   // the id, the handles and the version
            }

            if (Crc32.HashToUInt32(checksummed) != ReadUInt32LittleEndian(handles.AsSpan(52)))
                throw Corrupt(number, "the footer has a bad checksum");

            if (version != TableFormat.Version && version != TableFormat.Version4 && version != TableFormat.Version3 && version != TableFormat.Version2)
                throw Corrupt(number, $"format version {version} is not supported");

            if (version == TableFormat.Version && id == 0)
                throw Corrupt(number, "the footer has table id 0");

            long blocksEnd = length - footerSize;
            RangeTombstone[] tombstones = DecodeTombstones(ReadFooterBlock(file, number, id, handles, blocksEnd, 0));
            TableProperties properties = DecodeProperties(ReadFooterBlock(file, number, id, handles, blocksEnd, 2), length, id);

            TablePartitions partitions;
            IndexAndFilter[] blocks;
            if (version is TableFormat.Version or TableFormat.Version4)
            {
                partitions = DecodeTopLevel(ReadFooterBlock(file, number, id, handles, blocksEnd, IndexHandle), number, blocksEnd);
                blocks = new IndexAndFilter[partitions.Count];
                for (int partition = 0; partition < blocks.Length; partition++)
                {
                    blocks[partition] = ReadPartition(file, number, id, partitions, partition, blocksEnd);
                    CheckPartition(number, partitions, partition, blocks[partition].Index);
                }
            }
            else
            {
                // One index and one filter block: one partition, whose last key is the index's.
                (long filterOffset, int filterSize) = FooterHandle(handles, FilterHandle);
                (long indexOffset, int indexSize) = FooterHandle(handles, IndexHandle);
                IndexAndFilter whole = new(
                    DecodeIndex(ReadBlockAt(file, number, id, indexOffset, indexSize, blocksEnd), number, blocksEnd),
                    BloomFilter.Decode(ReadBlockAt(file, number, id, filterOffset, filterSize, blocksEnd)));
                int count = whole.Index.Count;
                partitions = count == 0
                    ? new TablePartitions(new TableIndex([], [0], [], [], []), [], [])
                    : new TablePartitions(
                        new TableIndex(whole.Index.LastKey(count - 1).ToArray(), [0, whole.Index.LastKey(count - 1).Length],
                            [whole.Index.LastSequence(count - 1)], [indexOffset], [indexSize]),
                        [filterOffset], [filterSize]);
                blocks = count == 0 ? [] : [whole];
            }

            // The top-level index holds the last key of the last block, the table's last entry.
            int partitionCount = partitions.Count;
            properties = properties with { LargestIsExclusive = TableProperties.IsExclusive(partitionCount == 0 ? null : partitions.Index.LastKey(partitionCount - 1).ToArray(), properties.Largest) };
            return new TableReader(number, file, blocksEnd, partitions, blocks, caching, tombstones, properties, version);
        }
        catch (Exception e) when (e is ArgumentOutOfRangeException or IndexOutOfRangeException)
        {
            throw Corrupt(number, "a block does not decode");
        }
    }

    /// <inheritdoc />
    public bool TryGetNewest(ReadOnlySpan<byte> key, long snapshot, bool fillCache, out EntryVersion entry) =>
        TryGetNewest(key, FilterHash(key), snapshot, fillCache, out entry);

    /// <summary>
    /// <see cref="TryGetNewest(ReadOnlySpan{byte}, long, bool, out EntryVersion)"/> with the key's
    /// <see cref="FilterHash"/> as <paramref name="hash"/>, for a lookup that computed it once for all its sources.
    /// </summary>
    public bool TryGetNewest(ReadOnlySpan<byte> key, ulong hash, long snapshot, bool fillCache, out EntryVersion entry)
    {
        entry = default;
        if (!TryLocate(key, hash, snapshot, fillCache, out DataBlockHandle handle))
            return false;

        // Only the value is copied out of the block.
        using BlockBuffer block = DataBlock(handle, fillCache);
        return TryGetFromEntries(block.Entries, handle, key, snapshot, out entry);
    }

    /// <summary>
    /// The data block that holds the first entry at or after (<paramref name="key"/>, <paramref name="snapshot"/>)
    /// when the partition's Bloom filter may have the key, without reading it; false when the table has no
    /// version of the key at or below the snapshot. See <see cref="IEntrySource.TryGetNewest"/> for
    /// <paramref name="fillCache"/>.
    /// </summary>
    public bool TryLocate(ReadOnlySpan<byte> key, long snapshot, bool fillCache, out DataBlockHandle handle) =>
        TryLocate(key, FilterHash(key), snapshot, fillCache, out handle);

    /// <summary>
    /// The hash of <paramref name="key"/> that every table's Bloom filter is built from and probed with. A lookup
    /// computes it once and gives it to each table it searches.
    /// </summary>
    public static ulong FilterHash(ReadOnlySpan<byte> key) => XxHash3.HashToUInt64(key);

    /// <summary>
    /// <see cref="TryLocate(ReadOnlySpan{byte}, long, bool, out DataBlockHandle)"/> with the key's
    /// <see cref="FilterHash"/> as <paramref name="hash"/>.
    /// </summary>
    public bool TryLocate(ReadOnlySpan<byte> key, ulong hash, long snapshot, bool fillCache, out DataBlockHandle handle)
    {
        handle = default;
        int partition = _partitions.Index.FindBlock(key, snapshot);
        if (partition == _partitions.Count)
            return false;

        // The partition's last entry is at or after (key, snapshot), so the first entry at or after it is in
        // the partition, and the partition's filter has the key if that entry is a version of it.
        IndexAndFilter blocks = Partition(partition, fillCache);
        if (!blocks.Filter.MayContain(hash))
            return false;

        // The block's last entry is at or after (key, snapshot), so the first entry at or after it is in this block.
        TableIndex index = blocks.Index;
        int block = index.FindBlock(key, snapshot);
        if (block == index.Count)
            return false;

        handle = new DataBlockHandle(index, partition, block);
        return true;
    }

    /// <summary>The table's file, for a batch read of its blocks through <see cref="IStorageEnvironment.MultiRead"/>.</summary>
    public IStorageFile File => _file;

    /// <summary>
    /// The entries of the data block <paramref name="handle"/> names, verified and decompressed, when the table's
    /// cache holds them; the block becomes the most recently used. False when the table does not cache data blocks,
    /// or on a miss. The block is the cache's and never changes; it comes with a reference that the caller releases
    /// (<see cref="NativeBlock.Release"/>) in a <c>finally</c>, and its bytes stay valid until then, after an
    /// eviction too.
    /// </summary>
    public bool TryGetCachedBlock(DataBlockHandle handle, [NotNullWhen(true)] out NativeBlock? entries)
    {
        if (_blockCache is not null && _blockCache.TryGet(BlockKey(handle.Offset), out object? cached))
        {
            entries = (NativeBlock)cached;
            return true;
        }

        entries = null;
        return false;
    }

    /// <summary>
    /// Caches a native copy of the data block's <paramref name="entries"/>, which <see cref="DecodeReadBlock"/>
    /// verified and decompressed, as a read that fills the cache does. Nothing when the table does not cache data
    /// blocks, or when a reader cached the block first.
    /// </summary>
    public void CacheBlock(DataBlockHandle handle, ReadOnlySpan<byte> entries) => CacheBlockAndKeep(handle, entries)?.Release();

    // The block cached for entries, the copy or the one a reader cached first, with a reference that the caller
    // releases; null, and nothing copied, when the table does not cache data blocks.
    private NativeBlock? CacheBlockAndKeep(DataBlockHandle handle, ReadOnlySpan<byte> entries)
    {
        if (_blockCache is null)
            return null;

        CacheKey key = BlockKey(handle.Offset);
        NativeBlock copy = _blockCache.NewBlock(key, entries);
        try
        {
            return (NativeBlock)_blockCache.Insert(key, copy, copy.Length);
        }
        finally
        {
            // The cache took its own reference on the copy, or kept the block it had.
            copy.Release();
        }
    }

    /// <summary>
    /// The entries of the data block <paramref name="handle"/> names, from its bytes as the file has them: the first
    /// <paramref name="bytesRead"/> of <paramref name="raw"/>, which must be the whole block. Verifies it, decompresses
    /// it, and checks its keys against its index entry. Returns <paramref name="raw"/> when the block is not compressed, else a buffer rented from
    /// <see cref="ArrayPool{T}.Shared"/> that the caller returns; the entries are its first
    /// <paramref name="length"/> bytes. The caller keeps <paramref name="raw"/> either way.
    /// </summary>
    /// <exception cref="SolariumException">
    /// A corruption: the file ended before the block did, or the block fails its checksum, does not decompress or
    /// decode, or holds keys of another place than its index entry's.
    /// </exception>
    public byte[] DecodeReadBlock(byte[] raw, int bytesRead, DataBlockHandle handle, out int length)
    {
        if (bytesRead < handle.Size)
            throw Corrupt(Number, $"the file ends before offset {handle.Offset + handle.Size}");

        return DecodeBlock(raw, handle, out length);
    }

    /// <summary>
    /// The newest version of <paramref name="key"/> at or below <paramref name="snapshot"/> in a data block's
    /// <paramref name="entries"/> (<see cref="DecodeReadBlock"/>). Its value is a new array.
    /// </summary>
    /// <exception cref="SolariumException">The entries do not decode.</exception>
    public bool TryGetFromEntries(ReadOnlySpan<byte> entries, DataBlockHandle handle, ReadOnlySpan<byte> key, long snapshot, out EntryVersion entry)
    {
        entry = default;
        if (!FindVersion(entries, key, snapshot, handle, out long sequence, out OperationKind kind, out ReadOnlySpan<byte> value))
            return false;

        entry = new EntryVersion(sequence, kind, kind == OperationKind.Put ? value.ToArray() : null);
        return true;
    }

    /// <inheritdoc />
    public bool HasTombstones => _tombstones.Length > 0;

    /// <inheritdoc />
    public long CoveringTombstone(ReadOnlySpan<byte> key, long snapshot)
    {
        if (_fragments is not null)
            return _fragments.Covering(key, snapshot);

        long highest = 0;
        foreach (RangeTombstone tombstone in _tombstones)
            if (tombstone.Sequence > highest && tombstone.Covers(key, snapshot))
                highest = tombstone.Sequence;

        return highest;
    }

    /// <summary>The range tombstones stored in the table.</summary>
    public IReadOnlyList<RangeTombstone> Tombstones => _tombstones;

    /// <inheritdoc />
    public byte[]? LastKeyBefore(byte[]? limit, bool fillCache)
    {
        if (_partitions.Count == 0)
            return null;

        if (limit is null)
            return _lastKey;

        // The first partition whose last key is at or after the limit, and in it the first such block, hold
        // the answer; or the block before that one does, in this partition or at the end of the one before.
        int partition = _partitions.Index.FindBlockAtOrAfter(limit);
        if (partition == _partitions.Count)
            return _lastKey;

        TableIndex index = Partition(partition, fillCache).Index;
        int low = index.FindBlockAtOrAfter(limit);
        BlockBuffer block = DataBlock(new DataBlockHandle(index, partition, low), fillCache);
        try
        {
            SpanReader reader = new(block.Entries);
            ReadOnlySpan<byte> last = default;
            bool found = false;
            while (!reader.AtEnd)
            {
                ReadOnlySpan<byte> entryKey = reader.Span();
                reader.Int64();
                reader.Byte();
                reader.Span();

                if (entryKey.SequenceCompareTo(limit) >= 0)
                    break;

                last = entryKey;
                found = true;
            }

            if (found)
                return last.ToArray();

            if (low > 0)
                return index.LastKey(low - 1).ToArray();

            return partition > 0 ? _partitions.Index.LastKey(partition - 1).ToArray() : null;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw DataBlockCorrupt(low, index.Offset(low), e);
        }
        finally
        {
            block.Dispose();
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// The cursor reads ahead once its reads of the file follow one another, as RocksDB's iterators do: from the
    /// third data block in a row, a read takes <see cref="InitialReadahead"/> bytes, and each read after it twice
    /// as many, up to <see cref="MaxReadahead"/>. A seek to another place starts again with single blocks.
    /// </remarks>
    public IEntryCursor NewCursor(bool fillCache) => new Cursor(this, fillCache, readahead: 0);

    /// <summary>
    /// A cursor as <see cref="NewCursor(bool)"/> gives, whose every read of the file takes
    /// <paramref name="readahead"/> bytes, from the first one: for a reader that takes every entry, as a
    /// compaction does with <see cref="CompactionReadahead"/>. 0 reads ahead as <see cref="NewCursor(bool)"/> does.
    /// </summary>
    public IEntryCursor NewCursor(bool fillCache, int readahead) => new Cursor(this, fillCache, readahead);

    /// <summary>
    /// Writes the table file's bytes, all <see cref="TableProperties.Size"/> of them, to the start of
    /// <paramref name="target"/>, and flushes it. The caller keeps the table open meanwhile, by a
    /// version it is counted in. Throws <see cref="SolariumException"/> when the file is shorter, and
    /// <see cref="OperationCanceledException"/> between two chunks when <paramref name="stop"/> is cancelled.
    /// </summary>
    public void CopyTo(IStorageFile target, CancellationToken stop)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(1 << 20);
        try
        {
            long size = Properties.Size;
            long offset = 0;
            while (offset < size)
            {
                stop.ThrowIfCancellationRequested();
                int read = _file.Read(buffer.AsSpan(0, (int)Math.Min(buffer.Length, size - offset)), offset);
                if (read == 0)
                    throw SolariumException.Corruption($"SSTable {TableFormat.FileName(Number)} ended at {offset} bytes of {size} while it was copied.");

                target.Write(buffer.AsSpan(0, read), offset);
                offset += read;
            }

            target.Flush();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Count one more version that lists the table; see <see cref="FamilyVersion"/>.</summary>
    public void AddReference() => Interlocked.Increment(ref _references);

    /// <summary>Release a closed version's reference. Returns true when it was the last one, so no reader can reach the table any more.</summary>
    public bool Release() => Interlocked.Decrement(ref _references) == 0;

    /// <summary>
    /// Unpins the index and filter partitions, as a table that leaves level 0 does: the cache may evict them from
    /// now on, and a lookup after that reads them again. Does nothing when they are not pinned.
    /// </summary>
    public void Unpin()
    {
        CacheEntry[]? pinned = Interlocked.Exchange(ref _pinned, null);
        if (pinned is not null)
            foreach (CacheEntry entry in pinned)
                _cache!.Unpin(entry);
    }

    /// <summary>
    /// Closes the table file and erases the table's partitions from its cache: the number of a closed table
    /// is never read again. Its data blocks stay until the cache evicts them, as RocksDB's do: finding them would
    /// take the whole index, and nothing looks them up again. Only the first call closes it.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        Unpin();
        if (_resident is null)
            for (int partition = 0; partition < _partitions.Count; partition++)
                _cache!.Erase(PartitionKey(partition));

        _file.Dispose();
    }

    // A partition's index and filter, from the reader, from its pinned entry, or from the cache; on a miss they
    // are read from the file and verified, and cached unless fillCache is false. An evicted copy that a caller
    // still holds stays valid: the blocks of a table never change.
    private IndexAndFilter Partition(int partition, bool fillCache)
    {
        if (_resident is not null)
            return _resident[partition];

        CacheEntry[]? pinned = Volatile.Read(ref _pinned);
        if (pinned is not null)
            return (IndexAndFilter)pinned[partition].Value;

        CacheKey key = PartitionKey(partition);
        if (_cache!.TryGet(key, out object? cached))
            return (IndexAndFilter)cached;

        IndexAndFilter blocks;
        try
        {
            blocks = ReadPartition(_file, Number, _id, _partitions, partition, _blocksEnd);
        }
        catch (Exception e) when (e is ArgumentOutOfRangeException or IndexOutOfRangeException)
        {
            throw Corrupt(Number, $"index partition {partition} read again from the file does not decode");
        }

        return fillCache ? (IndexAndFilter)_cache.Insert(key, blocks, blocks.Charge, _highPriority) : blocks;
    }

    private CacheKey PartitionKey(int partition) => new(_cacheOwner, Number, -1L - partition);

    private CacheKey BlockKey(long offset) => new(_cacheOwner, Number, offset);

    // A data block's entries: the cache's block, or read from the file and verified, and cached when fillCache is
    // set and the table caches data blocks. The caller disposes the buffer, which releases the cached block or
    // returns the pooled array.
    private BlockBuffer DataBlock(DataBlockHandle handle, bool fillCache)
    {
        if (TryGetCachedBlock(handle, out NativeBlock? cached))
            return new BlockBuffer(cached);

        byte[] buffer = RentDataBlock(handle, out int length);
        NativeBlock? inserted;
        try
        {
            inserted = fillCache ? CacheBlockAndKeep(handle, buffer.AsSpan(0, length)) : null;
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }

        if (inserted is null)
            return new BlockBuffer(buffer, length);

        ArrayPool<byte>.Shared.Return(buffer);
        return new BlockBuffer(inserted);
    }

    // A data block's entries in a pooled buffer, checksum verified and decompressed; they are the
    // first length bytes. The caller returns the buffer to ArrayPool<byte>.Shared.
    private byte[] RentDataBlock(DataBlockHandle handle, out int length)
    {
        byte[] raw = ArrayPool<byte>.Shared.Rent(handle.Size);
        byte[]? entries = null;
        try
        {
            ReadExactly(_file, raw.AsSpan(0, handle.Size), handle.Offset, Number);
            entries = DecodeBlock(raw, handle, out length);
            return entries;
        }
        finally
        {
            // The buffer the caller gets back is the one it returns.
            if (!ReferenceEquals(entries, raw))
                ArrayPool<byte>.Shared.Return(raw);
        }
    }

    // The entries of the data block whose bytes, checksum included, are the first handle.Size of raw, checked
    // against the block's index entry: raw itself when the block is not compressed, else a buffer rented from
    // ArrayPool<byte>.Shared that the caller returns. The caller keeps raw either way. Every block that a read takes
    // from the file comes through here before it reaches a cache, so a cached block is a checked one.
    private byte[] DecodeBlock(byte[] raw, DataBlockHandle handle, out int length)
    {
        byte[] entries = DecompressBlock(raw, handle, out length, out int lastEntry);
        try
        {
            CheckBounds(entries.AsSpan(0, length), handle, lastEntry);
            return entries;
        }
        catch
        {
            if (!ReferenceEquals(entries, raw))
                ArrayPool<byte>.Shared.Return(entries);

            throw;
        }
    }

    // The entries of the data block whose bytes, checksum included, are raw, checked as the DecodeBlock of an array
    // checks them: a part of raw when the block is not compressed, and rented is null; else the first bytes of
    // rented, a buffer from ArrayPool<byte>.Shared that the caller returns. A cursor decodes a block so where it
    // lies in the cursor's readahead window.
    private ReadOnlySpan<byte> DecodeBlock(ReadOnlySpan<byte> raw, DataBlockHandle handle, out byte[]? rented)
    {
        ReadOnlySpan<byte> entries = DecompressBlock(raw, handle, out rented, out int lastEntry);
        try
        {
            CheckBounds(entries, handle, lastEntry);
            return entries;
        }
        catch
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);

            rented = null;
            throw;
        }
    }

    // A data block's entries must lie where its index entry places it: its last entry is the last internal key the
    // index holds for it, and its first comes after the last key of the block before it, in this partition or at the
    // end of the one before. A whole, valid block that a misdirected write put at another block's place passes the
    // checksum of a table before version 5, and would answer a lookup for its own keys with "not found", or a scan
    // with another range's rows: as damage it fails here instead. A version 5 block names its last entry at
    // lastEntry, so the check decodes two entries, and the last must end where the block does; an older block
    // (lastEntry -1) is walked to its end.
    private void CheckBounds(ReadOnlySpan<byte> entries, DataBlockHandle handle, int lastEntry)
    {
        int block = handle.Block;
        long offset = handle.Offset;
        TableIndex index = handle.Index;
        ReadOnlySpan<byte> firstKey;
        long firstSequence;
        ReadOnlySpan<byte> lastKey;
        long lastSequence;
        try
        {
            SpanReader reader = new(entries);
            if (reader.AtEnd)
                throw Corrupt(Number, $"data block {block} at offset {offset} has no entries");

            firstKey = reader.Span();
            firstSequence = reader.Int64();
            reader.EntryKind();
            reader.Span();
            lastKey = firstKey;
            lastSequence = firstSequence;
            if (lastEntry > 0)
            {
                if (lastEntry < reader.Position || lastEntry >= entries.Length)
                    throw Corrupt(Number, $"data block {block} at offset {offset} names its last entry at {lastEntry}, not between the end of its first entry at {reader.Position} and the end of its {entries.Length} bytes of entries");

                reader = new SpanReader(entries[lastEntry..]);
                lastKey = reader.Span();
                lastSequence = reader.Int64();
                reader.EntryKind();
                reader.Span();
                if (!reader.AtEnd)
                    throw Corrupt(Number, $"data block {block} at offset {offset} names its last entry at {lastEntry}, but an entry there does not end the block");
            }
            else if (lastEntry == 0)
            {
                if (!reader.AtEnd)
                    throw Corrupt(Number, $"data block {block} at offset {offset} names its first entry as its last, but the entry does not end the block");
            }
            else
            {
                while (!reader.AtEnd)
                {
                    lastKey = reader.Span();
                    lastSequence = reader.Int64();
                    reader.EntryKind();
                    reader.Span();
                }
            }
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw DataBlockCorrupt(block, offset, e);
        }

        if (!lastKey.SequenceEqual(index.LastKey(block)) || lastSequence != index.LastSequence(block))
            throw Corrupt(Number, $"data block {block} at offset {offset} of index partition {handle.Partition} ends at another key than its index entry holds; it holds another place's data");

        bool hasPrevious = block > 0 || handle.Partition > 0;
        ReadOnlySpan<byte> previousKey = block > 0 ? index.LastKey(block - 1) : hasPrevious ? _partitions.Index.LastKey(handle.Partition - 1) : default;
        long previousSequence = block > 0 ? index.LastSequence(block - 1) : hasPrevious ? _partitions.Index.LastSequence(handle.Partition - 1) : 0;
        if (hasPrevious && InternalKey.Compare(firstKey, firstSequence, previousKey, previousSequence) <= 0)
            throw Corrupt(Number, $"data block {block} at offset {offset} of index partition {handle.Partition} starts at or before the last key of the block before it; it holds another place's data");
    }

    // The entries of the data block whose bytes, checksum included, are the first handle.Size of raw, verified and
    // decompressed, as DecodeBlock returns them.
    private byte[] DecompressBlock(byte[] raw, DataBlockHandle handle, out int length, out int lastEntry)
    {
        ReadOnlySpan<byte> entries = DecompressBlock(raw.AsSpan(0, handle.Size), handle, out byte[]? rented, out lastEntry);
        length = entries.Length;

        // The entries of a block that is not compressed start where the block does.
        return rented ?? raw;
    }

    // The entries of the data block whose bytes, checksum included, are raw, handle.Size of them, verified and
    // decompressed: the first bytes of raw when the block is not compressed, and rented is null; else the first
    // bytes of rented, a buffer from ArrayPool<byte>.Shared that the caller returns.
    private ReadOnlySpan<byte> DecompressBlock(ReadOnlySpan<byte> raw, DataBlockHandle handle, out byte[]? rented, out int lastEntry)
    {
        rented = null;
        int size = handle.Size;
        long offset = handle.Offset;
        int block = handle.Block;
        if (TableFormat.BlockChecksum(raw[..(size - 4)], _id, offset) != ReadUInt32LittleEndian(raw[(size - 4)..]))
            throw Corrupt(Number, $"the block at offset {offset} has a bad checksum");

        lastEntry = -1;
        if (!_typedBlocks)
            return raw[..(size - 4)];

        int trailer = _lastEntryOffsets ? TableFormat.DataTrailerSize : TableFormat.DataTrailerSize4;
        if (size < trailer)
            throw Corrupt(Number, $"data block {block} at offset {offset} is {size} bytes, shorter than its {trailer} byte trailer");

        // The type is the byte before the CRC-32 in every version that has one.
        int payload = size - trailer;
        byte type = raw[size - 5];
        if (_lastEntryOffsets)
        {
            lastEntry = ReadInt32LittleEndian(raw[payload..]);
            if (lastEntry < 0)
                throw Corrupt(Number, $"data block {block} at offset {offset} names its last entry at {lastEntry}");
        }

        if (type == TableFormat.RawBlock)
            return raw[..payload];

        if (type != TableFormat.Lz4Block)
            throw Corrupt(Number, $"data block {block} at offset {offset} has type {type}");

        // The checksum passed, so the size was written as it is; the bound keeps a writer's bug
        // from becoming a huge allocation.
        int rawSize = payload < sizeof(int) ? -1 : ReadInt32LittleEndian(raw);
        if (rawSize < 0 || rawSize > ((long)(payload - sizeof(int)) * TableFormat.Lz4MaxRatio) + 16)
            throw Corrupt(Number, $"data block {block} at offset {offset} has a raw size of {rawSize} for {payload} compressed bytes");

        byte[] entries = ArrayPool<byte>.Shared.Rent(rawSize);
        if (LZ4Codec.Decode(raw.Slice(sizeof(int), payload - sizeof(int)), entries.AsSpan(0, rawSize)) != rawSize)
        {
            ArrayPool<byte>.Shared.Return(entries);
            throw Corrupt(Number, $"data block {block} at offset {offset} does not decompress to its {rawSize} bytes");
        }

        rented = entries;
        return entries.AsSpan(0, rawSize);
    }

    // The newest version of key at or below snapshot in a block's decoded entries; value is a slice of entries.
    private bool FindVersion(ReadOnlySpan<byte> entries, ReadOnlySpan<byte> key, long snapshot, DataBlockHandle handle,
        out long sequence, out OperationKind kind, out ReadOnlySpan<byte> value)
    {
        try
        {
            // The first entry at or after (key, snapshot) decides: it is the newest version at or below the
            // snapshot when it is a version of the key.
            SpanReader reader = new(entries);
            while (!reader.AtEnd)
            {
                ReadOnlySpan<byte> entryKey = reader.Span();
                sequence = reader.Int64();
                kind = reader.EntryKind();
                value = reader.Span();

                if (InternalKey.Compare(entryKey, sequence, key, snapshot) >= 0)
                    return entryKey.SequenceEqual(key);
            }

            sequence = 0;
            kind = default;
            value = default;
            return false;
        }
        catch (ArgumentOutOfRangeException e)
        {
            throw DataBlockCorrupt(handle.Block, handle.Offset, e);
        }
    }

    // The offset and size of the block that footer handle `handle` names, from the footer's 48 bytes of handles.
    private static (long Offset, int Size) FooterHandle(byte[] handles, int handle) =>
        (ReadInt64LittleEndian(handles.AsSpan(handle * 12)), ReadInt32LittleEndian(handles.AsSpan((handle * 12) + 8)));

    // The payload of the block at footer handle `handle`, read and verified.
    private static byte[] ReadFooterBlock(IStorageFile file, long number, ulong id, byte[] handles, long blocksEnd, int handle)
    {
        (long offset, int size) = FooterHandle(handles, handle);
        return ReadBlockAt(file, number, id, offset, size, blocksEnd);
    }

    // The payload of the block at (offset, size) of table `id`, read and verified.
    private static byte[] ReadBlockAt(IStorageFile file, long number, ulong id, long offset, int size, long blocksEnd)
    {
        CheckHandle(number, offset, size, blocksEnd);
        byte[] block = new byte[size];
        ReadVerifiedBlock(file, number, id, offset, block);
        return block[..^4];
    }

    // A partition's filter and index blocks, read, verified and decoded.
    private static IndexAndFilter ReadPartition(IStorageFile file, long number, ulong id, TablePartitions partitions, int partition, long blocksEnd) =>
        new(DecodeIndex(ReadBlockAt(file, number, id, partitions.Index.Offset(partition), partitions.Index.Size(partition), blocksEnd), number, blocksEnd),
            BloomFilter.Decode(ReadBlockAt(file, number, id, partitions.FilterOffset(partition), partitions.FilterSize(partition), blocksEnd)));

    // An index block, decoded into the compact index: the last keys laid end to end in one array, measured in a
    // first pass over the block. No handle may reach past blocksEnd, where the footer starts.
    private static TableIndex DecodeIndex(byte[] payload, long number, long blocksEnd)
    {
        SpanReader reader = new(payload);
        int count = reader.Count(IndexEntrySize);
        int keyBytes = 0;
        for (int i = 0; i < count; i++)
        {
            keyBytes += reader.Span().Length;
            reader.Int64();
            reader.Int64();
            reader.Int32();
        }

        reader.ExpectEnd();

        byte[] keys = new byte[keyBytes];
        int[] keyStarts = new int[count + 1];
        long[] lastSequences = new long[count];
        long[] offsets = new long[count];
        int[] sizes = new int[count];
        reader = new SpanReader(payload);
        reader.Int32();
        int position = 0;
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> key = reader.Span();
            keyStarts[i] = position;
            key.CopyTo(keys.AsSpan(position));
            position += key.Length;
            lastSequences[i] = reader.Int64();
            offsets[i] = reader.Int64();
            sizes[i] = reader.Int32();
            CheckHandle(number, offsets[i], sizes[i], blocksEnd);
        }

        keyStarts[count] = position;
        return new TableIndex(keys, keyStarts, lastSequences, offsets, sizes);
    }

    // The top-level index of a version 5 or 4 table: each partition's last key and the handles of its index and filter.
    private static TablePartitions DecodeTopLevel(byte[] payload, long number, long blocksEnd)
    {
        SpanReader reader = new(payload);
        int count = reader.Count(TopLevelEntrySize);
        int keyBytes = 0;
        for (int i = 0; i < count; i++)
        {
            keyBytes += reader.Span().Length;
            reader.Int64();
            reader.Int64();
            reader.Int32();
            reader.Int64();
            reader.Int32();
        }

        reader.ExpectEnd();

        byte[] keys = new byte[keyBytes];
        int[] keyStarts = new int[count + 1];
        long[] lastSequences = new long[count];
        long[] indexOffsets = new long[count];
        int[] indexSizes = new int[count];
        long[] filterOffsets = new long[count];
        int[] filterSizes = new int[count];
        reader = new SpanReader(payload);
        reader.Int32();
        int position = 0;
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> key = reader.Span();
            keyStarts[i] = position;
            key.CopyTo(keys.AsSpan(position));
            position += key.Length;
            lastSequences[i] = reader.Int64();
            indexOffsets[i] = reader.Int64();
            indexSizes[i] = reader.Int32();
            filterOffsets[i] = reader.Int64();
            filterSizes[i] = reader.Int32();
            CheckHandle(number, indexOffsets[i], indexSizes[i], blocksEnd);
            CheckHandle(number, filterOffsets[i], filterSizes[i], blocksEnd);
        }

        keyStarts[count] = position;
        return new TablePartitions(new TableIndex(keys, keyStarts, lastSequences, indexOffsets, indexSizes), filterOffsets, filterSizes);
    }

    // A partition must have blocks, and its last block must end at the key the top-level index holds for it:
    // otherwise a lookup routed to it by that key would miss the entries it holds.
    private static void CheckPartition(long number, TablePartitions partitions, int partition, TableIndex index)
    {
        int last = index.Count - 1;
        if (last < 0 || !index.LastKey(last).SequenceEqual(partitions.Index.LastKey(partition)) || index.LastSequence(last) != partitions.Index.LastSequence(partition))
            throw Corrupt(number, $"index partition {partition} at offset {partitions.Index.Offset(partition)} does not end at the key the top-level index holds for it");
    }

    // Read a whole block (payload and checksum) of table `id` into block, and verify the checksum, which binds the
    // block to the table and the offset from version 5.
    private static void ReadVerifiedBlock(IStorageFile file, long number, ulong id, long offset, Span<byte> block)
    {
        ReadExactly(file, block, offset, number);
        if (TableFormat.BlockChecksum(block[..^4], id, offset) != ReadUInt32LittleEndian(block[^4..]))
            throw Corrupt(number, $"the block at offset {offset} has a bad checksum");
    }

    private static void ReadExactly(IStorageFile file, Span<byte> buffer, long offset, long number)
    {
        while (!buffer.IsEmpty)
        {
            int read = file.Read(buffer, offset);
            if (read == 0)
                throw Corrupt(number, $"the file ends before offset {offset + buffer.Length}");

            buffer = buffer[read..];
            offset += read;
        }
    }

    // A handle must point inside the file, so a corrupt size cannot cause a huge allocation. The end is compared as
    // limit - size, which cannot overflow (limit is not negative and size is at least 4): offset + size can, and an
    // offset near the largest long would then pass with a negative end.
    private static void CheckHandle(long number, long offset, int size, long limit)
    {
        if (offset < 0 || size < 4 || offset > limit - size)
            throw Corrupt(number, $"a block handle ({offset}, {size}) points outside the file");
    }

    private static RangeTombstone[] DecodeTombstones(byte[] payload)
    {
        SpanReader reader = new(payload);
        RangeTombstone[] tombstones = new RangeTombstone[reader.Count(TombstoneSize)];
        for (int i = 0; i < tombstones.Length; i++)
            tombstones[i] = new RangeTombstone(reader.Bytes(), reader.Bytes(), reader.Int64());

        reader.ExpectEnd();
        return tombstones;
    }

    private static TableProperties DecodeProperties(byte[] payload, long size, ulong id)
    {
        SpanReader reader = new(payload);
        TableProperties properties = new(size, reader.Int64(), reader.Int64(), reader.Int64(), reader.Bytes(), reader.Bytes(), LargestIsExclusive: false, id);
        reader.ExpectEnd();
        return properties;
    }

    private static SolariumException Corrupt(long number, string detail)
    {
        return SolariumException.Corruption($"SSTable {TableFormat.FileName(number)} is corrupt: {detail}.");
    }

    private SolariumException DataBlockCorrupt(int block, long offset, ArgumentOutOfRangeException cause)
    {
        return SolariumException.Corruption($"SSTable {TableFormat.FileName(Number)} is corrupt: data block {block} at offset {offset} has a valid checksum but does not decode: {cause.Message}", cause);
    }

    // Reads the entries of one data block at a time in place, with no copy of a key or a value: the block is the
    // cache's, which the cursor holds a reference on, or bytes in a buffer of the cursor's own. It keeps the index of
    // the partition it reads until it moves to another partition, as RocksDB's iterators keep the index block they
    // read. One thread at a time.
    //
    // A block that the cache does not hold comes from the file: alone, into a pooled buffer, or out of the window,
    // a pooled buffer that one read fills with the block and the bytes after it, so the blocks that follow cost no
    // read. The entries of a block that is not compressed are read where they lie in the window, which is filled
    // again only when the cursor moves to a block outside it.
    private sealed class Cursor(TableReader table, bool fillCache, int readahead) : IEntryCursor
    {
        private TableIndex? _partitionIndex;
        private int _loadedPartition = -1;
        private int _partition;
        private int _block;

        // The block's entries: the cached block, or _length bytes of _bytes from _start, where _bytes is _pooled
        // (a block read alone, or a decompressed one) or _window.
        private NativeBlock? _cached;
        private byte[]? _pooled;
        private byte[]? _bytes;
        private int _start;
        private int _length;

        // Where each entry of the block starts among its entries, in an array that the cursor reuses.
        private int[] _offsets = new int[64];
        private int _count;
        private int _index;

        // The entry at _index, decoded once per position.
        private int _keyStart;
        private int _keyLength;
        private int _valueStart;
        private int _valueLength;
        private long _sequence;
        private OperationKind _kind;
        private Entry? _current;

        // The window holds the _windowLength bytes of the file from _windowOffset. _nextOffset is where the last
        // block the cursor loaded ends, and _sequentialReads the reads of the file in a row that started there.
        private byte[]? _window;
        private long _windowOffset;
        private int _windowLength;
        private long _nextOffset = -1;
        private int _sequentialReads;

        public bool Valid => _index < _count;

        public Entry Current => _current ??= new Entry(CurrentKey.ToArray(), _sequence, _kind, _kind == OperationKind.Put ? CurrentValue.ToArray() : null);

        public ReadOnlySpan<byte> CurrentKey => Entries.Slice(_keyStart, _keyLength);

        public long CurrentSequence => _sequence;

        public OperationKind CurrentKind => _kind;

        public ReadOnlySpan<byte> CurrentValue => Entries.Slice(_valueStart, _valueLength);

        private ReadOnlySpan<byte> Entries => _cached is not null ? _cached.Span : _bytes.AsSpan(_start, _length);

        public void SeekToFirst() => Load(0, 0);

        public void Seek(ReadOnlySpan<byte> key)
        {
            TablePartitions partitions = table._partitions;
            int partition = partitions.Index.FindBlock(key, long.MaxValue);
            if (partition == partitions.Count)
            {
                Load(partition, 0);
                return;
            }

            Load(partition, PartitionIndex(partition).FindBlock(key, long.MaxValue));

            // The first entry at or after the key's first version, by a binary search over the entries' offsets.
            ReadOnlySpan<byte> entries = Entries;
            int low = 0, high = _count;
            while (low < high)
            {
                int middle = (low + high) / 2;
                int at = _offsets[middle];
                int keyLength = ReadInt32LittleEndian(entries[at..]);
                if (InternalKey.Compare(entries.Slice(at + 4, keyLength), ReadInt64LittleEndian(entries[(at + 4 + keyLength)..]), key, long.MaxValue) < 0)
                    low = middle + 1;
                else
                    high = middle;
            }

            if (low == 0)
                return;

            // The index sent the cursor to a block whose last entry is at or after the key, so the entry is here.
            _index = low;
            if (low < _count)
                Position(low);
            else
                Load(_partition, _block + 1);
        }

        public void Next()
        {
            if (++_index < _count)
                Position(_index);
            else
                Load(_partition, _block + 1);
        }

        public void Dispose()
        {
            ReleaseBlock();
            ReleaseWindow();
        }

        private TableIndex PartitionIndex(int partition)
        {
            if (_loadedPartition != partition)
            {
                _partitionIndex = table.Partition(partition, fillCache).Index;
                _loadedPartition = partition;
            }

            return _partitionIndex!;
        }

        // The first entry of the block at (partition, block), or of the first block of the partitions after it once
        // a partition ends. Past the last block the cursor is not valid, and holds no buffer.
        private void Load(int partition, int block)
        {
            ReleaseBlock();
            TablePartitions partitions = table._partitions;
            while (partition < partitions.Count)
            {
                TableIndex index = PartitionIndex(partition);
                if (block < index.Count)
                {
                    _partition = partition;
                    _block = block;
                    LoadBlock(new DataBlockHandle(index, partition, block));
                    Position(0);
                    return;
                }

                partition++;
                block = 0;
            }

            _partition = partition;
            _block = 0;
            ReleaseWindow();
        }

        // The block's entries, from the cache, the window or the file, and the offset of each. Every entry is
        // decoded here once, so a block that does not decode fails before the cursor gives any of its entries, and
        // the steps over it read its fields with no check of their own.
        private void LoadBlock(DataBlockHandle handle)
        {
            if (table.TryGetCachedBlock(handle, out NativeBlock? cached))
                _cached = cached;
            else
                ReadBlock(handle);

            _nextOffset = handle.Offset + handle.Size;
            int count = 0;
            try
            {
                SpanReader reader = new(Entries);
                while (!reader.AtEnd)
                {
                    if (count == _offsets.Length)
                        Array.Resize(ref _offsets, 2 * count);

                    _offsets[count] = reader.Position;
                    reader.Span();
                    reader.Int64();
                    reader.EntryKind();
                    reader.Span();
                    count++;
                }
            }
            catch (ArgumentOutOfRangeException e)
            {
                ReleaseBlock();
                throw table.DataBlockCorrupt(handle.Block, handle.Offset, e);
            }

            // Every block that reaches a cursor passed CheckBounds, which refuses a block with no entry.
            if (count == 0)
            {
                ReleaseBlock();
                throw Corrupt(table.Number, $"data block {handle.Block} at offset {handle.Offset} has no entries");
            }

            _count = count;
        }

        // The block from the window, or from the file: with the bytes after it when the cursor reads ahead, else alone.
        private void ReadBlock(DataBlockHandle handle)
        {
            long offset = handle.Offset;
            int size = handle.Size;
            byte[] source;
            int at;
            if (_window is not null && offset >= _windowOffset && offset + size <= _windowOffset + _windowLength)
            {
                source = _window;
                at = (int)(offset - _windowOffset);
            }
            else
            {
                // No block handle reaches past the footer's start, so a read up to it never meets the end of the file.
                int ahead = (int)Math.Min(ReadaheadAt(offset), table._blocksEnd - offset);
                if (ahead > size)
                {
                    if (_window is null || _window.Length < ahead)
                    {
                        ReleaseWindow();
                        _window = ArrayPool<byte>.Shared.Rent(ahead);
                    }

                    // Not a window until the read fills it: a failed read leaves bytes of another place.
                    _windowLength = 0;
                    ReadExactly(table._file, _window.AsSpan(0, ahead), offset, table.Number);
                    _windowOffset = offset;
                    _windowLength = ahead;
                    source = _window;
                }
                else
                {
                    _pooled = ArrayPool<byte>.Shared.Rent(size);
                    ReadExactly(table._file, _pooled.AsSpan(0, size), offset, table.Number);
                    source = _pooled;
                }

                at = 0;
            }

            ReadOnlySpan<byte> entries = table.DecodeBlock(source.AsSpan(at, size), handle, out byte[]? decompressed);
            if (decompressed is not null)
            {
                if (_pooled is not null)
                    ArrayPool<byte>.Shared.Return(_pooled);

                _pooled = decompressed;
                _bytes = decompressed;
                _start = 0;
            }
            else
            {
                _bytes = source;
                _start = at;
            }

            _length = entries.Length;

            // A read that fills the cache leaves the block there, and the cursor reads the cache's copy.
            if (fillCache && table.CacheBlockAndKeep(handle, entries) is { } inserted)
            {
                ReleaseBlock();
                _cached = inserted;
            }
        }

        // The bytes that a read of the file at offset takes: the cursor's fixed readahead, or one that starts once
        // two reads in a row followed the block before them and doubles with each read after that; 0 for the block alone.
        private int ReadaheadAt(long offset)
        {
            if (readahead > 0)
                return readahead;

            _sequentialReads = offset == _nextOffset ? _sequentialReads + 1 : 0;
            if (_sequentialReads < 2)
                return 0;

            return Math.Min(MaxReadahead, InitialReadahead << Math.Min(_sequentialReads - 2, 16));
        }

        // The fields of entry `index` of the block, which LoadBlock decoded once already.
        private void Position(int index)
        {
            ReadOnlySpan<byte> entries = Entries;
            int at = _offsets[index];
            _keyLength = ReadInt32LittleEndian(entries[at..]);
            _keyStart = at + 4;
            at = _keyStart + _keyLength;
            _sequence = ReadInt64LittleEndian(entries[at..]);
            _kind = (OperationKind)entries[at + 8];
            _valueLength = ReadInt32LittleEndian(entries[(at + 9)..]);
            _valueStart = at + 13;
            _current = null;
        }

        // Lets the block go: the cursor is on no entry until the next block is loaded.
        private void ReleaseBlock()
        {
            _index = 0;
            _count = 0;
            _current = null;
            _cached?.Release();
            _cached = null;
            if (_pooled is not null)
                ArrayPool<byte>.Shared.Return(_pooled);

            _pooled = null;
            _bytes = null;
            _length = 0;
        }

        private void ReleaseWindow()
        {
            if (_window is not null)
                ArrayPool<byte>.Shared.Return(_window);

            _window = null;
            _windowLength = 0;
        }
    }

    // A data block's entries for one read: a cached native block that the read holds a reference on, or a buffer
    // rented from ArrayPool<byte>.Shared. Dispose releases the one or returns the other, once.
    private struct BlockBuffer : IDisposable
    {
        private NativeBlock? _cached;
        private byte[]? _pooled;
        private readonly int _length;

        public BlockBuffer(NativeBlock cached)
        {
            _cached = cached;
            _length = cached.Length;
        }

        public BlockBuffer(byte[] pooled, int length)
        {
            _pooled = pooled;
            _length = length;
        }

        public readonly ReadOnlySpan<byte> Entries => _cached is not null ? _cached.Span : _pooled.AsSpan(0, _length);

        public void Dispose()
        {
            _cached?.Release();
            _cached = null;
            if (_pooled is not null)
                ArrayPool<byte>.Shared.Return(_pooled);

            _pooled = null;
        }
    }

    // Bounds-checked little-endian reads; a read past the end throws ArgumentOutOfRangeException.
    private ref struct SpanReader(ReadOnlySpan<byte> data)
    {
        private readonly ReadOnlySpan<byte> _data = data;
        private int _position;

        public readonly bool AtEnd => _position == _data.Length;

        public readonly int Position => _position;

        public byte Byte()
        {
            if (_position >= _data.Length)
                throw new ArgumentOutOfRangeException("block", $"a byte at {_position} is past the end of {_data.Length}");

            return _data[_position++];
        }

        /// <summary>
        /// The kind of a data block entry. A table holds puts and deletes only, so any other byte is
        /// damage, not a delete: read as one, it would hide data, and a compaction would drop it.
        /// </summary>
        public OperationKind EntryKind()
        {
            OperationKind kind = (OperationKind)Byte();
            return kind is OperationKind.Put or OperationKind.Delete
                ? kind
                : throw new ArgumentOutOfRangeException("block", $"unknown entry kind {(byte)kind}");
        }

        /// <summary>
        /// A count of items that take at least <paramref name="minimumSize"/> bytes each, checked
        /// against the bytes left, so a damaged count fails before anything is allocated for it.
        /// </summary>
        public int Count(int minimumSize)
        {
            int count = Int32();
            if (count < 0 || count > (_data.Length - _position) / minimumSize)
                throw new ArgumentOutOfRangeException("block", $"a count of {count} does not fit in the block");

            return count;
        }

        public int Int32()
        {
            int value = ReadInt32LittleEndian(_data[_position..]);
            _position += 4;
            return value;
        }

        public long Int64()
        {
            long value = ReadInt64LittleEndian(_data[_position..]);
            _position += 8;
            return value;
        }

        public byte[] Bytes() => Span().ToArray();

        /// <summary>A length-prefixed field, as a view into the block.</summary>
        public ReadOnlySpan<byte> Span()
        {
            int length = Int32();
            ReadOnlySpan<byte> bytes = _data.Slice(_position, length);
            _position += length;
            return bytes;
        }

        public readonly void ExpectEnd()
        {
            if (!AtEnd)
                throw new ArgumentOutOfRangeException("block", "trailing bytes in a block");
        }
    }
}
