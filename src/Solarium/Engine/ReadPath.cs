using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Solarium.Storage;

namespace Solarium.Engine;

/// <summary>Point lookups over a column family's sources, searched newest first.</summary>
internal static class ReadPath
{
    /// <summary>
    /// The most keys that <see cref="GetMany"/> reads in one batch of rounds, as RocksDB's
    /// <c>MultiGetContext::MAX_BATCH_SIZE</c>: it bounds the blocks a round holds in memory and in flight.
    /// </summary>
    public const int MaxBatchKeys = 32;

    // The value in a new array: the caller's to keep and change.
    private static readonly ValueReader<byte[]> s_copy = static value => value.ToArray();

    /// <summary>
    /// The value of <paramref name="key"/> at <paramref name="snapshot"/> in a new array, as
    /// <see cref="TryGet{TResult}"/> finds it.
    /// </summary>
    public static bool TryGet(IEntrySource[] sources, ReadOnlySpan<byte> key, long snapshot, bool fillCache, [NotNullWhen(true)] out byte[]? value) =>
        TryGet(sources, key, snapshot, fillCache, s_copy, out value);

    /// <summary>
    /// Gives the value of <paramref name="key"/> at <paramref name="snapshot"/> to <paramref name="reader"/> where it
    /// lies, from sources searched newest first, and returns false, with the reader not called, when the key has
    /// none. The first source with a version decides it: every older source holds only older data,
    /// so neither its versions nor its range tombstones can be newer. <paramref name="fillCache"/> is
    /// <see cref="ReadOptions.FillCache"/>. The caller is counted in the version of the sources until this returns.
    /// </summary>
    /// <exception cref="SolariumException">A table is damaged.</exception>
    public static bool TryGet<TResult>(IEntrySource[] sources, ReadOnlySpan<byte> key, long snapshot, bool fillCache, ValueReader<TResult> reader, [MaybeNullWhen(false)] out TResult result)
    {
        result = default;
        long covering = 0;

        // The hash that the tables' filters take, computed at the first table: a key that a memtable answers
        // is never hashed, and a key that several tables search is hashed once.
        ulong hash = 0;
        bool hashed = false;

        foreach (IEntrySource source in sources)
        {
            covering = Math.Max(covering, source.CoveringTombstone(key, snapshot));

            long sequence;
            OperationKind kind;
            ReadOnlySpan<byte> value;
            if (source is MemTable memTable)
            {
                if (!memTable.TryFind(key, snapshot, out sequence, out kind, out value))
                    continue;

                if (!IsVisible(kind, sequence, covering))
                    return false;

                // The value is in the memtable's memory, which its version keeps while the caller is counted in it.
                result = reader(value);
                GC.KeepAlive(memTable);
                return true;
            }

            // A sorted level with no file for the key has no version of it.
            TableReader? table = source is SortedLevel level ? level.TableFor(key) : (TableReader)source;
            if (table is null)
                continue;

            if (!hashed)
            {
                hash = TableReader.FilterHash(key);
                hashed = true;
            }

            if (!table.TryLocate(key, hash, snapshot, fillCache, out DataBlockHandle handle))
                continue;

            // Held until the reader returns or throws.
            using TableReader.BlockBuffer block = table.DataBlock(handle, fillCache);
            if (!table.TryFind(block.Entries, handle, key, snapshot, out sequence, out kind, out value))
                continue;

            if (!IsVisible(kind, sequence, covering))
                return false;

            result = reader(value);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Gives the value of each of <paramref name="keys"/> at <paramref name="snapshot"/>, as
    /// <see cref="TryGet{TResult}"/> finds it, to <paramref name="visitor"/> where it lies, with the key's index; a key
    /// with no value is not visited. It reads in
    /// rounds, as RocksDB's batched <c>MultiGet</c>, over batches of up to <see cref="MaxBatchKeys"/> keys: a round
    /// searches each unsettled key of the batch from the source where it stopped until a memtable or a block in
    /// the table's cache answers, or a table's index points to a data block that is not cached, then reads the
    /// distinct blocks of the round in one <see cref="IStorageEnvironment.MultiRead"/> through
    /// <paramref name="environment"/>, and caches them when <paramref name="fillCache"/> is set. A key whose block
    /// has no version of it (a Bloom filter's false positive) goes on to the next source: at once for a cached
    /// block, in the next round for one read. So the visits follow the order in which the keys settle. The caller is
    /// counted in the version of the sources until this returns.
    /// </summary>
    /// <returns>The rounds that read blocks.</returns>
    /// <exception cref="SolariumException">A data block is damaged; no key of its round is visited.</exception>
    public static int GetMany<TVisitor>(IEntrySource[] sources, IReadOnlyList<byte[]> keys, long snapshot, bool fillCache, IStorageEnvironment environment, ref TVisitor visitor)
        where TVisitor : struct, IValueVisitor
    {
        int count = keys.Count;
        int[] next = new int[count];          // the source each key searches next
        long[] covering = new long[count];    // the highest range tombstone over the sources it searched
        ulong[] hashes = new ulong[count];    // the hash for the tables' filters, 0 until the key reaches a table
        int capacity = Math.Min(count, MaxBatchKeys);
        List<int> pending = new(capacity);

        // One round's keys that wait for a block, the block of each, and the distinct blocks, which keys share.
        List<int> waiting = new(capacity);
        List<int> blockOf = new(capacity);
        List<(TableReader Table, DataBlockHandle Handle)> blocks = new(capacity);
        Dictionary<(TableReader, long), int> distinct = new(capacity);
        int rounds = 0;

        for (int start = 0; start < count; start += MaxBatchKeys)
        {
            pending.Clear();
            for (int i = start; i < Math.Min(count, start + MaxBatchKeys); i++)
                pending.Add(i);

            rounds += ReadBatch(sources, keys, snapshot, fillCache, environment, ref visitor, next, covering, hashes, pending, waiting, blockOf, blocks, distinct);
        }

        return rounds;
    }

    // The rounds of one batch of keys, the pending ones, until each is settled; returns the rounds that read blocks.
    private static int ReadBatch<TVisitor>(IEntrySource[] sources, IReadOnlyList<byte[]> keys, long snapshot, bool fillCache, IStorageEnvironment environment,
        ref TVisitor visitor, int[] next, long[] covering, ulong[] hashes, List<int> pending, List<int> waiting, List<int> blockOf,
        List<(TableReader Table, DataBlockHandle Handle)> blocks, Dictionary<(TableReader, long), int> distinct)
        where TVisitor : struct, IValueVisitor
    {
        int rounds = 0;
        while (pending.Count > 0)
        {
            waiting.Clear();
            blockOf.Clear();
            blocks.Clear();
            distinct.Clear();
            foreach (int k in pending)
            {
                if (!Advance(sources, keys[k], k, snapshot, fillCache, ref visitor, ref next[k], ref covering[k], ref hashes[k], out TableReader? table, out DataBlockHandle handle))
                    continue;

                if (!distinct.TryGetValue((table!, handle.Offset), out int block))
                {
                    block = blocks.Count;
                    blocks.Add((table!, handle));
                    distinct.Add((table!, handle.Offset), block);
                }

                waiting.Add(k);
                blockOf.Add(block);
            }

            if (waiting.Count == 0)
                break;

            rounds++;
            pending.Clear();
            ReadAndSettle(environment, keys, snapshot, fillCache, ref visitor, covering, waiting, blockOf, blocks, pending);
        }

        return rounds;
    }

    /// <summary>The highest range tombstone at or below <paramref name="snapshot"/> that covers <paramref name="key"/>, over all sources.</summary>
    public static long CoveringTombstone(IEntrySource[] sources, ReadOnlySpan<byte> key, long snapshot)
    {
        long highest = 0;
        foreach (IEntrySource source in sources)
            highest = Math.Max(highest, source.CoveringTombstone(key, snapshot));

        return highest;
    }

    // Search the sources of the key at index from next, adding each one's covering tombstone, until one decides the
    // key or a table must read a data block that its cache does not hold. Returns true with the table and block to
    // read, next past that table; false with the key settled, and visited when it has a value.
    private static bool Advance<TVisitor>(IEntrySource[] sources, byte[] key, int index, long snapshot, bool fillCache, ref TVisitor visitor,
        ref int next, ref long covering, ref ulong hash, out TableReader? table, out DataBlockHandle handle)
        where TVisitor : struct, IValueVisitor
    {
        handle = default;
        for (; next < sources.Length; next++)
        {
            IEntrySource source = sources[next];
            covering = Math.Max(covering, source.CoveringTombstone(key, snapshot));

            long sequence;
            OperationKind kind;
            ReadOnlySpan<byte> value;
            if (source is MemTable memTable)
            {
                if (!memTable.TryFind(key, snapshot, out sequence, out kind, out value))
                    continue;

                if (IsVisible(kind, sequence, covering))
                    visitor.OnValue(index, value);

                GC.KeepAlive(memTable);
                table = null;
                return false;
            }

            // A sorted level with no file for the key has no version of it.
            table = source is SortedLevel level ? level.TableFor(key) : (TableReader)source;
            if (table is null)
                continue;

            // Computed at the key's first table and kept for the others, in this round and the next ones. A
            // hash of 0 is computed again at each table, which costs only the time.
            if (hash == 0)
                hash = TableReader.FilterHash(key);

            if (!table.TryLocate(key, hash, snapshot, fillCache, out handle))
                continue;

            if (!table.TryGetCachedBlock(handle, out NativeBlock? cached))
            {
                next++;
                return true;
            }

            // Held until the visitor returns or throws.
            try
            {
                if (!table.TryFind(cached.Span, handle, key, snapshot, out sequence, out kind, out value))
                    continue;

                if (IsVisible(kind, sequence, covering))
                    visitor.OnValue(index, value);
            }
            finally
            {
                cached.Release();
            }

            table = null;
            return false;
        }

        table = null;
        return false;
    }

    // Read the round's blocks in one batch, and cache them when fillCache is set, then settle each waiting key that
    // its block has a version of, and visit it when it has a value; the others go to pending, for the next round. The
    // blocks are held until the last visitor returns or one throws.
    private static void ReadAndSettle<TVisitor>(IStorageEnvironment environment, IReadOnlyList<byte[]> keys, long snapshot, bool fillCache, ref TVisitor visitor,
        long[] covering, List<int> waiting, List<int> blockOf, List<(TableReader Table, DataBlockHandle Handle)> blocks, List<int> pending)
        where TVisitor : struct, IValueVisitor
    {
        ReadRequest[] requests = new ReadRequest[blocks.Count];
        byte[]?[] raw = new byte[]?[blocks.Count];
        byte[]?[] entries = new byte[]?[blocks.Count];
        int[] lengths = new int[blocks.Count];
        try
        {
            for (int b = 0; b < blocks.Count; b++)
            {
                (TableReader table, DataBlockHandle handle) = blocks[b];
                raw[b] = ArrayPool<byte>.Shared.Rent(handle.Size);
                requests[b] = new ReadRequest(table.File, raw[b].AsMemory(0, handle.Size), handle.Offset);
            }

            environment.MultiRead(requests);
            for (int b = 0; b < blocks.Count; b++)
            {
                entries[b] = blocks[b].Table.DecodeReadBlock(raw[b]!, requests[b].BytesRead, blocks[b].Handle, out lengths[b]);
                if (fillCache)
                    blocks[b].Table.CacheBlock(blocks[b].Handle, entries[b].AsSpan(0, lengths[b]));
            }

            for (int w = 0; w < waiting.Count; w++)
            {
                int k = waiting[w];
                int b = blockOf[w];
                if (!blocks[b].Table.TryFind(entries[b].AsSpan(0, lengths[b]), blocks[b].Handle, keys[k], snapshot, out long sequence, out OperationKind kind, out ReadOnlySpan<byte> value))
                    pending.Add(k);
                else if (IsVisible(kind, sequence, covering[k]))
                    visitor.OnValue(k, value);
            }
        }
        finally
        {
            for (int b = 0; b < blocks.Count; b++)
            {
                if (entries[b] is { } decoded && !ReferenceEquals(decoded, raw[b]))
                    ArrayPool<byte>.Shared.Return(decoded);

                if (raw[b] is { } read)
                    ArrayPool<byte>.Shared.Return(read);
            }
        }
    }

    // Whether the newest version of a key gives it a value: a put that no newer range tombstone covers.
    private static bool IsVisible(OperationKind kind, long sequence, long covering) => kind == OperationKind.Put && covering < sequence;
}
