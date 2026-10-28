using System.Buffers;
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

    /// <summary>
    /// The value of <paramref name="key"/> at <paramref name="snapshot"/>, from sources searched newest
    /// first. The first source with a version decides it: every older source holds only older data,
    /// so neither its versions nor its range tombstones can be newer. <paramref name="fillCache"/> is
    /// <see cref="ReadOptions.FillCache"/>.
    /// </summary>
    public static bool TryGet(IEntrySource[] sources, ReadOnlySpan<byte> key, long snapshot, bool fillCache, out byte[]? value)
    {
        value = null;
        long covering = 0;

        foreach (IEntrySource source in sources)
        {
            covering = Math.Max(covering, source.CoveringTombstone(key, snapshot));

            if (source.TryGetNewest(key, snapshot, fillCache, out Entry entry))
            {
                if (entry.Kind == OperationKind.Put && covering < entry.Sequence)
                {
                    // A memtable hands out the array it stores, which the caller must not be able to
                    // change; an SSTable decodes a new array for each lookup.
                    value = source is MemTable ? entry.Value!.ToArray() : entry.Value;
                    return true;
                }

                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// The values of <paramref name="keys"/> at <paramref name="snapshot"/>, each as <see cref="TryGet"/> gives it, in
    /// rounds, as RocksDB's batched <c>MultiGet</c>, over batches of up to <see cref="MaxBatchKeys"/> keys: a round
    /// searches each unsettled key of the batch from the source where it stopped until a memtable or a block in
    /// the table's cache answers, or a table's index points to a data block that is not cached, then reads the
    /// distinct blocks of the round in one <see cref="IStorageEnvironment.MultiRead"/> through
    /// <paramref name="environment"/>, and caches them when <paramref name="fillCache"/> is set. A key whose block
    /// has no version of it (a Bloom filter's false positive) goes on to the next source: at once for a cached
    /// block, in the next round for one read. <paramref name="rounds"/> counts the rounds that read blocks.
    /// </summary>
    /// <exception cref="SolariumException">A data block is damaged.</exception>
    public static byte[]?[] GetMany(IEntrySource[] sources, IReadOnlyList<byte[]> keys, long snapshot, bool fillCache, IStorageEnvironment environment, out int rounds)
    {
        int count = keys.Count;
        byte[]?[] values = new byte[]?[count];
        int[] next = new int[count];          // the source each key searches next
        long[] covering = new long[count];    // the highest range tombstone over the sources it searched
        int capacity = Math.Min(count, MaxBatchKeys);
        List<int> pending = new(capacity);

        // One round's keys that wait for a block, the block of each, and the distinct blocks, which keys share.
        List<int> waiting = new(capacity);
        List<int> blockOf = new(capacity);
        List<(TableReader Table, DataBlockHandle Handle)> blocks = new(capacity);
        Dictionary<(TableReader, long), int> distinct = new(capacity);
        rounds = 0;

        for (int start = 0; start < count; start += MaxBatchKeys)
        {
            pending.Clear();
            for (int i = start; i < Math.Min(count, start + MaxBatchKeys); i++)
                pending.Add(i);

            rounds += ReadBatch(sources, keys, snapshot, fillCache, environment, values, next, covering, pending, waiting, blockOf, blocks, distinct);
        }

        return values;
    }

    // The rounds of one batch of keys, the pending ones, until each is settled; returns the rounds that read blocks.
    private static int ReadBatch(IEntrySource[] sources, IReadOnlyList<byte[]> keys, long snapshot, bool fillCache, IStorageEnvironment environment,
        byte[]?[] values, int[] next, long[] covering, List<int> pending, List<int> waiting, List<int> blockOf,
        List<(TableReader Table, DataBlockHandle Handle)> blocks, Dictionary<(TableReader, long), int> distinct)
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
                if (!Advance(sources, keys[k], snapshot, fillCache, ref next[k], ref covering[k], out values[k], out TableReader? table, out DataBlockHandle handle))
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
            ReadAndSettle(environment, keys, snapshot, fillCache, values, covering, waiting, blockOf, blocks, pending);
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

    // Search key's sources from next, adding each one's covering tombstone, until one decides the key or a table
    // must read a data block that its cache does not hold. Returns true with the table and block to read, next
    // past that table; false with the key's value settled.
    private static bool Advance(IEntrySource[] sources, byte[] key, long snapshot, bool fillCache, ref int next, ref long covering,
        out byte[]? value, out TableReader? table, out DataBlockHandle handle)
    {
        value = null;
        handle = default;
        for (; next < sources.Length; next++)
        {
            IEntrySource source = sources[next];
            covering = Math.Max(covering, source.CoveringTombstone(key, snapshot));

            table = source switch
            {
                TableReader reader => reader,
                SortedLevel level => level.TableFor(key),
                _ => null,
            };

            if (table is not null)
            {
                if (!table.TryLocate(key, snapshot, fillCache, out handle))
                    continue;

                if (!table.TryGetCachedBlock(handle, out byte[]? cached))
                {
                    next++;
                    return true;
                }

                if (table.TryGetFromEntries(cached, handle, key, snapshot, out Entry found))
                {
                    value = Value(found, covering, copy: false);
                    table = null;
                    return false;
                }

                continue;
            }

            // A sorted level with no file for the key has no version of it; a memtable answers at once.
            if (source is not SortedLevel && source.TryGetNewest(key, snapshot, fillCache, out Entry entry))
            {
                value = Value(entry, covering, copy: source is MemTable);
                return false;
            }
        }

        table = null;
        return false;
    }

    // Read the round's blocks in one batch, and cache them when fillCache is set, then settle each waiting key that
    // its block has a version of; the others go to pending, for the next round.
    private static void ReadAndSettle(IStorageEnvironment environment, IReadOnlyList<byte[]> keys, long snapshot, bool fillCache, byte[]?[] values, long[] covering,
        List<int> waiting, List<int> blockOf, List<(TableReader Table, DataBlockHandle Handle)> blocks, List<int> pending)
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
                if (blocks[b].Table.TryGetFromEntries(entries[b].AsSpan(0, lengths[b]), blocks[b].Handle, keys[k], snapshot, out Entry entry))
                    values[k] = Value(entry, covering[k], copy: false);
                else
                    pending.Add(k);
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

    // The value a version gives, as TryGet decides it: a Put that no newer range tombstone covers.
    private static byte[]? Value(Entry entry, long covering, bool copy)
    {
        if (entry.Kind != OperationKind.Put || covering >= entry.Sequence)
            return null;

        // A memtable hands out the array it stores, which the caller must not be able to change.
        return copy ? entry.Value!.ToArray() : entry.Value;
    }
}
