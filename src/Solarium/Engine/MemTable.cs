using System.Runtime.CompilerServices;

namespace Solarium.Engine;

/// <summary>
/// The in-memory table of one column family: a multi-version skip list in internal key order.
/// </summary>
/// <remarks>
/// <para>Any number of lock-free readers, and either one writer at a time (<see cref="Add"/>, the
/// LevelDB design) or several writers at once (<see cref="AddConcurrently"/>, as RocksDB's
/// <c>InlineSkipList::InsertConcurrently</c>), never both at once. A node is fully built before a
/// release-store or a compare-and-swap links it in, and nodes are never removed, so a reader can walk
/// the list while writers insert.</para>
/// <para>A reader sees a consistent state through sequence numbers, not through the list: the
/// database publishes a group's last sequence number only after every entry of the group is in,
/// and readers skip entries newer than the snapshot they read.</para>
/// <para>Range tombstones sit in a separate array that only grows, and each range delete publishes a
/// new <see cref="TombstoneSet"/> over a longer prefix of it, so the set a reader holds never changes.
/// The writer indexes them as runs of <see cref="FragmentedTombstones"/> whose sizes count in binary,
/// in units of <see cref="LinearTombstoneLimit"/>: a lookup searches each run and scans the few
/// tombstones past the last one, and no reader builds anything. A tombstone takes part in one build
/// each time the count of tombstones doubles, and the largest build, of all of them, comes once per
/// doubling. RocksDB instead caches the fragments of all of a memtable's tombstones until the next
/// range delete, and the first read after it fragments all of them again.</para>
/// </remarks>
internal sealed class MemTable : IEntrySource
{
    private const int MaxHeight = 12;

    /// <summary>
    /// The size of the smallest run of fragments: fewer range tombstones than this past the last
    /// run are scanned, and at this many the writer indexes them. A scan of a few is cheaper than
    /// a build of their fragments; an SSTable with no more than this many scans them all too.
    /// </summary>
    internal const int LinearTombstoneLimit = 8;

    private readonly Node _head = new([], 0, OperationKind.Put, null, MaxHeight);
    private readonly Random _random = new(0x5eed);   // the single writer only
    private readonly Node?[] _splice = new Node?[MaxHeight];   // the single writer only
    private readonly object _tombstoneLock = new();   // concurrent writers of range tombstones
    private int _height = 1;
    private TombstoneSet _tombstones = TombstoneSet.Empty;
    private long _approximateBytes;
    private long _entryCount;
    private long _minSequence;
    private long _maxSequence;
    private long _charged;            // the bytes reserved on the database's WriteBufferManager
    private bool _chargeImmutable;    // the charge left the manager's active memory

    /// <summary>Key and value bytes held, for the flush threshold.</summary>
    public long ApproximateBytes => Volatile.Read(ref _approximateBytes);

    /// <summary>Point entries held (puts and deletes); <c>rocksdb.num-entries-active-mem-table</c> adds the range tombstones.</summary>
    public long EntryCount => Volatile.Read(ref _entryCount);

    /// <summary>True until the first entry or range tombstone is added.</summary>
    public bool IsEmpty => Volatile.Read(ref _maxSequence) == 0;

    /// <summary>The lowest sequence number held, or 0 when empty.</summary>
    public long MinSequence => Volatile.Read(ref _minSequence);

    /// <summary>The highest sequence number held, or 0 when empty.</summary>
    public long MaxSequence => Volatile.Read(ref _maxSequence);

    /// <summary>A snapshot of the range tombstones held, in the order they were added; the slots it covers never change.</summary>
    public IReadOnlyList<RangeTombstone> Tombstones => Volatile.Read(ref _tombstones).All;

    /// <summary>Set once the memtable's data is durable in an SSTable listed by the manifest.</summary>
    public bool Flushed { get; set; }

    // ── WriteBufferManager charge ────────────────────────────────────────────────────────────────
    // Each call comes from the thread that holds the lock that keeps a memtable switch out (the memtable
    // stage, the switch, the flush install), from replay, or from the close, so they never overlap.

    /// <summary>
    /// Reserve on <paramref name="manager"/> the bytes added since the last charge. Only while the memtable
    /// is active, after the inserts of a group and before the group is published.
    /// </summary>
    public void Charge(WriteBufferManager manager)
    {
        long bytes = ApproximateBytes - _charged;
        if (bytes <= 0)
            return;

        manager.Reserve(bytes);
        _charged += bytes;
    }

    /// <summary>Take the memtable's bytes out of the manager's active memory, once: it took its last write.</summary>
    public void ChargeImmutable(WriteBufferManager manager)
    {
        if (_chargeImmutable)
            return;

        manager.ScheduleFree(_charged);
        _chargeImmutable = true;
    }

    /// <summary>
    /// Take the memtable's bytes out of the manager: the database no longer holds it. A second call
    /// releases nothing.
    /// </summary>
    public void ReleaseCharge(WriteBufferManager manager)
    {
        ChargeImmutable(manager);
        manager.Free(_charged);
        _charged = 0;
    }

    // ── Writer side: one writer ──────────────────────────────────────────────────────────────────

    /// <summary>The bytes that one entry or range tombstone adds to <see cref="ApproximateBytes"/>.</summary>
    public static long EntryBytes(byte[] key, byte[]? value) => key.Length + (value?.Length ?? 0) + 32;

    /// <summary>Insert one version of <paramref name="key"/>. One writer at a time, with no concurrent writer; the memtable keeps the arrays it is given.</summary>
    public void Add(byte[] key, long sequence, OperationKind kind, byte[]? value)
    {
        Node x = _head;
        for (int level = Volatile.Read(ref _height) - 1; level >= 0; level--)
        {
            Node? next = x.Next[level];
            while (next is not null && Compare(next, key, sequence) < 0)
            {
                x = next;
                next = x.Next[level];
            }

            _splice[level] = x;
        }

        int height = RandomHeight();
        int current = _height;
        if (height > current)
        {
            for (int level = current; level < height; level++)
                _splice[level] = _head;

            // Published before the links: a reader that sees the new height finds null at the
            // new levels of the head and drops down.
            Volatile.Write(ref _height, height);
        }

        Node node = new(key, sequence, kind, value, height);
        for (int level = 0; level < height; level++)
        {
            node.Next[level] = _splice[level]!.Next[level];
            Volatile.Write(ref _splice[level]!.Next[level], node);
        }

        Volatile.Write(ref _approximateBytes, _approximateBytes + key.Length + (value?.Length ?? 0) + 32);
        Volatile.Write(ref _entryCount, _entryCount + 1);
        NoteSequence(sequence);
    }

    /// <summary>Record a range delete of [<paramref name="begin"/>, <paramref name="end"/>). One writer at a time, with no concurrent writer.</summary>
    public void AddRangeTombstone(byte[] begin, byte[] end, long sequence)
    {
        // A range with no key in it deletes nothing. WriteBatch refuses one with its ends swapped,
        // but a WAL written before it did can still hold one, and its table would have a smallest
        // key above its largest.
        if (begin.AsSpan().SequenceCompareTo(end) >= 0)
        {
            NoteSequence(sequence);
            return;
        }

        TombstoneSet current = _tombstones;
        TombstoneSet next = current.With(new RangeTombstone(begin, end, sequence));
        Volatile.Write(ref _tombstones, next);
        Volatile.Write(ref _approximateBytes, _approximateBytes + begin.Length + end.Length + 32 + next.FragmentBytes - current.FragmentBytes);
        NoteSequence(sequence);
    }

    private void NoteSequence(long sequence)
    {
        if (_minSequence == 0)
            Volatile.Write(ref _minSequence, sequence);

        Volatile.Write(ref _maxSequence, Math.Max(_maxSequence, sequence));
    }

    // ── Writer side: several writers at once ─────────────────────────────────────────────────────

    /// <summary>
    /// Insert one version of <paramref name="key"/> while other threads insert too. The caller adds
    /// the entry to the counters with <see cref="AddCounts"/>, once for each run of entries, and no
    /// thread calls <see cref="Add"/> or <see cref="AddRangeTombstone"/> until every concurrent
    /// insert has returned. The memtable keeps the arrays it is given.
    /// </summary>
    /// <returns>How many times a link changed under the insert, so a level was searched again.</returns>
    public int AddConcurrently(byte[] key, long sequence, OperationKind kind, byte[]? value)
    {
        int height = RandomHeight(Random.Shared);

        // Raised before the search, so the search covers every level of the node. A reader that
        // sees the new height before the links finds null or older links at the new levels.
        int top = Volatile.Read(ref _height);
        while (height > top)
        {
            int seen = Interlocked.CompareExchange(ref _height, height, top);
            top = seen == top ? height : seen;
        }

        // On the stack: an inserter's splice is its own, and a thread keeps no node of a memtable.
        Splice prev = default;
        Splice next = default;
        Node x = _head;
        for (int level = top - 1; level >= 0; level--)
        {
            x = FindSpliceForLevel(x, key, sequence, level, out next[level]);
            prev[level] = x;
        }

        Node node = new(key, sequence, kind, value, height);
        int retries = 0;
        for (int level = 0; level < height; level++)
        {
            // The node's own link is written before the compare-and-swap that makes it reachable
            // at this level, a full fence. Linked from level 0 up, so a node that a reader reaches
            // at a level is linked at every level below it. Nodes are never removed, so prev is
            // still before the key when the swap fails, and the search goes on from it.
            while (true)
            {
                node.Next[level] = next[level];
                if (ReferenceEquals(Interlocked.CompareExchange(ref prev[level]!.Next[level], node, next[level]), next[level]))
                    break;

                retries++;
                prev[level] = FindSpliceForLevel(prev[level]!, key, sequence, level, out next[level]);
            }
        }

        return retries;
    }

    /// <summary>
    /// Record a range delete of [<paramref name="begin"/>, <paramref name="end"/>) while other threads
    /// insert too. The caller adds it to the counters with <see cref="AddCounts"/>.
    /// </summary>
    public void AddRangeTombstoneConcurrently(byte[] begin, byte[] end, long sequence)
    {
        // A range with no key in it deletes nothing; see AddRangeTombstone.
        if (begin.AsSpan().SequenceCompareTo(end) >= 0)
            return;

        // A lock and not a compare-and-swap loop: only one writer may append to the array, and a
        // retry would build a run again. The caller counts the tombstone; this counts its fragments.
        long grown;
        lock (_tombstoneLock)
        {
            TombstoneSet current = _tombstones;
            TombstoneSet next = current.With(new RangeTombstone(begin, end, sequence));
            Volatile.Write(ref _tombstones, next);
            grown = next.FragmentBytes - current.FragmentBytes;
        }

        if (grown != 0)
            Interlocked.Add(ref _approximateBytes, grown);
    }

    /// <summary>
    /// Add a run of concurrent inserts to the counters: <paramref name="bytes"/> by
    /// <see cref="EntryBytes"/>, <paramref name="entries"/> point entries, and the sequence numbers
    /// from <paramref name="firstSequence"/> to <paramref name="lastSequence"/>. One atomic update
    /// for each counter and run, as RocksDB's <c>MemTable::BatchPostProcess</c>.
    /// </summary>
    public void AddCounts(long bytes, long entries, long firstSequence, long lastSequence)
    {
        Interlocked.Add(ref _approximateBytes, bytes);
        if (entries > 0)
            Interlocked.Add(ref _entryCount, entries);

        // Concurrent runs end in any order, so both ends move with a compare-and-swap.
        long min = Volatile.Read(ref _minSequence);
        while (min == 0 || firstSequence < min)
        {
            long seen = Interlocked.CompareExchange(ref _minSequence, firstSequence, min);
            if (seen == min)
                break;

            min = seen;
        }

        long max = Volatile.Read(ref _maxSequence);
        while (lastSequence > max)
        {
            long seen = Interlocked.CompareExchange(ref _maxSequence, lastSequence, max);
            if (seen == max)
                break;

            max = seen;
        }
    }

    // The last node of the level before (key, sequence), from start on, which must be before it;
    // next is the node after it.
    private static Node FindSpliceForLevel(Node start, byte[] key, long sequence, int level, out Node? next)
    {
        Node x = start;
        next = Volatile.Read(ref x.Next[level]);
        while (next is not null && Compare(next, key, sequence) < 0)
        {
            x = next;
            next = Volatile.Read(ref x.Next[level]);
        }

        return x;
    }

    // ── Reader side: any thread, no lock ─────────────────────────────────────────────────────────

    /// <inheritdoc />
    public bool TryGetNewest(ReadOnlySpan<byte> key, long snapshot, bool fillCache, out Entry entry)
    {
        Node? node = Seek(key, snapshot);
        if (node is not null && node.Key.AsSpan().SequenceEqual(key))
        {
            entry = node.ToEntry();
            return true;
        }

        entry = default;
        return false;
    }

    /// <inheritdoc />
    public long CoveringTombstone(ReadOnlySpan<byte> key, long snapshot) => Volatile.Read(ref _tombstones).Covering(key, snapshot);

    /// <inheritdoc />
    public byte[]? LastKeyBefore(byte[]? limit, bool fillCache) => (limit is null ? Last() : FindLessThan(limit))?.Key;

    /// <inheritdoc />
    public IEntryCursor NewCursor(bool fillCache) => new Cursor(this);

    /// <summary>Every entry in internal key order, every version of each key included, when no writer is left.</summary>
    public IEnumerable<Entry> Entries()
    {
        for (Node? node = First(); node is not null; node = Volatile.Read(ref node.Next[0]))
            yield return node.ToEntry();
    }

    /// <summary>
    /// The entries a flush writes, in internal key order, when no writer is left: the newest version
    /// of each key, unless a newer range tombstone of this memtable covers it, as RocksDB's flush with
    /// no snapshot. The range tombstones all stay, for the older tables they cover, and so does a
    /// delete that no tombstone covers. Every reader of the flushed table takes its snapshot after
    /// the table replaces this memtable, above every entry here, so it needs nothing that is dropped;
    /// a reader that took its snapshot before reads this memtable.
    /// </summary>
    public IEnumerable<Entry> FlushEntries() => FlushEntries(long.MaxValue, Tombstones);

    /// <summary>
    /// The entries a flush would write if the memtable held only what a read at
    /// <paramref name="snapshot"/> sees, with <paramref name="tombstones"/> from
    /// <see cref="TombstonesAt"/> at the same snapshot. Safe while writers insert, as a read is:
    /// an entry above the snapshot is skipped, and every entry at or below a published sequence
    /// number is linked before that number is published.
    /// </summary>
    public IEnumerable<Entry> FlushEntries(long snapshot, IReadOnlyList<RangeTombstone> tombstones)
    {
        TombstoneSweep sweep = new(tombstones);
        byte[]? previous = null;
        for (Node? node = First(); node is not null; node = Volatile.Read(ref node.Next[0]))
        {
            // The first version at or below the snapshot of each key is its newest.
            if (node.Sequence > snapshot || (previous is not null && node.Key.AsSpan().SequenceEqual(previous)))
                continue;

            previous = node.Key;
            if (sweep.Covering(node.Key) < node.Sequence)
                yield return node.ToEntry();
        }
    }

    /// <summary>The range tombstones a read at <paramref name="snapshot"/> sees, in the order they were added.</summary>
    public List<RangeTombstone> TombstonesAt(long snapshot)
    {
        IReadOnlyList<RangeTombstone> all = Tombstones;
        List<RangeTombstone> visible = new(all.Count);
        foreach (RangeTombstone tombstone in all)
            if (tombstone.Sequence <= snapshot)
                visible.Add(tombstone);

        return visible;
    }

    /// <summary>
    /// The first entry at or after (<paramref name="key"/>, <paramref name="sequence"/>) in internal key
    /// order. <c>long.MaxValue</c> finds the first version of the first key at or after <paramref name="key"/>.
    /// </summary>
    private Node? Seek(ReadOnlySpan<byte> key, long sequence)
    {
        Node x = _head;
        int level = Volatile.Read(ref _height) - 1;

        while (true)
        {
            Node? next = Volatile.Read(ref x.Next[level]);
            if (next is not null && Compare(next, key, sequence) < 0)
                x = next;
            else if (level == 0)
                return next;
            else
                level--;
        }
    }

    // The last entry whose key is less than key, or null.
    private Node? FindLessThan(ReadOnlySpan<byte> key)
    {
        Node x = _head;
        int level = Volatile.Read(ref _height) - 1;

        while (true)
        {
            Node? next = Volatile.Read(ref x.Next[level]);
            if (next is not null && Compare(next, key, long.MaxValue) < 0)
                x = next;
            else if (level == 0)
                return ReferenceEquals(x, _head) ? null : x;
            else
                level--;
        }
    }

    private Node? First() => Volatile.Read(ref _head.Next[0]);

    private Node? Last()
    {
        Node x = _head;
        for (int level = Volatile.Read(ref _height) - 1; level >= 0; level--)
            for (Node? next = Volatile.Read(ref x.Next[level]); next is not null; next = Volatile.Read(ref x.Next[level]))
                x = next;

        return ReferenceEquals(x, _head) ? null : x;
    }

    private static int Compare(Node node, ReadOnlySpan<byte> key, long sequence)
    {
        return InternalKey.Compare(node.Key, node.Sequence, key, sequence);
    }

    private int RandomHeight() => RandomHeight(_random);

    private static int RandomHeight(Random random)
    {
        int height = 1;
        while (height < MaxHeight && random.Next(4) == 0)
            height++;

        return height;
    }

    private sealed class Node(byte[] key, long sequence, OperationKind kind, byte[]? value, int height)
    {
        public readonly byte[] Key = key;
        public readonly long Sequence = sequence;
        public readonly OperationKind Kind = kind;
        public readonly byte[]? Value = value;
        public readonly Node?[] Next = new Node?[height];

        public Entry ToEntry() => new(Key, Sequence, Kind, Value);
    }

    [InlineArray(MaxHeight)]
    private struct Splice
    {
        private Node? _element;
    }

    private sealed class Cursor(MemTable table) : IEntryCursor
    {
        private Node? _node;

        public bool Valid => _node is not null;

        public Entry Current => _node!.ToEntry();

        public void SeekToFirst() => _node = table.First();

        public void Seek(ReadOnlySpan<byte> key) => _node = table.Seek(key, long.MaxValue);

        public void Next() => _node = Volatile.Read(ref _node!.Next[0]);
    }

    /// <summary>
    /// The range tombstones of a memtable as one published object, never changed: the first
    /// <see cref="Count"/> slots of an array that the sets after it share and only append to, and
    /// the runs of fragments over them. Publishing both as one object keeps a reader from pairing
    /// tombstones with the runs of another set.
    /// </summary>
    private sealed class TombstoneSet
    {
        private readonly RangeTombstone[] _items;   // the slots below Count are never written again
        private readonly Run[] _runs;               // over _items[.._indexed], oldest first, each larger than the next
        private readonly int _indexed;

        private TombstoneSet(RangeTombstone[] items, int count, Run[] runs, int indexed, long fragmentBytes)
        {
            _items = items;
            Count = count;
            _runs = runs;
            _indexed = indexed;
            FragmentBytes = fragmentBytes;
        }

        /// <summary>The set of no tombstone.</summary>
        public static TombstoneSet Empty { get; } = new([], 0, [], 0, 0);

        /// <summary>The number of tombstones.</summary>
        public int Count { get; }

        /// <summary>The tombstones, in the order they were added.</summary>
        public ArraySegment<RangeTombstone> All => new(_items, 0, Count);

        /// <summary>The memory that the runs' fragments add to the tombstones.</summary>
        public long FragmentBytes { get; }

        /// <summary>The highest sequence number at or below <paramref name="snapshot"/> of a tombstone over <paramref name="key"/>, or 0.</summary>
        public long Covering(ReadOnlySpan<byte> key, long snapshot)
        {
            // Each run holds a part of the tombstones, so the highest over the key is the highest of
            // the runs' answers and the tail's.
            long highest = 0;
            foreach (Run run in _runs)
                highest = Math.Max(highest, run.Fragments.Covering(key, snapshot));

            for (int i = _indexed; i < Count; i++)
            {
                RangeTombstone tombstone = _items[i];
                if (tombstone.Sequence > highest && tombstone.Covers(key, snapshot))
                    highest = tombstone.Sequence;
            }

            return highest;
        }

        /// <summary>
        /// A new set with <paramref name="tombstone"/> added; this one does not change. One writer at
        /// a time. When <see cref="LinearTombstoneLimit"/> tombstones are past the last run, they
        /// become a run, built together with the runs before it that are no larger, as a binary
        /// counter carries.
        /// </summary>
        public TombstoneSet With(RangeTombstone tombstone)
        {
            // The slot at Count is past the tombstones of every published set, so no reader reads it
            // until the set that holds it is published, by the release store of the caller.
            RangeTombstone[] items = _items;
            if (Count == items.Length)
            {
                items = new RangeTombstone[Math.Max(LinearTombstoneLimit, 2 * items.Length)];
                Array.Copy(_items, items, Count);
            }

            items[Count] = tombstone;
            int count = Count + 1;
            if (count - _indexed < LinearTombstoneLimit)
                return new TombstoneSet(items, count, _runs, _indexed, FragmentBytes);

            int keep = _runs.Length;
            int start = _indexed;
            long bytes = FragmentBytes;
            while (keep > 0 && _runs[keep - 1].Count <= count - start)
            {
                keep--;
                start = _runs[keep].Start;
                bytes -= _runs[keep].Fragments.MemoryBytes;
            }

            FragmentedTombstones fragments = FragmentedTombstones.Build(new ArraySegment<RangeTombstone>(items, start, count - start));
            Run[] runs = new Run[keep + 1];
            Array.Copy(_runs, runs, keep);
            runs[keep] = new Run(fragments, start, count - start);
            return new TombstoneSet(items, count, runs, count, bytes + fragments.MemoryBytes);
        }

        /// <summary>The fragments of <paramref name="Count"/> tombstones from slot <paramref name="Start"/>.</summary>
        private readonly record struct Run(FragmentedTombstones Fragments, int Start, int Count);
    }
}
