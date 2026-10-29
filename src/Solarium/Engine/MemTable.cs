using System.Runtime.CompilerServices;

namespace Solarium.Engine;

/// <summary>
/// The in-memory table of one column family: a multi-version skip list in internal key order, in native memory.
/// </summary>
/// <remarks>
/// <para>Any number of lock-free readers, and either one writer at a time (<see cref="Add"/>, the
/// LevelDB design) or several writers at once (<see cref="AddConcurrently"/>, as RocksDB's
/// <c>InlineSkipList::InsertConcurrently</c>), never both at once. A node is fully built before a
/// release-store or a compare-and-swap links it in, and nodes are never removed, so a reader can walk
/// the list while writers insert.</para>
/// <para>Each node is one allocation in the memtable's <see cref="Arena"/>, as RocksDB's <c>InlineSkipList</c>
/// lays it out: the sequence number, the lengths, the kind and the height, then the links, then the key and the
/// value. The links are addresses, which never change, since the arena never moves or frees a node while the
/// memtable lives. The garbage collector sees none of it: an entry is no object, so no collection copies or marks
/// it, and its bytes count against no heap limit. The key and the value are copied in from the caller's arrays,
/// and a read copies them out into new arrays (<see cref="Entry"/>), as a read of an SSTable does.</para>
/// <para>The arena is freed when no reader can reach the memtable: each <see cref="FamilyVersion"/> that lists it
/// counts a reference (<see cref="AddReference"/>, <see cref="Release"/>), and a reader is counted in a version for
/// as long as it reads, so the last reference goes after the last reader leaves.</para>
/// <para>A reader sees a consistent state through sequence numbers, not through the list: the
/// database publishes a group's last sequence number only after every entry of the group is in,
/// and readers skip entries newer than the snapshot they read.</para>
/// <para>Range tombstones sit in a separate array that only grows, and each range delete publishes a
/// new <see cref="TombstoneSet"/> over a longer prefix of it, so the set a reader holds never changes.
/// They stay managed objects: a memtable holds few. The writer indexes them as runs of
/// <see cref="FragmentedTombstones"/> whose sizes count in binary,
/// in units of <see cref="LinearTombstoneLimit"/>: a lookup searches each run and scans the few
/// tombstones past the last one, and no reader builds anything. A tombstone takes part in one build
/// each time the count of tombstones doubles, and the largest build, of all of them, comes once per
/// doubling. RocksDB instead caches the fragments of all of a memtable's tombstones until the next
/// range delete, and the first read after it fragments all of them again.</para>
/// </remarks>
internal sealed unsafe class MemTable : IEntrySource
{
    private const int MaxHeight = 12;

    // A node: the sequence number (8 bytes), the key's length (4), the value's length (4, -1 for none), the kind
    // (1) and the height (1), padding to 24, then one link of 8 bytes for each level, then the key and the value.
    private const int SequenceOffset = 0;
    private const int KeyLengthOffset = 8;
    private const int ValueLengthOffset = 12;
    private const int KindOffset = 16;
    private const int HeightOffset = 17;
    private const int LinksOffset = 24;

    /// <summary>
    /// The size of the smallest run of fragments: fewer range tombstones than this past the last
    /// run are scanned, and at this many the writer indexes them. A scan of a few is cheaper than
    /// a build of their fragments; an SSTable with no more than this many scans them all too.
    /// </summary>
    internal const int LinearTombstoneLimit = 8;

    private readonly Arena _arena;
    private readonly byte* _head;   // a node of MaxHeight levels with no key, never compared
    private readonly Random _random = new(0x5eed);   // the single writer only
    private readonly nint[] _splice = new nint[MaxHeight];   // the single writer only
    private readonly object _tombstoneLock = new();   // concurrent writers of range tombstones
    private int _height = 1;
    private TombstoneSet _tombstones = TombstoneSet.Empty;
    private long _approximateBytes;
    private long _entryCount;
    private long _minSequence;
    private long _maxSequence;
    private int _references;          // Interlocked: the versions that list the memtable and are not closed
    private long _charged;            // the bytes reserved on the database's WriteBufferManager
    private bool _chargeImmutable;    // the charge left the manager's active memory

    /// <summary>An empty memtable whose arena takes its chunks from <see cref="ArenaChunkPool.Shared"/>.</summary>
    public MemTable()
        : this(ArenaChunkPool.Shared)
    {
    }

    /// <summary>An empty memtable whose arena takes its chunks from <paramref name="pool"/>.</summary>
    public MemTable(ArenaChunkPool pool)
    {
        _arena = new Arena(pool);
        int size = NodeSize(MaxHeight, 0, 0);
        _head = _arena.Allocate(size);
        new Span<byte>(_head, size).Clear();
        _head[HeightOffset] = MaxHeight;
    }

    /// <summary>
    /// The bytes the memtable holds, for the flush threshold and the <see cref="WriteBufferManager"/>: its nodes in
    /// the arena, keys, values and links included, and its range tombstones with their fragments. The arena's
    /// chunks hold up to one partly filled chunk per writer's processor besides (<see cref="ArenaBytes"/>).
    /// </summary>
    public long ApproximateBytes => Volatile.Read(ref _approximateBytes);

    /// <summary>The native memory the memtable's arena holds: its chunks, used or not, and its large blocks.</summary>
    public long ArenaBytes => _arena.AllocatedBytes;

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

    /// <summary>True once the arena is freed: no reader may read the memtable's entries any more.</summary>
    internal bool IsFreed => _arena.IsFreed;

    // ── References ───────────────────────────────────────────────────────────────────────────────

    /// <summary>Count one more holder: a version that lists the memtable, or a flush that waits to write it.</summary>
    /// <exception cref="InvalidOperationException">The arena is freed already.</exception>
    public void AddReference()
    {
        // A holder takes its reference from a version that holds one, so the count is never 0 here, except for a
        // new memtable's first version.
        if (Interlocked.Increment(ref _references) <= 1 && _arena.IsFreed)
            throw new InvalidOperationException("A memtable whose memory was freed was referenced again.");
    }

    /// <summary>Drop a holder's reference. The last one frees the arena: no reader can reach the memtable any more.</summary>
    public void Release()
    {
        int left = Interlocked.Decrement(ref _references);
        if (left == 0)
            _arena.Free();
        else if (left < 0)
            throw new InvalidOperationException("A memtable was released more times than it was referenced.");
    }

    /// <summary>
    /// Frees the arena of a memtable that no version ever listed, as a test does with one it built alone; the
    /// database frees its memtables through <see cref="Release"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">A holder still references the memtable.</exception>
    internal void FreeUnreferenced()
    {
        if (Volatile.Read(ref _references) != 0)
            throw new InvalidOperationException("The memtable is still referenced.");

        _arena.Free();
    }

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

    /// <summary>
    /// The bytes of the node of an entry of <paramref name="keyLength"/> and <paramref name="valueLength"/> bytes at
    /// <paramref name="height"/> levels, as the arena gives them: a multiple of 8, so every node's links are aligned.
    /// </summary>
    public static int NodeSize(int height, int keyLength, int valueLength) =>
        checked((LinksOffset + (height * sizeof(long)) + keyLength + valueLength + 7) & ~7);

    /// <summary>The bytes that one range tombstone adds to <see cref="ApproximateBytes"/>, its fragments aside.</summary>
    public static long TombstoneBytes(byte[] begin, byte[] end) => begin.Length + end.Length + 32;

    /// <summary>Insert one version of <paramref name="key"/>. One writer at a time, with no concurrent writer; the memtable copies the bytes.</summary>
    public void Add(byte[] key, long sequence, OperationKind kind, byte[]? value)
    {
        byte* x = _head;
        for (int level = Volatile.Read(ref _height) - 1; level >= 0; level--)
        {
            byte* next = (byte*)Link(x, level);
            while (next is not null && Compare(next, key, sequence) < 0)
            {
                x = next;
                next = (byte*)Link(x, level);
            }

            _splice[level] = (nint)x;
        }

        int height = RandomHeight();
        int current = _height;
        if (height > current)
        {
            for (int level = current; level < height; level++)
                _splice[level] = (nint)_head;

            // Published before the links: a reader that sees the new height finds null at the
            // new levels of the head and drops down.
            Volatile.Write(ref _height, height);
        }

        int size = NodeSize(height, key.Length, value?.Length ?? 0);
        byte* node = Build(_arena.Allocate(size), height, key, sequence, kind, value);
        for (int level = 0; level < height; level++)
        {
            // The node's bytes and its own link are written before the release-store that makes it reachable.
            Link(node, level) = Link((byte*)_splice[level], level);
            Volatile.Write(ref Link((byte*)_splice[level], level), (nint)node);
        }

        Volatile.Write(ref _approximateBytes, _approximateBytes + size);
        Volatile.Write(ref _entryCount, _entryCount + 1);
        NoteSequence(sequence);
        GC.KeepAlive(this);
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
        Volatile.Write(ref _approximateBytes, _approximateBytes + TombstoneBytes(begin, end) + next.FragmentBytes - current.FragmentBytes);
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
    /// the entry to the counters with <see cref="AddCounts"/>, with the node's <paramref name="bytes"/>, once for
    /// each run of entries, and no thread calls <see cref="Add"/> or <see cref="AddRangeTombstone"/> until every
    /// concurrent insert has returned. The memtable copies the bytes.
    /// </summary>
    /// <returns>How many times a link changed under the insert, so a level was searched again.</returns>
    public int AddConcurrently(byte[] key, long sequence, OperationKind kind, byte[]? value, out long bytes)
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
        byte* x = _head;
        for (int level = top - 1; level >= 0; level--)
        {
            x = FindSpliceForLevel(x, key, sequence, level, out next[level]);
            prev[level] = (nint)x;
        }

        int size = NodeSize(height, key.Length, value?.Length ?? 0);
        byte* node = Build(_arena.AllocateConcurrently(size), height, key, sequence, kind, value);
        int retries = 0;
        for (int level = 0; level < height; level++)
        {
            // The node's own link is written before the compare-and-swap that makes it reachable
            // at this level, a full fence. Linked from level 0 up, so a node that a reader reaches
            // at a level is linked at every level below it. Nodes are never removed, so prev is
            // still before the key when the swap fails, and the search goes on from it.
            while (true)
            {
                Link(node, level) = next[level];
                if (Interlocked.CompareExchange(ref Link((byte*)prev[level], level), (nint)node, next[level]) == next[level])
                    break;

                retries++;
                prev[level] = (nint)FindSpliceForLevel((byte*)prev[level], key, sequence, level, out next[level]);
            }
        }

        bytes = size;
        GC.KeepAlive(this);
        return retries;
    }

    /// <summary>
    /// Record a range delete of [<paramref name="begin"/>, <paramref name="end"/>) while other threads
    /// insert too. The caller adds it to the counters with <see cref="AddCounts"/>, with <see cref="TombstoneBytes"/>.
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
    /// Add a run of concurrent inserts to the counters: <paramref name="bytes"/> from
    /// <see cref="AddConcurrently"/> and <see cref="TombstoneBytes"/>, <paramref name="entries"/> point entries, and
    /// the sequence numbers from <paramref name="firstSequence"/> to <paramref name="lastSequence"/>. One atomic
    /// update for each counter and run, as RocksDB's <c>MemTable::BatchPostProcess</c>.
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
    private static byte* FindSpliceForLevel(byte* start, byte[] key, long sequence, int level, out nint next)
    {
        byte* x = start;
        next = Volatile.Read(ref Link(x, level));
        while (next != 0 && Compare((byte*)next, key, sequence) < 0)
        {
            x = (byte*)next;
            next = Volatile.Read(ref Link(x, level));
        }

        return x;
    }

    // The node's fields, key and value, written into its memory, which no reader can reach yet; its links are the caller's.
    private static byte* Build(byte* node, int height, byte[] key, long sequence, OperationKind kind, byte[]? value)
    {
        *(long*)(node + SequenceOffset) = sequence;
        *(int*)(node + KeyLengthOffset) = key.Length;
        *(int*)(node + ValueLengthOffset) = value?.Length ?? -1;
        node[KindOffset] = (byte)kind;
        node[HeightOffset] = (byte)height;
        byte* data = node + LinksOffset + (height * sizeof(long));
        key.CopyTo(new Span<byte>(data, key.Length));
        value?.CopyTo(new Span<byte>(data + key.Length, value.Length));
        return node;
    }

    // ── Reader side: any thread, no lock ─────────────────────────────────────────────────────────

    /// <inheritdoc />
    public bool TryGetNewest(ReadOnlySpan<byte> key, long snapshot, bool fillCache, out Entry entry)
    {
        byte* node = Seek(key, snapshot);
        bool found = node is not null && KeyOf(node).SequenceEqual(key);
        entry = found ? ToEntry(node) : default;
        GC.KeepAlive(this);
        return found;
    }

    /// <inheritdoc />
    public long CoveringTombstone(ReadOnlySpan<byte> key, long snapshot) => Volatile.Read(ref _tombstones).Covering(key, snapshot);

    /// <inheritdoc />
    public byte[]? LastKeyBefore(byte[]? limit, bool fillCache)
    {
        byte* node = limit is null ? Last() : FindLessThan(limit);
        byte[]? key = node is null ? null : KeyOf(node).ToArray();
        GC.KeepAlive(this);
        return key;
    }

    /// <inheritdoc />
    public IEntryCursor NewCursor(bool fillCache) => new Cursor(this);

    /// <summary>Every entry in internal key order, every version of each key included, when no writer is left.</summary>
    public IEnumerable<Entry> Entries()
    {
        // An iterator's body is safe code, so it walks the nodes by address.
        for (nint node = First(); node != 0; node = NextOf(node))
            yield return EntryOf(node);
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
        nint previous = 0;
        for (nint node = First(); node != 0; node = NextOf(node))
        {
            // The first version at or below the snapshot of each key is its newest.
            if (SequenceAt(node) > snapshot || (previous != 0 && SameKey(node, previous)))
                continue;

            previous = node;
            Entry entry = EntryOf(node);
            if (sweep.Covering(entry.Key) < entry.Sequence)
                yield return entry;
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
    private byte* Seek(ReadOnlySpan<byte> key, long sequence)
    {
        byte* x = _head;
        int level = Volatile.Read(ref _height) - 1;

        while (true)
        {
            byte* next = (byte*)Volatile.Read(ref Link(x, level));
            if (next is not null && Compare(next, key, sequence) < 0)
                x = next;
            else if (level == 0)
                return next;
            else
                level--;
        }
    }

    // The last entry whose key is less than key, or null.
    private byte* FindLessThan(ReadOnlySpan<byte> key)
    {
        byte* x = _head;
        int level = Volatile.Read(ref _height) - 1;

        while (true)
        {
            byte* next = (byte*)Volatile.Read(ref Link(x, level));
            if (next is not null && Compare(next, key, long.MaxValue) < 0)
                x = next;
            else if (level == 0)
                return x == _head ? null : x;
            else
                level--;
        }
    }

    private nint First() => Volatile.Read(ref Link(_head, 0));

    private byte* Last()
    {
        byte* x = _head;
        for (int level = Volatile.Read(ref _height) - 1; level >= 0; level--)
            for (byte* next = (byte*)Volatile.Read(ref Link(x, level)); next is not null; next = (byte*)Volatile.Read(ref Link(x, level)))
                x = next;

        return x == _head ? null : x;
    }

    // ── Nodes ────────────────────────────────────────────────────────────────────────────────────

    // The nodes by address, for the iterators.
    private static nint NextOf(nint node) => Volatile.Read(ref Link((byte*)node, 0));

    private static long SequenceAt(nint node) => SequenceOf((byte*)node);

    private static bool SameKey(nint node, nint other) => KeyOf((byte*)node).SequenceEqual(KeyOf((byte*)other));

    private static Entry EntryOf(nint node) => ToEntry((byte*)node);

    private static ref nint Link(byte* node, int level) => ref *(nint*)(node + LinksOffset + (level * sizeof(long)));

    private static long SequenceOf(byte* node) => *(long*)(node + SequenceOffset);

    private static ReadOnlySpan<byte> KeyOf(byte* node) =>
        new(node + LinksOffset + (node[HeightOffset] * sizeof(long)), *(int*)(node + KeyLengthOffset));

    // The value of a node, empty for none.
    private static ReadOnlySpan<byte> ValueOf(byte* node)
    {
        int valueLength = *(int*)(node + ValueLengthOffset);
        return valueLength <= 0
            ? default
            : new(node + LinksOffset + (node[HeightOffset] * sizeof(long)) + *(int*)(node + KeyLengthOffset), valueLength);
    }

    // The entry of a node, its key and value copied into new arrays.
    private static Entry ToEntry(byte* node)
    {
        int keyLength = *(int*)(node + KeyLengthOffset);
        int valueLength = *(int*)(node + ValueLengthOffset);
        byte* data = node + LinksOffset + (node[HeightOffset] * sizeof(long));
        byte[] key = new ReadOnlySpan<byte>(data, keyLength).ToArray();
        byte[]? value = valueLength < 0 ? null : new ReadOnlySpan<byte>(data + keyLength, valueLength).ToArray();
        return new Entry(key, SequenceOf(node), (OperationKind)node[KindOffset], value);
    }

    private static int Compare(byte* node, ReadOnlySpan<byte> key, long sequence) =>
        InternalKey.Compare(KeyOf(node), SequenceOf(node), key, sequence);

    private int RandomHeight() => RandomHeight(_random);

    private static int RandomHeight(Random random)
    {
        int height = 1;
        while (height < MaxHeight && random.Next(4) == 0)
            height++;

        return height;
    }

    [InlineArray(MaxHeight)]
    private struct Splice
    {
        private nint _element;
    }

    // Copies an entry out only when it is asked for, once per position: an iterator compares keys and sequence
    // numbers in place and takes the entry of the cursor that wins.
    private sealed class Cursor(MemTable table) : IEntryCursor
    {
        private nint _node;
        private Entry? _current;

        public bool Valid => _node != 0;

        public Entry Current
        {
            get
            {
                Entry entry = _current ??= ToEntry((byte*)_node);
                GC.KeepAlive(table);
                return entry;
            }
        }

        public ReadOnlySpan<byte> CurrentKey => KeyOf((byte*)_node);

        public long CurrentSequence
        {
            get
            {
                long sequence = SequenceOf((byte*)_node);
                GC.KeepAlive(table);
                return sequence;
            }
        }

        public OperationKind CurrentKind
        {
            get
            {
                OperationKind kind = (OperationKind)((byte*)_node)[KindOffset];
                GC.KeepAlive(table);
                return kind;
            }
        }

        public ReadOnlySpan<byte> CurrentValue => ValueOf((byte*)_node);

        public void SeekToFirst() => Move(table.First());

        public void Seek(ReadOnlySpan<byte> key) => Move((nint)table.Seek(key, long.MaxValue));

        public void Next() => Move(Volatile.Read(ref Link((byte*)_node, 0)));

        private void Move(nint node)
        {
            _node = node;
            _current = null;
            GC.KeepAlive(table);
        }
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
