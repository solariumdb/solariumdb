using Solarium.Engine;

namespace Solarium;

/// <summary>
/// The sources of one column family at one moment, like RocksDB's <c>SuperVersion</c>: the active
/// memtable, the immutable memtables that wait for a flush, the SSTables of level 0, and the sorted
/// levels 1 to 6. Its sources never change after it is built, so a reader can hold it for as long as it needs.
/// </summary>
/// <remarks>
/// <para>A reader counts itself in with <see cref="Enter"/> and out with <see cref="Leave"/>, so a
/// table that a newer version no longer lists stays open while a reader of this version can still
/// read it. The counts are kept per CPU, each on its own cache line, so readers on different CPUs
/// do not contend; a reader leaves through the count it entered, from whatever CPU it is on.</para>
/// <para>A version replaced by a newer one is retired. It closes once it is retired and its counts
/// are all 0, and the first to see that, the installer or the last reader, closes it. Closing
/// releases the version's reference on each of its tables.</para>
/// </remarks>
internal sealed class FamilyVersion
{
    // 32 ints: 128 bytes, so neither a count's cache line nor the adjacent one the CPU prefetches
    // with it holds another count.
    private const int Stride = 32;
    private static readonly int SlotCount = Environment.ProcessorCount;

    private readonly int[] _readers = new int[SlotCount * Stride];   // Interlocked
    private readonly Action<TableReader> _unused;
    private int _retired;   // Interlocked: 1 once a newer version replaced this one
    private int _closed;    // Interlocked: 1 once the table references are released

    /// <summary>
    /// Builds a version and takes a reference on each of its tables. <paramref name="levels"/> has
    /// <see cref="Manifest.LevelCount"/> entries, indexed by level, with <see cref="SortedLevel.Empty"/>
    /// at index 0: level 0 is <paramref name="level0"/>. The arrays must not change afterwards.
    /// <paramref name="unused"/> is called, by the thread that closes a version, for each table whose
    /// last reference that close released.
    /// </summary>
    public FamilyVersion(MemTable active, MemTable[] immutables, TableReader[] level0, SortedLevel[] levels, Action<TableReader> unused)
    {
        if (levels.Length != Manifest.LevelCount || !levels[0].IsEmpty)
            throw new ArgumentException($"A version needs {Manifest.LevelCount} levels, with an empty level 0.", nameof(levels));

        Active = active;
        Immutables = immutables;
        Level0 = level0;
        Levels = levels;
        _unused = unused;

        List<IEntrySource> sources = new(1 + immutables.Length + level0.Length + Manifest.BottomLevel) { active };
        sources.AddRange(immutables);
        sources.AddRange(level0);
        for (int level = 1; level < levels.Length; level++)
            if (!levels[level].IsEmpty)
                sources.Add(levels[level]);

        Sources = [.. sources];

        foreach (TableReader table in Tables)
            table.AddReference();
    }

    /// <summary>The memtable that accepts writes.</summary>
    public MemTable Active { get; }

    /// <summary>The memtables that wait for a flush, newest first.</summary>
    public MemTable[] Immutables { get; }

    /// <summary>The SSTables of level 0, newest first. Their key ranges can overlap.</summary>
    public TableReader[] Level0 { get; }

    /// <summary>The sorted levels, indexed by level: index 0 is empty, and 1 to 6 hold each level's SSTables in key order, as one source.</summary>
    public SortedLevel[] Levels { get; }

    /// <summary>The bottom level, level 6.</summary>
    public SortedLevel Level6 => Levels[Manifest.BottomLevel];

    /// <summary>Every SSTable of the version: level 0 newest first, then each sorted level in key order, as the manifest lists them.</summary>
    public IEnumerable<TableReader> Tables
    {
        get
        {
            foreach (TableReader table in Level0)
                yield return table;

            for (int level = 1; level < Levels.Length; level++)
                foreach (TableReader table in Levels[level].Tables)
                    yield return table;
        }
    }

    /// <summary>
    /// Every source, newest first for each key: the order in which reads search them. A sorted level
    /// is left out when it has no files.
    /// </summary>
    public IEntrySource[] Sources { get; }

    /// <summary>
    /// Count a reader in on the current CPU's count. Returns the count to pass to
    /// <see cref="Leave"/>. A full fence: a read after it comes after the count.
    /// </summary>
    public int Enter()
    {
        int slot = Thread.GetCurrentProcessorId() % SlotCount * Stride;
        Interlocked.Increment(ref _readers[slot]);
        return slot;
    }

    /// <summary>Count a reader out of the count <paramref name="slot"/> that <see cref="Enter"/> returned.</summary>
    public void Leave(int slot)
    {
        // The decrement is a full fence, and Retire sets the flag with one before it reads the
        // counts: either this read sees the flag, or Retire's read sees the decrement.
        Interlocked.Decrement(ref _readers[slot]);
        if (Volatile.Read(ref _retired) != 0)
            CloseIfUnused();
    }

    /// <summary>
    /// Mark the version replaced, after the newer one is installed. It closes now if no reader is
    /// in it, or when its last reader leaves.
    /// </summary>
    public void Retire()
    {
        Interlocked.Exchange(ref _retired, 1);
        CloseIfUnused();
    }

    private void CloseIfUnused()
    {
        for (int slot = 0; slot < _readers.Length; slot += Stride)
            if (Volatile.Read(ref _readers[slot]) != 0)
                return;

        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;

        foreach (TableReader table in Tables)
            if (table.Release())
                _unused(table);
    }
}
