namespace Solarium.Engine;

/// <summary>
/// The SSTables of one sorted level, 1 to 6, as one source, as RocksDB's <c>LevelIterator</c>: the files
/// are in key order and no two of them share a user key, so a lookup searches the one file whose key
/// range holds the key. Never changed after it is built.
/// </summary>
internal sealed class SortedLevel : IEntrySource
{
    /// <summary>A level with no files.</summary>
    public static SortedLevel Empty { get; } = new([]);

    private readonly TableReader[] _tables;

    /// <summary>
    /// Builds a level over <paramref name="tables"/>, which must be in key order with ranges that do
    /// not overlap (<see cref="TableProperties.Overlaps"/>), and must not change afterwards.
    /// </summary>
    public SortedLevel(TableReader[] tables)
    {
        _tables = tables;
        foreach (TableReader table in tables)
            Size += table.Properties.Size;
    }

    /// <summary>The files of the level, in key order.</summary>
    public TableReader[] Tables => _tables;

    /// <summary>True when the level has no files.</summary>
    public bool IsEmpty => _tables.Length == 0;

    /// <summary>The bytes of the files of the level.</summary>
    public long Size { get; }

    /// <summary>
    /// True when the key range of a file of the level holds <paramref name="key"/>: the level may have
    /// a version of it, as RocksDB's <c>KeyNotExistsBeyondOutputLevel</c> asks.
    /// </summary>
    public bool MayContain(ReadOnlySpan<byte> key) => TableFor(key) is not null;

    /// <summary>True when the key range of a file of the level shares a key with [<paramref name="begin"/>, <paramref name="end"/>).</summary>
    public bool OverlapsRange(ReadOnlySpan<byte> begin, ReadOnlySpan<byte> end)
    {
        int first = FirstNotBefore(_tables, begin);
        return first < _tables.Length && _tables[first].Properties.OverlapsRange(begin, end);
    }

    /// <inheritdoc />
    public bool TryGetNewest(ReadOnlySpan<byte> key, long snapshot, bool fillCache, out Entry entry)
    {
        TableReader? table = TableFor(key);
        if (table is null)
        {
            entry = default;
            return false;
        }

        return table.TryGetNewest(key, snapshot, fillCache, out entry);
    }

    /// <inheritdoc />
    /// <remarks>
    /// A file's key range covers its range tombstones, so only the file whose range holds the key can
    /// have a tombstone that covers it.
    /// </remarks>
    public long CoveringTombstone(ReadOnlySpan<byte> key, long snapshot) => TableFor(key)?.CoveringTombstone(key, snapshot) ?? 0;

    /// <inheritdoc />
    public byte[]? LastKeyBefore(byte[]? limit, bool fillCache)
    {
        // The last file that starts below the limit, or an earlier one: a file whose only entries
        // below the limit are range tombstones has no key to give.
        int index = limit is null ? _tables.Length : FirstAtOrAfter(limit);
        for (int i = index - 1; i >= 0; i--)
        {
            byte[]? key = _tables[i].LastKeyBefore(limit, fillCache);
            if (key is not null)
                return key;
        }

        return null;
    }

    /// <inheritdoc />
    public IEntryCursor NewCursor(bool fillCache) => new Cursor(this, fillCache);

    /// <summary>The file whose key range holds <paramref name="key"/>, the only one that can have a version of it; null when none does.</summary>
    public TableReader? TableFor(ReadOnlySpan<byte> key)
    {
        int index = FirstNotBefore(_tables, key);
        return index < _tables.Length && _tables[index].Properties.Smallest.AsSpan().SequenceCompareTo(key) <= 0 ? _tables[index] : null;
    }

    // The index of the first file that does not end before key: every earlier file ends below it.
    private static int FirstNotBefore(TableReader[] tables, ReadOnlySpan<byte> key)
    {
        int low = 0, high = tables.Length;
        while (low < high)
        {
            int middle = (low + high) / 2;
            if (tables[middle].Properties.EndsBefore(key))
                low = middle + 1;
            else
                high = middle;
        }

        return low;
    }

    // The index of the first file whose smallest key is at or after limit.
    private int FirstAtOrAfter(ReadOnlySpan<byte> limit)
    {
        int low = 0, high = _tables.Length;
        while (low < high)
        {
            int middle = (low + high) / 2;
            if (_tables[middle].Properties.Smallest.AsSpan().SequenceCompareTo(limit) < 0)
                low = middle + 1;
            else
                high = middle;
        }

        return low;
    }

    // One file's cursor at a time; it moves to the next file when the current one ends.
    private sealed class Cursor(SortedLevel level, bool fillCache) : IEntryCursor
    {
        private IEntryCursor? _current;
        private int _file;

        public bool Valid => _current is { Valid: true };

        public Entry Current => _current!.Current;

        public void SeekToFirst()
        {
            Open(0);
            _current?.SeekToFirst();
            SkipEmptyFiles();
        }

        public void Seek(ReadOnlySpan<byte> key)
        {
            Open(FirstNotBefore(level._tables, key));
            _current?.Seek(key);
            SkipEmptyFiles();
        }

        public void Next()
        {
            _current!.Next();
            SkipEmptyFiles();
        }

        // Past the end of a file, the next file's first entry.
        private void SkipEmptyFiles()
        {
            while (_current is { Valid: false } && _file + 1 < level._tables.Length)
            {
                Open(_file + 1);
                _current!.SeekToFirst();
            }
        }

        private void Open(int file)
        {
            _file = file;
            _current = file < level._tables.Length ? level._tables[file].NewCursor(fillCache) : null;
        }
    }
}
