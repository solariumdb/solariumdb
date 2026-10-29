using Solarium.Engine;

namespace Solarium;

/// <summary>
/// An ordered cursor over one column family, at the snapshot taken when it was created: writes
/// after that are not visible to it, and neither are the flushes and compactions after it. Use one
/// iterator from one thread at a time, and dispose it: until then it keeps the SSTables of its
/// snapshot open, as RocksDB's iterators keep their <c>SuperVersion</c>.
/// </summary>
/// <remarks>
/// <para>Each position is the newest version of a key at or below the snapshot, when that version is a
/// put that no newer range tombstone covers, in any source.</para>
/// <para>Forward moves merge one cursor per source (memtables and SSTables): each step takes the
/// smallest key under any cursor and moves every cursor past it. Backward moves ask each source for its
/// largest key below the current one and look the winner up, so a step costs a search in every source.</para>
/// </remarks>
public sealed class Iterator : IDisposable
{
    private readonly FamilyVersion _version;
    private readonly int _slot;
    private readonly IEntrySource[] _sources;
    private readonly IEntryCursor[] _cursors;
    private readonly long _snapshot;
    private readonly byte[]? _upperBound;
    private readonly bool _fillCache;
    // The current key and value, copied into buffers of the iterator's own that it reuses, so a step allocates
    // nothing and a span that Key or Value returned never points into a source's memory.
    private byte[] _key = new byte[64];
    private byte[] _value = new byte[256];
    private int _keyLength;
    private int _valueLength;
    private bool _valid;

    // False after a backward move: the cursors no longer sit at the current key.
    private bool _cursorsAtKey;
    private bool _disposed;

    /// <summary>
    /// Creates an iterator over <paramref name="version"/>, which the caller entered with count
    /// <paramref name="slot"/>, that sees writes up to <paramref name="snapshot"/> and stops before
    /// <paramref name="upperBound"/> (null: no bound), and caches the index and filter it reads again only with
    /// <paramref name="fillCache"/>. The iterator leaves the version at <see cref="Dispose"/>.
    /// </summary>
    internal Iterator(FamilyVersion version, int slot, long snapshot, byte[]? upperBound, bool fillCache)
    {
        _version = version;
        _slot = slot;
        _sources = version.Sources;
        _cursors = new IEntryCursor[_sources.Length];
        for (int i = 0; i < _sources.Length; i++)
            _cursors[i] = _sources[i].NewCursor(fillCache);

        _snapshot = snapshot;
        _upperBound = upperBound;
        _fillCache = fillCache;
    }

    /// <summary>True when the iterator is positioned on a key.</summary>
    public bool Valid() => _valid;

    /// <summary>
    /// The current key, valid until the iterator moves. Throws <see cref="InvalidOperationException"/> when the
    /// iterator is not <see cref="Valid"/>.
    /// </summary>
    public ReadOnlySpan<byte> Key() => _valid ? CurrentKey : throw NotPositioned();

    /// <summary>
    /// The current value, valid until the iterator moves. Throws <see cref="InvalidOperationException"/> when the
    /// iterator is not <see cref="Valid"/>.
    /// </summary>
    public ReadOnlySpan<byte> Value() => _valid ? _value.AsSpan(0, _valueLength) : throw NotPositioned();

    private ReadOnlySpan<byte> CurrentKey => _key.AsSpan(0, _keyLength);

    /// <summary>Move to the first key, or become invalid when there is none. Throws <see cref="ObjectDisposedException"/> after <see cref="Dispose"/>.</summary>
    public void SeekToFirst()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        foreach (IEntryCursor cursor in _cursors)
            cursor.SeekToFirst();

        _cursorsAtKey = true;
        FindForward();
    }

    /// <summary>Move to the first key at or after <paramref name="target"/>. Throws <see cref="ObjectDisposedException"/> after <see cref="Dispose"/>.</summary>
    public void Seek(ReadOnlySpan<byte> target)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        foreach (IEntryCursor cursor in _cursors)
            cursor.Seek(target);

        _cursorsAtKey = true;
        FindForward();
    }

    /// <summary>Move to the last key, or to the last key before the upper bound when there is one. Throws <see cref="ObjectDisposedException"/> after <see cref="Dispose"/>.</summary>
    public void SeekToLast()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        FindBackward(_upperBound);
    }

    /// <summary>Move to the next key. Throws <see cref="InvalidOperationException"/> when the iterator is not <see cref="Valid"/>.</summary>
    public void Next()
    {
        if (!_valid)
            throw NotPositioned();

        if (!_cursorsAtKey)
        {
            foreach (IEntryCursor cursor in _cursors)
                cursor.Seek(CurrentKey);

            _cursorsAtKey = true;
        }

        SkipPast(CurrentKey);
        FindForward();
    }

    /// <summary>Move to the previous key. Throws <see cref="InvalidOperationException"/> when the iterator is not <see cref="Valid"/>.</summary>
    public void Prev() => FindBackward(_valid ? CurrentKey.ToArray() : throw NotPositioned());

    /// <summary>Invalidate the iterator and release its snapshot's SSTables. Later calls do nothing.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _valid = false;
        _version.Leave(_slot);
    }

    // The first visible key at or after the cursors. Keys, sequence numbers and values are read in place; only the
    // key and the value of the position are copied, into the iterator's buffers.
    private void FindForward()
    {
        while (true)
        {
            IEntryCursor? smallest = null;
            foreach (IEntryCursor cursor in _cursors)
                if (cursor.Valid && (smallest is null || cursor.CurrentKey.SequenceCompareTo(smallest.CurrentKey) < 0))
                    smallest = cursor;

            if (smallest is null || (_upperBound is not null && smallest.CurrentKey.SequenceCompareTo(_upperBound) >= 0))
            {
                _valid = false;
                GC.KeepAlive(this);
                return;
            }

            Copy(smallest.CurrentKey, ref _key, out _keyLength);
            ReadOnlySpan<byte> key = CurrentKey;

            // The newest version in the snapshot, from the newest source that has one.
            IEntryCursor? newest = null;
            foreach (IEntryCursor cursor in _cursors)
            {
                while (cursor.Valid && cursor.CurrentSequence > _snapshot && cursor.CurrentKey.SequenceEqual(key))
                    cursor.Next();

                if (newest is null && cursor.Valid && cursor.CurrentKey.SequenceEqual(key))
                    newest = cursor;
            }

            if (newest is { CurrentKind: OperationKind.Put } && ReadPath.CoveringTombstone(_sources, key, _snapshot) < newest.CurrentSequence)
            {
                Copy(newest.CurrentValue, ref _value, out _valueLength);
                _valid = true;
                GC.KeepAlive(this);
                return;
            }

            SkipPast(key);
        }
    }

    // The last visible key before limit (null: no limit).
    private void FindBackward(byte[]? limit)
    {
        _cursorsAtKey = false;
        while (true)
        {
            byte[]? key = null;
            foreach (IEntrySource source in _sources)
            {
                byte[]? candidate = source.LastKeyBefore(limit, _fillCache);
                if (candidate is not null && (key is null || candidate.AsSpan().SequenceCompareTo(key) > 0))
                    key = candidate;
            }

            if (key is null)
            {
                _valid = false;
                return;
            }

            if (ReadPath.TryGet(_sources, key, _snapshot, _fillCache, out byte[]? value))
            {
                Copy(key, ref _key, out _keyLength);
                Copy(value, ref _value, out _valueLength);
                _valid = true;
                return;
            }

            limit = key;
        }
    }

    private void SkipPast(ReadOnlySpan<byte> key)
    {
        foreach (IEntryCursor cursor in _cursors)
        {
            while (cursor.Valid && cursor.CurrentKey.SequenceEqual(key))
                cursor.Next();
        }

        // The cursors' keys are read in place: a memtable's stay valid only while the iterator keeps its version.
        GC.KeepAlive(this);
    }

    // Copies bytes into one of the iterator's buffers, which grows to fit them.
    private static void Copy(ReadOnlySpan<byte> bytes, ref byte[] buffer, out int length)
    {
        if (buffer.Length < bytes.Length)
            buffer = new byte[Math.Max(bytes.Length, 2 * buffer.Length)];

        bytes.CopyTo(buffer);
        length = bytes.Length;
    }

    private static InvalidOperationException NotPositioned() => new("The iterator is not positioned on a key.");
}
