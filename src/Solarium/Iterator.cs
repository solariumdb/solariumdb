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
    private byte[]? _key;
    private byte[]? _value;

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
    public bool Valid() => _key is not null;

    /// <summary>The current key. Throws <see cref="InvalidOperationException"/> when the iterator is not <see cref="Valid"/>.</summary>
    public ReadOnlySpan<byte> Key() => _key ?? throw NotPositioned();

    /// <summary>The current value. Throws <see cref="InvalidOperationException"/> when the iterator is not <see cref="Valid"/>.</summary>
    public ReadOnlySpan<byte> Value() => _key is not null ? _value : throw NotPositioned();

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
        byte[] key = _key ?? throw NotPositioned();
        if (!_cursorsAtKey)
        {
            foreach (IEntryCursor cursor in _cursors)
                cursor.Seek(key);

            _cursorsAtKey = true;
        }

        SkipPast(key);
        FindForward();
    }

    /// <summary>Move to the previous key. Throws <see cref="InvalidOperationException"/> when the iterator is not <see cref="Valid"/>.</summary>
    public void Prev() => FindBackward(_key ?? throw NotPositioned());

    /// <summary>Invalidate the iterator and release its snapshot's SSTables. Later calls do nothing.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _key = null;
        _version.Leave(_slot);
    }

    // The first visible key at or after the cursors.
    private void FindForward()
    {
        while (true)
        {
            byte[]? key = null;
            foreach (IEntryCursor cursor in _cursors)
                if (cursor.Valid && (key is null || cursor.Current.Key.AsSpan().SequenceCompareTo(key) < 0))
                    key = cursor.Current.Key;

            if (key is null || (_upperBound is not null && key.AsSpan().SequenceCompareTo(_upperBound) >= 0))
            {
                _key = null;
                return;
            }

            // The newest version in the snapshot, from the newest source that has one.
            Entry? newest = null;
            foreach (IEntryCursor cursor in _cursors)
            {
                while (cursor.Valid && cursor.Current.Sequence > _snapshot && cursor.Current.Key.AsSpan().SequenceEqual(key))
                    cursor.Next();

                if (newest is null && cursor.Valid && cursor.Current.Key.AsSpan().SequenceEqual(key))
                    newest = cursor.Current;
            }

            if (newest is { Kind: OperationKind.Put } entry && ReadPath.CoveringTombstone(_sources, key, _snapshot) < entry.Sequence)
            {
                _key = key;
                _value = entry.Value;
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
                _key = null;
                return;
            }

            if (ReadPath.TryGet(_sources, key, _snapshot, _fillCache, out byte[]? value))
            {
                _key = key;
                _value = value;
                return;
            }

            limit = key;
        }
    }

    private void SkipPast(byte[] key)
    {
        foreach (IEntryCursor cursor in _cursors)
        {
            while (cursor.Valid && cursor.Current.Key.AsSpan().SequenceEqual(key))
                cursor.Next();
        }
    }

    private static InvalidOperationException NotPositioned() => new("The iterator is not positioned on a key.");
}
