namespace Solarium.Engine;

/// <summary>
/// The decoded index block of an SSTable: for each data block, its last internal key and its handle.
/// The last keys are one contiguous array with an array of their offsets, so a table's index is a
/// handful of objects however many blocks it has, where one array per key made a long-lived object
/// for every 4 KB of data. Never changed after it is built.
/// </summary>
internal sealed class TableIndex
{
    // The last sequence number, offset and size that the index keeps beside each last key.
    private const int HandleBytes = sizeof(long) + sizeof(long) + sizeof(int);

    private readonly byte[] _keys;
    private readonly int[] _keyStarts;   // Count + 1 entries: key i is _keys[_keyStarts[i].._keyStarts[i + 1]]
    private readonly long[] _lastSequences;
    private readonly long[] _offsets;
    private readonly int[] _sizes;

    /// <summary>
    /// Builds an index over <paramref name="keys"/>, the last keys laid end to end, where key <c>i</c> starts at
    /// <c>keyStarts[i]</c> and ends where key <c>i + 1</c> starts; <paramref name="keyStarts"/> has one entry more
    /// than there are blocks. The arrays become the index's.
    /// </summary>
    public TableIndex(byte[] keys, int[] keyStarts, long[] lastSequences, long[] offsets, int[] sizes)
    {
        _keys = keys;
        _keyStarts = keyStarts;
        _lastSequences = lastSequences;
        _offsets = offsets;
        _sizes = sizes;
    }

    /// <summary>The number of data blocks.</summary>
    public int Count => _sizes.Length;

    /// <summary>
    /// The bytes the index keeps, as <c>rocksdb.estimate-table-readers-mem</c> counts them: each last key
    /// and its handle.
    /// </summary>
    public long KeyAndHandleBytes => _keys.Length + ((long)Count * HandleBytes);

    /// <summary>The bytes of the index's arrays, without their object headers: what a cache charges for it.</summary>
    public long ArrayBytes => _keys.Length + ((long)_keyStarts.Length * sizeof(int)) + ((long)Count * HandleBytes);

    /// <summary>The last user key of block <paramref name="block"/>.</summary>
    public ReadOnlySpan<byte> LastKey(int block) => _keys.AsSpan(_keyStarts[block], _keyStarts[block + 1] - _keyStarts[block]);

    /// <summary>The sequence number of the last internal key of block <paramref name="block"/>.</summary>
    public long LastSequence(int block) => _lastSequences[block];

    /// <summary>The offset of block <paramref name="block"/> in the file.</summary>
    public long Offset(int block) => _offsets[block];

    /// <summary>The size of block <paramref name="block"/> in the file, its trailer included.</summary>
    public int Size(int block) => _sizes[block];

    /// <summary>The first block whose last internal key is at or after (<paramref name="key"/>, <paramref name="sequence"/>), or <see cref="Count"/>.</summary>
    public int FindBlock(ReadOnlySpan<byte> key, long sequence)
    {
        int low = 0, high = Count;
        while (low < high)
        {
            int middle = (low + high) / 2;
            if (InternalKey.Compare(LastKey(middle), _lastSequences[middle], key, sequence) < 0)
                low = middle + 1;
            else
                high = middle;
        }

        return low;
    }

    /// <summary>The first block whose last user key is at or after <paramref name="limit"/>, or <see cref="Count"/>.</summary>
    public int FindBlockAtOrAfter(ReadOnlySpan<byte> limit)
    {
        int low = 0, high = Count;
        while (low < high)
        {
            int middle = (low + high) / 2;
            if (LastKey(middle).SequenceCompareTo(limit) < 0)
                low = middle + 1;
            else
                high = middle;
        }

        return low;
    }
}
