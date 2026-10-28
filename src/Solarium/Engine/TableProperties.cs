namespace Solarium.Engine;

/// <summary>
/// What the manifest records about one SSTable. Its key range runs from <see cref="Smallest"/>, included,
/// to <see cref="Largest"/>, which is included unless <see cref="LargestIsExclusive"/>: the largest bound
/// is then the end of a range tombstone, which covers keys below it only, and no entry of the table has
/// that key. Two tables of a sorted level can then meet at one key without overlapping, as RocksDB's
/// files meet at a range tombstone's sentinel key.
/// </summary>
internal sealed record TableProperties(long Size, long EntryCount, long MinSequence, long MaxSequence, byte[] Smallest, byte[] Largest,
    bool LargestIsExclusive)
{
    /// <summary>
    /// Whether a table whose last entry has the user key <paramref name="lastKey"/> (null: no entry) and
    /// whose largest bound is <paramref name="largest"/> ends below it: the bound is then a range
    /// tombstone's end.
    /// </summary>
    public static bool IsExclusive(byte[]? lastKey, byte[] largest) => lastKey is null || lastKey.AsSpan().SequenceCompareTo(largest) < 0;

    /// <summary>True when every key of the table, and every key its tombstones cover, is below <paramref name="key"/>.</summary>
    public bool EndsBefore(ReadOnlySpan<byte> key)
    {
        int order = Largest.AsSpan().SequenceCompareTo(key);
        return order < 0 || (order == 0 && LargestIsExclusive);
    }

    /// <summary>True when the key range of the table holds <paramref name="key"/>.</summary>
    public bool Contains(ReadOnlySpan<byte> key) => Smallest.AsSpan().SequenceCompareTo(key) <= 0 && !EndsBefore(key);

    /// <summary>True when the key ranges of this table and <paramref name="other"/> share a key.</summary>
    public bool Overlaps(TableProperties other) => !EndsBefore(other.Smallest) && !other.EndsBefore(Smallest);

    /// <summary>True when the key range of the table shares a key with [<paramref name="begin"/>, <paramref name="end"/>), a range tombstone's range.</summary>
    public bool OverlapsRange(ReadOnlySpan<byte> begin, ReadOnlySpan<byte> end) => Smallest.AsSpan().SequenceCompareTo(end) < 0 && !EndsBefore(begin);
}
