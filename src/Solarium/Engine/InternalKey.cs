namespace Solarium.Engine;

/// <summary>
/// Internal key order: user key ascending, then sequence number descending, so the newest version
/// of a key comes first.
/// </summary>
internal static class InternalKey
{
    /// <summary>Negative when (keyA, sequenceA) sorts before (keyB, sequenceB).</summary>
    public static int Compare(ReadOnlySpan<byte> keyA, long sequenceA, ReadOnlySpan<byte> keyB, long sequenceB)
    {
        int order = keyA.SequenceCompareTo(keyB);
        return order != 0 ? order : sequenceB.CompareTo(sequenceA);
    }
}
