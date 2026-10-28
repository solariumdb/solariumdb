namespace Solarium.Engine;

/// <summary>A <c>DeleteRange</c>: hides every version older than it of the keys in [Begin, End).</summary>
internal readonly record struct RangeTombstone(byte[] Begin, byte[] End, long Sequence)
{
    /// <summary>
    /// True when this tombstone is visible at <paramref name="snapshot"/> and <paramref name="key"/>
    /// lies in [Begin, End).
    /// </summary>
    public bool Covers(ReadOnlySpan<byte> key, long snapshot)
    {
        return Sequence <= snapshot && key.SequenceCompareTo(Begin) >= 0 && key.SequenceCompareTo(End) < 0;
    }
}
