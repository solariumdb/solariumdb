namespace Solarium.Engine;

/// <summary>
/// The range tombstones that cover each key, for keys that arrive in ascending order: tombstones
/// join when the keys reach their begin and leave when the keys reach their end.
/// </summary>
internal sealed class TombstoneSweep
{
    private readonly RangeTombstone[] _byBegin;
    private readonly List<RangeTombstone> _active = [];
    private int _next;

    /// <summary>Prepares a sweep over <paramref name="tombstones"/>, which it copies.</summary>
    public TombstoneSweep(IEnumerable<RangeTombstone> tombstones)
    {
        _byBegin = [.. tombstones];
        Array.Sort(_byBegin, (a, b) => a.Begin.AsSpan().SequenceCompareTo(b.Begin));
    }

    /// <summary>
    /// The highest sequence number of a tombstone that covers <paramref name="key"/>, or 0. Each key
    /// must be at or after the key of the call before.
    /// </summary>
    public long Covering(byte[] key)
    {
        while (_next < _byBegin.Length && _byBegin[_next].Begin.AsSpan().SequenceCompareTo(key) <= 0)
            _active.Add(_byBegin[_next++]);

        long highest = 0;
        for (int i = _active.Count - 1; i >= 0; i--)
        {
            if (_active[i].End.AsSpan().SequenceCompareTo(key) <= 0)
                _active.RemoveAt(i);
            else
                highest = Math.Max(highest, _active[i].Sequence);
        }

        return highest;
    }
}
