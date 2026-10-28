namespace Solarium.Engine;

/// <summary>
/// The range tombstones that a merge's output keeps, handed out to the output tables in key order. The
/// inputs' tombstones are fragmented, each fragment keeps only the newest sequence number over it, and a
/// fragment is kept only while a file of a level under the output overlaps it: elsewhere it hides
/// nothing that the merge does not drop itself.
/// </summary>
/// <remarks>
/// One sequence number per fragment is enough: every reader of the output reads at a snapshot at or
/// after the merge's newest entry, and the merge drops every entry of the inputs that a fragment hides,
/// so the older tombstones under a fragment hide nothing the newest does not.
/// </remarks>
internal sealed class KeptTombstones
{
    private readonly List<RangeTombstone> _kept = [];
    private int _next;

    /// <summary>The fragments of <paramref name="tombstones"/> that a file of a level in <paramref name="below"/> overlaps.</summary>
    public KeptTombstones(List<RangeTombstone> tombstones, SortedLevel[] below)
    {
        if (below.Length == 0 || tombstones.Count == 0)
            return;

        foreach (RangeTombstone fragment in FragmentedTombstones.Build(tombstones).Newest())
        {
            if (!OverlapsAny(below, fragment))
                continue;

            // Neighbours with one sequence number become one tombstone.
            if (_kept.Count > 0 && _kept[^1].Sequence == fragment.Sequence && _kept[^1].End.AsSpan().SequenceEqual(fragment.Begin))
                _kept[^1] = _kept[^1] with { End = fragment.End };
            else
                _kept.Add(fragment);
        }
    }

    /// <summary>
    /// The kept tombstones below <paramref name="limit"/>, the first key of the next output table, with a
    /// tombstone that spans it cut there; the part from <paramref name="limit"/> on is left for the next
    /// call. Null: every tombstone that is left. Each limit must be above the one before.
    /// </summary>
    public List<RangeTombstone> Take(byte[]? limit)
    {
        List<RangeTombstone> taken = [];
        while (_next < _kept.Count)
        {
            RangeTombstone tombstone = _kept[_next];
            if (limit is not null && tombstone.Begin.AsSpan().SequenceCompareTo(limit) >= 0)
                break;

            if (limit is not null && tombstone.End.AsSpan().SequenceCompareTo(limit) > 0)
            {
                taken.Add(tombstone with { End = limit });
                _kept[_next] = tombstone with { Begin = limit };
                break;
            }

            taken.Add(tombstone);
            _next++;
        }

        return taken;
    }

    private static bool OverlapsAny(SortedLevel[] levels, RangeTombstone fragment)
    {
        foreach (SortedLevel level in levels)
            if (level.OverlapsRange(fragment.Begin, fragment.End))
                return true;

        return false;
    }
}
