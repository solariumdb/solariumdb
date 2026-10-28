namespace Solarium.Engine;

/// <summary>
/// Range tombstones cut at every begin and end key into fragments that do not overlap, in key order,
/// each with the sequence numbers of the tombstones over it, as RocksDB's
/// <c>FragmentedRangeTombstoneList</c>. The highest tombstone over a key at a snapshot is a binary
/// search for the key's fragment, then one among the fragment's sequence numbers; a plain list of
/// tombstones takes a scan of all of them. Never changed after it is built.
/// </summary>
/// <remarks>
/// A fragment keeps a sequence number for every tombstone over it, because a read at an older
/// snapshot needs the older ones. So tombstones that overlap many others cost memory and build time
/// in the product of the two counts, as RocksDB's do; the ranges of a compaction floor, which do not
/// overlap, cost one fragment each.
/// </remarks>
internal sealed class FragmentedTombstones
{
    /// <summary>The fragments of no tombstone.</summary>
    public static FragmentedTombstones Empty { get; } = new([], [], [0], []);

    private readonly byte[][] _starts;    // fragment i covers [_starts[i], _ends[i])
    private readonly byte[][] _ends;
    private readonly int[] _offsets;      // its sequence numbers: _sequences[_offsets[i].._offsets[i + 1]], ascending
    private readonly long[] _sequences;

    private FragmentedTombstones(byte[][] starts, byte[][] ends, int[] offsets, long[] sequences)
    {
        _starts = starts;
        _ends = ends;
        _offsets = offsets;
        _sequences = sequences;
    }

    /// <summary>The fragments, for tests.</summary>
    internal int FragmentCount => _starts.Length;

    /// <summary>The memory the fragments add to their tombstones: two key references and an offset per fragment, and the sequence numbers. The keys are the tombstones'.</summary>
    public long MemoryBytes => ((long)_starts.Length * ((2 * IntPtr.Size) + sizeof(int))) + ((long)_sequences.Length * sizeof(long));

    /// <summary>
    /// Fragments <paramref name="tombstones"/>, which may overlap, nest and repeat. Empty ranges, which
    /// cover no key, add nothing. The keys are shared with the tombstones, not copied.
    /// </summary>
    public static FragmentedTombstones Build(IReadOnlyList<RangeTombstone> tombstones)
    {
        if (tombstones.Count == 0)
            return Empty;

        // Every begin and end, sorted and unique: candidate fragment f is [bounds[f], bounds[f + 1]).
        byte[][] bounds = new byte[tombstones.Count * 2][];
        for (int i = 0; i < tombstones.Count; i++)
        {
            bounds[2 * i] = tombstones[i].Begin;
            bounds[(2 * i) + 1] = tombstones[i].End;
        }

        Array.Sort(bounds, static (a, b) => a.AsSpan().SequenceCompareTo(b));
        int unique = 0;
        for (int i = 0; i < bounds.Length; i++)
            if (unique == 0 || !bounds[unique - 1].AsSpan().SequenceEqual(bounds[i]))
                bounds[unique++] = bounds[i];

        // How many tombstones cover each candidate, by a difference array over their [first, last) spans.
        int candidates = unique - 1;
        int[] spanFirst = new int[tombstones.Count];
        int[] spanLast = new int[tombstones.Count];
        int[] cover = new int[Math.Max(candidates, 0) + 1];
        for (int i = 0; i < tombstones.Count; i++)
        {
            spanFirst[i] = IndexOf(bounds, unique, tombstones[i].Begin);
            spanLast[i] = IndexOf(bounds, unique, tombstones[i].End);
            if (spanFirst[i] >= spanLast[i])
                continue;

            cover[spanFirst[i]]++;
            cover[spanLast[i]]--;
        }

        // Keep the candidates that a tombstone covers, and lay out their sequence numbers.
        int[] fragmentOf = new int[Math.Max(candidates, 0)];
        List<int> offsets = [0];
        List<byte[]> starts = [];
        List<byte[]> ends = [];
        int covering = 0;
        long total = 0;
        for (int f = 0; f < candidates; f++)
        {
            covering += cover[f];
            if (covering == 0)
            {
                fragmentOf[f] = -1;
                continue;
            }

            fragmentOf[f] = starts.Count;
            starts.Add(bounds[f]);
            ends.Add(bounds[f + 1]);
            total += covering;
            if (total > Array.MaxLength)
                throw new InvalidOperationException($"{tombstones.Count} range tombstones overlap into more than {Array.MaxLength} fragment entries.");

            offsets.Add((int)total);
        }

        long[] sequences = new long[total];
        int[] next = [.. offsets];
        for (int i = 0; i < tombstones.Count; i++)
            for (int f = spanFirst[i]; f < spanLast[i]; f++)
                sequences[next[fragmentOf[f]]++] = tombstones[i].Sequence;

        for (int i = 0; i < starts.Count; i++)
            Array.Sort(sequences, offsets[i], offsets[i + 1] - offsets[i]);

        return new FragmentedTombstones([.. starts], [.. ends], [.. offsets], sequences);
    }

    /// <summary>
    /// The fragments in key order, each as one tombstone with the highest sequence number over it: what
    /// a read at the newest snapshot sees of the tombstones.
    /// </summary>
    public IEnumerable<RangeTombstone> Newest()
    {
        for (int i = 0; i < _starts.Length; i++)
            yield return new RangeTombstone(_starts[i], _ends[i], _sequences[_offsets[i + 1] - 1]);
    }

    /// <summary>The highest sequence number at or below <paramref name="snapshot"/> of a tombstone over <paramref name="key"/>, or 0.</summary>
    public long Covering(ReadOnlySpan<byte> key, long snapshot)
    {
        // The last fragment that starts at or before the key.
        int low = 0, high = _starts.Length;
        while (low < high)
        {
            int middle = (low + high) >>> 1;
            if (_starts[middle].AsSpan().SequenceCompareTo(key) <= 0)
                low = middle + 1;
            else
                high = middle;
        }

        int fragment = low - 1;
        if (fragment < 0 || key.SequenceCompareTo(_ends[fragment]) >= 0)
            return 0;

        int first = _offsets[fragment];
        int found = Array.BinarySearch(_sequences, first, _offsets[fragment + 1] - first, snapshot);
        if (found >= 0)
            return snapshot;

        int above = ~found;
        return above == first ? 0 : _sequences[above - 1];
    }

    // The index of key among the first count bounds, which hold it.
    private static int IndexOf(byte[][] bounds, int count, byte[] key)
    {
        int low = 0, high = count - 1;
        while (low <= high)
        {
            int middle = (low + high) >>> 1;
            int order = bounds[middle].AsSpan().SequenceCompareTo(key);
            if (order == 0)
                return middle;

            if (order < 0)
                low = middle + 1;
            else
                high = middle - 1;
        }

        throw new InvalidOperationException("A tombstone bound is missing from its own bounds.");
    }
}
