namespace Solarium.Engine;

/// <summary>One live SSTable as the manifest records it.</summary>
internal sealed class ManifestTable
{
    /// <summary>The file number of the SSTable.</summary>
    public long Number { get; init; }

    /// <summary>The level the SSTable belongs to.</summary>
    public int Level { get; init; }

    /// <summary>The file size in bytes.</summary>
    public long Size { get; init; }

    /// <summary>The number of entries in the table.</summary>
    public long EntryCount { get; init; }

    /// <summary>The lowest sequence number in the table.</summary>
    public long MinSequence { get; init; }

    /// <summary>The highest sequence number in the table.</summary>
    public long MaxSequence { get; init; }

    /// <summary>The smallest user key in the table.</summary>
    public byte[] Smallest { get; init; } = [];

    /// <summary>The largest user key in the table, or the end of a range tombstone (see <see cref="LargestIsExclusive"/>).</summary>
    public byte[] Largest { get; init; } = [];

    /// <summary>True when <see cref="Largest"/> is a range tombstone's end that no entry of the table has, as <see cref="TableProperties.LargestIsExclusive"/>.</summary>
    public bool LargestIsExclusive { get; init; }

    /// <summary>Describes the table <paramref name="number"/> at <paramref name="level"/> from the properties its builder returned.</summary>
    public static ManifestTable From(long number, int level, TableProperties properties) => new()
    {
        Number = number,
        Level = level,
        Size = properties.Size,
        EntryCount = properties.EntryCount,
        MinSequence = properties.MinSequence,
        MaxSequence = properties.MaxSequence,
        Smallest = properties.Smallest,
        Largest = properties.Largest,
        LargestIsExclusive = properties.LargestIsExclusive,
    };

    /// <summary>True when every key in the range of the table is below the smallest key of <paramref name="next"/>.</summary>
    public bool EndsBefore(ManifestTable next)
    {
        int order = Largest.AsSpan().SequenceCompareTo(next.Smallest);
        return order < 0 || (order == 0 && LargestIsExclusive);
    }
}
