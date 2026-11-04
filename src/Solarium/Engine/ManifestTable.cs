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

    /// <summary>The random id in the table's footer, or 0 for a table of a version before 5, which has none.</summary>
    public ulong Id { get; init; }

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
        Id = properties.Id,
    };

    /// <summary>
    /// What differs between this record and <paramref name="properties"/>, those of the table file opened under its
    /// number, or null when they match: the file the manifest recorded is the one in place. A table file that another,
    /// whole and valid, replaced differs in its id, or, for a table of a version before 5, in its size, its entries,
    /// its sequence numbers or its key range. A record whose <see cref="Id"/> is 0 vouches for no id: the manifest
    /// was written before it recorded them, or the table has none. Not compared: <see cref="LargestIsExclusive"/>,
    /// which a version 2 manifest does not record.
    /// </summary>
    public string? Mismatch(TableProperties properties)
    {
        if (Id != 0 && Id != properties.Id)
            return $"its table id is {properties.Id:x16}, and the manifest records {Id:x16}";

        if (Size != properties.Size)
            return $"it has {properties.Size} bytes, and the manifest records {Size}";

        if (EntryCount != properties.EntryCount)
            return $"it has {properties.EntryCount} entries, and the manifest records {EntryCount}";

        if (MinSequence != properties.MinSequence || MaxSequence != properties.MaxSequence)
            return $"its sequence numbers run from {properties.MinSequence} to {properties.MaxSequence}, and the manifest records {MinSequence} to {MaxSequence}";

        if (!Smallest.AsSpan().SequenceEqual(properties.Smallest) || !Largest.AsSpan().SequenceEqual(properties.Largest))
            return "its key range is not the one the manifest records";

        return null;
    }

    /// <summary>True when every key in the range of the table is below the smallest key of <paramref name="next"/>.</summary>
    public bool EndsBefore(ManifestTable next)
    {
        int order = Largest.AsSpan().SequenceCompareTo(next.Smallest);
        return order < 0 || (order == 0 && LargestIsExclusive);
    }
}
