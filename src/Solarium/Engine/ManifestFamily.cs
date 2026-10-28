namespace Solarium.Engine;

/// <summary>One column family as the manifest records it.</summary>
internal sealed class ManifestFamily
{
    /// <summary>The column family id, unique and non-negative.</summary>
    public int Id { get; init; }

    /// <summary>The column family name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Every write to this family at or below this sequence number is in an SSTable; WAL replay skips it.</summary>
    public long FlushedSequence { get; set; }

    /// <summary>The live SSTables of this family: level 0 newest first, then each of levels 1 to 6 in key order.</summary>
    public List<ManifestTable> Tables { get; } = [];
}
