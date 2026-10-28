namespace Solarium.Engine;

/// <summary>
/// One compaction of a column family from a start level into the level below it, or from level 0
/// into the base level: the tables it takes from the start level and the tables of the output level
/// in their key range. A move changes only the level of its start tables, which overlap no table of
/// the output level; a merge rewrites all its tables into new tables of the output level. Never
/// changed after it is built.
/// </summary>
internal sealed class Compaction
{
    /// <summary>
    /// Describes a compaction of <paramref name="family"/> from <paramref name="startLevel"/> into
    /// <paramref name="outputLevel"/>. <paramref name="inputs"/> are the start level's tables, newest
    /// first for level 0 and in key order otherwise, and <paramref name="outputLevelInputs"/> the
    /// output level's, in key order; a move has none of the latter. <paramref name="below"/> are the
    /// non-empty levels under the output level when the compaction was picked.
    /// </summary>
    public Compaction(ColumnFamily family, int startLevel, TableReader[] inputs, int outputLevel, TableReader[] outputLevelInputs, bool isMove,
        SortedLevel[] below)
    {
        Family = family;
        StartLevel = startLevel;
        Inputs = inputs;
        OutputLevel = outputLevel;
        OutputLevelInputs = outputLevelInputs;
        IsMove = isMove;
        Below = below;

        // An empty key is a key, so the first input sets both ends.
        bool first = true;
        foreach (TableReader table in AllInputs)
        {
            if (first || table.Properties.Smallest.AsSpan().SequenceCompareTo(Smallest) < 0)
                Smallest = table.Properties.Smallest;

            if (first || table.Properties.Largest.AsSpan().SequenceCompareTo(Largest) > 0)
                Largest = table.Properties.Largest;

            first = false;
        }
    }

    /// <summary>The family whose tables the compaction takes.</summary>
    public ColumnFamily Family { get; }

    /// <summary>The level the compaction takes <see cref="Inputs"/> from: 0, or 1 to 5.</summary>
    public int StartLevel { get; }

    /// <summary>The tables of the start level: newest first for level 0, in key order for a sorted level.</summary>
    public TableReader[] Inputs { get; }

    /// <summary>The level the new or moved tables go to, below <see cref="StartLevel"/>.</summary>
    public int OutputLevel { get; }

    /// <summary>The tables of the output level in the key range of <see cref="Inputs"/>, in key order.</summary>
    public TableReader[] OutputLevelInputs { get; }

    /// <summary>True when <see cref="Inputs"/> move to <see cref="OutputLevel"/> as they are, with no rewrite.</summary>
    public bool IsMove { get; }

    /// <summary>
    /// The non-empty levels under the output level, from when the compaction was picked: where a key
    /// can have older versions that a delete in the output must still hide. Other compactions and
    /// <see cref="SolariumDb.DeleteFilesInRange"/> can change them while the merge runs, but none can
    /// bring a key of this compaction's range into them: that key would come down from a file of the
    /// output level in the range, which this compaction holds, or from a file below that these levels
    /// already showed. So a change can make a kept delete needless, never a dropped one needed.
    /// </summary>
    public SortedLevel[] Below { get; }

    /// <summary>The smallest key of the inputs: with <see cref="Largest"/>, the key range the outputs fall in.</summary>
    public byte[] Smallest { get; } = [];

    /// <summary>The largest bound of the inputs, counted in.</summary>
    public byte[] Largest { get; } = [];

    /// <summary>Every table the compaction takes: <see cref="Inputs"/>, then <see cref="OutputLevelInputs"/>.</summary>
    public IEnumerable<TableReader> AllInputs => Inputs.Concat(OutputLevelInputs);

    /// <summary>True when <paramref name="table"/> is one of <see cref="AllInputs"/>.</summary>
    public bool Takes(TableReader table) => Array.IndexOf(Inputs, table) >= 0 || Array.IndexOf(OutputLevelInputs, table) >= 0;

    /// <summary>The size of every input table, in bytes.</summary>
    public long InputBytes
    {
        get
        {
            long total = 0;
            foreach (TableReader table in AllInputs)
                total += table.Properties.Size;

            return total;
        }
    }
}
