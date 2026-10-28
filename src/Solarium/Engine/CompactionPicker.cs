namespace Solarium.Engine;

/// <summary>
/// Chooses the tables of a compaction from a column family's current version, as RocksDB's
/// <c>LevelCompactionPicker</c>. A compaction into a level must take every table of that level that
/// overlaps its range, so the new tables share no key with the ones that stay, and must take every
/// older version of a key in the levels it reads: then the merge may drop what that version hides,
/// and every version of a key in a level stays newer than every one in the levels below.
/// </summary>
/// <remarks>
/// A range built from several tables is compared with both ends included, which can see an overlap at
/// a range tombstone's end where there is none; that only makes a compaction larger. The checks that
/// decide whether tables may share a sorted level use <see cref="TableProperties.Overlaps"/>, which
/// knows an end that is a tombstone's.
/// </remarks>
internal static class CompactionPicker
{
    /// <summary>
    /// The score of each level of <paramref name="family"/> that can compact, as RocksDB's
    /// <c>ComputeCompactionScore</c>: for level 0 its file count over
    /// <see cref="ColumnFamilyOptions.Level0FileNumCompactionTrigger"/>, then for each level from the
    /// base level to level 5 its bytes over its target. A score of at least 1 needs a compaction. The
    /// tables that <paramref name="running"/> compactions read do not count, as RocksDB's
    /// <c>level_bytes_no_compacting</c>. Under the database's state lock.
    /// </summary>
    public static IEnumerable<(int Level, double Score)> Scores(ColumnFamily family, LevelTargets targets, IReadOnlyList<Compaction> running)
    {
        FamilyVersion version = family.Version;
        int files = 0;
        foreach (TableReader table in version.Level0)
            if (!Taken(table, running))
                files++;

        yield return (0, (double)files / family.Options.Level0FileNumCompactionTrigger);
        for (int level = targets.BaseLevel; level < Manifest.BottomLevel; level++)
        {
            long size = 0;
            foreach (TableReader table in version.Levels[level].Tables)
                if (!Taken(table, running))
                    size += table.Properties.Size;

            yield return (level, (double)size / targets.MaxBytes(level));
        }
    }

    /// <summary>
    /// The level of <paramref name="family"/> with the highest of its <see cref="Scores"/>, ties to the
    /// higher level, and that score. Under the database's state lock.
    /// </summary>
    public static (int Level, double Score) BestLevel(ColumnFamily family, LevelTargets targets, IReadOnlyList<Compaction> running)
    {
        (int Level, double Score) best = (0, -1);
        foreach ((int level, double score) in Scores(family, targets, running))
            if (score > best.Score)
                best = (level, score);

        return best;
    }

    /// <summary>
    /// The compaction of <paramref name="level"/> of <paramref name="family"/>, which must have at
    /// least one table: level 0 into the base level of <paramref name="targets"/>, or a level from 1
    /// to 5 into the level below. Null when the <paramref name="running"/> compactions stand in its
    /// way, as RocksDB's picker: a level 0 compaction of the family runs, one of its tables is an input
    /// of a running compaction, or its key range overlaps the range of a running compaction into the
    /// same level, whose outputs would overlap its own. Under the database's state lock.
    /// </summary>
    public static Compaction? Pick(ColumnFamily family, int level, LevelTargets targets, IReadOnlyList<Compaction> running)
    {
        if (level != 0)
            return PickSorted(family, level, running);

        // Level 0's tables overlap each other, so two of its compactions could take a newer table
        // before an older one that overlaps it; RocksDB runs one at a time too. A level 0 compaction
        // always holds the oldest table, where the next pick would start, so the check of the inputs
        // below refuses a second one as well; this states the rule.
        foreach (Compaction other in running)
            if (other.Family == family && other.StartLevel == 0)
                return null;

        Compaction compaction = PickLevel0(family, targets.BaseLevel);
        foreach (TableReader table in compaction.AllInputs)
            if (Taken(table, running))
                return null;

        return OutputConflict(family, compaction.OutputLevel, compaction.Smallest, compaction.Largest, running) ? null : compaction;
    }

    /// <summary>
    /// The compaction of <paramref name="family"/>'s level 0, which must have at least one table, into
    /// <paramref name="baseLevel"/>: a move of the oldest tables when they overlap nothing, as RocksDB's
    /// <c>TryPickL0TrivialMove</c>, or else a merge from the oldest table. Under the database's state lock.
    /// </summary>
    public static Compaction PickLevel0(ColumnFamily family, int baseLevel)
    {
        FamilyVersion version = family.Version;
        TableReader[] level0 = version.Level0;
        TableReader[] output = version.Levels[baseLevel].Tables;

        // From the oldest table: each one taken overlaps no table taken before it and no table of the
        // base level. The first that does ends the walk, so every older table is taken too, and the
        // tables that stay in level 0 are newer than every table that moves.
        int first = level0.Length;
        while (first > 0)
        {
            TableProperties candidate = level0[first - 1].Properties;
            if (OverlapsAny(level0.AsSpan(first), candidate) || OverlapsAny(output, candidate))
                break;

            first--;
        }

        if (first < level0.Length)
            return new Compaction(family, 0, level0[first..], baseLevel, [], isMove: true, Below(version, baseLevel));

        // No level 0 table is older than the oldest, so none holds an older version of its keys. The
        // merge also takes every level 0 table that overlaps its range, closed over, as RocksDB does,
        // so one merge empties level 0 of that range.
        bool[] taken = new bool[level0.Length];
        taken[^1] = true;
        return CloseLevel0(family, version, taken, baseLevel);
    }

    /// <summary>
    /// The next compaction of a <see cref="SolariumDb.CompactRange"/> over [<paramref name="begin"/>,
    /// <paramref name="end"/>] (null: unbounded) of <paramref name="family"/>, from the first level at
    /// or after <paramref name="fromLevel"/> with a table in the range, as RocksDB's manual compaction
    /// runs one level after the other: level 0 into the base level, with every level 0 table that
    /// overlaps the range and every one that overlaps those, closed over; a sorted level into the
    /// level below, with every table of it in the range. The bottom level alone is not compacted, as
    /// RocksDB's <c>CompactRange</c> leaves it with no compaction filter. Returns null when no level
    /// from <paramref name="fromLevel"/> to 5 has a table in the range, and sets
    /// <paramref name="level"/> to the start level of the compaction. Under the database's state lock.
    /// </summary>
    public static Compaction? PickRange(ColumnFamily family, int fromLevel, byte[]? begin, byte[]? end, LevelTargets targets, out int level)
    {
        FamilyVersion version = family.Version;
        for (level = fromLevel; level < Manifest.BottomLevel; level++)
        {
            if (level == 0)
            {
                TableReader[] level0 = version.Level0;
                bool[] taken = new bool[level0.Length];
                bool any = false;
                for (int i = 0; i < level0.Length; i++)
                {
                    taken[i] = InRange(level0[i].Properties, begin, end);
                    any |= taken[i];
                }

                if (!any)
                    continue;

                Compaction merge = CloseLevel0(family, version, taken, targets.BaseLevel);
                return merge.OutputLevelInputs.Length == 0 && !OverlapEachOther(merge.Inputs)
                    ? new Compaction(family, 0, merge.Inputs, merge.OutputLevel, [], isMove: true, merge.Below)
                    : merge;
            }

            List<TableReader> inputs = [];
            foreach (TableReader table in version.Levels[level].Tables)
                if (InRange(table.Properties, begin, end))
                    inputs.Add(table);

            if (inputs.Count > 0)
                return Sorted(family, version, level, [.. inputs]);
        }

        return null;
    }

    // The compaction of one table of a sorted level into the level below, as RocksDB's default
    // kMinOverlappingRatio: the table whose overlap in the level below is smallest against its own
    // size, so the merge rewrites the fewest bytes for each byte it moves down. A table that overlaps
    // nothing moves. Tables that a running compaction reads, or whose overlap below holds one, are
    // passed over, and so are those whose range a running compaction into the level below writes;
    // null when that leaves none.
    private static Compaction? PickSorted(ColumnFamily family, int level, IReadOnlyList<Compaction> running)
    {
        FamilyVersion version = family.Version;
        TableReader[] tables = version.Levels[level].Tables;
        TableReader[] next = version.Levels[level + 1].Tables;
        int best = -1;
        double bestRatio = double.MaxValue;
        for (int i = 0; i < tables.Length; i++)
        {
            if (Taken(tables[i], running))
                continue;

            long overlap = 0;
            bool free = true;
            byte[] smallest = tables[i].Properties.Smallest;
            byte[] largest = tables[i].Properties.Largest;
            foreach (TableReader table in Overlapping(next, tables[i].Properties))
            {
                free &= !Taken(table, running);
                overlap += table.Properties.Size;
                if (table.Properties.Largest.AsSpan().SequenceCompareTo(largest) > 0)
                    largest = table.Properties.Largest;
                if (table.Properties.Smallest.AsSpan().SequenceCompareTo(smallest) < 0)
                    smallest = table.Properties.Smallest;
            }

            if (!free || OutputConflict(family, level + 1, smallest, largest, running))
                continue;

            double ratio = (double)overlap / Math.Max(1, tables[i].Properties.Size);
            if (ratio < bestRatio)
            {
                best = i;
                bestRatio = ratio;
            }
        }

        return best < 0 ? null : Sorted(family, version, level, [tables[best]]);
    }

    // A compaction of tables of a sorted level, in key order, into the level below, with every table
    // of that level in their range: a move when there is none.
    private static Compaction Sorted(ColumnFamily family, FamilyVersion version, int level, TableReader[] inputs)
    {
        byte[] smallest = inputs[0].Properties.Smallest;
        byte[] largest = inputs[^1].Properties.Largest;
        List<TableReader> output = [];
        foreach (TableReader table in version.Levels[level + 1].Tables)
            if (inputs.Length == 1 ? table.Properties.Overlaps(inputs[0].Properties) : Overlaps(table.Properties, smallest, largest))
                output.Add(table);

        return new Compaction(family, level, inputs, level + 1, [.. output], isMove: output.Count == 0, Below(version, level + 1));
    }

    // A merge of the level 0 tables marked in taken, with every level 0 table that overlaps their range
    // added until none is left, and the tables of the base level in the final range.
    private static Compaction CloseLevel0(ColumnFamily family, FamilyVersion version, bool[] taken, int baseLevel)
    {
        TableReader[] level0 = version.Level0;
        byte[]? smallest = null;
        byte[]? largest = null;
        for (int i = 0; i < level0.Length; i++)
            if (taken[i])
                Widen(level0[i].Properties, ref smallest, ref largest);

        bool grew = true;
        while (grew)
        {
            grew = false;
            for (int i = 0; i < level0.Length; i++)
            {
                if (taken[i] || !Overlaps(level0[i].Properties, smallest!, largest!))
                    continue;

                taken[i] = true;
                Widen(level0[i].Properties, ref smallest, ref largest);
                grew = true;
            }
        }

        List<TableReader> inputs = new(level0.Length);
        for (int i = 0; i < level0.Length; i++)
            if (taken[i])
                inputs.Add(level0[i]);

        List<TableReader> output = [];
        foreach (TableReader table in version.Levels[baseLevel].Tables)
            if (Overlaps(table.Properties, smallest!, largest!))
                output.Add(table);

        return new Compaction(family, 0, [.. inputs], baseLevel, [.. output], isMove: false, Below(version, baseLevel));
    }

    // True when a running compaction reads table.
    private static bool Taken(TableReader table, IReadOnlyList<Compaction> running)
    {
        foreach (Compaction compaction in running)
            if (compaction.Takes(table))
                return true;

        return false;
    }

    // True when a running compaction of family into outputLevel writes into [smallest, largest].
    private static bool OutputConflict(ColumnFamily family, int outputLevel, byte[] smallest, byte[] largest, IReadOnlyList<Compaction> running)
    {
        foreach (Compaction compaction in running)
            if (compaction.Family == family && compaction.OutputLevel == outputLevel
                && compaction.Smallest.AsSpan().SequenceCompareTo(largest) <= 0 && smallest.AsSpan().SequenceCompareTo(compaction.Largest) <= 0)
                return true;

        return false;
    }

    // The non-empty levels under level.
    private static SortedLevel[] Below(FamilyVersion version, int level)
    {
        List<SortedLevel> below = [];
        for (int lower = level + 1; lower <= Manifest.BottomLevel; lower++)
            if (!version.Levels[lower].IsEmpty)
                below.Add(version.Levels[lower]);

        return [.. below];
    }

    // The tables of a sorted level that overlap table, a run of them in key order.
    private static IEnumerable<TableReader> Overlapping(TableReader[] level, TableProperties table)
    {
        int low = 0, high = level.Length;
        while (low < high)
        {
            int middle = (low + high) / 2;
            if (level[middle].Properties.EndsBefore(table.Smallest))
                low = middle + 1;
            else
                high = middle;
        }

        for (int i = low; i < level.Length && level[i].Properties.Overlaps(table); i++)
            yield return level[i];
    }

    private static bool InRange(TableProperties table, byte[]? begin, byte[]? end)
    {
        return (begin is null || begin.AsSpan().SequenceCompareTo(table.Largest) <= 0)
            && (end is null || table.Smallest.AsSpan().SequenceCompareTo(end) <= 0);
    }

    private static bool OverlapEachOther(TableReader[] tables)
    {
        for (int i = 1; i < tables.Length; i++)
            if (OverlapsAny(tables.AsSpan(0, i), tables[i].Properties))
                return true;

        return false;
    }

    private static bool OverlapsAny(ReadOnlySpan<TableReader> tables, TableProperties candidate)
    {
        foreach (TableReader table in tables)
            if (table.Properties.Overlaps(candidate))
                return true;

        return false;
    }

    private static bool Overlaps(TableProperties table, byte[] smallest, byte[] largest)
    {
        return table.Smallest.AsSpan().SequenceCompareTo(largest) <= 0 && smallest.AsSpan().SequenceCompareTo(table.Largest) <= 0;
    }

    private static void Widen(TableProperties table, ref byte[]? smallest, ref byte[]? largest)
    {
        if (smallest is null || table.Smallest.AsSpan().SequenceCompareTo(smallest) < 0)
            smallest = table.Smallest;

        if (largest is null || table.Largest.AsSpan().SequenceCompareTo(largest) > 0)
            largest = table.Largest;
    }
}
