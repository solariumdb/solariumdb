namespace Solarium.Engine;

/// <summary>
/// The base level of a column family, where level 0 compacts into, and the most bytes each sorted level
/// should hold, as RocksDB's <c>VersionStorageInfo::CalculateBaseBytes</c> computes them with
/// <c>level_compaction_dynamic_level_bytes</c>: the largest level's size is projected up one
/// <see cref="ColumnFamilyOptions.MaxBytesForLevelMultiplier"/> per level, and the base level is the
/// highest one whose projection is still at most <see cref="ColumnFamilyOptions.MaxBytesForLevelBase"/>.
/// The base level is never below the first level with files, so the levels above it are empty.
/// </summary>
internal sealed class LevelTargets
{
    private readonly long[] _maxBytes;

    private LevelTargets(int baseLevel, long[] maxBytes)
    {
        BaseLevel = baseLevel;
        _maxBytes = maxBytes;
    }

    /// <summary>The level that level 0 compacts into, 1 to 6.</summary>
    public int BaseLevel { get; }

    /// <summary>
    /// The bytes at which a compaction of <paramref name="level"/> (1 to 6) into the level below
    /// starts; <see cref="long.MaxValue"/> above the base level. The bottom level has no level below.
    /// </summary>
    public long MaxBytes(int level) => _maxBytes[level];

    /// <summary>The targets of the sorted levels of <paramref name="version"/> under <paramref name="options"/>.</summary>
    public static LevelTargets Compute(FamilyVersion version, ColumnFamilyOptions options)
    {
        Span<long> sizes = stackalloc long[Manifest.LevelCount];
        for (int level = 1; level < Manifest.LevelCount; level++)
            sizes[level] = version.Levels[level].Size;

        return Compute(sizes, options);
    }

    /// <summary>
    /// The targets of sorted levels of <paramref name="sizes"/> bytes, indexed by level (index 0 is not
    /// read), under <paramref name="options"/>.
    /// </summary>
    public static LevelTargets Compute(ReadOnlySpan<long> sizes, ColumnFamilyOptions options)
    {
        long[] maxBytes = new long[Manifest.LevelCount];
        Array.Fill(maxBytes, long.MaxValue);

        int firstNonEmpty = 0;
        long maxLevelSize = 0;
        for (int level = 1; level <= Manifest.BottomLevel; level++)
        {
            long size = sizes[level];
            if (size > 0 && firstNonEmpty == 0)
                firstNonEmpty = level;

            maxLevelSize = Math.Max(maxLevelSize, size);
        }

        // No sorted data: level 0 compacts straight into the bottom level.
        if (maxLevelSize == 0)
            return new LevelTargets(Manifest.BottomLevel, maxBytes);

        double multiplier = options.MaxBytesForLevelMultiplier;
        long baseBytesMax = options.MaxBytesForLevelBase;
        long baseBytesMin = (long)(baseBytesMax / multiplier);

        // The largest level's size, as the target of the first level with files.
        long current = maxLevelSize;
        for (int level = Manifest.BottomLevel - 1; level >= firstNonEmpty; level--)
            current = (long)(current / multiplier);

        int baseLevel = firstNonEmpty;
        long baseLevelSize;
        if (current <= baseBytesMin)
        {
            // More levels hold files than the data needs; RocksDB then keeps the base level at the
            // first of them, and the multiplier between the levels does not hold.
            baseLevelSize = baseBytesMin + 1;
        }
        else
        {
            while (baseLevel > 1 && current > baseBytesMax)
            {
                baseLevel--;
                current = (long)(current / multiplier);
            }

            baseLevelSize = current > baseBytesMax ? baseBytesMax : Math.Max(1, current);
        }

        // No level's target is below MaxBytesForLevelBase, as RocksDB's: a smaller one would favor
        // compactions of the sorted levels over level 0's.
        double levelSize = baseLevelSize;
        for (int level = baseLevel; level <= Manifest.BottomLevel; level++)
        {
            if (level > baseLevel)
                levelSize *= multiplier;

            maxBytes[level] = levelSize >= long.MaxValue ? long.MaxValue : Math.Max((long)levelSize, baseBytesMax);
        }

        return new LevelTargets(baseLevel, maxBytes);
    }
}
