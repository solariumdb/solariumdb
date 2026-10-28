namespace Solarium.Benchmarks;

/// <summary>
/// An engine that a compaction scenario drives: Solarium or RocksDB, opened with the shard families
/// and a <c>metadata</c> family, the options of <see cref="CompactionSettings"/>, and no compression,
/// so the bytes of both engines compare. Writers call it from several threads at once.
/// </summary>
internal abstract class CompactionStore : IDisposable
{
    /// <summary>The store that <paramref name="name"/> names: <c>solarium</c> or <c>rocksdb</c>.</summary>
    public static CompactionStore Create(string name, string directory, CompactionSettings settings, string[] families)
    {
        return name switch
        {
            "solarium" => new SolariumCompactionStore(directory, settings, families),
            "rocksdb" => new RocksDbCompactionStore(directory, settings, families),
            _ => throw new ArgumentException($"Unknown store '{name}': solarium or rocksdb."),
        };
    }

    /// <summary>Writes rows <paramref name="firstId"/> to <paramref name="firstId"/> + <paramref name="count"/> - 1 of a partition in one batch.</summary>
    public abstract void Append(int family, int partition, long firstId, int count, byte[] value);

    /// <summary>
    /// Kommander's floor pass: one batch that writes the new floor to <c>metadata</c> and a range
    /// tombstone from <paramref name="tombstoneStart"/> to <paramref name="newFloor"/>, then
    /// <c>DeleteFilesInRange</c> from row 0 to <paramref name="newFloor"/> - 1. With
    /// <paramref name="confined"/>, the tombstone starts no lower than the active memtable's first
    /// row of the partition, which ends at <paramref name="head"/>, plus Kommander's margin.
    /// </summary>
    public void FloorPass(int family, int partition, long floor, long newFloor, long head, bool confined)
    {
        long tombstoneStart = confined ? Math.Max(floor, head - ActiveEntries(family) + 1 + 64) : floor;
        WriteFloor(family, partition, newFloor, tombstoneStart < newFloor ? tombstoneStart : -1);
        DropBelow(family, partition, newFloor);
    }

    /// <summary>Writes one key in the first shard family.</summary>
    public abstract void Put(ReadOnlySpan<byte> key, byte[] value);

    /// <summary>The level 0 files of the family that has the most now.</summary>
    public abstract int MaxLevel0Files();

    /// <summary>The bytes of every live SSTable.</summary>
    public abstract long LiveSstBytes();

    /// <summary>Waits until no compaction runs or is due, at most <paramref name="timeout"/>; false when it ran out.</summary>
    public abstract bool Settle(TimeSpan timeout);

    /// <summary>
    /// The work since open: the bytes flushes wrote, the bytes compactions read and wrote, the level 0
    /// files moved without a rewrite (null when the engine does not count them), and the time writes
    /// were slowed or stopped.
    /// </summary>
    public abstract (long FlushBytes, long CompactionRead, long CompactionWritten, long? MovedFiles, TimeSpan Stalled) Counts();

    /// <summary>Closes the database.</summary>
    public abstract void Dispose();

    /// <summary>Drops the tables of the partition that lie wholly below <paramref name="floor"/>.</summary>
    public abstract void DropBelow(int family, int partition, long floor);

    /// <summary>The entries in the active memtable of <paramref name="family"/>.</summary>
    protected abstract long ActiveEntries(int family);

    /// <summary>Writes the floor and, unless <paramref name="tombstoneStart"/> is negative, the range tombstone up to it, in one batch.</summary>
    protected abstract void WriteFloor(int family, int partition, long newFloor, long tombstoneStart);
}
