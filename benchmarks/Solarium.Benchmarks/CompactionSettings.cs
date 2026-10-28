using System.Globalization;

namespace Solarium.Benchmarks;

/// <summary>
/// The workload and the column family options of one compaction scenario run, from the
/// <c>name=value</c> arguments of <see cref="CompactionLoad"/>. Both engines get the same values.
/// </summary>
internal sealed record CompactionSettings
{
    /// <summary>Kommander's partition width in a log key: the partition id in fixed-width decimal.</summary>
    public const int PartitionWidth = 10;

    /// <summary>Kommander's log key: the partition, a separator, and the id in fixed-width decimal.</summary>
    public const int LogKeyWidth = PartitionWidth + 1 + 20;

    /// <summary>The bytes of a random-scenario key.</summary>
    public const int RandomKeyWidth = 16;

    /// <summary>Log partitions, one writer thread each (append scenarios).</summary>
    public int Partitions { get; init; } = 4;

    /// <summary>
    /// Shard column families; partition <c>p</c> writes to <c>shard{p % Families}</c>, as Kommander maps
    /// partitions to its shards (append scenarios).
    /// </summary>
    public int Families { get; init; } = 4;

    /// <summary>Rows each partition appends (append scenarios).</summary>
    public long Rows { get; init; } = 4_000_000;

    /// <summary>Rows in one write batch, as a Raft batch of log entries (append scenarios).</summary>
    public int Batch { get; init; } = 100;

    /// <summary>Rows a partition appends between two floor passes (append scenarios).</summary>
    public long Pass { get; init; } = 100_000;

    /// <summary>Rows a partition keeps below its head: the floor is <c>head + 1 - Window</c> (append scenarios).</summary>
    public long Window { get; init; } = 1_000_000;

    /// <summary>Distinct keys (random scenario).</summary>
    public long Keys { get; init; } = 4_000_000;

    /// <summary>Puts in all (random scenario).</summary>
    public long Puts { get; init; } = 12_000_000;

    /// <summary>Writer threads (random scenario).</summary>
    public int Threads { get; init; } = 4;

    /// <summary>The bytes of each value.</summary>
    public int Value { get; init; } = 256;

    /// <summary>The memtable size of each family, in bytes.</summary>
    public long WriteBufferSize { get; init; } = 16L * 1024 * 1024;

    /// <summary>The memtables, active and immutable, a family may hold.</summary>
    public int MaxWriteBufferNumber { get; init; } = 4;

    /// <summary>The level 0 files that start a compaction.</summary>
    public int Level0Trigger { get; init; } = 8;

    /// <summary>The level 0 files that slow writes.</summary>
    public int Level0Slowdown { get; init; } = 28;

    /// <summary>The level 0 files that stop writes.</summary>
    public int Level0Stop { get; init; } = 44;

    /// <summary>
    /// Solarium's compaction threads: by default the compactions that RocksDB runs for the
    /// <c>clamp(CPUs / 2, 2, 8)</c> background jobs it gets, a quarter of the jobs, at least one,
    /// going to flushes.
    /// </summary>
    public int Compactions { get; init; } = RocksDbCompactions(Math.Clamp(Environment.ProcessorCount / 2, 2, 8));

    /// <summary>The compactions that RocksDB runs for <paramref name="jobs"/> background jobs.</summary>
    public static int RocksDbCompactions(int jobs) => Math.Max(1, jobs - Math.Max(1, jobs / 4));

    /// <summary>
    /// Applies <c>name=value</c> arguments over the defaults: partitions, families, rows, batch, pass,
    /// window, keys, puts, threads, value, buffer (MiB), buffers, trigger, slowdown, stop, compactions.
    /// Throws <see cref="ArgumentException"/> on an unknown name or a value that does not parse.
    /// </summary>
    public CompactionSettings With(IEnumerable<string> arguments)
    {
        CompactionSettings settings = this;
        foreach (string argument in arguments)
        {
            string[] parts = argument.Split('=', 2);
            if (parts.Length != 2 || !long.TryParse(parts[1], CultureInfo.InvariantCulture, out long value) || value < 1)
                throw new ArgumentException($"'{argument}' is not name=value with a positive integer value.");

            settings = parts[0] switch
            {
                "partitions" => settings with { Partitions = (int)value },
                "families" => settings with { Families = (int)value },
                "rows" => settings with { Rows = value },
                "batch" => settings with { Batch = (int)value },
                "pass" => settings with { Pass = value },
                "window" => settings with { Window = value },
                "keys" => settings with { Keys = value },
                "puts" => settings with { Puts = value },
                "threads" => settings with { Threads = (int)value },
                "value" => settings with { Value = (int)value },
                "buffer" => settings with { WriteBufferSize = value * 1024 * 1024 },
                "buffers" => settings with { MaxWriteBufferNumber = (int)value },
                "trigger" => settings with { Level0Trigger = (int)value },
                "slowdown" => settings with { Level0Slowdown = (int)value },
                "stop" => settings with { Level0Stop = (int)value },
                "compactions" => settings with { Compactions = (int)value },
                _ => throw new ArgumentException($"Unknown setting '{parts[0]}'."),
            };
        }

        return settings;
    }

    /// <summary>The shard family names, in shard order.</summary>
    public string[] FamilyNames() => [.. Enumerable.Range(0, Families).Select(i => $"shard{i}")];

    /// <summary>Writes Kommander's log key for <paramref name="partition"/> and <paramref name="id"/> into <paramref name="key"/>, which is <see cref="LogKeyWidth"/> bytes.</summary>
    public static void LogKey(Span<byte> key, int partition, long id)
    {
        partition.TryFormat(key[..PartitionWidth], out _, "D10", CultureInfo.InvariantCulture);
        key[PartitionWidth] = (byte)':';
        id.TryFormat(key[(PartitionWidth + 1)..], out _, "D20", CultureInfo.InvariantCulture);
    }

    /// <summary>Kommander's log key for <paramref name="partition"/> and <paramref name="id"/>, as a new array.</summary>
    public static byte[] LogKey(int partition, long id)
    {
        byte[] key = new byte[LogKeyWidth];
        LogKey(key, partition, id);
        return key;
    }

    /// <summary>The metadata key that holds the compaction floor of <paramref name="partition"/>, as Kommander names it.</summary>
    public static byte[] FloorKey(int partition) => System.Text.Encoding.ASCII.GetBytes($"raft_compaction_floor_p{partition}");
}
