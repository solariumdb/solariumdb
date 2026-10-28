namespace Solarium.Kahuna.Benchmarks;

/// <summary>
/// The rows a session loads before it measures: <paramref name="Keys"/> keys with values of
/// <paramref name="ValueSize"/> bytes, each written <paramref name="Revisions"/> times. Keys come in groups
/// of <see cref="GroupSize"/> under one prefix, so a prefix scan reads one group.
/// </summary>
public sealed record Dataset(string Name, int Keys, int ValueSize, int Revisions)
{
    /// <summary>The keys under one prefix.</summary>
    public const int GroupSize = 100;

    /// <summary>The key of row <paramref name="index"/>, as <c>bench/000042/17</c>.</summary>
    public static string Key(int index) => $"bench/{index / GroupSize:D6}/{index % GroupSize:D2}";

    /// <summary>A key in the same key space that no session writes: a point read of it misses.</summary>
    public static string MissingKey(int index) => $"bench/{index / GroupSize:D6}/{index % GroupSize:D2}x";

    /// <summary>The prefix of the group that holds row <paramref name="index"/>, as <c>bench/000042/</c>.</summary>
    public static string Prefix(int index) => $"bench/{index / GroupSize:D6}/";

    /// <summary>A copy with <paramref name="scale"/> times the keys, for short runs; at least one group.</summary>
    public Dataset Scaled(double scale) => this with { Keys = Math.Max(GroupSize, (int)(Keys * scale) / GroupSize * GroupSize) };

    /// <summary>A short name for the tables, as <c>large 2000000x1024B</c>.</summary>
    public override string ToString() => $"{Name} {Keys}x{ValueSize}B" + (Revisions > 1 ? $"x{Revisions}rev" : "");
}
