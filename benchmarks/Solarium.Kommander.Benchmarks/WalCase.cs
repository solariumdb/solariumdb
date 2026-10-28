namespace Solarium.Kommander.Benchmarks;

/// <summary>
/// One measured shape: a workload on <paramref name="Threads"/> writer threads, synced or not, with
/// entries of <paramref name="Payload"/> bytes.
/// </summary>
public sealed record WalCase(WalWorkload Workload, int Threads, bool Sync, int Payload)
{
    /// <summary>A short name for the tables, as <c>append sync t16 1024B</c>.</summary>
    public override string ToString() => $"{Workload.ToString().ToLowerInvariant()} {(Sync ? "sync" : "nosync")} t{Threads} {Payload}B";
}
