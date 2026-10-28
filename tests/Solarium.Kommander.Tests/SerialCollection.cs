namespace Solarium.Kommander.Tests;

/// <summary>
/// Runs its tests alone, with no other test beside them. The cluster tests time heartbeats and
/// elections in milliseconds, and the load of other tests turns into election churn; the metrics
/// tests read process-wide gauges, which every open WAL reports to.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SerialCollection
{
    /// <summary>The collection's name.</summary>
    public const string Name = "Serial";
}
