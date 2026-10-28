namespace Solarium.Kommander.Benchmarks;

/// <summary>What each writer thread of a case does, on its own partition, with log ids that only grow.</summary>
public enum WalWorkload
{
    /// <summary>One <c>Proposed</c> entry per <c>Write</c>: a leader's append.</summary>
    Append,

    /// <summary>A <c>Proposed</c> entry, then its <c>Committed</c> marker in a second <c>Write</c>: an append and its commit.</summary>
    Commit,

    /// <summary>64 <c>Proposed</c> entries per <c>Write</c>: a follower's append of a replicated batch.</summary>
    Batch,

    /// <summary>
    /// An append and its commit per entry, a checkpoint and a compaction pass every 1,000 entries,
    /// and reader threads that read the last 64 entries of a partition: a node at work.
    /// </summary>
    Mixed,
}
