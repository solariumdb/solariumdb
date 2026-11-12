namespace Solarium.Engine;

/// <summary>
/// A forward cursor over every version of every key of one source. Dispose it when it is done: a cursor of an
/// SSTable holds the data block it reads, a pooled buffer or a reference on a cached block, until it moves past
/// its last entry or is disposed.
/// </summary>
internal interface IEntryCursor : IDisposable
{
    /// <summary>True when the cursor is positioned on an entry, so <see cref="Current"/> may be read.</summary>
    bool Valid { get; }

    /// <summary>
    /// The entry the cursor is on, its key and value copied into new arrays that the caller keeps; defined only
    /// while <see cref="Valid"/> is true. A merge or a scan reads <see cref="CurrentKey"/> and
    /// <see cref="CurrentValue"/> instead, which copy nothing.
    /// </summary>
    Entry Current { get; }

    /// <summary>
    /// The key of <see cref="Current"/>, without a copy: valid until the cursor moves, and only while the caller keeps
    /// the cursor reachable and not disposed, for a memtable's key is in its native memory and an SSTable's in the
    /// data block that the cursor holds. Defined only while <see cref="Valid"/> is true.
    /// </summary>
    ReadOnlySpan<byte> CurrentKey { get; }

    /// <summary>The sequence number of <see cref="Current"/>, without a copy; defined only while <see cref="Valid"/> is true.</summary>
    long CurrentSequence { get; }

    /// <summary>The kind of <see cref="Current"/>; defined only while <see cref="Valid"/> is true.</summary>
    OperationKind CurrentKind { get; }

    /// <summary>The value of <see cref="Current"/>, empty for a delete, without a copy and valid as <see cref="CurrentKey"/> is.</summary>
    ReadOnlySpan<byte> CurrentValue { get; }

    /// <summary>Move to the first entry of the source.</summary>
    void SeekToFirst();

    /// <summary>Move to the first version of the first key at or after <paramref name="key"/>.</summary>
    void Seek(ReadOnlySpan<byte> key);

    /// <summary>Move to the next entry in internal key order.</summary>
    void Next();
}
