namespace Solarium.Engine;

/// <summary>A forward cursor over every version of every key of one source.</summary>
internal interface IEntryCursor
{
    /// <summary>True when the cursor is positioned on an entry, so <see cref="Current"/> may be read.</summary>
    bool Valid { get; }

    /// <summary>The entry the cursor is on, its key and value arrays the caller's to keep; defined only while <see cref="Valid"/> is true.</summary>
    Entry Current { get; }

    /// <summary>
    /// The key of <see cref="Current"/>, without a copy: valid until the cursor moves, and only while the caller keeps
    /// the cursor reachable, for a memtable's key is in its native memory. Defined only while <see cref="Valid"/> is true.
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
