namespace Solarium.Engine;

/// <summary>A forward cursor over every version of every key of one source.</summary>
internal interface IEntryCursor
{
    /// <summary>True when the cursor is positioned on an entry, so <see cref="Current"/> may be read.</summary>
    bool Valid { get; }

    /// <summary>The entry the cursor is on; defined only while <see cref="Valid"/> is true.</summary>
    Entry Current { get; }

    /// <summary>Move to the first entry of the source.</summary>
    void SeekToFirst();

    /// <summary>Move to the first version of the first key at or after <paramref name="key"/>.</summary>
    void Seek(ReadOnlySpan<byte> key);

    /// <summary>Move to the next entry in internal key order.</summary>
    void Next();
}
