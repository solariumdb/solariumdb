namespace Solarium.Engine;

/// <summary>
/// A sorted run of entries that reads can search: a memtable, an SSTable, or a sorted level of
/// SSTables. Within one column family, for each key, every version of the key and every range
/// tombstone that covers it in a newer source has a higher sequence number than every one in an
/// older source. Reads search the sources newest first and rely on this; flushes and compactions
/// keep it. Across keys, a newer source can hold older data than an older one.
/// </summary>
internal interface IEntrySource
{
    /// <summary>
    /// The newest version of <paramref name="key"/> at or below <paramref name="snapshot"/>, its value in a new
    /// array and its key not copied. An SSTable that reads
    /// its index and filter again on a cache miss caches them unless <paramref name="fillCache"/> is false
    /// (<see cref="ReadOptions.FillCache"/>).
    /// </summary>
    bool TryGetNewest(ReadOnlySpan<byte> key, long snapshot, bool fillCache, out EntryVersion entry);

    /// <summary>
    /// False when the source holds no range tombstone, so <see cref="CoveringTombstone"/> is 0 for every key. A
    /// memtable can gain one later, with a sequence number above every snapshot taken before this was read; an
    /// SSTable and a sorted level never change.
    /// </summary>
    bool HasTombstones { get; }

    /// <summary>The highest sequence number of a range tombstone at or below <paramref name="snapshot"/> that covers <paramref name="key"/>, or 0.</summary>
    long CoveringTombstone(ReadOnlySpan<byte> key, long snapshot);

    /// <summary>
    /// The largest key below <paramref name="limit"/> (null: no limit) with any version here, or null; see
    /// <see cref="TryGetNewest"/> for <paramref name="fillCache"/>.
    /// </summary>
    byte[]? LastKeyBefore(byte[]? limit, bool fillCache);

    /// <summary>
    /// A new forward cursor over every version of every key of this source, unpositioned until a seek; see
    /// <see cref="TryGetNewest"/> for <paramref name="fillCache"/>.
    /// </summary>
    IEntryCursor NewCursor(bool fillCache);
}
