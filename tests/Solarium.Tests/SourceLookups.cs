using Solarium.Engine;

namespace Solarium.Tests;

/// <summary>
/// Point lookups in one memtable or one table alone, with the steps that the database's lookups take in each, for the
/// tests of a source.
/// </summary>
internal static class SourceLookups
{
    /// <summary>The newest version of <paramref name="key"/> at or below <paramref name="snapshot"/> in the memtable.</summary>
    public static bool TryGetNewest(this MemTable memTable, ReadOnlySpan<byte> key, long snapshot, out EntryVersion entry)
    {
        bool found = memTable.TryFind(key, snapshot, out long sequence, out OperationKind kind, out ReadOnlySpan<byte> value);
        entry = found ? new EntryVersion(sequence, kind, kind == OperationKind.Put ? value.ToArray() : null) : default;
        GC.KeepAlive(memTable);
        return found;
    }

    /// <summary>
    /// The newest version of <paramref name="key"/> at or below <paramref name="snapshot"/> in the table, whose blocks
    /// the lookup caches unless <paramref name="fillCache"/> is false.
    /// </summary>
    public static bool TryGetNewest(this TableReader table, ReadOnlySpan<byte> key, long snapshot, bool fillCache, out EntryVersion entry)
    {
        entry = default;
        if (!table.TryLocate(key, snapshot, fillCache, out DataBlockHandle handle))
            return false;

        using TableReader.BlockBuffer block = table.DataBlock(handle, fillCache);
        if (!table.TryFind(block.Entries, handle, key, snapshot, out long sequence, out OperationKind kind, out ReadOnlySpan<byte> value))
            return false;

        entry = new EntryVersion(sequence, kind, kind == OperationKind.Put ? value.ToArray() : null);
        return true;
    }
}
