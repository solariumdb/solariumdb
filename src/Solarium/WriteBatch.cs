using Solarium.Engine;

namespace Solarium;

/// <summary>
/// Writes that are applied all together or not at all, across any number of column families of
/// one database. The batch copies keys and values when they are added.
/// </summary>
public sealed class WriteBatch
{
    /// <summary>The operations in the order they were added.</summary>
    internal List<BatchOperation> Operations { get; } = [];

    /// <summary>The number of operations in the batch.</summary>
    public int Count => Operations.Count;

    /// <summary>Set <paramref name="key"/> to <paramref name="value"/>. Null family: the default one.</summary>
    public WriteBatch Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamily? family = null)
    {
        Operations.Add(new BatchOperation(OperationKind.Put, family, key.ToArray(), value.ToArray()));
        return this;
    }

    /// <summary>Delete <paramref name="key"/>. Null family: the default one.</summary>
    public WriteBatch Delete(ReadOnlySpan<byte> key, ColumnFamily? family = null)
    {
        Operations.Add(new BatchOperation(OperationKind.Delete, family, key.ToArray(), null));
        return this;
    }

    /// <summary>
    /// Delete every key in [<paramref name="begin"/>, <paramref name="end"/>) with one range tombstone.
    /// An empty range, with <paramref name="begin"/> equal to <paramref name="end"/>, adds nothing, as
    /// RocksDB applies nothing for it.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="end"/> comes before <paramref name="begin"/>. The batch is unchanged; RocksDB fails
    /// the write with <c>InvalidArgument</c>.
    /// </exception>
    public WriteBatch DeleteRange(ReadOnlySpan<byte> begin, ReadOnlySpan<byte> end, ColumnFamily? family = null)
    {
        int order = begin.SequenceCompareTo(end);
        if (order > 0)
            throw new ArgumentException("The end key comes before the begin key of the range delete.", nameof(end));

        if (order < 0)
            Operations.Add(new BatchOperation(OperationKind.DeleteRange, family, begin.ToArray(), end.ToArray()));

        return this;
    }

    /// <summary>Remove every operation so the batch can be reused.</summary>
    public void Clear() => Operations.Clear();
}
