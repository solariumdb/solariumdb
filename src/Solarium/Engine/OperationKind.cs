namespace Solarium.Engine;

/// <summary>The kind of a write, as a batch holds it and as the WAL stores it (one byte).</summary>
internal enum OperationKind : byte
{
    /// <summary>Set a key to a value.</summary>
    Put = 1,

    /// <summary>Delete one key.</summary>
    Delete = 2,

    /// <summary>Key is the inclusive begin, Value the exclusive end.</summary>
    DeleteRange = 3,
}
