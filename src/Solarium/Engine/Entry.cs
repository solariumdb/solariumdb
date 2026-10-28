namespace Solarium.Engine;

/// <summary>One version of a key: a put (with its value) or a point delete.</summary>
internal readonly record struct Entry(byte[] Key, long Sequence, OperationKind Kind, byte[]? Value);
