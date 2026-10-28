namespace Solarium.Engine;

/// <summary>One write of a <see cref="WriteBatch"/>. A null family means the default one.</summary>
internal readonly record struct BatchOperation(OperationKind Kind, ColumnFamily? Family, byte[] Key, byte[]? Value);
