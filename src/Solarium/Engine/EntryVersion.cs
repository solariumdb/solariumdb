namespace Solarium.Engine;

/// <summary>
/// What a point lookup takes from the newest version of the key it looked up: the version's sequence number, its
/// kind, and for a put its value, in a new array that the caller keeps; null for a delete. It has no key: the
/// caller holds the key it looked up, so a lookup copies none.
/// </summary>
internal readonly record struct EntryVersion(long Sequence, OperationKind Kind, byte[]? Value);
