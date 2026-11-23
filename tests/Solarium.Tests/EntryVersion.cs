using Solarium.Engine;

namespace Solarium.Tests;

/// <summary>
/// The newest version of a key in one source (<see cref="SourceLookups"/>): its sequence number, its kind, and for a put
/// its value, in a new array; null for a delete.
/// </summary>
internal readonly record struct EntryVersion(long Sequence, OperationKind Kind, byte[]? Value);
