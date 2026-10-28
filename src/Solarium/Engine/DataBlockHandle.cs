namespace Solarium.Engine;

/// <summary>
/// Where a data block of an SSTable is: its number in its partition's index, for error messages, and its offset
/// and size in the file, checksum included. <see cref="TableReader.TryLocate"/> returns it.
/// </summary>
internal readonly record struct DataBlockHandle(int Block, long Offset, int Size);
