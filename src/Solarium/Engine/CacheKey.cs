namespace Solarium.Engine;

/// <summary>
/// The key of a <see cref="Cache"/> entry: the owner, a number <see cref="Cache.NewOwner"/> handed out once per
/// database (or per memtable budget), the owner's number for the entry, an SSTable's file number, and the part
/// of it: the offset of a data block, or -1 - p for partition p of the table's index and filter, so the two never
/// meet. A table number is never reused within a database, and the owner keeps two databases that share a cache
/// apart.
/// </summary>
internal readonly record struct CacheKey(long Owner, long Number, long Part = 0);
