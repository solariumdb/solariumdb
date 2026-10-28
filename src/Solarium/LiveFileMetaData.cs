namespace Solarium;

/// <summary>
/// One live SSTable, as RocksDB's <c>LiveFileMetaData</c>, from <see cref="SolariumDb.GetLiveFilesMetaData"/>.
/// The keys are copies that belong to the caller.
/// </summary>
/// <param name="ColumnFamilyName">The column family the table belongs to.</param>
/// <param name="Level">0 for a flushed table, 6 for a table of the sorted level.</param>
/// <param name="Name">The file name with a leading slash, as RocksDB gives it: <c>/000012.sst</c>.</param>
/// <param name="FileNumber">The number in the file name.</param>
/// <param name="Size">The file size in bytes.</param>
/// <param name="SmallestKey">The smallest user key in the table.</param>
/// <param name="LargestKey">The largest user key in the table, which can be the exclusive end of a range tombstone.</param>
/// <param name="SmallestSequenceNumber">The lowest sequence number in the table.</param>
/// <param name="LargestSequenceNumber">The highest sequence number in the table.</param>
/// <param name="EntryCount">The point entries in the table: puts and deletes.</param>
public sealed record LiveFileMetaData(string ColumnFamilyName, int Level, string Name, long FileNumber, long Size,
    byte[] SmallestKey, byte[] LargestKey, long SmallestSequenceNumber, long LargestSequenceNumber, long EntryCount);
