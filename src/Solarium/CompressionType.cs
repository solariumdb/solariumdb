namespace Solarium;

/// <summary>How a column family's SSTables store their data blocks, as RocksDB's <c>CompressionType</c>.</summary>
public enum CompressionType
{
    /// <summary>Data blocks are stored as they are, as RocksDB's <c>kNoCompression</c>.</summary>
    None = 0,

    /// <summary>
    /// Data blocks are compressed with LZ4's block format, as RocksDB's <c>kLZ4Compression</c>. A block
    /// that compression shrinks by less than an eighth is stored as it is.
    /// </summary>
    Lz4 = 1,
}
