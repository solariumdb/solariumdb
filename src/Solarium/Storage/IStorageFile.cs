namespace Solarium.Storage;

/// <summary>
/// A file with positional I/O only: there is no file position, every call names its offset.
/// Calls are synchronous and safe from any thread.
/// </summary>
public interface IStorageFile : IDisposable
{
    /// <summary>The current size in bytes, read from the OS.</summary>
    long GetLength();

    /// <summary>Read up to <c>buffer.Length</c> bytes at <paramref name="offset"/>. Returns 0 at end of file.</summary>
    int Read(Span<byte> buffer, long offset);

    /// <summary>Write all of <paramref name="buffer"/> at <paramref name="offset"/>, or throw.</summary>
    void Write(ReadOnlySpan<byte> buffer, long offset);

    /// <summary>Return when every write before it is durable on the device.</summary>
    void Flush();

    /// <summary>
    /// Start writing the bytes from <paramref name="offset"/> for <paramref name="length"/> to the
    /// device, without waiting for them, as Linux's <c>sync_file_range(SYNC_FILE_RANGE_WRITE)</c> and
    /// RocksDB's <c>RangeSync</c>: a large file then reaches the device as it is written, not all at its
    /// <see cref="Flush"/>. It makes nothing durable. The default does nothing.
    /// </summary>
    /// <exception cref="IOException">The system refused the request.</exception>
    void SyncRange(long offset, long length)
    {
    }

    /// <summary>Truncate or extend the file. Not durable until the next <see cref="Flush"/>.</summary>
    void SetLength(long length);
}
