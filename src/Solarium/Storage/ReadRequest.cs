namespace Solarium.Storage;

/// <summary>
/// One read of a <see cref="IStorageEnvironment.MultiRead"/> batch: fill <see cref="Buffer"/> from
/// <see cref="File"/> at <see cref="Offset"/>. The environment sets <see cref="BytesRead"/>.
/// </summary>
/// <param name="file">The file to read; it must stay open until the batch returns.</param>
/// <param name="buffer">Where the bytes go; the caller must not touch it until the batch returns.</param>
/// <param name="offset">The file offset of the first byte.</param>
public struct ReadRequest(IStorageFile file, Memory<byte> buffer, long offset)
{
    /// <summary>The file to read.</summary>
    public IStorageFile File { get; } = file;

    /// <summary>Where the bytes go: all of it, unless the file ends first.</summary>
    public Memory<byte> Buffer { get; } = buffer;

    /// <summary>The file offset of the first byte.</summary>
    public long Offset { get; } = offset;

    /// <summary>
    /// The bytes read into <see cref="Buffer"/>, set by the batch: its length, or fewer when the file ends
    /// before the buffer is full.
    /// </summary>
    public int BytesRead { get; set; }
}
