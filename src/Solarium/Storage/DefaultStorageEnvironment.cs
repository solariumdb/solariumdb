using Microsoft.Win32.SafeHandles;

namespace Solarium.Storage;

/// <summary>
/// The environment for every platform: <see cref="RandomAccess"/> over a <see cref="SafeFileHandle"/>,
/// which is pread/pwrite on Linux and macOS and positional ReadFile/WriteFile on Windows.
/// </summary>
public sealed class DefaultStorageEnvironment : IStorageEnvironment
{
    /// <summary>The shared instance; the environment holds no state, so one serves every database.</summary>
    public static DefaultStorageEnvironment Instance { get; } = new();

    private DefaultStorageEnvironment()
    {
    }

    /// <inheritdoc />
    public string Name => "default (RandomAccess)";

    /// <inheritdoc />
    /// <remarks>
    /// Other handles may read, write and delete the file: the engine flushes the current WAL file
    /// through a second handle, and deletes a file that a handle may still hold. Windows enforces
    /// the share mode; Unix ignores it. The <c>LOCK</c> file keeps other processes out.
    /// </remarks>
    public IStorageFile OpenFile(string path)
    {
        return new DefaultStorageFile(File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete));
    }

    private sealed class DefaultStorageFile(SafeFileHandle handle) : IStorageFile
    {
        public long GetLength() => RandomAccess.GetLength(handle);

        public int Read(Span<byte> buffer, long offset) => RandomAccess.Read(handle, buffer, offset);

        public void Write(ReadOnlySpan<byte> buffer, long offset) => RandomAccess.Write(handle, buffer, offset);

        public void Flush() => RandomAccess.FlushToDisk(handle);

        public void SyncRange(long offset, long length) => DurableFiles.SyncRange(handle, offset, length);

        public void SetLength(long length) => RandomAccess.SetLength(handle, length);

        public void Dispose() => handle.Dispose();
    }
}
