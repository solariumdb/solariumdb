namespace Solarium.Storage;

/// <summary>
/// Where the engine's file I/O goes, like RocksDB's <c>Env</c> and <c>FileSystem</c>. The default is
/// <see cref="DefaultStorageEnvironment"/>; the <c>Solarium.IoUring</c> package adds an io_uring
/// environment for Linux. The host owns an environment and disposes it after every database that
/// uses it is closed.
/// </summary>
/// <remarks>
/// Every file of a database goes through the environment: data files through <see cref="OpenFile"/>,
/// and the metadata operations (create, list, delete, rename, link, directory flush) through the other
/// members, so an environment sees each change to the directory. Only the database's <c>LOCK</c>
/// file, a process lock, does not. The metadata members have implementations on the BCL, which an
/// environment overrides to observe or change them.
/// </remarks>
public interface IStorageEnvironment
{
    /// <summary>A short human-readable name of the environment, for diagnostics and test names.</summary>
    string Name { get; }

    /// <summary>Open <paramref name="path"/> for reading and writing, creating it if it does not exist.</summary>
    IStorageFile OpenFile(string path);

    /// <summary>
    /// Read every request of <paramref name="requests"/>, each until its buffer is full or its file ends, and set its
    /// <see cref="ReadRequest.BytesRead"/>, as RocksDB's <c>FSRandomAccessFile::MultiRead</c>, but across files. An
    /// environment may keep the reads in flight together; the default reads them one after another through
    /// <see cref="IStorageFile.Read"/> of their files. Returns when every read is done.
    /// </summary>
    /// <exception cref="IOException">A read failed; the other requests may or may not have been read.</exception>
    void MultiRead(Span<ReadRequest> requests)
    {
        foreach (ref ReadRequest request in requests)
        {
            Span<byte> buffer = request.Buffer.Span;
            int total = 0;
            while (total < buffer.Length)
            {
                int read = request.File.Read(buffer[total..], request.Offset + total);
                if (read == 0)
                    break;

                total += read;
            }

            request.BytesRead = total;
        }
    }

    /// <summary>True when <paramref name="path"/> names an existing file, as RocksDB's <c>FileExists</c>.</summary>
    bool FileExists(string path) => File.Exists(path);

    /// <summary>The names, without the directory, of the entries of <paramref name="directory"/>, as RocksDB's <c>GetChildren</c>.</summary>
    string[] GetChildren(string directory) => [.. Directory.EnumerateFileSystemEntries(directory).Select(e => Path.GetFileName(e))];

    /// <summary>Delete <paramref name="path"/>; nothing happens when it does not exist. Durable only after <see cref="FlushDirectory"/>.</summary>
    void DeleteFile(string path) => File.Delete(path);

    /// <summary>
    /// Rename <paramref name="source"/> to <paramref name="target"/>, replacing <paramref name="target"/>
    /// atomically when it exists, as RocksDB's <c>RenameFile</c>. Durable only after <see cref="FlushDirectory"/>.
    /// </summary>
    void RenameFile(string source, string target) => File.Move(source, target, overwrite: true);

    /// <summary>
    /// Give the file <paramref name="source"/> a second name, <paramref name="target"/> (a hard link),
    /// as RocksDB's <c>LinkFile</c>. Returns false, and creates nothing, when the file system cannot
    /// link these two paths: they are on different file systems, it has no hard links, or the file
    /// has as many as it allows; RocksDB's <c>NotSupported</c>, after which a caller copies instead.
    /// Durable only after <see cref="FlushDirectory"/> of the directory of <paramref name="target"/>.
    /// </summary>
    /// <exception cref="IOException">Any other failure, such as a <paramref name="target"/> that exists or a <paramref name="source"/> that does not.</exception>
    bool TryLinkFile(string source, string target) => DurableFiles.TryLinkFile(source, target);

    /// <summary>Create <paramref name="directory"/> and its missing parents. Durable only after <see cref="FlushDirectory"/> of the parent.</summary>
    void CreateDirectory(string directory) => Directory.CreateDirectory(directory);

    /// <summary>
    /// Make the entries of <paramref name="directory"/> durable, as RocksDB's <c>FSDirectory::Fsync</c>:
    /// a file created, renamed or deleted in it survives a power loss only after this.
    /// </summary>
    void FlushDirectory(string directory) => DurableFiles.FlushDirectory(directory);
}
