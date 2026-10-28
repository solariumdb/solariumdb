using Solarium.Storage;

namespace Solarium.Tests;

/// <summary>Wraps an environment so a test can hold writes and flushes or make them fail.</summary>
public sealed class HookedEnvironment(IStorageEnvironment inner) : IStorageEnvironment
{
    /// <summary>Flushes wait while this is reset.</summary>
    public ManualResetEventSlim FlushGate { get; } = new(initialState: true);

    /// <summary>Set when a flush starts.</summary>
    public ManualResetEventSlim FlushStarted { get; } = new();

    /// <summary>Writes wait while this is reset.</summary>
    public ManualResetEventSlim WriteGate { get; } = new(initialState: true);

    /// <summary>Set when a write starts.</summary>
    public ManualResetEventSlim WriteStarted { get; } = new();

    /// <summary>When true, writes to <see cref="FailingPaths"/> throw <see cref="IOException"/>.</summary>
    public volatile bool FailWrites;

    /// <summary>Flushes of <see cref="FailingPaths"/> fail after <see cref="FlushGate"/> lets them through.</summary>
    public volatile bool FailFlushes;

    /// <summary>When true, <see cref="FlushDirectory"/> throws <see cref="IOException"/>.</summary>
    public volatile bool FailDirectoryFlushes;

    /// <summary>When false, <see cref="TryLinkFile"/> answers that the file system cannot link, as across two of them.</summary>
    public volatile bool LinkFiles = true;

    /// <summary>When true, <see cref="IStorageFile.SyncRange"/> throws <see cref="IOException"/> on the files of <see cref="FailingPaths"/>.</summary>
    public volatile bool FailSyncRanges;

    private int _linkAttempts;
    private readonly List<(string Path, long Offset, long Length)> _syncRanges = [];

    /// <summary>Every <see cref="IStorageFile.SyncRange"/> call so far, in order, with the file it was on.</summary>
    public List<(string Path, long Offset, long Length)> SyncRanges
    {
        get
        {
            lock (_syncRanges)
            {
                return [.. _syncRanges];
            }
        }
    }

    private int _writeCount;

    /// <summary>The calls of <see cref="IStorageFile.Write"/> so far on the files of <see cref="CountedPaths"/>.</summary>
    public int WriteCount => Volatile.Read(ref _writeCount);

    /// <summary>The files whose writes <see cref="WriteCount"/> counts. Default: all.</summary>
    public Func<string, bool> CountedPaths { get; set; } = _ => true;

    /// <summary>The calls of <see cref="TryLinkFile"/> so far, refused ones included.</summary>
    public int LinkAttempts => Volatile.Read(ref _linkAttempts);

    /// <summary>The files whose writes <see cref="WriteGate"/> and flushes <see cref="FlushGate"/> hold. Default: all.</summary>
    public Func<string, bool> GatedPaths { get; set; } = _ => true;

    /// <summary>The files whose writes <see cref="FailWrites"/> and flushes <see cref="FailFlushes"/> fail. Default: all.</summary>
    public Func<string, bool> FailingPaths { get; set; } = _ => true;

    /// <summary>
    /// The reads to damage, by file path and read offset: a matching read returns its bytes with the first one
    /// inverted, as a bit flip on the medium would. Default: none.
    /// </summary>
    public Func<string, long, bool> CorruptedReads { get; set; } = (_, _) => false;

    private int _openFileCount;

    /// <summary>The files opened through this environment and not disposed yet.</summary>
    public int OpenFileCount => Volatile.Read(ref _openFileCount);

    /// <summary>The wrapped environment's name, prefixed with "hooked".</summary>
    public string Name => $"hooked {inner.Name}";

    /// <summary>
    /// Returns a scope that opens both gates when it is disposed. A test declares it after its
    /// database, so the gates open before the database's <c>Dispose</c>, which waits for a held
    /// writer or flush: a test that fails while it holds one then fails instead of hanging.
    /// </summary>
    public GateRelease OpenGatesAtEnd() => new(this);

    /// <summary>Opens the file in the wrapped environment and returns it wrapped with this environment's hooks.</summary>
    public IStorageFile OpenFile(string path)
    {
        HookedFile file = new(inner.OpenFile(path), path, this);
        Interlocked.Increment(ref _openFileCount);
        return file;
    }

    /// <summary>Asks the wrapped environment.</summary>
    public bool FileExists(string path) => inner.FileExists(path);

    /// <summary>Lists through the wrapped environment.</summary>
    public string[] GetChildren(string directory) => inner.GetChildren(directory);

    /// <summary>Deletes through the wrapped environment.</summary>
    public void DeleteFile(string path) => inner.DeleteFile(path);

    /// <summary>Renames through the wrapped environment.</summary>
    public void RenameFile(string source, string target) => inner.RenameFile(source, target);

    /// <summary>
    /// Links through the wrapped environment, or answers false when <see cref="LinkFiles"/> is off. A
    /// link writes its target, so <see cref="WriteGate"/> holds it and <see cref="FailWrites"/> fails it.
    /// </summary>
    public bool TryLinkFile(string source, string target)
    {
        Interlocked.Increment(ref _linkAttempts);
        if (!LinkFiles)
            return false;

        if (GatedPaths(target))
        {
            WriteStarted.Set();
            WriteGate.Wait();
        }

        if (FailWrites && FailingPaths(target))
            throw new IOException("injected link failure");

        return inner.TryLinkFile(source, target);
    }

    /// <summary>Creates through the wrapped environment.</summary>
    public void CreateDirectory(string directory) => inner.CreateDirectory(directory);

    /// <summary>Flushes the directory through the wrapped environment, or fails when <see cref="FailDirectoryFlushes"/> is set.</summary>
    public void FlushDirectory(string directory)
    {
        if (FailDirectoryFlushes)
            throw new IOException("injected directory flush failure");

        inner.FlushDirectory(directory);
    }

    /// <summary>Opens the gates of a <see cref="HookedEnvironment"/> when disposed; see <see cref="OpenGatesAtEnd"/>.</summary>
    public sealed class GateRelease(HookedEnvironment hooks) : IDisposable
    {
        /// <summary>Sets <see cref="WriteGate"/> and <see cref="FlushGate"/>.</summary>
        public void Dispose()
        {
            hooks.WriteGate.Set();
            hooks.FlushGate.Set();
        }
    }

    private sealed class HookedFile(IStorageFile file, string path, HookedEnvironment hooks) : IStorageFile
    {
        private int _disposed;

        public long GetLength() => file.GetLength();

        public int Read(Span<byte> buffer, long offset)
        {
            int read = file.Read(buffer, offset);
            if (read > 0 && hooks.CorruptedReads(path, offset))
                buffer[0] ^= 0xFF;

            return read;
        }

        public void Write(ReadOnlySpan<byte> buffer, long offset)
        {
            if (hooks.GatedPaths(path))
            {
                hooks.WriteStarted.Set();
                hooks.WriteGate.Wait();
            }

            if (hooks.FailWrites && hooks.FailingPaths(path))
                throw new IOException("injected write failure");

            if (hooks.CountedPaths(path))
                Interlocked.Increment(ref hooks._writeCount);

            file.Write(buffer, offset);
        }

        public void SyncRange(long offset, long length)
        {
            if (hooks.FailSyncRanges && hooks.FailingPaths(path))
                throw new IOException("injected sync_file_range failure");

            lock (hooks._syncRanges)
                hooks._syncRanges.Add((path, offset, length));

            file.SyncRange(offset, length);
        }

        public void Flush()
        {
            if (hooks.GatedPaths(path))
            {
                hooks.FlushStarted.Set();
                hooks.FlushGate.Wait();
            }

            if (hooks.FailFlushes && hooks.FailingPaths(path))
                throw new IOException("injected flush failure");

            file.Flush();
        }

        public void SetLength(long length) => file.SetLength(length);

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                Interlocked.Decrement(ref hooks._openFileCount);

            file.Dispose();
        }
    }
}
