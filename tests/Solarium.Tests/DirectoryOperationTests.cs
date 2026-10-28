using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Every file of a database goes through its storage environment, so an environment that simulates
/// a power loss sees each create, write, rename, delete and directory flush.
/// </summary>
public sealed class DirectoryOperationTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_file_is_created_renamed_and_deleted_through_the_environment(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        RecordingEnvironment recording = new(scope.Environment);
        DbOptions options = scope.Options(createMissingColumnFamilies: true) with
        {
            Environment = recording,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 16 * 1024, MaxWriteBufferNumber = 3, Level0FileNumCompactionTrigger = 2 },
            MaxTotalWalSize = 64 * 1024,
        };

        // Flushes, WAL rotation and deletion, moves, merges, a drop, orphans at open, and a reopen.
        for (int round = 0; round < 2; round++)
        {
            using SolariumDb db = SolariumDb.Open(DbPath, options, ["other"]);
            ColumnFamily other = db.GetColumnFamily("other");
            for (int i = 0; i < 1500; i++)
            {
                db.Put(B($"k{i % 600:D4}"), new byte[100]);
                if (i % 7 == 0)
                    db.Put(B($"o{i:D4}"), B("v"), other);
            }

            db.Flush();
            db.CompactRange(null, null);
            db.DeleteFilesInRange(B("k0000"), B("k0300"));
            db.Flush(other);
        }

        HashSet<string> onDisk = [.. Directory.GetFiles(DbPath).Select(path => Path.GetFileName(path)).Where(name => name != "LOCK")];
        Assert.Equal(recording.Live.Order(), onDisk.Order());
        Assert.Contains(recording.Log, entry => entry.StartsWith("delete ", StringComparison.Ordinal) && entry.EndsWith(".log", StringComparison.Ordinal));
        Assert.Contains(recording.Log, entry => entry.StartsWith("delete ", StringComparison.Ordinal) && entry.EndsWith(".sst", StringComparison.Ordinal));

        // Each manifest replace: the temporary file is flushed, renamed over the manifest, and the
        // directory is flushed before the next save starts. Other threads' files interleave.
        int replaces = 0;
        for (int i = 0; i < recording.Log.Count; i++)
        {
            if (recording.Log[i] != "rename MANIFEST.tmp MANIFEST")
                continue;

            replaces++;
            Assert.Equal("flush MANIFEST.tmp", recording.Log[..i].Last(entry => entry.EndsWith("MANIFEST.tmp", StringComparison.Ordinal)));
            Assert.Equal("flush directory", recording.Log[(i + 1)..].First(entry => entry == "flush directory" || entry.Contains("MANIFEST", StringComparison.Ordinal)));
        }

        Assert.True(replaces > 5, $"{replaces} manifest saves");
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    /// <summary>
    /// Forwards to another environment and records, by file name, the files it holds and each
    /// operation: open, flush, rename, link, delete and directory flush.
    /// </summary>
    private sealed class RecordingEnvironment(IStorageEnvironment inner) : IStorageEnvironment
    {
        private readonly object _lock = new();

        /// <summary>The files that the operations so far leave in the directory.</summary>
        public HashSet<string> Live { get; } = [];

        /// <summary>Every operation in order, as "open NAME", "flush NAME", "rename FROM TO", "link FROM TO", "delete NAME" or "flush directory".</summary>
        public List<string> Log { get; } = [];

        /// <summary>The wrapped environment's name.</summary>
        public string Name => inner.Name;

        /// <summary>Opens through the wrapped environment; the file is live from now on.</summary>
        public IStorageFile OpenFile(string path)
        {
            Record($"open {Path.GetFileName(path)}", () => Live.Add(Path.GetFileName(path)));
            return new RecordingFile(inner.OpenFile(path), Path.GetFileName(path), this);
        }

        /// <summary>Asks the wrapped environment.</summary>
        public bool FileExists(string path) => inner.FileExists(path);

        /// <summary>Lists through the wrapped environment.</summary>
        public string[] GetChildren(string directory) => inner.GetChildren(directory);

        /// <summary>Deletes through the wrapped environment.</summary>
        public void DeleteFile(string path)
        {
            inner.DeleteFile(path);
            Record($"delete {Path.GetFileName(path)}", () => Live.Remove(Path.GetFileName(path)));
        }

        /// <summary>Renames through the wrapped environment.</summary>
        public void RenameFile(string source, string target)
        {
            inner.RenameFile(source, target);
            Record($"rename {Path.GetFileName(source)} {Path.GetFileName(target)}", () =>
            {
                Live.Remove(Path.GetFileName(source));
                Live.Add(Path.GetFileName(target));
            });
        }

        /// <summary>Links through the wrapped environment; the target is live from now on when the link is made.</summary>
        public bool TryLinkFile(string source, string target)
        {
            if (!inner.TryLinkFile(source, target))
                return false;

            Record($"link {Path.GetFileName(source)} {Path.GetFileName(target)}", () => Live.Add(Path.GetFileName(target)));
            return true;
        }

        /// <summary>Creates through the wrapped environment.</summary>
        public void CreateDirectory(string directory) => inner.CreateDirectory(directory);

        /// <summary>Flushes through the wrapped environment.</summary>
        public void FlushDirectory(string directory)
        {
            inner.FlushDirectory(directory);
            Record("flush directory", () => { });
        }

        private void Record(string entry, Action change)
        {
            lock (_lock)
            {
                Log.Add(entry);
                change();
            }
        }

        private sealed class RecordingFile(IStorageFile file, string name, RecordingEnvironment owner) : IStorageFile
        {
            public long GetLength() => file.GetLength();

            public int Read(Span<byte> buffer, long offset) => file.Read(buffer, offset);

            public void Write(ReadOnlySpan<byte> buffer, long offset) => file.Write(buffer, offset);

            public void Flush()
            {
                file.Flush();
                owner.Record($"flush {name}", () => { });
            }

            public void SyncRange(long offset, long length) => file.SyncRange(offset, length);

            public void SetLength(long length) => file.SetLength(length);

            public void Dispose() => file.Dispose();
        }
    }
}
