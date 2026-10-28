using Solarium.Storage;

namespace Solarium.Tests;

/// <summary>
/// Forwards every operation of one database directory to another environment, and keeps beside
/// it what a power loss would leave: the bytes of each file as of its last flush, and the
/// directory's entries as of its last directory flush. <see cref="CrashAfter"/> arms a crash at a
/// given operation; at that moment the state a power loss can leave is written to a new directory,
/// while the database goes on running on the real files, as RocksDB's <c>FaultInjectionTestFS</c>.
/// </summary>
/// <remarks>
/// The files of the directory when the environment is created count as durable. The real files
/// never face a crash, since a crash writes its state from the model, so their flushes are skipped:
/// a flush only moves the model's durable point, which keeps a run fast. The model is
/// conservative: a write counts as durable only when a flush of its file started after the write
/// returned, and an entry change only when a flush of its directory started after it.
/// </remarks>
public sealed class PowerLossEnvironment : IStorageEnvironment
{
    // The unit in which a device persists bytes: a power loss keeps or loses each one whole.
    private const int SectorSize = 512;

    private readonly IStorageEnvironment _inner;
    private readonly string _directory;
    private readonly object _lock = new();
    private readonly Dictionary<string, Image> _entries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Image> _durableEntries = new(StringComparer.Ordinal);
    private readonly List<EntryChange> _pendingEntries = [];
    private long _operations;
    private long _crashAt = long.MaxValue;
    private Action? _crash;

    /// <summary>Watches <paramref name="directory"/>, which must exist, through <paramref name="inner"/>; its files count as durable.</summary>
    public PowerLossEnvironment(IStorageEnvironment inner, string directory)
    {
        _inner = inner;
        _directory = Path.GetFullPath(directory);
        foreach (string path in Directory.GetFiles(_directory))
        {
            if (Path.GetFileName(path) == "LOCK")
                continue;

            Image image = new(File.ReadAllBytes(path));
            _entries[Path.GetFileName(path)] = image;
            _durableEntries[Path.GetFileName(path)] = image;
        }
    }

    /// <summary>How a crash treats what was not flushed.</summary>
    public enum Loss
    {
        /// <summary>Only flushed bytes and flushed directory entries survive.</summary>
        Unflushed,

        /// <summary>
        /// The device also wrote part of the rest, in order: each file keeps its unflushed bytes up
        /// to a random sector, and the directory a random prefix of its unflushed changes.
        /// </summary>
        InOrder,

        /// <summary>
        /// As <see cref="InOrder"/>, but each unflushed sector of a file survives or not on its own,
        /// as write-back in any order leaves it: a sector that was not written reads as zeros.
        /// </summary>
        AnyOrder,

        /// <summary>
        /// A process crash, not a power loss: the OS keeps every write and directory change made
        /// before the crash, flushed or not, and only the process stops.
        /// </summary>
        None,
    }

    /// <summary>True once the armed crash has happened.</summary>
    public bool Crashed { get; private set; }

    /// <summary>The operations so far: opens of new files, writes, length changes, flushes, renames, links, deletes and directory flushes.</summary>
    public long Operations
    {
        get
        {
            lock (_lock)
            {
                return _operations;
            }
        }
    }

    /// <summary>The wrapped environment's name, prefixed.</summary>
    public string Name => $"power loss over {_inner.Name}";

    /// <summary>
    /// Arms a crash at operation <paramref name="operation"/> (counted from the start): just
    /// before it runs, the state that <paramref name="loss"/> leaves, drawn with
    /// <paramref name="random"/>, is written to <paramref name="target"/>, a directory that does
    /// not exist yet. With an operation at or below <see cref="Operations"/>, the crash happens now.
    /// </summary>
    public void CrashAfter(long operation, string target, Loss loss, Random random)
    {
        lock (_lock)
        {
            _crash = () => Write(target, loss, random);
            _crashAt = operation;
            if (_operations >= operation)
                CrashNow();
        }
    }

    /// <inheritdoc />
    public IStorageFile OpenFile(string path)
    {
        string name = NameOf(path);
        lock (_lock)
        {
            if (!_entries.TryGetValue(name, out Image? image))
            {
                Count();
                image = new Image([]);
                _entries[name] = image;
                _pendingEntries.Add(new EntryChange(name, null, image));
            }

            return new PowerLossFile(_inner.OpenFile(path), image, this);
        }
    }

    /// <inheritdoc />
    public bool FileExists(string path) => _inner.FileExists(path);

    /// <inheritdoc />
    public string[] GetChildren(string directory) => _inner.GetChildren(directory);

    /// <inheritdoc />
    public void DeleteFile(string path)
    {
        string name = NameOf(path);
        lock (_lock)
        {
            Count();
            _inner.DeleteFile(path);
            if (_entries.Remove(name))
                _pendingEntries.Add(new EntryChange(name, null, null));
        }
    }

    /// <inheritdoc />
    public void RenameFile(string source, string target)
    {
        string from = NameOf(source);
        string to = NameOf(target);
        lock (_lock)
        {
            Count();
            _inner.RenameFile(source, target);
            Image image = _entries[from];
            _entries.Remove(from);
            _entries[to] = image;
            _pendingEntries.Add(new EntryChange(to, from, image));
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// A link into the directory is a new entry that shares the source's image, as a hard link shares
    /// its file: a write through either name changes both. A link elsewhere is not recorded.
    /// </remarks>
    public bool TryLinkFile(string source, string target)
    {
        string full = Path.GetFullPath(target);
        if (Path.GetDirectoryName(full) != _directory)
            return _inner.TryLinkFile(source, target);

        string name = NameOf(target);
        lock (_lock)
        {
            Count();
            if (!_inner.TryLinkFile(source, target))
                return false;

            string from = Path.GetFullPath(source);
            Image image = Path.GetDirectoryName(from) == _directory ? _entries[Path.GetFileName(from)] : new Image(File.ReadAllBytes(from));
            _entries[name] = image;
            _pendingEntries.Add(new EntryChange(name, null, image));
            return true;
        }
    }

    /// <inheritdoc />
    public void CreateDirectory(string directory) => _inner.CreateDirectory(directory);

    /// <inheritdoc />
    public void FlushDirectory(string directory)
    {
        if (Path.GetFullPath(directory) != _directory)
        {
            _inner.FlushDirectory(directory);
            return;
        }

        lock (_lock)
        {
            Count();
            foreach (EntryChange change in _pendingEntries)
                Apply(_durableEntries, change);

            _pendingEntries.Clear();
        }
    }

    // Under the lock: one more operation, and the crash when it is the armed one.
    private void Count()
    {
        _operations++;
        if (_operations >= _crashAt && !Crashed)
            CrashNow();
    }

    // Under the lock.
    private void CrashNow()
    {
        Crashed = true;
        _crash!();
    }

    private string NameOf(string path)
    {
        string full = Path.GetFullPath(path);
        if (Path.GetDirectoryName(full) != _directory)
            throw new InvalidOperationException($"'{path}' is outside '{_directory}'.");

        return Path.GetFileName(full);
    }

    private static void Apply(Dictionary<string, Image> entries, EntryChange change)
    {
        if (change.From is not null)
            entries.Remove(change.From);

        if (change.Image is null)
            entries.Remove(change.Name);
        else
            entries[change.Name] = change.Image;
    }

    // Under the lock: the directory a power loss leaves now.
    private void Write(string target, Loss loss, Random random)
    {
        Dictionary<string, Image> entries = new(_durableEntries, StringComparer.Ordinal);
        if (loss != Loss.Unflushed)
        {
            int kept = loss == Loss.None ? _pendingEntries.Count : random.Next(_pendingEntries.Count + 1);
            for (int i = 0; i < kept; i++)
                Apply(entries, _pendingEntries[i]);
        }

        Directory.CreateDirectory(target);
        foreach ((string name, Image image) in entries)
            File.WriteAllBytes(Path.Combine(target, name), image.AfterLoss(loss, random));
    }

    /// <summary>A change to a directory entry: a new file or link, a rename onto <see cref="Name"/>, or a delete (no image).</summary>
    private sealed record EntryChange(string Name, string? From, Image? Image);

    /// <summary>One file's bytes: the flushed ones, and the changes since its last flush, in order.</summary>
    private sealed class Image(byte[] durable)
    {
        private byte[] _durable = durable;
        private long _persisted;

        /// <summary>The writes and length changes not yet durable, in order. Under the environment's lock.</summary>
        public List<(long Offset, byte[]? Bytes, long Length)> Pending { get; } = [];

        /// <summary>The changes recorded since the file was created, durable or not. Under the environment's lock.</summary>
        public long Recorded => _persisted + Pending.Count;

        /// <summary>
        /// Under the environment's lock: the changes up to the <paramref name="recorded"/>-th are durable.
        /// Flushes of one file from several handles overlap, so a flush may find a later one did it.
        /// </summary>
        public void Persist(long recorded)
        {
            int count = (int)(recorded - _persisted);
            if (count <= 0)
                return;

            _durable = Applied(_durable, Pending.Take(count));
            Pending.RemoveRange(0, count);
            _persisted = recorded;
        }

        /// <summary>Under the environment's lock: the bytes that <paramref name="loss"/> leaves.</summary>
        public byte[] AfterLoss(Loss loss, Random random)
        {
            if (loss == Loss.Unflushed || Pending.Count == 0)
                return _durable;

            byte[] current = Applied(_durable, Pending);
            if (loss == Loss.None)
                return current;

            int shorter = Math.Min(_durable.Length, current.Length);
            int longer = Math.Max(_durable.Length, current.Length);

            // The length the device recorded: anything from one to the other, at a sector or an end.
            int length = random.Next(3) switch
            {
                0 => current.Length,
                1 => _durable.Length,
                _ => Math.Min(longer, shorter + (random.Next(((longer - shorter) / SectorSize) + 1) * SectorSize)),
            };

            // Each sector comes back whole, as it was flushed or as it was last written: a device
            // writes a sector or not, so the flushed bytes of a sector that holds new bytes too
            // survive either way. Past the end of either version, a sector reads as zeros.
            byte[] result = new byte[length];
            for (int start = 0; start < length; start += SectorSize)
            {
                int end = Math.Min(start + SectorSize, length);
                Span<byte> before = Sector(_durable, start, end);
                Span<byte> after = Sector(current, start, end);
                bool written = !before.SequenceEqual(after) && (loss == Loss.InOrder || random.Next(2) == 0);
                (written ? after : before).CopyTo(result.AsSpan(start));
            }

            return result;
        }

        // Bytes start to end of a file's version, with zeros past its end.
        private static Span<byte> Sector(byte[] bytes, int start, int end)
        {
            byte[] sector = new byte[end - start];
            if (start < bytes.Length)
                bytes.AsSpan(start, Math.Min(end, bytes.Length) - start).CopyTo(sector);

            return sector;
        }

        private static byte[] Applied(byte[] bytes, IEnumerable<(long Offset, byte[]? Bytes, long Length)> changes)
        {
            byte[] result = bytes;
            foreach ((long offset, byte[]? written, long length) in changes)
            {
                long size = written is null ? length : Math.Max(result.Length, offset + written.Length);
                if (size != result.Length)
                {
                    byte[] resized = new byte[size];
                    result.AsSpan(0, (int)Math.Min(result.Length, size)).CopyTo(resized);
                    result = resized;
                }
                else if (ReferenceEquals(result, bytes))
                {
                    result = (byte[])bytes.Clone();
                }

                written?.CopyTo(result.AsSpan((int)offset));
            }

            return result;
        }
    }

    /// <summary>A file whose writes, length changes and flushes the environment records.</summary>
    private sealed class PowerLossFile(IStorageFile file, Image image, PowerLossEnvironment owner) : IStorageFile
    {
        public long GetLength() => file.GetLength();

        public int Read(Span<byte> buffer, long offset) => file.Read(buffer, offset);

        public void Write(ReadOnlySpan<byte> buffer, long offset)
        {
            byte[] copy = buffer.ToArray();
            lock (owner._lock)
                owner.Count();

            file.Write(buffer, offset);
            lock (owner._lock)
                image.Pending.Add((offset, copy, 0));
        }

        public void SetLength(long length)
        {
            lock (owner._lock)
                owner.Count();

            file.SetLength(length);
            lock (owner._lock)
                image.Pending.Add((0, null, length));
        }

        public void Flush()
        {
            // The real file is not flushed; see the remarks of the environment.
            lock (owner._lock)
            {
                owner.Count();
                image.Persist(image.Recorded);
            }
        }

        // A range's writeback makes nothing durable, so a power loss after it keeps what it kept before.
        public void SyncRange(long offset, long length)
        {
        }

        public void Dispose() => file.Dispose();
    }
}
