using Solarium.Storage;

namespace Solarium.Engine;

/// <summary>
/// Rewrites the tables of a merge into new tables of its output level. The merge holds every version of
/// the keys in its range from the levels it reads, so for each key only the newest version is kept, and
/// only when no range tombstone of the inputs hides it: older versions go. Below the output level, older
/// versions of a key can still exist, as RocksDB's <c>KeyNotExistsBeyondOutputLevel</c> asks: a delete
/// is kept while a lower level's file holds its key, and a range tombstone while one overlaps it, so a
/// merge into the bottom level drops them all. No reader of the new tables needs what is dropped: a
/// reader that took its snapshot before the compaction reads the version it entered, whose tables stay
/// open. Sequence numbers are kept.
/// </summary>
/// <remarks>
/// A kept range tombstone is cut at the boundaries between the output tables, as RocksDB truncates them:
/// each table gets the part from the first key after the table before it up to its next table's first
/// key, which then is the table's largest bound, excluded (<see cref="TableProperties.LargestIsExclusive"/>).
/// So every table's key range covers its tombstones, and two tables of a level never overlap.
/// </remarks>
internal sealed class CompactionMerge
{
    // Keys between two checks for a stop.
    private const int StopCheckInterval = 1024;

    private readonly IStorageEnvironment _environment;
    private readonly TableCaching _caching;   // how the output tables keep their index and filter
    private readonly string _directory;
    private readonly Func<long> _nextFileNumber;
    private readonly long _targetFileSize;
    private readonly CompressionType _compression;
    private readonly TablePacing _pacing;
    private readonly CancellationToken _stop;

    /// <summary>
    /// Prepares merges that write their tables to <paramref name="directory"/> through
    /// <paramref name="environment"/>, numbered by <paramref name="nextFileNumber"/>, with data blocks
    /// stored as <paramref name="compression"/> says and writes paced as <paramref name="pacing"/> says,
    /// and end an output table once it reaches <paramref name="targetFileSize"/> bytes. The output tables
    /// open with their index and filter where <paramref name="caching"/> says. A merge gives up
    /// when <paramref name="stop"/> is cancelled.
    /// </summary>
    public CompactionMerge(IStorageEnvironment environment, string directory, Func<long> nextFileNumber, long targetFileSize, CompressionType compression,
        TablePacing pacing, TableCaching caching, CancellationToken stop)
    {
        _pacing = pacing;
        _caching = caching;
        _environment = environment;
        _directory = directory;
        _nextFileNumber = nextFileNumber;
        _targetFileSize = targetFileSize;
        _compression = compression;
        _stop = stop;
    }

    /// <summary>
    /// Merge the tables of <paramref name="compaction"/> and return the new tables, open, in key
    /// order and flushed; or null when the merge was stopped. On a stop or a failure, every table it
    /// wrote is closed and deleted.
    /// </summary>
    public List<TableReader>? Run(Compaction compaction)
    {
        // Level 0 tables can overlap, one cursor each; the tables of a sorted level share no key, so
        // one cursor walks them all. A compaction reads every input once, so it fills no cache, as
        // RocksDB's compaction reads with fill_cache off.
        List<IEntryCursor> cursors = new(compaction.Inputs.Length + 1);
        List<RangeTombstone> tombstones = [];
        if (compaction.StartLevel == 0)
            foreach (TableReader table in compaction.Inputs)
                cursors.Add(table.NewCursor(fillCache: false));
        else
            cursors.Add(new SortedLevel(compaction.Inputs).NewCursor(fillCache: false));

        if (compaction.OutputLevelInputs.Length > 0)
            cursors.Add(new SortedLevel(compaction.OutputLevelInputs).NewCursor(fillCache: false));

        foreach (TableReader table in compaction.AllInputs)
            tombstones.AddRange(table.Tombstones);

        foreach (IEntryCursor cursor in cursors)
            cursor.SeekToFirst();

        SortedLevel[] below = compaction.Below;
        TombstoneSweep sweep = new(tombstones);
        KeptTombstones kept = new(tombstones, below);
        List<TableReader> outputs = [];
        Output? output = null;
        bool full = false;
        try
        {
            long keys = 0;
            while (true)
            {
                // The smallest key under any cursor, and its newest version: the first entry of the key
                // in each cursor is that cursor's newest version.
                Entry newest = default;
                bool found = false;
                foreach (IEntryCursor cursor in cursors)
                {
                    if (!cursor.Valid)
                        continue;

                    Entry current = cursor.Current;
                    int order = found ? current.Key.AsSpan().SequenceCompareTo(newest.Key) : -1;
                    if (order < 0 || (order == 0 && current.Sequence > newest.Sequence))
                    {
                        newest = current;
                        found = true;
                    }
                }

                if (!found)
                    break;

                if (++keys % StopCheckInterval == 0 && _stop.IsCancellationRequested)
                {
                    Discard(outputs, output);
                    return null;
                }

                bool keep = sweep.Covering(newest.Key) < newest.Sequence
                    && (newest.Kind == OperationKind.Put || MayExistBelow(below, newest.Key));
                if (keep)
                {
                    // A full table ends before this key, with the tombstones below it.
                    if (full)
                    {
                        outputs.Add(output!.Finish(kept.Take(newest.Key)));
                        output = null;
                        full = false;
                    }

                    output ??= Start();
                    output.Builder.Add(newest);

                    // Each key is written once, so the table can end after any of them.
                    full = output.Builder.EstimatedSize >= _targetFileSize;
                }

                foreach (IEntryCursor cursor in cursors)
                    while (cursor.Valid && cursor.Current.Key.AsSpan().SequenceEqual(newest.Key))
                        cursor.Next();
            }

            // The last table takes the tombstones that are left; with no key kept, they get a table of their own.
            List<RangeTombstone> rest = kept.Take(null);
            if (output is not null || rest.Count > 0)
            {
                output ??= Start();
                outputs.Add(output.Finish(rest));
                output = null;
            }

            return outputs;
        }
        catch
        {
            Discard(outputs, output);
            throw;
        }
    }

    private static bool MayExistBelow(SortedLevel[] below, byte[] key)
    {
        foreach (SortedLevel level in below)
            if (level.MayContain(key))
                return true;

        return false;
    }

    private Output Start()
    {
        long number = _nextFileNumber();
        string path = TablePath(number);
        return new Output(number, path, _environment.OpenFile(path), _compression, _pacing, _caching);
    }

    private string TablePath(long number) => Path.Combine(_directory, TableFormat.FileName(number));

    private void Discard(List<TableReader> outputs, Output? output)
    {
        foreach (TableReader table in outputs)
        {
            table.Dispose();
            TryDelete(TablePath(table.Number));
        }

        if (output is not null)
        {
            output.Builder.Dispose();
            output.File.Dispose();
            TryDelete(output.Path);
        }
    }

    private void TryDelete(string path)
    {
        try
        {
            _environment.DeleteFile(path);
        }
        catch (IOException)
        {
        }
    }

    // An output table that is being written. Finish hands its file to the reader it returns.
    private sealed class Output(long number, string path, IStorageFile file, CompressionType compression, TablePacing pacing, TableCaching caching)
    {
        public long Number { get; } = number;

        public string Path { get; } = path;

        public IStorageFile File { get; } = file;

        public TableBuilder Builder { get; } = new(file, compression, pacing);

        public TableReader Finish(IReadOnlyList<RangeTombstone> tombstones)
        {
            Builder.Finish(tombstones);
            return TableReader.Open(Number, File, caching);
        }
    }
}
