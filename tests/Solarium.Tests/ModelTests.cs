using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Random operations against the database and against a sorted dictionary at the same time, with
/// small memtables so data moves through memtables, immutable memtables and SSTables. Every read
/// path must agree with the dictionary.
/// </summary>
public sealed class ModelTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void Every_read_path_agrees_with_a_sorted_dictionary(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = new DbOptions
        {
            Environment = scope.Environment,
            EnablePipelinedWrite = pipelined,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 16 * 1024, MaxWriteBufferNumber = 3 },
        };

        string path = _directory.Combine("db");
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        Random random = new(20261017);
        SolariumDb db = SolariumDb.Open(path, options);

        try
        {
            for (int step = 1; step <= 6000; step++)
            {
                int choice = random.Next(1000);
                string key = $"k{random.Next(400):D3}";

                if (choice < 700)
                {
                    string value = $"{step}:" + new string('x', random.Next(50, 250));
                    db.Put(B(key), B(value));
                    model[key] = value;
                }
                else if (choice < 850)
                {
                    db.Delete(B(key));
                    model.Remove(key);
                }
                else if (choice < 880)
                {
                    string end = $"k{random.Next(400):D3}";
                    (string begin, string last) = string.CompareOrdinal(key, end) <= 0 ? (key, end) : (end, key);
                    db.DeleteRange(B(begin), B(last));
                    foreach (string doomed in model.Keys.Where(k => string.CompareOrdinal(k, begin) >= 0 && string.CompareOrdinal(k, last) < 0).ToList())
                        model.Remove(doomed);
                }
                else if (choice < 890)
                    db.Flush();
                else if (choice < 893)
                {
                    db.Dispose();
                    db = SolariumDb.Open(path, options);
                }
                else
                    Assert.Equal(model.GetValueOrDefault(key), S(db.Get(B(key))));

                if (step % 500 == 0)
                    Verify(db, model, random);
            }

            Assert.True(db.FlushCount > 0 || Directory.GetFiles(path, "*.sst").Length > 0, "no flush happened");
            db.Dispose();
            db = SolariumDb.Open(path, options);
            Verify(db, model, random);
        }
        finally
        {
            db.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_read_path_agrees_with_a_sorted_dictionary_across_moves_and_merges(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        AgreeAcrossCompactions(scope, new ColumnFamilyOptions
        {
            WriteBufferSize = 8 * 1024,
            MaxWriteBufferNumber = 3,
            Level0FileNumCompactionTrigger = 2,
            TargetFileSizeBase = 4 * 1024,
            Compression = CompressionType.Lz4,
        }, middleLevels: false);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_read_path_agrees_with_a_sorted_dictionary_across_the_levels(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        AgreeAcrossCompactions(scope, new ColumnFamilyOptions
        {
            WriteBufferSize = 8 * 1024,
            MaxWriteBufferNumber = 3,
            Level0FileNumCompactionTrigger = 2,
            TargetFileSizeBase = 2 * 1024,
            MaxBytesForLevelBase = 8 * 1024,
            MaxBytesForLevelMultiplier = 2,
        }, middleLevels: true);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_read_path_agrees_with_a_sorted_dictionary_across_the_levels_with_four_compaction_threads(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        AgreeAcrossCompactions(scope, new ColumnFamilyOptions
        {
            WriteBufferSize = 8 * 1024,
            MaxWriteBufferNumber = 3,
            Level0FileNumCompactionTrigger = 2,
            TargetFileSizeBase = 2 * 1024,
            MaxBytesForLevelBase = 8 * 1024,
            MaxBytesForLevelMultiplier = 2,
        }, middleLevels: true, compactionThreads: 4);
    }

    // A random workload of puts, deletes, range deletes, flushes, manual compactions and reopens,
    // checked against a dictionary; with middleLevels, the tables must have reached levels 1 to 5.
    private void AgreeAcrossCompactions(EnvironmentScope scope, ColumnFamilyOptions familyOptions, bool middleLevels, int compactionThreads = 1)
    {
        DbOptions options = new DbOptions
        {
            Environment = scope.Environment,
            DefaultColumnFamilyOptions = familyOptions,
            MaxBackgroundCompactions = compactionThreads,
        };

        string path = _directory.Combine("db");
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        Random random = new(20261021);
        SolariumDb db = SolariumDb.Open(path, options);
        long moves = 0, merges = 0;
        bool reachedMiddle = false;

        try
        {
            for (int step = 1; step <= 8000; step++)
            {
                int choice = random.Next(1000);

                // Keys from a window that moves up, so some flushes overlap nothing and move, and
                // others land on older keys and merge.
                int window = step / 40;
                string key = $"k{window + random.Next(random.Next(4) == 0 ? 400 : 20):D4}";

                if (choice < 700)
                {
                    string value = $"{step}:" + new string('x', random.Next(20, 120));
                    db.Put(B(key), B(value));
                    model[key] = value;
                }
                else if (choice < 840)
                {
                    db.Delete(B(key));
                    model.Remove(key);
                }
                else if (choice < 870)
                {
                    string end = $"k{window + random.Next(400):D4}";
                    (string begin, string last) = string.CompareOrdinal(key, end) <= 0 ? (key, end) : (end, key);
                    db.DeleteRange(B(begin), B(last));
                    foreach (string doomed in model.Keys.Where(k => string.CompareOrdinal(k, begin) >= 0 && string.CompareOrdinal(k, last) < 0).ToList())
                        model.Remove(doomed);
                }
                else if (choice < 885)
                    db.Flush();
                else if (choice < 890)
                {
                    string end = $"k{window + random.Next(400):D4}";
                    (string begin, string last) = string.CompareOrdinal(key, end) <= 0 ? (key, end) : (end, key);
                    db.CompactRange(random.Next(5) == 0 ? null : B(begin), random.Next(5) == 0 ? null : B(last));
                    CheckLevels(db);
                    Verify(db, model, random, i => $"k{window + i:D4}");
                }
                else if (choice < 892)
                {
                    (long Moves, long MovedFiles, long Merges, long BytesRead, long BytesWritten) counts = db.CompactionCounts();
                    moves += counts.Moves;
                    merges += counts.Merges;
                    db.Dispose();
                    db = SolariumDb.Open(path, options);
                }
                else
                    Assert.Equal(model.GetValueOrDefault(key), S(db.Get(B(key))));

                if (step % 500 == 0)
                {
                    reachedMiddle |= db.GetLiveFilesMetaData().Any(f => f.Level is > 0 and < Manifest.BottomLevel);
                    CheckLevels(db);
                    Verify(db, model, random, i => $"k{window + i:D4}");
                }
            }

            (long Moves, long MovedFiles, long Merges, long BytesRead, long BytesWritten) final = db.CompactionCounts();
            Assert.True(moves + final.Moves > 0, "no table moved");
            Assert.True(merges + final.Merges > 0, "no tables merged");
            Assert.Equal(middleLevels, reachedMiddle);

            db.Dispose();
            db = SolariumDb.Open(path, options);
            CheckLevels(db);
            Verify(db, model, random, i => $"k{8000 / 40 + i:D4}");
        }
        finally
        {
            db.Dispose();
        }
    }

    // Level 0 first, then each sorted level in key order with no shared key, as the manifest and the version list them.
    private static void CheckLevels(SolariumDb db)
    {
        int[] levels = [.. db.GetLiveFilesMetaData().Where(f => f.ColumnFamilyName == ColumnFamily.DefaultName).Select(f => f.Level)];
        Assert.Equal(levels.Order(), levels);

        FamilyVersion version = db.DefaultColumnFamily.Version;
        for (int level = 1; level < version.Levels.Length; level++)
        {
            TableReader[] tables = version.Levels[level].Tables;
            for (int i = 1; i < tables.Length; i++)
                Assert.False(tables[i - 1].Properties.Overlaps(tables[i].Properties), $"tables {tables[i - 1].Number} and {tables[i].Number} overlap at level {level}");
        }
    }

    // Memtables and tables that hold many range deletes, overlapping, nested and repeated, so reads
    // search the fragmented tombstones rather than scan them. An iterator opened before a burst of
    // deletes must still see the state at its snapshot, so the search must skip newer tombstones.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_read_path_agrees_with_a_sorted_dictionary_beside_many_overlapping_range_deletes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = new DbOptions
        {
            Environment = scope.Environment,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, MaxWriteBufferNumber = 3, Level0FileNumCompactionTrigger = 3 },
        };

        string path = _directory.Combine("db");
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        Random random = new(20261022);
        SolariumDb db = SolariumDb.Open(path, options);

        try
        {
            for (int step = 1; step <= 6000; step++)
            {
                int choice = random.Next(1000);
                string key = $"k{random.Next(400):D3}";

                if (choice < 650)
                {
                    string value = $"{step}";
                    db.Put(B(key), B(value));
                    model[key] = value;
                }
                else if (choice < 900)
                {
                    // Short and long ranges, so they nest and overlap.
                    string end = $"k{Math.Min(399, random.Next(400) + random.Next(random.Next(4) == 0 ? 200 : 8)):D3}";
                    (string begin, string last) = string.CompareOrdinal(key, end) <= 0 ? (key, end) : (end, key);
                    db.DeleteRange(B(begin), B(last));
                    foreach (string doomed in model.Keys.Where(k => string.CompareOrdinal(k, begin) >= 0 && string.CompareOrdinal(k, last) < 0).ToList())
                        model.Remove(doomed);
                }
                else if (choice < 905)
                    db.Flush();
                else if (choice < 907)
                {
                    db.Dispose();
                    db = SolariumDb.Open(path, options);
                }
                else if (choice < 910)
                {
                    // An iterator keeps its snapshot through a burst of deletes.
                    List<(string, string)> before = [.. model.Select(p => (p.Key, p.Value))];
                    using Iterator iterator = db.NewIterator();
                    for (int i = 0; i < 20; i++)
                    {
                        string begin = $"k{random.Next(400):D3}";
                        string end = $"k{random.Next(400):D3}";
                        if (string.CompareOrdinal(begin, end) > 0)
                            (begin, end) = (end, begin);

                        db.DeleteRange(B(begin), B(end));
                        foreach (string doomed in model.Keys.Where(k => string.CompareOrdinal(k, begin) >= 0 && string.CompareOrdinal(k, end) < 0).ToList())
                            model.Remove(doomed);
                    }

                    List<(string, string)> seen = [];
                    for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                        seen.Add((S(iterator.Key()), S(iterator.Value())));

                    Assert.Equal(before, seen);
                }
                else
                    Assert.Equal(model.GetValueOrDefault(key), S(db.Get(B(key))));

                if (step % 500 == 0)
                    Verify(db, model, random);
            }

            Assert.True(db.DefaultColumnFamily.Version.Active.Tombstones.Count > MemTable.LinearTombstoneLimit
                || db.DefaultColumnFamily.Version.Level0.Length > 0, "no source held more tombstones than a scan covers");
            db.Dispose();
            db = SolariumDb.Open(path, options);
            Verify(db, model, random);
        }
        finally
        {
            db.Dispose();
        }
    }

    private static void Verify(SolariumDb db, SortedDictionary<string, string> model, Random random) => Verify(db, model, random, i => $"k{i:D3}");

    // Full scans, and point reads and seeks on keyAt(0) to keyAt(399).
    private static void Verify(SolariumDb db, SortedDictionary<string, string> model, Random random, Func<int, string> keyAt)
    {

        // Point reads, present and absent.
        for (int i = 0; i < 400; i++)
        {
            string key = keyAt(i);
            Assert.Equal(model.GetValueOrDefault(key), S(db.Get(B(key))));
        }

        // Full scans in both directions.
        using (Iterator iterator = db.NewIterator())
        {
            List<(string, string)> forward = new();
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                forward.Add((S(iterator.Key()), S(iterator.Value())));

            Assert.Equal(model.Select(p => (p.Key, p.Value)), forward);

            List<string> backward = new();
            for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
                backward.Add(S(iterator.Key()));

            Assert.Equal(model.Keys.Reverse(), backward);
        }

        // Seeks, then a few steps, with and without an upper bound.
        for (int i = 0; i < 20; i++)
        {
            string target = keyAt(random.Next(400));
            string bound = keyAt(random.Next(400));
            ReadOptions options = new ReadOptions { IterateUpperBound = B(bound) };

            using Iterator iterator = db.NewIterator(options: options);
            iterator.Seek(B(target));
            List<string> expected = [.. model.Keys.Where(k => string.CompareOrdinal(k, target) >= 0 && string.CompareOrdinal(k, bound) < 0).Take(5)];
            List<string> actual = new();
            for (int n = 0; n < 5 && iterator.Valid(); n++, iterator.Next())
                actual.Add(S(iterator.Key()));

            Assert.Equal(expected, actual);

            // Next after Prev: the direction change.
            if (actual.Count >= 2)
            {
                iterator.Seek(B(actual[1]));
                iterator.Prev();
                Assert.Equal(actual[0], S(iterator.Key()));
                iterator.Next();
                Assert.Equal(actual[1], S(iterator.Key()));
            }

            iterator.SeekToLast();
            Assert.Equal(model.Keys.LastOrDefault(k => string.CompareOrdinal(k, bound) < 0), iterator.Valid() ? S(iterator.Key()) : null);
        }
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();
}
