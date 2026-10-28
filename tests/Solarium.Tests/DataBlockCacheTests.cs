using Solarium.Engine;
using Solarium.IoUring;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Data blocks in <see cref="ColumnFamilyOptions.BlockCache"/> with <see cref="ColumnFamilyOptions.CacheDataBlocks"/>,
/// and none without it: a read of a cached block reads nothing from the table file on every read path;
/// <see cref="ReadOptions.FillCache"/> and compactions read without caching; reads follow overwrites, deletes,
/// flushes, compactions and a reopen through a cache that evicts; two databases that share a cache read their own
/// blocks; a damaged block is never cached; and readers racing a writer that overwrites its keys read only values
/// that were written.
/// </summary>
public sealed class DataBlockCacheTests : IDisposable
{
    private const int Megabyte = 1024 * 1024;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly ReadOptions NoFill = new() { FillCache = false };

    private readonly TempDirectory _directory = new();
    private int _tableReads;

    /// <summary>
    /// Every available environment, with plain tables whose index and filter stay in the readers, and with LZ4
    /// tables whose index and filter the cache holds too.
    /// </summary>
    public static TheoryData<string, bool> EnvironmentsAndTables()
    {
        TheoryData<string, bool> cases = new();
        foreach (string environment in IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
        {
            cases.Add(environment, false);
            cases.Add(environment, true);
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(EnvironmentsAndTables))]
    public void A_second_read_of_cached_blocks_reads_nothing_from_the_tables_on_every_read_path(string environment, bool compressedWithIndexInCache)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = Counted(scope);
        Cache cache = new(64 * Megabyte);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), Options(hooks, cache, compressedWithIndexInCache));
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);

        // Three tables in level 0, each overwriting part of the one before.
        for (int table = 0; table < 3; table++)
        {
            for (int i = table * 300; i < (table * 300) + 900; i += 2)
                Put(db, model, i, table);

            db.Flush();
        }

        Assert.Equal(3, db.DefaultColumnFamily.Version.Level0.Length);
        long usageAfterWrites = cache.Usage;
        for (int pass = 0; pass < 2; pass++)
        {
            Volatile.Write(ref _tableReads, 0);
            AssertReadsAsModel(db, model, options: null);

            // The first pass reads every data block from the files; the second finds them all in the cache.
            if (pass == 0)
                Assert.True(Volatile.Read(ref _tableReads) > 0);
            else
                Assert.Equal(0, Volatile.Read(ref _tableReads));
        }

        Assert.True(cache.Usage > usageAfterWrites, "no data block was cached");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Reads_that_do_not_fill_the_cache_and_compactions_read_blocks_without_caching_them(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = Counted(scope);
        Cache cache = new(64 * Megabyte);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), Options(hooks, cache, compressedWithIndexInCache: false));
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        for (int table = 0; table < 2; table++)
        {
            for (int i = table; i < 1000; i += 2)
                Put(db, model, i, table);

            db.Flush();
        }

        // The index and filter stay in the readers, so the cache holds data blocks only, and a flush caches none.
        Assert.Equal(0, cache.Usage);
        AssertReadsAsModel(db, model, NoFill);
        Assert.True(Volatile.Read(ref _tableReads) > 0);
        Assert.Equal(0, cache.Usage);

        db.CompactRange(null, null);
        Assert.Empty(db.DefaultColumnFamily.Version.Level0);
        Assert.Equal(0, cache.Usage);
        Assert.Equal(0, cache.InsertCount);

        // A plain read caches its block; after it, a read reads nothing, whether or not it fills the cache.
        Assert.Equal(model[KeyText(10)], S(db.Get(Key(10))));
        Assert.Equal(1, cache.InsertCount);
        Volatile.Write(ref _tableReads, 0);
        Assert.Equal(model[KeyText(10)], S(db.Get(Key(10), options: NoFill)));
        Assert.Equal(model[KeyText(10)], S(db.MultiGet([Key(10)], options: NoFill)[0]));
        Assert.Equal(0, Volatile.Read(ref _tableReads));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Reads_follow_overwrites_deletes_flushes_compactions_and_a_reopen_through_a_cache_that_evicts(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(16 * 1024, shardBits: 0);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                BlockCache = cache,
                CacheDataBlocks = true,
                CacheIndexAndFilterBlocks = true,
                Compression = CompressionType.Lz4,
                Level0FileNumCompactionTrigger = 2,
            },
        };

        string path = _directory.Combine("db");
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        Random random = new(11);
        using (SolariumDb db = SolariumDb.Open(path, options))
        {
            for (int round = 0; round < 8; round++)
            {
                for (int n = 0; n < 500; n++)
                    Put(db, model, random.Next(3000), round);

                for (int n = 0; n < 100; n++)
                {
                    int i = random.Next(3000);
                    db.Delete(Key(i));
                    model.Remove(KeyText(i));
                }

                int begin = random.Next(2900);
                db.DeleteRange(Key(begin), Key(begin + 50));
                for (int i = begin; i < begin + 50; i++)
                    model.Remove(KeyText(i));

                db.Flush();
                AssertReadsAsModel(db, model, options: null);
            }

            db.CompactRange(null, null);
            AssertReadsAsModel(db, model, options: null);
            Assert.True(cache.Usage <= cache.Capacity, $"usage {cache.Usage} over the capacity {cache.Capacity} with nothing pinned");
        }

        // The reopened database is a new owner of the same table numbers: it reads its own blocks, not the ones
        // the closed one left in the cache.
        using (SolariumDb reopened = SolariumDb.Open(path, options))
            AssertReadsAsModel(reopened, model, options: null);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Two_databases_that_share_a_cache_and_their_table_numbers_read_their_own_blocks_and_a_close_erases_only_its_own(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = Counted(scope);
        Cache cache = new(64 * Megabyte);
        DbOptions options = Options(hooks, cache, compressedWithIndexInCache: true);
        SolariumDb one = SolariumDb.Open(_directory.Combine("one"), options);
        using SolariumDb two = SolariumDb.Open(_directory.Combine("two"), options);
        SortedDictionary<string, string> oneModel = new(StringComparer.Ordinal);
        SortedDictionary<string, string> twoModel = new(StringComparer.Ordinal);
        for (int i = 0; i < 1000; i++)
        {
            Put(one, oneModel, i, version: 1);
            Put(two, twoModel, i, version: 2);
        }

        one.Flush();
        two.Flush();
        Assert.Equal(one.DefaultColumnFamily.Version.Level0[0].Number, two.DefaultColumnFamily.Version.Level0[0].Number);

        for (int pass = 0; pass < 2; pass++)
        {
            Volatile.Write(ref _tableReads, 0);
            AssertReadsAsModel(one, oneModel, options: null);
            AssertReadsAsModel(two, twoModel, options: null);
            if (pass == 1)
                Assert.Equal(0, Volatile.Read(ref _tableReads));
        }

        // Closing one erases its blocks and partitions, and only those: the other reads on from the cache.
        long both = cache.Usage;
        one.Dispose();
        Assert.InRange(cache.Usage, 1, (both / 2) + 1024);
        Volatile.Write(ref _tableReads, 0);
        AssertReadsAsModel(two, twoModel, options: null);
        Assert.Equal(0, Volatile.Read(ref _tableReads));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_damaged_data_block_fails_the_read_naming_its_offset_and_is_never_cached(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        Cache cache = new(64 * Megabyte);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), Options(hooks, cache, compressedWithIndexInCache: true));
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        for (int i = 0; i < 500; i++)
            Put(db, model, i, version: 1);

        db.Flush();
        TableReader table = db.DefaultColumnFamily.Version.Level0[0];
        Assert.True(table.TryLocate(Key(250), long.MaxValue, fillCache: false, out DataBlockHandle handle));
        string name = TableFormat.FileName(table.Number);

        hooks.CorruptedReads = (path, offset) => Path.GetFileName(path) == name && offset == handle.Offset;
        foreach (Func<byte[]?> read in new Func<byte[]?>[] { () => db.Get(Key(250)), () => db.MultiGet([Key(250)])[0] })
        {
            SolariumException damage = Assert.Throws<SolariumException>(read);
            Assert.Contains(name, damage.Message, StringComparison.Ordinal);
            Assert.Contains($"offset {handle.Offset}", damage.Message, StringComparison.Ordinal);
            Assert.False(table.TryGetCachedBlock(handle, out _));
        }

        // Read whole, the block is cached; the medium's damage after that no longer reaches the reads.
        hooks.CorruptedReads = (_, _) => false;
        Assert.Equal(model[KeyText(250)], S(db.Get(Key(250))));
        Assert.True(table.TryGetCachedBlock(handle, out _));
        hooks.CorruptedReads = (path, offset) => Path.GetFileName(path) == name && offset == handle.Offset;
        Assert.Equal(model[KeyText(250)], S(db.Get(Key(250))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Readers_racing_a_writer_that_overwrites_its_keys_through_flushes_and_compactions_read_only_written_values(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64 * 1024, shardBits: 2);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                BlockCache = cache,
                CacheDataBlocks = true,
                CacheIndexAndFilterBlocks = true,
                Compression = CompressionType.Lz4,
                Level0FileNumCompactionTrigger = 2,
            },
        };

        const int Keys = 2000;
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        for (int i = 0; i < Keys; i++)
            db.Put(Key(i), Value(i, 0));

        db.Flush();
        int versions = 1;
        bool stop = false;
        Exception? failure = null;

        // The writer overwrites every key with a higher version per flush, so each key's versions only grow.
        Thread writer = new(() =>
        {
            for (int version = 1; !Volatile.Read(ref stop); version++)
            {
                for (int i = 0; i < Keys; i++)
                    db.Put(Key(i), Value(i, version));

                db.Flush();
                Volatile.Write(ref versions, version + 1);
            }
        });

        Thread[] readers = new Thread[6];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r;
            readers[r] = new Thread(() =>
            {
                Random random = new(seed);
                int[] seen = new int[Keys];
                try
                {
                    while (!Volatile.Read(ref stop))
                    {
                        int i = random.Next(Keys - 16);
                        byte[]?[] values = seed % 2 == 0
                            ? [db.Get(Key(i))]
                            : db.MultiGet([.. Enumerable.Range(i, 16).Select(Key)]);
                        for (int v = 0; v < values.Length; v++)
                        {
                            int version = VersionOf(i + v, values[v]);
                            if (version < seen[i + v])
                                throw new InvalidOperationException($"key {i + v} read version {version} after {seen[i + v]}");

                            seen[i + v] = version;
                        }
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            });
        }

        writer.Start();
        foreach (Thread reader in readers)
            reader.Start();

        Thread.Sleep(TimeSpan.FromSeconds(2));
        SpinWait.SpinUntil(() => Volatile.Read(ref versions) > 4 || Volatile.Read(ref failure) is not null, Timeout);
        Volatile.Write(ref stop, true);
        Assert.True(writer.Join(Timeout), "the writer did not finish");
        foreach (Thread reader in readers)
            Assert.True(reader.Join(Timeout), "a reader did not finish");

        Assert.Null(failure);
        Assert.True(versions > 4, $"the writer flushed {versions} versions");
        Assert.True(db.CompactionCounts().Merges + db.CompactionCounts().Moves > 0, "no compaction ran beside the readers");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Without_cache_data_blocks_a_block_cache_holds_no_data_block_and_every_read_reads_the_file(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = Counted(scope);
        Cache cache = new(64 * Megabyte);
        DbOptions options = Options(hooks, cache, compressedWithIndexInCache: true);
        options = options with { DefaultColumnFamilyOptions = options.DefaultColumnFamilyOptions with { CacheDataBlocks = false } };
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        SortedDictionary<string, string> model = new(StringComparer.Ordinal);
        for (int i = 0; i < 1000; i++)
            Put(db, model, i, version: 1);

        db.Flush();
        long partitions = cache.Usage;
        for (int pass = 0; pass < 2; pass++)
        {
            Volatile.Write(ref _tableReads, 0);
            AssertReadsAsModel(db, model, options: null);
            Assert.True(Volatile.Read(ref _tableReads) > 0);
        }

        Assert.Equal(partitions, cache.Usage);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Caching_data_blocks_without_a_cache_is_refused(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { CacheDataBlocks = true } };

        ArgumentException refused = Assert.Throws<ArgumentException>(() => SolariumDb.Open(_directory.Combine("db"), options));
        Assert.Contains("CacheDataBlocks", refused.Message, StringComparison.Ordinal);
    }

    public void Dispose() => _directory.Dispose();

    // An environment whose reads of table files add to _tableReads.
    private HookedEnvironment Counted(EnvironmentScope scope) =>
        new(scope.Environment)
        {
            CorruptedReads = (path, _) =>
            {
                if (path.EndsWith(".sst", StringComparison.Ordinal))
                    Interlocked.Increment(ref _tableReads);

                return false;
            },
        };

    private static DbOptions Options(HookedEnvironment hooks, Cache cache, bool compressedWithIndexInCache) =>
        new()
        {
            Environment = hooks,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                BlockCache = cache,
                CacheDataBlocks = true,
                CacheIndexAndFilterBlocks = compressedWithIndexInCache,
                Compression = compressedWithIndexInCache ? CompressionType.Lz4 : CompressionType.None,
                DisableAutoCompactions = true,
            },
        };

    private static void Put(SolariumDb db, SortedDictionary<string, string> model, int i, int version)
    {
        db.Put(Key(i), Value(i, version));
        model[KeyText(i)] = S(Value(i, version))!;
    }

    // Every key of the model by Get, all of them and some absent ones by MultiGet, and the whole store by an
    // iterator forwards, all as the model has them.
    private static void AssertReadsAsModel(SolariumDb db, SortedDictionary<string, string> model, ReadOptions? options)
    {
        foreach (KeyValuePair<string, string> pair in model)
            Assert.Equal(pair.Value, S(db.Get(B(pair.Key), options: options)));

        List<byte[]> keys = [.. model.Keys.Select(B), B("absent"), Key(999_999)];
        byte[]?[] values = db.MultiGet(keys, options: options);
        for (int k = 0; k < keys.Count; k++)
            Assert.Equal(model.GetValueOrDefault(S(keys[k])!), S(values[k]));

        using Iterator iterator = db.NewIterator(options: options);
        using IEnumerator<KeyValuePair<string, string>> expected = model.GetEnumerator();
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            Assert.True(expected.MoveNext(), $"the iterator read {S(iterator.Key())} past the model's end");
            Assert.Equal(expected.Current.Key, S(iterator.Key()));
            Assert.Equal(expected.Current.Value, S(iterator.Value()));
        }

        Assert.False(expected.MoveNext(), "the iterator ended before the model");
    }

    private static string KeyText(int i) => $"key{i:D6}";

    private static byte[] Key(int i) => B(KeyText(i));

    // A value that names its key and version, padded to about 100 bytes, so a block holds tens of entries and LZ4
    // has something to compress.
    private static byte[] Value(int i, int version) => B($"{KeyText(i)}@{version:D6}|{new string((char)('a' + (i % 26)), 80)}");

    // The version of key i's value; throws when the value is missing or another key's.
    private static int VersionOf(int i, byte[]? value)
    {
        string text = S(value) ?? throw new InvalidOperationException($"key {i} read null");
        if (!text.StartsWith(KeyText(i) + "@", StringComparison.Ordinal))
            throw new InvalidOperationException($"key {i} read {text}");

        return int.Parse(text.AsSpan(KeyText(i).Length + 1, 6), System.Globalization.CultureInfo.InvariantCulture);
    }
}
