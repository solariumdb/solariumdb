using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// SSTable index and filter blocks in a <see cref="Cache"/>: what leaves the readers' memory, reads through
/// evictions and rereads, a cache shared by two databases, level 0 pins and the move that drops them,
/// <see cref="ReadOptions.FillCache"/>, damage found on a reread, memtables charged by a
/// <see cref="WriteBufferManager"/>, and readers racing a writer on a cache too small for the tables.
/// </summary>
public sealed class BlockCacheTests : IDisposable
{
    private const int Megabyte = 1024 * 1024;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void The_cache_holds_the_index_and_filter_that_the_readers_no_longer_keep_and_every_read_finds_its_key(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64 * Megabyte);
        string cachedPath = _directory.Combine("cached");

        using (SolariumDb cached = SolariumDb.Open(cachedPath, Cached(scope, cache)))
        using (SolariumDb resident = SolariumDb.Open(_directory.Combine("resident"), scope.Options()))
        {
            foreach (SolariumDb db in new[] { cached, resident })
                WriteTables(db, tables: 4, keysPerTable: 2000);

            // The cached database's readers keep only their top-level index and their range tombstones, none
            // here: a key and its handles for each partition.
            long cachedBytes = long.Parse(cached.GetProperty("rocksdb.estimate-table-readers-mem")!, System.Globalization.CultureInfo.InvariantCulture);
            long residentBytes = long.Parse(resident.GetProperty("rocksdb.estimate-table-readers-mem")!, System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(cachedBytes > 0 && cachedBytes * 20 < residentBytes, $"the cached readers keep {cachedBytes} bytes, the resident ones {residentBytes}");
            Assert.True(cache.Usage > residentBytes / 2, $"the cache holds {cache.Usage} bytes where the readers held {residentBytes}");
            Assert.Equal(cache.Capacity.ToString(System.Globalization.CultureInfo.InvariantCulture), cached.GetProperty("rocksdb.block-cache-capacity"));
            Assert.Equal(cache.Usage.ToString(System.Globalization.CultureInfo.InvariantCulture), cached.GetProperty("rocksdb.block-cache-usage"));
            Assert.Null(resident.GetProperty("rocksdb.block-cache-usage"));

            AssertEveryKey(cached, tables: 4, keysPerTable: 2000);
        }

        // Closing the database erased its tables from the cache, and a reopen caches them again.
        Assert.Equal(0, cache.Usage);
        using (SolariumDb reopened = SolariumDb.Open(cachedPath, Cached(scope, cache)))
        {
            Assert.True(cache.Usage > 0);
            AssertEveryKey(reopened, tables: 4, keysPerTable: 2000);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Two_databases_that_share_a_cache_smaller_than_one_index_answer_every_read(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(4 * 1024, shardBits: 0);
        DbOptions options = Cached(scope, cache);

        using SolariumDb one = SolariumDb.Open(_directory.Combine("one"), options);
        using SolariumDb two = SolariumDb.Open(_directory.Combine("two"), options);
        WriteTables(one, tables: 3, keysPerTable: 1500);
        WriteTables(two, tables: 3, keysPerTable: 1500);

        long inserts = cache.InsertCount;
        AssertEveryKey(one, tables: 3, keysPerTable: 1500);
        AssertEveryKey(two, tables: 3, keysPerTable: 1500);

        // Every table's blocks are larger than the cache: each lookup read them again, and none stayed.
        Assert.True(cache.InsertCount > inserts + 100, $"{cache.InsertCount - inserts} rereads");
        Assert.True(cache.Usage <= cache.Capacity, $"usage {cache.Usage} over the capacity {cache.Capacity} with nothing pinned");
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Level_0_blocks_stay_pinned_until_a_move_takes_their_table_out_of_level_0(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64 * Megabyte);
        DbOptions options = Cached(scope, cache, pin: true, level0Trigger: 2);

        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        db.Put(B("a1"), B("v"));
        db.Flush();
        TableReader first = db.DefaultColumnFamily.Version.Level0[0];
        Assert.True(first.IsPinned);
        Assert.True(cache.PinnedUsage > 0);

        // Pinned entries outlive a capacity of nothing.
        cache.SetCapacity(0);
        Assert.Equal(cache.PinnedUsage, cache.Usage);
        Assert.Equal("v", S(db.Get(B("a1"))));

        // A second table with keys of its own: the two do not overlap the empty base level, so the
        // compaction moves them, readers and all.
        // The move installs its version, then unpins the tables it took out of level 0, so a reader can see the
        // version before the unpin.
        db.Put(B("b1"), B("w"));
        db.Flush();
        WaitUntil(() => db.DefaultColumnFamily.Version.Level0.Length == 0 && db.DefaultColumnFamily.Version.Tables.All(table => !table.IsPinned));

        // The compaction can move both tables before the flush returns, so they are read from the version.
        TableReader[] moved = [.. db.DefaultColumnFamily.Version.Tables];
        Assert.True(db.CompactionCounts().Moves > 0);
        Assert.Equal(2, moved.Length);
        Assert.Contains(first, moved);
        Assert.All(moved, table => Assert.False(table.IsPinned));
        Assert.Equal(0, cache.PinnedUsage);
        Assert.Equal(0, cache.Usage);
        Assert.Equal("v", S(db.Get(B("a1"))));
        Assert.Equal("w", S(db.Get(B("b1"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_read_with_fill_cache_off_reads_evicted_blocks_without_caching_them(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64 * Megabyte, shardBits: 0);

        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), Cached(scope, cache));
        WriteTables(db, tables: 2, keysPerTable: 1000);
        cache.SetCapacity(0);
        cache.SetCapacity(64 * Megabyte);
        long inserts = cache.InsertCount;

        ReadOptions noFill = new() { FillCache = false };
        Assert.Equal(Value(0, 5), db.Get(Key(0, 5), options: noFill));
        Assert.Equal(Value(1, 7), db.MultiGet([Key(1, 7)], options: noFill)[0]);
        using (Iterator iterator = db.NewIterator(options: noFill))
        {
            int count = 0;
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                count++;

            iterator.SeekToLast();
            iterator.Prev();
            Assert.Equal(2000, count);
        }

        Assert.Equal(inserts, cache.InsertCount);
        Assert.Equal(0, cache.Usage);

        // A plain read caches what it reads again: the newer table, whose filter rules the key out, and the older one.
        Assert.Equal(Value(0, 5), db.Get(Key(0, 5)));
        Assert.Equal(inserts + 2, cache.InsertCount);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_iterator_reads_on_after_its_tables_blocks_are_evicted_and_a_compaction_replaces_its_tables(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64 * Megabyte);

        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), Cached(scope, cache));
        WriteTables(db, tables: 3, keysPerTable: 1000);

        // A table that spans the other three, so the compaction must rewrite them all, not move them.
        db.Put(Key(0, 0), Value(0, 0));
        db.Put(Key(2, 999), Value(2, 999));
        db.Flush();
        long[] before = [.. db.DefaultColumnFamily.Version.Tables.Select(t => t.Number)];

        using Iterator iterator = db.NewIterator();
        iterator.Seek(Key(1, 500));
        Assert.Equal(Key(1, 500), iterator.Key().ToArray());

        cache.SetCapacity(0);
        long inserts = cache.InsertCount;
        db.CompactRange(null, null);
        Assert.DoesNotContain(db.DefaultColumnFamily.Version.Tables, t => before.Contains(t.Number));

        // The merge read its inputs without caching them; only the outputs went in, at their open.
        Assert.Equal(inserts + db.DefaultColumnFamily.Version.Tables.Count(), cache.InsertCount);

        int seen = 0;
        for (; iterator.Valid(); iterator.Next())
            seen++;

        Assert.Equal(1500, seen);
        iterator.SeekToLast();
        Assert.Equal(Key(2, 999), iterator.Key().ToArray());
        iterator.Prev();
        Assert.Equal(Key(2, 998), iterator.Key().ToArray());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_block_damaged_after_open_fails_the_read_that_reads_it_again_naming_the_table_and_the_offset(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        Cache cache = new(64 * Megabyte, shardBits: 0);
        string path = _directory.Combine("db");

        using SolariumDb db = SolariumDb.Open(path, Cached(scope, cache) with { Environment = hooks });
        WriteTables(db, tables: 1, keysPerTable: 500);
        long number = db.DefaultColumnFamily.Version.Level0[0].Number;
        string file = Path.Combine(path, TableFormat.FileName(number));

        // The index block of the table's one partition, which a lookup reads again after an eviction.
        long indexOffset = db.DefaultColumnFamily.Version.Level0[0].PartitionOffsets(0).IndexOffset;

        hooks.CorruptedReads = (read, offset) => Path.GetFileName(read) == TableFormat.FileName(number) && offset == indexOffset;
        Assert.Equal(Value(0, 1), db.Get(Key(0, 1)));   // cached at open: nothing is read again

        cache.SetCapacity(0);
        SolariumException damage = Assert.Throws<SolariumException>(() => db.Get(Key(0, 1)));
        Assert.Contains(TableFormat.FileName(number), damage.Message, StringComparison.Ordinal);
        Assert.Contains($"offset {indexOffset}", damage.Message, StringComparison.Ordinal);

        hooks.CorruptedReads = (_, _) => false;
        Assert.Equal(Value(0, 1), db.Get(Key(0, 1)));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_write_buffer_manager_with_the_cache_charges_the_memtables_pinned_and_evicts_index_blocks_for_them(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(2 * Megabyte, shardBits: 0);
        WriteBufferManager manager = new(64 * Megabyte, cache);
        DbOptions options = Cached(scope, cache) with { WriteBufferManager = manager };

        using (SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options))
        {
            WriteTables(db, tables: 2, keysPerTable: 2000);
            Assert.Equal(0, cache.PinnedUsage);
            long tablesCached = cache.Usage;
            Assert.True(tablesCached > 0);

            // 2 MB in the memtable, charged in whole units of 256 KiB, all pinned: the cache evicts the
            // index blocks to make room, and stays over its capacity by what the pins take.
            for (int i = 0; i < 2000; i++)
                db.Put(B($"m{i:D5}"), new byte[1000]);

            long units = (manager.MemoryUsage + WriteBufferManager.CacheChargeUnit - 1) / WriteBufferManager.CacheChargeUnit;
            Assert.Equal(units * WriteBufferManager.CacheChargeUnit, cache.PinnedUsage);
            Assert.True(cache.Usage - cache.PinnedUsage < tablesCached, "the memtable charge evicted no index block");
            AssertEveryKey(db, tables: 2, keysPerTable: 2000, otherKeys: 2000);

            // The flush hands the memory back, and the charge with it.
            db.Flush();
            Assert.Equal(0, manager.MemoryUsage);
            Assert.Equal(0, cache.PinnedUsage);
        }

        Assert.Equal(0, cache.Usage);
    }

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void Readers_racing_a_writer_and_its_compactions_on_a_cache_too_small_for_the_tables_read_every_value(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(32 * 1024, shardBits: 2);
        DbOptions options = Cached(scope, cache, pin: true, level0Trigger: 2) with { EnablePipelinedWrite = pipelined };

        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        int written = 0;
        bool stop = false;
        Exception? failure = null;

        Thread writer = new(() =>
        {
            for (int table = 0; !Volatile.Read(ref stop); table++)
            {
                for (int i = 0; i < 300; i++)
                    db.Put(Key(table, i), Value(table, i));

                db.Flush();
                Volatile.Write(ref written, table + 1);
            }
        });

        Thread[] readers = new Thread[6];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r;
            readers[r] = new Thread(() =>
            {
                Random random = new(seed);
                try
                {
                    while (!Volatile.Read(ref stop))
                    {
                        int tables = Volatile.Read(ref written);
                        if (tables == 0)
                            continue;

                        int table = random.Next(tables), i = random.Next(300);
                        byte[]? value = db.Get(Key(table, i));
                        if (value is null || !value.AsSpan().SequenceEqual(Value(table, i)))
                            throw new InvalidOperationException($"key {table}/{i} read {S(value) ?? "null"}");
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

        // The race needs a few flushes and the compactions they start beside the readers, however slow the host:
        // on a few CPUs that busy readers fill, an io_uring writer waits for the reactor thread on every write.
        Thread.Sleep(TimeSpan.FromSeconds(3));
        SpinWait.SpinUntil(() => Volatile.Read(ref written) > 3 || Volatile.Read(ref failure) is not null, Timeout);
        Volatile.Write(ref stop, true);
        Assert.True(writer.Join(Timeout), "the writer did not finish");
        foreach (Thread reader in readers)
            Assert.True(reader.Join(Timeout), "a reader did not finish");

        Assert.Null(failure);
        Assert.True(written > 3, $"the writer flushed {written} tables");
        AssertEveryKey(db, tables: written, keysPerTable: 300);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Caching_index_and_filter_blocks_without_a_cache_is_refused(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { CacheIndexAndFilterBlocks = true } };

        ArgumentException refused = Assert.Throws<ArgumentException>(() => SolariumDb.Open(_directory.Combine("db"), options));
        Assert.Contains("BlockCache", refused.Message, StringComparison.Ordinal);
    }

    public void Dispose() => _directory.Dispose();

    private static DbOptions Cached(EnvironmentScope scope, Cache cache, bool pin = false, int level0Trigger = 64) =>
        scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                BlockCache = cache,
                CacheIndexAndFilterBlocks = true,
                PinL0FilterAndIndexBlocksInCache = pin,
                Level0FileNumCompactionTrigger = level0Trigger,
                Level0SlowdownWritesTrigger = Math.Max(20, level0Trigger),
                Level0StopWritesTrigger = Math.Max(36, level0Trigger),
            },
        };

    // One flushed table per table index, each with its own keys.
    private static void WriteTables(SolariumDb db, int tables, int keysPerTable)
    {
        for (int table = 0; table < tables; table++)
        {
            for (int i = 0; i < keysPerTable; i++)
                db.Put(Key(table, i), Value(table, i));

            db.Flush();
        }
    }

    // Every key the tables were written with, and otherKeys more in the iterator's count.
    private static void AssertEveryKey(SolariumDb db, int tables, int keysPerTable, int otherKeys = 0)
    {
        for (int table = 0; table < tables; table++)
            for (int i = 0; i < keysPerTable; i++)
                Assert.Equal(Value(table, i), db.Get(Key(table, i)));

        Assert.Null(db.Get(B("absent")));
        using Iterator iterator = db.NewIterator();
        int count = 0;
        for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
            count++;

        Assert.Equal((tables * keysPerTable) + otherKeys, count);
    }

    private static byte[] Key(int table, int i) => B($"t{table:D3}k{i:D5}");

    private static byte[] Value(int table, int i) => B($"value of {table} {i} {new string('x', 40)}");

    private static void WaitUntil(Func<bool> condition)
    {
        DateTime deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition did not hold in time");
            Thread.Sleep(10);
        }
    }
}
