using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Reads of a value in place (<see cref="SolariumDb.TryGet{TResult}"/>): the reader gets the value of every kind of
/// source and is called only for a key that has one; the bytes stay its own while it runs, across a flush, an
/// eviction, a compaction and the close of the database, and the read lets go of them when it returns or throws; and
/// the read copies no value.
/// </summary>
public sealed class ValueReaderTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    // ── What a reader gets ───────────────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_reader_gets_the_value_of_every_source_and_none_of_a_key_that_is_absent_deleted_or_covered(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (CompressionType compression in new[] { CompressionType.None, CompressionType.Lz4 })
        {
            foreach (bool cached in new[] { false, true })
            {
                string path = _directory.Combine($"db-{compression}-{cached}");
                HookedEnvironment hooks = new(scope.Environment) { GatedPaths = file => file.EndsWith(".sst", StringComparison.Ordinal) };
                DbOptions options = new()
                {
                    Environment = hooks,
                    DefaultColumnFamilyOptions = new ColumnFamilyOptions
                    {
                        MaxWriteBufferNumber = 4,
                        DisableAutoCompactions = true,
                        Compression = compression,
                        BlockCache = cached ? new Cache(8L << 20) : null,
                        CacheDataBlocks = cached,
                    },
                };

                string large = new('L', 200_000);   // a data block of its own, larger than the others
                Dictionary<string, string?> expected = new()
                {
                    ["bottom"] = "from level 6",
                    ["level0"] = "from level 0",
                    ["immutable"] = "from the immutable memtable",
                    ["active"] = "from the active memtable",
                    ["overwritten"] = "newest",
                    ["empty-bottom"] = "",
                    ["empty-active"] = "",
                    ["large-bottom"] = large,
                    ["large-active"] = large,
                    ["deleted-active"] = null,
                    ["deleted-level0"] = null,
                    ["covered-by-active"] = null,
                    ["covered-by-level0"] = null,
                    ["covered-inside-active"] = null,
                    ["covered-inside-level0"] = null,
                    ["covered-then-put"] = "after the range delete",
                    ["absent"] = null,
                    ["zz-past-every-key"] = null,
                };

                using (SolariumDb db = SolariumDb.Open(path, options))
                {
                    // Opens the gate before the database closes, which waits for the held flush.
                    using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
                    foreach (string key in expected.Keys.Where(k => k is not ("absent" or "zz-past-every-key")))
                        db.Put(B(key), B($"old {key}"));

                    db.Put(B("bottom"), B("from level 6"));
                    db.Put(B("empty-bottom"), []);
                    db.Put(B("large-bottom"), B(large));
                    db.Flush();
                    db.CompactRange(null, null);
                    Assert.False(db.DefaultColumnFamily.Version.Level6.IsEmpty);

                    db.Put(B("level0"), B("from level 0"));
                    db.Put(B("overwritten"), B("newer"));
                    db.Delete(B("deleted-level0"));
                    db.DeleteRange(B("covered-by-level0"), B("covered-by-level1"));
                    db.Put(B("covered-inside-level0"), B("newer"));
                    db.DeleteRange(B("covered-inside-level0"), B("covered-inside-level1"));
                    db.DeleteRange(B("covered-then-put"), B("covered-then-puu"));
                    db.Flush();
                    Assert.Single(db.DefaultColumnFamily.Version.Level0);

                    // The flush of this memtable is held, so it stays immutable while the reads run.
                    hooks.WriteGate.Reset();
                    db.Put(B("immutable"), B("from the immutable memtable"));
                    db.Flush(wait: false);
                    Assert.Single(db.DefaultColumnFamily.Version.Immutables);

                    db.Put(B("active"), B("from the active memtable"));
                    db.Put(B("overwritten"), B("newest"));
                    db.Put(B("empty-active"), []);
                    db.Put(B("large-active"), B(large));
                    db.Put(B("covered-then-put"), B("after the range delete"));
                    db.Delete(B("deleted-active"));
                    db.DeleteRange(B("covered-by-active"), B("covered-by-activf"));
                    db.Put(B("covered-inside-active"), B("newest"));
                    db.DeleteRange(B("covered-inside-active"), B("covered-inside-activf"));
                    Check(db);
                }

                using SolariumDb reopened = SolariumDb.Open(path, options);
                Check(reopened);

                void Check(SolariumDb db)
                {
                    foreach ((string key, string? value) in expected)
                    {
                        // Twice: from the file, and from the cache when there is one.
                        for (int read = 0; read < 2; read++)
                        {
                            int calls = 0;
                            bool found = db.TryGet(B(key), bytes =>
                            {
                                calls++;
                                return S(bytes);
                            }, out string? text);

                            Assert.Equal(value is not null, found);
                            Assert.Equal(value, text);
                            Assert.Equal(value is null ? 0 : 1, calls);
                            Assert.Equal(value, S(db.Get(B(key))));
                        }
                    }
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_reader_reads_the_family_it_is_given_and_fills_the_cache_only_when_asked(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(8L << 20);
        DbOptions options = scope.Options(createMissingColumnFamilies: true) with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = true },
        };
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options, columnFamilies: ["other"]);
        ColumnFamily other = db.GetColumnFamily("other");
        db.Put(B("k"), B("default"));
        db.Put(B("k"), B("other"), other);
        db.Flush();

        long before = cache.InsertCount;
        Assert.True(db.TryGet(B("k"), static value => S(value), out string? text, options: new ReadOptions { FillCache = false }));
        Assert.Equal("default", text);
        Assert.Equal(before, cache.InsertCount);

        Assert.True(db.TryGet(B("k"), static value => S(value), out text));
        Assert.Equal("default", text);
        Assert.Equal(before + 1, cache.InsertCount);

        Assert.True(db.TryGet(B("k"), static value => S(value), out text, other));
        Assert.Equal("other", text);
    }

    [Fact]
    public void A_null_reader_a_family_of_another_database_and_a_closed_database_are_refused()
    {
        SolariumDb db = SolariumDb.Open(_directory.Combine("db"));
        using SolariumDb another = SolariumDb.Open(_directory.Combine("another"));
        db.Put(B("k"), B("v"));

        Assert.Throws<ArgumentNullException>(() => db.TryGet<int>(B("k"), null!, out _));
        Assert.Throws<ArgumentException>(() => db.TryGet(B("k"), static value => value.Length, out _, another.DefaultColumnFamily));
        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.TryGet(B("k"), static value => value.Length, out _));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_reader_is_not_called_with_a_damaged_value(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), new DbOptions { Environment = hooks });
        for (int i = 0; i < 100; i++)
            db.Put(B($"k{i:D3}"), B($"value {i}"));

        db.Flush();
        hooks.CorruptedReads = (path, offset) => path.EndsWith(".sst", StringComparison.Ordinal) && offset == 0;

        int calls = 0;
        SolariumException e = Assert.Throws<SolariumException>(() => db.TryGet(B("k000"), _ => ++calls, out _));
        Assert.True(e.IsCorruption);
        Assert.Equal(0, calls);

        hooks.CorruptedReads = (_, _) => false;
        Assert.True(db.TryGet(B("k000"), static value => S(value), out string? text));
        Assert.Equal("value 0", text);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_reader_may_read_and_write_the_database(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), scope.Options());
        db.Put(B("a"), B("first"));
        db.Put(B("b"), B("second"));
        db.Flush();

        // Twice: the second time, the values come from the memtable that the first wrote.
        for (int round = 0; round < 2; round++)
        {
            Assert.True(db.TryGet(B("a"), outer =>
            {
                Assert.True(db.TryGet(B("b"), static inner => S(inner), out string? inner));
                db.Put(B("a"), B("rewritten"), options: WriteOptions.Synced);
                db.Delete(B("b"));
                Assert.Null(db.Get(B("b")));
                db.Put(B("b"), B("second"));
                return $"{S(outer)} {inner}";
            }, out string? text));

            Assert.Equal(round == 0 ? "first second" : "rewritten second", text);
            Assert.Equal("rewritten", S(db.Get(B("a"))));
        }
    }

    // ── How long a value stays readable ──────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_value_stays_readable_across_the_flush_of_its_memtable_which_is_freed_when_the_reader_returns(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        // A manager of the database's own, so a chunk freed early would go to the next memtable, and be written over.
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), scope.Options() with { DbWriteBufferSize = 64L << 20 });
        Fill(db, "first");
        MemTable first = db.DefaultColumnFamily.Version.Active;

        Assert.True(db.TryGet(B("k0007"), value =>
        {
            db.Flush();
            Fill(db, "second");
            db.Flush();
            Fill(db, "third");
            Assert.False(first.IsFreed);
            return S(value);
        }, out string? text));

        Assert.Equal("first-7", text);
        Assert.True(first.IsFreed);
        Assert.Equal("third-7", S(db.Get(B("k0007"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_value_stays_readable_after_its_cached_block_is_evicted_and_its_table_compacted_away(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(8L << 20, shardBits: 0);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = true, DisableAutoCompactions = true },
        };
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        Fill(db, "first");
        db.Flush();
        long table = db.DefaultColumnFamily.Version.Level0[0].Number;

        Assert.True(db.TryGet(B("k0007"), value =>
        {
            Fill(db, "second");
            db.Flush();
            db.CompactRange(null, null);
            Assert.DoesNotContain(table, db.DefaultColumnFamily.Version.Tables.Select(t => t.Number));
            cache.SetCapacity(0);

            // The cache has let go of the block; only this read holds it.
            Assert.Equal(1, cache.NativeBlocks.Count);
            return S(value);
        }, out string? text));

        Assert.Equal("first-7", text);
        Assert.Equal((0L, 0L), cache.NativeBlocks);
        Assert.Equal("second-7", S(db.Get(B("k0007"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_value_stays_readable_when_the_database_closes_under_its_reader(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (bool flushed in new[] { false, true })
        {
            Cache cache = new(8L << 20);
            DbOptions options = scope.Options() with
            {
                DbWriteBufferSize = 64L << 20,
                DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = true },
            };
            SolariumDb db = SolariumDb.Open(_directory.Combine($"db-{flushed}"), options);
            Fill(db, "v");
            if (flushed)
                db.Flush();

            ArenaChunkPool pool = db.ChunkPool;
            MemTable active = db.DefaultColumnFamily.Version.Active;
            Assert.True(db.TryGet(B("k0007"), value =>
            {
                db.Dispose();
                Assert.False(active.IsFreed);
                return S(value);
            }, out string? text));

            Assert.Equal("v-7", text);
            Assert.True(active.IsFreed);
            Assert.Equal(0, pool.AllocatedBytes);
            Assert.Equal((0L, 0L), cache.NativeBlocks);
            Assert.Throws<ObjectDisposedException>(() => db.TryGet(B("k0007"), static value => value.Length, out _));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_exception_from_a_reader_reaches_the_caller_and_the_read_lets_go_of_the_value(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(8L << 20, shardBits: 0);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = true, DisableAutoCompactions = true },
        };
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);

        // Not a kind that the table reader takes for a damaged block, and one that it does.
        foreach (Exception failure in new Exception[] { new InvalidOperationException("reader"), new ArgumentOutOfRangeException("reader") })
        {
            Fill(db, "v");
            MemTable memTable = db.DefaultColumnFamily.Version.Active;
            Assert.Same(failure, Record.Exception(() => db.TryGet<int>(B("k0007"), _ => throw failure, out _)));
            db.Flush();
            Assert.True(memTable.IsFreed);

            Assert.Same(failure, Record.Exception(() => db.TryGet<int>(B("k0007"), _ => throw failure, out _)));
            Assert.True(cache.NativeBlocks.Count > 0);
            cache.SetCapacity(0);
            Assert.Equal((0L, 0L), cache.NativeBlocks);
            cache.SetCapacity(8L << 20);
        }

        Assert.Equal("v-7", S(db.Get(B("k0007"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Readers_see_whole_values_of_their_own_key_while_writes_flush_compact_evict_and_close(string environment)
    {
        const int Keys = 64;
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(16 * 1024, shardBits: 0);
        DbOptions options = scope.Options() with
        {
            DbWriteBufferSize = 256 * 1024,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = 64 * 1024,
                MaxWriteBufferNumber = 3,
                Level0FileNumCompactionTrigger = 2,
                TargetFileSizeBase = 32 * 1024,
                BlockCache = cache,
                CacheDataBlocks = true,
                Compression = CompressionType.Lz4,
            },
        };
        SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        for (int key = 0; key < Keys; key++)
            db.Put(B($"k{key:D4}"), Value(key, 0));

        long reads = 0;
        Task[] readers = [.. Enumerable.Range(0, 4).Select(r => Task.Factory.StartNew(() =>
        {
            Random random = new(r);
            long[] newest = new long[Keys];
            while (true)
            {
                int key = random.Next(Keys);
                try
                {
                    Assert.True(db.TryGet(B($"k{key:D4}"), value => Version(value, key), out long version));

                    // A key's versions only go up, as one thread writes it and each read takes a newer snapshot.
                    Assert.True(version >= newest[key], $"key {key} read version {version} after {newest[key]}");
                    newest[key] = version;
                    Interlocked.Increment(ref reads);
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
            }
        }, TaskCreationOptions.LongRunning))];

        Random writes = new(20261123);
        for (int step = 1; step <= 12_000; step++)
        {
            int key = writes.Next(Keys);
            db.Put(B($"k{key:D4}"), Value(key, step));
            if (step % 1_000 == 0)
                db.Flush();

            if (step % 3_000 == 0)
                db.CompactRange(null, null);
        }

        ArenaChunkPool pool = db.ChunkPool;
        Assert.True(Interlocked.Read(ref reads) > 0);
        db.Dispose();
        await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(60));

        // A read that was in flight across the close can cache its block after the close erased the others. No reader
        // holds one, so an eviction frees them all.
        cache.SetCapacity(0);
        Assert.Equal((0L, 0L), cache.NativeBlocks);
        Assert.Equal(0, pool.AllocatedBytes);
    }

    // ── What a read in place costs ───────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_read_in_place_allocates_no_value(string environment)
    {
        const int Reads = 1_000;
        const int ValueSize = 64 * 1024;
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), scope.Options());
        byte[] key = B("k");
        byte[] value = new byte[ValueSize];
        value[^1] = 42;
        db.Put(key, value);

        // From the memtable, nothing at all.
        Assert.Equal(0, Allocated());

        // From the table, the block goes through a pooled buffer, which a collection may drop now and then.
        db.Flush();
        Assert.InRange(Allocated(), 0, 16L * ValueSize);

        // The first reads make the reader's delegate, which every later one uses again.
        long Allocated()
        {
            ReadLast(100);
            long before = GC.GetAllocatedBytesForCurrentThread();
            ReadLast(Reads);
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        void ReadLast(int count)
        {
            for (int i = 0; i < count; i++)
                if (!db.TryGet(key, static bytes => bytes[^1], out byte last) || last != 42)
                    Assert.Fail("the value was not read");
        }
    }

    private static void Fill(SolariumDb db, string prefix)
    {
        for (int i = 0; i < 200; i++)
            db.Put(B($"k{i:D4}"), B($"{prefix}-{i}"));
    }

    // A value that says its key and version, and whose every other byte follows from them.
    private static byte[] Value(int key, long version)
    {
        byte[] value = new byte[16 + (int)((version * 37) % 2_000)];
        BitConverter.TryWriteBytes(value, (long)key);
        BitConverter.TryWriteBytes(value.AsSpan(8), version);
        for (int i = 16; i < value.Length; i++)
            value[i] = (byte)((key * 31) + (version * 17) + i);

        return value;
    }

    // The version of a value of Value, every byte of which must be its own.
    private static long Version(ReadOnlySpan<byte> value, int key)
    {
        long version = BitConverter.ToInt64(value[8..]);
        Assert.True(value.SequenceEqual(Value(key, version)), $"the value of key {key} is not one that was written");
        return version;
    }
}
