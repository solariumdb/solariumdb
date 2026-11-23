using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Batched reads of values in place (<see cref="SolariumDb.MultiGet{TVisitor}"/>): the visitor gets the index and the value
/// of each key that has one, all at one snapshot; the bytes stay its own while it runs, across a flush, an eviction, a
/// compaction and the close of the database, and the call lets go of them when it returns or the visitor throws; and it
/// copies no value. <see cref="MultiGetTests"/> compares it with a <see cref="SolariumDb.Get"/> per key.
/// </summary>
public sealed class ValueVisitorTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    // ── What a visitor gets ──────────────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_visitor_gets_the_index_and_the_value_once_for_each_key_that_has_a_value(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (bool cached in new[] { false, true })
        {
            using SolariumDb db = SolariumDb.Open(_directory.Combine($"db-{cached}"), CachedOptions(scope, cached ? new Cache(8L << 20) : null));
            string large = new('L', 200_000);
            foreach (string key in new[] { "bottom", "deleted", "deleted-in-memory", "covered", "overwritten" })
                db.Put(B(key), B($"old {key}"));

            db.Put(B("empty"), []);
            db.Put(B("large"), B(large));
            db.Flush();
            db.CompactRange(null, null);
            db.Put(B("level0"), B("from level 0"));
            db.Put(B("overwritten"), B("newer"));
            db.Delete(B("deleted"));
            db.Flush();
            db.Put(B("active"), B("from the memtable"));
            db.Put(B("overwritten"), B("newest"));
            db.Delete(B("deleted-in-memory"));
            db.Put(B("covered"), B("newer"));
            db.DeleteRange(B("covered"), B("coveree"));

            (string Key, string? Value)[] expected =
            [
                ("absent", null), ("active", "from the memtable"), ("bottom", "old bottom"), ("covered", null), ("deleted", null), ("deleted-in-memory", null), ("empty", ""),
                ("large", large), ("level0", "from level 0"), ("overwritten", "newest"), ("bottom", "old bottom"), ("active", "from the memtable"), ("zzz", null),
            ];

            // Twice: from the files, and from the cache when there is one.
            for (int read = 0; read < 2; read++)
            {
                Dictionary<int, string> visits = [];
                db.MultiGet([.. expected.Select(e => B(e.Key))], (index, value) => visits.Add(index, S(value)));
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i].Value, visits.GetValueOrDefault(i));

                Assert.Equal(expected.Count(e => e.Value is not null), visits.Count);
            }

            db.MultiGet([], static (_, _) => Assert.Fail("no key has a value"));
            db.MultiGet([B("absent")], static (_, _) => Assert.Fail("no key has a value"));
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_visitor_keeps_what_it_counted_over_a_memtable_a_file_and_a_cache(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), CachedOptions(scope, new Cache(8L << 20)));
        Fill(db, "v");
        db.Flush();
        for (int i = 200; i < 250; i++)
            db.Put(Key(i), B($"v-{i}"));

        // More keys than one batch, in a table and in the memtable, and some that are absent; from the file, and then
        // from the cache.
        byte[][] keys = [.. Enumerable.Range(150, 150).Select(Key)];
        for (int read = 0; read < 2; read++)
        {
            CountingVisitor visitor = default;
            db.MultiGet(keys, ref visitor);
            Assert.Equal(100, visitor.Count);
            Assert.Equal(500, visitor.Bytes);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_visitor_reads_the_family_it_is_given_and_fills_the_cache_only_when_asked(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(8L << 20);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), CachedOptions(scope, cache) with { CreateMissingColumnFamilies = true }, columnFamilies: ["other"]);
        ColumnFamily other = db.GetColumnFamily("other");
        db.Put(B("k"), B("default"));
        db.Put(B("k"), B("other"), other);
        db.Flush();

        long before = cache.InsertCount;
        Assert.Equal("default", Read(null, new ReadOptions { FillCache = false }));
        Assert.Equal(before, cache.InsertCount);
        Assert.Equal("default", Read(null, null));
        Assert.Equal(before + 1, cache.InsertCount);
        Assert.Equal("other", Read(other, null));

        string? Read(ColumnFamily? family, ReadOptions? options)
        {
            string?[] text = new string?[1];
            db.MultiGet([B("k")], (index, value) => text[index] = S(value), family, options);
            return text[0];
        }
    }

    [Fact]
    public void Null_keys_a_family_of_another_database_and_a_closed_database_are_refused()
    {
        SolariumDb db = SolariumDb.Open(_directory.Combine("db"));
        using SolariumDb another = SolariumDb.Open(_directory.Combine("another"));
        db.Put(B("k"), B("v"));
        byte[][] keys = [B("k")];

        Assert.Throws<ArgumentNullException>(() => db.MultiGet(null!, static (_, _) => { }));
        Assert.Throws<ArgumentException>(() => db.MultiGet(keys, static (_, _) => { }, another.DefaultColumnFamily));

        // A null family still names the form that returns arrays.
        Assert.Equal(["v"], db.MultiGet(keys, null).Select(S));
        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.MultiGet(keys, static (_, _) => { }));
    }

    // Every block of a round is checked before the first key of the round is visited, so a damaged one hides no other
    // key's value behind a partial answer of its round.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_round_with_a_damaged_block_visits_none_of_its_keys(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), new DbOptions { Environment = hooks });
        for (int i = 0; i < 500; i++)
            db.Put(Key(i), B($"{i}:" + new string('v', 100)));

        db.Flush();
        db.Put(B("memtable"), B("in memory"));
        TableReader table = db.DefaultColumnFamily.Version.Level0[0];
        Assert.True(table.TryLocate(Key(250), long.MaxValue, fillCache: true, out DataBlockHandle handle));
        hooks.CorruptedReads = (path, offset) => Path.GetFileName(path) == TableFormat.FileName(table.Number) && offset == handle.Offset;

        List<int> visited = [];
        byte[][] keys = [Key(1), Key(250), B("memtable"), Key(499)];
        SolariumException e = Assert.Throws<SolariumException>(() => db.MultiGet(keys, (index, _) => visited.Add(index)));
        Assert.True(e.IsCorruption);
        Assert.Equal([2], visited);

        hooks.CorruptedReads = (_, _) => false;
        visited.Clear();
        db.MultiGet(keys, (index, _) => visited.Add(index));
        Assert.Equal([0, 1, 2, 3], visited.Order());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_visitor_may_read_and_write_the_database_and_its_own_value_does_not_change(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (bool cached in new[] { false, true })
        {
            using SolariumDb db = SolariumDb.Open(_directory.Combine($"db-{cached}"), CachedOptions(scope, cached ? new Cache(8L << 20) : null));
            Fill(db, "first");
            db.Flush();

            int visits = 0;
            byte[][] keys = [.. Enumerable.Range(0, 100).Select(Key)];
            db.MultiGet(keys, (index, value) =>
            {
                // Reads that rent and return buffers of their own, and a write over the key that is visited.
                string?[] inner = new string?[keys.Length];
                db.MultiGet(keys, (i, v) => inner[i] = S(v));
                db.Put(Key(index), B($"second-{index}"));
                Assert.Equal($"second-{index}", S(db.Get(Key(index))));
                for (int i = 0; i < inner.Length; i++)
                    Assert.True(inner[i] == $"first-{i}" || inner[i] == $"second-{i}", $"key {i} read {inner[i] ?? "null"}");

                Assert.Equal($"first-{index}", S(value));
                visits++;
            });

            Assert.Equal(keys.Length, visits);
        }
    }

    // ── How long a value stays readable ──────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Values_stay_readable_across_the_flush_of_their_memtable_which_is_freed_when_the_call_returns(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        // A manager of the database's own, so a chunk freed early would go to the next memtable, and be written over.
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), scope.Options() with { DbWriteBufferSize = 64L << 20 });
        Fill(db, "first");
        MemTable first = db.DefaultColumnFamily.Version.Active;

        int visits = 0;
        db.MultiGet([Key(7), Key(8), Key(9)], (index, value) =>
        {
            if (visits++ == 0)
            {
                db.Flush();
                Fill(db, "second");
                db.Flush();
                Fill(db, "third");
            }

            Assert.False(first.IsFreed);
            Assert.Equal($"first-{7 + index}", S(value));
        });

        Assert.Equal(3, visits);
        Assert.True(first.IsFreed);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Values_stay_readable_after_their_blocks_are_evicted_and_their_table_is_compacted_away(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);

        // Read from the file and then from the cache, where the block of a visited value has no holder but the call.
        foreach (bool warm in new[] { false, true })
        {
            Cache cache = new(8L << 20, shardBits: 0);
            using SolariumDb db = SolariumDb.Open(_directory.Combine($"db-{warm}"), CachedOptions(scope, cache));
            Fill(db, "first");
            db.Flush();
            if (warm)
                Assert.Equal("first-7", S(db.Get(Key(7))));

            long table = db.DefaultColumnFamily.Version.Level0[0].Number;
            int visits = 0;
            db.MultiGet([Key(7), Key(8)], (index, value) =>
            {
                if (visits++ == 0)
                {
                    Fill(db, "second");
                    db.Flush();
                    db.CompactRange(null, null);
                    Assert.DoesNotContain(table, db.DefaultColumnFamily.Version.Tables.Select(t => t.Number));
                    cache.SetCapacity(0);
                    Assert.Equal(warm ? 1 : 0, cache.NativeBlocks.Count);
                }

                Assert.Equal($"first-{7 + index}", S(value));
            });

            Assert.Equal(2, visits);
            Assert.Equal((0L, 0L), cache.NativeBlocks);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Values_stay_readable_when_the_database_closes_under_their_visitor(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (bool flushed in new[] { false, true })
        {
            Cache cache = new(8L << 20);
            SolariumDb db = SolariumDb.Open(_directory.Combine($"db-{flushed}"), CachedOptions(scope, cache) with { DbWriteBufferSize = 64L << 20 });
            Fill(db, "v");
            if (flushed)
                db.Flush();

            ArenaChunkPool pool = db.ChunkPool;
            MemTable active = db.DefaultColumnFamily.Version.Active;
            int visits = 0;
            db.MultiGet([Key(7), Key(8)], (index, value) =>
            {
                visits++;
                db.Dispose();
                Assert.False(active.IsFreed);
                Assert.Equal($"v-{7 + index}", S(value));
            });

            Assert.Equal(2, visits);
            Assert.True(active.IsFreed);
            Assert.Equal(0, pool.AllocatedBytes);
            Assert.Equal((0L, 0L), cache.NativeBlocks);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_exception_from_a_visitor_ends_the_call_and_the_call_lets_go_of_the_values(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(8L << 20, shardBits: 0);
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), CachedOptions(scope, cache));

        // Not a kind that the table reader takes for a damaged block, and one that it does.
        foreach (Exception failure in new Exception[] { new InvalidOperationException("visitor"), new ArgumentOutOfRangeException("visitor") })
        {
            Fill(db, "v");
            MemTable memTable = db.DefaultColumnFamily.Version.Active;
            AssertEndsTheCall(failure);
            db.Flush();
            Assert.True(memTable.IsFreed);

            // From the file, and then from the cache that the first call filled.
            AssertEndsTheCall(failure);
            AssertEndsTheCall(failure);
            Assert.True(cache.NativeBlocks.Count > 0);
            cache.SetCapacity(0);
            Assert.Equal((0L, 0L), cache.NativeBlocks);
            cache.SetCapacity(8L << 20);
        }

        Assert.Equal(["v-7", "v-8"], db.MultiGet([Key(7), Key(8)]).Select(S));

        void AssertEndsTheCall(Exception failure)
        {
            int visits = 0;
            Assert.Same(failure, Record.Exception(() => db.MultiGet([Key(7), Key(8)], (_, _) =>
            {
                visits++;
                throw failure;
            })));
            Assert.Equal(1, visits);
        }
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Visitors_see_one_snapshot_of_whole_values_while_batches_flush_compact_evict_and_close(string environment)
    {
        const int Keys = 48;
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
        Write(0);

        long reads = 0;
        byte[][] keys = [.. Enumerable.Range(0, Keys).Select(Key)];
        Task[] readers = [.. Enumerable.Range(0, 3).Select(_ => Task.Factory.StartNew(() =>
        {
            long newest = 0;
            long[] versions = new long[Keys];
            while (true)
            {
                try
                {
                    Array.Fill(versions, -1);
                    VersionVisitor visitor = new(versions);
                    db.MultiGet(keys, ref visitor);
                    Assert.All(versions, version => Assert.Equal(versions[0], version));
                    Assert.True(versions[0] >= newest, $"read version {versions[0]} after {newest}");
                    newest = versions[0];
                    Interlocked.Increment(ref reads);
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
            }
        }, TaskCreationOptions.LongRunning))];

        for (int version = 1; version <= 300; version++)
        {
            Write(version);
            if (version % 25 == 0)
                db.Flush();

            if (version % 75 == 0)
                db.CompactRange(null, null);
        }

        ArenaChunkPool pool = db.ChunkPool;
        Assert.True(Interlocked.Read(ref reads) > 0);
        db.Dispose();
        await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(60));
        Assert.Equal((0L, 0L), cache.NativeBlocks);
        Assert.Equal(0, pool.AllocatedBytes);

        void Write(long version)
        {
            WriteBatch batch = new();
            for (int key = 0; key < Keys; key++)
                batch.Put(Key(key), Value(key, version));

            db.Write(batch);
        }
    }

    // ── What a batched read in place costs ───────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_batched_read_in_place_allocates_no_value(string environment)
    {
        const int Calls = 50;
        const int ValueSize = 64 * 1024;
        using EnvironmentScope scope = Environments.Create(environment);
        foreach (bool cached in new[] { false, true })
        {
            using SolariumDb db = SolariumDb.Open(_directory.Combine($"db-{cached}"), CachedOptions(scope, cached ? new Cache(8L << 20) : null));
            byte[][] keys = [.. Enumerable.Range(0, 8).Select(Key)];
            byte[] value = new byte[ValueSize];
            value[^1] = 42;
            foreach (byte[] key in keys)
                db.Put(key, value);

            // Less than one value for each call of eight: its lists, and now and then a buffer that a collection dropped.
            Assert.InRange(Allocated(), 0, (long)Calls * ValueSize);
            db.Flush();
            Assert.InRange(Allocated(), 0, (long)Calls * ValueSize);

            long Allocated()
            {
                ReadLast(5);
                long before = GC.GetAllocatedBytesForCurrentThread();
                ReadLast(Calls);
                return GC.GetAllocatedBytesForCurrentThread() - before;
            }

            void ReadLast(int calls)
            {
                byte[] last = new byte[keys.Length];
                for (int i = 0; i < calls; i++)
                {
                    Array.Clear(last);
                    LastByteVisitor visitor = new(last);
                    db.MultiGet(keys, ref visitor);
                    Assert.Equal(-1, last.AsSpan().IndexOfAnyExcept((byte)42));
                }
            }
        }
    }

    private static DbOptions CachedOptions(EnvironmentScope scope, Cache? cache) => scope.Options() with
    {
        DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = cache is not null, DisableAutoCompactions = true },
    };

    private static byte[] Key(int i) => B($"k{i:D4}");

    private static void Fill(SolariumDb db, string prefix)
    {
        for (int i = 0; i < 200; i++)
            db.Put(Key(i), B($"{prefix}-{i}"));
    }

    // A value that says its key and version, and whose every other byte follows from them.
    private static byte[] Value(int key, long version)
    {
        byte[] value = new byte[16 + (int)(((version * 37) + (key * 13)) % 600)];
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

    /// <summary>Stores the version of each value (<see cref="Version"/>) at its key's index.</summary>
    private readonly struct VersionVisitor(long[] versions) : IValueVisitor
    {
        public void OnValue(int index, ReadOnlySpan<byte> value) => versions[index] = Version(value, index);
    }

    /// <summary>Stores the last byte of each value at its key's index.</summary>
    private readonly struct LastByteVisitor(byte[] last) : IValueVisitor
    {
        public void OnValue(int index, ReadOnlySpan<byte> value) => last[index] = value[^1];
    }

    /// <summary>Counts the values and their bytes in its own fields.</summary>
    private struct CountingVisitor : IValueVisitor
    {
        public int Count;
        public int Bytes;

        public void OnValue(int index, ReadOnlySpan<byte> value)
        {
            Count++;
            Bytes += value.Length;
        }
    }
}
