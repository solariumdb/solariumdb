using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// The memory that memtables and cached data blocks keep outside the managed heap: a memtable's entries live in its
/// native arena and read back their own bytes whatever their size and whatever the caller does to its arrays; an
/// insert allocates nothing on the managed heap; the bytes a memtable reports are its nodes; an arena is freed only
/// once no reader can reach it, so an iterator reads a flushed memtable, and one left open across the close of its
/// database still reads it; a read that races the close gets a value or <see cref="ObjectDisposedException"/>; the
/// chunk pool keeps freed chunks only within its limit; and a cached block stays readable under its reader after an
/// eviction, and every block is freed once its last holder lets it go.
/// </summary>
public sealed class NativeMemoryTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    // ── Memtable entries ─────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Entries_of_every_size_read_back_their_own_bytes_after_the_caller_changes_its_arrays(bool concurrently)
    {
        int[] keyLengths = [0, 1, 31, 300];
        int[] valueLengths = [-1, 0, 1, 128, Arena.LargeAllocation - 64, Arena.LargeAllocation + 1, ArenaChunkPool.ChunkSize + 1, 1 << 20];
        ArenaChunkPool pool = new(0);
        MemTable table = new(pool);
        List<(byte[] Key, byte[]? Value, long Sequence)> written = [];
        long sequence = 0;
        foreach (int keyLength in keyLengths)
        {
            foreach (int valueLength in valueLengths)
            {
                sequence++;
                byte[] key = Pattern(keyLength, (int)sequence);
                byte[]? value = valueLength < 0 ? null : Pattern(valueLength, (int)sequence * 7);
                written.Add((key.ToArray(), value?.ToArray(), sequence));
                OperationKind kind = value is null ? OperationKind.Delete : OperationKind.Put;
                if (concurrently)
                {
                    table.AddConcurrently(key, sequence, kind, value, out long bytes);
                    table.AddCounts(bytes, 1, sequence, sequence);
                }
                else
                {
                    table.Add(key, sequence, kind, value);
                }

                // The memtable copied the bytes: what the caller does to its arrays after the insert changes nothing.
                Array.Fill(key, (byte)0xEE);
                if (value is not null)
                    Array.Fill(value, (byte)0xEE);
            }
        }

        Assert.Equal(written.Count, table.EntryCount);
        foreach ((byte[] key, byte[]? value, long version) in written)
        {
            Assert.True(table.TryGetNewest(key, version, fillCache: true, out Entry found));
            Assert.Equal(key, found.Key);
            Assert.Equal(version, found.Sequence);
            Assert.Equal(value is null ? OperationKind.Delete : OperationKind.Put, found.Kind);
            Assert.Equal(value, found.Value);
        }

        // In internal key order through Entries and a cursor, with the same bytes.
        List<(byte[] Key, byte[]? Value, long Sequence)> ordered =
            [.. written.OrderBy(w => w.Key, ByteComparer.Instance).ThenByDescending(w => w.Sequence)];
        Entry[] entries = [.. table.Entries()];
        Assert.Equal(ordered.Count, entries.Length);
        IEntryCursor cursor = table.NewCursor(fillCache: true);
        cursor.SeekToFirst();
        for (int i = 0; i < ordered.Count; i++)
        {
            Assert.Equal(ordered[i].Key, entries[i].Key);
            Assert.Equal(ordered[i].Value, entries[i].Value);
            Assert.True(cursor.Valid);
            Assert.True(cursor.CurrentKey.SequenceEqual(ordered[i].Key));
            Assert.Equal(ordered[i].Sequence, cursor.CurrentSequence);
            Assert.Equal(ordered[i].Value, cursor.Current.Value);
            cursor.Next();
        }

        Assert.False(cursor.Valid);
        Assert.Equal(ordered[^1].Key, table.LastKeyBefore(null, fillCache: true));

        // Large values have blocks of their own; the pool keeps nothing (limit 0) and has nothing out once freed.
        Assert.True(table.ArenaBytes >= table.ApproximateBytes);
        table.FreeUnreferenced();
        Assert.Equal(0, pool.RentedBytes);
        Assert.Equal(0, pool.AllocatedBytes);
    }

    [Fact]
    public void An_insert_allocates_nothing_on_the_managed_heap_and_the_bytes_counted_are_the_nodes()
    {
        const int Count = 20_000;
        byte[][] keys = [.. Enumerable.Range(0, Count).Select(i => B($"key{i:D8}"))];
        byte[] value = new byte[100];
        ArenaChunkPool pool = new(0);
        MemTable table = new(pool);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Count; i++)
            table.Add(keys[i], i + 1, OperationKind.Put, value);

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // Only the small handle of each 64 KiB chunk and the arena's list of them: under a byte per entry. A managed
        // memtable allocated four objects and about 300 bytes for each.
        Assert.True(allocated < Count, $"{allocated} bytes allocated for {Count} entries");

        // Each node holds its key and value, its header and at least one link; one writer leaves one chunk partly filled.
        long smallest = Count * MemTable.NodeSize(1, keys[0].Length, value.Length);
        Assert.InRange(table.ApproximateBytes, smallest, smallest + (Count * 8L * 4));
        Assert.InRange(table.ArenaBytes - table.ApproximateBytes, 0, 2 * ArenaChunkPool.ChunkSize);
        Assert.Equal(table.ArenaBytes, pool.RentedBytes);
        table.FreeUnreferenced();
        Assert.Equal(0, pool.RentedBytes);
    }

    [Fact]
    public async Task Concurrent_inserts_over_many_chunks_keep_their_own_bytes_while_readers_walk()
    {
        const int Threads = 8;
        const int Each = 2_000;
        ArenaChunkPool pool = new(0);
        MemTable table = new(pool);
        using Barrier start = new(Threads);
        using CancellationTokenSource stop = new();
        int wrong = 0;

        Task reader = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
                foreach (Entry entry in table.Entries())
                    if (!entry.Value.AsSpan().SequenceEqual(ValueFor(entry.Sequence)))
                        Interlocked.Increment(ref wrong);
        });

        Task[] writers = [.. Enumerable.Range(0, Threads).Select(t => Task.Factory.StartNew(() =>
        {
            start.SignalAndWait();
            for (int i = 0; i < Each; i++)
            {
                long sequence = 1 + ((long)i * Threads) + t;
                table.AddConcurrently(B($"k{sequence % 997:D4}"), sequence, OperationKind.Put, ValueFor(sequence), out long bytes);
                table.AddCounts(bytes, 1, sequence, sequence);
            }
        }, TaskCreationOptions.LongRunning))];

        await Task.WhenAll(writers);
        stop.Cancel();
        await reader;

        Assert.Equal(0, wrong);
        Entry[] entries = [.. table.Entries()];
        Assert.Equal(Threads * Each, entries.Length);
        foreach (Entry entry in entries)
            Assert.Equal(ValueFor(entry.Sequence), entry.Value);

        Assert.True(table.ArenaBytes > 16 * ArenaChunkPool.ChunkSize);
        table.FreeUnreferenced();
        Assert.Equal(0, pool.RentedBytes);
    }

    // ── Memtable lifetime in a database ──────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_iterator_reads_a_flushed_memtable_until_it_is_disposed_and_then_its_arena_is_freed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        WriteBufferManager manager = new(64L << 20);
        DbOptions options = scope.Options() with { WriteBufferManager = manager };
        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        for (int i = 0; i < 500; i++)
            db.Put(B($"k{i:D4}"), B($"first-{i}"));

        MemTable first = db.DefaultColumnFamily.Version.Active;
        using (Iterator iterator = db.NewIterator())
        {
            // The memtable becomes a table, and the next one too, so no version lists it but the iterator's.
            db.Flush();
            for (int i = 0; i < 500; i++)
                db.Put(B($"k{i:D4}"), B($"second-{i}"));

            db.Flush();
            Assert.False(first.IsFreed);

            int count = 0;
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                Assert.Equal($"first-{count}", S(iterator.Value()));
                count++;
            }

            Assert.Equal(500, count);
        }

        Assert.True(first.IsFreed);
        Assert.Equal(CurrentArenaBytes(db), manager.ChunkPool.RentedBytes);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_iterator_left_open_across_the_close_reads_its_memtable_and_frees_it_when_disposed(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        // A manager of the database's own, so its pool is the database's alone, and given back at the close.
        SolariumDb db = SolariumDb.Open(_directory.Combine("db"), scope.Options() with { DbWriteBufferSize = 64L << 20 });
        for (int i = 0; i < 300; i++)
            db.Put(B($"k{i:D4}"), B($"v{i}"));

        ArenaChunkPool pool = db.ChunkPool;
        MemTable active = db.DefaultColumnFamily.Version.Active;
        Iterator iterator = db.NewIterator();
        db.Dispose();

        Assert.Throws<ObjectDisposedException>(() => db.Get(B("k0001")));
        Assert.False(active.IsFreed);
        int count = 0;
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            Assert.Equal($"v{count++}", S(iterator.Value()));

        Assert.Equal(300, count);
        iterator.Dispose();
        Assert.True(active.IsFreed);
        Assert.Equal(0, pool.AllocatedBytes);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Reads_that_race_the_close_get_their_value_or_an_ObjectDisposedException(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        for (int round = 0; round < 20; round++)
        {
            SolariumDb db = SolariumDb.Open(_directory.Combine($"db{round}"), scope.Options() with { DbWriteBufferSize = 64L << 20 });
            for (int i = 0; i < 200; i++)
                db.Put(B($"k{i:D4}"), B($"v{i}"));

            using Barrier start = new(5);
            Task[] readers = [.. Enumerable.Range(0, 4).Select(r => Task.Factory.StartNew(() =>
            {
                start.SignalAndWait();
                for (int i = 0; ; i = (i + 1) % 200)
                {
                    try
                    {
                        Assert.Equal($"v{i}", S(db.Get(B($"k{i:D4}"))));
                        using Iterator iterator = db.NewIterator();
                        iterator.Seek(B($"k{i:D4}"));
                        Assert.Equal($"v{i}", S(iterator.Value()));
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }
                }
            }, TaskCreationOptions.LongRunning))];

            start.SignalAndWait();
            Thread.Sleep(round % 5);
            ArenaChunkPool pool = db.ChunkPool;
            db.Dispose();
            await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(0, pool.AllocatedBytes);
        }
    }

    [Fact]
    public void The_pool_keeps_freed_chunks_for_the_next_memtables_only_within_its_limit()
    {
        const long Budget = 1L << 20;
        WriteBufferManager manager = new(Budget);
        DbOptions options = new()
        {
            WriteBufferManager = manager,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, DisableAutoCompactions = true },
        };

        using SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options);
        ArenaChunkPool pool = manager.ChunkPool;
        byte[] value = new byte[200];
        for (int round = 0; round < 8; round++)
        {
            for (int i = 0; i < 3_000; i++)
                db.Put(B($"r{round}-k{i:D5}"), value);

            db.Flush();

            // Within the cap, a flushed memtable's chunks wait in the pool for the next one; past it they go back.
            Assert.Equal(CurrentArenaBytes(db), pool.RentedBytes);
            Assert.True(pool.KeptBytes <= Math.Max(0, Budget - pool.RentedBytes), $"kept {pool.KeptBytes} with {pool.RentedBytes} rented");
            if (round > 0)
                Assert.True(pool.KeptBytes > 0, "no chunk was kept for the next memtable");
        }

        manager.SetBufferSize(ArenaChunkPool.ChunkSize);
        Assert.True(pool.KeptBytes <= ArenaChunkPool.ChunkSize);
    }

    [Fact]
    public void Databases_without_a_manager_share_one_pool_and_one_with_a_manager_uses_the_managers()
    {
        WriteBufferManager manager = new(8L << 20);
        using SolariumDb plain = SolariumDb.Open(_directory.Combine("plain"), new DbOptions());
        using SolariumDb shared = SolariumDb.Open(_directory.Combine("shared"), new DbOptions { WriteBufferManager = manager });
        using SolariumDb own = SolariumDb.Open(_directory.Combine("own"), new DbOptions { DbWriteBufferSize = 8L << 20 });

        Assert.Same(ArenaChunkPool.Shared, plain.ChunkPool);
        Assert.Same(manager.ChunkPool, shared.ChunkPool);
        Assert.NotSame(ArenaChunkPool.Shared, own.ChunkPool);
        Assert.Equal(8L << 20, own.ChunkPool.RetainLimit);

        // While it is open, the plain database's two default memtables of 64 MiB are in the shared pool's limit.
        Assert.True(ArenaChunkPool.Shared.RetainLimit >= 2 * (64L << 20));
    }

    // ── Cached data blocks ───────────────────────────────────────────────────────────────────────

    [Fact]
    public void A_cached_block_stays_readable_under_its_reader_after_an_eviction_and_the_last_release_frees_it()
    {
        Cache cache = new(4096, shardBits: 0, highPriorityPoolRatio: 0);
        byte[] contents = Pattern(1000, 3);
        CacheKey key = new(1, 1, 0);

        NativeBlock created = cache.NewBlock(key, contents);
        NativeBlock cached = (NativeBlock)cache.Insert(key, created, created.Length);
        created.Release();
        Assert.Same(created, cached);
        cached.Release();

        Assert.True(cache.TryGet(key, out object? found));
        NativeBlock held = (NativeBlock)found;

        // Evict it: the cache drops its reference, the reader keeps the bytes.
        for (int i = 1; i <= 8; i++)
        {
            CacheKey other = new(1, 1, i);
            NativeBlock block = cache.NewBlock(other, Pattern(1000, i));
            ((NativeBlock)cache.Insert(other, block, block.Length)).Release();
            block.Release();
        }

        Assert.False(cache.TryGet(key, out _));
        Assert.True(held.Span.SequenceEqual(contents));
        long liveWithHeld = cache.NativeBlocks.Count;
        held.Release();
        Assert.Equal(liveWithHeld - 1, cache.NativeBlocks.Count);

        // A second insert of a cached key keeps the first block and frees the new copy once its creator lets it go.
        CacheKey last = new(1, 1, 8);
        NativeBlock duplicate = cache.NewBlock(last, Pattern(1000, 99));
        NativeBlock kept = (NativeBlock)cache.Insert(last, duplicate, duplicate.Length);
        Assert.NotSame(duplicate, kept);
        Assert.True(kept.Span.SequenceEqual(Pattern(1000, 8)));
        kept.Release();
        duplicate.Release();

        cache.EraseOwner(1);
        Assert.Equal((0L, 0L), cache.NativeBlocks);
        Assert.Equal(0, cache.Usage);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Readers_on_a_cache_far_smaller_than_the_blocks_read_their_values_and_leave_no_block_behind(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Cache cache = new(64 * 1024, shardBits: 2);
        DbOptions options = scope.Options() with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { BlockCache = cache, CacheDataBlocks = true, Compression = CompressionType.Lz4 },
        };

        using (SolariumDb db = SolariumDb.Open(_directory.Combine("db"), options))
        {
            for (int table = 0; table < 4; table++)
            {
                for (int i = table; i < 4_000; i += 4)
                    db.Put(B($"k{i:D5}"), B($"value-{i}-{new string('x', i % 300)}"));

                db.Flush();
            }

            Task[] readers = [.. Enumerable.Range(0, 6).Select(r => Task.Factory.StartNew(() =>
            {
                Random random = new(r);
                for (int n = 0; n < 4_000; n++)
                {
                    int i = random.Next(4_000);
                    Assert.Equal($"value-{i}-{new string('x', i % 300)}", S(db.Get(B($"k{i:D5}"))));
                    if (n % 200 == 0)
                    {
                        byte[]?[] values = db.MultiGet([B($"k{i:D5}"), B($"k{(i + 1) % 4_000:D5}")]);
                        Assert.Equal($"value-{i}-{new string('x', i % 300)}", S(values[0]));
                        using Iterator iterator = db.NewIterator();
                        iterator.Seek(B($"k{i:D5}"));
                        for (int step = 0; step < 50 && iterator.Valid(); step++)
                            iterator.Next();
                    }
                }
            }, TaskCreationOptions.LongRunning))];

            // A compaction retires tables under the readers.
            db.CompactRange(null, null);
            await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.True(cache.NativeBlocks.Count > 0);
            Assert.True(cache.NativeBlocks.Bytes <= cache.Usage);
        }

        // The close erased the database's blocks, and no reader holds one.
        Assert.Equal((0L, 0L), cache.NativeBlocks);
    }

    private static long CurrentArenaBytes(SolariumDb db)
    {
        FamilyVersion version = db.DefaultColumnFamily.Version;
        return version.Active.ArenaBytes + version.Immutables.Sum(m => m.ArenaBytes);
    }

    private static byte[] ValueFor(long sequence) => Pattern((int)(sequence * 37 % 3_000), (int)sequence);

    private static byte[] Pattern(int length, int seed)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
            bytes[i] = (byte)((seed * 31) + (i * 7));

        return bytes;
    }

    private sealed class ByteComparer : IComparer<byte[]>
    {
        public static ByteComparer Instance { get; } = new();

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}
