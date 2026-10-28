using Solarium.Engine;
using Solarium.IoUring;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// <see cref="SolariumDb.MultiGet"/> reads in rounds: it finds the data block of every key, then reads the round's
/// distinct blocks in one <see cref="IStorageEnvironment.MultiRead"/>. It returns what a <see cref="SolariumDb.Get"/>
/// per key returns, wherever the keys are and whatever covers them; a key that a Bloom filter's false positive
/// sent to the wrong block goes on to an older table in a second round; and a damaged block fails the call.
/// </summary>
public sealed class MultiGetTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    /// <summary>Every available environment, with plain tables, and with LZ4 tables whose partitions a cache of 4 KiB holds.</summary>
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
    public void A_multi_get_reads_what_a_get_per_key_reads_across_memtables_tables_and_deletes(string environment, bool compressedAndCached)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        ColumnFamilyOptions family = new() { DisableAutoCompactions = true };
        if (compressedAndCached)
            family = family with { Compression = CompressionType.Lz4, BlockCache = new Cache(4096, shardBits: 0), CacheIndexAndFilterBlocks = true };

        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = family };
        Random random = new(7);
        List<byte[]> probes = [B(""), B("k00010"), B("k00010"), B("zzz")];
        for (int i = 0; i < 400; i++)
            probes.Add(Key(random.Next(2400)));

        using (SolariumDb db = SolariumDb.Open(DbPath, options))
        {
            Assert.Empty(db.MultiGet([]));

            // Seven rounds of puts, deletes and range deletes: the first three compacted into level 6, three more
            // in level 0, and the last one in the memtable.
            for (int round = 0; round < 7; round++)
            {
                for (int i = 0; i < 600; i++)
                {
                    int k = random.Next(2000);
                    if (random.Next(10) == 0)
                        db.Delete(Key(k));
                    else
                        db.Put(Key(k), B($"{round}:{k}:" + new string('v', random.Next(200))));
                }

                int begin = random.Next(2000);
                db.DeleteRange(Key(begin), Key(begin + random.Next(1, 40)));
                if (round < 6)
                    db.Flush();

                if (round == 2)
                    db.CompactRange(null, null);
            }

            Assert.False(db.DefaultColumnFamily.Version.Level6.IsEmpty);
            Assert.Equal(3, db.DefaultColumnFamily.Version.Level0.Length);
            AssertMatchesGets(db, probes);
        }

        using SolariumDb reopened = SolariumDb.Open(DbPath, options);
        AssertMatchesGets(reopened, probes);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_round_reads_the_distinct_blocks_of_its_keys_in_one_batch(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        CountingEnvironment counting = new(scope.Environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { Environment = counting });
        for (int i = 0; i < 2000; i++)
            db.Put(Key(i), B($"{i}:" + new string('v', 100)));

        db.Flush();
        db.CompactRange(null, null);

        // Keys spread over the table, and pairs of neighbours that share a block.
        List<byte[]> keys = [];
        for (int i = 0; i < 2000; i += 125)
            keys.AddRange([Key(i), Key(i + 1)]);

        HashSet<long> blocks = [];
        foreach (byte[] key in keys)
        {
            TableReader table = db.DefaultColumnFamily.Version.Level6.TableFor(key)!;
            Assert.True(table.TryLocate(key, long.MaxValue, fillCache: true, out DataBlockHandle handle));
            blocks.Add((table.Number << 40) + handle.Offset);
        }

        Assert.True(blocks.Count < keys.Count, "no two keys share a block");
        long rounds = db.MultiGetRounds;
        counting.Batches.Clear();
        byte[]?[] values = db.MultiGet(keys);

        Assert.Equal([blocks.Count], counting.Batches);
        Assert.Equal(rounds + 1, db.MultiGetRounds);
        for (int i = 0; i < keys.Count; i++)
            Assert.StartsWith($"{int.Parse(S(keys[i])![1..])}:", S(values[i]));

        // Keys that the memtable answers, and absent keys that the filters rule out, read nothing.
        db.Put(Key(5000), B("memtable"));
        counting.Batches.Clear();
        Assert.Equal(["memtable", null], db.MultiGet([Key(5000), B("absent")]).Select(S));
        Assert.Empty(counting.Batches);
        Assert.Equal(rounds + 1, db.MultiGetRounds);

        // A call of more keys reads them in batches of 32, each with a batch read of its own.
        List<byte[]> many = [.. Enumerable.Range(0, 100).Select(i => Key(i * 19))];
        values = db.MultiGet(many);
        for (int i = 0; i < many.Count; i++)
            Assert.StartsWith($"{i * 19}:", S(values[i]));

        Assert.Equal(4, counting.Batches.Count);
        Assert.All(counting.Batches, requests => Assert.InRange(requests, 1, ReadPath.MaxBatchKeys));
        Assert.Equal(rounds + 5, db.MultiGetRounds);
    }

    // The key is in an older table, and a newer table's filter says it may have it too: the block that the newer
    // table's index names has no version of it, so the key reads the older table in a second round.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_bloom_filter_false_positive_sends_the_key_to_the_older_table_in_a_second_round(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { DisableAutoCompactions = true } };
        using SolariumDb db = SolariumDb.Open(DbPath, options);
        for (int i = 0; i < 4000; i += 2)
            db.Put(Key(i), B($"older:{i}"));

        db.Flush();
        db.CompactRange(null, null);
        for (int i = 1; i < 4000; i += 2)
            db.Put(Key(i), B($"newer:{i}"));

        db.Flush();
        TableReader newer = db.DefaultColumnFamily.Version.Level0[0];
        byte[]? falsePositive = null;
        for (int i = 0; i < 4000 && falsePositive is null; i += 2)
        {
            if (newer.TryLocate(Key(i), long.MaxValue, fillCache: true, out _) && !newer.TryGetNewest(Key(i), long.MaxValue, fillCache: true, out _))
                falsePositive = Key(i);
        }

        Assert.NotNull(falsePositive);
        long rounds = db.MultiGetRounds;
        byte[]?[] values = db.MultiGet([falsePositive, Key(1)]);

        Assert.Equal(S(db.Get(falsePositive)), S(values[0]));
        Assert.StartsWith("older:", S(values[0]));
        Assert.Equal("newer:1", S(values[1]));
        Assert.Equal(rounds + 2, db.MultiGetRounds);
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_damaged_data_block_fails_the_multi_get_naming_the_table_and_the_offset(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { Environment = hooks });
        for (int i = 0; i < 500; i++)
            db.Put(Key(i), B($"{i}:" + new string('v', 100)));

        db.Flush();
        TableReader table = db.DefaultColumnFamily.Version.Level0[0];
        Assert.True(table.TryLocate(Key(250), long.MaxValue, fillCache: true, out DataBlockHandle handle));
        string name = TableFormat.FileName(table.Number);

        hooks.CorruptedReads = (path, offset) => Path.GetFileName(path) == name && offset == handle.Offset;
        SolariumException damage = Assert.Throws<SolariumException>(() => db.MultiGet([Key(1), Key(250)]));
        Assert.Contains(name, damage.Message, StringComparison.Ordinal);
        Assert.Contains($"offset {handle.Offset}", damage.Message, StringComparison.Ordinal);

        hooks.CorruptedReads = (_, _) => false;
        Assert.Equal(["1:" + new string('v', 100), "250:" + new string('v', 100)], db.MultiGet([Key(1), Key(250)]).Select(S));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Multi_gets_racing_a_writer_and_its_compactions_read_every_value(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = scope.Options() with { DefaultColumnFamilyOptions = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 } };
        using SolariumDb db = SolariumDb.Open(DbPath, options);
        int written = 0;
        bool stop = false;
        Exception? failure = null;

        Thread writer = new(() =>
        {
            for (int table = 0; !Volatile.Read(ref stop); table++)
            {
                for (int i = 0; i < 300; i++)
                    db.Put(Key((table * 300) + i), B($"{table}:{i}"));

                db.Flush();
                Volatile.Write(ref written, table + 1);
            }
        });

        Thread[] readers = new Thread[4];
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

                        int[] picks = [.. Enumerable.Range(0, 16).Select(_ => random.Next(tables * 300))];
                        byte[]?[] values = db.MultiGet([.. picks.Select(Key)]);
                        for (int i = 0; i < picks.Length; i++)
                            if (S(values[i]) != $"{picks[i] / 300}:{picks[i] % 300}")
                                throw new InvalidOperationException($"key {picks[i]} read {S(values[i]) ?? "null"}");
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

        // The race needs a few flushes and the compactions they start beside the readers, however slow the host.
        Thread.Sleep(TimeSpan.FromSeconds(2));
        SpinWait.SpinUntil(() => Volatile.Read(ref written) > 3 || Volatile.Read(ref failure) is not null, Timeout);
        Volatile.Write(ref stop, true);
        Assert.True(writer.Join(Timeout), "the writer did not finish");
        foreach (Thread reader in readers)
            Assert.True(reader.Join(Timeout), "a reader did not finish");

        Assert.Null(failure);
        Assert.True(written > 3, $"the writer flushed {written} tables");
        Assert.True(db.MultiGetRounds > 0);
    }

    public void Dispose() => _directory.Dispose();

    private static byte[] Key(int i) => B($"k{i:D5}");

    private static void AssertMatchesGets(SolariumDb db, List<byte[]> probes)
    {
        byte[]?[] values = db.MultiGet(probes);
        Assert.Equal(probes.Count, values.Length);
        for (int i = 0; i < probes.Count; i++)
            Assert.Equal(S(db.Get(probes[i])), S(values[i]));
    }

    /// <summary>Forwards to an environment, and records the number of requests of each batch read.</summary>
    private sealed class CountingEnvironment(IStorageEnvironment inner) : IStorageEnvironment
    {
        public List<int> Batches { get; } = [];

        public string Name => inner.Name;

        public IStorageFile OpenFile(string path) => inner.OpenFile(path);

        public void MultiRead(Span<ReadRequest> requests)
        {
            lock (Batches)
                Batches.Add(requests.Length);

            inner.MultiRead(requests);
        }

        public bool FileExists(string path) => inner.FileExists(path);

        public string[] GetChildren(string directory) => inner.GetChildren(directory);

        public void DeleteFile(string path) => inner.DeleteFile(path);

        public void RenameFile(string source, string target) => inner.RenameFile(source, target);

        public bool TryLinkFile(string source, string target) => inner.TryLinkFile(source, target);

        public void CreateDirectory(string directory) => inner.CreateDirectory(directory);

        public void FlushDirectory(string directory) => inner.FlushDirectory(directory);
    }
}
