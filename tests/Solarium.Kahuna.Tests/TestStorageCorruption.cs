using System.Security.Cryptography;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The store's answer to storage damage. Solarium checks every block it reads, so a flipped byte in a data
/// block is found by the next read of that block; what matters is what the backend does with the finding.
/// A damaged block makes a point read, a batched read and a scan throw <see cref="StorageCorruptionException"/>,
/// never a missing key or a short page; the first finding fail-stops the store, so every later operation is
/// refused and stores return false; the storage-failure recovery neither reopens the store nor touches its
/// files; and a store whose files fail at open, or whose own bookkeeping rows do not decode, is refused.
/// Ported from Kahuna's tests of its RocksDB backend.
/// </summary>
public sealed class TestStorageCorruption : IDisposable
{
    private const string Revision = "v1";
    private const string Prefix = "corrupt/";
    private const int Keys = 256;
    private const int ValueBytes = 2048;

    private readonly TempDirectory _directory = new();

    private string StorePath => _directory.Combine("store");

    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_range_scan_over_a_damaged_block_fail_stops_the_store_and_never_ends_short(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PrepareFlushedStore(scope);
        string table = LargestTable();
        FlipByteAtMiddle(table);
        byte[] damaged = File.ReadAllBytes(table);

        using SolariumPersistenceBackend backend = Open(scope);

        // The bookkeeping rows sort first and live in the first block: the open and the identity read see intact data.
        Assert.Null(backend.GetFailStopReason());
        Assert.Null(backend.GetStoreIdentity());

        StorageCorruptionException thrown = Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValueByRange(Prefix, null, Keys * 2));
        Assert.Contains("fail-stopped", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("bad checksum", thrown.Message, StringComparison.Ordinal);
        Assert.Equal(Path.Combine(StorePath, Revision), thrown.StorePath);

        // Latched: every later operation is refused with the same failure, whether or not it would touch the damaged block.
        Assert.Contains("bad checksum", backend.GetFailStopReason(), StringComparison.Ordinal);
        AssertRefusesEverything(backend);

        // Fail-stop, not reopen: the recovery declines, the store stays latched, and the damaged file is as it was.
        Assert.False(backend.TryRecoverFromStorageFailure());
        Assert.NotNull(backend.GetFailStopReason());
        Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValue(Key(0)));
        Assert.Equal(damaged, File.ReadAllBytes(table));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_point_read_of_a_damaged_block_is_corruption_and_never_a_missing_key(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PrepareFlushedStore(scope);
        FlipByteAtMiddle(LargestTable());

        using SolariumPersistenceBackend backend = Open(scope);

        // The damaged block holds some of the keys; the first point read of one of them fails, with the terminal
        // exception, never with null ("does not exist").
        int served = 0;
        StorageCorruptionException? thrown = null;
        for (int i = 0; i < Keys && thrown is null; i++)
            try
            {
                Assert.NotNull(backend.GetKeyValue(Key(i)));
                served++;
            }
            catch (StorageCorruptionException e)
            {
                thrown = e;
            }

        Assert.NotNull(thrown);
        Assert.InRange(served, 1, Keys - 1);
        Assert.NotNull(backend.GetFailStopReason());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_batched_read_of_a_damaged_block_is_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PrepareFlushedStore(scope);
        FlipByteAtMiddle(LargestTable());

        using SolariumPersistenceBackend backend = Open(scope);
        string[] keys = [.. Enumerable.Range(0, Keys).Select(Key)];
        Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValues(keys));
        Assert.NotNull(backend.GetFailStopReason());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_whole_family_scan_over_a_damaged_block_is_corruption(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PrepareFlushedStore(scope);
        FlipByteAtMiddle(LargestTable());

        using SolariumPersistenceBackend backend = Open(scope);
        Assert.Throws<StorageCorruptionException>(() =>
        {
            string? cursor = null;
            do
                cursor = backend.ScanKeyValues(cursor, 32).NextCursor;
            while (cursor is not null);
        });
        Assert.NotNull(backend.GetFailStopReason());
    }

    // A compaction that reads the damaged block fails in the background and stops the database's writes; the next
    // store meets the refusal, which carries the corruption, and fail-stops instead of reopening. The rounds write
    // keys between the damaged table's, so their tables overlap it and the compaction must merge it, while the
    // store's reads of their current rows find nothing in it: the Bloom filters keep them out of its blocks.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Damage_found_by_a_compaction_fail_stops_the_store_without_a_reopen_and_leaves_its_files(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PrepareFlushedStore(scope);
        FlipByteAtMiddle(LargestTable());

        using SolariumPersistenceBackend backend = Open(scope);
        Dictionary<string, byte[]> before = [];
        bool stopped = false;
        for (int round = 0; round < 8 && !stopped; round++)
        {
            // The fourth level 0 table starts the compaction that reads the damage.
            stopped = !backend.StoreKeyValues([.. Enumerable.Range(0, 16).Select(i => MakeItem($"{Key(i * 16)}r{round}", 1))]);
            if (stopped)
                break;

            // The forced flush meets the stopped writes as soon as the compaction has failed.
            try
            {
                backend.FlushAndMeasureTableMemoryForTesting();
            }
            catch (SolariumException e) when (e.IsCorruption)
            {
                break;
            }
        }

        // Each probe is a new key: the store skips a write that is not newer than the stored row, and would answer true.
        int probes = 0;
        Assert.True(WaitUntil(() => !backend.StoreKeyValues([MakeItem($"{Key(0)}probe{probes++}", 1)])));
        Assert.Contains("accepts no more writes after a WAL, flush or compaction failure", backend.GetFailStopReason(), StringComparison.Ordinal);
        Assert.Contains("bad checksum", backend.GetFailStopReason(), StringComparison.Ordinal);

        foreach (string file in StoreFiles())
            before[Path.GetFileName(file)] = File.ReadAllBytes(file);

        Assert.False(backend.TryRecoverFromStorageFailure());
        AssertRefusesEverything(backend);
        foreach (string file in StoreFiles())
            Assert.Equal(before[Path.GetFileName(file)], File.ReadAllBytes(file));

        Assert.Equal(before.Count, StoreFiles().Count());
    }

    // The store's files but LOCK, which the open store holds.
    private IEnumerable<string> StoreFiles() =>
        Directory.GetFiles(Path.Combine(StorePath, Revision)).Where(f => Path.GetFileName(f) != "LOCK");

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_truncated_table_refuses_the_open_and_releases_the_directory(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PrepareFlushedStore(scope);
        string table = LargestTable();
        byte[] whole = File.ReadAllBytes(table);
        File.WriteAllBytes(table, whole[..^1024]);

        StorageCorruptionException thrown = Assert.Throws<StorageCorruptionException>(() => Open(scope));
        Assert.Contains("fail-stopped", thrown.Message, StringComparison.Ordinal);
        Assert.Contains(Path.GetFileName(table), thrown.Message, StringComparison.Ordinal);

        // The refusal released the directory: the whole table opens again.
        File.WriteAllBytes(table, whole);
        using SolariumPersistenceBackend reopened = Open(scope);
        Assert.NotNull(reopened.GetKeyValue(Key(0)));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_durability_floor_row_of_the_wrong_length_refuses_the_open(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumPersistenceBackend backend = Open(scope))
        {
            Assert.True(backend.StoreKeyValues([MakeItem(Key(0), 1)]));
            Assert.True(backend.StoreDurabilityFloors([(1, 10L)]));
        }

        PutRawKvRow(scope, " durability_floor_1"u8.ToArray(), [1, 2, 3, 4, 5]);
        StorageCorruptionException thrown = Assert.Throws<StorageCorruptionException>(() => Open(scope));
        Assert.Contains("durability floor row", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("5 bytes", thrown.Message, StringComparison.Ordinal);

        // The refusal released the database: a corrected row opens again.
        PutRawKvRow(scope, " durability_floor_1"u8.ToArray(), BitConverter.GetBytes(10L));
        using SolariumPersistenceBackend reopened = Open(scope);
        Assert.Equal(10L, reopened.GetDurabilityFloor(1));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_identity_row_that_does_not_decode_is_damage(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumPersistenceBackend backend = Open(scope))
            Assert.True(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("localhost:1")));

        PutRawKvRow(scope, "\0store_identity"u8.ToArray(), Encoding.UTF8.GetBytes("garbage"));
        using SolariumPersistenceBackend reopened = Open(scope);
        Assert.Throws<StorageCorruptionException>(() => reopened.GetStoreIdentity());
        Assert.Contains("store identity row", reopened.GetFailStopReason(), StringComparison.Ordinal);
        Assert.False(reopened.StoreKeyValues([MakeItem(Key(1), 1)]));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_store_is_stamped_once_and_keeps_its_identity_across_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        PersistentStoreIdentity identity = PersistentStoreIdentity.Create("node-a:2070");
        using (SolariumPersistenceBackend backend = Open(scope))
        {
            Assert.True(backend.SupportsStoreIdentity);
            Assert.True(backend.KeepsDurabilityFloors);
            Assert.Null(backend.GetStoreIdentity());
            Assert.False(backend.HoldsDurableState());
            Assert.True(backend.WriteStoreIdentity(identity));

            // The identity is an internal row: it is not durable state of the node's own, and no scan sees it.
            Assert.False(backend.HoldsDurableState());
            Assert.Empty(backend.ScanKeyValues(null, 16).Items);
        }

        using SolariumPersistenceBackend reopened = Open(scope);
        Assert.Equal(identity, reopened.GetStoreIdentity());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Rows_locks_and_floors_each_count_as_durable_state(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumPersistenceBackend backend = Open(scope))
        {
            Assert.True(backend.StoreDurabilityFloors([(2, 1L)]));
            Assert.True(backend.HoldsDurableState());
        }

        Directory.Delete(StorePath, recursive: true);
        using (SolariumPersistenceBackend backend = Open(scope))
        {
            Assert.True(backend.StoreLocks([new PersistenceRequestItem("lock/a", Encoding.UTF8.GetBytes("owner"), 1, 0, 1000, 0, 0, 0, 0, 0, 1, 0, (int)LockState.Locked)]));
            Assert.True(backend.HoldsDurableState());
        }

        Directory.Delete(StorePath, recursive: true);
        using SolariumPersistenceBackend rows = Open(scope);
        Assert.True(rows.StoreKeyValues([MakeItem(Key(0), 1)]));
        Assert.True(rows.HoldsDurableState());
    }

    // The identity names the node that owns the live store, never the data: an as-of image, which a restore or a
    // seed on another node opens, carries none, so that node stamps its own instead of refusing the store as foreign.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void An_as_of_image_carries_no_identity(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumPersistenceBackend backend = Open(scope))
        {
            Assert.True(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("localhost:1")));
            Assert.True(backend.StoreKeyValues([MakeItem("image/k", 1)]));
            backend.CreateCheckpointAsOf(_directory.Combine("image"), 10, new HLCTimestamp(0, long.MaxValue, 0), TestContext.Current.CancellationToken);
        }

        using SolariumPersistenceBackend restored = new(_directory.Path, "image", NullLogger.Instance, scope.Environment);
        Assert.NotNull(restored.GetKeyValue("image/k"));
        Assert.Null(restored.GetStoreIdentity());
        Assert.True(restored.HoldsDurableState());
    }

    // A merge copies another node's rows into an image, never that node's identity.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_merge_does_not_copy_the_identity_of_its_source(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string source = _directory.Combine("source");
        using (SolariumPersistenceBackend backend = Open(scope))
        {
            Assert.True(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("other-node:1")));
            Assert.True(backend.StoreKeyValues([MakeItem("merge/a", 1)]));
            backend.CreateCheckpoint(source, 10, new HLCTimestamp(0, 1, 0));
        }

        using (SolariumPersistenceBackend copy = new(_directory.Path, "source", NullLogger.Instance, scope.Environment))
            Assert.Equal("other-node:1", copy.GetStoreIdentity()?.NodeEndpoint);

        string target = _directory.Combine("target");
        using (SolariumPersistenceBackend other = new(_directory.Combine("other"), Revision, NullLogger.Instance, scope.Environment))
        {
            Assert.True(other.StoreKeyValues([MakeItem("merge/b", 1)]));
            other.CreateCheckpointAsOf(target, 10, new HLCTimestamp(0, long.MaxValue, 0), TestContext.Current.CancellationToken);
        }

        SolariumPersistenceBackend.MergeCheckpointInto(target, source, scope.Environment, TestContext.Current.CancellationToken);
        using SolariumPersistenceBackend merged = new(_directory.Path, "target", NullLogger.Instance, scope.Environment);
        Assert.NotNull(merged.GetKeyValue("merge/a"));
        Assert.NotNull(merged.GetKeyValue("merge/b"));
        Assert.Null(merged.GetStoreIdentity());
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    private SolariumPersistenceBackend Open(EnvironmentScope scope) => new(StorePath, Revision, NullLogger.Instance, scope.Environment);

    private static string Key(int i) => $"{Prefix}k{i:D4}";

    // Every operation is refused once the store is fail-stopped: reads throw, stores return false.
    private static void AssertRefusesEverything(SolariumPersistenceBackend backend)
    {
        Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValue(Key(0)));
        Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValues([Key(0)]));
        Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValueByPrefix(Prefix));
        Assert.Throws<StorageCorruptionException>(() => backend.ScanKeyValues(null, 1));
        Assert.Throws<StorageCorruptionException>(() => backend.GetLock("lock/a"));
        Assert.Throws<StorageCorruptionException>(() => backend.GetStoreIdentity());
        Assert.Throws<StorageCorruptionException>(() => backend.HoldsDurableState());
        Assert.Throws<StorageCorruptionException>(() => backend.GetDurabilityFloor(1));
        Assert.Throws<StorageCorruptionException>(() => backend.GetPrunedHistoryFloor());
        Assert.False(backend.StoreKeyValues([MakeItem("corrupt/after", 1)]));
        Assert.False(backend.StoreLocks([new PersistenceRequestItem("lock/a", Encoding.UTF8.GetBytes("owner"), 1, 0, 1000, 0, 0, 0, 0, 0, 1, 0, (int)LockState.Locked)]));
        Assert.False(backend.StoreDurabilityFloors([(1, 10L)]));
        Assert.False(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("localhost:1")));
    }

    // The keys with incompressible values, so their blocks stay raw, flushed to one table; the store is closed after.
    private void PrepareFlushedStore(EnvironmentScope scope)
    {
        using SolariumPersistenceBackend backend = Open(scope);
        List<PersistenceRequestItem> items = new(Keys);
        for (int i = 0; i < Keys; i++)
        {
            byte[] value = new byte[ValueBytes];
            RandomNumberGenerator.Fill(value);
            items.Add(MakeItem(Key(i), 1, value));
        }

        Assert.True(backend.StoreKeyValues(items));
        Assert.True(backend.StoreDurabilityFloors([(1, 5L)]));
        backend.FlushAndMeasureTableMemoryForTesting();
    }

    private string LargestTable()
    {
        string[] files = Directory.GetFiles(Path.Combine(StorePath, Revision), "*.sst");
        Assert.NotEmpty(files);
        return files.OrderByDescending(f => new FileInfo(f).Length).First();
    }

    // Data blocks come first in a table and the index partitions, the properties and the footer last, so with
    // hundreds of kilobytes of rows the middle lies in a data block of user rows, past the first block, which
    // holds the store's bookkeeping rows.
    private static void FlipByteAtMiddle(string table)
    {
        byte[] bytes = File.ReadAllBytes(table);
        bytes[bytes.Length / 2] ^= 0xFF;
        File.WriteAllBytes(table, bytes);
    }

    // One raw row in the closed store's kv family, written through Solarium itself.
    private void PutRawKvRow(EnvironmentScope scope, byte[] key, byte[] value)
    {
        using SolariumDb db = SolariumDb.Open(Path.Combine(StorePath, Revision), scope.Options() with { CreateIfMissing = false }, ["kv", "locks"]);
        db.Put(key, value, db.GetColumnFamily("kv"), WriteOptions.Synced);
    }

    private static bool WaitUntil(Func<bool> condition)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;

            Thread.Sleep(20);
        }

        return condition();
    }

    private static PersistenceRequestItem MakeItem(string key, long revision, byte[]? value = null) =>
        new(key,
            value ?? Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);
}
