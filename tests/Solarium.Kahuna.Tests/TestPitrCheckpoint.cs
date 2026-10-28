
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// Unit tests for the storage-engine checkpoint primitive.
/// Uses <see cref="MemoryPersistenceBackend"/> (no disk DB required):
/// write N keys → CreateCheckpoint → OpenCheckpoint → assert all N present;
/// original store remains writable throughout.
/// </summary>
public sealed class TestPitrCheckpoint : IDisposable
{
    private readonly string _tempRoot = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    private string NewDir(string name = "cp") =>
        Path.Combine(_tempRoot, name);

    private static HLCTimestamp T(long ticks) => new(0, ticks, 0);

    // ── RocksDB round-trip ────────────────────────────────────────────────────────

    [Fact]
    public void RocksDb_Checkpoint_AllKeysReadableFromCheckpoint()
    {
        // RocksDB db lives at {base}/v1/ (the constructor appends /{dbRevision}).
        // Checkpoint goes to {base}/cp — RocksDB must create the leaf itself.
        string baseDir = Path.Combine(_tempRoot, "rocksdb_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp"); // must NOT exist before Save

        using (SolariumPersistenceBackend backend = new(baseDir, "v1"))
        {
            for (int i = 1; i <= 20; i++)
                backend.StoreKeyValues([new($"rk/{i:D3}", [(byte)i], i, 0, 0, 0, 0, i, 0, 0, i, 0, 1)]);

            backend.CreateCheckpoint(cpDir, appliedIndex: 20, appliedTime: T(20));
        }

        // Reopen the checkpoint: path=baseDir, dbRevision="cp" → opens {baseDir}/cp
        using SolariumPersistenceBackend restored = new(baseDir, "cp");
        for (int i = 1; i <= 20; i++)
        {
            var e = restored.GetKeyValue($"rk/{i:D3}");
            Assert.NotNull(e);
            Assert.Equal(i, e!.Revision);
            Assert.Equal((byte)i, e.Value![0]);
        }

        CheckpointManifest manifest = CheckpointManifest.ReadFrom(cpDir);
        Assert.Equal(20, manifest.AppliedIndex);
        Assert.Equal(T(20), manifest.AppliedTime);
    }

    // The checkpoint copies the live WAL instead of flushing the memtables, so the live store gets no new
    // SST (and no L0 file that could start a compaction) and the copy still holds every unflushed row.
    [Fact]
    public void RocksDb_Checkpoint_DoesNotFlushLiveMemtables()
    {
        string baseDir = Path.Combine(_tempRoot, "rocksdb_noflush");
        Directory.CreateDirectory(baseDir);
        string liveDir = Path.Combine(baseDir, "v1");
        string plainCp = Path.Combine(baseDir, "cp");
        string asOfCp = Path.Combine(baseDir, "cp_asof");

        using (SolariumPersistenceBackend backend = new(baseDir, "v1"))
        {
            for (int i = 1; i <= 20; i++)
                backend.StoreKeyValues([Item($"nf/{i:D3}", (byte)i, rev: 1, physicalMs: 50)]);
            backend.StoreKeyValues([Item("nf/post", 9, rev: 1, physicalMs: 200)]);

            int sstBefore = Directory.GetFiles(liveDir, "*.sst").Length;

            backend.CreateCheckpoint(plainCp, appliedIndex: 21, appliedTime: T(200));
            backend.CreateCheckpointAsOf(asOfCp, appliedIndex: 21, cut: T(100), ct: TestContext.Current.CancellationToken);

            Assert.Equal(sstBefore, Directory.GetFiles(liveDir, "*.sst").Length);
        }

        using (SolariumPersistenceBackend plain = new(baseDir, "cp"))
        {
            for (int i = 1; i <= 20; i++)
                Assert.Equal((byte)i, plain.GetKeyValue($"nf/{i:D3}")!.Value![0]);
            Assert.NotNull(plain.GetKeyValue("nf/post"));
        }

        using SolariumPersistenceBackend asOf = new(baseDir, "cp_asof");
        for (int i = 1; i <= 20; i++)
            Assert.Equal((byte)i, asOf.GetKeyValue($"nf/{i:D3}")!.Value![0]);
        Assert.Null(asOf.GetKeyValue("nf/post"));
    }

    [Fact]
    public void RocksDb_Checkpoint_OriginalWritableAfterCheckpoint()
    {
        string baseDir = Path.Combine(_tempRoot, "rocksdb_rw");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using SolariumPersistenceBackend backend = new(baseDir, "v1");
        backend.StoreKeyValues([new("a", [1], 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);
        backend.CreateCheckpoint(cpDir, 1, T(1));

        backend.StoreKeyValues([new("b", [2], 2, 0, 0, 0, 0, 2, 0, 0, 2, 0, 1)]);
        Assert.NotNull(backend.GetKeyValue("b"));
    }

    [Fact]
    public void RocksDb_Checkpoint_SnapshotExcludesPostCheckpointWrites()
    {
        string baseDir = Path.Combine(_tempRoot, "rocksdb_iso");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (SolariumPersistenceBackend backend = new(baseDir, "v1"))
        {
            backend.StoreKeyValues([new("snap/k", [1], 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);
            backend.CreateCheckpoint(cpDir, 1, T(1));
            backend.StoreKeyValues([new("post/k", [2], 2, 0, 0, 0, 0, 2, 0, 0, 2, 0, 1)]);
        }

        using SolariumPersistenceBackend r = new(baseDir, "cp");
        Assert.NotNull(r.GetKeyValue("snap/k"));
        Assert.Null(r.GetKeyValue("post/k"));
    }

    // ── as-of (exact cut) checkpoint ─────────────────────────────────────────────────

    // Stores one key at a specific revision with LastModified = (0, physicalMs, 0).
    private static PersistenceRequestItem Item(string key, byte val, long rev, long physicalMs) =>
        new(key, [val], rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1);

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_CutsAtTimestamp()
    {
        string baseDir = Path.Combine(_tempRoot, "asof_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (SolariumPersistenceBackend backend = new(baseDir, "v1"))
        {
            backend.StoreKeyValues([Item("keep", 1, rev: 1, physicalMs: 50)]);
            backend.StoreKeyValues([Item("future", 1, rev: 1, physicalMs: 200)]);
            backend.StoreKeyValues([Item("updated", 1, rev: 1, physicalMs: 50)]);
            backend.StoreKeyValues([Item("updated", 2, rev: 2, physicalMs: 200)]);

            backend.CreateCheckpointAsOf(cpDir, appliedIndex: 10, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SolariumPersistenceBackend r = new(baseDir, "cp");
        Assert.NotNull(r.GetKeyValue("keep"));
        Assert.Null(r.GetKeyValue("future"));

        ReadOnlyKeyValueEntry? updated = r.GetKeyValue("updated");
        Assert.NotNull(updated);
        Assert.Equal(1, updated!.Revision);        // ~CURRENT reset to the pre-cut revision
        Assert.Equal((byte)1, updated.Value![0]);
    }

    // ── as-of: SetNoRevision keys and locks ──────────────────────────────────────────────────

    private static PersistenceRequestItem NoRevItem(string key, byte val, long rev, long physicalMs) =>
        new(key, [val], rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1, noRevision: true);

    // For locks, StoreLocks maps the 'revision' field to the fencing token and 'value' to the owner.
    private static PersistenceRequestItem LockItem(string resource, long physicalMs) =>
        new(resource, [1, 2, 3], 1, 0, 0, 0, 0, 0, 0, 0, physicalMs, 0, 1);

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_ExcludesLocksKeepsKv()
    {
        string baseDir = Path.Combine(_tempRoot, "asof_locks_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (SolariumPersistenceBackend b = new(baseDir, "v1"))
        {
            b.StoreKeyValues([Item("k", 1, rev: 1, physicalMs: 1)]);
            b.StoreLocks([LockItem("res", 50)]);
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SolariumPersistenceBackend r = new(baseDir, "cp");
        Assert.NotNull(r.GetKeyValue("k"));
        Assert.Null(r.GetLock("res"));
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_NoRevisionKeyCreatedAfterCut_Omitted()
    {
        string baseDir = Path.Combine(_tempRoot, "asof_norev_rocks");
        Directory.CreateDirectory(baseDir);
        using (SolariumPersistenceBackend b = new(baseDir, "v1"))
        {
            b.StoreKeyValues([NoRevItem("nr", 9, rev: 1, physicalMs: 200)]);
            b.CreateCheckpointAsOf(Path.Combine(baseDir, "cp"), appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SolariumPersistenceBackend r = new(baseDir, "cp");
        Assert.Null(r.GetKeyValue("nr"));
    }

    // ── physical purge of post-cut bytes ─────────────────────────────────────────────────────

    private static PersistenceRequestItem ValItem(string key, byte[] value, long rev, long physicalMs) =>
        new(key, value, rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1);

    private static readonly byte[] PreCutSentinel = "PRECUT-Sentinel-4a7f9c2e1b6d80"u8.ToArray();
    private static readonly byte[] PostCutSentinel = "POSTCUT-Sentinel-e3d1f0a9b8c76"u8.ToArray();

    private static bool DirContainsBytes(string dir, byte[] needle)
    {
        foreach (string file in Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories))
        {
            byte[] bytes = File.ReadAllBytes(file);
            if (IndexOf(bytes, needle) >= 0)
                return true;
        }
        return false;
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        for (int i = 0; i + needle.Length <= haystack.Length; i++)
        {
            int j = 0;
            while (j < needle.Length && haystack[i + j] == needle[j]) j++;
            if (j == needle.Length) return i;
        }
        return -1;
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_PhysicallyPurgesPostCutBytes()
    {
        string baseDir = Path.Combine(_tempRoot, "purge_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (SolariumPersistenceBackend b = new(baseDir, "v1"))
        {
            b.StoreKeyValues([ValItem("k", PreCutSentinel, rev: 1, physicalMs: 50)]);
            b.StoreKeyValues([ValItem("k", PostCutSentinel, rev: 2, physicalMs: 200)]);
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        Assert.True(DirContainsBytes(cpDir, PreCutSentinel), "pre-cut value must remain restorable");
        Assert.False(DirContainsBytes(cpDir, PostCutSentinel), "post-cut value must be physically absent");
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_LargeStore_StreamsCorrectly()
    {
        // Exercises the streaming, bounded-batch trim across the flush threshold with many keys.
        string baseDir = Path.Combine(_tempRoot, "large_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        const int n = 6000; // > TrimBatchFlushThreshold (4096)
        using (SolariumPersistenceBackend b = new(baseDir, "v1"))
        {
            for (int i = 0; i < n; i++)
            {
                b.StoreKeyValues([Item($"k/{i:D5}", (byte)(i & 0xFF), rev: 1, physicalMs: 50)]);   // ≤ cut
                b.StoreKeyValues([Item($"k/{i:D5}", (byte)0xEE, rev: 2, physicalMs: 200)]);         // > cut
            }
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SolariumPersistenceBackend r = new(baseDir, "cp");
        for (int i = 0; i < n; i++)
        {
            ReadOnlyKeyValueEntry? e = r.GetKeyValue($"k/{i:D5}");
            Assert.NotNull(e);
            Assert.Equal(1, e!.Revision);                 // rolled back to the pre-cut revision
            Assert.Equal((byte)(i & 0xFF), e.Value![0]);
        }
    }
}
