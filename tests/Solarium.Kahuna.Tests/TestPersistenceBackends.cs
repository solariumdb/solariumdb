
using Kommander;
using Kommander.Time;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// Tests for GetKeyValueByRange across all three IPersistenceBackend implementations.
/// </summary>
public class TestPersistenceBackends
{
    private static PersistenceRequestItem MakeItem(string key, long revision, long lastModifiedPhysical = 0) =>
        new(key,
            System.Text.Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: lastModifiedPhysical,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static PersistenceRequestItem MakeItem(string key, int i) => MakeItem(key, (long)i);

    private static List<PersistenceRequestItem> MakeItems(string prefix, int count, int startIndex = 0) =>
        Enumerable.Range(startIndex, count)
                  .Select(i => MakeItem($"{prefix}/{i:D4}", i))
                  .ToList();

    private static void AssertOrderedAndBounded(
        List<(string, ReadOnlyKeyValueEntry)> items,
        string prefix,
        int expectedCount)
    {
        Assert.Equal(expectedCount, items.Count);
        for (int i = 0; i < items.Count; i++)
        {
            Assert.StartsWith(prefix, items[i].Item1, StringComparison.Ordinal);
            if (i > 0)
                Assert.True(string.CompareOrdinal(items[i].Item1, items[i - 1].Item1) > 0);
        }
    }

    // ─── SolariumPersistenceBackend ───────────────────────────────────────────────

    private static string RocksDbTempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_rocksdb_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>
    /// The persisted durability floor is monotonic (parity with the SQLite backend's MAX()
    /// upsert): after a restart the background writer's advanced-only filter resets, so its first
    /// write can carry a floor below the persisted one — the backend must keep the higher value.
    /// The floor must also survive a close/reopen and read back per partition.
    /// </summary>
    [Fact]
    public void TestRocksDbDurabilityFloorIsMonotonicAndDurable()
    {
        string path = RocksDbTempPath();

        using (SolariumPersistenceBackend backend = new(path, "v1"))
        {
            Assert.Equal(-1, backend.GetDurabilityFloor(2));

            Assert.True(backend.StoreDurabilityFloors([(2, 42), (3, 7)]));
            Assert.Equal(42, backend.GetDurabilityFloor(2));
            Assert.Equal(7, backend.GetDurabilityFloor(3));

            // A lower floor never regresses the persisted value; a higher one advances it.
            Assert.True(backend.StoreDurabilityFloors([(2, 10)]));
            Assert.Equal(42, backend.GetDurabilityFloor(2));

            Assert.True(backend.StoreDurabilityFloors([(2, 50)]));
            Assert.Equal(50, backend.GetDurabilityFloor(2));
        }

        using (SolariumPersistenceBackend reopened = new(path, "v1"))
        {
            Assert.Equal(50, reopened.GetDurabilityFloor(2));
            Assert.Equal(7, reopened.GetDurabilityFloor(3));

            Assert.True(reopened.RemoveDurabilityFloor(2));
            Assert.Equal(-1, reopened.GetDurabilityFloor(2));
            Assert.Equal(7, reopened.GetDurabilityFloor(3));
        }
    }

    [Fact]
    public void TestRocksDbGetByRangeReturnsAllInPrefix()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        AssertOrderedAndBounded(result, "svc", 10);
    }

    [Fact]
    public void TestRocksDbGetByRangeLimitCaps()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 4);

        Assert.Equal(4, result.Count);
    }

    [Fact]
    public void TestRocksDbGetByRangeStartKeyResumes()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0005", 100);

        Assert.Equal(5, result.Count);
        Assert.Equal("svc/0005", result[0].Item1);
        Assert.Equal("svc/0009", result[4].Item1);
    }

    [Fact]
    public void TestRocksDbGetByRangeStartKeyBetweenExistingKeys()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(Enumerable.Range(0, 5)
            .Select(i => MakeItem($"svc/{i * 2:D4}", i))
            .ToList());

        // "svc/0003" falls between svc/0002 and svc/0004
        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0003", 100);

        Assert.Equal(3, result.Count);
        Assert.Equal("svc/0004", result[0].Item1);
    }

    [Fact]
    public void TestRocksDbGetByRangePrefixBoundaryExcludesSiblingPrefix()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 5));
        backend.StoreKeyValues(MakeItems("config", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        Assert.Equal(5, result.Count);
        Assert.All(result, item =>
            Assert.StartsWith("svc", item.Item1, StringComparison.Ordinal));
    }

    [Fact]
    public void TestRocksDbGetByRangeEmptyRange()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/9999", 100);

        Assert.Empty(result);
    }

    [Fact]
    public void TestRocksDbGetByRangeFullPageCoverage()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 25));

        const int pageSize = 7;
        List<string> allKeys = [];
        string? cursor = null;

        while (true)
        {
            List<(string, ReadOnlyKeyValueEntry)> page = backend.GetKeyValueByRange("svc", cursor, pageSize);
            if (page.Count == 0) break;

            allKeys.AddRange(page.Select(p => p.Item1));
            // GetKeyValueByRange is inclusive-only (no startInclusive parameter).
            // Appending "~￿" skips the last returned key's RocksDB internal
            // entries (~revision / ~CURRENT) and lands before the next logical key
            // because all test keys are fixed-width (svc/NNNN) so no key is a prefix
            // of another. Variable-length keys require the handler's exclusive-start
            // path (startInclusive=false) instead — see KeyValuesManager.LocateAndScanRange.
            cursor = page[^1].Item1 + "~￿";

            if (page.Count < pageSize) break;
        }

        Assert.Equal(25, allKeys.Count);
        Assert.Equal(allKeys.Distinct().Count(), allKeys.Count);
        for (int i = 0; i < allKeys.Count - 1; i++)
            Assert.True(string.CompareOrdinal(allKeys[i], allKeys[i + 1]) < 0);
    }

    private static void StoreRevisionsRocksDb(SolariumPersistenceBackend backend, string key, int count)
    {
        for (long revision = 1; revision <= count; revision++)
            backend.StoreKeyValues([MakeItem(key, revision)]);
    }

    private static int CountRocksDbRevisionEntries(SolariumPersistenceBackend backend, string key)
    {
        // Exercise GetKeyValueRevision for each possible revision up to a generous upper bound.
        // Count only entries that are found (non-null) and are not the CURRENT read path.
        // We do this by probing a range wider than any test writes (1..100).
        int count = 0;
        for (long rev = 1; rev <= 100; rev++)
        {
            if (backend.GetKeyValueRevision(key, rev) is not null)
                count++;
        }
        return count;
    }

    [Fact]
    public void TestRocksDbRetentionCountPrunesOldRevisions()
    {
        const string key = "retention/count";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 10);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
        Assert.False(result.BatchLimitReached);
        Assert.Equal(7, result.RevisionsDeleted);
        Assert.Equal(3, CountRocksDbRevisionEntries(backend, key));

        ReadOnlyKeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(10, current.Revision);
        Assert.Equal("val10", System.Text.Encoding.UTF8.GetString(current.Value!));

        Assert.NotNull(backend.GetKeyValueRevision(key, 10));
        Assert.Null(backend.GetKeyValueRevision(key, 7));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Fact]
    public void TestRocksDbRetentionCountPreservesCurrentRevisionWhenRetentionIsOne()
    {
        const string key = "retention/current";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 5);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out _));

        Assert.Equal(1, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 5));
        Assert.Null(backend.GetKeyValueRevision(key, 4));
    }

    [Fact]
    public void TestRocksDbRetentionBatchSizeLimitsDeletesAndReportsBacklog()
    {
        const string key = "retention/batch";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 10);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 2, HLCTimestamp.Zero, out RevisionPruneResult firstPass));
        Assert.Equal(2, firstPass.RevisionsDeleted);
        Assert.True(firstPass.BatchLimitReached);
        Assert.True(CountRocksDbRevisionEntries(backend, key) > 3);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out RevisionPruneResult secondPass));
        Assert.False(secondPass.BatchLimitReached);
        Assert.Equal(3, CountRocksDbRevisionEntries(backend, key));
    }

    [Fact]
    public void TestRocksDbRetentionAgePrunesOldRowsButProtectsCurrentRevision()
    {
        const string key = "retention/age";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (long revision = 1; revision <= 5; revision++)
        {
            long lastModified = revision < 5 ? now - 3_600_000 : now;
            backend.StoreKeyValues([MakeItem(key, revision, lastModified)]);
        }

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 0, TimeSpan.FromMinutes(30), batchSize: 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
        Assert.False(result.BatchLimitReached);
        Assert.Equal(1, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValue(key));
        Assert.Equal(5, backend.GetKeyValue(key)!.Revision);
        Assert.NotNull(backend.GetKeyValueRevision(key, 5));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Fact]
    public void TestRocksDbRetentionSweepPrunesWithoutTargetedKeys()
    {
        const string key = "retention/sweep";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 8);

        Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
        Assert.Equal(2, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 8));
        Assert.Null(backend.GetKeyValueRevision(key, 6));
    }

    [Fact]
    public void TestRocksDbRetentionCurrentMarkerNeverDeleted()
    {
        const string key = "retention/marker";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 5);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out _));

        Assert.NotNull(backend.GetKeyValue(key));
        Assert.Equal(5, backend.GetKeyValue(key)!.Revision);
    }

    [Fact]
    public void TestRocksDbRetentionHandlesKeysContainingTilde()
    {
        const string key = "prefix~part/suffix";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 6);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 2, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
        Assert.Equal(4, result.RevisionsDeleted);
        Assert.Equal(2, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValue(key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 6));
        Assert.Null(backend.GetKeyValueRevision(key, 3));
    }

    [Fact]
    public void TestRocksDbRetentionLongKeyExceedsStackThreshold()
    {
        // Key whose UTF-8 byte count exceeds the 256-byte KeyStackThreshold so
        // PruneRevisionsForKey takes the ArrayPool path for both key lookups.
        string key = "services/" + new string('k', 260) + "/state";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 6);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 2, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
        Assert.Equal(4, result.RevisionsDeleted);
        Assert.Equal(2, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValue(key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 6));
        Assert.Null(backend.GetKeyValueRevision(key, 3));
    }

    [Fact]
    public void TestRocksDbTargetedBacklogRequeuesOnlyKeysWithRemainingWork()
    {
        const string keyA = "ret/a";
        const string keyB = "ret/b";
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, keyA, 4);   // retentionCount 3 → 1 prunable
        StoreRevisionsRocksDb(backend, keyB, 10);  // retentionCount 3 → 7 prunable

        // batchSize 4: keyA fully pruned (1 delete), keyB consumes the remaining 3 and still has work.
        Assert.True(backend.PruneKeyValueRevisions([keyA, keyB], retentionCount: 3, TimeSpan.Zero, batchSize: 4, HLCTimestamp.Zero, out RevisionPruneResult result));

        Assert.True(result.BatchLimitReached);
        Assert.NotNull(result.RemainingKeys);
        Assert.Equal([keyB], result.RemainingKeys!.ToArray());

        Assert.Equal(3, CountRocksDbRevisionEntries(backend, keyA));
        Assert.True(CountRocksDbRevisionEntries(backend, keyB) > 3);
    }

    [Fact]
    public void TestRocksDbSweepIsBoundedAndResumesAcrossPasses()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        for (int k = 0; k < 20; k++)
            StoreRevisionsRocksDb(backend, $"swp/k{k:D2}", 4);  // 2 prunable each at count 2

        const int budget = 5;
        int passes = 0;
        int totalDeleted = 0;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: budget, HLCTimestamp.Zero, out RevisionPruneResult r));

            Assert.True(r.KeysVisited <= budget);
            Assert.True(r.RevisionsDeleted <= budget);

            totalDeleted += r.RevisionsDeleted;
            passes++;

            if (!r.BatchLimitReached)
                break;

            Assert.True(passes < 100, "sweep did not converge");
        }

        Assert.True(passes > 1);
        Assert.Equal(40, totalDeleted);

        for (int k = 0; k < 20; k++)
            Assert.Equal(2, CountRocksDbRevisionEntries(backend, $"swp/k{k:D2}"));
    }

    [Fact]
    public void TestRocksDbSweepWithNothingPrunableScansBoundedThenReportsNoBacklog()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        for (int k = 0; k < 20; k++)
            StoreRevisionsRocksDb(backend, $"none/k{k:D2}", 1);

        const int budget = 5;
        int passes = 0;
        bool sawBacklog = false;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: budget, HLCTimestamp.Zero, out RevisionPruneResult r));

            Assert.Equal(0, r.RevisionsDeleted);
            Assert.True(r.KeysVisited <= budget);

            passes++;
            if (r.BatchLimitReached)
                sawBacklog = true;
            else
                break;

            Assert.True(passes < 100, "sweep did not converge");
        }

        Assert.True(sawBacklog);
        Assert.True(passes > 1);
    }

    private static IPersistenceBackend CreateBackend(string kind) => kind switch
    {
        "rocksdb" => new SolariumPersistenceBackend(RocksDbTempPath(), "v1"),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static PersistenceRequestItem MakeLockItem(string resource, long fencingToken) =>
        new(resource,
            System.Text.Encoding.UTF8.GetBytes("owner" + fencingToken),
            revision: fencingToken,
            expiresNode: 0, expiresPhysical: 1000, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: fencingToken, lastModifiedCounter: 0,
            state: (int)global::Kahuna.Server.Locks.Data.LockState.Locked);

    private static List<(string Key, ReadOnlyKeyValueEntry Entry)> DrainKeyValueScan(IPersistenceBackend backend, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> all = [];
        string? cursor = null;
        int pages = 0;

        while (true)
        {
            KeyValueScanPage page = backend.ScanKeyValues(cursor, limit);
            all.AddRange(page.Items);
            Assert.True(page.Items.Count <= limit);
            if (page.NextCursor is null)
                return all;
            cursor = page.NextCursor;
            Assert.True(++pages < 10_000, "scan did not converge");
        }
    }

    private static List<(string Resource, global::Kahuna.Server.Locks.Data.LockRow Entry)> DrainLockScan(IPersistenceBackend backend, int limit)
    {
        List<(string, global::Kahuna.Server.Locks.Data.LockRow)> all = [];
        string? cursor = null;
        int pages = 0;

        while (true)
        {
            LockScanPage page = backend.ScanLocks(cursor, limit);
            all.AddRange(page.Items);
            Assert.True(page.Items.Count <= limit);
            if (page.NextCursor is null)
                return all;
            cursor = page.NextCursor;
            Assert.True(++pages < 10_000, "scan did not converge");
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    public void TestScanKeyValuesCoversEveryKeyExactlyOnce(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            // Many key spaces (spreads across SQLite shards), plus a second write of every third
            // key at a higher revision — the scan must return each key once, at its latest
            // revision, never surfacing revision-history rows as extra keys.
            HashSet<string> expected = [];
            for (int space = 0; space < 8; space++)
            {
                List<PersistenceRequestItem> items = MakeItems($"space{space}", 5);
                Assert.True(backend.StoreKeyValues(items));
                foreach (PersistenceRequestItem item in items)
                    expected.Add(item.Key);
            }

            List<PersistenceRequestItem> rewrites = [];
            foreach (string key in expected.Where(static (_, i) => i % 3 == 0))
                rewrites.Add(MakeItem(key, revision: 90, lastModifiedPhysical: 90));
            Assert.True(backend.StoreKeyValues(rewrites));

            List<(string Key, ReadOnlyKeyValueEntry Entry)> scanned = DrainKeyValueScan(backend, limit: 7);

            Assert.Equal(expected.Count, scanned.Count);
            Assert.Equal(expected, scanned.Select(static s => s.Key).ToHashSet());

            HashSet<string> rewritten = rewrites.Select(static r => r.Key).ToHashSet();
            foreach ((string key, ReadOnlyKeyValueEntry entry) in scanned)
                Assert.Equal(rewritten.Contains(key) ? 90 : long.Parse(key[^4..]), entry.Revision);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    public void TestScanLocksCoversEveryResourceExactlyOnce(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            HashSet<string> expected = [];
            List<PersistenceRequestItem> items = [];
            for (int space = 0; space < 5; space++)
            {
                for (int i = 0; i < 3; i++)
                {
                    string resource = $"lockspace{space}/res{i}";
                    items.Add(MakeLockItem(resource, fencingToken: 1));
                    expected.Add(resource);
                }
            }
            Assert.True(backend.StoreLocks(items));

            // Re-acquisitions at a higher fencing token must not duplicate the resource.
            Assert.True(backend.StoreLocks([MakeLockItem("lockspace0/res0", 7), MakeLockItem("lockspace4/res2", 7)]));

            List<(string Resource, global::Kahuna.Server.Locks.Data.LockRow Entry)> scanned = DrainLockScan(backend, limit: 4);

            Assert.Equal(expected.Count, scanned.Count);
            Assert.Equal(expected, scanned.Select(static s => s.Resource).ToHashSet());
            Assert.Equal(7, scanned.Single(static s => s.Resource == "lockspace0/res0").Entry.FencingToken);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    public void TestScanOnEmptyBackendTerminatesWithNothing(string kind)
    {
        IPersistenceBackend backend = CreateBackend(kind);
        try
        {
            Assert.Empty(DrainKeyValueScan(backend, limit: 10));
            Assert.Empty(DrainLockScan(backend, limit: 10));
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    // ─── End-bounded range pages (every backend) ─────────────────────────────────

    private static List<string> Keys(List<(string, ReadOnlyKeyValueEntry)> items) => items.Select(static i => i.Item1).ToList();

    private static List<(string, ReadOnlyKeyValueEntry)> Bounded(
        IPersistenceBackend backend, string prefix, string? startKey, string? endKey, bool endInclusive, int limit)
    {
        KeyValueScanTally tally = default;
        return backend.GetKeyValueByRange(prefix, startKey, endKey, endInclusive, limit, ref tally);
    }

    /// <summary>
    /// The end-bounded page contract, identical for every backend: the page holds the first `limit`
    /// keys of the prefix at or after the start key and inside the end bound, in ordinal order, and a
    /// page shorter than `limit` means the bounded range is exhausted.
    /// </summary>
    private static void AssertEndBoundedRangeContract(IPersistenceBackend backend)
    {
        backend.StoreKeyValues(MakeItems("svc", 20));          // svc/0000 .. svc/0019
        backend.StoreKeyValues([MakeItem("svd/0000", 1)]);    // sibling prefix: never part of an "svc" page

        // Exclusive and inclusive bounds inside the page.
        Assert.Equal(["svc/0005", "svc/0006", "svc/0007"], Keys(Bounded(backend, "svc", "svc/0005", "svc/0008", false, 100)));
        Assert.Equal(["svc/0005", "svc/0006", "svc/0007", "svc/0008"], Keys(Bounded(backend, "svc", "svc/0005", "svc/0008", true, 100)));

        // A bound between two keys behaves the same either way.
        Assert.Equal(["svc/0005", "svc/0006"], Keys(Bounded(backend, "svc", "svc/0005", "svc/0006x", false, 100)));
        Assert.Equal(["svc/0005", "svc/0006"], Keys(Bounded(backend, "svc", "svc/0005", "svc/0006x", true, 100)));

        // The limit cuts before the bound; the bound cuts before the limit.
        Assert.Equal(["svc/0005", "svc/0006", "svc/0007"], Keys(Bounded(backend, "svc", "svc/0005", "svc/0015", true, 3)));
        Assert.Equal(["svc/0005", "svc/0006", "svc/0007", "svc/0008"], Keys(Bounded(backend, "svc", "svc/0005", "svc/0008", true, 4)));

        // A bound past the last key of the prefix: the prefix still ends the page.
        Assert.Equal(["svc/0017", "svc/0018", "svc/0019"], Keys(Bounded(backend, "svc", "svc/0017", "svc/9999", false, 100)));
        Assert.Equal(["svc/0017", "svc/0018", "svc/0019"], Keys(Bounded(backend, "svc", "svc/0017", "t", false, 100)));

        // A bound at or below the start.
        Assert.Equal(["svc/0010"], Keys(Bounded(backend, "svc", "svc/0010", "svc/0010", true, 100)));
        Assert.Empty(Bounded(backend, "svc", "svc/0010", "svc/0010", false, 100));
        Assert.Empty(Bounded(backend, "svc", "svc/0010", "svc/0005", true, 100));

        // No start key: the page begins at the prefix.
        Assert.Equal(["svc/0000", "svc/0001"], Keys(Bounded(backend, "svc", null, "svc/0002", false, 100)));

        // A null bound is the plain page.
        Assert.Equal(Keys(backend.GetKeyValueByRange("svc", "svc/0015", 100)), Keys(Bounded(backend, "svc", "svc/0015", null, false, 100)));

        // Zero limit returns nothing whatever the bound.
        Assert.Empty(Bounded(backend, "svc", null, "svc/0010", true, 0));
    }

    [Fact]
    public void TestRocksDbGetByRangeStopsAtEndKey()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");
        AssertEndBoundedRangeContract(backend);
    }

    /// <summary>
    /// The bound ends the RocksDB walk before the revision rows of the first key past it: the cost of a
    /// bounded page is the cost of the keys inside the bound only. Each key holds more revisions than the
    /// run-seek threshold, so a walk that stepped into a key past the bound would show in the tally.
    /// </summary>
    [Fact]
    public void TestRocksDbGetByRangeWithEndKeyStepsNoRevisionRowsPastTheBound()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        for (int i = 1; i <= 6; i++)
            for (int revision = 1; revision <= 12; revision++)
                Assert.True(backend.StoreKeyValues([MakeItem($"acct/{i:D4}", (long)revision)]));

        KeyValueScanTally oneKey = default;
        Assert.Equal(["acct/0001"], Keys(backend.GetKeyValueByRange("acct/0001", null, null, false, 100, ref oneKey)));
        Assert.True(oneKey.RevisionRowsExamined > 0);

        KeyValueScanTally inclusive = default;
        Assert.Equal(["acct/0001", "acct/0002"], Keys(backend.GetKeyValueByRange("acct", null, "acct/0002", true, 100, ref inclusive)));
        Assert.Equal(2 * oneKey.RevisionRowsExamined, inclusive.RevisionRowsExamined);
        Assert.Equal(2 * oneKey.RevisionRunSeeks, inclusive.RevisionRunSeeks);

        KeyValueScanTally exclusive = default;
        Assert.Equal(["acct/0001", "acct/0002"], Keys(backend.GetKeyValueByRange("acct", null, "acct/0003", false, 100, ref exclusive)));
        Assert.Equal(2 * oneKey.RevisionRowsExamined, exclusive.RevisionRowsExamined);

        KeyValueScanTally unbounded = default;
        Assert.Equal(6, backend.GetKeyValueByRange("acct", null, 100, ref unbounded).Count);
        Assert.Equal(6 * oneKey.RevisionRowsExamined, unbounded.RevisionRowsExamined);
    }

    /// <summary>
    /// A '~' extension of an inclusive bound key shares the bound key's row prefix: it must stay out of
    /// the page while the bound key itself, whose rows sort after the extension's, is still returned.
    /// </summary>
    [Fact]
    public void TestRocksDbGetByRangeInclusiveEndKeyKeepsTheBoundKeyAndNotItsTildeExtension()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        Assert.True(backend.StoreKeyValues([MakeItem("acct/0001", 1), MakeItem("acct/0002", 1), MakeItem("acct/0002~05x", 1), MakeItem("acct/0003", 1)]));

        Assert.Equal(["acct/0001", "acct/0002"], Keys(Bounded(backend, "acct", null, "acct/0002", true, 100)));
        Assert.Equal(["acct/0001"], Keys(Bounded(backend, "acct", null, "acct/0002", false, 100)));
        Assert.Equal(["acct/0001", "acct/0002", "acct/0002~05x"], Keys(Bounded(backend, "acct", null, "acct/0002~05x", true, 100)));
    }

    /// <summary>
    /// A key that is a proper prefix of the end key ("bucket" against "bucket/") sorts below the bound, but
    /// its RocksDB rows sort past the bound position because '/' sorts below the '~' row marker. The bounded
    /// walk stops before it reaches them, so the backend must look such a head up directly.
    /// </summary>
    [Fact]
    public void TestRocksDbGetByRangeEndKeyKeepsAHeadThatIsAProperPrefixOfTheBound()
    {
        string path = RocksDbTempPath();
        using SolariumPersistenceBackend backend = new(path, "v1");

        Assert.True(backend.StoreKeyValues([MakeItem("bucket", 1), MakeItem("bucket/x", 1), MakeItem("bucket/y", 1), MakeItem("c", 1)]));

        Assert.Equal(["bucket"], Keys(Bounded(backend, "bucket", "bucket", "bucket/", false, 100)));
        Assert.Equal(["bucket", "bucket/x"], Keys(Bounded(backend, "bucket", null, "bucket/x", true, 100)));
        Assert.Equal(["bucket", "bucket/x"], Keys(Bounded(backend, "bucket", null, "bucket/y", false, 100)));
        Assert.Equal(["bucket"], Keys(Bounded(backend, "bucket", null, "bucket/y", false, 1)));
        Assert.Equal(["bucket", "bucket/x", "bucket/y"], Keys(Bounded(backend, "bucket", null, "bucket/z", false, 100)));

        // A seek past the prefix key never brings it back.
        Assert.Equal(["bucket/x"], Keys(Bounded(backend, "bucket", "bucket/", "bucket/y", false, 100)));
    }
}
