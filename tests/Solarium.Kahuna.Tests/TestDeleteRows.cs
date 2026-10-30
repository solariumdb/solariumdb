using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// <c>DeleteKeyValues</c> and <c>DeleteLocks</c> physically remove every row of a key: its current row, its
/// history and its provenance marker. A whole-partition purge before a snapshot install calls them with pages of
/// keys that can hold millions of history rows, so the write they make must not grow with the history: one
/// range tombstone per key, over exactly the key's rows, never a neighbour's.
/// </summary>
public sealed class TestDeleteRows : IDisposable
{
    private const int Keys = 256;
    private const int Revisions = 1000;

    private readonly TempDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    // Keys that sort right beside a purged key's rows: "deep/007~..." lies between them.
    private static readonly string[] Neighbours = ["deep/0070", "deep/007}", "deep/007\u007f", "deep/007é"];

    private static PersistenceRequestItem Item(string key, long revision) =>
        new(key, Encoding.UTF8.GetBytes($"{key}@{revision}"), revision, 0, 0, 0, 0, 0, 0, 0, revision, 0, (int)KeyValueState.Set);

    private static PersistenceRequestItem LockItem(string resource) =>
        new(resource, Encoding.UTF8.GetBytes("owner"), 1, 0, 1000, 0, 0, 0, 0, 0, 1, 0, (int)LockState.Locked);

    private static string KeyName(int i) => $"deep/{i:D3}";

    private static string ValueOf(ReadOnlyKeyValueEntry? entry) => Encoding.UTF8.GetString(entry!.Value!);

    private SolariumPersistenceBackend Open(EnvironmentScope scope) =>
        new(_directory.Combine("store"), "v1", NullLogger.Instance, scope.Environment);

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_purge_of_deep_histories_writes_one_tombstone_per_key_and_removes_every_row(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        List<string> keys = [.. Enumerable.Range(0, Keys).Select(KeyName)];

        using (SolariumPersistenceBackend store = Open(scope))
        {
            for (int revision = 1; revision <= Revisions; revision++)
            {
                Assert.True(store.StoreKeyValues([.. keys.Select(key => Item(key, revision))]));

                // Half of the history in tables and half in the memtable, so the tombstones cover both.
                if (revision == Revisions / 2)
                    store.FlushAndMeasureTableMemoryForTesting();
            }

            Assert.True(store.StoreKeyValues([.. Neighbours.Select(key => Item(key, 1))]));
            Assert.Equal("deep/007@500", ValueOf(store.GetKeyValueRevision("deep/007", 500)));

            // A delete per row would copy each of the 256,000 history keys out of an iterator into the
            // batch and its WAL record: tens of megabytes. One range tombstone per key is a few kilobytes.
            long allocated = GC.GetAllocatedBytesForCurrentThread();
            Assert.True(store.DeleteKeyValues(keys));
            allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;
            Assert.InRange(allocated, 0, 1024 * 1024);

            AssertPurged(store);
        }

        using (SolariumPersistenceBackend store = Open(scope))
        {
            AssertPurged(store);

            // The keys come back from revision 1, above the tombstones that hid their old rows.
            Assert.True(store.StoreKeyValues([.. keys.Select(key => Item(key, 1))]));
            Assert.Equal("deep/255@1", ValueOf(store.GetKeyValue("deep/255")));
        }

        using (SolariumPersistenceBackend store = Open(scope))
        {
            foreach (string key in keys)
                Assert.Equal($"{key}@1", ValueOf(store.GetKeyValue(key)));
            Assert.Null(store.GetKeyValueRevision("deep/007", 2));
            Assert.Equal("deep/007@1", ValueOf(store.GetKeyValueRevision("deep/007", 1)));
        }
    }

    private static void AssertPurged(SolariumPersistenceBackend store)
    {
        for (int i = 0; i < Keys; i++)
        {
            string key = KeyName(i);
            Assert.Null(store.GetKeyValue(key));
            Assert.Null(store.GetKeyValueRevision(key, 1));
            Assert.Null(store.GetKeyValueRevision(key, Revisions / 2));
            Assert.Null(store.GetKeyValueRevision(key, Revisions));
        }

        foreach (string neighbour in Neighbours)
            Assert.Equal($"{neighbour}@1", ValueOf(store.GetKeyValue(neighbour)));

        List<string> scanned = [];
        string? cursor = null;
        do
        {
            KeyValueScanPage page = store.ScanKeyValues(cursor, 100);
            scanned.AddRange(page.Items.Select(static item => item.Item1));
            cursor = page.NextCursor;
        }
        while (cursor is not null);

        Assert.Equal(Neighbours.Order(StringComparer.Ordinal), scanned.Order(StringComparer.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Deleting_locks_removes_the_named_resources_and_keeps_their_neighbours(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string[] deleted = ["locks/a", "locks/b"];
        string[] kept = ["locks/a0", "locks/a}", "locks/a\u007f", "locks/c"];

        using (SolariumPersistenceBackend store = Open(scope))
        {
            Assert.True(store.StoreLocks([.. deleted.Concat(kept).Select(LockItem)]));
            Assert.True(store.DeleteLocks(deleted));
        }

        using (SolariumPersistenceBackend store = Open(scope))
        {
            foreach (string resource in deleted)
                Assert.Null(store.GetLock(resource));
            foreach (string resource in kept)
                Assert.NotNull(store.GetLock(resource));
        }
    }

    [Fact]
    public void Deleting_an_empty_list_of_keys_keeps_every_row()
    {
        using EnvironmentScope scope = Environments.Create("default");
        using SolariumPersistenceBackend store = Open(scope);
        Assert.True(store.StoreKeyValues([Item("deep/000", 1)]));

        Assert.True(store.DeleteKeyValues([]));
        Assert.Equal("deep/000@1", ValueOf(store.GetKeyValue("deep/000")));
    }
}
