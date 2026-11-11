using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The revision prune writes the deletes of many keys in one shared, unsynced batch, with the pruned-history
/// floor those deletes need. A batch that fails at the storage layer must leave the store, the floor and the
/// memos as they were, so that the keys walk again: the next pass after the recovery deletes every row the
/// failed pass meant to, and the result survives a reopen.
/// </summary>
public sealed class TestPruneWriteFailure
{
    private static readonly TimeSpan Age = TimeSpan.FromHours(1);

    // Two rows to release for each key: more than one prune batch of 2,048 rows.
    private const int Keys = 1_500;

    private static long HoursAgo(double hours) => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)TimeSpan.FromHours(hours).TotalMilliseconds;

    private static string Key(int i) => $"dropped/{i:D6}";

    private static PersistenceRequestItem Value(string key, long revision, long physical) =>
        new(key, [1, 2, 3], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set);

    private static PersistenceRequestItem DeleteRecord(string key, long revision, long physical) =>
        new(key, null, revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Deleted);

    private static void AssertOnlyTheDeleteRecordIsLeft(SolariumPersistenceBackend backend, string key)
    {
        ReadOnlyKeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(KeyValueState.Deleted, current.State);
        Assert.Equal(1, current.Revision);
        Assert.Null(backend.GetKeyValueRevision(key, 0));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_failed_prune_write_deletes_nothing_and_the_pass_after_recovery_deletes_it_all(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        HookedEnvironment hooks = new(scope.Environment);

        long valueAt = HoursAgo(4);
        long deletedAt = HoursAgo(2);

        using (SolariumPersistenceBackend backend = new(directory.Path, "v1", environment: hooks))
        {
            List<PersistenceRequestItem> rows = new(Keys * 2);
            for (int i = 0; i < Keys; i++)
            {
                rows.Add(Value(Key(i), 0, valueAt));
                rows.Add(DeleteRecord(Key(i), 1, deletedAt));
            }

            Assert.True(backend.StoreKeyValues(rows));
            HLCTimestamp floorBefore = backend.GetPrunedHistoryFloor();

            // The first batch of the sweep fails: nothing of it lands, and the floor stays where it was.
            hooks.FailWrites = true;
            Assert.ThrowsAny<SolariumException>(() =>
                backend.PruneKeyValueRevisions(null, 0, Age, 10_000, HLCTimestamp.Zero, Timeout.InfiniteTimeSpan, out _));
            hooks.FailWrites = false;

            Assert.Equal(floorBefore, backend.GetPrunedHistoryFloor());
            for (int i = 0; i < Keys; i++)
            {
                Assert.NotNull(backend.GetKeyValueRevision(Key(i), 0));
                Assert.NotNull(backend.GetKeyValueRevision(Key(i), 1));
            }

            Assert.True(backend.TryRecoverFromStorageFailure());

            long batchesBefore = backend.PruneWriteBatches;
            int deleted = 0;
            int released = 0;
            int passes = 0;
            RevisionPruneResult result;
            do
            {
                Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 10_000, HLCTimestamp.Zero, Timeout.InfiniteTimeSpan, out result));
                deleted += result.RevisionsDeleted;
                released += result.DeleteRecordsReleased;
                Assert.True(++passes < 10, "the sweep took too many passes");
            }
            while (result.BatchLimitReached);

            Assert.Equal(2 * Keys, deleted);
            Assert.Equal(Keys, released);
            Assert.InRange(backend.PruneWriteBatches - batchesBefore, 2, 4);
            Assert.Equal(new HLCTimestamp(0, deletedAt, 0), backend.GetPrunedHistoryFloor());
        }

        // The prune's batches are not synced, but a clean close keeps them, and the floor with them.
        using SolariumPersistenceBackend reopened = new(directory.Path, "v1", environment: scope.Environment);
        for (int i = 0; i < Keys; i++)
            AssertOnlyTheDeleteRecordIsLeft(reopened, Key(i));

        Assert.Equal(new HLCTimestamp(0, deletedAt, 0), reopened.GetPrunedHistoryFloor());
    }
}
