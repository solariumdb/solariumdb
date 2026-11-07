using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// <see cref="WriteBatch.Sequence"/> and <see cref="WriteBatch.GroupLastSequence"/>: the sequence number the
/// database gave a batch's first operation, and the last number of the write group that carried the batch. A
/// caller that syncs a write records what the write made durable, so the first must be exactly the batch's
/// own number, whichever group the batch joined and whoever led it, and the second must be the last number of
/// a group that was flushed whole: the same for every writer of the group, and never past the group.
/// </summary>
public sealed class WriteBatchSequenceTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void Each_written_batch_holds_the_sequence_numbers_its_operations_were_given(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { EnablePipelinedWrite = pipelined });

        WriteBatch first = new WriteBatch().Put(B("a"), B("1")).Put(B("b"), B("2"));
        Assert.Equal(0, first.Sequence);
        Assert.Equal(0, first.GroupLastSequence);
        db.Write(first, WriteOptions.Synced);
        Assert.Equal(1, first.Sequence);

        // Alone in its group, the batch's last number is the group's.
        Assert.Equal(2, first.GroupLastSequence);

        WriteBatch second = new WriteBatch().Delete(B("a"));
        db.Write(second);
        Assert.Equal(3, second.Sequence);
        Assert.Equal(3, second.GroupLastSequence);
        Assert.Equal(3, db.LatestSequenceNumber);

        // Written again, a batch takes new numbers.
        db.Write(first);
        Assert.Equal(4, first.Sequence);
        Assert.Equal(5, first.GroupLastSequence);
        Assert.Equal(5, db.LatestSequenceNumber);
    }

    // Writers that join one group get consecutive ranges: together they cover every number once, and the
    // batches that name the same group's last number are exactly the ones whose ranges run up to it.
    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void Concurrent_batches_hold_disjoint_ranges_that_cover_every_sequence_number(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { EnablePipelinedWrite = pipelined });

        const int Threads = 8;
        const int Writes = 500;
        List<(long First, int Count, long GroupLast)>[] ranges = new List<(long, int, long)>[Threads];
        Parallel.For(0, Threads, new ParallelOptions { MaxDegreeOfParallelism = Threads }, t =>
        {
            ranges[t] = new List<(long, int, long)>(Writes);
            for (int i = 0; i < Writes; i++)
            {
                int count = 1 + ((t + i) % 3);
                WriteBatch batch = new();
                for (int op = 0; op < count; op++)
                    batch.Put(B($"t{t}-{i}-{op}"), B($"{op}"));

                db.Write(batch, i % 7 == 0 ? WriteOptions.Synced : WriteOptions.Default);

                // The group is published before any of its writers returns.
                Assert.True(batch.GroupLastSequence <= db.LatestSequenceNumber);
                ranges[t].Add((batch.Sequence, count, batch.GroupLastSequence));
            }
        });

        List<(long First, int Count, long GroupLast)> all = [.. ranges.SelectMany(r => r).OrderBy(r => r.First)];
        long next = 1;
        foreach ((long first, int count, long groupLast) in all)
        {
            Assert.Equal(next, first);
            Assert.True(groupLast >= first + count - 1);
            next += count;
        }

        Assert.Equal(db.LatestSequenceNumber, next - 1);

        // Each group's last number is the last number of the batch that ends the group, and its batches
        // are the contiguous run before it.
        foreach (IGrouping<long, (long First, int Count, long GroupLast)> group in all.GroupBy(r => r.GroupLast))
        {
            List<(long First, int Count, long GroupLast)> members = [.. group.OrderBy(r => r.First)];
            Assert.Equal(group.Key, members[^1].First + members[^1].Count - 1);
            for (int i = 1; i < members.Count; i++)
                Assert.Equal(members[i - 1].First + members[i - 1].Count, members[i].First);
        }
    }

    // The leader's write is appended and its flush is held; the followers queue behind it and form one
    // group, whose last number every one of them reports, synced or not, while the leader, alone in its
    // group, reports its own last.
    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public async Task The_writers_of_one_group_hold_the_groups_last_sequence_number(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment);
        const int followerCount = 8;
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { Environment = hooks, EnablePipelinedWrite = pipelined });
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();

        hooks.FlushGate.Reset();
        hooks.FlushStarted.Reset();
        WriteBatch leaderBatch = new WriteBatch().Put(B("leader-a"), B("1")).Put(B("leader-b"), B("2"));
        Task leader = Task.Factory.StartNew(() => db.Write(leaderBatch, WriteOptions.Synced), TaskCreationOptions.LongRunning);
        Assert.True(hooks.FlushStarted.Wait(TimeSpan.FromSeconds(10)));

        WriteBatch[] batches = new WriteBatch[followerCount];
        Task[] followers = new Task[followerCount];
        for (int i = 0; i < followerCount; i++)
        {
            WriteBatch batch = new();
            for (int op = 0; op <= i % 3; op++)
                batch.Put(B($"f{i}-{op}"), B("1"));

            WriteOptions options = i % 2 == 0 ? WriteOptions.Synced : WriteOptions.Default;
            batches[i] = batch;
            followers[i] = Task.Factory.StartNew(() => db.Write(batch, options), TaskCreationOptions.LongRunning);
        }

        Assert.True(SpinWait.SpinUntil(() => db.QueuedWriterCount == followerCount, TimeSpan.FromSeconds(10)));
        hooks.FlushGate.Set();
        await leader;
        await Task.WhenAll(followers);

        Assert.Equal(1, leaderBatch.Sequence);
        Assert.Equal(2, leaderBatch.GroupLastSequence);

        long last = db.LatestSequenceNumber;
        Assert.Equal(2 + batches.Sum(b => b.Count), last);
        foreach (WriteBatch batch in batches)
        {
            Assert.InRange(batch.Sequence, 3, last);
            Assert.Equal(last, batch.GroupLastSequence);
        }
    }
}
