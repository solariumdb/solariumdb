using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// <see cref="WriteBatch.Sequence"/>: the sequence number the database gave a batch's first operation. A
/// caller that syncs a write records it as what the write made durable, so it must be exactly the batch's
/// own numbers, whichever group the batch joined and whoever led it.
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
        db.Write(first, WriteOptions.Synced);
        Assert.Equal(1, first.Sequence);

        WriteBatch second = new WriteBatch().Delete(B("a"));
        db.Write(second);
        Assert.Equal(3, second.Sequence);
        Assert.Equal(3, db.LatestSequenceNumber);

        // Written again, a batch takes new numbers.
        db.Write(first);
        Assert.Equal(4, first.Sequence);
        Assert.Equal(5, db.LatestSequenceNumber);
    }

    // Writers that join one group get consecutive ranges: together they cover every number once.
    [Theory]
    [MemberData(nameof(Environments.AvailableWithCommits), MemberType = typeof(Environments))]
    public void Concurrent_batches_hold_disjoint_ranges_that_cover_every_sequence_number(string environment, bool pipelined)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using SolariumDb db = SolariumDb.Open(DbPath, scope.Options() with { EnablePipelinedWrite = pipelined });

        const int Threads = 8;
        const int Writes = 500;
        List<(long First, int Count)>[] ranges = new List<(long, int)>[Threads];
        Parallel.For(0, Threads, new ParallelOptions { MaxDegreeOfParallelism = Threads }, t =>
        {
            ranges[t] = new List<(long, int)>(Writes);
            for (int i = 0; i < Writes; i++)
            {
                int count = 1 + ((t + i) % 3);
                WriteBatch batch = new();
                for (int op = 0; op < count; op++)
                    batch.Put(B($"t{t}-{i}-{op}"), B($"{op}"));

                db.Write(batch, i % 7 == 0 ? WriteOptions.Synced : WriteOptions.Default);
                ranges[t].Add((batch.Sequence, count));
            }
        });

        List<(long First, int Count)> all = [.. ranges.SelectMany(r => r).OrderBy(r => r.First)];
        long next = 1;
        foreach ((long first, int count) in all)
        {
            Assert.Equal(next, first);
            next += count;
        }

        Assert.Equal(db.LatestSequenceNumber, next - 1);
    }
}
