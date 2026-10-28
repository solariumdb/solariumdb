using System.Diagnostics;
using Kommander;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Storage;

namespace Solarium.Kommander.Tests;

/// <summary>Opens the WALs the tests use, and waits for the engine's background work.</summary>
internal static class Wals
{
    /// <summary>
    /// Opens <see cref="SolariumWAL"/> in <paramref name="path"/>/wal on <paramref name="environment"/>,
    /// with <paramref name="options"/> (null: the defaults).
    /// </summary>
    public static SolariumWAL Open(IStorageEnvironment environment, string path, bool syncWrites = false, SolariumWalOptions? options = null) =>
        new(path, "wal", NullLogger<IRaft>.Instance, syncWrites, (options ?? SolariumWalOptions.Default) with { Environment = environment });

    /// <summary>The flushes, moves and merges of the shard that holds <paramref name="partitionId"/>, since the WAL opened.</summary>
    public static (long Flushes, long Moves, long Merges) ShardCounts(SolariumWAL wal, int partitionId)
    {
        SolariumDb db = wal.DatabaseForTesting;
        return db.FamilyCounts(db.GetColumnFamily("shard" + (partitionId % 8)));
    }

    /// <summary>
    /// Reads <paramref name="read"/> until <paramref name="ready"/> accepts the value or
    /// <paramref name="timeout"/> passes, and returns the last value; fails the test on a timeout
    /// unless <paramref name="failOnTimeout"/> is false.
    /// </summary>
    public static T WaitUntil<T>(Func<T> read, Func<T, bool> ready, TimeSpan timeout, bool failOnTimeout = true)
    {
        Stopwatch clock = Stopwatch.StartNew();
        T value = read();
        while (!ready(value))
        {
            if (clock.Elapsed >= timeout)
            {
                Assert.False(failOnTimeout, $"The condition was not met within {timeout}.");
                return value;
            }

            Thread.Sleep(50);
            value = read();
        }

        return value;
    }
}
