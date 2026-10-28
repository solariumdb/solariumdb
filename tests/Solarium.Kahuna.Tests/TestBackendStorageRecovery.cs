using System.Text;
using Kahuna;
using Solarium.Tests;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// Solarium refuses every write after a failed WAL write until the database is opened again, as RocksDB
/// latches a background error. The backend's recovery closes and reopens the store in place, behind a
/// swap fence: data survives the swap, new writes land after it, and concurrent readers are fenced
/// rather than crashed. Ported from Kahuna's backend recovery tests; the full-disk test runs only on a
/// small filesystem an operator provides, and an injected write failure covers the same latch here.
/// </summary>
public sealed class TestBackendStorageRecovery
{
    private static PersistenceRequestItem MakeItem(string key, long revision, byte[]? value = null) =>
        new(key,
            value ?? Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    /// <summary>
    /// The close-and-reopen swap itself: data stored before the swap survives it, new writes land
    /// after it, and readers that race the swap block on the fence and then answer correctly —
    /// they never observe a disposed native handle.
    /// </summary>
    [Fact]
    public async Task SolariumBackend_ReopensInPlace_PreservingDataAndFencingConcurrentReaders()
    {
        string dir = CreateTempDir("kahuna-rocksdb-recovery-");

        try
        {
            using SolariumPersistenceBackend backend = new(dir, "v1");

            backend.StoreKeyValues([.. Enumerable.Range(0, 64).Select(i => MakeItem($"swap/k{i:D3}", revision: 1))]);
            Assert.True(backend.StoreDurabilityFloors([(0, 41L)]));
            Assert.NotNull(backend.GetKeyValue("swap/k000"));

            using CancellationTokenSource readersDone = new();
            List<Exception> readerFailures = [];

            // Dedicated threads, never the pool: each reader is a synchronous loop that runs for the
            // whole test without yielding. Queued through Task.Run on a runner with few cores, four of
            // them occupy every pool thread, and everything pool-driven in the process (timers, actor
            // dispatch, the Raft election of tests running alongside) stalls until the pool grows.
            Task[] readers = [.. Enumerable.Range(0, 4).Select(_ => Task.Factory.StartNew(() =>
            {
                try
                {
                    int i = 0;
                    while (!readersDone.IsCancellationRequested)
                    {
                        ReadOnlyKeyValueEntry? entry = backend.GetKeyValue($"swap/k{i++ % 64:D3}");
                        if (entry is null)
                            throw new InvalidOperationException("a pre-swap key vanished during the swap");
                    }
                }
                catch (Exception ex)
                {
                    lock (readerFailures)
                        readerFailures.Add(ex);
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default))];

            // Several swaps under concurrent point reads: each one closes the native handle and
            // reopens it while the readers keep hammering the fence.
            for (int swap = 0; swap < 3; swap++)
            {
                Assert.True(backend.TryRecoverFromStorageFailure());
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }

            readersDone.Cancel();
            await Task.WhenAll(readers);

            Assert.Empty(readerFailures);

            // The reopened instance serves reads, revisions, floors — and accepts new writes.
            Assert.NotNull(backend.GetKeyValue("swap/k063"));
            Assert.NotNull(backend.GetKeyValueRevision("swap/k000", 1));
            Assert.Equal(41L, backend.GetDurabilityFloor(0));

            Assert.True(backend.StoreKeyValues([MakeItem("swap/after", revision: 7)]));
            ReadOnlyKeyValueEntry? after = backend.GetKeyValue("swap/after");
            Assert.NotNull(after);
            Assert.Equal(7, after.Revision);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    /// <summary>
    /// The genuine latch, on a genuinely full filesystem. RocksDB caches the background error
    /// inside the native engine, so no managed double can reproduce it — this test needs a small
    /// dedicated filesystem (~256 MB; a loopback mount on Linux, a small disk image elsewhere)
    /// whose directory is supplied via <c>KAHUNA_ENOSPC_FIXTURE_PATH</c>. It fills the volume,
    /// drives the store into the latched failure, frees the space, proves the latch persists,
    /// and then proves the in-place recovery clears it without a restart.
    /// </summary>
    [Fact]
    public void SolariumBackend_LatchedEnospcError_ClearsOnlyThroughRecovery()
    {
        string? fixture = Environment.GetEnvironmentVariable("KAHUNA_ENOSPC_FIXTURE_PATH");
        Assert.SkipWhen(string.IsNullOrEmpty(fixture),
            "Set KAHUNA_ENOSPC_FIXTURE_PATH to a directory on a small (~256 MB) dedicated filesystem to run this test.");

        string dir = Path.Combine(fixture!, "kahuna-enospc-" + Guid.NewGuid().ToString("N"));
        string fillerPath = Path.Combine(fixture!, "kahuna-enospc-filler-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);

        try
        {
            using SolariumPersistenceBackend backend = new(dir, "v1");

            Assert.True(backend.StoreKeyValues([MakeItem("enospc/seed", revision: 1)]));

            // Fill the volume: one filler file grown until the filesystem refuses the write.
            byte[] chunk = new byte[1024 * 1024];
            Random.Shared.NextBytes(chunk);

            using (FileStream filler = new(fillerPath, FileMode.CreateNew, FileAccess.Write))
            {
                try
                {
                    while (true)
                    {
                        filler.Write(chunk);
                        filler.Flush(flushToDisk: true);
                    }
                }
                catch (IOException)
                {
                    // The volume is full.
                }
            }

            // Drive the store into the failure: with the volume full, a synced write must fail
            // and the engine latches the background error.
            byte[] payload = new byte[256 * 1024];
            Random.Shared.NextBytes(payload);

            bool sawFailure = false;
            for (int i = 0; i < 64 && !sawFailure; i++)
                sawFailure = !backend.StoreKeyValues([MakeItem($"enospc/fill{i}", revision: 1, payload)]);

            Assert.True(sawFailure, "the store never failed on a full volume; the fixture filesystem may be too large");

            // Free the space. The latch is the defect: the engine still fails every write,
            // because it returns the cached error without new I/O.
            File.Delete(fillerPath);

            Assert.False(backend.StoreKeyValues([MakeItem("enospc/latched", revision: 1)]),
                "the store succeeded right after the space was freed; the latched-error premise did not hold");

            // In-place recovery: close and reopen, then writes flow again and old data is intact.
            Assert.True(backend.TryRecoverFromStorageFailure());
            Assert.True(backend.StoreKeyValues([MakeItem("enospc/after", revision: 2)]));
            Assert.NotNull(backend.GetKeyValue("enospc/seed"));
            Assert.NotNull(backend.GetKeyValue("enospc/after"));
        }
        finally
        {
            try { if (File.Exists(fillerPath)) File.Delete(fillerPath); } catch { /* best-effort */ }
            TryDeleteDir(dir);
        }
    }

    /// <summary>
    /// The latch, with the failure injected: a write that fails at the storage layer stops every later
    /// write, even once the storage works again, and only the in-place recovery clears it. Reads go on
    /// throughout, and every write acknowledged before the failure survives the reopen.
    /// </summary>
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void SolariumBackend_FailedWrite_LatchesUntilRecovery(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        HookedEnvironment hooks = new(scope.Environment);
        using SolariumPersistenceBackend backend = new(directory.Path, "v1", environment: hooks);

        Assert.True(backend.StoreKeyValues([MakeItem("latch/seed", revision: 1)]));

        hooks.FailWrites = true;
        Assert.False(backend.StoreKeyValues([MakeItem("latch/failed", revision: 1)]));

        // The storage works again, but the database still refuses writes: the WAL's tail is unknown.
        hooks.FailWrites = false;
        Assert.False(backend.StoreKeyValues([MakeItem("latch/still-refused", revision: 1)]));
        Assert.NotNull(backend.GetKeyValue("latch/seed"));

        Assert.True(backend.TryRecoverFromStorageFailure());

        Assert.True(backend.StoreKeyValues([MakeItem("latch/after", revision: 2)]));
        Assert.Equal(2, backend.GetKeyValue("latch/after")!.Revision);
        Assert.NotNull(backend.GetKeyValue("latch/seed"));
        Assert.Null(backend.GetKeyValue("latch/still-refused"));
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}
