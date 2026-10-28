
using System.Text;
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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// A full backup must fail closed when the revision history needed to reconstruct its cut has already
/// been pruned by retention before the backup began — a live snapshot hold only fences pruning from
/// that point on, it cannot restore an already-deleted boundary. Each pruning backend records a
/// durable pruned-history floor (the highest oldest-surviving boundary a prune produced); the driver
/// refuses a cut below it. The memory backend never prunes, so its floor stays Zero.
/// </summary>
public sealed class TestPitrPrunedHistoryFloor : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_prunedfloor_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static PersistenceRequestItem ItemAt(string key, long revision, HLCTimestamp hlc) =>
        new(key, Encoding.UTF8.GetBytes("v" + revision), revision,
            0, 0, 0, 0, 0, 0,
            hlc.N, hlc.L, (uint)hlc.C, (int)KeyValueState.Set);

    private static PersistenceRequestItem ItemAt(string key, long revision, HLCTimestamp hlc, bool noRevision) =>
        new(key, Encoding.UTF8.GetBytes("v" + revision), revision,
            0, 0, 0, 0, 0, 0,
            hlc.N, hlc.L, (uint)hlc.C, (int)KeyValueState.Set, noRevision: noRevision);

    private static HLCTimestamp TS(long ms) => new(0, ms, 0);

    private (IPersistenceBackend Backend, string Dir) CreateBackend(string storage, string suffix)
    {
        string dir = Path.Combine(_tempRoot, storage + "_" + suffix + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);
        IPersistenceBackend backend = storage switch
        {
            "rocksdb" => new SolariumPersistenceBackend(dir, "pf"),
            _         => throw new ArgumentException($"unknown storage '{storage}'", nameof(storage))
        };
        return (backend, dir);
    }

    private static IPersistenceBackend Reopen(string storage, string dir) => storage switch
    {
        "rocksdb" => new SolariumPersistenceBackend(dir, "pf"),
        _         => throw new ArgumentException($"unknown storage '{storage}'", nameof(storage))
    };

    // ── durable backends: prune advances the floor and it survives reopen ──────────────────────

    [Theory]
    [InlineData("rocksdb")]
    public void Prune_AdvancesFloorToOldestSurviving_AndPersistsAcrossReopen(string storage)
    {
        const string key = "acct/pruned";
        (IPersistenceBackend backend, string dir) = CreateBackend(storage, "advance");
        try
        {
            // Revisions at T1..T5 (physical 1000..5000); rev 5 is current.
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);

            Assert.Equal(HLCTimestamp.Zero, backend.GetPrunedHistoryFloor());

            // retentionCount=1 keeps only the current revision (rev 5 @ 5000); rev 1–4 are pruned.
            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero,
                out RevisionPruneResult result));
            Assert.True(result.RevisionsDeleted > 0);

            // The oldest surviving boundary is rev 5 @ 5000 — cuts below it can no longer be reconstructed.
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), backend.GetPrunedHistoryFloor());
        }
        finally { (backend as IDisposable)?.Dispose(); }

        // Reopen at the same path: the floor is durable (the deleted history did not come back).
        IPersistenceBackend reopened = Reopen(storage, dir);
        try
        {
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), reopened.GetPrunedHistoryFloor());
        }
        finally { (reopened as IDisposable)?.Dispose(); }
    }

    // ── durable, crash-coupled watermark: stored in the DB, fails closed when unreadable ─────────

    [Theory]
    [InlineData("rocksdb")]
    public void Prune_StoresFloorInDb_NoSidecarFile(string storage)
    {
        const string key = "acct/indb";
        (IPersistenceBackend backend, string dir) = CreateBackend(storage, "indb");
        try
        {
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);
            backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, 1000, HLCTimestamp.Zero, out _);
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), backend.GetPrunedHistoryFloor());
        }
        finally { (backend as IDisposable)?.Dispose(); }

        // No sidecar floor file is used any more — the floor lives in the backend's own storage.
        Assert.Empty(Directory.GetFiles(dir, "pruned_history_floor*", SearchOption.AllDirectories));

        // Durable across reopen (from the DB, not a sidecar).
        IPersistenceBackend reopened = Reopen(storage, dir);
        try { Assert.Equal(new HLCTimestamp(0, 5000L, 0), reopened.GetPrunedHistoryFloor()); }
        finally { (reopened as IDisposable)?.Dispose(); }
    }

    [Fact]
    public void RocksDb_LegacySpacePrefixedFloor_IsAdoptedOnReopen_AndMigrated()
    {
        const string key = "acct/legacyfloor";
        string dir = Path.Combine(_tempRoot, "rocks_legacy_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);

        // A store with data but no prune under the current build: no floor under the current key.
        using (SolariumPersistenceBackend backend = new(dir, "pf"))
        {
            backend.StoreKeyValues([ItemAt(key, 1, TS(1000))]);
            Assert.Equal(HLCTimestamp.Zero, ((IPersistenceBackend)backend).GetPrunedHistoryFloor());
        }

        // Simulate a store that pruned under a pre-rename build: its floor sits under the legacy
        // space-prefixed key only. The payload layout matches the backend's packed floor: N, L, C
        // as little-endian Int64s.
        byte[] legacyKey = " pitr_pruned_history_floor"u8.ToArray();
        byte[] packed = new byte[24];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(packed.AsSpan(0, 8), 0);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(packed.AsSpan(8, 8), 5000L);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(packed.AsSpan(16, 8), 0);
        byte[] currentKey = new byte[] { 0 }
            .Concat(System.Text.Encoding.UTF8.GetBytes("pitr_pruned_history_floor")).ToArray();
        DbOptions existing = new() { CreateIfMissing = false };
        using (SolariumDb raw = SolariumDb.Open(Path.Combine(dir, "pf"), existing, ["kv", "locks"]))
        {
            raw.Put(legacyKey, packed, raw.GetColumnFamily("kv"));
            raw.Delete(currentKey, raw.GetColumnFamily("kv"));
        }

        // Reopen with the current build: the legacy floor must be adopted, not read as "never
        // pruned" — a zero floor would let a backup trust already-deleted history.
        using (SolariumPersistenceBackend reopened = new(dir, "pf"))
        {
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), ((IPersistenceBackend)reopened).GetPrunedHistoryFloor());
        }

        // The read migrated the value to the current key, and the legacy key stays for rollback.
        using (SolariumDb raw = SolariumDb.Open(Path.Combine(dir, "pf"), existing, ["kv", "locks"]))
        {
            Assert.Equal(packed, raw.Get(currentKey, raw.GetColumnFamily("kv")));
            Assert.Equal(packed, raw.Get(legacyKey, raw.GetColumnFamily("kv")));
        }
    }

}
