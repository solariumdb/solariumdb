// Ported from Kommander's RocksDbWAL (https://github.com/kahunakv/kommander, Kommander/WAL/RocksDbWAL.cs at
// commit 1bee500), MIT License, Copyright (c) 2025 Andres Gutierrez. The key layout, the row encoding, the
// metadata keys, the commit frontier and the compaction floor are the same; each RocksDB call is replaced by
// its Solarium equivalent.

using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Solarium.Storage;

namespace Solarium.Kommander;

/// <summary>
/// A Kommander write-ahead log (<see cref="IWAL"/>) on Solarium. Pass it to <c>RaftManager</c> where a host
/// passes <c>RocksDbWAL</c>; it keeps the same layout: the column families <c>default</c>, <c>metadata</c> and
/// <c>shard0</c> to <c>shard7</c>, partition <c>p</c> in <c>shard{p % 8}</c>, one row per log entry under the
/// key <c>{partition:D10}:{id:D20}</c> holding the <c>RaftLogMessage</c> protobuf encoding, and per-partition
/// metadata keys for the last checkpoint, the commit frontier and the compaction floor.
/// </summary>
/// <remarks>
/// <para>Every member is safe to call from any thread. Appends hold the shared side of a write guard and so
/// overlap, which lets Solarium's group commit join their WAL flushes; the operations that scan and then
/// delete (truncations, the snapshot boundary) and the appends that carry a checkpoint hold its exclusive
/// side.</para>
/// <para>A storage failure stops all writes of a Solarium database until it is opened again. After a failed
/// write, the WAL checks that the file system accepts a small durable write and that the database refuses
/// one, then closes the database and opens it again, and the write is retried once; every write path is an
/// idempotent upsert or delete.</para>
/// </remarks>
public sealed class SolariumWAL : IWAL
{
    private const string FormatVersion = "2.0.0";
    private const int MaxShards = 8;
    private const int IdWidth = 20;
    private const int PartitionIdWidth = 10;
    private const int LogKeyWidth = PartitionIdWidth + 1 + IdWidth;
    private const int PartitionPrefixWidth = PartitionIdWidth + 1;
    private const byte LogKeySeparator = (byte)':';
    private const byte PartitionUpperBoundSeparator = (byte)';';   // sorts after ':', before the next partition
    private const int StackallocThreshold = 256;

    // A compaction tombstone confined to the active memtable starts this many ids above the span that
    // num-entries-active-mem-table implies, which counts tombstones and deletes too, so it never covers a
    // row of an already flushed file; that would make the next file overlap it and cost a merge.
    private const long ConfinedTombstoneMargin = 64;

    // The gap the commit frontier may verify row by row when a commit lands above F + 1. A larger gap
    // writes the full row, so the probes of one marker stay bounded.
    private const int FrontierCatchUpBound = 64;

    // Keys per MultiGet when the marker headers of a batch are read together; it bounds the rows one call holds.
    private const int MarkerHeaderReadBatchSize = 128;

    private const int LogTypeCacheSize = 8;
    private const string SpaceProbeFileName = ".kommander-space-probe";
    private const int SpaceProbeBytes = 64 * 1024;

    private static readonly byte[] EngineCanaryKey = "kommander_engine_canary"u8.ToArray();
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SustainedFailureLogInterval = TimeSpan.FromSeconds(30);

    [ThreadStatic]
    private static (byte[] Utf8, string Value)[]? t_logTypeCache;

    [ThreadStatic]
    private static int t_logTypeCacheCount;

    [ThreadStatic]
    private static int t_logTypeCacheCursor;

    private readonly string _enginePath;
    private readonly DbOptions _dbOptions;
    private readonly string[] _familyNames;
    private readonly ILogger<IRaft> _logger;
    private readonly IMonotonicTickSource _tickSource;
    private readonly WriteOptions _writeOptions;
    private readonly long _metadataFlushEveryBytes;

    // Appends hold the shared side; scan-then-delete operations and checkpoint appends the exclusive side.
    private readonly ReaderWriterLockSlim _writeGuard = new(LockRecursionPolicy.NoRecursion);

    // Every operation holds the shared side for its duration; a reopen and Dispose hold the exclusive side,
    // so no read, iterator or write touches the database being replaced.
    private readonly ReaderWriterLockSlim _engineGuard = new(LockRecursionPolicy.NoRecursion);

    private readonly ConcurrentDictionary<int, byte[]> _lastCheckpointKeys = new();
    private readonly ConcurrentDictionary<int, byte[]> _commitFrontierKeys = new();
    private readonly ConcurrentDictionary<int, byte[]> _compactionFloorKeys = new();

    // Raised only after the advancing batch landed, set exactly by the truncations under the exclusive
    // write guard, and cleared by a reopen, which reloads them from the metadata family.
    private readonly ConcurrentDictionary<int, long> _commitFrontierCache = new();
    private readonly ConcurrentDictionary<int, long> _compactionFloorCache = new();

    // One lock for each partition's commit frontier. An advance reads the frontier, probes the rows
    // above it, and puts the new value with an unconditional write; two that overlap under the shared
    // write guard could land in the opposite order and leave the lower value on disk, which reads an
    // absorbed commit as Proposed after a reopen. Held from the read through the write and the cache
    // raise; taken inside the shared guard, in ascending partition order when a batch advances several.
    // The paths that lower the frontier hold the exclusive guard, which no advance can overlap.
    private readonly ConcurrentDictionary<int, object> _frontierLocks = new();

    // The highest physically present id of a partition. A value exists only after a scan under the
    // exclusive guard computed it; appends raise it, truncations set or remove it; absent means unknown.
    private readonly ConcurrentDictionary<int, PartitionMaxLog> _maxLogCache = new();

    private SolariumDb _db;
    private ColumnFamily _metadata;
    private ColumnFamily[] _shards;
    private volatile bool _engineClosed;
    private long _shardBytesSinceMetadataFlush;
    private long _markerHeadersPrefetched;
    private long _commitMarkersAbsorbed;
    private Exception? _lastStorageFailure;
    private long _lastProbeTicks;
    private int _failureStreak;
    private long _failureStreakStartTicks;
    private long _lastFailureLogTicks;

    /// <summary>
    /// Opens the WAL in <paramref name="path"/>/<paramref name="revision"/>, creating it if needed, with
    /// <paramref name="options"/> (null: <see cref="SolariumWalOptions.Default"/>). With
    /// <paramref name="syncWrites"/> false, every write is unsynced, as <c>RocksDbWAL</c>'s.
    /// </summary>
    /// <exception cref="RaftException"><paramref name="revision"/> is not a single file name.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An option is out of range.</exception>
    /// <exception cref="InvalidOperationException">The directory holds a WAL of another format version.</exception>
    public SolariumWAL(string path, string revision, ILogger<IRaft> logger, bool syncWrites = true, SolariumWalOptions? options = null)
    {
        ValidateRevision(revision);
        options ??= SolariumWalOptions.Default;
        if (options.ShardWriteBufferSizeBytes < 1 || options.ShardMaxWriteBufferNumber < 2)
            throw new ArgumentOutOfRangeException(nameof(options), "ShardWriteBufferSizeBytes must be positive and ShardMaxWriteBufferNumber at least 2.");

        if (options.MaxTotalWalSizeFlushUnits < 0)
            throw new ArgumentOutOfRangeException(nameof(options), $"MaxTotalWalSizeFlushUnits ({options.MaxTotalWalSizeFlushUnits}) must be 0 or positive.");

        _logger = logger;
        _tickSource = options.TickSource;
        _writeOptions = syncWrites ? WriteOptions.Synced : WriteOptions.Default;
        _metadataFlushEveryBytes = options.ShardWriteBufferSizeBytes;
        SyncWritesEnabled = syncWrites;

        // With a block cache, every family keeps its index and filter blocks in it, pinned at level 0, as
        // RocksDbWAL's shared path: the readers' memory then counts against the cache, and level 0, read on
        // every lookup, is never evicted. Without one, they stay resident in the readers.
        ColumnFamilyOptions cached = options.BlockCache is null
            ? new ColumnFamilyOptions()
            : new ColumnFamilyOptions { BlockCache = options.BlockCache, CacheIndexAndFilterBlocks = true, PinL0FilterAndIndexBlocksInCache = true };
        ColumnFamilyOptions shard = cached with
        {
            WriteBufferSize = options.ShardWriteBufferSizeBytes,
            MaxWriteBufferNumber = options.ShardMaxWriteBufferNumber,
            Level0FileNumCompactionTrigger = options.ShardLevel0FileNumCompactionTrigger,
            Level0SlowdownWritesTrigger = options.ShardLevel0SlowdownWritesTrigger,
            Level0StopWritesTrigger = options.ShardLevel0StopWritesTrigger,
        };
        Dictionary<string, ColumnFamilyOptions> families = new(StringComparer.Ordinal);
        for (int i = 0; i < MaxShards; i++)
            families["shard" + i] = shard;

        _familyNames = ["default", "metadata", .. families.Keys];
        _dbOptions = new DbOptions
        {
            Environment = options.Environment,
            CreateMissingColumnFamilies = true,
            DefaultColumnFamilyOptions = cached,
            ColumnFamilyOptions = families,
            MaxTotalWalSize = options.MaxTotalWalSizeFlushUnits > 0 ? checked(options.ShardWriteBufferSizeBytes * options.MaxTotalWalSizeFlushUnits) : 0,
            MaxBackgroundCompactions = options.MaxBackgroundCompactions,
            WriteBufferManager = options.WriteBufferManager,
        };

        _enginePath = Path.Combine(path, revision);
        EnsureDirectory(_enginePath);

        _db = SolariumDb.Open(_enginePath, _dbOptions, _familyNames);
        (_metadata, _shards) = Families(_db);

        // A WAL is new when its database holds nothing, not when its directory is missing: a host may
        // create the directory first, and a crash during the first open can lose the unsynced version
        // with the directory already there. The version is written synced, before any row, so a
        // database that holds data without a version has another layout and is refused.
        string? currentVersion = GetMetaData("version");
        if (currentVersion is null && IsEmpty())
            _db.Put(Encoding.UTF8.GetBytes("version"), Encoding.UTF8.GetBytes(FormatVersion), _metadata, WriteOptions.Synced);
        else if (!string.Equals(currentVersion, FormatVersion, StringComparison.Ordinal))
        {
            _db.Dispose();
            throw new InvalidOperationException(
                $"Solarium WAL format version '{currentVersion ?? "<missing>"}' in '{_enginePath}' is not compatible with '{FormatVersion}'.");
        }

        WarnIfSharedBudgetStarvesFlushUnit(options);
        SolariumWalMetrics.Register(this);
    }

    /// <summary>
    /// Logs once, at open, when the shared <see cref="WriteBufferManager"/> cannot hold one shard memtable
    /// plus one more for what shares it, as <c>RocksDbWAL</c> warns. Below that, the WAL trips the cap far
    /// more often than a store beside it (one write per Raft batch) and flushes the Raft log on the cap's
    /// cadence instead of at <see cref="SolariumWalOptions.ShardWriteBufferSizeBytes"/>, so entries that a
    /// compaction pass would delete in memory reach disk once.
    /// </summary>
    private void WarnIfSharedBudgetStarvesFlushUnit(SolariumWalOptions options)
    {
        if (options.WriteBufferManager is not { } manager)
            return;

        long needed = 2 * options.ShardWriteBufferSizeBytes;
        if (manager.BufferSize < needed)
            _logger.LogWarning(
                "Solarium WAL at '{Path}': the shared memtable budget ({BudgetMb} MB) is below the shard flush unit plus headroom ({NeededMb} MB); Solarium will flush the Raft log on the budget's cadence instead of at {FlushUnitMb} MB, so entries that would die in memory reach disk once. Raise the shared memtable budget or lower ShardWriteBufferSizeBytes",
                _enginePath, manager.BufferSize >> 20, needed >> 20, options.ShardWriteBufferSizeBytes >> 20);
    }

    /// <summary>Whether every write is synced, as the constructor's <c>syncWrites</c> chose.</summary>
    internal bool SyncWritesEnabled { get; }

    /// <summary>For tests: the database writes that the last compaction pass made.</summary>
    internal int LastCompactionWriteCount { get; private set; }

    /// <summary>For tests: the <c>Committed</c> markers absorbed into the persisted commit frontier, counted after their batch landed.</summary>
    internal long CommitMarkersAbsorbed => Interlocked.Read(ref _commitMarkersAbsorbed);

    /// <summary>For tests: the marker target headers read through the batched prefetch of the append path.</summary>
    internal long MarkerHeadersPrefetched => Interlocked.Read(ref _markerHeadersPrefetched);

    /// <summary>For tests: the reopens that recovery performed.</summary>
    internal int EngineReopenCount { get; private set; }

    /// <summary>For tests: the message of the most recent write failure, or null.</summary>
    internal string? LastStorageFailureMessage => Volatile.Read(ref _lastStorageFailure)?.Message;

    /// <summary>For tests: runs inside <see cref="InstallSnapshotBoundary"/> after its suffix scan, under the exclusive write guard.</summary>
    internal Action? OnAfterBoundaryScanForTesting { get; set; }

    // ── Reads ────────────────────────────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public List<RaftLog> ReadLogs(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        List<RaftLog> result = [];
        ColumnFamily family = Shard(partitionId);
        long lastCheckpoint = GetLastCheckpointFromMeta(partitionId);
        long commitFrontier = GetCommitFrontier(partitionId);

        // Rows below the compaction floor are logically deleted even while their file is still on disk.
        using Iterator iterator = NewPartitionBoundedIterator(family, partitionId);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, Math.Max(Math.Max(0, lastCheckpoint), GetCompactionFloor(partitionId)));
        iterator.Seek(seekKey);

        Span<byte> prefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(prefix, partitionId);
        for (; iterator.Valid() && iterator.Key().StartsWith(prefix); iterator.Next())
        {
            ReadLogFromWire(iterator.Value(), out RaftLogWireView view);
            if (view.Partition == partitionId && view.Id >= lastCheckpoint)
                result.Add(ToRaftLog(view, commitFrontier));
        }

        return result;
    }

    /// <inheritdoc/>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
    {
        return ReadLogsRange(partitionId, startLogIndex, maxEntries, long.MaxValue);
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> rows from <paramref name="startLogIndex"/>, stopping before
    /// the payloads pass <paramref name="maxBytes"/>; the first row always goes, so an entry larger than the
    /// budget still ships. The budget is checked before a payload is copied.
    /// </summary>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries, long maxBytes)
    {
        using EngineLease lease = AcquireEngine();
        List<RaftLog> result = maxEntries is > 0 and <= 1024 ? new(maxEntries) : [];
        ColumnFamily family = Shard(partitionId);
        long commitFrontier = GetCommitFrontier(partitionId);

        // Clamped at the compaction floor: ids below it read as absent, as when their rows were deleted.
        using Iterator iterator = NewPartitionBoundedIterator(family, partitionId);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, Math.Max(Math.Max(0, startLogIndex), GetCompactionFloor(partitionId)));
        iterator.Seek(seekKey);

        Span<byte> prefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(prefix, partitionId);
        long payloadBytes = 0;
        for (; iterator.Valid() && iterator.Key().StartsWith(prefix); iterator.Next())
        {
            ReadLogFromWire(iterator.Value(), out RaftLogWireView view);
            if (view.Partition != partitionId || view.Id < startLogIndex)
                continue;

            if (result.Count > 0 && payloadBytes + view.Log.Length > maxBytes)
                break;

            payloadBytes += view.Log.Length;
            result.Add(ToRaftLog(view, commitFrontier));
            if (result.Count >= maxEntries)
                break;
        }

        return result;
    }

    /// <inheritdoc/>
    public long GetTermAt(int partitionId, long logIndex)
    {
        using EngineLease lease = AcquireEngine();

        // Below the compaction floor the row is logically gone whether or not its file was dropped yet.
        if (logIndex < GetCompactionFloor(partitionId))
            return -1;

        Span<byte> key = stackalloc byte[LogKeyWidth];
        BuildLogKey(key, partitionId, logIndex);
        byte[]? value = _db.Get(key, Shard(partitionId));
        return value is null ? -1 : ReadTermFromWire(value);
    }

    /// <inheritdoc/>
    public long GetMaxLog(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        long floor = GetCompactionFloor(partitionId);
        if (_maxLogCache.TryGetValue(partitionId, out PartitionMaxLog? cached))
            return ClampMaxLogAtFloor(Volatile.Read(ref cached.Value), floor);

        // A miss scans under the exclusive guard, so no append or truncation lands between the scan and
        // the store; the reverse seek is paid once per invalidation, not once per call.
        _writeGuard.EnterWriteLock();
        try
        {
            if (_maxLogCache.TryGetValue(partitionId, out cached))
                return ClampMaxLogAtFloor(cached.Value, floor);

            long max = ScanMaxLogId(partitionId);
            _maxLogCache[partitionId] = new PartitionMaxLog(max);
            return ClampMaxLogAtFloor(max, floor);
        }
        finally
        {
            _writeGuard.ExitWriteLock();
        }
    }

    /// <inheritdoc/>
    public long GetCurrentTerm(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        using Iterator iterator = _db.NewIterator(Shard(partitionId));
        SeekToLastPartitionKey(iterator, partitionId);
        if (iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId)
            && ParseLogIdFromKey(iterator.Key()) >= GetCompactionFloor(partitionId))
        {
            ReadHeaderFromWire(iterator.Value(), out _, out _, out long term, out _);
            return term;
        }

        return 0;
    }

    /// <inheritdoc/>
    public long GetLastCheckpoint(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        return GetLastCheckpointFromMeta(partitionId);
    }

    /// <inheritdoc/>
    long IWAL.GetCompactionFloor(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        return GetCompactionFloor(partitionId);
    }

    /// <inheritdoc/>
    public int CountPersistedLogs(int partitionId)
    {
        using EngineLease lease = AcquireEngine();

        // A maintenance scan over the whole partition: no cache fill, so a count never evicts the tail's indexes.
        using Iterator iterator = NewPartitionBoundedIterator(Shard(partitionId), partitionId, fillCache: false);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, GetCompactionFloor(partitionId));
        iterator.Seek(seekKey);

        Span<byte> prefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(prefix, partitionId);
        int count = 0;
        for (; iterator.Valid() && iterator.Key().StartsWith(prefix); iterator.Next())
            count++;

        return count;
    }

    /// <inheritdoc/>
    public int CountRemovableLogs(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        long lastCheckpoint = GetLastCheckpointFromMeta(partitionId);
        if (lastCheckpoint <= 0)
            return 0;

        // Bounded at the checkpoint: only the rows below it are removable. No cache fill, as in CountPersistedLogs.
        using Iterator iterator = NewBoundedIterator(Shard(partitionId), partitionId, lastCheckpoint, fillCache: false);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, GetCompactionFloor(partitionId));
        iterator.Seek(seekKey);

        Span<byte> prefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(prefix, partitionId);
        int count = 0;
        for (; iterator.Valid() && iterator.Key().StartsWith(prefix) && ParseLogIdFromKey(iterator.Key()) < lastCheckpoint; iterator.Next())
            count++;

        return count;
    }

    /// <inheritdoc/>
    public string? GetMetaData(string key)
    {
        using EngineLease lease = AcquireEngine();
        byte[]? value = _db.Get(Encoding.UTF8.GetBytes(key), _metadata);
        return value is not null ? Encoding.UTF8.GetString(value) : null;
    }

    // ── Writes ───────────────────────────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

    /// <inheritdoc/>
    /// <remarks>
    /// With <paramref name="sync"/> false the batch is not flushed on its own; the next synced write
    /// flushes the WAL up to and including it. With <c>syncWrites</c> false at construction, every write
    /// is unsynced.
    /// </remarks>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
    {
        RaftOperationStatus status = WriteCore(logs, sync);
        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = WriteCore(logs, sync);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    /// <inheritdoc/>
    public bool SetMetaData(string key, string value)
    {
        bool ok = SetMetaDataCore(key, value);
        if (!ok && TryRecoverEngineAfterFailure())
            ok = SetMetaDataCore(key, value);

        if (ok)
            NoteStorageSuccess();

        return ok;
    }

    /// <inheritdoc/>
    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        RaftOperationStatus status = DeletePartitionWALCore(partitionId);
        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = DeletePartitionWALCore(partitionId);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateLogsAfterCore(partitionId, afterLogId);
        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = TruncateLogsAfterCore(partitionId, afterLogId);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    /// <inheritdoc/>
    public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateProposedLogsAfterCore(partitionId, afterLogId);
        if (status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            status = TruncateProposedLogsAfterCore(partitionId, afterLogId);

        if (status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return status;
    }

    /// <inheritdoc/>
    public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId)
    {
        RaftOperationStatus status = TruncateLogsAfter(partitionId, afterLogId);
        return status != RaftOperationStatus.Success ? (status, 0) : (status, GetMaxLog(partitionId));
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The suffix deletes and the checkpoint row land in one batch, under the exclusive write guard, so an
    /// append cannot land between the scan and the batch and survive the truncation.
    /// </remarks>
    public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        (RaftOperationStatus Status, bool SuffixTruncated) result = InstallSnapshotBoundaryCore(partitionId, snapshotIndex, lastIncludedTerm, sync);
        if (result.Status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            result = InstallSnapshotBoundaryCore(partitionId, snapshotIndex, lastIncludedTerm, sync);

        if (result.Status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return result;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The rows below <paramref name="lastCheckpoint"/> are deleted logically, by one persisted compaction
    /// floor, and physically by whole files: the pass writes the floor with a range delete (confined to the
    /// active memtable when the partition owns its shard, so its flushed files stay apart and move to the
    /// sorted level whole), then drops every table that lies below the floor. The entry cap bounds only the
    /// counting scan; one pass deletes everything below the checkpoint.
    /// </remarks>
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null)
    {
        (RaftOperationStatus Status, int Removed) result = CompactLogsOlderThanCore(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        if (result.Status == RaftOperationStatus.Errored && TryRecoverEngineAfterFailure())
            result = CompactLogsOlderThanCore(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);

        if (result.Status == RaftOperationStatus.Success)
            NoteStorageSuccess();

        return result;
    }

    private RaftOperationStatus WriteCore(List<(int, List<RaftLog>)> logs, bool sync)
    {
        WriteOptions options = sync ? _writeOptions : WriteOptions.Default;

        // A checkpoint batch reads and then writes the persisted last checkpoint, so it is exclusive;
        // plain appends share the guard and keep group commit across threads.
        bool exclusive = ContainsCommittedCheckpoint(logs);
        try
        {
            using EngineLease lease = AcquireEngine();
            if (exclusive)
                _writeGuard.EnterWriteLock();
            else
                _writeGuard.EnterReadLock();

            try
            {
                return logs is [{ Item2.Count: 1 }] ? WriteOne(logs[0].Item1, logs[0].Item2[0], options) : WriteMany(logs, exclusive, options);
            }
            finally
            {
                if (exclusive)
                    _writeGuard.ExitWriteLock();
                else
                    _writeGuard.ExitReadLock();
            }
        }
        // A write after Dispose is the caller's misuse, not a storage fault: it throws, as a read does,
        // instead of probing the volume of a closed WAL. The other write paths filter it the same way.
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("Error during write: {Message}\n{StackTrace}", e.Message, e.StackTrace);
            NoteStorageFailure(e);
            return RaftOperationStatus.Errored;
        }
    }

    // One entry for one partition: the common shape of a proposal or a commit marker.
    private RaftOperationStatus WriteOne(int partitionId, RaftLog log, WriteOptions options)
    {
        ColumnFamily family = Shard(partitionId);
        if (log.Type == RaftLogType.CommittedCheckpoint)
        {
            // The row and the last-checkpoint id land in one batch. The id only rises, and only over a
            // prefix that is present here: a checkpoint broadcast over a replication gap lands as a plain
            // row, because the id certifies its whole prefix as applied and compactable.
            long currentFloor = GetLastCheckpointFromMeta(partitionId);
            long newCheckpoint = Math.Max(currentFloor, log.Id);
            WriteBatch checkpointBatch = new();
            PutLogToBatch(checkpointBatch, partitionId, log, family);
            if (newCheckpoint > currentFloor)
            {
                if (VerifyCheckpointPrefixPresent(partitionId, family, currentFloor, log.Id, stagedIds: null, stagedSingleId: log.Id, out long firstMissing))
                    PutLastCheckpointToBatch(checkpointBatch, partitionId, newCheckpoint);
                else
                    LogWithheldCheckpoint(partitionId, log.Id, currentFloor, firstMissing);
            }

            _db.Write(checkpointBatch, options);
            RaiseMaxLogCache(partitionId, log.Id);
            return RaftOperationStatus.Success;
        }

        if (log.Type == RaftLogType.Committed)
        {
            lock (FrontierLock(partitionId))
            {
                long frontier = GetCommitFrontier(partitionId);

                // A marker the durable frontier already certifies writes nothing.
                if (log.Id <= frontier)
                    return RaftOperationStatus.Success;

                (long advanced, bool absorbed) = TryAdvanceFrontierFor(partitionId, family, frontier, log, stagedProposed: null, stagedResolved: null);
                if (absorbed)
                {
                    // The commit is one small metadata put instead of a rewrite of the full row.
                    _db.Put(CommitFrontierKey(partitionId), Utf8(advanced), _metadata, options);
                    RaiseCommitFrontierCache(partitionId, advanced);
                    Interlocked.Increment(ref _commitMarkersAbsorbed);
                    return RaftOperationStatus.Success;
                }

                if (advanced > frontier)
                {
                    // A partial catch-up: the marker writes its full row, and the certified advance lands with it.
                    WriteBatch commitBatch = new();
                    PutLogToBatch(commitBatch, partitionId, log, family);
                    PutCommitFrontierToBatch(commitBatch, partitionId, advanced);
                    _db.Write(commitBatch, options);
                    RaiseCommitFrontierCache(partitionId, advanced);
                    RaiseMaxLogCache(partitionId, log.Id);
                    return RaftOperationStatus.Success;
                }
            }
        }

        Span<byte> key = stackalloc byte[LogKeyWidth];
        BuildLogKey(key, partitionId, log.Id);
        int size = MeasureLogEntry(partitionId, log, out int logTypeByteCount);
        byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
        Span<byte> value = (rented ?? stackalloc byte[StackallocThreshold])[..size];
        try
        {
            WriteLogEntry(value, partitionId, log, logTypeByteCount);
            _db.Put(key, value, family, options);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }

        NoteShardBytesWritten(size);
        RaiseMaxLogCache(partitionId, log.Id);
        return RaftOperationStatus.Success;
    }

    // The frontier locks of the partitions with commit markers, in ascending order, held through the write.
    private RaftOperationStatus WriteMany(List<(int, List<RaftLog>)> logs, bool exclusive, WriteOptions options)
    {
        List<int>? advancing = null;
        foreach ((int partitionId, List<RaftLog> entries) in logs)
            foreach (RaftLog log in entries)
                if (log.Type == RaftLogType.Committed)
                {
                    if (!(advancing ??= []).Contains(partitionId))
                        advancing.Add(partitionId);

                    break;
                }

        if (advancing is null)
            return WriteManyLocked(logs, exclusive, options);

        advancing.Sort();
        int held = 0;
        try
        {
            for (; held < advancing.Count; held++)
                Monitor.Enter(FrontierLock(advancing[held]));

            return WriteManyLocked(logs, exclusive, options);
        }
        finally
        {
            for (int i = held - 1; i >= 0; i--)
                Monitor.Exit(FrontierLock(advancing[i]));
        }
    }

    private RaftOperationStatus WriteManyLocked(List<(int, List<RaftLog>)> logs, bool exclusive, WriteOptions options)
    {
        // The entries of each partition, grouped by shard. A partition that appears once keeps the
        // caller's list; a second appearance makes an owned merged copy.
        Dictionary<ColumnFamily, Dictionary<int, List<RaftLog>>> plan = new();
        Dictionary<int, long> checkpointMaxByPartition = new();

        // The ids staged per checkpoint partition: the prefix check reads the family, which cannot see
        // rows still in the batch. Only on the exclusive checkpoint path, to keep appends allocation-free.
        Dictionary<int, HashSet<long>>? stagedIdsByPartition = exclusive ? new() : null;
        HashSet<List<RaftLog>>? ownedLists = null;
        foreach ((int partitionId, List<RaftLog> entries) in logs)
        {
            ColumnFamily family = Shard(partitionId);
            foreach (RaftLog entry in entries)
            {
                if (entry.Type == RaftLogType.CommittedCheckpoint && entry.Id > checkpointMaxByPartition.GetValueOrDefault(partitionId, -1))
                    checkpointMaxByPartition[partitionId] = entry.Id;

                if (stagedIdsByPartition is not null)
                {
                    if (!stagedIdsByPartition.TryGetValue(partitionId, out HashSet<long>? staged))
                        stagedIdsByPartition[partitionId] = staged = [];

                    staged.Add(entry.Id);
                }
            }

            if (plan.TryGetValue(family, out Dictionary<int, List<RaftLog>>? perPartition))
            {
                if (perPartition.TryGetValue(partitionId, out List<RaftLog>? existing))
                {
                    ownedLists ??= [];
                    if (!ownedLists.Contains(existing))
                    {
                        List<RaftLog> owned = new(existing.Count + entries.Count);
                        owned.AddRange(existing);
                        perPartition[partitionId] = owned;
                        ownedLists.Add(owned);
                        existing = owned;
                    }

                    existing.AddRange(entries);
                }
                else
                    perPartition.Add(partitionId, entries);
            }
            else
                plan.Add(family, new Dictionary<int, List<RaftLog>> { { partitionId, entries } });
        }

        WriteBatch batch = new();
        Dictionary<int, long>? frontierAdvances = null;
        Dictionary<int, long>? stagedMaxByPartition = null;
        long markersAbsorbed = 0;
        long shardBytes = 0;
        foreach ((ColumnFamily family, Dictionary<int, List<RaftLog>> perPartition) in plan)
        {
            foreach ((int partitionId, List<RaftLog> entries) in perPartition)
            {
                long frontier = GetCommitFrontier(partitionId);
                long pendingFrontier = frontier;

                // Rows staged in this batch, by resolution: the probes read the family, which cannot see
                // them. A later staging of the same id replaces its membership, as the last put wins.
                HashSet<long>? stagedProposed = null;
                HashSet<long>? stagedResolved = null;
                using MarkerHeaderPrefetch? prefetched = PrefetchMarkerHeaders(partitionId, family, entries, frontier);
                foreach (RaftLog log in entries)
                {
                    if (log.Type == RaftLogType.Committed)
                    {
                        if (log.Id <= pendingFrontier)
                            continue;

                        (long advanced, bool absorbed) = TryAdvanceFrontierFor(partitionId, family, pendingFrontier, log, stagedProposed, stagedResolved, prefetched);
                        pendingFrontier = advanced;
                        if (absorbed)
                        {
                            markersAbsorbed++;
                            continue;
                        }
                    }

                    shardBytes += PutLogToBatch(batch, partitionId, log, family);
                    stagedMaxByPartition ??= new();
                    if (log.Id > stagedMaxByPartition.GetValueOrDefault(partitionId, -1))
                        stagedMaxByPartition[partitionId] = log.Id;

                    if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                    {
                        (stagedProposed ??= []).Add(log.Id);
                        stagedResolved?.Remove(log.Id);
                    }
                    else
                    {
                        (stagedResolved ??= []).Add(log.Id);
                        stagedProposed?.Remove(log.Id);
                    }
                }

                if (pendingFrontier > frontier)
                    (frontierAdvances ??= new())[partitionId] = pendingFrontier;
            }
        }

        // The last-checkpoint id of each partition that committed a checkpoint lands with its rows, only
        // rises, and is withheld over a gap; a later checkpoint advances it once backfill closes the gap.
        foreach ((int partitionId, long batchMaxCheckpoint) in checkpointMaxByPartition)
        {
            long currentFloor = GetLastCheckpointFromMeta(partitionId);
            if (batchMaxCheckpoint <= currentFloor)
                continue;

            HashSet<long>? staged = null;
            stagedIdsByPartition?.TryGetValue(partitionId, out staged);
            if (VerifyCheckpointPrefixPresent(partitionId, Shard(partitionId), currentFloor, batchMaxCheckpoint, staged, stagedSingleId: -1, out long firstMissing))
                PutLastCheckpointToBatch(batch, partitionId, batchMaxCheckpoint);
            else
                LogWithheldCheckpoint(partitionId, batchMaxCheckpoint, currentFloor, firstMissing);
        }

        if (frontierAdvances is not null)
            foreach ((int partitionId, long value) in frontierAdvances)
                PutCommitFrontierToBatch(batch, partitionId, value);

        if (batch.Count > 0)
            _db.Write(batch, options);

        if (frontierAdvances is not null)
            foreach ((int partitionId, long value) in frontierAdvances)
                RaiseCommitFrontierCache(partitionId, value);

        if (stagedMaxByPartition is not null)
            foreach ((int partitionId, long value) in stagedMaxByPartition)
                RaiseMaxLogCache(partitionId, value);

        Interlocked.Add(ref _commitMarkersAbsorbed, markersAbsorbed);
        NoteShardBytesWritten(shardBytes);
        return RaftOperationStatus.Success;
    }

    private void LogWithheldCheckpoint(int partitionId, long checkpointId, long floor, long firstMissing)
    {
        _logger.LogWarning(
            "Withholding last-checkpoint advance for partition {Partition}: checkpoint {CheckpointId} landed over a gap (floor {Floor}, first missing id {MissingId}); the floor advances when a later checkpoint finds the gap backfilled",
            partitionId, checkpointId, floor, firstMissing);
    }

    private static bool ContainsCommittedCheckpoint(List<(int, List<RaftLog>)> logs)
    {
        foreach ((int _, List<RaftLog> entries) in logs)
            foreach (RaftLog log in entries)
                if (log.Type == RaftLogType.CommittedCheckpoint)
                    return true;

        return false;
    }

    // Stages one row with the hand-written encoder; the batch copies the value, so the buffer is
    // returned at once. Returns the value's size.
    private static int PutLogToBatch(WriteBatch batch, int partitionId, RaftLog log, ColumnFamily family)
    {
        Span<byte> key = stackalloc byte[LogKeyWidth];
        BuildLogKey(key, partitionId, log.Id);
        int size = MeasureLogEntry(partitionId, log, out int logTypeByteCount);
        byte[]? rented = size > StackallocThreshold ? ArrayPool<byte>.Shared.Rent(size) : null;
        Span<byte> value = (rented ?? stackalloc byte[StackallocThreshold])[..size];
        try
        {
            WriteLogEntry(value, partitionId, log, logTypeByteCount);
            batch.Put(key, value, family);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }

        return size;
    }

    // After a flush unit of shard rows, flush the metadata family, which never fills its memtable on its
    // own and would otherwise keep every WAL file since its last flush alive.
    private void NoteShardBytesWritten(long bytes)
    {
        if (bytes <= 0)
            return;

        long total = Interlocked.Add(ref _shardBytesSinceMetadataFlush, bytes);
        if (total < _metadataFlushEveryBytes || Interlocked.CompareExchange(ref _shardBytesSinceMetadataFlush, 0, total) != total)
            return;

        try
        {
            _db.Flush(_metadata, wait: false);
        }
        catch (Exception e)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Solarium WAL at '{Path}': the metadata flush that releases WAL files could not start: {Message}", _enginePath, e.Message);
        }
    }

    private bool SetMetaDataCore(string key, string value)
    {
        // Backs the hard state on the election path, so a failure is false, never an exception.
        try
        {
            using EngineLease lease = AcquireEngine();
            _db.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), _metadata);
            return true;
        }
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("Error during SetMetaData({Key}): {Message}", key, e.Message);
            NoteStorageFailure(e);
            return false;
        }
    }

    private RaftOperationStatus DeletePartitionWALCore(int partitionId)
    {
        try
        {
            using EngineLease lease = AcquireEngine();
            ColumnFamily family = Shard(partitionId);

            // One range delete covers the partition; the probe only decides whether any row exists.
            WriteBatch batch = new();
            using (Iterator iterator = _db.NewIterator(family))
            {
                Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                BuildLogKey(seekKey, partitionId, 0);
                iterator.Seek(seekKey);
                if (iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId))
                {
                    byte[] end = new byte[PartitionIdWidth + 1];
                    BuildPartitionUpperBoundKey(end, partitionId);
                    batch.DeleteRange(seekKey, end, family);
                }
            }

            // The metadata goes too: a stale floor, frontier or checkpoint would hide or certify the rows a
            // reused partition id writes from low ids again.
            batch.Delete(LastCheckpointKey(partitionId), _metadata);
            batch.Delete(CommitFrontierKey(partitionId), _metadata);
            batch.Delete(CompactionFloorKey(partitionId), _metadata);

            // An advance that read the old frontier must not put it back after the delete.
            lock (FrontierLock(partitionId))
            {
                _db.Write(batch, _writeOptions);
                _compactionFloorCache.TryRemove(partitionId, out _);
                _commitFrontierCache.TryRemove(partitionId, out _);
            }

            InvalidateMaxLogCache(partitionId);
            return RaftOperationStatus.Success;
        }
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("Error during DeletePartitionWAL({PartitionId}): {Message}", partitionId, e.Message);
            NoteStorageFailure(e);
            return RaftOperationStatus.Errored;
        }
    }

    private RaftOperationStatus TruncateLogsAfterCore(int partitionId, long afterLogId)
    {
        try
        {
            using EngineLease lease = AcquireEngine();
            _writeGuard.EnterWriteLock();
            try
            {
                ColumnFamily family = Shard(partitionId);
                WriteBatch batch = new();
                int staged = 0;
                using (Iterator iterator = NewPartitionBoundedIterator(family, partitionId))
                {
                    Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                    BuildLogKey(seekKey, partitionId, afterLogId + 1);
                    iterator.Seek(seekKey);
                    for (; iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId); iterator.Next())
                    {
                        batch.Delete(iterator.Key(), family);
                        staged++;
                    }
                }

                // The frontier and the floor clamp to the cut even when no row went: a Proposed row later
                // written at a truncated id must not read as Committed or as compacted.
                long clampedFrontier = Math.Max(afterLogId, 0);
                bool clampFrontier = GetCommitFrontier(partitionId) > clampedFrontier;
                if (clampFrontier)
                    PutCommitFrontierToBatch(batch, partitionId, clampedFrontier);

                long clampedFloor = Math.Max(afterLogId + 1, 0);
                bool clampFloor = GetCompactionFloor(partitionId) > clampedFloor;
                if (clampFloor)
                    PutCompactionFloorToBatch(batch, partitionId, clampedFloor);

                // A truncation that removes the recorded checkpoint records the surviving one, exactly.
                if (staged > 0 && GetLastCheckpointFromMeta(partitionId) > afterLogId)
                {
                    long surviving = ScanHighestCheckpointAtMost(partitionId, family, afterLogId);
                    if (surviving < 0)
                        batch.Delete(LastCheckpointKey(partitionId), _metadata);
                    else
                        PutLastCheckpointToBatch(batch, partitionId, surviving);
                }

                if (staged > 0 || clampFrontier || clampFloor)
                    _db.Write(batch, _writeOptions);

                if (staged > 0)
                    InvalidateMaxLogCache(partitionId);

                // An exact set: truncation is the one path that lowers them, under the exclusive guard.
                if (clampFrontier)
                    _commitFrontierCache[partitionId] = clampedFrontier;

                if (clampFloor)
                    _compactionFloorCache[partitionId] = clampedFloor;

                return RaftOperationStatus.Success;
            }
            finally
            {
                _writeGuard.ExitWriteLock();
            }
        }
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("TruncateLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, e.Message);
            NoteStorageFailure(e);
            return RaftOperationStatus.Errored;
        }
    }

    private RaftOperationStatus TruncateProposedLogsAfterCore(int partitionId, long afterLogId)
    {
        // A follower append that extends the tail has nothing above its cut: a known max at or below it
        // proves the sweep empty without the exclusive guard.
        if (_maxLogCache.TryGetValue(partitionId, out PartitionMaxLog? knownMax) && Volatile.Read(ref knownMax.Value) <= afterLogId)
            return RaftOperationStatus.Success;

        try
        {
            using EngineLease lease = AcquireEngine();
            _writeGuard.EnterWriteLock();
            try
            {
                ColumnFamily family = Shard(partitionId);

                // A Proposed row at or below the commit frontier is committed; deleting it would lose it.
                long commitFrontier = GetCommitFrontier(partitionId);
                WriteBatch batch = new();
                int staged = 0;
                long survivingAbove = -1;
                using (Iterator iterator = NewPartitionBoundedIterator(family, partitionId))
                {
                    Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                    BuildLogKey(seekKey, partitionId, afterLogId + 1);
                    iterator.Seek(seekKey);
                    for (; iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId); iterator.Next())
                    {
                        ReadHeaderFromWire(iterator.Value(), out _, out _, out _, out int type);
                        long id = ParseLogIdFromKey(iterator.Key());
                        if (type is (int)RaftLogType.Proposed or (int)RaftLogType.ProposedCheckpoint && id > commitFrontier)
                        {
                            batch.Delete(iterator.Key(), family);
                            staged++;
                        }
                        else if (id > survivingAbove)
                            survivingAbove = id;
                    }
                }

                if (staged > 0)
                {
                    _db.Write(batch, _writeOptions);
                    if (_maxLogCache.TryGetValue(partitionId, out PartitionMaxLog? cachedMax) && cachedMax.Value > afterLogId)
                    {
                        if (survivingAbove > 0)
                            SetMaxLogCache(partitionId, survivingAbove);
                        else
                            InvalidateMaxLogCache(partitionId);
                    }
                }

                // Only unresolved rows go, and a CommittedCheckpoint is resolved: the recorded checkpoint stays.
                return RaftOperationStatus.Success;
            }
            finally
            {
                _writeGuard.ExitWriteLock();
            }
        }
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("TruncateProposedLogsAfter({PartitionId}, {AfterLogId}): {Message}", partitionId, afterLogId, e.Message);
            NoteStorageFailure(e);
            return RaftOperationStatus.Errored;
        }
    }

    private (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundaryCore(int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
    {
        WriteOptions options = sync ? _writeOptions : WriteOptions.Default;
        try
        {
            using EngineLease lease = AcquireEngine();
            _writeGuard.EnterWriteLock();
            try
            {
                ColumnFamily family = Shard(partitionId);
                byte[] boundaryKey = new byte[LogKeyWidth];
                BuildLogKey(boundaryKey, partitionId, snapshotIndex);
                byte[]? existing = snapshotIndex < GetCompactionFloor(partitionId) ? null : _db.Get(boundaryKey, family);
                long localTerm = existing is null ? -1 : ReadTermFromWire(existing);

                WriteBatch batch = new();
                bool suffixTruncated = false;
                if (localTerm != lastIncludedTerm)
                {
                    int staged = 0;
                    using (Iterator iterator = NewPartitionBoundedIterator(family, partitionId))
                    {
                        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                        BuildLogKey(seekKey, partitionId, snapshotIndex + 1);
                        iterator.Seek(seekKey);
                        for (; iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId); iterator.Next())
                        {
                            batch.Delete(iterator.Key(), family);
                            staged++;
                        }
                    }

                    suffixTruncated = staged > 0;
                }

                // The checkpoint row at the boundary overwrites any entry there.
                PutLogToBatch(batch, partitionId, new RaftLog { Id = snapshotIndex, Term = lastIncludedTerm, Type = RaftLogType.CommittedCheckpoint }, family);

                // With the suffix gone the boundary is the highest checkpoint; with it kept, a higher one may remain.
                long newCheckpoint = suffixTruncated ? snapshotIndex : Math.Max(GetLastCheckpointFromMeta(partitionId), snapshotIndex);
                PutLastCheckpointToBatch(batch, partitionId, newCheckpoint);

                // A frontier above a removed suffix clamps to the boundary; it is never raised to it, since it
                // only certifies rows it verified.
                bool clampFrontier = suffixTruncated && GetCommitFrontier(partitionId) > snapshotIndex;
                if (clampFrontier)
                    PutCommitFrontierToBatch(batch, partitionId, snapshotIndex);

                // The boundary row must be readable, so a floor above it clamps to it.
                bool clampFloor = GetCompactionFloor(partitionId) > snapshotIndex;
                if (clampFloor)
                    PutCompactionFloorToBatch(batch, partitionId, snapshotIndex);

                OnAfterBoundaryScanForTesting?.Invoke();
                _db.Write(batch, options);

                if (clampFrontier)
                    _commitFrontierCache[partitionId] = snapshotIndex;

                if (clampFloor)
                    _compactionFloorCache[partitionId] = snapshotIndex;

                if (suffixTruncated)
                    SetMaxLogCache(partitionId, snapshotIndex);
                else
                    RaiseMaxLogCache(partitionId, snapshotIndex);

                return (RaftOperationStatus.Success, suffixTruncated);
            }
            finally
            {
                _writeGuard.ExitWriteLock();
            }
        }
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("InstallSnapshotBoundary({PartitionId}, {SnapshotIndex}): {Message}", partitionId, snapshotIndex, e.Message);
            NoteStorageFailure(e);
            return (RaftOperationStatus.Errored, false);
        }
    }

    private (RaftOperationStatus Status, int Removed) CompactLogsOlderThanCore(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries)
    {
        int passCap = maxTotalEntries ?? compactNumberEntries;
        try
        {
            using EngineLease lease = AcquireEngine();
            ColumnFamily family = Shard(partitionId);
            long floor = GetCompactionFloor(partitionId);
            long newFloor = Math.Max(floor, lastCheckpoint);

            // Removed counts the rows present in [floor, newFloor): exactly up to the cap, and beyond it by
            // arithmetic, since ids are contiguous, so a large backlog costs O(cap) steps.
            int removed = 0;
            using (Iterator iterator = NewBoundedIterator(family, partitionId, newFloor, fillCache: false))
            {
                Span<byte> seekKey = stackalloc byte[LogKeyWidth];
                BuildLogKey(seekKey, partitionId, floor);
                iterator.Seek(seekKey);
                long lastCounted = -1;
                for (; iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId) && removed < passCap; iterator.Next())
                {
                    long id = ParseLogIdFromKey(iterator.Key());
                    if (id >= newFloor)
                        break;

                    lastCounted = id;
                    removed++;
                }

                if (removed == passCap && iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId) && ParseLogIdFromKey(iterator.Key()) < newFloor)
                    removed = (int)Math.Min(int.MaxValue, removed + Math.Max(0, newFloor - 1 - lastCounted));
            }

            if (newFloor > floor)
            {
                // The floor is durable before any file below it is dropped. The range delete lets rows die
                // in memory: confined to the active memtable when the partition owns its shard, so the
                // flushed file's range never reaches below its first row; over the whole dead range in a
                // shared shard, whose files can never be dropped whole for one partition.
                WriteBatch batch = new();
                PutCompactionFloorToBatch(batch, partitionId, newFloor);
                long tombstoneStart = ShardHoldsOtherPartitions(family, partitionId) ? floor : Math.Max(floor, ConfinedTombstoneStart(family, partitionId));
                if (tombstoneStart < newFloor)
                {
                    byte[] begin = new byte[LogKeyWidth];
                    BuildLogKey(begin, partitionId, tombstoneStart);
                    byte[] end = new byte[LogKeyWidth];
                    BuildLogKey(end, partitionId, newFloor);
                    batch.DeleteRange(begin, end, family);
                }

                _db.Write(batch, _writeOptions);
                _compactionFloorCache[partitionId] = newFloor;
                LastCompactionWriteCount = 1;
                if (removed > 0 && _logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Removed {Count} logs from the WAL of partition {Partition}", removed, partitionId);
            }
            else
                LastCompactionWriteCount = 0;

            // On every pass: files below an unchanged floor may have moved out of level 0 since the last one.
            DropFullyDeadFiles(family, partitionId, newFloor);
            return (RaftOperationStatus.Success, removed);
        }
        catch (Exception e) when (e is not ObjectDisposedException)
        {
            _logger.LogError("Error during compact: {Message}", e.Message);
            NoteStorageFailure(e);
            return (RaftOperationStatus.Errored, 0);
        }
    }

    // Drops the tables of the shard that lie wholly in [key(p, 0), key(p, floor - 1)]: dead data reclaimed
    // with no rewrite. A failure leaves the rows on disk until a later pass; they are logically gone through
    // the floor anyway.
    private void DropFullyDeadFiles(ColumnFamily family, int partitionId, long floor)
    {
        if (floor < 2)
            return;

        byte[] begin = new byte[LogKeyWidth];
        BuildLogKey(begin, partitionId, 0);
        byte[] end = new byte[LogKeyWidth];
        BuildLogKey(end, partitionId, floor - 1);
        try
        {
            _db.DeleteFilesInRange(begin, end, family);
        }
        catch (SolariumException e)
        {
            _logger.LogWarning("DeleteFilesInRange for partition {PartitionId} below floor {Floor} failed: {Message}; the dead prefix stays on disk until a later pass reclaims it", partitionId, floor, e.Message);
        }
    }

    // True when the shard holds a key of another partition: two seeks, no value read.
    private bool ShardHoldsOtherPartitions(ColumnFamily family, int partitionId)
    {
        using Iterator iterator = _db.NewIterator(family);
        iterator.SeekToFirst();
        if (iterator.Valid() && !KeyBelongsToPartition(iterator.Key(), partitionId))
            return true;

        Span<byte> upperBound = stackalloc byte[PartitionIdWidth + 1];
        BuildPartitionUpperBoundKey(upperBound, partitionId);
        iterator.Seek(upperBound);
        return iterator.Valid();
    }

    // The lowest id a compaction tombstone may start at without reaching below the active memtable: the
    // partition owns the shard, so the memtable's entries are its newest rows. A property that does not
    // parse means no tombstone at all, which is safe: the rows flush and the whole-file drop takes them.
    private long ConfinedTombstoneStart(ColumnFamily family, int partitionId)
    {
        if (!long.TryParse(_db.GetProperty("rocksdb.num-entries-active-mem-table", family), NumberStyles.None, CultureInfo.InvariantCulture, out long activeEntries))
            return long.MaxValue;

        using Iterator iterator = _db.NewIterator(family);
        SeekToLastPartitionKey(iterator, partitionId);
        if (!iterator.Valid() || !KeyBelongsToPartition(iterator.Key(), partitionId))
            return long.MaxValue;

        return Math.Max(0, ParseLogIdFromKey(iterator.Key()) - activeEntries + 1 + ConfinedTombstoneMargin);
    }

    // ── Commit frontier ──────────────────────────────────────────────────────────────────────────
    //
    // A commit carries one bit, so it is persisted as a per-partition COMMIT FRONTIER F, the highest id
    // such that every row at or below it is resolved, instead of a rewrite of the full row. A Proposed row
    // at or below F reads as Committed. F only advances over rows durably present with the marker's term
    // (or staged as Proposed in the same batch); truncations clamp it to the surviving prefix; it never
    // certifies a Proposed row it did not verify. Absent the key, reads fall back to the row types.

    // How far the frontier may advance for the marker log, walking up from pendingFrontier. Absorbed when
    // the walk reached the marker's id, which then needs no row. The walk passes an id only on evidence
    // that keeps every read the same: a present (or staged) resolved row, or an absent id at or below the
    // last checkpoint, which compaction removed after it was certified.
    private (long NewFrontier, bool Absorbed) TryAdvanceFrontierFor(
        int partitionId, ColumnFamily family, long pendingFrontier, RaftLog log, HashSet<long>? stagedProposed, HashSet<long>? stagedResolved,
        MarkerHeaderPrefetch? prefetched = null)
    {
        long target = log.Id;
        long advanced = pendingFrontier;
        if (advanced < target - 1)
        {
            long certifiedFloor = GetLastCheckpointFromMeta(partitionId);
            long compactionFloor = GetCompactionFloor(partitionId);
            int steps = 0;

            // At or below the certified floor the walk hops over absent spans with an iterator bounded at
            // the floor, so it never pays for what lies above the checkpoint; above it, a point read per id.
            Iterator? certified = null;
            Span<byte> seekKey = stackalloc byte[LogKeyWidth];
            try
            {
                while (advanced < target - 1 && steps < FrontierCatchUpBound)
                {
                    steps++;
                    long next = advanced + 1;
                    if (stagedResolved is not null && stagedResolved.Contains(next))
                    {
                        advanced = next;
                        continue;
                    }

                    if (stagedProposed is not null && stagedProposed.Contains(next))
                        return (advanced, false);

                    if (next <= certifiedFloor)
                    {
                        long seekFrom = Math.Max(next, compactionFloor);
                        if (certified is null)
                        {
                            certified = NewBoundedIterator(family, partitionId, certifiedFloor + 1);
                            BuildLogKey(seekKey, partitionId, seekFrom);
                            certified.Seek(seekKey);
                        }
                        else
                            while (certified.Valid() && ParseLogIdFromKey(certified.Key()) < seekFrom)
                                certified.Next();

                        long present = certified.Valid() && KeyBelongsToPartition(certified.Key(), partitionId) ? ParseLogIdFromKey(certified.Key()) : long.MaxValue;
                        if (present > next)
                        {
                            advanced = Math.Min(present - 1, Math.Min(certifiedFloor, target - 1));
                            continue;
                        }

                        ReadHeaderFromWire(certified.Value(), out _, out _, out _, out int presentType);
                        if (!IsResolvedRowType(presentType))
                            return (advanced, false);

                        advanced = next;
                        continue;
                    }

                    (bool found, _, int rowType) = ProbeRowHeader(partitionId, family, next, prefetched);
                    if (!found || !IsResolvedRowType(rowType))
                        return (advanced, false);

                    advanced = next;
                }
            }
            finally
            {
                certified?.Dispose();
            }

            if (advanced < target - 1)
                return (advanced, false);
        }

        if (stagedProposed is not null && stagedProposed.Contains(target))
            return (target, true);

        (bool targetFound, long term, int targetType) = ProbeRowHeader(partitionId, family, target, prefetched);
        if (targetFound && term == log.Term && targetType is (int)RaftLogType.Proposed or (int)RaftLogType.Committed)
            return (target, true);

        return (advanced, false);
    }

    private static bool IsResolvedRowType(int type)
    {
        return type is (int)RaftLogType.Committed or (int)RaftLogType.CommittedCheckpoint or (int)RaftLogType.RolledBack or (int)RaftLogType.RolledBackCheckpoint;
    }

    // The header of the row at (partition, id), from the prefetch when it holds the id, else a point read.
    private (bool Found, long Term, int Type) ProbeRowHeader(int partitionId, ColumnFamily family, long logId, MarkerHeaderPrefetch? prefetched = null)
    {
        if (logId < GetCompactionFloor(partitionId))
            return default;

        if (prefetched is not null && prefetched.TryGet(logId, out (bool Found, long Term, int Type) header))
            return header;

        Span<byte> key = stackalloc byte[LogKeyWidth];
        BuildLogKey(key, partitionId, logId);
        byte[]? value = _db.Get(key, family);
        if (value is null)
            return default;

        ReadHeaderFromWire(value, out _, out _, out long term, out int type);
        return (true, term, type);
    }

    // Reads, in batched MultiGets, the target headers of the Committed markers above the frontier that the
    // probes will ask for. It only changes how the probes read, never what they decide: an id it does not
    // hold falls back to a point read. Null when fewer than two ids remain.
    private MarkerHeaderPrefetch? PrefetchMarkerHeaders(int partitionId, ColumnFamily family, List<RaftLog> logs, long frontier)
    {
        int candidates = 0;
        foreach (RaftLog log in logs)
            if (log.Type == RaftLogType.Committed && log.Id > frontier)
                candidates++;

        if (candidates < 2)
            return null;

        long compactionFloor = GetCompactionFloor(partitionId);
        long[] ids = ArrayPool<long>.Shared.Rent(candidates);
        int count = 0;
        HashSet<long>? proposedAhead = null;
        foreach (RaftLog log in logs)
        {
            // A marker whose Proposed row is staged earlier in the slice is accepted from the staged set.
            if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                (proposedAhead ??= []).Add(log.Id);
            else if (log.Type == RaftLogType.Committed && log.Id > frontier && log.Id >= compactionFloor && (proposedAhead is null || !proposedAhead.Contains(log.Id)))
                ids[count++] = log.Id;
        }

        Array.Sort(ids, 0, count);
        int distinct = 0;
        for (int i = 0; i < count; i++)
            if (distinct == 0 || ids[distinct - 1] != ids[i])
                ids[distinct++] = ids[i];

        if (distinct < 2)
        {
            ArrayPool<long>.Shared.Return(ids);
            return null;
        }

        MarkerHeaderPrefetch prefetch = new(ids, distinct);
        try
        {
            List<byte[]> keys = new(Math.Min(distinct, MarkerHeaderReadBatchSize));
            for (int start = 0; start < distinct; start += MarkerHeaderReadBatchSize)
            {
                int chunk = Math.Min(MarkerHeaderReadBatchSize, distinct - start);
                keys.Clear();
                for (int i = 0; i < chunk; i++)
                {
                    byte[] key = new byte[LogKeyWidth];
                    BuildLogKey(key, partitionId, ids[start + i]);
                    keys.Add(key);
                }

                byte[]?[] values = _db.MultiGet(keys, family);
                for (int i = 0; i < chunk; i++)
                {
                    if (values[i] is not { } value)
                        continue;

                    ReadHeaderFromWire(value, out _, out _, out long term, out int type);
                    prefetch.Set(start + i, term, type);
                }
            }
        }
        catch
        {
            prefetch.Dispose();
            throw;
        }

        Interlocked.Add(ref _markerHeadersPrefetched, distinct);
        return prefetch;
    }

    // ── Metadata keys ────────────────────────────────────────────────────────────────────────────

    private byte[] LastCheckpointKey(int partitionId) =>
        _lastCheckpointKeys.GetOrAdd(partitionId, static id => Encoding.UTF8.GetBytes($"raft_last_checkpoint_p{id}"));

    private byte[] CommitFrontierKey(int partitionId) =>
        _commitFrontierKeys.GetOrAdd(partitionId, static id => Encoding.UTF8.GetBytes($"raft_commit_frontier_p{id}"));

    private byte[] CompactionFloorKey(int partitionId) =>
        _compactionFloorKeys.GetOrAdd(partitionId, static id => Encoding.UTF8.GetBytes($"raft_compaction_floor_p{id}"));

    // -1 when no checkpoint was ever recorded for the partition. The key moves with every checkpoint
    // mutation, in the same batch, so there is no scan fallback.
    private long GetLastCheckpointFromMeta(int partitionId) => ReadLong(LastCheckpointKey(partitionId), -1);

    private void PutLastCheckpointToBatch(WriteBatch batch, int partitionId, long value) => batch.Put(LastCheckpointKey(partitionId), Utf8(value), _metadata);

    private long GetCommitFrontier(int partitionId)
    {
        if (_commitFrontierCache.TryGetValue(partitionId, out long cached))
            return cached;

        long persisted = ReadLong(CommitFrontierKey(partitionId), 0);
        return _commitFrontierCache.TryAdd(partitionId, persisted) ? persisted : _commitFrontierCache[partitionId];
    }

    private void PutCommitFrontierToBatch(WriteBatch batch, int partitionId, long value) => batch.Put(CommitFrontierKey(partitionId), Utf8(value), _metadata);

    private object FrontierLock(int partitionId) => _frontierLocks.GetOrAdd(partitionId, static _ => new object());

    private void RaiseCommitFrontierCache(int partitionId, long value) =>
        _commitFrontierCache.AddOrUpdate(partitionId, static (_, v) => v, static (_, old, v) => Math.Max(old, v), value);

    // Rows below the floor are logically deleted (0: nothing compacted).
    private long GetCompactionFloor(int partitionId)
    {
        if (_compactionFloorCache.TryGetValue(partitionId, out long cached))
            return cached;

        long persisted = ReadLong(CompactionFloorKey(partitionId), 0);
        return _compactionFloorCache.TryAdd(partitionId, persisted) ? persisted : _compactionFloorCache[partitionId];
    }

    private void PutCompactionFloorToBatch(WriteBatch batch, int partitionId, long value) => batch.Put(CompactionFloorKey(partitionId), Utf8(value), _metadata);

    private long ReadLong(byte[] key, long fallback)
    {
        byte[]? value = _db.Get(key, _metadata);
        return value is not null && long.TryParse(Encoding.UTF8.GetString(value), NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) ? parsed : fallback;
    }

    private static byte[] Utf8(long value) => Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture));

    // Whether every id in (floorExclusive, checkpointId] is present: in the family, staged in this batch,
    // or certified by the existing floor. A floor of -1 anchors the scan at id 1, so a partition whose
    // checkpoint metadata was truncated away withholds advances rather than certifying an unknown prefix.
    private bool VerifyCheckpointPrefixPresent(
        int partitionId, ColumnFamily family, long floorExclusive, long checkpointId, HashSet<long>? stagedIds, long stagedSingleId, out long firstMissing)
    {
        firstMissing = -1;
        long expected = Math.Max(floorExclusive, 0) + 1;
        if (expected > checkpointId)
            return true;

        // Rows below the compaction floor are logically absent even while their file is on disk.
        if (expected < GetCompactionFloor(partitionId))
        {
            firstMissing = expected;
            return false;
        }

        using Iterator iterator = NewBoundedIterator(family, partitionId, checkpointId + 1);
        Span<byte> seekKey = stackalloc byte[LogKeyWidth];
        BuildLogKey(seekKey, partitionId, expected);
        iterator.Seek(seekKey);
        while (expected <= checkpointId)
        {
            long present = iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId) ? ParseLogIdFromKey(iterator.Key()) : long.MaxValue;
            if (present <= expected)
            {
                if (present == expected)
                    expected++;

                iterator.Next();
                continue;
            }

            if (expected == stagedSingleId || (stagedIds is not null && stagedIds.Contains(expected)))
            {
                expected++;
                continue;
            }

            firstMissing = expected;
            return false;
        }

        return true;
    }

    // The highest CommittedCheckpoint id at most upperIdInclusive, or -1: a reverse scan on the rare path
    // of a truncation that removes the recorded checkpoint.
    private long ScanHighestCheckpointAtMost(int partitionId, ColumnFamily family, long upperIdInclusive)
    {
        long floor = GetCompactionFloor(partitionId);
        using Iterator iterator = _db.NewIterator(family);
        SeekToLastPartitionKey(iterator, partitionId);
        for (; iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId); iterator.Prev())
        {
            long id = ParseLogIdFromKey(iterator.Key());
            if (id < floor)
                break;

            if (id <= upperIdInclusive)
            {
                ReadHeaderFromWire(iterator.Value(), out _, out _, out _, out int type);
                if (type == (int)RaftLogType.CommittedCheckpoint)
                    return id;
            }
        }

        return -1;
    }

    // ── Max log cache ────────────────────────────────────────────────────────────────────────────

    // The highest physically present id of a partition (0 when it has no row), from the key of its last
    // row. Not clamped at the floor. Runs where no append or truncation interleaves.
    private long ScanMaxLogId(int partitionId)
    {
        using Iterator iterator = _db.NewIterator(Shard(partitionId));
        SeekToLastPartitionKey(iterator, partitionId);
        return iterator.Valid() && KeyBelongsToPartition(iterator.Key(), partitionId) ? ParseLogIdFromKey(iterator.Key()) : 0;
    }

    // A last row below the compaction floor means every row is logically deleted.
    private static long ClampMaxLogAtFloor(long id, long floor) => id >= floor ? id : 0;

    // After rows through id landed; a partition with no known value stays unknown.
    private void RaiseMaxLogCache(int partitionId, long id)
    {
        if (id <= 0 || !_maxLogCache.TryGetValue(partitionId, out PartitionMaxLog? entry))
            return;

        long current = Volatile.Read(ref entry.Value);
        while (current < id)
        {
            long seen = Interlocked.CompareExchange(ref entry.Value, id, current);
            if (seen == current)
                return;

            current = seen;
        }
    }

    private void SetMaxLogCache(int partitionId, long id) => _maxLogCache[partitionId] = new PartitionMaxLog(id);

    private void InvalidateMaxLogCache(int partitionId) => _maxLogCache.TryRemove(partitionId, out _);

    // ── Row encoding ─────────────────────────────────────────────────────────────────────────────
    //
    // The RaftLogMessage protobuf encoding, written and read by hand: proto3 scalars (fields 1, 2, 3, 4, 7,
    // 8, 9) are omitted when zero; logType (5) and log (6) are written, even empty, when the RaftLog's
    // reference is set; negative int32 fields sign-extend to 10-byte varints, as the generated code does.

    /// <summary>The size of the encoding of (<paramref name="partitionId"/>, <paramref name="log"/>); <paramref name="logTypeByteCount"/> is the UTF-8 length of the log type, or -1.</summary>
    internal static int MeasureLogEntry(int partitionId, RaftLog log, out int logTypeByteCount)
    {
        int size = 0;
        if (partitionId != 0)
            size += 1 + VarintSize((ulong)(long)partitionId);
        if (log.Id != 0)
            size += 1 + VarintSize((ulong)log.Id);
        if (log.Term != 0)
            size += 1 + VarintSize((ulong)log.Term);
        if ((int)log.Type != 0)
            size += 1 + VarintSize((ulong)(long)(int)log.Type);

        logTypeByteCount = -1;
        if (log.LogType is not null)
        {
            logTypeByteCount = Encoding.UTF8.GetByteCount(log.LogType);
            size += 1 + VarintSize((ulong)logTypeByteCount) + logTypeByteCount;
        }

        if (log.LogData is not null)
            size += 1 + VarintSize((ulong)log.LogData.Length) + log.LogData.Length;
        if (log.Time.N != 0)
            size += 1 + VarintSize((ulong)(long)log.Time.N);
        if (log.Time.L != 0)
            size += 1 + VarintSize((ulong)log.Time.L);
        if (log.Time.C != 0)
            size += 1 + VarintSize(log.Time.C);

        return size;
    }

    /// <summary>Writes the encoding into <paramref name="destination"/>, exactly <see cref="MeasureLogEntry"/> bytes.</summary>
    internal static void WriteLogEntry(Span<byte> destination, int partitionId, RaftLog log, int logTypeByteCount)
    {
        int position = 0;
        if (partitionId != 0)
        {
            destination[position++] = 0x08;
            WriteVarint(destination, ref position, (ulong)(long)partitionId);
        }

        if (log.Id != 0)
        {
            destination[position++] = 0x10;
            WriteVarint(destination, ref position, (ulong)log.Id);
        }

        if (log.Term != 0)
        {
            destination[position++] = 0x18;
            WriteVarint(destination, ref position, (ulong)log.Term);
        }

        if ((int)log.Type != 0)
        {
            destination[position++] = 0x20;
            WriteVarint(destination, ref position, (ulong)(long)(int)log.Type);
        }

        if (log.LogType is not null)
        {
            destination[position++] = 0x2A;
            WriteVarint(destination, ref position, (ulong)logTypeByteCount);
            position += Encoding.UTF8.GetBytes(log.LogType, destination[position..]);
        }

        if (log.LogData is not null)
        {
            destination[position++] = 0x32;
            WriteVarint(destination, ref position, (ulong)log.LogData.Length);
            log.LogData.CopyTo(destination[position..]);
            position += log.LogData.Length;
        }

        if (log.Time.N != 0)
        {
            destination[position++] = 0x38;
            WriteVarint(destination, ref position, (ulong)(long)log.Time.N);
        }

        if (log.Time.L != 0)
        {
            destination[position++] = 0x40;
            WriteVarint(destination, ref position, (ulong)log.Time.L);
        }

        if (log.Time.C != 0)
        {
            destination[position++] = 0x48;
            WriteVarint(destination, ref position, log.Time.C);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int VarintSize(ulong value)
    {
        int size = 1;
        while (value >= 0x80)
        {
            size++;
            value >>= 7;
        }

        return size;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteVarint(Span<byte> destination, ref int position, ulong value)
    {
        while (value >= 0x80)
        {
            destination[position++] = (byte)(value | 0x80);
            value >>= 7;
        }

        destination[position++] = (byte)value;
    }

    private static ulong ReadVarint(ReadOnlySpan<byte> data, ref int position)
    {
        ulong result = 0;
        int shift = 0;
        while (position < data.Length)
        {
            byte b = data[position++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                break;

            shift += 7;
        }

        return result;
    }

    // Moves past a length-delimited field: its length, then that many bytes. The length is read into a
    // variable first: in "position += ReadVarint(data, ref position)", C# reads position before the
    // call advances it, so the skip lands one byte short, inside the field, and its bytes parse as
    // fields. A length past the end of the value is corruption, not a field to skip.
    private static void SkipLengthDelimited(ReadOnlySpan<byte> data, ref int position)
    {
        ulong length = ReadVarint(data, ref position);
        if (length > (ulong)(data.Length - position))
            throw new InvalidDataException($"Truncated RaftLogMessage: a field declares {length} bytes but only {data.Length - position} remain.");

        position += (int)length;
    }

    // Only the term (field 3), skipping the rest, the payload included; 0 when absent, as proto3 omits it.
    private static long ReadTermFromWire(ReadOnlySpan<byte> data)
    {
        int position = 0;
        while (position < data.Length)
        {
            ulong tag = ReadVarint(data, ref position);
            int field = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);
            if (field == 3 && wireType == 0)
                return (long)ReadVarint(data, ref position);

            switch (wireType)
            {
                case 0: ReadVarint(data, ref position); break;
                case 1: position += 8; break;
                case 2: SkipLengthDelimited(data, ref position); break;
                case 5: position += 4; break;
                default: return 0;
            }
        }

        return 0;
    }

    // The scalar header fields (partition, id, term, type), skipping the length-delimited ones.
    private static void ReadHeaderFromWire(ReadOnlySpan<byte> data, out int partition, out long id, out long term, out int type)
    {
        partition = 0;
        id = 0;
        term = 0;
        type = 0;
        int position = 0;
        while (position < data.Length)
        {
            ulong tag = ReadVarint(data, ref position);
            int field = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);
            if (wireType == 0)
            {
                long value = (long)ReadVarint(data, ref position);
                switch (field)
                {
                    case 1: partition = (int)value; break;
                    case 2: id = value; break;
                    case 3: term = value; break;
                    case 4: type = (int)value; break;
                }

                continue;
            }

            switch (wireType)
            {
                case 1: position += 8; break;
                case 2: SkipLengthDelimited(data, ref position); break;
                case 5: position += 4; break;
                default: return;
            }
        }
    }

    // Every field, into a view over data. A truncated or malformed value throws: a torn entry must not
    // read as a record of defaults.
    private static void ReadLogFromWire(ReadOnlySpan<byte> data, out RaftLogWireView view)
    {
        view = default;
        int position = 0;
        while (position < data.Length)
        {
            ulong tag = ReadVarint(data, ref position);
            int field = (int)(tag >> 3);
            int wireType = (int)(tag & 0x7);
            switch (wireType)
            {
                case 0:
                {
                    ulong value = ReadVarint(data, ref position);
                    switch (field)
                    {
                        case 1: view.Partition = (int)value; break;
                        case 2: view.Id = (long)value; break;
                        case 3: view.Term = (long)value; break;
                        case 4: view.Type = (int)value; break;
                        case 7: view.TimeNode = (int)value; break;
                        case 8: view.TimePhysical = (long)value; break;
                        case 9: view.TimeCounter = (uint)value; break;
                    }

                    break;
                }

                case 2:
                {
                    int length = (int)ReadVarint(data, ref position);
                    if (length < 0 || position + length > data.Length)
                        throw new InvalidDataException($"Truncated RaftLogMessage: field {field} declares {length} bytes but only {data.Length - position} remain.");

                    if (field == 5)
                        view.LogType = data.Slice(position, length);
                    else if (field == 6)
                    {
                        view.Log = data.Slice(position, length);
                        view.HasLog = true;
                    }

                    position += length;
                    break;
                }

                case 1:
                    if (position + 8 > data.Length)
                        throw new InvalidDataException("Truncated RaftLogMessage: incomplete 64-bit field.");

                    position += 8;
                    break;

                case 5:
                    if (position + 4 > data.Length)
                        throw new InvalidDataException("Truncated RaftLogMessage: incomplete 32-bit field.");

                    position += 4;
                    break;

                default:
                    throw new InvalidDataException($"Malformed RaftLogMessage: unsupported wire type {wireType} for field {field}.");
            }
        }
    }

    // The RaftLog of a view, with its effective type: a Proposed row at or below the commit frontier is Committed.
    private static RaftLog ToRaftLog(in RaftLogWireView view, long commitFrontier)
    {
        RaftLogType type = (RaftLogType)view.Type;
        if (type == RaftLogType.Proposed && view.Id <= commitFrontier)
            type = RaftLogType.Committed;

        return new RaftLog
        {
            Id = view.Id,
            Term = view.Term,
            Type = type,
            Time = new(view.TimeNode, view.TimePhysical, view.TimeCounter),
            LogType = InternLogType(view.LogType),
            LogData = view.HasLog ? (view.Log.IsEmpty ? [] : view.Log.ToArray()) : null,
        };
    }

    // Log types are a handful of names, so a per-thread cache of their UTF-8 bytes turns one string per
    // entry into one per distinct type; an absent log type reads as empty, as the generated parser does.
    private static string InternLogType(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return "";

        (byte[] Utf8, string Value)[] cache = t_logTypeCache ??= new (byte[], string)[LogTypeCacheSize];
        for (int i = 0; i < t_logTypeCacheCount; i++)
            if (utf8.SequenceEqual(cache[i].Utf8))
                return cache[i].Value;

        string value = Encoding.UTF8.GetString(utf8);
        cache[t_logTypeCacheCursor] = (utf8.ToArray(), value);
        t_logTypeCacheCursor = (t_logTypeCacheCursor + 1) % LogTypeCacheSize;
        if (t_logTypeCacheCount < LogTypeCacheSize)
            t_logTypeCacheCount++;

        return value;
    }

    // ── Keys ─────────────────────────────────────────────────────────────────────────────────────

    private static void BuildLogKey(Span<byte> result, int partitionId, long logId)
    {
        ToDecimalBytes(result[..PartitionIdWidth], partitionId);
        result[PartitionIdWidth] = LogKeySeparator;
        ToDecimalBytes(result[(PartitionIdWidth + 1)..LogKeyWidth], logId);
    }

    // The bound above every log key of the partition and below every key of the next.
    private static void BuildPartitionUpperBoundKey(Span<byte> result, int partitionId)
    {
        ToDecimalBytes(result[..PartitionIdWidth], partitionId);
        result[PartitionIdWidth] = PartitionUpperBoundSeparator;
    }

    private static void BuildPartitionPrefix(Span<byte> prefix, int partitionId)
    {
        ToDecimalBytes(prefix[..PartitionIdWidth], partitionId);
        prefix[PartitionIdWidth] = LogKeySeparator;
    }

    private static bool KeyBelongsToPartition(ReadOnlySpan<byte> key, int partitionId)
    {
        Span<byte> prefix = stackalloc byte[PartitionPrefixWidth];
        BuildPartitionPrefix(prefix, partitionId);
        return key.StartsWith(prefix);
    }

    // The key alone carries the id, so scans that need only it read no value.
    private static long ParseLogIdFromKey(ReadOnlySpan<byte> key)
    {
        long id = 0;
        for (int i = PartitionIdWidth + 1; i < key.Length; i++)
            id = (id * 10) + (key[i] - (byte)'0');

        return id;
    }

    private static void SeekToLastPartitionKey(Iterator iterator, int partitionId)
    {
        Span<byte> upperBound = stackalloc byte[PartitionIdWidth + 1];
        BuildPartitionUpperBoundKey(upperBound, partitionId);
        iterator.Seek(upperBound);
        if (iterator.Valid())
            iterator.Prev();
        else
            iterator.SeekToLast();
    }

    // Fixed-width decimal, left-padded with '0', so keys sort by number.
    private static void ToDecimalBytes(Span<byte> result, long value)
    {
        result.Fill((byte)'0');
        int position = result.Length - 1;
        do
        {
            result[position--] = (byte)('0' + (value % 10));
            value /= 10;
        }
        while (value > 0);
    }

    // An iterator that stops below the log key (partition, upperIdExclusive): a seek into a run of
    // tombstones stops at the bound instead of skipping past the caller's range. fillCache false is for
    // scans of rows no hot path reads again, such as the dead prefix a compaction pass counts: with a
    // block cache, the indexes they read must not evict the ones the tail reads need, as RocksDbWAL's.
    private Iterator NewBoundedIterator(ColumnFamily family, int partitionId, long upperIdExclusive, bool fillCache = true)
    {
        byte[] bound = new byte[LogKeyWidth];
        BuildLogKey(bound, partitionId, upperIdExclusive);
        return _db.NewIterator(family, new ReadOptions { IterateUpperBound = bound, FillCache = fillCache });
    }

    // An iterator that stops at the end of the partition, never walking into the next one of a shared shard.
    // See NewBoundedIterator for fillCache.
    private Iterator NewPartitionBoundedIterator(ColumnFamily family, int partitionId, bool fillCache = true)
    {
        byte[] bound = new byte[PartitionIdWidth + 1];
        BuildPartitionUpperBoundKey(bound, partitionId);
        return _db.NewIterator(family, new ReadOptions { IterateUpperBound = bound, FillCache = fillCache });
    }

    private ColumnFamily Shard(int partitionId) => _shards[partitionId % MaxShards];

    // True when no family of the database holds a key.
    private bool IsEmpty()
    {
        foreach (ColumnFamily family in _db.ColumnFamilies)
        {
            using Iterator iterator = _db.NewIterator(family);
            iterator.SeekToFirst();
            if (iterator.Valid())
                return false;
        }

        return true;
    }

    private static (ColumnFamily Metadata, ColumnFamily[] Shards) Families(SolariumDb db)
    {
        ColumnFamily[] shards = new ColumnFamily[MaxShards];
        for (int i = 0; i < MaxShards; i++)
            shards[i] = db.GetColumnFamily("shard" + i);

        return (db.GetColumnFamily("metadata"), shards);
    }

    // ── Engine recovery after a storage failure ──────────────────────────────────────────────────
    //
    // A failed write stops every later write of a Solarium database until it is opened again. After a
    // failure the WAL asks whether the file system accepts a small durable write (a probe file) and whether
    // the database does (a canary put into the metadata family). File system yes and database no is a
    // latched database, and only then is it closed and opened again, under the exclusive engine guard;
    // the operation is retried once. A still-full disk fails the probe, so writes keep reporting Errored
    // and the Raft layer retries on its own cadence. A reopen loses nothing acknowledged: synced writes
    // were flushed, and unsynced ones reached the OS before they were acknowledged.

    private EngineLease AcquireEngine()
    {
        EngineLease lease = new(_engineGuard);
        if (!_engineClosed)
            return lease;

        lease.Dispose();
        throw new RaftException($"Solarium WAL at '{_enginePath}' is closed after a storage failure and has not been reopened yet; the next write retries the reopen.");
    }

    private void NoteStorageFailure(Exception e)
    {
        Volatile.Write(ref _lastStorageFailure, e);
        long now = _tickSource.GetTimestamp();
        int streak = Interlocked.Increment(ref _failureStreak);
        if (streak == 1)
        {
            Volatile.Write(ref _failureStreakStartTicks, now);
            Volatile.Write(ref _lastFailureLogTicks, now);
            _logger.LogWarning(
                "Solarium WAL at '{Path}' rejected a write: {Message}. Raft frontiers regress to the durable log; the WAL probes the file system on each failure and reopens the database once space is back",
                _enginePath, e.Message);
            return;
        }

        if (_tickSource.GetElapsedTime(Volatile.Read(ref _lastFailureLogTicks), now) < SustainedFailureLogInterval)
            return;

        Volatile.Write(ref _lastFailureLogTicks, now);
        _logger.LogWarning(
            "Solarium WAL at '{Path}' has rejected writes for {Duration} ({Count} failures); last error: {Message}",
            _enginePath, _tickSource.GetElapsedTime(Volatile.Read(ref _failureStreakStartTicks), now), streak, e.Message);
    }

    private void NoteStorageSuccess()
    {
        if (Volatile.Read(ref _failureStreak) == 0)
            return;

        int streak = Interlocked.Exchange(ref _failureStreak, 0);
        if (streak > 0 && _logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation(
                "Solarium WAL at '{Path}' accepts writes again after {Count} failures over {Duration}",
                _enginePath, streak, _tickSource.GetElapsedTime(Volatile.Read(ref _failureStreakStartTicks), _tickSource.GetTimestamp()));
    }

    // Called without the engine lease after a write path returned Errored. True when the caller should
    // retry once: the database was reopened, or it accepts writes again.
    private bool TryRecoverEngineAfterFailure()
    {
        // A managed exception (a bug, an invalid argument) is not a storage fault.
        Exception? failure = Volatile.Read(ref _lastStorageFailure);
        if (failure is not (SolariumException or IOException) && !_engineClosed)
            return false;

        if (!FilesystemAcceptsWrites())
            return false;

        _engineGuard.EnterWriteLock();
        try
        {
            if (!_engineClosed && EngineAcceptsWrites())
            {
                _logger.LogInformation("Solarium WAL at '{Path}' accepts writes again without a reopen", _enginePath);
                return true;
            }

            return ReopenEngine(failure);
        }
        finally
        {
            _engineGuard.ExitWriteLock();
        }
    }

    // Can the volume take a small durable write now? At most one probe per interval across all callers.
    // The probe goes through the database's storage environment, as every file of the database does.
    private bool FilesystemAcceptsWrites()
    {
        long now = _tickSource.GetTimestamp();
        long last = Volatile.Read(ref _lastProbeTicks);
        if ((last != 0 && _tickSource.GetElapsedTime(last, now) < ProbeInterval) || Interlocked.CompareExchange(ref _lastProbeTicks, now, last) != last)
            return false;

        IStorageEnvironment environment = _dbOptions.Environment;
        string probePath = Path.Combine(_enginePath, SpaceProbeFileName);
        try
        {
            using (IStorageFile file = environment.OpenFile(probePath))
            {
                file.Write(new byte[SpaceProbeBytes], 0);
                file.Flush();
            }

            return true;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            return false;
        }
        finally
        {
            try
            {
                if (environment.FileExists(probePath))
                    environment.DeleteFile(probePath);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
            }
        }
    }

    // Does the database accept a durable write now? A latched one refuses at once. Under the exclusive guard.
    private bool EngineAcceptsWrites()
    {
        try
        {
            _db.Put(EngineCanaryKey, BitConverter.GetBytes(_tickSource.GetTimestamp()), _metadata, WriteOptions.Synced);
            return true;
        }
        catch (SolariumException)
        {
            return false;
        }
    }

    // Closes the database and opens it again with the same options, under the exclusive guard. A failed
    // open leaves the WAL closed, and the next failed write tries again; the directory lock allows one
    // open database per directory, so the old one cannot stay as a fallback.
    private bool ReopenEngine(Exception? cause)
    {
        if (!_engineClosed)
        {
            _engineClosed = true;

            // The caches describe the database being closed; the reopened one reloads them from metadata.
            _commitFrontierCache.Clear();
            _compactionFloorCache.Clear();
            _maxLogCache.Clear();
            try
            {
                _db.Dispose();
            }
            catch (Exception e)
            {
                _logger.LogWarning("Solarium WAL at '{Path}': the close before the reopen reported {Message}", _enginePath, e.Message);
            }
        }

        try
        {
            _db = SolariumDb.Open(_enginePath, _dbOptions, _familyNames);
            (_metadata, _shards) = Families(_db);
            _engineClosed = false;
            EngineReopenCount++;
            SolariumWalMetrics.RecordEngineReopen(succeeded: true);
            _logger.LogWarning(
                "Solarium WAL at '{Path}' was reopened to clear a storage failure (reopen #{Count}); the failure was: {Message}",
                _enginePath, EngineReopenCount, cause?.Message ?? "<closed>");
            return true;
        }
        catch (Exception e)
        {
            SolariumWalMetrics.RecordEngineReopen(succeeded: false);
            _logger.LogCritical("Solarium WAL at '{Path}' could not be reopened: {Message}", _enginePath, e.Message);
            return false;
        }
    }

    // ── Diagnostics and tests ────────────────────────────────────────────────────────────────────

    /// <summary>For tests: switches every family's memtable to a flush, waiting for the tables when <paramref name="wait"/> is set.</summary>
    internal void FlushMemTablesForTesting(bool wait = false)
    {
        using EngineLease lease = AcquireEngine();
        foreach (ColumnFamily family in _db.ColumnFamilies)
            _db.Flush(family, wait);
    }

    /// <summary>For tests: compacts the shard of <paramref name="partitionId"/> into its sorted level.</summary>
    internal void CompactShardToBottomForTesting(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        _db.CompactRange(null, null, Shard(partitionId));
    }

    /// <summary>For tests: the open database, for the engine's own diagnostics. Replaced by a reopen.</summary>
    internal SolariumDb DatabaseForTesting => _db;

    /// <summary>For tests: the persisted compaction floor of <paramref name="partitionId"/>.</summary>
    internal long GetCompactionFloorForTesting(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        return GetCompactionFloor(partitionId);
    }

    /// <summary>For tests: rewrites the persisted commit frontier, as a crash that lost its put leaves it; at or below 0 it is removed.</summary>
    internal void RegressCommitFrontierForTesting(int partitionId, long value)
    {
        using EngineLease lease = AcquireEngine();
        if (value <= 0)
            _db.Delete(CommitFrontierKey(partitionId), _metadata, _writeOptions);
        else
            _db.Put(CommitFrontierKey(partitionId), Utf8(value), _metadata, _writeOptions);

        _commitFrontierCache[partitionId] = Math.Max(value, 0);
    }

    /// <summary>For tests: the live tables of the shard of <paramref name="partitionId"/>, with the id span and size of each.</summary>
    internal List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> GetShardLiveFilesForTesting(int partitionId)
    {
        using EngineLease lease = AcquireEngine();
        string shard = "shard" + (partitionId % MaxShards);
        return [.. _db.GetLiveFilesMetaData()
            .Where(f => f.ColumnFamilyName == shard)
            .Select(f => (f.Name, f.Level, ParseLogIdFromKey(f.SmallestKey), ParseLogIdFromKey(f.LargestKey), f.Size))];
    }

    /// <summary>The bytes of the live SSTables of the shard families, for the <c>raft.wal.shard_live_sst_bytes</c> gauge; 0 when closed.</summary>
    internal long GetShardLiveSstBytes() => SumShardProperty("rocksdb.live-sst-files-size");

    /// <summary>The level 0 files of the shard families; 0 when closed.</summary>
    internal long GetShardLevel0FileCount() => SumShardProperty("rocksdb.num-files-at-level0");

    /// <summary>The level 6 files of the shard families; 0 when closed.</summary>
    internal long GetShardLevel6FileCount() => SumShardProperty("rocksdb.num-files-at-level6");

    /// <summary>The bytes of the memtables of every family; 0 when closed.</summary>
    internal long GetMemtableMemoryBytes() => SumProperty("rocksdb.cur-size-all-mem-tables", shardsOnly: false);

    /// <summary>The cache of the tables' index and filter blocks, or null when they stay resident.</summary>
    internal Cache? BlockCache => _dbOptions.DefaultColumnFamilyOptions.BlockCache;

    /// <summary>The bytes the open tables of every family hold in memory; 0 when closed.</summary>
    internal long GetTableReadersMemoryBytes() => SumProperty("rocksdb.estimate-table-readers-mem", shardsOnly: false);

    /// <summary>The delayed write rate while writes are slowed, else 0.</summary>
    internal long GetActualDelayedWriteRate() => SumProperty("rocksdb.actual-delayed-write-rate", shardsOnly: true, first: true);

    /// <summary>1 while writes are stopped, else 0.</summary>
    internal long GetIsWriteStopped() => SumProperty("rocksdb.is-write-stopped", shardsOnly: true, first: true);

    /// <summary>The bytes of the WAL files in the database directory, which a reopen replays. Never throws.</summary>
    internal long GetAliveWriteAheadLogBytes()
    {
        long total = 0;
        try
        {
            foreach (FileInfo file in new DirectoryInfo(_enginePath).EnumerateFiles("*.log", SearchOption.TopDirectoryOnly))
                try
                {
                    total += file.Length;
                }
                catch (IOException)
                {
                }
        }
        catch (Exception)
        {
        }

        return total;
    }

    private long SumShardProperty(string name) => SumProperty(name, shardsOnly: true);

    // A sum over the families of an integer property; 0 for a closed or failing database, which is no
    // evidence either way. A database-wide property is read once (first).
    private long SumProperty(string name, bool shardsOnly, bool first = false)
    {
        if (_engineClosed)
            return 0;

        try
        {
            using EngineLease lease = AcquireEngine();
            long total = 0;
            foreach (ColumnFamily family in shardsOnly ? _shards : _db.ColumnFamilies)
            {
                if (long.TryParse(_db.GetProperty(name, family), NumberStyles.None, CultureInfo.InvariantCulture, out long value))
                    total += value;

                if (first)
                    break;
            }

            return total;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    /// <summary>Stops the WAL's <see cref="SolariumWalMetrics"/> gauges, closes the database and releases the directory.</summary>
    public void Dispose()
    {
        // Before the database closes, so no gauge reads it while it does.
        SolariumWalMetrics.Unregister(this);
        _engineGuard.EnterWriteLock();
        try
        {
            if (!_engineClosed)
            {
                _engineClosed = true;
                _db.Dispose();
            }
        }
        finally
        {
            _engineGuard.ExitWriteLock();
        }

        _writeGuard.Dispose();
        _engineGuard.Dispose();
    }

    private static void ValidateRevision(string revision)
    {
        if (string.IsNullOrEmpty(revision))
            return;

        if (revision.Contains('/') || revision.Contains('\\'))
            throw new RaftException($"WAL revision '{revision}' contains a directory separator; it is one path component appended to the WAL directory.");

        if (revision is "." or ".." || revision.Contains("..", StringComparison.Ordinal))
            throw new RaftException($"WAL revision '{revision}' contains a relative path segment; it is one path component appended to the WAL directory.");

        foreach (char c in revision)
            if (Array.IndexOf(Path.GetInvalidFileNameChars(), c) >= 0)
                throw new RaftException($"WAL revision '{revision}' contains a character that is not valid in a file name (U+{(int)c:X4}).");
    }

    // Owner-only on Unix: the directory holds the replicated state.
    private static void EnsureDirectory(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            Directory.CreateDirectory(path);
        else
            Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }

    /// <summary>The shared side of the engine guard, released by <c>using</c>; a struct, so it allocates nothing.</summary>
    private readonly struct EngineLease : IDisposable
    {
        private readonly ReaderWriterLockSlim _guard;

        /// <summary>Takes the shared side of <paramref name="guard"/>.</summary>
        public EngineLease(ReaderWriterLockSlim guard)
        {
            _guard = guard;
            guard.EnterReadLock();
        }

        /// <summary>Releases it.</summary>
        public void Dispose() => _guard.ExitReadLock();
    }

    /// <summary>One partition's cached highest physically present log id.</summary>
    private sealed class PartitionMaxLog(long value)
    {
        /// <summary>The id; raised with a compare-and-swap.</summary>
        public long Value = value;
    }

    /// <summary>A borrowed view of a <c>RaftLogMessage</c>: its spans point into the iterator's value, valid until the next move.</summary>
    private ref struct RaftLogWireView
    {
        public int Partition;
        public long Id;
        public long Term;
        public int Type;
        public ReadOnlySpan<byte> LogType;
        public ReadOnlySpan<byte> Log;
        public bool HasLog;
        public int TimeNode;
        public long TimePhysical;
        public uint TimeCounter;
    }

    /// <summary>The headers a prefetch read for a sorted set of ids; an id outside the set is not answered. Owns pooled arrays.</summary>
    private sealed class MarkerHeaderPrefetch : IDisposable
    {
        private readonly int _count;
        private long[]? _ids;
        private (bool Found, long Term, int Type)[]? _headers;

        /// <summary>Takes <paramref name="ids"/>, sorted and distinct in its first <paramref name="count"/> slots; every header starts absent.</summary>
        public MarkerHeaderPrefetch(long[] ids, int count)
        {
            _ids = ids;
            _count = count;
            _headers = ArrayPool<(bool Found, long Term, int Type)>.Shared.Rent(count);
            _headers.AsSpan(0, count).Clear();
        }

        /// <summary>Records the header of the row at <paramref name="position"/> of the sorted ids.</summary>
        public void Set(int position, long term, int type) => _headers![position] = (true, term, type);

        /// <summary>The header for <paramref name="logId"/>, when the prefetch covers it.</summary>
        public bool TryGet(long logId, out (bool Found, long Term, int Type) header)
        {
            int position = Array.BinarySearch(_ids!, 0, _count, logId);
            header = position >= 0 ? _headers![position] : default;
            return position >= 0;
        }

        /// <summary>Returns the pooled arrays.</summary>
        public void Dispose()
        {
            if (_ids is not null)
            {
                ArrayPool<long>.Shared.Return(_ids);
                _ids = null;
            }

            if (_headers is not null)
            {
                ArrayPool<(bool Found, long Term, int Type)>.Shared.Return(_headers);
                _headers = null;
            }
        }
    }
}
