using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;
using Solarium.Engine;
using Solarium.Storage;

namespace Solarium;

/// <summary>
/// An embedded key-value database in one directory, with an API shaped like RocksDB's. Every
/// member is safe to call from any thread.
/// </summary>
/// <remarks>
/// <para><b>Writes</b> use group commit. Concurrent writers queue up; the first one becomes the
/// leader, takes the queue up to <see cref="DbOptions.MaxWriteBatchGroupSizeBytes"/>, and writes it to the WAL in one append (and one flush if any
/// writer asked for <see cref="WriteOptions.Sync"/>). Commits are pipelined
/// (<see cref="DbOptions.EnablePipelinedWrite"/>, on by default): the leader then offers leadership
/// to the oldest writer in the queue, so the next group appends while this one goes into the
/// memtables. One memtable stage runs at a time: a group that finds none running starts one, and
/// the groups appended meanwhile queue up in WAL order for the next stage, which takes them all.
/// A stage inserts its groups into the memtables, publishes their last sequence number, and
/// completes each writer. When <see cref="DbOptions.AllowConcurrentMemtableWrite"/> is on, each
/// writer of a stage of several that still spins inserts its own batch at the same time as the
/// stage's leader, as RocksDB's <c>allow_concurrent_memtable_write</c>, and the leader inserts the
/// batches of the writers that blocked. With pipelining off, the leader inserts and publishes its
/// group before it offers leadership.</para>
/// <para><b>Reads</b> take no lock. A read counts itself into the column family's current version
/// (its memtables and SSTables), then takes the last published sequence number as its snapshot, so
/// it sees every batch of a group or none of it, across all column families. A table stays open
/// while a version that lists it has readers.</para>
/// <para><b>Flushes.</b> A full memtable becomes immutable and a background thread writes it to an
/// SSTable. The manifest then records the table, and a new version replaces the memtable with it.
/// A WAL file is deleted when no memtable holds data from it.</para>
/// <para><b>Compactions.</b> A second background thread moves level 0 SSTables to level 6, the
/// bottom level, whose files are sorted and share no key: as they are when they overlap nothing,
/// or else merged with the tables they overlap, which drops the versions that no reader can see. A
/// table that leaves the manifest is deleted when the last version that lists it closes. While a
/// family's level 0 is past its slowdown or stop trigger, the WAL leader delays or stops every
/// write group until compaction catches up.</para>
/// </remarks>
public sealed class SolariumDb : IDisposable
{
    private const string FormatFileName = "FORMAT";
    private const string FormatVersion = "solarium-format 5";
    private const string FormatVersion4 = "solarium-format 4";
    private const string FormatVersion3 = "solarium-format 3";
    private const string FormatVersion2 = "solarium-format 2";
    private const string FormatVersion1 = "solarium-format 1";

    private readonly string _directory;
    private readonly FileStream _lock;
    private readonly IStorageEnvironment _environment;
    private readonly Manifest _manifest;
    private readonly long _maxTotalWalSize;
    private readonly Dictionary<string, ColumnFamily> _byName = new(StringComparer.Ordinal);
    private readonly ColumnFamily?[] _byId;   // a gap is an id that is no longer in use

    // The write path.
    private readonly object _writeLock = new();
    private readonly List<Writer> _queue = [];
    private ArrayBufferWriter<byte> _records = new();   // leader only
    private long _lastGroupBytes;                        // leader only: the bytes the last group appended
    private readonly long _maxGroupBytes;
    private readonly HashSet<ColumnFamily> _written = [];        // leader only
    private bool _leaderActive;
    private bool _activeGroupSyncs;   // under _writeLock: the active leader's group flushes the WAL
    private bool _queueSyncs;         // under _writeLock: a writer in the queue asked for a flush
    private bool _disposed;
    private long _lastSequence;
    private long _writeGroups;
    private readonly long[] _groupSizes = new long[MaxCountedGroupSize + 1];   // leader increments

    // The WAL record of the batch that this thread writes. A writer encodes its batch into it before
    // it joins a group, and stays in Write until its group completes, so the leader reads it only
    // while its writer waits, and a thread needs one at a time. A larger record gets an array of its
    // own, so one large batch does not keep its memory on the thread.
    [ThreadStatic]
    private static byte[]? t_record;
    private const int MaxKeptRecord = 64 * 1024;
    private readonly long _writerSpinTicks;        // Stopwatch ticks
    private readonly long _writerShortSpinTicks;   // Stopwatch ticks, at most _writerSpinTicks
    private readonly int _spinProbeInterval;       // 0: no probes
    private readonly WriterSpinCredit _spinCredit = new();
    private int _turnsToProbe;       // under _writeLock: below 0 after the decrement, a probe or a window starts
    private int _probeWindowLeft;    // under _writeLock: turns of the current window of probes
    private long _spinProbes;        // under _writeLock
    private long _shortSpins;        // under _writeLock
    private long _probeBlocks;       // Interlocked: the probe that blocks counts itself, outside the lock
    private long _writerWaits;       // under _writeLock
    private long _flushWaits;        // under _writeLock
    private long _blockedWaits;      // Interlocked: the writer that blocks counts itself, outside the lock
    private readonly long[] _spinWaitTimes = new long[Environment.ProcessorCount * WaitTimeStride];   // Interlocked, a row per CPU
    private long _overtakenOffers;   // under _writeLock

    // The inserts of a group of several writers. The leader resets them before it launches the
    // members, and the members only count down and record a failure.
    private readonly bool _concurrentInserts;
    private int _insertsRunning;   // Interlocked: the writers of the group that have not finished their insert
    private Exception? _insertFailure;   // the first insert of the group that failed
    // Never disposed: the last member to finish can still set it after the leader saw the count
    // reach 0 and the database closed, and it holds no handle until a WaitHandle is asked for.
    private readonly ManualResetEventSlim _insertsDone = new(initialState: false, spinCount: 0);
    private readonly List<Writer> _keptMembers = new();   // the leader's: members whose batches it inserts
    private long _parallelGroups;     // Interlocked, by the leader
    private long _launchedInserts;    // Interlocked, by the leader
    private long _keptInserts;        // Interlocked, by the leader
    private long _insertRetries;      // Interlocked
    private long _blocksAfterInsert;  // Interlocked: a member that blocks after its insert counts itself

    // The time of each stage of a group, in Stopwatch ticks; see DbOptions.MeasureGroupStages. The
    // leader writes them while it leads, and _writeLock hands them to the next leader.
    private const int PrepareStage = 0;
    private const int EncodeStage = 1;
    private const int AppendStage = 2;
    private const int MemTableStage = 3;
    private const int HandOffStage = 4;
    private const int GapStage = 5;
    private const int TurnStage = 6;
    private readonly bool _measureStages;
    private readonly long[] _stageTicks = new long[TurnStage + 1];
    private long _handOffFrom;     // when the leader's work on the path of the next group ended
    private long _groupEndedAt;    // when the last group gave up its leadership; 0 before the first

    // A pipelined commit (DbOptions.EnablePipelinedWrite): the WAL stage of a group runs while earlier
    // groups are in a memtable stage. A group whose append succeeds takes a turn, and, while a stage
    // runs, joins the memtable queue in WAL order. One memtable stage runs at a time. A group that
    // finds none running starts one with itself alone; the leader of the oldest queued group starts
    // the next with the whole queue. A stage inserts and publishes its groups and offers the next
    // stage, as RocksDB's memtable writer groups. So groups are published in WAL order, and the
    // groups that wait for a stage need one wake together, not one each.
    private readonly bool _pipelined;
    private readonly Action<long>? _beforeMemTableStage;
    private readonly Action<long>? _afterWalLeadership;
    private readonly TablePacing _flushPacing;        // flush writes: the rate limiter's high priority
    private readonly TablePacing _compactionPacing;   // compaction writes: its low priority
    // The memtables of the database are charged to it after each group's inserts, before the group is
    // published, and released when their flush installs; null: no cap across column families.
    private readonly WriteBufferManager? _writeBufferManager;

    // The native chunks of the memtables' arenas: the WriteBufferManager's, shared with the databases that share it
    // and disposed at the close when the manager is this database's own (DbWriteBufferSize), or the pool of the
    // databases without one, whose limit this database raises by _sharedPoolBytes while it is open.
    private readonly ArenaChunkPool _chunkPool;
    private readonly bool _ownsChunkPool;
    private readonly long _sharedPoolBytes;

    // The owner that keys this database's tables in a block cache it shares with other databases.
    private readonly long _cacheOwner = Cache.NewOwner();
    private readonly List<Cache> _blockCaches = [];   // under _tablesLock: every block cache a table of this database used
    private readonly Action<long>? _beforeMemTableInsert;
    private readonly Action? _afterReadVersion;
    private readonly Action? _afterManifestSave;
    private readonly object _memTableQueueLock = new();
    private List<Writer>? _memTableQueue;   // under _memTableQueueLock: the writers of the waiting groups, in WAL order
    private Writer? _memTableQueueLeader;   // under _memTableQueueLock: the leader of the oldest waiting group
    private GroupRange _memTableQueueRange; // under _memTableQueueLock: the sequence numbers of the waiting groups, the newest turn
    private bool _memTableLeaderActive;     // under _memTableQueueLock: a stage runs, or its leader was offered it
    private long _memTableStages;           // the stage's leader; the queue lock orders the stages
    private long _memTableQueueBlocks;      // Interlocked: a group's leader that blocks in the queue counts itself
    // Held through a memtable stage. A switch takes it to install a new memtable, so all the
    // entries of a stage go to one memtable. Taken after _stateLock, never before it.
    private readonly object _memTableLock = new();
    private long _lastAssigned;     // the WAL leader: the last sequence number given to a group
    private long _assignedTurn;     // the WAL leader: the turn of the last group appended
    private long _publishedTurn;    // under _memTableLock: the turn of the newest group that a memtable stage ended
    private bool _memTableFailed;   // under _memTableLock: a memtable stage failed, so no later group publishes
    private int _stageWaiters;      // under _memTableLock: the threads in WaitForMemTableStages

    // A WAL or flush failure; after it, writes fail and reads continue.
    private Exception? _failure;

    // Versions, WAL files, the manifest and the flush queue: under _stateLock.
    private readonly object _stateLock = new();
    private readonly List<WalFile> _walFiles = [];   // oldest first; the last one is the current one
    private readonly Queue<(ColumnFamily Family, MemTable MemTable)> _flushQueue = new();
    private readonly Thread _flushThread;
    private bool _rotateWal;
    private bool _stopping;
    private long _flushCount;
    private bool _flushRunning;   // the flush thread writes the memtable at the head of _flushQueue
    private long _flushBytesWritten;

    // Compactions, up to MaxBackgroundCompactions at once, one thread each. A thread picks and installs
    // under _stateLock and merges outside it; _running keeps two compactions off one file or range.
    private readonly Thread[] _compactionThreads;
    private readonly CancellationTokenSource _closing = new();   // cancelled by Dispose: a merge or a checkpoint stops at its next check
    private int _checkpoints;   // under _stateLock: checkpoints that hold versions; Dispose waits for them
    private readonly Queue<ManualCompaction> _manualCompactions = new();   // under _stateLock
    private readonly List<Compaction> _running = [];   // under _stateLock: the compactions that run, whose tables no other compaction or DeleteFilesInRange takes
    private int _mostRunning;         // under _stateLock: the most compactions that ran at once
    private long _conflictedPicks;    // under _stateLock: compactions not started because a running one held a file or a range they needed
    private long _moves;              // under _stateLock
    private long _movedFiles;         // under _stateLock
    private long _merges;             // under _stateLock
    private long _mergeBytesRead;     // under _stateLock
    private long _mergeBytesWritten;  // under _stateLock
    private long _droppedFiles;       // under _stateLock
    private long _memTableWaits;      // under _stateLock: switches that waited for a flush
    private long _memTableWaitTicks;  // under _stateLock: the Stopwatch ticks they waited
    private long _bufferManagerSwitches;   // under _stateLock: switches the WriteBufferManager asked for
    private long _multiGetRounds;     // Interlocked: the rounds of MultiGet that read data blocks
    private readonly Action? _beforeCompaction;
    private readonly Action<Compaction>? _compactionStarting;

    // Write stalls, as RocksDB's write controller: a family whose level 0 is past a trigger slows or
    // stops the writes to every family. The WAL leader applies them before it appends its group.
    // _writeStall is written under _stateLock after each change of a level 0, and is volatile so a
    // leader that sleeps out a delay without the lock sees the delay end.
    private volatile WriteStallCondition _writeStall;
    private readonly long _delayedWriteRate;   // bytes per second
    private long _delayCredit;        // under _stateLock: the bytes groups may write before the next refill
    private long _nextRefill;         // under _stateLock: the Stopwatch timestamp of the next refill; 0 when a delay starts
    private long _delayedGroups;      // under _stateLock
    private long _stoppedGroups;      // under _stateLock
    private long _stallTicks;         // under _stateLock

    // Every open SSTable, so Dispose closes the tables of retired versions that readers still hold.
    // A table leaves when the last version that lists it closes, on that version's closing thread.
    private readonly object _tablesLock = new();
    private readonly HashSet<TableReader> _openTables = [];   // under _tablesLock
    private bool _tablesClosed;                               // under _tablesLock
    private readonly Action<TableReader> _tableUnused;

    private SolariumDb(string directory, FileStream lockFile, Manifest manifest, DbOptions options)
    {
        _directory = directory;
        _lock = lockFile;
        _environment = options.Environment;
        _writerSpinTicks = (long)(options.WriterSpin.TotalSeconds * Stopwatch.Frequency);
        _writerShortSpinTicks = Math.Min(_writerSpinTicks, (long)(options.WriterShortSpin.TotalSeconds * Stopwatch.Frequency));
        _spinProbeInterval = Math.Max(0, options.WriterSpinProbeInterval);
        _concurrentInserts = options.AllowConcurrentMemtableWrite;
        _measureStages = options.MeasureGroupStages;
        _pipelined = options.EnablePipelinedWrite;
        _beforeMemTableStage = options.BeforeMemTableStage;
        _afterWalLeadership = options.AfterWalLeadership;
        long bytesPerSync = options.BytesPerSync > 0 ? options.BytesPerSync : options.RateLimiter is null ? 0 : 1L << 20;
        _flushPacing = new TablePacing(options.RateLimiter, High: true, bytesPerSync);
        _compactionPacing = new TablePacing(options.RateLimiter, High: false, bytesPerSync);
        _writeBufferManager = options.WriteBufferManager ?? (options.DbWriteBufferSize > 0 ? new WriteBufferManager(options.DbWriteBufferSize) : null);
        _ownsChunkPool = options.WriteBufferManager is null && _writeBufferManager is not null;
        _chunkPool = _writeBufferManager?.ChunkPool ?? ArenaChunkPool.Shared;
        if (_writeBufferManager is null)
        {
            _sharedPoolBytes = MemTableBytes(manifest, options);
            _chunkPool.AddToRetainLimit(_sharedPoolBytes);
        }
        _beforeMemTableInsert = options.BeforeMemTableInsert;
        _afterReadVersion = options.AfterReadVersion;
        _afterManifestSave = options.AfterManifestSave;
        _beforeCompaction = options.BeforeCompaction;
        _compactionStarting = options.CompactionStarting;
        _delayedWriteRate = options.DelayedWriteRate;
        _maxGroupBytes = options.MaxWriteBatchGroupSizeBytes;
        _manifest = manifest;
        _byId = new ColumnFamily?[manifest.ColumnFamilies.Max(f => f.Id) + 1];
        _tableUnused = TableUnused;

        try
        {
            DeleteOrphanFiles();

            foreach (ManifestFamily record in manifest.ColumnFamilies)
            {
                ColumnFamilyOptions familyOptions = options.OptionsFor(record.Name);
                familyOptions.Validate(record.Name);

                (TableReader[] level0, SortedLevel[] levels) = OpenTables(record, familyOptions);
                ColumnFamily family = new(this, record, familyOptions, new FamilyVersion(new MemTable(_chunkPool), [], level0, levels, _tableUnused));
                _byId[record.Id] = family;
                _byName[record.Name] = family;
                _lastSequence = Math.Max(_lastSequence, record.FlushedSequence);
            }

            DefaultColumnFamily = _byName[ColumnFamily.DefaultName];
            _maxTotalWalSize = options.MaxTotalWalSize > 0
                ? options.MaxTotalWalSize
                : 4 * _byName.Values.Sum(f => f.Options.WriteBufferSize * f.Options.MaxWriteBufferNumber);

            ReplayWalFiles();
            UpdateWriteStall();
            _lastAssigned = _lastSequence;
            StartWalFile();
            int obsolete = ObsoleteWalFiles(flushing: null);
            SaveManifest(obsolete);
            DeleteWalFiles(obsolete);
        }
        catch
        {
            ReleaseMemTables();
            CloseFiles();
            CloseVersions();
            throw;
        }

        _flushThread = new Thread(FlushLoop) { Name = "solarium-flush", IsBackground = true };
        _flushThread.Start();
        _compactionThreads = new Thread[options.MaxBackgroundCompactions];
        for (int i = 0; i < _compactionThreads.Length; i++)
        {
            _compactionThreads[i] = new Thread(CompactionLoop) { Name = $"solarium-compaction-{i}", IsBackground = true };
            _compactionThreads[i].Start();
        }
    }

    /// <summary>
    /// Open the database in <paramref name="path"/>. Every column family that exists is opened;
    /// <paramref name="columnFamilies"/> names the ones that must exist, and
    /// <see cref="DbOptions.CreateMissingColumnFamilies"/> decides what happens when one does not.
    /// </summary>
    public static SolariumDb Open(string path, DbOptions? options = null, IEnumerable<string>? columnFamilies = null)
    {
        options ??= new DbOptions();
        if (options.DelayedWriteRate < 1)
            throw new ArgumentException($"DelayedWriteRate ({options.DelayedWriteRate}) must be positive.", nameof(options));

        if (options.MaxWriteBatchGroupSizeBytes < 1)
            throw new ArgumentException($"MaxWriteBatchGroupSizeBytes ({options.MaxWriteBatchGroupSizeBytes}) must be positive.", nameof(options));

        if (options.MaxBackgroundCompactions < 1)
            throw new ArgumentException($"MaxBackgroundCompactions ({options.MaxBackgroundCompactions}) must be positive.", nameof(options));

        if (options.BytesPerSync < 0)
            throw new ArgumentException($"BytesPerSync ({options.BytesPerSync}) must not be negative.", nameof(options));

        if (options.DbWriteBufferSize < 0)
            throw new ArgumentException($"DbWriteBufferSize ({options.DbWriteBufferSize}) must not be negative.", nameof(options));

        string directory = Path.GetFullPath(path);
        IStorageEnvironment environment = options.Environment;

        if (!Directory.Exists(directory))
        {
            if (!options.CreateIfMissing)
                throw new SolariumException($"'{directory}' does not exist, and CreateIfMissing is false.");

            environment.CreateDirectory(directory);
            environment.FlushDirectory(Path.GetDirectoryName(directory)!);
        }

        FileStream lockFile = Lock(directory);
        try
        {
            PrepareFormat(environment, directory);
            Manifest manifest = Manifest.Load(environment, directory) ?? NewManifest(environment, directory);
            AddMissingFamilies(environment, directory, manifest, columnFamilies ?? [], options.CreateMissingColumnFamilies);
            return new SolariumDb(directory, lockFile, manifest, options);
        }
        catch
        {
            lockFile.Dispose();
            throw;
        }
    }

    /// <summary>The column family that operations use when they pass no family. It always exists.</summary>
    public ColumnFamily DefaultColumnFamily { get; } = null!;

    /// <summary>Every column family of the database, the default one included.</summary>
    public IReadOnlyCollection<ColumnFamily> ColumnFamilies => _byName.Values;

    /// <summary>The sequence number of the newest write that reads can see.</summary>
    public long LatestSequenceNumber => Volatile.Read(ref _lastSequence);

    /// <summary>WAL appends so far; lower than the write count when group commit merges writers.</summary>
    internal long WriteGroupCount => Interlocked.Read(ref _writeGroups);

    /// <summary>The largest group size that <see cref="WriteGroupSizes"/> counts on its own.</summary>
    internal const int MaxCountedGroupSize = 64;

    /// <summary>
    /// The groups behind <see cref="WriteGroupCount"/> by the number of writers in them: element n
    /// counts the groups of n writers, and the last element counts every group of
    /// <see cref="MaxCountedGroupSize"/> writers or more. Element 0 is always 0.
    /// </summary>
    internal long[] WriteGroupSizes()
    {
        long[] sizes = new long[_groupSizes.Length];
        
        for (int i = 0; i < sizes.Length; i++)
            sizes[i] = Interlocked.Read(ref _groupSizes[i]);

        return sizes;
    }

    /// <summary>
    /// Writers that joined the queue behind a leader; those of them that waited for a WAL flush and
    /// so did not spin; the times such a writer blocked because its turn did not come within the
    /// spin, so a thread wake had to end the wait; and the offers of leadership that a running writer
    /// overtook by taking the queue first.
    /// </summary>
    internal (long Waits, long FlushWaits, long Blocked, long Overtaken) WriterWaits()
    {
        lock (_writeLock)
        {
            return (_writerWaits, _flushWaits, Interlocked.Read(ref _blockedWaits), _overtakenOffers);
        }
    }

    /// <summary>
    /// The turns that were probes of the long spin (see <see cref="DbOptions.WriterSpinProbeInterval"/>);
    /// those of the probes that blocked, a part of the blocks of <see cref="WriterWaits"/>; the turns
    /// that spun for <see cref="DbOptions.WriterShortSpin"/> because the probes showed that the long
    /// spin did not pay; and the credit now, negative while the spin is short. A wait for a WAL flush
    /// has no spin, so it is in none of the counts.
    /// </summary>
    internal (long Probes, long ProbeBlocks, long ShortSpins, int Credit) WriterSpinTurns()
    {
        lock (_writeLock)
        {
            return (_spinProbes, Interlocked.Read(ref _probeBlocks), _shortSpins, _spinCredit.Value);
        }
    }

    /// <summary>
    /// The groups in which members inserted their own batches at the same time as the leader (the
    /// others, of <see cref="WriteGroupCount"/>, were inserted by their leader alone); the members
    /// whose inserts the leader launched; the members whose batches the leader inserted because they
    /// had blocked; the times a concurrent insert found a link changed by another insert and searched
    /// a level again; and the times a launched member blocked after its insert, while it waited for
    /// the leader to publish the group. Those blocks are not in <see cref="WriterWaits"/>.
    /// </summary>
    internal (long ParallelGroups, long LaunchedInserts, long KeptInserts, long InsertRetries, long BlocksAfterInsert) ConcurrentInserts()
    {
        return (Interlocked.Read(ref _parallelGroups), Interlocked.Read(ref _launchedInserts), Interlocked.Read(ref _keptInserts),
            Interlocked.Read(ref _insertRetries), Interlocked.Read(ref _blocksAfterInsert));
    }

    /// <summary>
    /// The time that the leaders of all write groups so far spent in each stage of their group, while
    /// <see cref="DbOptions.MeasureGroupStages"/> is on, or zero: <c>Prepare</c>, the switch of full
    /// memtables and the WAL rotation before a group, with the wait for the state lock;
    /// <c>Encode</c>, the encode of the group's records; <c>Append</c>, the WAL append, with its
    /// flush when the group syncs; <c>MemTable</c>, from the end of the append to the publication of
    /// the group, the members' inserts included; <c>HandOff</c>, from the publication until the leader
    /// gives up its leadership; and <c>Gap</c>, from then until the next group's leader starts: the
    /// offer of leadership and its wake, or the time without writers. Read while writers run, a stage
    /// can miss the newest group. With <see cref="DbOptions.EnablePipelinedWrite"/>, <c>HandOff</c>
    /// runs from the end of the append, <c>Turn</c> is the wait of a memtable stage's leader from
    /// the end of its WAL leadership until it has its stage's groups, and <c>MemTable</c> runs from
    /// then until the stage publishes; both count once for each stage, which can take several groups
    /// (see <see cref="MemTableStages"/>). Without it, <c>Turn</c> is zero.
    /// </summary>
    internal (TimeSpan Prepare, TimeSpan Encode, TimeSpan Append, TimeSpan MemTable, TimeSpan HandOff, TimeSpan Gap, TimeSpan Turn) GroupStages()
    {
        TimeSpan Stage(int stage) => Stopwatch.GetElapsedTime(0, Volatile.Read(ref _stageTicks[stage]));

        return (Stage(PrepareStage), Stage(EncodeStage), Stage(AppendStage), Stage(MemTableStage), Stage(HandOffStage), Stage(GapStage), Stage(TurnStage));
    }

    /// <summary>
    /// With <see cref="DbOptions.EnablePipelinedWrite"/>, the memtable stages so far, each of which
    /// inserts and publishes every group in the memtable queue (the groups are
    /// <see cref="WriteGroupCount"/>), and the times the leader of a group that joined the queue
    /// while a stage ran blocked before its group was taken or it was offered the next stage.
    /// </summary>
    internal (long Stages, long QueueBlocks) MemTableStages()
    {
        return (Volatile.Read(ref _memTableStages), Interlocked.Read(ref _memTableQueueBlocks));
    }

    /// <summary>The upper limits of the buckets of <see cref="SpinWaitTimes"/> but the last, which has none.</summary>
    internal static readonly TimeSpan[] SpinWaitTimeLimits =
    [
        TimeSpan.FromMicroseconds(1),
        TimeSpan.FromMicroseconds(5),
        TimeSpan.FromMicroseconds(20),
        TimeSpan.FromMicroseconds(100),
    ];

    private static readonly long[] SpinWaitTickLimits = [.. SpinWaitTimeLimits.Select(limit => (long)(limit.TotalSeconds * Stopwatch.Frequency))];

    // One cache line of counters for each CPU, so writers that count at once seldom share a line.
    private const int WaitTimeStride = 8;

    /// <summary>
    /// The waits that spun first, by the time until the writer's turn came: a completion, an offer
    /// of leadership, or the launch of its insert in a group of several writers, seen in the spin or
    /// after a block. Element n counts the waits of at most
    /// <see cref="SpinWaitTimeLimits"/>[n]; the last element counts the longer ones. A wait for a WAL
    /// flush does not spin and is not counted. The counts show which spin would end what share of
    /// the waits. Read while writers run, a count can miss the newest waits.
    /// </summary>
    internal long[] SpinWaitTimes()
    {
        long[] times = new long[SpinWaitTimeLimits.Length + 1];
        for (int row = 0; row < _spinWaitTimes.Length; row += WaitTimeStride)
            for (int bucket = 0; bucket < times.Length; bucket++)
                times[bucket] += Interlocked.Read(ref _spinWaitTimes[row + bucket]);

        return times;
    }

    /// <summary>Writers waiting for the current leader.</summary>
    internal int QueuedWriterCount
    {
        get
        {
            lock (_writeLock)
            {
                return _queue.Count;
            }
        }
    }

    /// <summary>True once <see cref="Dispose"/> has told running merges and checkpoints to stop.</summary>
    internal bool IsClosing => _closing.IsCancellationRequested;

    /// <summary>Memtables flushed to SSTables since open.</summary>
    internal long FlushCount
    {
        get
        {
            lock (_stateLock)
            {
                return _flushCount;
            }
        }
    }

    /// <summary>The bytes of the SSTables that flushes wrote since open, as RocksDB's <c>rocksdb.flush.write.bytes</c>.</summary>
    internal long FlushBytesWritten
    {
        get
        {
            lock (_stateLock)
            {
                return _flushBytesWritten;
            }
        }
    }

    /// <summary>
    /// The compactions since open: moves and the tables they moved, and merges with the bytes of the
    /// tables they read and wrote.
    /// </summary>
    internal (long Moves, long MovedFiles, long Merges, long BytesRead, long BytesWritten) CompactionCounts()
    {
        lock (_stateLock)
        {
            return (_moves, _movedFiles, _merges, _mergeBytesRead, _mergeBytesWritten);
        }
    }

    /// <summary>
    /// The flushes, moves and merges of <paramref name="family"/> since open: the per-family share of
    /// <see cref="FlushCount"/> and <see cref="CompactionCounts"/>, as RocksDB keeps compaction
    /// statistics per column family.
    /// </summary>
    internal (long Flushes, long Moves, long Merges) FamilyCounts(ColumnFamily family)
    {
        lock (_stateLock)
        {
            return (family.FlushCount, family.MoveCount, family.MergeCount);
        }
    }

    /// <summary>The <see cref="CompactRange"/> calls that wait or run.</summary>
    internal int QueuedManualCompactions
    {
        get
        {
            lock (_stateLock)
            {
                return _manualCompactions.Count;
            }
        }
    }

    /// <summary>
    /// How the compaction threads shared the work since open: the most compactions that ran at once,
    /// and the compactions that a thread did not start because a running one held a file or a key
    /// range they needed (each try counts).
    /// </summary>
    internal (int MostRunning, long ConflictedPicks) CompactionConcurrency()
    {
        lock (_stateLock)
        {
            return (_mostRunning, _conflictedPicks);
        }
    }

    /// <summary>
    /// The write stall now, as RocksDB's <c>rocksdb.is-write-stopped</c> and
    /// <c>rocksdb.actual-delayed-write-rate</c> (0 when writes are not delayed), and since open: the
    /// groups that a delay or a stop held back, and the time they were held.
    /// </summary>
    internal (bool Stopped, long DelayedWriteRate, long DelayedGroups, long StoppedGroups, TimeSpan Stalled) WriteStalls()
    {
        lock (_stateLock)
        {
            WriteStallCondition condition = _writeStall;
            return (condition == WriteStallCondition.Stopped, condition == WriteStallCondition.Delayed ? _delayedWriteRate : 0,
                _delayedGroups, _stoppedGroups, Stopwatch.GetElapsedTime(0, _stallTicks));
        }
    }

    /// <summary>
    /// The rounds of <see cref="MultiGet"/> since open that read data blocks, each in one batch read: one for each batch
    /// of up to 32 keys of a call whose keys reach a table, and one more where a Bloom filter's false positive sent a
    /// key on to an older table.
    /// </summary>
    internal long MultiGetRounds => Interlocked.Read(ref _multiGetRounds);

    /// <summary>The pool the arenas of this database's memtables take their native chunks from; for tests.</summary>
    internal ArenaChunkPool ChunkPool => _chunkPool;

    /// <summary>
    /// The memtable switches since open that waited for a flush because their family held
    /// <c>MaxWriteBufferNumber</c> memtables, and the time they waited, as RocksDB's memtable-limit
    /// stalls. A switch by a write group's leader holds every writer of the database while it waits.
    /// </summary>
    internal (long Waits, TimeSpan Waited) MemTableWaits()
    {
        lock (_stateLock)
        {
            return (_memTableWaits, Stopwatch.GetElapsedTime(0, _memTableWaitTicks));
        }
    }

    /// <summary>
    /// The memtable switches since open that the <see cref="WriteBufferManager"/> asked for, because the
    /// memtables of the databases that share it passed its cap.
    /// </summary>
    internal long BufferManagerSwitches
    {
        get
        {
            lock (_stateLock)
            {
                return _bufferManagerSwitches;
            }
        }
    }

    /// <summary>The SSTables that <see cref="DeleteFilesInRange"/> dropped since open.</summary>
    internal long DroppedFileCount
    {
        get
        {
            lock (_stateLock)
            {
                return _droppedFiles;
            }
        }
    }

    /// <summary>The numbers of the live WAL files, oldest first.</summary>
    internal long[] WalFileNumbers
    {
        get
        {
            lock (_stateLock)
            {
                return [.. _walFiles.Select(f => f.Number)];
            }
        }
    }

    /// <summary>The column family called <paramref name="name"/>. Throws <see cref="KeyNotFoundException"/> when there is none.</summary>
    public ColumnFamily GetColumnFamily(string name)
    {
        return _byName.TryGetValue(name, out ColumnFamily? family)
            ? family
            : throw new KeyNotFoundException($"The database has no column family '{name}'.");
    }

    // ── Properties ───────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The value of the RocksDB property <paramref name="name"/> for <paramref name="family"/> (null: the
    /// default one), as RocksDB's <c>GetProperty</c>: a decimal integer, or null when the name is unknown
    /// or not supported. The names are RocksDB's, with the <c>rocksdb.</c> prefix:
    /// <list type="bullet">
    /// <item><c>num-entries-active-mem-table</c>: the entries of the active memtable, every put, delete and range delete counted.</item>
    /// <item><c>num-immutable-mem-table</c>: the memtables that wait for a flush or are being flushed.</item>
    /// <item><c>mem-table-flush-pending</c>: 1 when a memtable waits for a flush that has not started.</item>
    /// <item><c>cur-size-all-mem-tables</c>: the bytes of the active memtable and the ones not yet flushed.</item>
    /// <item><c>estimate-table-readers-mem</c>: the bytes the open SSTables keep in memory: range tombstones, and the
    /// index and filter unless <see cref="ColumnFamilyOptions.CacheIndexAndFilterBlocks"/> puts them in the block cache.</item>
    /// <item><c>live-sst-files-size</c>: the bytes of the SSTables of the current version.</item>
    /// <item><c>num-files-at-level</c><i>N</i>: the SSTables at level <i>N</i>, from 0 to 6.</item>
    /// <item><c>base-level</c>: the level that level 0 compacts into (see <see cref="ColumnFamilyOptions.MaxBytesForLevelBase"/>).</item>
    /// <item><c>compaction-pending</c>: 1 when a level needs a compaction, without the files a compaction
    /// reads: level 0 holds <see cref="ColumnFamilyOptions.Level0FileNumCompactionTrigger"/> files, or a
    /// sorted level above the bottom holds more than its target, with automatic compactions on or off.</item>
    /// </list>
    /// These are the same for every family: <c>num-running-flushes</c> and <c>num-running-compactions</c>
    /// (0 or 1 flushes, one thread; up to <see cref="DbOptions.MaxBackgroundCompactions"/> compactions),
    /// <c>is-write-stopped</c> (1 or 0), and <c>actual-delayed-write-rate</c>
    /// (bytes per second while writes are delayed, else 0). <c>block-cache-capacity</c>, <c>block-cache-usage</c> and
    /// <c>block-cache-pinned-usage</c> are the family's <see cref="ColumnFamilyOptions.BlockCache"/>'s
    /// <see cref="Cache.Capacity"/>, <see cref="Cache.Usage"/> and <see cref="Cache.PinnedUsage"/>, for every database
    /// and family that shares it; null when the family has no block cache, as in RocksDB.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The database is disposed.</exception>
    public string? GetProperty(string name, ColumnFamily? family = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        const string Prefix = "rocksdb.";
        if (!name.StartsWith(Prefix, StringComparison.Ordinal))
            return null;

        long? value = IntegerProperty(name[Prefix.Length..], target);
        return value?.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Every live SSTable of every family, as RocksDB's <c>GetLiveFilesMetaData</c>: the tables the
    /// manifest lists, in its order for each family (level 0 newest first, then each of levels 1 to 6 in key order).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The database is disposed.</exception>
    public List<LiveFileMetaData> GetLiveFilesMetaData()
    {
        ThrowIfDisposed();
        lock (_stateLock)
        {
            List<LiveFileMetaData> files = [];
            foreach (ManifestFamily family in _manifest.ColumnFamilies)
                foreach (ManifestTable table in family.Tables)
                    files.Add(new LiveFileMetaData(family.Name, table.Level, "/" + TableFormat.FileName(table.Number), table.Number,
                        table.Size, [.. table.Smallest], [.. table.Largest], table.MinSequence, table.MaxSequence, table.EntryCount));

            return files;
        }
    }

    private long? IntegerProperty(string name, ColumnFamily family)
    {
        const string LevelPrefix = "num-files-at-level";
        FamilyVersion version = family.Version;
        switch (name)
        {
            case "num-entries-active-mem-table":
                return version.Active.EntryCount + version.Active.Tombstones.Count;
            case "num-immutable-mem-table":
                return version.Immutables.Length;
            case "cur-size-all-mem-tables":
                return version.Active.ApproximateBytes + version.Immutables.Sum(m => m.ApproximateBytes);
            case "estimate-table-readers-mem":
                return version.Tables.Sum(t => t.MemoryBytes);
            case "live-sst-files-size":
                return version.Tables.Sum(t => t.Properties.Size);
            case "is-write-stopped":
                return _writeStall == WriteStallCondition.Stopped ? 1 : 0;
            case "actual-delayed-write-rate":
                return _writeStall == WriteStallCondition.Delayed ? _delayedWriteRate : 0;
            case "block-cache-capacity":
                return family.Options.BlockCache?.Capacity;
            case "block-cache-usage":
                return family.Options.BlockCache?.Usage;
            case "block-cache-pinned-usage":
                return family.Options.BlockCache?.PinnedUsage;
        }

        if (name.StartsWith(LevelPrefix, StringComparison.Ordinal))
        {
            // RocksDB's default of 7 levels: a level past 6, or no number, is not a property.
            if (!int.TryParse(name.AsSpan(LevelPrefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int level) || level > 6)
                return null;

            return level == 0 ? version.Level0.Length : version.Levels[level].Tables.Length;
        }

        lock (_stateLock)
        {
            switch (name)
            {
                case "mem-table-flush-pending":
                {
                    // The memtable at the head of the queue is not pending once its flush runs.
                    int waiting = _flushQueue.Count(job => job.Family == family);
                    if (_flushRunning && _flushQueue.Peek().Family == family)
                        waiting--;

                    return waiting > 0 ? 1 : 0;
                }
                case "compaction-pending":
                    // As RocksDB's scores, the files a compaction reads do not count.
                    return CompactionPicker.BestLevel(family, LevelTargets.Compute(family.Version, family.Options), _running).Score >= 1 ? 1 : 0;
                case "base-level":
                    return LevelTargets.Compute(family.Version, family.Options).BaseLevel;
                case "num-running-flushes":
                    return _flushRunning ? 1 : 0;
                case "num-running-compactions":
                    return _running.Count;
                default:
                    return null;
            }
        }
    }

    // ── Reads ────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The newest value of <paramref name="key"/>, or null when the key is absent or deleted. Null family: the
    /// default one. Of <paramref name="options"/>, only <see cref="ReadOptions.FillCache"/> applies.
    /// </summary>
    public byte[]? Get(ReadOnlySpan<byte> key, ColumnFamily? family = null, ReadOptions? options = null)
    {
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        FamilyVersion version = target.EnterVersion(out int slot);
        try
        {
            // The version first, then the snapshot, as RocksDB's GetImpl: a compaction installed
            // later can drop versions that an older snapshot needs from the files of a newer
            // version, but not from this one's. The snapshot can be newer than the version's
            // memtables; a memtable switch splits the sequence numbers at one point, so the read
            // still sees the whole state at one sequence number.
            _afterReadVersion?.Invoke();
            long snapshot = LatestSequenceNumber;
            return ReadPath.TryGet(version.Sources, key, snapshot, options?.FillCache ?? true, out byte[]? value) ? value : null;
        }
        finally
        {
            version.Leave(slot);
        }
    }

    /// <summary>
    /// Look up several keys at one snapshot: one value per key, in order, null for a key that is absent or deleted.
    /// The data blocks that each batch of up to 32 keys needs are read together, through
    /// <see cref="IStorageEnvironment.MultiRead"/>. Of <paramref name="options"/>, only <see cref="ReadOptions.FillCache"/> applies.
    /// </summary>
    public byte[]?[] MultiGet(IReadOnlyList<byte[]> keys, ColumnFamily? family = null, ReadOptions? options = null)
    {
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        FamilyVersion version = target.EnterVersion(out int slot);
        try
        {
            // The version first, then the snapshot, as in Get.
            _afterReadVersion?.Invoke();
            long snapshot = LatestSequenceNumber;
            byte[]?[] values = ReadPath.GetMany(version.Sources, keys, snapshot, options?.FillCache ?? true, _environment, out int rounds);
            if (rounds > 0)
                Interlocked.Add(ref _multiGetRounds, rounds);

            return values;
        }
        finally
        {
            version.Leave(slot);
        }
    }

    /// <summary>
    /// Create an iterator over a snapshot of the family. The caller disposes it; it is not safe for
    /// concurrent use. Null family: the default one.
    /// </summary>
    public Iterator NewIterator(ColumnFamily? family = null, ReadOptions? options = null)
    {
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        // The version first, then the snapshot, as in Get. The iterator leaves the version when it is disposed.
        FamilyVersion version = target.EnterVersion(out int slot);
        _afterReadVersion?.Invoke();
        long snapshot = LatestSequenceNumber;
        return new Iterator(version, slot, snapshot, options?.IterateUpperBound?.ToArray(), options?.FillCache ?? true);
    }

    // ── Writes ───────────────────────────────────────────────────────────────────────────────────

    /// <summary>Set <paramref name="key"/> to <paramref name="value"/> as a one-operation batch. Null family: the default one.</summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamily? family = null, WriteOptions? options = null)
    {
        Write(new WriteBatch().Put(key, value, family), options);
    }

    /// <summary>Delete <paramref name="key"/> as a one-operation batch. Null family: the default one.</summary>
    public void Delete(ReadOnlySpan<byte> key, ColumnFamily? family = null, WriteOptions? options = null)
    {
        Write(new WriteBatch().Delete(key, family), options);
    }

    /// <summary>Delete every key in [<paramref name="begin"/>, <paramref name="end"/>); an empty range writes nothing.</summary>
    /// <exception cref="ArgumentException"><paramref name="end"/> comes before <paramref name="begin"/>.</exception>
    public void DeleteRange(ReadOnlySpan<byte> begin, ReadOnlySpan<byte> end, ColumnFamily? family = null, WriteOptions? options = null)
    {
        Write(new WriteBatch().DeleteRange(begin, end, family), options);
    }

    /// <summary>Apply <paramref name="batch"/> atomically. It is visible to reads when this returns.</summary>
    public void Write(WriteBatch batch, WriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(batch);

        // The families of the batch, for the leader's preparation of the group: usually one.
        ColumnFamily? first = null;
        List<ColumnFamily>? others = null;
        foreach (BatchOperation operation in batch.Operations)
        {
            ColumnFamily family = Resolve(operation.Family);
            if (first is null)
                first = family;
            else if (!ReferenceEquals(family, first) && !(others?.Contains(family) ?? false))
                (others ??= []).Add(family);
        }

        if (batch.Count == 0)
            return;

        // Encoded here, on the writer's thread, so the leader only copies the record and gives it
        // its sequence numbers.
        int recordLength = WriteAheadLog.CheckSize(batch.Operations);
        byte[] record = RecordBuffer(recordLength);
        WriteAheadLog.EncodeRecord(record.AsSpan(0, recordLength), batch.Operations, DefaultColumnFamily.Id);

        ColumnFamily[] families = others is null ? first!.AsArray : [first!, .. others];
        Writer writer = new(batch, (options ?? WriteOptions.Default).Sync, record, recordLength, families);
        List<Writer>? group = null;
        long spinTicks = 0;
        bool probe = false;

        lock (_writeLock)
        {
            ThrowIfDisposed();
            ThrowIfFailed();

            if (_leaderActive)
            {
                // A wait for a WAL flush is far longer than the spin, so a spin would only use CPU
                // time. The writer waits for one behind a group that syncs, and as a member of a next
                // group that syncs. The oldest writer in the queue leads the next group instead, so
                // it waits only for the active group.
                bool waitsForFlush = _activeGroupSyncs || (_queue.Count > 0 && (_queueSyncs || writer.Sync));
                if (!waitsForFlush)
                    spinTicks = SpinFor(out probe);
                _queue.Add(writer);
                _queueSyncs |= writer.Sync;
                _writerWaits++;
                if (waitsForFlush)
                    _flushWaits++;
            }
            else
            {
                // The queue is not empty when its oldest writer was offered leadership but has not
                // taken it yet. A running writer takes it first and saves the wait for that wake.
                _leaderActive = true;
                group = TakeGroup(writer, queued: false);
            }
        }

        if (group is null)
        {
            group = WaitForGroup(writer, spinTicks, probe);
            if (group is null)
            {
                writer.ThrowIfFailed();
                return;
            }
        }

        Exception? failure = null;
        GroupRange range = default;
        bool leadsMemTableStage = false;
        try
        {
            range = AppendGroup(group);
            if (_pipelined)
                leadsMemTableStage = JoinMemTableQueue(group, writer, range);
            else
            {
                // Under the state lock, so a switch from Flush cannot take the memtable while it is filled.
                lock (_stateLock)
                    InsertAndPublish(group, writer, range);
                _handOffFrom = EndStage(MemTableStage, _handOffFrom);
            }
        }
        catch (Exception e)
        {
            failure = e;
        }

        Writer? head = null;
        lock (_writeLock)
        {
            if (_measureStages)
            {
                // Before the release: the next leader reads _groupEndedAt once it holds the lock.
                long now = Stopwatch.GetTimestamp();
                if (failure is null)
                    _stageTicks[HandOffStage] += now - _handOffFrom;
                _groupEndedAt = now;
            }

            // The offer is made here, while the writer is still the oldest in the queue: made after the
            // release, it could reach a writer that has left the queue since, led a group of its own,
            // and waits for its memtable stage, where it would read the offer as one of that stage.
            _leaderActive = false;
            if (_queue.Count > 0 && _queue[0].OfferLeadership())
                head = _queue[0];

            Monitor.PulseAll(_writeLock);   // Dispose waits for the leader
        }

        // The wake goes first: it is on the path of the next group, and the members' wakes are not.
        _afterWalLeadership?.Invoke(range.First);
        head?.Wake();

        // The next group's WAL stage runs while this group waits for its memtable stage. The group's
        // writers now belong to the memtable queue, and the leader of their stage completes them.
        if (_pipelined && failure is null)
        {
            CommitInMemTableStage(group, writer, range, leadsMemTableStage);
            writer.ThrowIfFailed();
            return;
        }

        foreach (Writer member in group)
            member.Complete(failure);

        writer.ThrowIfFailed();
    }

    /// <summary>
    /// Wait in the queue until a leader commits the writer's group (null), or until the writer must
    /// lead the queue it is the oldest writer of (the group to commit). A leader that launches the
    /// writer's insert gets it here: the writer inserts its batch, and then waits for the group.
    /// </summary>
    /// <remarks>
    /// Each writer waits on its own signal, not on <c>_writeLock</c>: the end of a group wakes only its
    /// members and the oldest writer in the queue, and a member returns without the lock.
    /// </remarks>
    private List<Writer>? WaitForGroup(Writer writer, long spinTicks, bool probe)
    {
        while (true)
        {
            int turn = WaitForTurn(writer, spinTicks, probe);
            if (turn == Writer.Completed)
                return null;

            if (turn == Writer.Inserting)
            {
                InsertBatch(writer);
                writer.WaitAgain();
                EndInsert(1);

                // The leader completes the writer once the last insert of the group is done. The
                // wait spins as long as the writer's turn did: a long spin holds a CPU that the other
                // writers, the io_uring reactor or readers may need, and the credit of the turn's
                // spin shows whether they do.
                int end = writer.WaitForTurn(spinTicks, ref _blocksAfterInsert, out _);
                Debug.Assert(end == Writer.Completed, "A writer of a group was offered leadership.");
                return null;
            }

            lock (_writeLock)
            {
                // The offer holds only while this writer is still the oldest in the queue and no
                // leader is active. A leader takes writers from the front of the queue only, so a
                // writer that is still in it has no leader. A running writer may have become leader
                // first, without this one when the group size cap left it out: the end of that
                // group offers leadership again.
                if (_queue.Count > 0 && ReferenceEquals(_queue[0], writer) && !_leaderActive)
                {
                    // The previous leader may have failed or the database closed while this writer waited.
                    if (_disposed || Volatile.Read(ref _failure) is not null)
                    {
                        Exception reason = _disposed ? new ObjectDisposedException(nameof(SolariumDb)) : Stopped();
                        FailQueue(reason);
                        throw reason;
                    }

                    _leaderActive = true;
                    return TakeGroup(writer, queued: true);
                }

                // A running writer took the queue first; this writer's group commits under that
                // leader, so the writer now waits for that group only.
                _overtakenOffers++;
                spinTicks = 0;
                probe = false;
                if (!_activeGroupSyncs)
                    spinTicks = SpinFor(out probe);
                writer.WithdrawOffer();
            }
        }
    }

    // Under the write lock, by a writer that becomes leader: its group, from the front of the queue,
    // which the writer leads (queued) or follows. The group stops before the writer that would take
    // it past RocksDB's max_write_batch_group_size_bytes rule; the leader's own batch always goes.
    // The writers left wait for the offer of leadership at the end of the group.
    private List<Writer> TakeGroup(Writer leader, bool queued)
    {
        long eighth = _maxGroupBytes / 8;
        long limit = leader.RecordLength <= eighth ? leader.RecordLength + eighth : _maxGroupBytes;
        long bytes = queued ? 0 : leader.RecordLength;
        int count = 0;
        while (count < _queue.Count && ((queued && count == 0) || bytes + _queue[count].RecordLength <= limit))
            bytes += _queue[count++].RecordLength;

        List<Writer> group = new(count + 1);
        bool syncs = !queued && leader.Sync;
        for (int i = 0; i < count; i++)
        {
            group.Add(_queue[i]);
            syncs |= _queue[i].Sync;
        }

        if (!queued)
            group.Add(leader);

        _queue.RemoveRange(0, count);
        _activeGroupSyncs = syncs;
        _queueSyncs = false;
        foreach (Writer waiting in _queue)
            _queueSyncs |= waiting.Sync;

        return group;
    }

    // While the spin is short, a window of probes starts once in this many probe intervals. Each
    // window spins long where the long spin does not pay, so the windows cost about 1/33 of the
    // turns: on io_uring with 4 CPUs, where a long spin makes a Put twice as slow, 16 intervals
    // apart made the adaptive spin 7% slower than a fixed short spin.
    private const int ProbeWindowsApart = 32;

    /// <summary>
    /// The spin of a turn of a waiting writer that does not wait for a WAL flush, in
    /// <see cref="Stopwatch"/> ticks, under <c>_writeLock</c>. A probe (<paramref name="probe"/>)
    /// spins for the whole <see cref="DbOptions.WriterSpin"/>, and its outcome updates the credit.
    /// The other turns spin as long only while the credit shows that the long spin pays.
    /// </summary>
    private long SpinFor(out bool probe)
    {
        probe = false;
        if (_writerSpinTicks == 0 || _spinProbeInterval == 0)
            return _writerSpinTicks;

        // The turns are counted under the lock that the join takes anyway, so the choice of the
        // probes adds no shared write, and the tests can tell which turn is a probe.
        if (_probeWindowLeft == 0 && --_turnsToProbe < 0)
        {
            if (_spinCredit.LongSpinPays)
            {
                // Every turn spins long, so one turn shows how a long spin ends: the first turn,
                // and then one in _spinProbeInterval.
                _turnsToProbe = _spinProbeInterval - 1;
                probe = true;
                _spinProbes++;
                return _writerSpinTicks;
            }

            // A probe among short spins does not show how a long spin ends: the writers before it
            // blocked, so its group waits for their wakes, and it blocks more often than it would
            // among long spins. So probes run in a window of _spinProbeInterval long turns.
            _probeWindowLeft = _spinProbeInterval;
            _turnsToProbe = ProbeWindowsApart * _spinProbeInterval - 1;
        }

        if (_probeWindowLeft > 0)
        {
            // The first half of the window lets the waits of the short spins end, so only the turns
            // of the second half wait among long spins alone.
            _probeWindowLeft--;
            probe = _probeWindowLeft < (_spinProbeInterval + 1) / 2;
            if (probe)
                _spinProbes++;
            return _writerSpinTicks;
        }

        if (_spinCredit.LongSpinPays)
        {
            // A window brought the credit back. Its count to the next window does not apply to the
            // long spin: without a probe for the rest of it, a long spin that does not pay would hold
            // for thousands of turns. So the probes go on at once, one in _spinProbeInterval.
            if (_turnsToProbe >= _spinProbeInterval)
                _turnsToProbe = _spinProbeInterval - 1;
            return _writerSpinTicks;
        }

        _shortSpins++;
        return _writerShortSpinTicks;
    }

    /// <summary>
    /// Wait as <see cref="Writer.WaitForTurn"/> does, count the time of a wait that spins, and record
    /// the outcome of a <paramref name="probe"/> in the credit.
    /// </summary>
    private int WaitForTurn(Writer writer, long spinTicks, bool probe)
    {
        long start = Stopwatch.GetTimestamp();
        int turn = writer.WaitForTurn(spinTicks, ref _blockedWaits, out bool blocked);
        if (probe)
        {
            if (blocked)
                Interlocked.Increment(ref _probeBlocks);
            _spinCredit.Record(inTime: !blocked);
        }

        if (spinTicks == 0)
            return turn;

        long waited = Stopwatch.GetTimestamp() - start;
        int bucket = 0;
        while (bucket < SpinWaitTickLimits.Length && waited > SpinWaitTickLimits[bucket])
            bucket++;

        // The processor id can be stale, which costs only a shared line now and then; the count is exact.
        int row = Thread.GetCurrentProcessorId() % Environment.ProcessorCount * WaitTimeStride;
        Interlocked.Increment(ref _spinWaitTimes[row + bucket]);
        return turn;
    }

    /// <summary>
    /// Make the family's memtable immutable and flush it to an SSTable. With <paramref name="wait"/>,
    /// return when that data is durable in an SSTable; otherwise return at once, as RocksDB's
    /// <c>FlushOptions.wait = false</c>.
    /// </summary>
    public void Flush(ColumnFamily? family = null, bool wait = true)
    {
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        lock (_stateLock)
        {
            ThrowIfFailed();
            MemTable? pending = SwitchMemTable(target) ?? target.Version.Immutables.FirstOrDefault();
            if (pending is null || !wait)
                return;

            // Flushes of one family run oldest first, so this one being done means all are.
            while (!pending.Flushed)
            {
                ThrowIfFailed();
                if (_stopping)
                    throw new ObjectDisposedException(nameof(SolariumDb));

                Monitor.Wait(_stateLock);
            }
        }
    }

    /// <summary>
    /// Compact the SSTables of the family in [<paramref name="begin"/>, <paramref name="end"/>] (null:
    /// unbounded) one level after the other, as RocksDB's manual compaction: the level 0 tables that
    /// overlap the range, with every level 0 table that overlaps them, into the base level, then the
    /// tables of each level from the base level to level 5 in the range into the level below, with
    /// the tables of that level in their range. So a delete in the range reaches the bottom level and
    /// drops what it hides. Returns when the last of them is installed: at once when no table above
    /// level 6 is in the range. A compaction that runs is finished first. Null family: the default one.
    /// </summary>
    /// <remarks>
    /// Memtables are not flushed first; call <see cref="Flush"/> for that. RocksDB's
    /// <c>CompactRange</c> flushes a memtable that overlaps the range. As RocksDB with no compaction
    /// filter, it does not rewrite level 6 tables that no table above them overlaps. Throws
    /// <see cref="SolariumException"/> after a WAL, flush or compaction failure, and
    /// <see cref="ObjectDisposedException"/> when the database closes first.
    /// </remarks>
    public void CompactRange(byte[]? begin, byte[]? end, ColumnFamily? family = null)
    {
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        ManualCompaction request = new(target, begin?.ToArray(), end?.ToArray());
        lock (_stateLock)
        {
            ThrowIfFailed();
            if (_stopping)
                throw new ObjectDisposedException(nameof(SolariumDb));

            _manualCompactions.Enqueue(request);
            Monitor.PulseAll(_stateLock);
            while (!request.Done)
            {
                ThrowIfFailed();
                if (_stopping)
                    throw new ObjectDisposedException(nameof(SolariumDb));

                Monitor.Wait(_stateLock);
            }
        }
    }

    /// <summary>
    /// Drop every SSTable of levels 1 to 6 of the family whose keys all lie in [<paramref name="begin"/>,
    /// <paramref name="end"/>] (null: unbounded), in one manifest save, as RocksDB's
    /// <c>DeleteFilesInRange</c>. With <paramref name="includeEnd"/> false, a table whose largest key is
    /// <paramref name="end"/> stays. Level 0 tables, memtables, and the tables of a compaction that
    /// runs stay too. Returns when the manifest no longer lists the dropped tables; each file is
    /// deleted when no reader uses it any more. Null family: the default one.
    /// </summary>
    /// <remarks>
    /// As in RocksDB, it ignores sequence numbers: every key of a dropped table is gone for the reads
    /// that start after the call, even when no delete was written for it, while a read or an iterator
    /// that started before still sees it. Newer versions of those keys in level 0 or a memtable stay.
    /// As in RocksDB, dropping a table can bring back an older version of one of its keys from a table
    /// of a lower level that reaches past the range, which a delete in the dropped table hid. A store
    /// whose keys are written once, as Kommander's log, has no such version. Throws
    /// <see cref="SolariumException"/> after a WAL, flush or compaction failure, or when the manifest
    /// save fails, which also stops writes.
    /// </remarks>
    public void DeleteFilesInRange(byte[]? begin, byte[]? end, ColumnFamily? family = null, bool includeEnd = true)
    {
        ColumnFamily target = Resolve(family);
        ThrowIfDisposed();

        lock (_stateLock)
        {
            ThrowIfFailed();
            if (_stopping)
                throw new ObjectDisposedException(nameof(SolariumDb));

            List<TableReader> dropped = [];
            foreach (TableReader table in target.Version.Levels.SelectMany(level => level.Tables))
            {
                if (_running.Exists(compaction => compaction.Takes(table)))
                    continue;

                TableProperties properties = table.Properties;
                if (begin is not null && properties.Smallest.AsSpan().SequenceCompareTo(begin) < 0)
                    continue;

                if (end is not null)
                {
                    int order = properties.Largest.AsSpan().SequenceCompareTo(end);
                    if (order > 0 || (order == 0 && !includeEnd))
                        continue;
                }

                dropped.Add(table);
            }

            if (dropped.Count == 0)
                return;

            try
            {
                ReplaceTables(target, [.. dropped], 0, []);
            }
            catch (Exception e)
            {
                // The manifest on disk may or may not list the tables; the files stay either way, and
                // a reopen deletes them if it does not.
                SolariumException failure = new($"Dropping {dropped.Count} SSTables of column family '{target.Name}' in '{_directory}' failed.", e);
                Interlocked.CompareExchange(ref _failure, failure, null);
                Monitor.PulseAll(_stateLock);
                throw failure;
            }

            _droppedFiles += dropped.Count;
        }
    }

    /// <summary>
    /// Write a copy of the database, as of one moment, to <paramref name="directory"/>, which must not
    /// exist, as RocksDB's <c>Checkpoint::CreateCheckpoint</c>. The copy opens as a database with the
    /// same column families, and reads in every family as this one did at the
    /// <see cref="LatestSequenceNumber"/> of the start of the call. Writes go on meanwhile, and the
    /// copy does not have them.
    /// </summary>
    /// <remarks>
    /// <para>The copy holds no WAL file. Its SSTables are hard links to this database's, as
    /// RocksDB's are, or copies when the file system cannot link the two directories (see
    /// <see cref="IStorageEnvironment.TryLinkFile"/>). What its memtables hold at that sequence
    /// number goes to new SSTables of the copy, so this database is not flushed; RocksDB instead
    /// copies its WAL files while they are below <c>log_size_for_flush</c>.</para>
    /// <para>The call creates the directory and holds its <c>LOCK</c> file, as <see cref="Open"/>
    /// does, until the copy is done; another checkpoint or an <see cref="Open"/> of the same
    /// directory that comes first makes it fail without touching a file there. The copy keeps its
    /// <c>LOCK</c> file.</para>
    /// <para>The copy is complete when the call returns. Its <c>FORMAT</c> file is written last, and
    /// <see cref="Open"/> refuses a directory with files and no <c>FORMAT</c>, so a copy that a crash
    /// or a failure cut short never opens as a database. After a failure, the call deletes the files
    /// it wrote and leaves the directory with its <c>LOCK</c> file only. A failure of the copy does
    /// not stop this database's writes.</para>
    /// <para><see cref="Dispose"/> stops a checkpoint that has not started to write its
    /// <c>FORMAT</c> file, which then fails as above with <see cref="ObjectDisposedException"/>, and
    /// waits for it; one that has started completes before <see cref="Dispose"/> returns.</para>
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="directory"/> exists, or another database or checkpoint holds it.</exception>
    /// <exception cref="ObjectDisposedException">The database is disposed before the call or while it copies.</exception>
    /// <exception cref="SolariumException">After a WAL, flush or compaction failure, or when the copy cannot be written.</exception>
    public void CreateCheckpoint(string directory)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ThrowIfDisposed();
        string target = Path.GetFullPath(directory);
        if (Directory.Exists(target) || _environment.FileExists(target))
            throw new ArgumentException($"'{target}' exists; a checkpoint creates its directory.", nameof(directory));

        // One sequence number and the version of every family, under the state lock: a memtable
        // switch, a flush and a compaction install under it, so each version holds every entry at or
        // below the number, in a memtable or a table. Being counted in the versions keeps their
        // tables open and their files in place until the copy is done, and being counted in
        // _checkpoints keeps Dispose from closing them before.
        List<(ManifestFamily Record, FamilyVersion Version, int Slot, CompressionType Compression)> families = new(_manifest.ColumnFamilies.Count);
        long snapshot;
        long nextNumber;
        CancellationToken closing;
        lock (_stateLock)
        {
            ThrowIfFailed();
            if (_stopping)
                throw new ObjectDisposedException(nameof(SolariumDb));

            closing = _closing.Token;
            snapshot = LatestSequenceNumber;
            nextNumber = _manifest.NextFileNumber;
            foreach (ManifestFamily record in _manifest.ColumnFamilies)
            {
                ColumnFamily family = _byId[record.Id]!;
                FamilyVersion version = family.EnterVersion(out int slot);
                families.Add((record, version, slot, family.Options.Compression));
            }

            _checkpoints++;
        }

        FileStream? claim = null;
        List<string> written = [];
        try
        {
            claim = ClaimCheckpointDirectory(target);
            WriteCheckpoint(target, families, snapshot, nextNumber, written, closing);
        }
        catch (Exception e) when (claim is not null)
        {
            foreach (string path in written)
                TryDelete(_environment, path);

            if (closing.IsCancellationRequested)
                throw new ObjectDisposedException($"'{_directory}' was closed while its checkpoint to '{target}' was written.", e);

            if (e is SolariumException)
                throw;

            throw new SolariumException($"The checkpoint of '{_directory}' to '{target}' failed.", e);
        }
        finally
        {
            claim?.Dispose();
            foreach ((_, FamilyVersion version, int slot, _) in families)
                version.Leave(slot);

            lock (_stateLock)
            {
                _checkpoints--;
                Monitor.PulseAll(_stateLock);
            }
        }
    }

    // The directory of a checkpoint, created and locked as Open locks a database, and empty but for
    // LOCK: the check that it did not exist came before, so a checkpoint or an Open that created it
    // since holds the lock or left files, and the call fails without touching them. The lock is
    // held until the copy is written, or deleted after a failure.
    private FileStream ClaimCheckpointDirectory(string target)
    {
        try
        {
            _environment.CreateDirectory(target);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw new SolariumException($"The checkpoint of '{_directory}' cannot create '{target}'.", e);
        }

        FileStream claim;
        try
        {
            claim = Lock(target);
        }
        catch (SolariumException e)
        {
            throw new ArgumentException($"'{target}' is held by another database or checkpoint.", "directory", e);
        }

        if (_environment.GetChildren(target).Any(name => name != "LOCK"))
        {
            claim.Dispose();
            throw new ArgumentException($"'{target}' exists and is not empty; a checkpoint creates its directory.", "directory");
        }

        return claim;
    }

    // The files of a checkpoint: the tables, then MANIFEST, then FORMAT, each durable before the next.
    // Each path goes to written before its file or link is created, so a failure deletes only what
    // it made. Each file starts with a check of closing, and a table copy checks it as it goes; past
    // the check before FORMAT, the copy completes.
    private void WriteCheckpoint(string target, List<(ManifestFamily Record, FamilyVersion Version, int Slot, CompressionType Compression)> families, long snapshot, long nextNumber, List<string> written,
        CancellationToken closing)
    {
        Manifest manifest = new() { NextFileNumber = nextNumber };
        bool link = true;
        foreach ((ManifestFamily record, FamilyVersion version, _, CompressionType compression) in families)
        {
            // Level 0 newest first: the memtables, the active one first, then the tables of level 0.
            // Every family is flushed up to the snapshot, so the copy replays no WAL.
            ManifestFamily copy = new() { Id = record.Id, Name = record.Name, FlushedSequence = snapshot };
            foreach (MemTable memTable in (MemTable[])[version.Active, .. version.Immutables])
            {
                closing.ThrowIfCancellationRequested();
                TableProperties? properties = WriteCheckpointTable(memTable, snapshot, Path.Combine(target, TableFormat.FileName(manifest.NextFileNumber)), compression, written);
                if (properties is not null)
                    copy.Tables.Add(ManifestTable.From(manifest.NextFileNumber++, 0, properties));
            }

            foreach (TableReader table in version.Level0)
                copy.Tables.Add(CheckpointTable(table, 0, target, written, ref link, closing));

            for (int level = 1; level < version.Levels.Length; level++)
                foreach (TableReader table in version.Levels[level].Tables)
                    copy.Tables.Add(CheckpointTable(table, level, target, written, ref link, closing));

            manifest.ColumnFamilies.Add(copy);
        }

        // The manifest's save flushes the directory, which makes the table files durable too; FORMAT
        // comes after it, so a directory with FORMAT has everything.
        // Both replace a file through a temporary one beside it, path + ".tmp".
        string manifestPath = Path.Combine(target, Manifest.FileName);
        closing.ThrowIfCancellationRequested();
        written.AddRange([manifestPath + ".tmp", manifestPath]);
        manifest.Save(_environment, target);
        closing.ThrowIfCancellationRequested();
        string formatPath = Path.Combine(target, FormatFileName);
        written.AddRange([formatPath + ".tmp", formatPath]);
        DurableFiles.ReplaceText(_environment, formatPath, FormatVersion);
        _environment.FlushDirectory(Path.GetDirectoryName(target)!);
    }

    // What a read at the snapshot sees of the memtable, as a table at path; null, and no file, when it sees nothing.
    private TableProperties? WriteCheckpointTable(MemTable memTable, long snapshot, string path, CompressionType compression, List<string> written)
    {
        List<RangeTombstone> tombstones = memTable.TombstonesAt(snapshot);
        if (tombstones.Count == 0 && !memTable.FlushEntries(snapshot, tombstones).Any())
            return null;

        written.Add(path);
        using IStorageFile file = _environment.OpenFile(path);
        return TableBuilder.Build(file, memTable.FlushEntries(snapshot, tombstones), tombstones, compression);
    }

    // A table of the checkpoint under its own number: a link to the source's file, which no one
    // writes again once a manifest names it, or a copy once the file system has refused a link,
    // as RocksDB stops linking at its first NotSupported. The version the checkpoint holds keeps
    // the source's file in place until then.
    private ManifestTable CheckpointTable(TableReader table, int level, string target, List<string> written, ref bool link, CancellationToken closing)
    {
        closing.ThrowIfCancellationRequested();
        string path = Path.Combine(target, TableFormat.FileName(table.Number));
        written.Add(path);
        link = link && _environment.TryLinkFile(TablePath(table.Number), path);
        if (!link)
        {
            using IStorageFile file = _environment.OpenFile(path);
            table.CopyTo(file, closing);
        }

        return ManifestTable.From(table.Number, level, table.Properties);
    }

    // The WAL stage of a group, by its leader outside the write lock: prepare the group, give it its
    // sequence numbers, append it, and, when commits are pipelined, give it its turn: the count of
    // groups appended, which the memtable stages publish and RotateWal and Dispose wait for.
    private GroupRange AppendGroup(List<Writer> group)
    {
        long stamp = StartStages();
        WalFile wal;
        long delay = 0;
        lock (_stateLock)
        {
            ThrowIfFailed();
            PrepareGroup(group);
            if (_writeStall != WriteStallCondition.Normal)
            {
                // The group pays for the bytes of the group before it, as RocksDB charges its last
                // group.
                if (_writeStall == WriteStallCondition.Delayed)
                    delay = WriteDelay(_lastGroupBytes);
                if (delay == 0)
                    WaitWhileStopped();
            }

            wal = _walFiles[^1];
        }

        // Only the leader rotates the WAL, so wal stays the current file while it sleeps.
        if (delay > 0)
            SleepForDelay(delay);

        stamp = EndStage(PrepareStage, stamp);

        // From the last number given to a group, which a pipelined commit can give before the group
        // before it is published.
        long first = _lastAssigned + 1;
        long sequence = first - 1;
        bool sync = false;
        long durable = wal.Log!.DurableLength;
        ReadOnlySpan<byte> records;
        if (group.Count == 1 && group[0].OwnedRecord is { } owned)
        {
            // A large batch is alone in its group, and its record is its writer's own array, which
            // waits in Write until the group completes: completed in place and appended from there,
            // it is not copied into _records, which would grow to its size, by doubling.
            Writer writer = group[0];
            writer.FirstSequence = first;
            records = owned.AsSpan(0, writer.RecordLength);
            WriteAheadLog.CompleteRecord(owned.AsSpan(0, writer.RecordLength), first, durable);
            sequence += writer.OperationCount;
            sync = writer.Sync;
        }
        else
        {
            // A group above the cap leaves a buffer that large; a new one lets it go.
            if (_records.Capacity > 2 * _maxGroupBytes)
                _records = new ArrayBufferWriter<byte>();
            else
                _records.ResetWrittenCount();

            foreach (Writer writer in group)
            {
                writer.FirstSequence = sequence + 1;
                WriteAheadLog.CopyRecord(_records, writer.Record, sequence + 1, durable);
                sequence += writer.OperationCount;
                sync |= writer.Sync;
            }

            records = _records.WrittenSpan;
        }

        _lastGroupBytes = records.Length;
        stamp = EndStage(EncodeStage, stamp);
        try
        {
            wal.Log!.Append(records, sync);
        }
        catch (Exception e)
        {
            // The WAL's tail is now unknown, so the database accepts no more writes.
            Interlocked.CompareExchange(ref _failure, e, null);
            throw;
        }

        _lastAssigned = sequence;

        // No lock: only the current WAL file takes appends, and a flush never deletes it. A file
        // stops being current in RotateWal, under the state lock that the flush thread takes too,
        // after the leader that wrote these fields gave its leadership up under _writeLock.
        wal.LastSequence = sequence;
        wal.Length = wal.Log!.Length;

        long turn = _pipelined ? ++_assignedTurn : 0;
        _handOffFrom = EndStage(AppendStage, stamp);

        Interlocked.Increment(ref _writeGroups);
        Interlocked.Increment(ref _groupSizes[Math.Min(group.Count, MaxCountedGroupSize)]);
        return new GroupRange(first, sequence, turn);
    }

    // Insert the group's batches into the active memtables and publish the group. The caller holds
    // the lock that keeps a switch out: the state lock, or the memtable lock when commits are
    // pipelined. The members' inserts take no lock; the leader waits for every one of them.
    private void InsertAndPublish(List<Writer> group, Writer leader, GroupRange range)
    {
        if (!_concurrentInserts || group.Count == 1 || !InsertInParallel(group, leader))
        {
            long sequence = range.First - 1;
            foreach (Writer writer in group)
                foreach (BatchOperation operation in writer.Batch.Operations)
                    Apply(Resolve(operation.Family).Version.Active, ++sequence, operation.Kind, operation.Key, operation.Value);
        }

        // Every insert of the group is done, and no switch can come before the publication, so each
        // active memtable's bytes are final for this group. A family that two writers share charges
        // nothing the second time.
        if (_writeBufferManager is not null)
            foreach (Writer writer in group)
                foreach (ColumnFamily family in writer.Families)
                    family.Version.Active.Charge(_writeBufferManager);

        // The release that makes the whole group visible at once.
        Volatile.Write(ref _lastSequence, range.Last);
    }

    // The WAL leader, after a successful append. When no stage runs, the group's leader leads the
    // next one with its group alone, and the group never enters the queue (true): no other thread
    // can reach it, so the stage needs no second lock to take it. Otherwise the group joins the
    // queue, behind every group appended before it, for a later stage (false).
    private bool JoinMemTableQueue(List<Writer> group, Writer leader, GroupRange range)
    {
        lock (_memTableQueueLock)
        {
            if (!_memTableLeaderActive)
            {
                // The end of a stage leaves the queue to the next stage's leader, so with no stage
                // running it is empty.
                Debug.Assert(_memTableQueue is null, "No memtable stage runs, but groups wait for one.");
                _memTableLeaderActive = true;
                return true;
            }

            // Before the group is visible: from then on, a stage's leader can launch the leader's
            // insert or complete it, or offer it the next stage.
            leader.WaitAgain();

            // The first group's list becomes the queue, so a join allocates nothing.
            if (_memTableQueue is null)
            {
                _memTableQueue = group;
                _memTableQueueLeader = leader;
                _memTableQueueRange = range;
            }
            else
            {
                _memTableQueue.AddRange(group);
                _memTableQueueRange = _memTableQueueRange with { Last = range.Last, Turn = range.Turn };
            }

            return false;
        }
    }

    // The memtable stage of a pipelined commit, for the leader of a group after it gave up its WAL
    // leadership. A leader whose group joined the queue while a stage ran waits as a member does:
    // until the stage that takes its group launches its insert and completes it, or until it is
    // offered the next stage, which takes the whole queue.
    private void CommitInMemTableStage(List<Writer> group, Writer leader, GroupRange range, bool leadsStage)
    {
        long stamp = _measureStages ? Stopwatch.GetTimestamp() : 0;
        if (leadsStage)
        {
            RunMemTableStage(group, leader, range, stamp);
            return;
        }

        int turn = leader.WaitForTurn(_writerSpinTicks, ref _memTableQueueBlocks, out _);
        if (turn == Writer.Inserting)
        {
            InsertBatch(leader);
            leader.WaitAgain();
            EndInsert(1);
            turn = leader.WaitForTurn(_writerSpinTicks, ref _blocksAfterInsert, out _);
        }

        if (turn == Writer.Completed)
            return;
        Debug.Assert(turn == Writer.MemTableLeading, "The leader of a group in the memtable queue was offered the WAL.");

        List<Writer> writers;
        GroupRange taken;
        lock (_memTableQueueLock)
        {
            writers = _memTableQueue!;
            taken = _memTableQueueRange;
            _memTableQueue = null;
            _memTableQueueLeader = null;
        }

        Debug.Assert(taken.First == range.First, "The memtable queue does not start with the group of the stage's leader.");
        RunMemTableStage(writers, leader, taken, stamp);
    }

    // Insert and publish the writers that a memtable stage took, offer the next stage, and complete
    // every writer that the stage took. The stage's leader is the first group's leader.
    private void RunMemTableStage(List<Writer> writers, Writer leader, GroupRange range, long stamp)
    {
        // A failure here still fails the stage, so the groups it took fail in WAL order.
        Exception? held = null;
        try
        {
            _beforeMemTableStage?.Invoke(range.First);
        }
        catch (Exception e)
        {
            held = e;
        }

        _memTableStages++;
        stamp = EndStage(TurnStage, stamp);
        Exception? failure = null;
        lock (_memTableLock)
        {
            try
            {
                if (_memTableFailed)
                    throw Stopped();
                if (held is not null)
                    throw held;

                _beforeMemTableInsert?.Invoke(range.First);
                InsertAndPublish(writers, leader, range);

                // Under the lock: the next stage's leader adds to the same counter.
                EndStage(MemTableStage, stamp);
            }
            catch (Exception e)
            {
                // Every later group has sequence numbers after these, so none of them may be
                // published: they fail in their stages, and later writes fail before they start.
                _memTableFailed = true;
                SolariumException stopped = e as SolariumException
                    ?? new SolariumException($"A memtable insert failed in '{_directory}' after the WAL append of sequence numbers {range.First} to {range.Last}.", e);
                Interlocked.CompareExchange(ref _failure, stopped, null);
                failure = stopped;
            }

            _publishedTurn = range.Turn;
            if (_stageWaiters > 0)
                Monitor.PulseAll(_memTableLock);
        }

        Writer? next;
        lock (_memTableQueueLock)
        {
            next = _memTableQueueLeader;
            _memTableLeaderActive = next is not null;
        }

        // The offer goes first: its wake is on the path of the next stage, and the completions are not.
        next?.OfferMemTableLeadership();
        foreach (Writer writer in writers)
            writer.Complete(failure);
    }

    // Under the state lock, before the WAL file stops being current: wait until every group that
    // was appended to it is in the memtables. A flush deletes a WAL file once no memtable holds
    // data from it, and cannot see a group that is still between its two stages.
    private void WaitForMemTableStages()
    {
        lock (_memTableLock)
        {
            _stageWaiters++;
            try
            {
                while (_publishedTurn != _assignedTurn)
                    Monitor.Wait(_memTableLock);
            }
            finally
            {
                _stageWaiters--;
            }
        }
    }

    // The leader, at the start of its group: the timestamp that the first stage starts from, after
    // the gap since the last group ended. 0 while the timers are off.
    private long StartStages()
    {
        if (!_measureStages)
            return 0;

        long now = Stopwatch.GetTimestamp();
        if (_groupEndedAt != 0)
            _stageTicks[GapStage] += now - _groupEndedAt;
        return now;
    }

    // The leader: add the time since `since` to `stage`, and return the timestamp that the next
    // stage starts from. 0 while the timers are off.
    private long EndStage(int stage, long since)
    {
        if (!_measureStages)
            return 0;

        long now = Stopwatch.GetTimestamp();
        _stageTicks[stage] += now - since;
        return now;
    }

    // A buffer of at least `length` bytes for the record of this thread's batch.
    private static byte[] RecordBuffer(int length)
    {
        if (t_record is { } kept && kept.Length >= length)
            return kept;

        if (length > MaxKeptRecord)
            return GC.AllocateUninitializedArray<byte>(length);

        byte[] record = GC.AllocateUninitializedArray<byte>(Math.Max(256, (int)BitOperations.RoundUpToPowerOf2((uint)length)));
        t_record = record;
        return record;
    }

    /// <summary>The length of this thread's kept record buffer, 0 when it has none.</summary>
    internal static int KeptRecordBufferLength => t_record?.Length ?? 0;

    /// <summary>
    /// The capacity of the buffer that the leader copies a group's records into. A batch above
    /// <see cref="MaxKeptRecord"/> bytes that is alone in its group does not grow it.
    /// </summary>
    internal int GroupBufferCapacity => _records.Capacity;

    // Before a group: switch full memtables, keep the WAL files under their limit, rotate the WAL.
    private void PrepareGroup(List<Writer> group)
    {
        _written.Clear();
        foreach (Writer writer in group)
            foreach (ColumnFamily family in writer.Families)
                _written.Add(family);

        foreach (ColumnFamily family in _written)
            if (family.Version.Active.ApproximateBytes >= family.Options.WriteBufferSize)
                SwitchMemTable(family);

        if (_walFiles.Count > 1 && LiveWalBytes() > _maxTotalWalSize)
        {
            // Flush the families that keep the oldest WAL file alive.
            long oldestLast = _walFiles[0].LastSequence;
            foreach (ColumnFamily family in _byName.Values)
            {
                MemTable active = family.Version.Active;
                if (!active.IsEmpty && active.MinSequence <= oldestLast)
                    SwitchMemTable(family);
            }
        }

        // The memtables of every database that shares the manager passed its cap. While commits are
        // pipelined, the groups still in their memtable stage are not charged yet, so the check can
        // trail by those groups.
        if (_writeBufferManager?.ShouldFlush() == true)
            SwitchOldestMemTable();

        if (_rotateWal)
            RotateWal();
    }

    // Under the state lock, by the WAL leader while writes are delayed: how long its group waits, in
    // Stopwatch ticks, as RocksDB's WriteController::GetDelay. A bucket of credit refills at
    // DelayedWriteRate once a millisecond, and a group that finds too little waits until the rate
    // pays for its bytes.
    private long WriteDelay(long bytes)
    {
        if (_delayCredit >= bytes)
        {
            _delayCredit -= bytes;
            return 0;
        }

        long now = Stopwatch.GetTimestamp();
        long refill = Stopwatch.Frequency / 1000;
        if (_nextRefill == 0)
            _nextRefill = now;

        if (_nextRefill <= now)
        {
            // In double, as RocksDB: the time since the last refill can be long enough for its
            // product with the rate to overflow.
            long elapsed = now - _nextRefill + refill;
            _delayCredit += (long)Math.Ceiling((double)elapsed / Stopwatch.Frequency * _delayedWriteRate);
            _nextRefill = now + refill;
            if (_delayCredit >= bytes)
            {
                _delayCredit -= bytes;
                return 0;
            }
        }

        _nextRefill += (long)((double)(bytes - _delayCredit) / _delayedWriteRate * Stopwatch.Frequency);
        _delayCredit = 0;
        return Math.Max(_nextRefill - now, refill);
    }

    // By the WAL leader, without the state lock, so flushes and compactions install meanwhile: sleep
    // out a delay in steps of 1 ms, as RocksDB's DelayWrite, ending early when the delay ends or
    // Dispose waits. Then wait while writes stop.
    private void SleepForDelay(long delay)
    {
        long start = Stopwatch.GetTimestamp();
        long end = start + delay;
        while (_writeStall == WriteStallCondition.Delayed && !Volatile.Read(ref _disposed) && Stopwatch.GetTimestamp() < end)
            Thread.Sleep(1);

        lock (_stateLock)
        {
            _delayedGroups++;
            _stallTicks += Stopwatch.GetTimestamp() - start;
            ThrowIfFailed();
            WaitWhileStopped();
        }
    }

    // Under the state lock, by the WAL leader: wait while writes stop. Monitor.Wait releases the lock
    // for the flushes and compactions that end the stop. A failure or Dispose ends the wait with an
    // exception, which fails the group: Dispose waits for the leader, so the leader cannot wait for it.
    private void WaitWhileStopped()
    {
        if (_writeStall != WriteStallCondition.Stopped)
            return;

        long start = Stopwatch.GetTimestamp();
        _stoppedGroups++;
        try
        {
            while (_writeStall == WriteStallCondition.Stopped)
            {
                ThrowIfFailed();
                if (Volatile.Read(ref _disposed))
                    throw new ObjectDisposedException(nameof(SolariumDb));

                Monitor.Wait(_stateLock);
            }
        }
        finally
        {
            _stallTicks += Stopwatch.GetTimestamp() - start;
        }
    }

    // Under the state lock, after a change of a level 0: the stall of the database, the worst over
    // its families, as RocksDB's RecalculateWriteStallConditions. A family without automatic
    // compactions stalls nothing, as RocksDB: no compaction would end its stall.
    private void UpdateWriteStall()
    {
        WriteStallCondition condition = WriteStallCondition.Normal;
        foreach (ColumnFamily? family in _byId)
        {
            if (family is null || family.Options.DisableAutoCompactions)
                continue;

            int files = family.Version.Level0.Length;
            if (files >= family.Options.Level0StopWritesTrigger)
            {
                condition = WriteStallCondition.Stopped;
                break;
            }

            if (files >= family.Options.Level0SlowdownWritesTrigger)
                condition = WriteStallCondition.Delayed;
        }

        // A delay starts with an empty bucket, as RocksDB's first delay token.
        if (condition == WriteStallCondition.Delayed && _writeStall != WriteStallCondition.Delayed)
        {
            _delayCredit = 0;
            _nextRefill = 0;
        }

        _writeStall = condition;
    }

    // Under the state lock. Returns the memtable that became immutable, or null when the active one is empty.
    private MemTable? SwitchMemTable(ColumnFamily family)
    {
        long waitedFrom = 0;
        while (true)
        {
            FamilyVersion version = family.Version;
            if (version.Active.IsEmpty)
                return null;

            if (version.Immutables.Length + 1 < family.Options.MaxWriteBufferNumber)
                break;

            // Too many memtables: wait for a flush. Monitor.Wait releases the lock for the flush thread.
            ThrowIfFailed();
            if (_stopping)
                throw new ObjectDisposedException(nameof(SolariumDb));

            if (waitedFrom == 0)
            {
                waitedFrom = Stopwatch.GetTimestamp();
                _memTableWaits++;
            }

            Monitor.Wait(_stateLock);
        }

        if (waitedFrom != 0)
            _memTableWaitTicks += Stopwatch.GetTimestamp() - waitedFrom;

        // A pipelined memtable stage fills the active memtable without the state lock; the switch
        // waits for it, so all the entries of a group go to one memtable.
        MemTable full;
        lock (_memTableLock)
        {
            FamilyVersion current = family.Version;
            full = current.Active;
            family.Install(new FamilyVersion(new MemTable(_chunkPool), [full, .. current.Immutables], current.Level0, current.Levels, _tableUnused));

            // Under the memtable lock, so the last stage that wrote it has charged it.
            if (_writeBufferManager is not null)
                full.ChargeImmutable(_writeBufferManager);
        }

        // The flush holds its own reference, so the memtable it writes stays whatever versions come and go.
        full.AddReference();
        _flushQueue.Enqueue((family, full));
        _rotateWal = true;
        Monitor.PulseAll(_stateLock);
        return full;
    }

    // Under the state lock, by the WAL leader, when the WriteBufferManager asks for a flush: switch the
    // family whose active memtable holds the oldest data, as RocksDB's HandleWriteBufferManagerFlush
    // picks the one created first. Nothing when every active memtable is empty: the memory is in
    // immutable memtables, which flush already, or in other databases.
    private void SwitchOldestMemTable()
    {
        ColumnFamily? oldest = null;
        foreach (ColumnFamily family in _byName.Values)
        {
            MemTable active = family.Version.Active;
            if (!active.IsEmpty && (oldest is null || active.MinSequence < oldest.Version.Active.MinSequence))
                oldest = family;
        }

        if (oldest is null)
            return;

        _bufferManagerSwitches++;
        SwitchMemTable(oldest);
    }

    // Under the state lock, by the leader. The members that still spin insert their own batches, the
    // leader inserts its own and those of the members that blocked, and returns when all of them are
    // done. RocksDB launches every member, but a blocked member needs a thread wake before it can
    // insert, which puts the wake on the path of the publication; kept, its wake comes after it.
    // The leader waits and then publishes the group, and not the last writer to finish, as RocksDB
    // does, because the state lock that keeps switches out belongs to the leader's thread. Returns
    // false, with nothing inserted, when every member blocked: the leader inserts the group alone.
    private bool InsertInParallel(List<Writer> group, Writer leader)
    {
        _insertFailure = null;
        _insertsDone.Reset();
        // Published to the members by the full fence of each launch. The leader counts down for
        // itself and its kept members at once, after its inserts, so no launched member can end
        // the count first.
        _insertsRunning = group.Count;

        // The launches go first, so the members insert while the leader does. The hint can be stale
        // either way, which costs only time: a launched member inserts after a wake, and a kept one
        // waits for its completion.
        int launched = 0;
        foreach (Writer member in group)
        {
            if (ReferenceEquals(member, leader))
                continue;

            if (member.IsBlocked)
                _keptMembers.Add(member);
            else
            {
                member.LaunchInsert();
                launched++;
            }
        }

        int kept = _keptMembers.Count;
        Interlocked.Add(ref _keptInserts, kept);
        if (launched == 0)
        {
            _keptMembers.Clear();
            return false;
        }

        Interlocked.Increment(ref _parallelGroups);
        Interlocked.Add(ref _launchedInserts, launched);
        InsertBatch(leader);
        foreach (Writer member in _keptMembers)
            InsertBatch(member);
        _keptMembers.Clear();

        EndInsert(1 + kept);
        WaitForInserts();

        if (Volatile.Read(ref _insertFailure) is { } failure)
        {
            // Part of the group is in the memtables under sequence numbers that are never
            // published, and the WAL holds all of it: the next group cannot reuse those numbers,
            // so the database accepts no more writes until an open replays the WAL.
            SolariumException stopped = new($"A memtable insert failed in '{_directory}' after the WAL append of sequence numbers {leader.FirstSequence} and on.", failure);
            Interlocked.CompareExchange(ref _failure, stopped, null);
            throw stopped;
        }

        return true;
    }

    // Insert the writer's batch from its own thread or, for a kept member, the leader's thread, at the sequence numbers the leader gave it. A
    // failure is recorded for the leader and does not stop the count of the group.
    private void InsertBatch(Writer writer)
    {
        try
        {
            long sequence = writer.FirstSequence;
            MemTable? target = null;
            long first = 0;
            long bytes = 0;
            long entries = 0;
            int retries = 0;

            foreach (BatchOperation operation in writer.Batch.Operations)
            {
                // No switch can come before the group ends, so the version is the one the leader prepared.
                MemTable memTable = Resolve(operation.Family).Version.Active;
                if (!ReferenceEquals(memTable, target))
                {
                    target?.AddCounts(bytes, entries, first, sequence - 1);
                    target = memTable;
                    first = sequence;
                    bytes = 0;
                    entries = 0;
                }

                if (operation.Kind == OperationKind.DeleteRange)
                {
                    memTable.AddRangeTombstoneConcurrently(operation.Key, operation.Value!, sequence);
                    bytes += MemTable.TombstoneBytes(operation.Key, operation.Value!);
                }
                else
                {
                    retries += memTable.AddConcurrently(operation.Key, sequence, operation.Kind, operation.Value, out long nodeBytes);
                    bytes += nodeBytes;
                    entries++;
                }

                sequence++;
            }

            target?.AddCounts(bytes, entries, first, sequence - 1);
            if (retries > 0)
                Interlocked.Add(ref _insertRetries, retries);
        }
        catch (Exception e)
        {
            Interlocked.CompareExchange(ref _insertFailure, e, null);
        }
    }

    // After the inserts of a writer, or of the leader and its kept members. The writer that ends
    // the count wakes the leader.
    private void EndInsert(int inserts)
    {
        if (Interlocked.Add(ref _insertsRunning, -inserts) == 0)
            _insertsDone.Set();
    }

    // The leader, under the state lock: spin while the inserts are likely to end soon, then block.
    private void WaitForInserts()
    {
        long start = Stopwatch.GetTimestamp();
        while (Volatile.Read(ref _insertsRunning) > 0 && Stopwatch.GetTimestamp() - start < _writerSpinTicks)
            Thread.SpinWait(1);

        while (Volatile.Read(ref _insertsRunning) > 0)
        {
            _insertsDone.Wait();

            // The last writer of an earlier group counts down and then sets the event, so its set
            // can come after this group's reset, and wake the leader early. A fence between the
            // reset and the next read of the count: an insert that ends after the read sets the
            // event again.
            if (Volatile.Read(ref _insertsRunning) > 0)
            {
                _insertsDone.Reset();
                Interlocked.MemoryBarrier();
            }
        }
    }

    private static void Apply(MemTable memTable, long sequence, OperationKind kind, byte[] key, byte[]? value)
    {
        if (kind == OperationKind.DeleteRange)
            memTable.AddRangeTombstone(key, value!, sequence);
        else
            memTable.Add(key, sequence, kind, value);
    }

    // ── Flushes ──────────────────────────────────────────────────────────────────────────────────

    private void FlushLoop()
    {
        while (true)
        {
            (ColumnFamily Family, MemTable MemTable) job;
            lock (_stateLock)
            {
                while (_flushQueue.Count == 0 && !_stopping)
                    Monitor.Wait(_stateLock);

                if (_stopping || _failure is not null)
                    return;

                job = _flushQueue.Peek();
                _flushRunning = true;
            }

            try
            {
                FlushMemTable(job.Family, job.MemTable);
            }
            catch (Exception e)
            {
                lock (_stateLock)
                {
                    _flushRunning = false;
                    Interlocked.CompareExchange(ref _failure, e, null);
                    Monitor.PulseAll(_stateLock);
                }

                return;
            }
        }
    }

    private void FlushMemTable(ColumnFamily family, MemTable memTable)
    {
        long number;
        lock (_stateLock)
        {
            number = _manifest.NextFileNumber++;
        }

        // The table keeps only what a reader that enters it after its install can see, which holds
        // only while every entry of the memtable is published: a switch waits for the memtable
        // stage, which publishes under the same lock. Checked, because a broken order would drop
        // versions silently.
        long published = LatestSequenceNumber;
        if (memTable.MaxSequence > published)
            throw new SolariumException(
                $"A flush of column family '{family.Name}' in '{_directory}' found sequence number {memTable.MaxSequence} in its memtable above the published {published}.");

        // No writer is left on an immutable memtable, so it is read here with no lock.
        string path = TablePath(number);
        IStorageFile file = _environment.OpenFile(path);
        TableReader table;
        try
        {
            TableBuilder.Build(file, memTable.FlushEntries(), memTable.Tombstones, family.Options.Compression, _flushPacing);
            table = TableReader.Open(number, file, Caching(family.Options, level: 0));
        }
        catch
        {
            file.Dispose();
            TryDelete(_environment, path);
            throw;
        }

        // The table must not outlive a WAL record before it. A batch of another family, or the rest
        // of a batch that spans families, may still be in the WAL only and not flushed: a power loss
        // would keep the table and lose them, a hole in the history. So the WAL that holds this
        // memtable's records is made durable before the manifest names the table, as RocksDB syncs
        // its closed WAL files before it commits a flush.
        try
        {
            SyncCurrentWal();
        }
        catch
        {
            table.Dispose();
            TryDelete(_environment, path);
            throw;
        }

        lock (_stateLock)
        {
            ManifestFamily record = family.Record;
            long flushedBefore = record.FlushedSequence;
            record.Tables.Insert(0, ManifestTable.From(number, 0, table.Properties));
            record.FlushedSequence = Math.Max(flushedBefore, memTable.MaxSequence);

            // The WAL files this flush makes obsolete leave the manifest in the same save that adds
            // the table, and are deleted only after it.
            int obsolete = ObsoleteWalFiles(memTable);
            try
            {
                SaveManifest(obsolete);
            }
            catch
            {
                // The save can fail after the new manifest replaced the old one, in the directory
                // flush, and then the manifest on disk lists the table, so the file stays. If it
                // does not list it, the next open deletes the file as an orphan.
                record.Tables.RemoveAt(0);
                record.FlushedSequence = flushedBefore;
                table.Dispose();
                throw;
            }

            lock (_tablesLock)
            {
                _openTables.Add(table);
            }

            FamilyVersion current = family.Version;
            family.Install(new FamilyVersion(current.Active, [.. current.Immutables.Where(m => m != memTable)], [table, .. current.Level0], current.Levels, _tableUnused));
            memTable.Flushed = true;

            // Readers of older versions can still hold the memtable until they leave; the manager
            // counts what the database keeps, not what the GC has yet to collect.
            if (_writeBufferManager is not null)
                memTable.ReleaseCharge(_writeBufferManager);

            _flushQueue.Dequeue();
            memTable.Release();
            _flushRunning = false;
            _flushCount++;
            family.FlushCount++;
            _flushBytesWritten += table.Properties.Size;
            UpdateWriteStall();

            DeleteWalFiles(obsolete);
            Monitor.PulseAll(_stateLock);
        }
    }

    // ── Compactions ──────────────────────────────────────────────────────────────────────────────

    // One of the MaxBackgroundCompactions threads. Each picks under the state lock, from what the
    // running compactions leave, and runs its compaction outside it.
    private void CompactionLoop()
    {
        while (true)
        {
            Compaction compaction;
            lock (_stateLock)
            {
                while (true)
                {
                    if (_stopping || _closing.IsCancellationRequested || _failure is not null)
                        return;

                    // A manual compaction runs alone, as RocksDB's exclusive_manual_compaction: no
                    // automatic one starts while it waits, and each of its steps waits until no other
                    // compaction runs. A manual compaction goes on with its next level, and ends when
                    // the picker has none left.
                    Compaction? picked = null;
                    if (_manualCompactions.TryPeek(out ManualCompaction? request))
                    {
                        if (_running.Count == 0)
                        {
                            picked = PickManual(request);
                            if (picked is null)
                            {
                                EndRequest();
                                continue;
                            }
                        }
                    }
                    else
                        picked = PickAutomatic();

                    if (picked is not null)
                    {
                        compaction = picked;
                        break;
                    }

                    Monitor.Wait(_stateLock);
                }

                // A merge holds its own reference on its inputs, so its reads stay safe whatever
                // installs meanwhile. Compact releases them.
                if (!compaction.IsMove)
                    foreach (TableReader table in compaction.AllInputs)
                        table.AddReference();

                _running.Add(compaction);
                _mostRunning = Math.Max(_mostRunning, _running.Count);
            }

            _beforeCompaction?.Invoke();
            bool installed;
            try
            {
                installed = Compact(compaction);
            }
            catch (Exception e)
            {
                lock (_stateLock)
                {
                    _running.Remove(compaction);
                    SolariumException failure = e as SolariumException
                        ?? new SolariumException($"A compaction of column family '{compaction.Family.Name}' failed in '{_directory}'.", e);
                    Interlocked.CompareExchange(ref _failure, failure, null);
                    Monitor.PulseAll(_stateLock);
                }

                return;
            }

            // The end of a compaction frees its files and its range for the other threads.
            lock (_stateLock)
            {
                _running.Remove(compaction);
                Monitor.PulseAll(_stateLock);
            }

            // Not installed: the merge stopped for Dispose or after a failure, and the loop ends.
            if (!installed)
                return;
        }
    }

    // Under the state lock: the oldest manual compaction is done.
    private void EndRequest()
    {
        _manualCompactions.Dequeue().Done = true;
        Monitor.PulseAll(_stateLock);
    }

    // Under the state lock. The compaction of the level with the highest score of every family, ties to
    // the lowest id and then to the higher level, that no running compaction stands in the way of; or
    // null when no score is at least 1, or every such level waits for a running compaction.
    private Compaction? PickAutomatic()
    {
        List<(ColumnFamily Family, LevelTargets Targets, int Level, double Score)> candidates = [];
        foreach (ColumnFamily? family in _byId)
        {
            if (family is null || family.Options.DisableAutoCompactions)
                continue;

            LevelTargets targets = LevelTargets.Compute(family.Version, family.Options);
            foreach ((int level, double score) in CompactionPicker.Scores(family, targets, _running))
                if (score >= 1)
                    candidates.Add((family, targets, level, score));
        }

        // A stable sort keeps the families in id order and each family's levels from 0 down.
        foreach ((ColumnFamily family, LevelTargets targets, int level, _) in candidates.OrderByDescending(candidate => candidate.Score))
        {
            Compaction? picked = CompactionPicker.Pick(family, level, targets, _running);
            if (picked is not null)
                return picked;

            _conflictedPicks++;
        }

        return null;
    }

    // Under the state lock. The next compaction of a CompactRange call, from the level after the one
    // it compacted last; or null when it has none left.
    private Compaction? PickManual(ManualCompaction request)
    {
        LevelTargets targets = LevelTargets.Compute(request.Family.Version, request.Family.Options);
        Compaction? picked = CompactionPicker.PickRange(request.Family, request.NextLevel, request.Begin, request.End, targets, out int level);
        request.NextLevel = level + 1;
        return picked;
    }

    // On a compaction thread. A move changes only the manifest and the version, so it runs under
    // the state lock; a merge reads and writes tables outside it, then releases the references it
    // took on its inputs. Returns false, with nothing installed, when the merge stopped for Dispose
    // or another compaction failed first.
    private bool Compact(Compaction compaction)
    {
        if (compaction.IsMove)
        {
            _compactionStarting?.Invoke(compaction);
            lock (_stateLock)
            {
                if (_failure is not null)
                    return false;

                ReplaceTables(compaction.Family, compaction.Inputs, compaction.OutputLevel, compaction.Inputs);
                _moves++;
                _movedFiles += compaction.Inputs.Length;
                compaction.Family.MoveCount++;
            }

            // A table moved out of level 0 keeps its reader, and its pin with it: drop it, as RocksDB
            // pins only level 0's. A reader that took the blocks keeps them.
            if (compaction.StartLevel == 0 && compaction.OutputLevel > 0)
                foreach (TableReader table in compaction.Inputs)
                    table.Unpin();

            return true;
        }

        try
        {
            _compactionStarting?.Invoke(compaction);
            CompactionMerge merge = new(_environment, _directory, NextFileNumber, compaction.Family.Options.TargetFileSizeBase, compaction.Family.Options.Compression,
                _compactionPacing, Caching(compaction.Family.Options, compaction.OutputLevel), _closing.Token);
            List<TableReader>? outputs = merge.Run(compaction);
            if (outputs is null)
                return false;

            lock (_stateLock)
            {
                // Another compaction failed while this one merged: writes have stopped, and nothing
                // installs. The outputs are in no manifest.
                if (_failure is not null)
                {
                    foreach (TableReader table in outputs)
                    {
                        table.Dispose();
                        TryDelete(_environment, TablePath(table.Number));
                    }

                    return false;
                }

                try
                {
                    ReplaceTables(compaction.Family, [.. compaction.AllInputs], compaction.OutputLevel, [.. outputs]);
                }
                catch
                {
                    // The manifest on disk may list the outputs, so they stay; a reopen deletes them
                    // if it does not.
                    foreach (TableReader table in outputs)
                        table.Dispose();

                    throw;
                }

                _merges++;
                compaction.Family.MergeCount++;
                _mergeBytesRead += compaction.InputBytes;
                foreach (TableReader table in outputs)
                    _mergeBytesWritten += table.Properties.Size;
            }

            return true;
        }
        finally
        {
            foreach (TableReader table in compaction.AllInputs)
                if (table.Release())
                    _tableUnused(table);
        }
    }

    private long NextFileNumber()
    {
        lock (_stateLock)
        {
            return _manifest.NextFileNumber++;
        }
    }

    // Under the state lock: remove tables from any level of the family and add tables to the sorted
    // level addedLevel, in one manifest save, and install the new version. A moved table is in both
    // lists. Flushes may have added level 0 tables since a compaction picked its tables; they are
    // newer than everything it holds, and stay.
    private void ReplaceTables(ColumnFamily family, TableReader[] removed, int addedLevel, TableReader[] added)
    {
        FamilyVersion current = family.Version;
        TableReader[] level0 = [.. current.Level0.Where(t => Array.IndexOf(removed, t) < 0)];
        SortedLevel[] levels = new SortedLevel[Manifest.LevelCount];
        levels[0] = SortedLevel.Empty;
        for (int level = 1; level < levels.Length; level++)
        {
            List<TableReader> tables = [.. current.Levels[level].Tables.Where(t => Array.IndexOf(removed, t) < 0)];
            if (level == addedLevel)
            {
                tables.AddRange(added);
                tables.Sort((a, b) => a.Properties.Smallest.AsSpan().SequenceCompareTo(b.Properties.Smallest));
                for (int i = 1; i < tables.Count; i++)
                    if (tables[i - 1].Properties.Overlaps(tables[i].Properties))
                        throw new SolariumException(
                            $"A change of column family '{family.Name}' in '{_directory}' would make SSTables {tables[i - 1].Number} and {tables[i].Number} overlap at level {level}.");
            }

            levels[level] = tables.Count == 0 ? SortedLevel.Empty : new SortedLevel([.. tables]);
        }

        ManifestFamily record = family.Record;
        List<ManifestTable> before = [.. record.Tables];
        record.Tables.Clear();
        foreach (TableReader table in level0)
            record.Tables.Add(ManifestTable.From(table.Number, 0, table.Properties));

        for (int level = 1; level < levels.Length; level++)
            foreach (TableReader table in levels[level].Tables)
                record.Tables.Add(ManifestTable.From(table.Number, level, table.Properties));

        try
        {
            SaveManifest(0);
        }
        catch
        {
            record.Tables.Clear();
            record.Tables.AddRange(before);
            throw;
        }

        // A moved table is open already, and stays in the set.
        lock (_tablesLock)
        {
            _openTables.UnionWith(added);
        }

        family.Install(new FamilyVersion(current.Active, current.Immutables, level0, levels, _tableUnused));
        UpdateWriteStall();
        Monitor.PulseAll(_stateLock);
    }

    // ── WAL files ────────────────────────────────────────────────────────────────────────────────

    private void ReplayWalFiles()
    {
        long[] numbers = [.. FilesWithExtension(".log").Order()];
        foreach (long listed in _manifest.WalNumbers)
        {
            if (Array.BinarySearch(numbers, listed) < 0)
                throw new SolariumException(
                    $"WAL file {WalPath(listed)} is missing. The manifest lists it as holding data that no SSTable has.");
        }

        long expected = 0;   // the first sequence number the next record must have; 0 before the first record
        long flushed = _lastSequence;

        for (int i = 0; i < numbers.Length; i++)
        {
            WalFile wal = new(numbers[i], WalPath(numbers[i]));
            bool newest = i == numbers.Length - 1;

            // The log owns the file only once Open returns. Damage, or a record that Replay refuses,
            // throws before that, and the file must not stay open: a handle on Windows blocks the
            // next open, and an io_uring descriptor has no finalizer to close it.
            IStorageFile file = _environment.OpenFile(wal.Path);
            WriteAheadLog opened;
            try
            {
                opened = WriteAheadLog.Open(file, Replay, tolerateTornTail: newest);
            }
            catch (SolariumException e)
            {
                file.Dispose();
                throw new SolariumException($"WAL file {wal.Path}: {e.Message}", e);
            }
            catch
            {
                file.Dispose();
                throw;
            }

            using (WriteAheadLog log = opened)
            {
                // Unsynced writes that survived a process crash may sit only in the OS cache. A newer
                // file is about to follow this one, and only the newest file may have a torn tail, so
                // this one must be durable first.
                if (newest)
                    log.Flush();

                wal.Length = log.Length;
            }

            _walFiles.Add(wal);

            void Replay(long firstSequence, List<WalOperation> operations)
            {
                bool continuous = expected == 0 ? firstSequence <= flushed + 1 : firstSequence == expected;
                if (!continuous)
                    throw new SolariumException(
                        $"The WAL in '{_directory}' is missing writes before sequence number {firstSequence}: a WAL file was lost or is damaged.");

                long sequence = firstSequence;
                foreach (WalOperation operation in operations)
                {
                    if ((uint)operation.FamilyId >= (uint)_byId.Length || _byId[operation.FamilyId] is not { } family)
                        throw new SolariumException($"The WAL refers to column family id {operation.FamilyId}, which the manifest does not list.");

                    // Data that an SSTable already holds: a WAL file can outlive the flush of its data.
                    if (sequence > family.Record.FlushedSequence)
                        Apply(family.Version.Active, sequence, operation.Kind, operation.Key, operation.Value);

                    sequence++;
                }

                expected = sequence;
                wal.LastSequence = sequence - 1;
                _lastSequence = Math.Max(_lastSequence, sequence - 1);

                // A memtable that fills is written as a table at once, so the WAL files need not fit
                // in memory, as RocksDB flushes during recovery.
                // Replay charges the WriteBufferManager but flushes only at WriteBufferSize, as RocksDB's
                // recovery: the first write after the open flushes when the manager is past its cap.
                foreach (WalOperation operation in operations)
                {
                    ColumnFamily family = _byId[operation.FamilyId]!;
                    if (_writeBufferManager is not null)
                        family.Version.Active.Charge(_writeBufferManager);

                    if (family.Version.Active.ApproximateBytes >= family.Options.WriteBufferSize)
                        FlushDuringReplay(family);
                }
            }
        }
    }

    // During replay, with no reader and no other thread yet: write the family's active memtable as a
    // level 0 table and start a new one. No reader older than the table exists, so the table keeps
    // only what FlushEntries keeps. The manifest that names the table is saved at the end of the open,
    // after the newest WAL file is flushed; until then the WAL files hold its data, and a crash leaves
    // the table as an orphan that the next open deletes.
    private void FlushDuringReplay(ColumnFamily family)
    {
        MemTable memTable = family.Version.Active;
        long number = _manifest.NextFileNumber++;
        string path = TablePath(number);
        IStorageFile file = _environment.OpenFile(path);
        TableReader table;
        try
        {
            // Replay runs inside Open, with no write to share the drive with, so it is not paced.
            TableBuilder.Build(file, memTable.FlushEntries(), memTable.Tombstones, family.Options.Compression);
            table = TableReader.Open(number, file, Caching(family.Options, level: 0));
        }
        catch
        {
            file.Dispose();
            TryDelete(_environment, path);
            throw;
        }

        ManifestFamily record = family.Record;
        record.Tables.Insert(0, ManifestTable.From(number, 0, table.Properties));
        record.FlushedSequence = Math.Max(record.FlushedSequence, memTable.MaxSequence);
        lock (_tablesLock)
            _openTables.Add(table);

        FamilyVersion current = family.Version;
        family.Install(new FamilyVersion(new MemTable(_chunkPool), current.Immutables, [table, .. current.Level0], current.Levels, _tableUnused));
        if (_writeBufferManager is not null)
            memTable.ReleaseCharge(_writeBufferManager);

        _flushCount++;
        family.FlushCount++;
        _flushBytesWritten += table.Properties.Size;
    }

    // A new WAL file. The caller saves the manifest next, and that save's directory flush makes
    // the new file's entry durable before any write to it.
    private void StartWalFile()
    {
        long number = _manifest.NextFileNumber++;
        IStorageFile file = _environment.OpenFile(WalPath(number));
        WriteAheadLog log;
        try
        {
            log = WriteAheadLog.Create(file);
        }
        catch
        {
            file.Dispose();
            throw;
        }

        _walFiles.Add(new WalFile(number, WalPath(number)) { Log = log });
    }

    // Under the state lock. A loop over a handful of files, once per write group.
    private long LiveWalBytes()
    {
        long total = 0;
        for (int i = 0; i < _walFiles.Count; i++)
            total += _walFiles[i].Length;

        return total;
    }

    // Every record appended to the WAL so far durable, with no lock held during the fsync. A WAL file
    // is flushed when it stops being current, so only the current one can hold records that are not.
    // The fsync goes through a handle of its own, opened while the file is current, so a rotation
    // that disposes the writers' handle meanwhile does not race it; an fsync covers the file's data
    // whatever handle wrote it.
    private void SyncCurrentWal()
    {
        IStorageFile file;
        WriteAheadLog? log;
        long length;
        lock (_stateLock)
        {
            WalFile current = _walFiles[^1];
            log = current.Log;
            length = log?.Length ?? 0;
            file = _environment.OpenFile(current.Path);
        }

        using (file)
            file.Flush();

        // Later records of the file carry the new durable length. After a rotation retired the log
        // meanwhile, no record follows in that file, and none needs to.
        log?.NoteDurable(length);
    }

    // Under the state lock, by the leader: flush the current WAL file and start a new one. A failure
    // stops all writes, as a failed append does: the old file may not be durable, or the new one may
    // be current without a durable directory entry, so a synced write to it could be lost.
    private void RotateWal()
    {
        _rotateWal = false;
        WalFile current = _walFiles[^1];
        if (current.Length == 0)
            return;

        WaitForMemTableStages();
        try
        {
            // Flushed before the next file starts, so only the newest file can have a torn tail.
            current.Log!.Flush();
            StartWalFile();
            current.Log.Dispose();
            current.Log = null;

            // The save flushes the directory, which makes the new file's entry durable.
            int obsolete = ObsoleteWalFiles(flushing: null);
            SaveManifest(obsolete);
            DeleteWalFiles(obsolete);
        }
        catch (Exception e)
        {
            SolariumException failure = new($"A WAL rotation in '{_directory}' failed, so the database accepts no more writes.", e);
            Interlocked.CompareExchange(ref _failure, failure, null);
            Monitor.PulseAll(_stateLock);
            throw failure;
        }
    }

    // Under the state lock. How many of the oldest WAL files no memtable needs, counting
    // flushing (a memtable whose table the caller is about to install) as gone. The current file stays.
    private int ObsoleteWalFiles(MemTable? flushing)
    {
        long oldestUnflushed = long.MaxValue;
        foreach (ColumnFamily family in _byName.Values)
        {
            FamilyVersion version = family.Version;
            if (!version.Active.IsEmpty)
                oldestUnflushed = Math.Min(oldestUnflushed, version.Active.MinSequence);

            foreach (MemTable memTable in version.Immutables)
                if (memTable != flushing && !memTable.IsEmpty)
                    oldestUnflushed = Math.Min(oldestUnflushed, memTable.MinSequence);
        }

        int count = 0;
        while (count < _walFiles.Count - 1 && _walFiles[count].LastSequence < oldestUnflushed)
            count++;

        return count;
    }

    // Under the state lock. Save the manifest with every WAL file except the oldest obsoleteWalFiles.
    private void SaveManifest(int obsoleteWalFiles)
    {
        _manifest.WalNumbers.Clear();
        for (int i = obsoleteWalFiles; i < _walFiles.Count; i++)
            _manifest.WalNumbers.Add(_walFiles[i].Number);

        _manifest.Save(_environment, _directory);
        _afterManifestSave?.Invoke();
    }

    // Under the state lock, after a manifest save without them.
    private void DeleteWalFiles(int count)
    {
        for (int i = 0; i < count; i++)
            TryDelete(_environment, _walFiles[i].Path);

        _walFiles.RemoveRange(0, count);
    }

    // ── Lifetime ─────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Wait for the write in progress and the flush in progress, stop the compaction in progress, flush
    /// the current WAL file, then close the files and release the directory lock. Memtables that are
    /// not flushed stay in the WAL and are replayed at the next open, so every write before the close
    /// survives a power loss after it. A <see cref="CompactRange"/> that waits throws
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <exception cref="SolariumException">
    /// The WAL flush failed. The database is closed anyway; the writes since the last synced one may
    /// not survive a power loss.
    /// </exception>
    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_disposed)
                return;

            _disposed = true;
        }

        // A leader that writes stop hold sees _disposed under the state lock, so this wake reaches it
        // in its wait or finds it past its check, and its group fails instead of waiting for compaction.
        lock (_stateLock)
        {
            Monitor.PulseAll(_stateLock);
        }

        lock (_writeLock)
        {
            while (_leaderActive)
                Monitor.Wait(_writeLock);

            // The writers left wait for an offer of leadership or for a leader that took them.
            FailQueue(new ObjectDisposedException(nameof(SolariumDb)));
        }

        // A group that gave up its WAL leadership may still be in its memtable stage, and its
        // writers wait for it. No group can take a new turn: the database is closed to writers.
        WaitForMemTableStages();

        // A merge or a checkpoint that runs stops at its next check and deletes what it wrote; one
        // that already ended installs or publishes its result first. Cancelled before the stop is
        // set, so a thread that sees the stop knows it was told to stop. The checkpoints read the
        // tables that CloseFiles closes, so they are waited for; no new one starts once the stop is set.
        _closing.Cancel();
        lock (_stateLock)
        {
            _stopping = true;
            Monitor.PulseAll(_stateLock);
            while (_checkpoints > 0)
                Monitor.Wait(_stateLock);
        }

        _flushThread.Join();
        foreach (Thread thread in _compactionThreads)
            thread.Join();
        _closing.Dispose();

        // Unsynced writes survive a power loss after a clean close: one flush of the current WAL
        // file, the only one that can hold unflushed records. RocksDB's close leaves them to the OS.
        // A failure is reported after the files and the lock are released, so the close completes.
        Exception? walFlush = null;
        try
        {
            if (Volatile.Read(ref _failure) is null)
                _walFiles[^1].Log?.Flush();
        }
        catch (Exception e)
        {
            walFlush = e;
        }

        ReleaseMemTables();
        CloseFiles();
        CloseVersions();
        _lock.Dispose();
        if (walFlush is not null)
            throw new SolariumException($"The flush of the WAL at the close of '{_directory}' failed; the writes since the last synced one may not survive a power loss.", walFlush);
    }

    // At the close or a failed open, with no writer and no flush left: the memtables that the database
    // still holds leave the WriteBufferManager, so a closed database keeps nothing charged on a shared one,
    // and the flushes that will not run drop their references.
    private void ReleaseMemTables()
    {
        while (_flushQueue.TryDequeue(out (ColumnFamily Family, MemTable MemTable) job))
            job.MemTable.Release();

        if (_writeBufferManager is null)
            return;

        foreach (ColumnFamily family in _byName.Values)
        {
            FamilyVersion version = family.Version;
            version.Active.ReleaseCharge(_writeBufferManager);
            foreach (MemTable memTable in version.Immutables)
                memTable.ReleaseCharge(_writeBufferManager);
        }
    }

    // After CloseFiles, so a table that a retired version releases is not deleted: each family's last version
    // retires, and its memtables are freed now, or when the last iterator still in it is disposed. A reader
    // that enters later is turned away. The pool keeps less, or nothing when it was this database's own.
    private void CloseVersions()
    {
        foreach (ColumnFamily family in _byName.Values)
            family.Close();

        if (_ownsChunkPool)
            _chunkPool.Dispose();
        else if (_sharedPoolBytes > 0)
            _chunkPool.AddToRetainLimit(-_sharedPoolBytes);
    }

    // The memtable bytes the families of a database without a WriteBufferManager may hold at once, what it adds to
    // the limit of the shared chunk pool: each family's write buffer, as many times as it may have memtables.
    private static long MemTableBytes(Manifest manifest, DbOptions options)
    {
        long bytes = 0;
        foreach (ManifestFamily record in manifest.ColumnFamilies)
        {
            ColumnFamilyOptions family = options.OptionsFor(record.Name);
            long most = family.WriteBufferSize > long.MaxValue / Math.Max(1, family.MaxWriteBufferNumber)
                ? long.MaxValue
                : family.WriteBufferSize * Math.Max(1, family.MaxWriteBufferNumber);
            bytes = most > long.MaxValue - bytes ? long.MaxValue : bytes + most;
        }

        return bytes;
    }

    private void CloseFiles()
    {
        foreach (WalFile wal in _walFiles)
            wal.Log?.Dispose();

        // Iterators that are still open can hold retired versions; their tables close here too, and
        // a version that closes later finds them gone.
        lock (_tablesLock)
        {
            _tablesClosed = true;
            foreach (TableReader table in _openTables)
                table.Dispose();

            _openTables.Clear();

            // The data blocks of the closed tables, which a table leaves to the LRU when it closes: a cache that
            // other databases share keeps nothing of this one. A read still in flight can cache a block after this,
            // which the LRU ages out as it does a closed table's.
            foreach (Cache cache in _blockCaches)
                cache.EraseOwner(_cacheOwner);
        }
    }

    // Called by the thread that released the last reference on the table: the installer of a newer
    // version, the last reader of a retired one, or a merge that held the table. The current version
    // lists every table of the manifest, so a table that no version lists has left the manifest, and
    // its file goes. A reader can be the one that deletes it, as in RocksDB without
    // avoid_unnecessary_blocking_io. After Dispose, the file stays, and the next open deletes it.
    private void TableUnused(TableReader table)
    {
        lock (_tablesLock)
        {
            if (_tablesClosed || !_openTables.Remove(table))
                return;
        }

        table.Dispose();
        TryDelete(_environment, TablePath(table.Number));
    }

    // What a table of the family at level keeps in the family's block cache, under this database's owner: its index
    // and filter when the family asks, pinned at level 0 when it asks that too, and its data blocks when it asks;
    // otherwise the index and filter stay resident in the reader, and every data block is read from the file.
    private TableCaching Caching(ColumnFamilyOptions options, int level)
    {
        if (options.BlockCache is not Cache cache)
            return TableCaching.Resident;

        lock (_tablesLock)
            if (!_blockCaches.Contains(cache))
                _blockCaches.Add(cache);

        return new TableCaching(cache, _cacheOwner, options.CacheIndexAndFilterBlocks,
            options.CacheIndexAndFilterBlocks && level == 0 && options.PinL0FilterAndIndexBlocksInCache,
            options.CacheIndexAndFilterBlocksWithHighPriority, options.CacheDataBlocks);
    }

    // Every table of the family, level 0 newest first and each sorted level in key order, as the
    // manifest lists them; or none: a failure closes the tables it opened. The manifest's check of the
    // sorted levels used its own copy of the bounds, so the tables' own are checked again.
    private (TableReader[] Level0, SortedLevel[] Levels) OpenTables(ManifestFamily record, ColumnFamilyOptions options)
    {
        List<TableReader>[] byLevel = new List<TableReader>[Manifest.LevelCount];
        for (int level = 0; level < byLevel.Length; level++)
            byLevel[level] = [];

        try
        {
            foreach (ManifestTable entry in record.Tables)
            {
                IStorageFile file = _environment.OpenFile(TablePath(entry.Number));
                try
                {
                    byLevel[entry.Level].Add(TableReader.Open(entry.Number, file, Caching(options, entry.Level)));
                }
                catch
                {
                    file.Dispose();
                    throw;
                }
            }

            for (int level = 1; level < byLevel.Length; level++)
                for (int i = 1; i < byLevel[level].Count; i++)
                    if (byLevel[level][i - 1].Properties.Overlaps(byLevel[level][i].Properties))
                        throw new SolariumException(
                            $"SSTables {byLevel[level][i - 1].Number} and {byLevel[level][i].Number} of column family '{record.Name}' in '{_directory}' overlap at level {level}.");
        }
        catch
        {
            foreach (TableReader table in byLevel.SelectMany(tables => tables))
                table.Dispose();

            throw;
        }

        lock (_tablesLock)
        {
            foreach (List<TableReader> tables in byLevel)
                _openTables.UnionWith(tables);
        }

        SortedLevel[] levels = new SortedLevel[Manifest.LevelCount];
        levels[0] = SortedLevel.Empty;
        for (int level = 1; level < levels.Length; level++)
            levels[level] = byLevel[level].Count == 0 ? SortedLevel.Empty : new SortedLevel([.. byLevel[level]]);

        return ([.. byLevel[0]], levels);
    }

    // SST files that no manifest lists (a flush that crashed before the manifest update), and temporary files.
    private void DeleteOrphanFiles()
    {
        HashSet<long> live = [.. _manifest.ColumnFamilies.SelectMany(f => f.Tables).Select(t => t.Number)];
        foreach (long number in FilesWithExtension(".sst"))
            if (!live.Contains(number))
                TryDelete(_environment, TablePath(number));

        foreach (string name in _environment.GetChildren(_directory))
            if (name.EndsWith(".tmp", StringComparison.Ordinal))
                TryDelete(_environment, Path.Combine(_directory, name));

        long highest = Math.Max(FilesWithExtension(".sst").DefaultIfEmpty().Max(), FilesWithExtension(".log").DefaultIfEmpty().Max());
        _manifest.NextFileNumber = Math.Max(_manifest.NextFileNumber, highest + 1);
    }

    private IEnumerable<long> FilesWithExtension(string extension)
    {
        foreach (string name in _environment.GetChildren(_directory))
            if (name.EndsWith(extension, StringComparison.Ordinal)
                && long.TryParse(name.AsSpan(0, name.Length - extension.Length), NumberStyles.None, CultureInfo.InvariantCulture, out long number))
                yield return number;
    }

    private string TablePath(long number) => Path.Combine(_directory, TableFormat.FileName(number));

    private string WalPath(long number) => Path.Combine(_directory, $"{number:D6}.log");

    private static void TryDelete(IStorageEnvironment environment, string path)
    {
        try
        {
            environment.DeleteFile(path);
        }
        catch (IOException)
        {
        }
    }

    private ColumnFamily Resolve(ColumnFamily? family)
    {
        if (family is null)
            return DefaultColumnFamily;

        return ReferenceEquals(family.Owner, this)
            ? family
            : throw new ArgumentException($"Column family '{family.Name}' belongs to a different database.");
    }

    private void FailQueue(Exception reason)
    {
        foreach (Writer queued in _queue)
            queued.Complete(reason);

        _queue.Clear();
        _queueSyncs = false;
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);

    private void ThrowIfFailed()
    {
        if (Volatile.Read(ref _failure) is not null)
            throw Stopped();
    }

    private SolariumException Stopped() => new("The database accepts no more writes after a WAL, flush or compaction failure.", _failure!);

    // ── Open ─────────────────────────────────────────────────────────────────────────────────────

    private static FileStream Lock(string directory)
    {
        // FileShare.None is an exclusive lock on every platform (flock on Unix).
        try
        {
            return new FileStream(Path.Combine(directory, "LOCK"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException e)
        {
            throw new SolariumException($"'{directory}' is in use by another process or another open database.", e);
        }
    }

    private static void PrepareFormat(IStorageEnvironment environment, string directory)
    {
        string path = Path.Combine(directory, FormatFileName);
        if (!environment.FileExists(path))
        {
            // A crash between the temporary file and the rename of the first FORMAT leaves FORMAT.tmp.
            if (environment.GetChildren(directory).Any(name => name != "LOCK" && !name.EndsWith(".tmp", StringComparison.Ordinal)))
                throw new SolariumException($"'{directory}' is not empty and is not a Solarium database.");

            DurableFiles.ReplaceText(environment, path, FormatVersion);
            return;
        }

        string version = Encoding.UTF8.GetString(DurableFiles.ReadAllBytes(environment, path));
        if (version == FormatVersion1)
            MigrateFromFormat1(environment, directory);
        else if (version == FormatVersion2 || version == FormatVersion3 || version == FormatVersion4)
        {
            // Format 3 differs from 2 in new WAL files only, which have a header and durable offsets;
            // the files of format 2 replay as WAL version 1. Format 4 differs from 3 in new SSTables
            // only, version 3, whose data blocks have a type and may be compressed; version 2 tables
            // still read. Format 5 differs from 4 in new SSTables only, version 4, whose index and
            // filter are partitioned; version 3 and 2 tables still read, as one partition. FORMAT
            // changes first, so a version of Solarium that reads an older format never sees a WAL
            // file it would cut as a torn tail, or a table it cannot read.
            DurableFiles.ReplaceText(environment, path, FormatVersion);
        }
        else if (version != FormatVersion)
            throw new SolariumException($"'{directory}' has format '{version}'; this version of Solarium reads '{FormatVersion}'.");
    }

    // Format 1: COLUMN_FAMILIES and one wal.log, no SSTables. Safe to run again after a crash.
    private static void MigrateFromFormat1(IStorageEnvironment environment, string directory)
    {
        Manifest manifest = new() { NextFileNumber = 2 };
        foreach ((int id, string name) in ColumnFamilyCatalog.Load(environment, directory))
            manifest.ColumnFamilies.Add(new ManifestFamily { Id = id, Name = name });

        string oldWal = Path.Combine(directory, "wal.log");
        if (environment.FileExists(oldWal))
        {
            environment.RenameFile(oldWal, Path.Combine(directory, "000001.log"));
            environment.FlushDirectory(directory);
        }

        if (environment.FileExists(Path.Combine(directory, "000001.log")))
            manifest.WalNumbers.Add(1);

        manifest.Save(environment, directory);
        DurableFiles.ReplaceText(environment, Path.Combine(directory, FormatFileName), FormatVersion);
        TryDelete(environment, Path.Combine(directory, ColumnFamilyCatalog.FileName));
    }

    // The manifest of a new database. A first open writes FORMAT, then MANIFEST, before any WAL or
    // SST file, so a directory without MANIFEST may hold only what a first open that a crash cut
    // short leaves. Anything else is a database whose MANIFEST was lost, and starting it over would
    // delete its tables as orphans and lose its WAL files' data.
    private static Manifest NewManifest(IStorageEnvironment environment, string directory)
    {
        string[] unknown = [.. environment.GetChildren(directory)
            .Where(name => name is not ("LOCK" or FormatFileName) && !name.EndsWith(".tmp", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)];
        if (unknown.Length > 0)
            throw new SolariumException(
                $"'{directory}' has no {Manifest.FileName} but holds {string.Join(", ", unknown.Take(5))}{(unknown.Length > 5 ? $" and {unknown.Length - 5} more" : "")}; " +
                $"it is a database whose {Manifest.FileName} is lost, and it is not opened so that nothing in it is deleted.");

        Manifest manifest = new();
        manifest.ColumnFamilies.Add(new ManifestFamily { Id = 0, Name = ColumnFamily.DefaultName });
        return manifest;
    }

    private static void AddMissingFamilies(IStorageEnvironment environment, string directory, Manifest manifest, IEnumerable<string> requested, bool create)
    {
        bool changed = !environment.FileExists(Path.Combine(directory, Manifest.FileName));
        foreach (string name in requested.Distinct(StringComparer.Ordinal))
        {
            ColumnFamilyCatalog.Validate(name);
            if (manifest.ColumnFamilies.Any(f => f.Name == name))
                continue;

            if (!create)
                throw new SolariumException($"Column family '{name}' does not exist, and CreateMissingColumnFamilies is false.");

            int id = manifest.ColumnFamilies.Max(f => f.Id) + 1;
            if (id > Manifest.MaxFamilyId)
                throw new SolariumException($"'{directory}' has the most column families a database can hold, {Manifest.MaxFamilyId + 1}.");

            manifest.ColumnFamilies.Add(new ManifestFamily { Id = id, Name = name });
            changed = true;
        }

        if (changed)
            manifest.Save(environment, directory);
    }

    /// <summary>A <see cref="CompactRange"/> call that waits for the compaction threads.</summary>
    private sealed class ManualCompaction(ColumnFamily family, byte[]? begin, byte[]? end)
    {
        /// <summary>The family to compact.</summary>
        public ColumnFamily Family { get; } = family;

        /// <summary>The first key of the range, included; null: unbounded.</summary>
        public byte[]? Begin { get; } = begin;

        /// <summary>The last key of the range, included; null: unbounded.</summary>
        public byte[]? End { get; } = end;

        /// <summary>The level the next compaction of the request starts from, under the state lock: each level is compacted once, from 0 down.</summary>
        public int NextLevel { get; set; }

        /// <summary>Set under the state lock when no level is left to compact: every compaction of the request is installed.</summary>
        public bool Done { get; set; }
    }

    /// <summary>
    /// The sequence numbers of one write group, or of the groups that one memtable stage takes, in
    /// WAL order, <paramref name="First"/> to <paramref name="Last"/>; and when commits are pipelined,
    /// the turn of the newest group: the count of groups appended up to it (0 otherwise).
    /// </summary>
    private readonly record struct GroupRange(long First, long Last, long Turn);

    /// <summary>A WAL file: the current one has a Log; older ones are only kept until their data is flushed.</summary>
    private sealed class WalFile(long number, string path)
    {
        /// <summary>The file number, which names the file and orders it among the WAL files.</summary>
        public long Number { get; } = number;

        /// <summary>The full path of the file.</summary>
        public string Path { get; } = path;

        /// <summary>The open log, or null once a newer file replaced this one.</summary>
        public WriteAheadLog? Log { get; set; }

        /// <summary>The highest sequence number in the file, or 0 when it holds no record.</summary>
        public long LastSequence { get; set; }

        /// <summary>The size of the file in bytes, as of the last group written to it.</summary>
        public long Length { get; set; }
    }

    /// <summary>
    /// A write that waits in the queue, with the signal that wakes only its thread: its batch, the
    /// batch's WAL record without its sequence numbers (<see cref="WriteAheadLog.EncodeRecord"/>), in
    /// the first <paramref name="recordLength"/> bytes of <paramref name="record"/>, and the column
    /// families it writes to.
    /// </summary>
    private sealed class Writer(WriteBatch batch, bool sync, byte[] record, int recordLength, ColumnFamily[] families)
    {
        /// <summary>The state of a writer whose turn has not come.</summary>
        public const int Waiting = 0;

        /// <summary>The state of a writer whose group is done, or failed.</summary>
        public const int Completed = 1;

        /// <summary>The state of a writer offered the leadership of the queue.</summary>
        public const int Leading = 2;

        /// <summary>The state of a writer whose leader launched its insert into the memtables.</summary>
        public const int Inserting = 3;

        /// <summary>
        /// The state of the leader of a group in the memtable queue that is offered the next memtable
        /// stage of a pipelined commit.
        /// </summary>
        public const int MemTableLeading = 4;

        private int _state;
        private ManualResetEventSlim? _wake;
        private bool _blocking;
        private Exception? _failure;

        /// <summary>The operations to write.</summary>
        public WriteBatch Batch { get; } = batch;

        /// <summary>True when the write must be flushed to stable storage before it returns.</summary>
        public bool Sync { get; } = sync;

        /// <summary>
        /// The batch's WAL record, without its first sequence number and CRC. A record of up to
        /// <see cref="MaxKeptRecord"/> bytes is in the buffer of the writer's thread: the leader
        /// copies it, and never writes it.
        /// </summary>
        public ReadOnlySpan<byte> Record => record.AsSpan(0, recordLength);

        /// <summary>
        /// The array that holds <see cref="Record"/> when it is this writer's alone, a record above
        /// <see cref="MaxKeptRecord"/> bytes, which no later write of the thread reuses: the leader
        /// may complete it in place. Null for a record in the thread's kept buffer.
        /// </summary>
        public byte[]? OwnedRecord => recordLength > MaxKeptRecord ? record : null;

        /// <summary>The length of <see cref="Record"/>, which the group size cap counts.</summary>
        public int RecordLength => recordLength;

        /// <summary>The number of operations in the batch, so the number of sequence numbers it takes.</summary>
        public int OperationCount { get; } = batch.Count;

        /// <summary>The column families that the batch writes to, each once.</summary>
        public ColumnFamily[] Families { get; } = families;

        /// <summary>
        /// True from just before the writer blocks on its event until it wakes. A hint for the
        /// leader, which launches the inserts of the members that spin and inserts the batches of
        /// the blocked ones itself: the writer can block or wake just after the read.
        /// </summary>
        public bool IsBlocked => Volatile.Read(ref _blocking);

        /// <summary>
        /// The sequence number of the batch's first operation, set by the leader before the WAL
        /// append, and read by the writer after <see cref="LaunchInsert"/>.
        /// </summary>
        public long FirstSequence { get; set; }

        /// <summary>
        /// Tell the writer to insert its batch into the memtables. The leader calls it after the WAL
        /// append, on a writer it took from the queue, so the writer is waiting. The state is
        /// published after <see cref="FirstSequence"/> and before the event is read.
        /// </summary>
        public void LaunchInsert()
        {
            // A full fence between the state and the read of the event; see WaitForTurn.
            Interlocked.Exchange(ref _state, Inserting);
            Volatile.Read(ref _wake)?.Set();
        }

        /// <summary>
        /// Wait again: after the writer's insert, for the completion, called before the writer counts
        /// its insert done, so the leader cannot complete it first; or after the writer led a group's
        /// WAL stage, for its memtable stage, called before the group joins the memtable queue, so no
        /// stage can launch or complete the writer first. The event is reset before the state, as in
        /// <see cref="WithdrawOffer"/>.
        /// </summary>
        public void WaitAgain()
        {
            _wake?.Reset();
            Interlocked.Exchange(ref _state, Waiting);
        }

        /// <summary>
        /// Finish the writer, with the failure of its group or null. The state is published after the
        /// failure and before the event is read, so a writer that sees the new state after its spin
        /// or its wait also sees the failure.
        /// </summary>
        public void Complete(Exception? failure)
        {
            _failure = failure;
            // A full fence between the state and the read of the event; see WaitForTurn.
            Interlocked.Exchange(ref _state, Completed);
            Volatile.Read(ref _wake)?.Set();
        }

        /// <summary>
        /// Offer leadership to the writer, the oldest in the queue, under the write lock: true when the
        /// writer was waiting, and must now be woken with <see cref="Wake"/>. False when an offer that
        /// the writer has not taken yet is still in its state. A full fence between the state and the
        /// read of the event in <see cref="Wake"/>; see <see cref="WaitForTurn"/>.
        /// </summary>
        public bool OfferLeadership() => Interlocked.CompareExchange(ref _state, Leading, Waiting) == Waiting;

        /// <summary>
        /// Wake the writer after <see cref="OfferLeadership"/>, outside the write lock. A wake that comes
        /// after the writer moved on finds it waiting for something else, and the writer checks its state
        /// again (see <see cref="WaitForTurn"/>).
        /// </summary>
        public void Wake() => Volatile.Read(ref _wake)?.Set();

        /// <summary>
        /// Offer the next memtable stage to the leader of the oldest group in the memtable queue. No
        /// stage took the group yet, so the writer is waiting, and only this offer changes its state.
        /// </summary>
        public void OfferMemTableLeadership()
        {
            // A full fence between the state and the read of the event; see WaitForTurn.
            Interlocked.Exchange(ref _state, MemTableLeading);
            Volatile.Read(ref _wake)?.Set();
        }

        /// <summary>
        /// Take back an offer that a running writer overtook, so the writer waits for its group. The
        /// event is reset before the state, so a completion that comes at any point still wakes it.
        /// The event is null when the offer came during the spin.
        /// </summary>
        public void WithdrawOffer()
        {
            _wake?.Reset();
            Interlocked.CompareExchange(ref _state, Waiting, Leading);
        }

        /// <summary>
        /// Wait until a leader completes this writer (<see cref="Completed"/>), offers leadership to
        /// it (<see cref="Leading"/>), launches its insert (<see cref="Inserting"/>), or offers it a
        /// memtable stage (<see cref="MemTableLeading"/>), and return that state. Spin for <paramref name="spinTicks"/> (<see cref="Stopwatch"/> ticks) first: a
        /// blocked writer puts a thread wake on the path of its group, and most unsynced groups end
        /// within the long spin. Zero blocks at once. A writer that blocks adds one to
        /// <paramref name="blockedWaits"/> before it blocks, and returns <paramref name="blocked"/>.
        /// </summary>
        public int WaitForTurn(long spinTicks, ref long blockedWaits, out bool blocked)
        {
            long start = Stopwatch.GetTimestamp();
            while (Volatile.Read(ref _state) == Waiting && Stopwatch.GetTimestamp() - start < spinTicks)
                Thread.SpinWait(1);

            blocked = false;
            while (Volatile.Read(ref _state) == Waiting)
            {
                // Most waits end in the spin, so the event exists only for a writer that blocks. The
                // writer publishes the event and then reads the state; the leader writes the state and
                // then reads the event. Both have a full fence between the two, so at least one of
                // them sees the other's write: the writer sees the new state and does not block, or
                // the leader sees the event and sets it.
                _wake ??= new ManualResetEventSlim(initialState: false, spinCount: 0);
                Volatile.Write(ref _blocking, true);
                Interlocked.MemoryBarrier();
                if (Volatile.Read(ref _state) != Waiting)
                    break;

                if (!blocked)
                    Interlocked.Increment(ref blockedWaits);
                blocked = true;
                _wake.Wait();

                // An offer or a launch that the writer saw in a spin sets the event after its state,
                // so the set can come after the writer took the offer back or finished its insert,
                // and wake it with its state still Waiting. Reset, and check the state again.
                if (Volatile.Read(ref _state) == Waiting)
                    _wake.Reset();
            }

            if (_blocking)
                Volatile.Write(ref _blocking, false);
            return Volatile.Read(ref _state);
        }

        /// <summary>Throw the failure of the writer's group, if there was one.</summary>
        public void ThrowIfFailed()
        {
            switch (_failure)
            {
                case null:
                    return;
                case ObjectDisposedException or SolariumException:
                    throw _failure;
                default:
                    throw new SolariumException("The write failed in the WAL.", _failure);
            }
        }
    }
}
