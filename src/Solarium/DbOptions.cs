using Solarium.Engine;
using Solarium.Storage;

namespace Solarium;

/// <summary>Options for <see cref="SolariumDb.Open"/>.</summary>
public sealed record DbOptions
{
    /// <summary>Create the database directory when it does not exist. Default: true.</summary>
    public bool CreateIfMissing { get; init; } = true;

    /// <summary>
    /// Create the column families passed to <see cref="SolariumDb.Open"/> that do not exist yet.
    /// When false, a missing one is an error. Default: false, as in RocksDB.
    /// </summary>
    public bool CreateMissingColumnFamilies { get; init; }

    /// <summary>Where file I/O goes. Default: <see cref="DefaultStorageEnvironment"/>.</summary>
    public IStorageEnvironment Environment { get; init; } = DefaultStorageEnvironment.Instance;

    /// <summary>Options for every column family that <see cref="ColumnFamilyOptions"/> does not name.</summary>
    public ColumnFamilyOptions DefaultColumnFamilyOptions { get; init; } = new();

    /// <summary>Options for named column families. Null: every family uses <see cref="DefaultColumnFamilyOptions"/>.</summary>
    public IReadOnlyDictionary<string, ColumnFamilyOptions>? ColumnFamilyOptions { get; init; }

    /// <summary>
    /// When the live WAL files are larger than this, flush the column families that keep the oldest
    /// one alive, so a family with few writes cannot keep WAL files forever. 0 (the default, as in
    /// RocksDB): 4 times the sum of WriteBufferSize × MaxWriteBufferNumber over all families.
    /// </summary>
    public long MaxTotalWalSize { get; init; }

    /// <summary>
    /// The rate, in bytes per second, that writes slow down to while a column family's level 0 holds
    /// <see cref="ColumnFamilyOptions.Level0SlowdownWritesTrigger"/> files. Default: 16 MiB/s, the
    /// rate RocksDB gives a <c>delayed_write_rate</c> of 0. Must be positive.
    /// </summary>
    public long DelayedWriteRate { get; init; } = 16L * 1024 * 1024;

    /// <summary>
    /// The most bytes of WAL records that one write group takes, as RocksDB's
    /// <c>max_write_batch_group_size_bytes</c>: when the leader's batch is at most an eighth of it,
    /// the group takes up to the leader's size plus an eighth. A batch larger than this is a group
    /// of its own. Default: 1 MiB, as in RocksDB. Must be positive.
    /// </summary>
    public long MaxWriteBatchGroupSizeBytes { get; init; } = 1L << 20;

    /// <summary>
    /// The most compactions that run at once, across every column family, as RocksDB's
    /// <c>max_background_compactions</c>; the database runs one thread for each. Two compactions never
    /// take one file, never run on level 0 of one family at once, and never write overlapping files
    /// into one level. Flushes have a thread of their own. Default: 1, the compactions of RocksDB's
    /// default of 2 background jobs. Must be positive.
    /// </summary>
    public int MaxBackgroundCompactions { get; init; } = 1;

    /// <summary>
    /// Paces what flushes and compactions write, as RocksDB's <c>rate_limiter</c>: before each megabyte of
    /// a table, the writer waits for its share of the rate, the flushes before the compactions. The WAL,
    /// the manifest and checkpoints are not paced. The host owns it, and one limiter can pace several
    /// databases. Default: none.
    /// </summary>
    public RateLimiter? RateLimiter { get; init; }

    /// <summary>
    /// Every how many bytes a flush or a compaction starts the writeback of the table it writes, as
    /// RocksDB's <c>bytes_per_sync</c> (<see cref="Storage.IStorageFile.SyncRange"/>), so the table reaches
    /// the drive as it is written instead of all at its last sync, where it would hold up the WAL's syncs.
    /// It makes nothing durable before the last sync. 0: never. Default: 0, or 1 MiB when
    /// <see cref="RateLimiter"/> is set, as in RocksDB. Must not be negative.
    /// </summary>
    public long BytesPerSync { get; init; }

    /// <summary>
    /// Caps the memtable memory of every database that shares it, as RocksDB's <c>write_buffer_manager</c>:
    /// when the memtables of all of them pass its cap, the next database to write flushes its column family
    /// with the oldest memtable. The host owns it. It overrides <see cref="DbWriteBufferSize"/>, as in
    /// RocksDB. Default: none.
    /// </summary>
    public WriteBufferManager? WriteBufferManager { get; init; }

    /// <summary>
    /// The memtable memory of this database across its column families, as RocksDB's
    /// <c>db_write_buffer_size</c>: a <see cref="Solarium.WriteBufferManager"/> of this cap that no other
    /// database shares. Ignored when <see cref="WriteBufferManager"/> is set. 0 (the default): none. Must
    /// not be negative.
    /// </summary>
    public long DbWriteBufferSize { get; init; }

    /// <summary>
    /// How long a writer that waits for its group to commit spins before it blocks, while most such
    /// spins end in time. Default: 20 µs. Most unsynced groups end within it, so a waiting writer
    /// seldom needs a thread wake, which is on the path of its group. When writers outnumber CPUs,
    /// the spin uses less CPU time than blocking and waking; when every waiting writer has a CPU, it
    /// uses CPU time that would otherwise be idle. When most spins end in a block anyway, writers
    /// spin for <see cref="WriterShortSpin"/> instead; see <see cref="WriterSpinProbeInterval"/>. A
    /// writer that waits for a WAL flush blocks at once: the flush takes far longer than any spin.
    /// No spin is longer than this. Internal, so the benchmarks can measure other values.
    /// </summary>
    internal TimeSpan WriterSpin { get; init; } = TimeSpan.FromMicroseconds(20);

    /// <summary>
    /// How long a waiting writer spins when most spins of <see cref="WriterSpin"/> end in a block, so
    /// a long spin would only hold a CPU that the leader or the storage environment's I/O thread can
    /// use. Default: 1 µs, as RocksDB's spin before its yields. A value larger than
    /// <see cref="WriterSpin"/> gives <see cref="WriterSpin"/>. Internal, so the benchmarks can
    /// measure other values.
    /// </summary>
    internal TimeSpan WriterShortSpin { get; init; } = TimeSpan.FromMicroseconds(1);

    /// <summary>
    /// While the spin is long, one turn of a waiting writer in this many is a probe: it spins for the
    /// whole <see cref="WriterSpin"/>, and whether its turn came in time or it blocked updates a
    /// credit that the database keeps. The other turns spin for <see cref="WriterSpin"/> while the
    /// credit shows that at most about a quarter of the probes block, and for
    /// <see cref="WriterShortSpin"/> otherwise. While the spin is short, a probe among short spins
    /// would wait for the wakes of the writers that blocked before it, and block more often than a
    /// long spin among long spins. So once in 32 times this many turns, this many turns spin long,
    /// and the second half of them are probes: the credit sees when long spins end in time again.
    /// Default: 256, as the sampling of RocksDB's adaptive yield credit. 0 turns the adaptation off:
    /// every turn spins for <see cref="WriterSpin"/>. Internal, so the tests can fix the spin.
    /// </summary>
    internal int WriterSpinProbeInterval { get; init; } = 256;

    /// <summary>
    /// In a write group of several writers, each writer that still spins inserts its own batch into
    /// the memtables at the same time as the others, after the leader's WAL append, and the leader
    /// inserts its own batch and those of the writers that blocked. The group is published when the
    /// last insert is done. Off: the leader inserts every batch of the group. A group of one is
    /// always inserted by its writer. Default: true, as RocksDB's
    /// <c>allow_concurrent_memtable_write</c>. Internal, so the tests can run both paths and the
    /// benchmarks can compare them.
    /// </summary>
    internal bool AllowConcurrentMemtableWrite { get; init; } = true;

    /// <summary>
    /// Time the stages of each write group on the leader's thread: the preparation, the encode, the
    /// WAL append, the memtable inserts with the publication, the hand-off of the leadership, and
    /// the gap until the next group starts; see <see cref="SolariumDb.GroupStages"/>. Each stage
    /// costs one timestamp. Default: false. Internal, so the benchmarks can show where the time of
    /// a group goes.
    /// </summary>
    internal bool MeasureGroupStages { get; init; }

    /// <summary>
    /// Commit write groups in two stages that overlap, as RocksDB's <c>enable_pipelined_write</c>:
    /// the leader of a group appends it to the WAL, adds it to a memtable queue and gives up its
    /// leadership, so the next group's append starts. One memtable stage at a time takes every group
    /// in the queue, inserts it into the memtables and publishes it, as RocksDB's memtable writer
    /// groups. The queue follows the WAL order, so groups still become visible in that order, each
    /// whole. Off: the leader inserts and publishes its group before the next group starts. Default:
    /// true, unlike RocksDB, whose default is false: in the <c>Put</c> benchmarks it makes 3 to 16
    /// unsynced writers 5% to 15% faster, and costs a single writer 1.5% to 1.7%. Internal, so the
    /// tests can run both paths and the benchmarks can compare them.
    /// </summary>
    internal bool EnablePipelinedWrite { get; init; } = true;

    /// <summary>
    /// Called by the leader of a memtable stage while <see cref="EnablePipelinedWrite"/> is on, with
    /// the first sequence number of the stage: after it took its groups, before it waits for the
    /// lock that keeps memtable switches out, with no lock held. The groups that append meanwhile
    /// wait for the next stage. An exception from it fails the stage as an insert failure would.
    /// Internal, so the tests can hold a group between its two stages.
    /// </summary>
    internal Action<long>? BeforeMemTableStage { get; init; }

    /// <summary>
    /// Called by the leader of a memtable stage while <see cref="EnablePipelinedWrite"/> is on, with
    /// the first sequence number of the stage, before it inserts. The leader holds the lock that keeps memtable switches out. An exception from it
    /// fails the stage as an insert failure would. Internal, so the tests can hold a group in its
    /// memtable stage.
    /// </summary>
    internal Action<long>? BeforeMemTableInsert { get; init; }

    /// <summary>
    /// Called by the leader of a WAL group, with the first sequence number of the group, after it gave
    /// up its leadership and before it wakes the writer it offered leadership to, with no lock held.
    /// Internal, so the tests can hold the wake while other writers lead.
    /// </summary>
    internal Action<long>? AfterWalLeadership { get; init; }

    /// <summary>
    /// Called by <see cref="SolariumDb.Get"/>, <see cref="SolariumDb.MultiGet"/> and
    /// <see cref="SolariumDb.NewIterator"/> after they enter the column family's version and before
    /// they take their snapshot, with no lock held. Internal, so the tests can hold a read between
    /// the two while a compaction replaces the version.
    /// </summary>
    internal Action? AfterReadVersion { get; init; }

    /// <summary>
    /// Called after each manifest save has replaced the file, before the database relies on it. An
    /// exception from it fails the save as a failed directory flush after the rename would: the
    /// manifest on disk is the new one. Internal, so the tests can fail a save that reached the disk.
    /// </summary>
    internal Action? AfterManifestSave { get; init; }

    /// <summary>
    /// Called on a compaction thread after it picks a compaction and before it runs it, with no lock
    /// held. It must not throw. Internal, so the tests can hold compactions back while level 0 grows.
    /// </summary>
    internal Action? BeforeCompaction { get; init; }

    /// <summary>
    /// Called by a compaction thread with the compaction it picked, after <see cref="BeforeCompaction"/>
    /// and before it moves or merges the tables, with no lock held. An exception from it fails the
    /// compaction as a failed merge would. Internal, so the tests can hold or fail one compaction
    /// while others run.
    /// </summary>
    internal Action<Compaction>? CompactionStarting { get; init; }

    /// <summary>The options for <paramref name="family"/>: its entry in <see cref="ColumnFamilyOptions"/>, else the default options.</summary>
    internal ColumnFamilyOptions OptionsFor(string family)
    {
        return ColumnFamilyOptions is not null && ColumnFamilyOptions.TryGetValue(family, out ColumnFamilyOptions? options)
            ? options
            : DefaultColumnFamilyOptions;
    }
}
