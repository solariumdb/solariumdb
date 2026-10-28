using System.Diagnostics;
using System.Text;
using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// <see cref="SolariumDb.CreateCheckpoint"/>: the copy opens as a database that reads, in every column
/// family, as the source did at one sequence number, from every kind of source (level 6, level 0, a
/// memtable that waits for its flush, the active memtable), while writes, flushes and compactions go
/// on; it holds no WAL; and a copy that fails or is cut short never opens.
/// </summary>
public sealed class CheckpointTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly string[] Families = ["kv", "locks"];

    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    private string CheckpointPath => _directory.Combine("checkpoint");

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_reads_as_the_source_did_from_every_kind_of_source_and_holds_no_wal(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.StartsWith(DbPath, StringComparison.Ordinal) && path.EndsWith(".sst", StringComparison.Ordinal) };
        DbOptions options = Options(hooks);

        Dictionary<string, List<string>> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, options, Families))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            ColumnFamily kv = db.GetColumnFamily("kv");
            ColumnFamily locks = db.GetColumnFamily("locks");
            FillKv(db, kv);

            // A flush of locks that waits at its table's flush leaves its memtable immutable.
            db.Put(B("lock-a"), B("1"), locks);
            db.Put(B("lock-b"), B("1"), locks);
            hooks.FlushStarted.Reset();
            hooks.FlushGate.Reset();
            db.Flush(locks, wait: false);
            Assert.True(hooks.FlushStarted.Wait(Timeout));
            db.Put(B("lock-b"), B("2"), locks);
            db.Delete(B("lock-a"), locks);
            db.Put(B("lock-c"), B("2"), locks);
            Assert.Single(locks.Version.Immutables);

            expected = ScanAll(db);
            long flushes = db.FlushCount;
            long sequence = db.LatestSequenceNumber;

            db.CreateCheckpoint(CheckpointPath);

            // The source was not flushed for it, and writes after it are not in the copy.
            Assert.Equal(flushes, db.FlushCount);
            db.Put(B("k500"), B("after"), kv);
            db.Delete(B("k000"), kv);
            db.Put(B("lock-z"), B("after"), locks);
            Assert.Equal(sequence + 3, db.LatestSequenceNumber);
        }

        Assert.Equal(["FORMAT", "LOCK", "MANIFEST"], Directory.GetFiles(CheckpointPath).Select(Path.GetFileName).Where(name => !name!.EndsWith(".sst", StringComparison.Ordinal)).Order(StringComparer.Ordinal));
        using (SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment)))
        {
            Assert.Equal(["default", "kv", "locks"], copy.ColumnFamilies.Select(f => f.Name).Order(StringComparer.Ordinal));
            Assert.Equal(expected, ScanAll(copy));
            Assert.Null(copy.Get(B("k500"), copy.GetColumnFamily("kv")));
            Assert.Equal("1:0", S(copy.Get(B("k000"), copy.GetColumnFamily("kv"))));

            // The copy takes writes of its own, numbered past what it holds.
            copy.Put(B("k600"), B("copy"), copy.GetColumnFamily("kv"));
            Assert.Equal("copy", S(copy.Get(B("k600"), copy.GetColumnFamily("kv"))));
        }

        using (SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment)))
        {
            Assert.Equal("copy", S(copy.Get(B("k600"), copy.GetColumnFamily("kv"))));
            Assert.Equal("2", S(copy.Get(B("lock-b"), copy.GetColumnFamily("locks"))));
        }
    }

    // The checkpoint waits at its first write, after it took its sequence number and before it read
    // the locks memtable; the writes meanwhile go to the memtables it copies, and must stay out.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task Writes_to_the_memtables_it_copies_after_the_checkpoint_starts_are_not_in_the_copy(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.StartsWith(CheckpointPath, StringComparison.Ordinal) };

        Dictionary<string, List<string>> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(hooks), Families))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            ColumnFamily kv = db.GetColumnFamily("kv");
            ColumnFamily locks = db.GetColumnFamily("locks");
            FillKv(db, kv);
            for (int i = 0; i < 20; i++)
                db.Put(B($"lock-{i:D2}"), B("before"), locks);

            expected = ScanAll(db);
            MemTable[] memTables = [kv.Version.Active, locks.Version.Active];

            hooks.WriteGate.Reset();
            Task checkpoint = Task.Run(() => db.CreateCheckpoint(CheckpointPath));
            Assert.True(hooks.WriteStarted.Wait(Timeout));

            db.Put(B("lock-05"), B("after"), locks);
            db.Put(B("lock-99"), B("after"), locks);
            db.DeleteRange(B("lock-10"), B("lock-15"), locks);
            db.Delete(B("lock-01"), locks);
            db.Put(B("k999"), B("after"), kv);
            Assert.Equal(memTables, new[] { kv.Version.Active, locks.Version.Active });

            hooks.WriteGate.Set();
            await checkpoint.WaitAsync(Timeout);
        }

        using SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
        Assert.Equal(expected, ScanAll(copy));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_checkpoint_taken_during_writes_holds_a_prefix_of_each_writer_and_every_batch_whole(string environment)
    {
        const int writers = 4;
        using EnvironmentScope scope = Environments.Create(environment);
        DbOptions options = Options(scope.Environment) with
        {
            DefaultColumnFamilyOptions = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, Level0FileNumCompactionTrigger = 2 },
        };

        long[] acknowledged = new long[writers];
        long[] beforeCheckpoint = new long[writers];
        using (SolariumDb db = SolariumDb.Open(DbPath, options, Families))
        {
            ColumnFamily kv = db.GetColumnFamily("kv");
            ColumnFamily locks = db.GetColumnFamily("locks");
            using CancellationTokenSource stop = new();
            // Each writer runs until the test stops it, on a thread of its own: on a thread pool that other tests
            // keep busy, a writer could start too late for the waits below.
            Task[] tasks = new Task[writers];
            for (int w = 0; w < writers; w++)
            {
                int writer = w;
                tasks[w] = Task.Factory.StartNew(() =>
                {
                    byte[] padding = new byte[200];
                    for (long i = 0; !stop.IsCancellationRequested; i++)
                    {
                        // One batch across both families: the copy must hold both halves or neither.
                        WriteBatch batch = new();
                        batch.Put(Key(writer, i), padding, kv);
                        batch.Put(Key(writer, i), B(i.ToString()), locks);
                        db.Write(batch);
                        Volatile.Write(ref acknowledged[writer], i + 1);
                    }
                }, TaskCreationOptions.LongRunning);
            }

            WaitUntil(() => acknowledged.Min() > 2_000);
            for (int w = 0; w < writers; w++)
                beforeCheckpoint[w] = Volatile.Read(ref acknowledged[w]);

            db.CreateCheckpoint(CheckpointPath);
            WaitUntil(() => Enumerable.Range(0, writers).All(w => Volatile.Read(ref acknowledged[w]) > beforeCheckpoint[w] + 500));
            stop.Cancel();
            await Task.WhenAll(tasks).WaitAsync(Timeout);
            Assert.True(db.FlushCount > 0, "The writes never flushed, so the copy read memtables only.");
        }

        using SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
        for (int w = 0; w < writers; w++)
        {
            List<long> kvKeys = Writes(copy, copy.GetColumnFamily("kv"), w);
            List<long> lockKeys = Writes(copy, copy.GetColumnFamily("locks"), w);
            Assert.Equal(kvKeys, lockKeys);
            Assert.Equal(Enumerable.Range(0, kvKeys.Count).Select(i => (long)i), kvKeys);
            Assert.True(kvKeys.Count >= beforeCheckpoint[w], $"Writer {w}: {kvKeys.Count} writes in the copy, {beforeCheckpoint[w]} acknowledged before the checkpoint.");
            Assert.True(kvKeys.Count < Volatile.Read(ref acknowledged[w]), $"Writer {w}: the copy has every write, even those after the checkpoint.");
        }

        static byte[] Key(int writer, long i) => B($"w{writer}-{i:D10}");

        static List<long> Writes(SolariumDb db, ColumnFamily family, int writer)
        {
            List<long> ids = [];
            byte[] prefix = B($"w{writer}-");
            using Iterator iterator = db.NewIterator(family);
            for (iterator.Seek(prefix); iterator.Valid() && iterator.Key().StartsWith(prefix); iterator.Next())
                ids.Add(long.Parse(S(iterator.Key()[prefix.Length..])));

            return ids;
        }
    }

    // With links, the checkpoint waits at its first link; with copies, at its first write.
    [Theory]
    [MemberData(nameof(EnvironmentsAndLinks))]
    public async Task A_compaction_while_the_copy_runs_does_not_delete_the_tables_it_copies(string environment, bool link)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.StartsWith(CheckpointPath, StringComparison.Ordinal), LinkFiles = link };
        DbOptions options = Options(hooks);

        List<long> copied;
        using (SolariumDb db = SolariumDb.Open(DbPath, options, Families))
        {
            using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
            ColumnFamily kv = db.GetColumnFamily("kv");
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 100; i++)
                    db.Put(B($"k{i:D3}"), B($"{round}:{i}"), kv);

                db.Flush(kv);
            }

            List<string> expected = ScanFamily(db, kv);
            copied = [.. kv.Version.Level0.Select(t => t.Number)];
            Assert.Equal(3, copied.Count);

            hooks.WriteGate.Reset();
            Task checkpoint = Task.Run(() => db.CreateCheckpoint(CheckpointPath));
            Assert.True(hooks.WriteStarted.Wait(Timeout));

            // The merge replaces the three tables; the checkpoint still holds them.
            db.CompactRange(null, null, kv);
            Assert.Empty(kv.Version.Level0);
            Assert.Subset(TableNumbers(DbPath).ToHashSet(), copied.ToHashSet());

            hooks.WriteGate.Set();
            await checkpoint.WaitAsync(Timeout);

            // Once the copy is done, nothing holds them.
            WaitUntil(() => !TableNumbers(DbPath).Intersect(copied).Any());
            Assert.Equal(expected, ScanFamily(db, kv));
        }

        using SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
        Assert.Equal(copied.Order(), copy.GetColumnFamily("kv").Version.Level0.Select(t => t.Number).Order());
        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"k{i:D3}=2:{i}"), ScanFamily(copy, copy.GetColumnFamily("kv")));
    }

    // A failing table fails at its first link, or with copies at its first write.
    [Theory]
    [InlineData(".sst", true)]
    [InlineData(".sst", false)]
    [InlineData("MANIFEST", true)]
    [InlineData("FORMAT", true)]
    public void A_checkpoint_that_fails_leaves_no_files_and_the_source_keeps_writing(string failing, bool link)
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance)
        {
            FailingPaths = path => path.StartsWith(CheckpointPath, StringComparison.Ordinal) && Path.GetFileName(path).Contains(failing, StringComparison.Ordinal),
            LinkFiles = link,
        };
        using SolariumDb db = SolariumDb.Open(DbPath, Options(hooks), Families);
        ColumnFamily kv = db.GetColumnFamily("kv");
        FillKv(db, kv);

        hooks.FailWrites = true;
        SolariumException failure = Assert.Throws<SolariumException>(() => db.CreateCheckpoint(CheckpointPath));
        Assert.Contains(CheckpointPath, failure.Message, StringComparison.Ordinal);
        Assert.Equal(["LOCK"], Directory.GetFileSystemEntries(CheckpointPath).Select(Path.GetFileName));

        db.Put(B("k700"), B("after"), kv);
        db.Flush(kv);
        Assert.Equal("after", S(db.Get(B("k700"), kv)));

        // The directory exists, so a second attempt goes elsewhere, and succeeds once writes do.
        hooks.FailWrites = false;
        Assert.Throws<ArgumentException>(() => db.CreateCheckpoint(CheckpointPath));
        db.CreateCheckpoint(CheckpointPath + "-2");
        using SolariumDb copy = SolariumDb.Open(CheckpointPath + "-2", Options(DefaultStorageEnvironment.Instance));
        Assert.Equal("after", S(copy.Get(B("k700"), copy.GetColumnFamily("kv"))));
    }

    // The checkpoint's tables are links: it writes none of their bytes, and each name keeps the file
    // after the other goes, the source's to a compaction and the checkpoint's to a delete.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_links_its_tables_and_each_name_keeps_the_file_when_the_other_goes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        RecordingEnvironment recorder = new(scope.Environment);
        Dictionary<string, List<string>> expected;
        List<long> linked;
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(recorder), Families))
        {
            ColumnFamily kv = db.GetColumnFamily("kv");
            ColumnFamily locks = db.GetColumnFamily("locks");
            FillKv(db, kv);
            db.Put(B("lock-a"), B("1"), locks);
            db.Flush(locks);
            linked = [.. db.ColumnFamilies.SelectMany(f => f.Version.Level0.Concat(f.Version.Level6.Tables)).Select(t => t.Number)];
            Assert.True(linked.Count >= 3);
            expected = ScanAll(db);

            recorder.Operations.Clear();
            db.CreateCheckpoint(CheckpointPath);
            List<string> operations = [.. recorder.Operations];
            foreach (long number in linked)
            {
                string path = Path.Combine(CheckpointPath, TableFormat.FileName(number));
                Assert.Contains($"link {path}", operations);
                Assert.DoesNotContain($"open {path}", operations);
            }

            // The source's names of the kv tables go: new rows, then a merge of every kv table into new ones.
            for (int i = 0; i < 200; i++)
                db.Put(B($"k{i:D3}"), B($"4:{i}"), kv);

            List<long> replaced = [.. kv.Version.Level0.Concat(kv.Version.Level6.Tables).Select(t => t.Number)];
            db.Flush(kv);
            db.CompactRange(null, null, kv);
            WaitUntil(() => !TableNumbers(DbPath).Intersect(replaced).Any());
        }

        using (SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment)))
            Assert.Equal(expected, ScanAll(copy));

        Directory.Delete(CheckpointPath, recursive: true);
        using SolariumDb reopened = SolariumDb.Open(DbPath, Options(scope.Environment), Families);
        Assert.Equal(Enumerable.Range(0, 200).Select(i => $"k{i:D3}=4:{i}").Concat(Enumerable.Range(200, 10).Select(i => $"k{i:D3}=3:{i}")),
            ScanFamily(reopened, reopened.GetColumnFamily("kv")));
        Assert.Equal(["lock-a=1"], ScanFamily(reopened, reopened.GetColumnFamily("locks")));
    }

    // The first refused link makes the checkpoint copy that table and every later one, in every
    // family, without asking again, as RocksDB does after its first NotSupported.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_copies_its_tables_once_the_file_system_refuses_a_link(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { LinkFiles = false };
        Dictionary<string, List<string>> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(hooks), Families))
        {
            ColumnFamily locks = db.GetColumnFamily("locks");
            FillKv(db, db.GetColumnFamily("kv"));
            db.Put(B("lock-a"), B("1"), locks);
            db.Flush(locks);
            expected = ScanAll(db);

            db.CreateCheckpoint(CheckpointPath);
            Assert.Equal(1, hooks.LinkAttempts);
        }

        using SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
        Assert.Equal(expected, ScanAll(copy));
    }

    // A crash leaves whatever the copy had written before it; the copy writes FORMAT last.
    [Fact]
    public void A_checkpoint_cut_short_before_its_format_file_does_not_open()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(DefaultStorageEnvironment.Instance), Families))
        {
            FillKv(db, db.GetColumnFamily("kv"));
            db.CreateCheckpoint(CheckpointPath);
        }

        File.Delete(Path.Combine(CheckpointPath, "FORMAT"));
        SolariumException refusal = Assert.Throws<SolariumException>(() => SolariumDb.Open(CheckpointPath, Options(DefaultStorageEnvironment.Instance)));
        Assert.Contains("not a Solarium database", refusal.Message, StringComparison.Ordinal);

        File.Delete(Path.Combine(CheckpointPath, "MANIFEST"));
        Assert.Throws<SolariumException>(() => SolariumDb.Open(CheckpointPath, Options(DefaultStorageEnvironment.Instance)));
    }

    // The order that keeps a copy cut short from opening: every table and MANIFEST are durable
    // before FORMAT is renamed into place, and the parent directory is flushed after it.
    [Fact]
    public void A_checkpoint_writes_its_tables_then_manifest_then_format()
    {
        RecordingEnvironment recorder = new(DefaultStorageEnvironment.Instance);
        using SolariumDb db = SolariumDb.Open(DbPath, Options(recorder), Families);
        FillKv(db, db.GetColumnFamily("kv"));

        recorder.Operations.Clear();
        db.CreateCheckpoint(CheckpointPath);

        List<string> operations = [.. recorder.Operations.Where(o => o.Contains(CheckpointPath, StringComparison.Ordinal))];
        int format = operations.IndexOf($"rename {Path.Combine(CheckpointPath, "FORMAT")}");
        int manifest = operations.IndexOf($"rename {Path.Combine(CheckpointPath, "MANIFEST")}");
        int lastTable = operations.FindLastIndex(o => (o.StartsWith("open ", StringComparison.Ordinal) || o.StartsWith("link ", StringComparison.Ordinal)) && o.EndsWith(".sst", StringComparison.Ordinal));
        Assert.True(lastTable >= 0 && lastTable < manifest && manifest < format, string.Join("\n", operations));
        Assert.Contains($"flush-directory {CheckpointPath}", operations[manifest..format]);
        Assert.Equal($"flush-directory {CheckpointPath}", operations[format + 1]);
        Assert.Equal($"flush-directory {_directory.Path}", recorder.Operations[^1]);
    }

    [Fact]
    public void A_checkpoint_refuses_a_directory_or_file_that_exists_and_a_disposed_database()
    {
        SolariumDb db = SolariumDb.Open(DbPath, Options(DefaultStorageEnvironment.Instance));
        Directory.CreateDirectory(CheckpointPath);
        Assert.Throws<ArgumentException>(() => db.CreateCheckpoint(CheckpointPath));
        File.WriteAllText(CheckpointPath + ".file", "");
        Assert.Throws<ArgumentException>(() => db.CreateCheckpoint(CheckpointPath + ".file"));
        Assert.Throws<ArgumentException>(() => db.CreateCheckpoint(""));

        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.CreateCheckpoint(CheckpointPath + "-2"));
        Assert.False(Directory.Exists(CheckpointPath + "-2"));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_checkpoint_of_an_opened_checkpoint_reads_the_same(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Dictionary<string, List<string>> expected;
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(scope.Environment), Families))
        {
            FillKv(db, db.GetColumnFamily("kv"));
            expected = ScanAll(db);
            db.CreateCheckpoint(CheckpointPath);
        }

        using (SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment)))
            copy.CreateCheckpoint(CheckpointPath + "-2");

        using SolariumDb second = SolariumDb.Open(CheckpointPath + "-2", Options(scope.Environment));
        Assert.Equal(expected, ScanAll(second));
    }

    // What a backup does to its copy: delete what is newer than its cut, flush, and compact, so no
    // byte of a deleted value stays in any file of the copy.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Deletes_flushed_and_compacted_in_an_opened_checkpoint_leave_no_byte_of_the_deleted_values(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, Options(scope.Environment), Families))
        {
            ColumnFamily kv = db.GetColumnFamily("kv");
            for (int i = 0; i < 100; i++)
                db.Put(B($"k{i:D3}"), B($"old-{i:D3}"), kv);

            db.Flush(kv);
            db.CompactRange(null, null, kv);
            for (int i = 0; i < 100; i += 2)
                db.Put(B($"k{i:D3}"), B($"SECRET-{i:D3}"), kv);

            db.Flush(kv);
            for (int i = 1; i < 100; i += 2)
                db.Put(B($"k{i:D3}"), B($"SECRET-{i:D3}"), kv);

            db.CreateCheckpoint(CheckpointPath);
        }

        Assert.Contains(Directory.GetFiles(CheckpointPath), file => Contains(file, "SECRET-"));
        using (SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment)))
        {
            ColumnFamily kv = copy.GetColumnFamily("kv");
            for (int i = 0; i < 100; i++)
                copy.Put(B($"k{i:D3}"), B($"old-{i:D3}"), kv);

            copy.Flush(kv);
            copy.CompactRange(null, null, kv);
            Assert.Equal(Enumerable.Range(0, 100).Select(i => $"k{i:D3}=old-{i:D3}"), ScanFamily(copy, kv));
        }

        Assert.DoesNotContain(Directory.GetFiles(CheckpointPath), file => Contains(file, "SECRET-"));

        static bool Contains(string file, string text) => File.ReadAllBytes(file).AsSpan().IndexOf(Encoding.UTF8.GetBytes(text)) >= 0;
    }

    // Dispose stops a checkpoint wherever it is before FORMAT, from every kind of source: the copy
    // fails with ObjectDisposedException and leaves only LOCK, and Dispose returns after it. The
    // checkpoint waits at its first write or link of the gated file while Dispose tells it to stop.
    [Theory]
    [InlineData("memtable", ".sst", true)]
    [InlineData("level0", ".sst", true)]
    [InlineData("level0", ".sst", false)]
    [InlineData("level6", ".sst", true)]
    [InlineData("level6", ".sst", false)]
    [InlineData("level6", "MANIFEST.tmp", true)]
    public async Task A_database_disposed_while_its_checkpoint_copies_fails_the_checkpoint(string source, string gated, bool link)
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance)
        {
            GatedPaths = path => path.StartsWith(CheckpointPath, StringComparison.Ordinal) && path.EndsWith(gated, StringComparison.Ordinal),
            LinkFiles = link,
        };
        SolariumDb db = SolariumDb.Open(DbPath, Options(hooks));
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        db.Put(B("k1"), B("v"));
        if (source != "memtable")
            db.Flush();

        if (source == "level6")
            db.CompactRange(null, null);

        Assert.Equal(source == "level6", db.DefaultColumnFamily.Version.Level6.Tables.Length > 0);
        Assert.Equal(source == "memtable", db.DefaultColumnFamily.Version.Level0.Length == 0 && db.DefaultColumnFamily.Version.Level6.Tables.Length == 0);

        hooks.WriteGate.Reset();
        Task checkpoint = Task.Run(() => db.CreateCheckpoint(CheckpointPath));
        Assert.True(hooks.WriteStarted.Wait(Timeout));
        Task dispose = Task.Run(db.Dispose);
        WaitUntil(() => db.IsClosing);
        Assert.False(dispose.IsCompleted);
        hooks.WriteGate.Set();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => checkpoint.WaitAsync(Timeout));
        await dispose.WaitAsync(Timeout);
        Assert.Equal(["LOCK"], Directory.GetFileSystemEntries(CheckpointPath).Select(Path.GetFileName));
        using SolariumDb reopened = SolariumDb.Open(DbPath, Options(DefaultStorageEnvironment.Instance));
        Assert.Equal("v", S(reopened.Get(B("k1"))));
    }

    // A checkpoint that is writing FORMAT is past its last check: Dispose waits for it, and it
    // completes as a copy that opens.
    [Fact]
    public async Task A_database_disposed_while_its_checkpoint_writes_format_waits_for_a_complete_copy()
    {
        HookedEnvironment hooks = new(DefaultStorageEnvironment.Instance)
        {
            GatedPaths = path => path == Path.Combine(CheckpointPath, "FORMAT.tmp"),
        };
        SolariumDb db = SolariumDb.Open(DbPath, Options(hooks));
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        db.Put(B("k1"), B("v"));
        db.Flush();
        db.Put(B("k2"), B("w"));

        hooks.WriteGate.Reset();
        Task checkpoint = Task.Run(() => db.CreateCheckpoint(CheckpointPath));
        Assert.True(hooks.WriteStarted.Wait(Timeout));
        Task dispose = Task.Run(db.Dispose);
        WaitUntil(() => db.IsClosing);
        await Task.Delay(100);
        Assert.False(dispose.IsCompleted);
        hooks.WriteGate.Set();

        await checkpoint.WaitAsync(Timeout);
        await dispose.WaitAsync(Timeout);
        using SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(DefaultStorageEnvironment.Instance));
        Assert.Equal("v", S(copy.Get(B("k1"))));
        Assert.Equal("w", S(copy.Get(B("k2"))));
    }

    // Two databases hold key with values of the same length; a's checkpoint checks that the target
    // does not exist and waits at the creation of its directory while the target becomes b's: a
    // checkpoint of b, open or closed, or a database opened there. a's checkpoint then fails, and
    // what b put there reads the same, live and after a reopen.
    [Theory]
    [MemberData(nameof(DestinationRaceCases))]
    public async Task A_checkpoint_that_loses_its_directory_to_another_owner_fails_and_touches_nothing_there(string environment, string winner)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        DirectoryGateEnvironment gated = new(scope.Environment, CheckpointPath);
        using SolariumDb a = SolariumDb.Open(_directory.Combine("a"), Options(gated));
        using SolariumDb b = SolariumDb.Open(_directory.Combine("b"), Options(scope.Environment));
        a.Put(B("key"), B("AAAA"));
        b.Put(B("key"), B("BBBB"));
        a.Flush();

        Task checkpoint;
        SolariumDb? held = null;
        try
        {
            using (gated.Hold())
            {
                checkpoint = Task.Run(() => a.CreateCheckpoint(CheckpointPath));
                Assert.True(gated.Started.Wait(Timeout));
                if (winner == "open")
                {
                    held = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
                    held.Put(B("key"), B("BBBB"));
                }
                else
                {
                    b.CreateCheckpoint(CheckpointPath);
                    if (winner == "checkpoint, open")
                        held = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
                }
            }

            Dictionary<string, byte[]> before = Contents(CheckpointPath);
            await Assert.ThrowsAsync<ArgumentException>(() => checkpoint.WaitAsync(Timeout));
            Dictionary<string, byte[]> after = Contents(CheckpointPath);
            Assert.Equal(before.Keys.Order(StringComparer.Ordinal), after.Keys.Order(StringComparer.Ordinal));
            foreach ((string name, byte[] bytes) in before)
                Assert.True(bytes.AsSpan().SequenceEqual(after[name]), $"{name} changed");

            if (held is not null)
                Assert.Equal("BBBB", S(held.Get(B("key"))));
        }
        finally
        {
            held?.Dispose();
        }

        using SolariumDb reopened = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
        Assert.Equal("BBBB", S(reopened.Get(B("key"))));

        // a still writes, and checkpoints elsewhere.
        a.Put(B("key"), B("AAA2"));
        a.CreateCheckpoint(CheckpointPath + "-a");

        static Dictionary<string, byte[]> Contents(string directory) =>
            Directory.GetFiles(directory).Where(file => Path.GetFileName(file) != "LOCK").ToDictionary(file => Path.GetFileName(file)!, File.ReadAllBytes);
    }

    /// <summary>Each environment with each owner that takes the directory first.</summary>
    /// <summary>Every environment of <see cref="Environments.Available"/>, with checkpoints that link their tables and with ones that copy them.</summary>
    public static TheoryData<string, bool> EnvironmentsAndLinks()
    {
        TheoryData<string, bool> cases = new();
        foreach (string environment in Solarium.IoUring.IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
        {
            cases.Add(environment, true);
            cases.Add(environment, false);
        }

        return cases;
    }

    public static TheoryData<string, string> DestinationRaceCases()
    {
        TheoryData<string, string> cases = new();
        foreach (string environment in Solarium.IoUring.IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
            foreach (string winner in new[] { "checkpoint", "checkpoint, open", "open" })
                cases.Add(environment, winner);

        return cases;
    }

    // While a checkpoint writes its copy it holds the directory's LOCK: another checkpoint there
    // and an Open there fail, and the first one completes.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task A_checkpoint_holds_its_directory_against_other_checkpoints_and_opens_until_it_is_done(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        HookedEnvironment hooks = new(scope.Environment) { GatedPaths = path => path.StartsWith(CheckpointPath, StringComparison.Ordinal) };
        using SolariumDb a = SolariumDb.Open(_directory.Combine("a"), Options(hooks));
        using HookedEnvironment.GateRelease release = hooks.OpenGatesAtEnd();
        using SolariumDb b = SolariumDb.Open(_directory.Combine("b"), Options(scope.Environment));
        a.Put(B("key"), B("AAAA"));
        b.Put(B("key"), B("BBBB"));

        hooks.WriteGate.Reset();
        Task checkpoint = Task.Run(() => a.CreateCheckpoint(CheckpointPath));
        Assert.True(hooks.WriteStarted.Wait(Timeout));

        // The directory exists, so the check up front refuses; the LOCK is what refuses past it.
        Assert.Throws<ArgumentException>(() => b.CreateCheckpoint(CheckpointPath));
        Assert.Contains("in use", Assert.Throws<SolariumException>(() => SolariumDb.Open(CheckpointPath, Options(scope.Environment))).Message);
        hooks.WriteGate.Set();

        await checkpoint.WaitAsync(Timeout);
        using SolariumDb copy = SolariumDb.Open(CheckpointPath, Options(scope.Environment));
        Assert.Equal("AAAA", S(copy.Get(B("key"))));
    }

    private static DbOptions Options(IStorageEnvironment environment) => new() { Environment = environment, CreateMissingColumnFamilies = true };

    // kv: k000-k199 in level 6, an overwrite of k100-k149 and deletes of k150-k159 in level 0, and a
    // range delete, new keys and an overwrite in the active memtable.
    private static void FillKv(SolariumDb db, ColumnFamily kv)
    {
        for (int i = 0; i < 200; i++)
            db.Put(B($"k{i:D3}"), B($"1:{i}"), kv);

        db.Flush(kv);
        db.CompactRange(null, null, kv);
        for (int i = 100; i < 150; i++)
            db.Put(B($"k{i:D3}"), B($"2:{i}"), kv);

        for (int i = 150; i < 160; i++)
            db.Delete(B($"k{i:D3}"), kv);

        db.Flush(kv);
        db.DeleteRange(B("k020"), B("k030"), kv);
        for (int i = 200; i < 210; i++)
            db.Put(B($"k{i:D3}"), B($"3:{i}"), kv);

        db.Put(B("k005"), B("3:5"), kv);
        Assert.NotEmpty(kv.Version.Level6.Tables);
        Assert.NotEmpty(kv.Version.Level0);
    }

    private static Dictionary<string, List<string>> ScanAll(SolariumDb db) =>
        db.ColumnFamilies.OrderBy(f => f.Name, StringComparer.Ordinal).ToDictionary(f => f.Name, f => ScanFamily(db, f));

    private static List<string> ScanFamily(SolariumDb db, ColumnFamily family)
    {
        using Iterator iterator = db.NewIterator(family);
        List<string> entries = [];
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            entries.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        return entries;
    }

    private static List<long> TableNumbers(string directory) =>
        [.. Directory.GetFiles(directory, "*.sst").Select(path => long.Parse(Path.GetFileNameWithoutExtension(path)))];

    private static void WaitUntil(Func<bool> condition)
    {
        Stopwatch clock = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(clock.Elapsed < Timeout, "The condition was not met in time.");
            Thread.Sleep(10);
        }
    }

    /// <summary>Forwards to another environment, and holds the creation of one directory while <see cref="Hold"/>'s scope lasts.</summary>
    private sealed class DirectoryGateEnvironment(IStorageEnvironment inner, string held) : IStorageEnvironment
    {
        private readonly ManualResetEventSlim _gate = new(initialState: true);

        /// <summary>Set when the creation of the held directory starts to wait.</summary>
        public ManualResetEventSlim Started { get; } = new();

        /// <summary>Holds the creation of the directory until the returned scope is disposed.</summary>
        public IDisposable Hold()
        {
            _gate.Reset();
            return new Release(_gate);
        }

        /// <inheritdoc />
        public string Name => $"directory-gated {inner.Name}";

        /// <inheritdoc />
        public IStorageFile OpenFile(string path) => inner.OpenFile(path);

        /// <inheritdoc />
        public bool FileExists(string path) => inner.FileExists(path);

        /// <inheritdoc />
        public string[] GetChildren(string directory) => inner.GetChildren(directory);

        /// <inheritdoc />
        public void DeleteFile(string path) => inner.DeleteFile(path);

        /// <inheritdoc />
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);

        /// <inheritdoc />
        public bool TryLinkFile(string source, string target) => inner.TryLinkFile(source, target);

        /// <inheritdoc />
        public void CreateDirectory(string directory)
        {
            if (directory == held)
            {
                Started.Set();
                Assert.True(_gate.Wait(Timeout));
            }

            inner.CreateDirectory(directory);
        }

        /// <inheritdoc />
        public void FlushDirectory(string directory) => inner.FlushDirectory(directory);

        private sealed class Release(ManualResetEventSlim gate) : IDisposable
        {
            public void Dispose() => gate.Set();
        }
    }

    /// <summary>Forwards to another environment and records each file open, rename, link and directory flush, in order.</summary>
    private sealed class RecordingEnvironment(IStorageEnvironment inner) : IStorageEnvironment
    {
        /// <summary>"open {path}", "rename {target}", "link {target}" and "flush-directory {directory}", in the order they ran.</summary>
        public List<string> Operations { get; } = [];

        /// <inheritdoc />
        public string Name => $"recording {inner.Name}";

        /// <inheritdoc />
        public IStorageFile OpenFile(string path)
        {
            Record($"open {path}");
            return inner.OpenFile(path);
        }

        /// <inheritdoc />
        public bool FileExists(string path) => inner.FileExists(path);

        /// <inheritdoc />
        public string[] GetChildren(string directory) => inner.GetChildren(directory);

        /// <inheritdoc />
        public void DeleteFile(string path) => inner.DeleteFile(path);

        /// <inheritdoc />
        public void RenameFile(string source, string target)
        {
            Record($"rename {target}");
            inner.RenameFile(source, target);
        }

        /// <inheritdoc />
        public bool TryLinkFile(string source, string target)
        {
            Record($"link {target}");
            return inner.TryLinkFile(source, target);
        }

        /// <inheritdoc />
        public void CreateDirectory(string directory) => inner.CreateDirectory(directory);

        /// <inheritdoc />
        public void FlushDirectory(string directory)
        {
            Record($"flush-directory {directory}");
            inner.FlushDirectory(directory);
        }

        private void Record(string operation)
        {
            lock (Operations)
                Operations.Add(operation);
        }
    }
}
