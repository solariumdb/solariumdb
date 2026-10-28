using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>Versions of a column family: the reader counts that keep tables open, the sorted level, and the manifest versions.</summary>
public sealed class VersionTests : IDisposable
{
    private readonly TempDirectory _directory = new();

    private string DbPath => _directory.Combine("db");

    [Fact]
    public void A_retired_version_releases_its_tables_only_after_its_last_reader_leaves()
    {
        TableReader table = BuildTable(DefaultStorageEnvironment.Instance, Puts("k", 1));
        List<TableReader> unused = [];
        FamilyVersion version = new(new MemTable(), [], [table], AtBottom(SortedLevel.Empty), unused.Add);

        int first = version.Enter();
        int second = version.Enter();
        version.Retire();
        Assert.Empty(unused);

        version.Leave(first);
        Assert.Empty(unused);

        version.Leave(second);
        Assert.Equal([table], unused);

        // A reader that enters a closed version and leaves does not release the tables again.
        version.Leave(version.Enter());
        Assert.Single(unused);
        table.Dispose();
    }

    [Fact]
    public void A_table_that_a_newer_version_lists_stays_referenced_when_the_older_version_closes()
    {
        TableReader shared = BuildTable(DefaultStorageEnvironment.Instance, Puts("a", 1));
        TableReader dropped = BuildTable(DefaultStorageEnvironment.Instance, Puts("b", 2));
        List<TableReader> unused = [];
        FamilyVersion older = new(new MemTable(), [], [dropped], AtBottom(new SortedLevel([shared])), unused.Add);
        FamilyVersion newer = new(new MemTable(), [], [], AtBottom(new SortedLevel([shared])), unused.Add);

        older.Retire();
        Assert.Equal([dropped], unused);

        newer.Retire();
        Assert.Equal([dropped, shared], unused);
        shared.Dispose();
        dropped.Dispose();
    }

    [Fact]
    public void Readers_never_read_a_table_after_the_last_version_that_lists_it_closes()
    {
        // The test installs versions into an idle database from one thread, which is what the state
        // lock guarantees to the database's own installers.
        using SolariumDb db = SolariumDb.Open(DbPath);
        ColumnFamily family = db.DefaultColumnFamily;
        string path = _directory.Combine("shared.sst");
        using (IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(path))
        {
            TableBuilder.Build(file, Puts("k", 1).Entries(), []);
        }

        int released = 0;
        void Unused(TableReader table)
        {
            table.Dispose();
            Interlocked.Increment(ref released);
        }

        const int Installs = 20000;
        bool done = false;
        long reads = 0;
        Exception? failure = null;
        Thread[] readers = new Thread[4];
        for (int r = 0; r < readers.Length; r++)
        {
            readers[r] = new Thread(() =>
            {
                try
                {
                    while (!Volatile.Read(ref done))
                    {
                        FamilyVersion version = family.EnterVersion(out int slot);
                        try
                        {
                            if (version.Level0.Length == 1)
                            {
                                Assert.True(version.Level0[0].TryGetNewest(B("k"), long.MaxValue, true, out Entry entry));
                                Assert.Equal("value", S(entry.Value));
                                Interlocked.Increment(ref reads);
                            }
                        }
                        finally
                        {
                            version.Leave(slot);
                        }
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            });
            readers[r].Start();
        }

        // Each version lists a table of its own, so a table is released exactly when its version closes.
        for (int i = 0; i < Installs; i++)
        {
            TableReader table = TableReader.Open(i, DefaultStorageEnvironment.Instance.OpenFile(path));
            family.Install(new FamilyVersion(family.Version.Active, [], [table], AtBottom(SortedLevel.Empty), Unused));
        }

        Volatile.Write(ref done, true);
        foreach (Thread reader in readers)
            reader.Join();

        family.Install(new FamilyVersion(family.Version.Active, [], [], AtBottom(SortedLevel.Empty), Unused));
        Assert.Null(failure);
        Assert.True(reads > 0);
        Assert.Equal(Installs, released);
    }

    [Fact]
    public void An_open_iterator_keeps_its_tables_until_it_is_disposed()
    {
        using SolariumDb db = SolariumDb.Open(DbPath);
        ColumnFamily family = db.DefaultColumnFamily;

        // Two writes, so the snapshots see the tables' entries at sequence numbers 1 and 2.
        db.Put(B("x"), B("x"));
        db.Put(B("x"), B("x"));
        List<TableReader> unused = [];
        TableReader first = BuildTable(DefaultStorageEnvironment.Instance, Puts("a", 1));
        TableReader second = BuildTable(DefaultStorageEnvironment.Instance, Puts("b", 2));
        family.Install(new FamilyVersion(family.Version.Active, [], [first], AtBottom(SortedLevel.Empty), unused.Add));

        Iterator iterator = db.NewIterator();
        family.Install(new FamilyVersion(family.Version.Active, [], [second], AtBottom(SortedLevel.Empty), unused.Add));
        Assert.Empty(unused);

        // The iterator still reads its snapshot's table; a new read sees the new version.
        iterator.SeekToFirst();
        Assert.Equal("a", S(iterator.Key()));
        Assert.Equal("value", S(db.Get(B("b"))));
        Assert.Null(db.Get(B("a")));

        iterator.Dispose();
        Assert.Equal([first], unused);
        Assert.Throws<ObjectDisposedException>(() => iterator.SeekToFirst());
        iterator.Dispose();
        Assert.Single(unused);

        family.Install(new FamilyVersion(family.Version.Active, [], [], AtBottom(SortedLevel.Empty), unused.Add));
        Assert.Equal([first, second], unused);
        first.Dispose();
        second.Dispose();
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_sorted_level_returns_what_its_files_return_as_separate_sources(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        Random random = new(7);
        long sequence = 0;
        string[] prefixes = ["b", "d", "f", "g", "h"];
        TableReader[] tables = new TableReader[prefixes.Length];
        for (int t = 0; t < prefixes.Length; t++)
        {
            // Each file holds its own key range, with several versions, deletes, and a tombstone
            // inside the range; the gaps between the files hold no key. The "g" file holds only its
            // tombstone, so a backward step inside its range must find the key in the file before.
            MemTable memTable = new();
            for (int i = 0; i < (prefixes[t] == "g" ? 0 : 400); i++)
            {
                byte[] key = B($"{prefixes[t]}{random.Next(30):D2}");
                if (random.Next(10) == 0)
                    memTable.Add(key, ++sequence, OperationKind.Delete, null);
                else
                    memTable.Add(key, ++sequence, OperationKind.Put, B($"v{sequence}"));
            }

            memTable.AddRangeTombstone(B($"{prefixes[t]}10"), B($"{prefixes[t]}15"), ++sequence);
            tables[t] = BuildTable(scope.Environment, memTable);
        }

        Action<TableReader> unused = _ => { };
        FamilyVersion separate = new(new MemTable(), [], [.. tables.Reverse()], AtBottom(SortedLevel.Empty), unused);
        FamilyVersion sorted = new(new MemTable(), [], [], AtBottom(new SortedLevel(tables)), unused);

        List<string> keys = [];
        foreach (char prefix in "abcdefghi")
            for (int i = 0; i < 31; i++)
                keys.Add($"{prefix}{i:D2}");

        keys.Add("");
        keys.Add("h99");
        keys.Add("z");

        foreach (long snapshot in new[] { sequence, sequence / 2, 1 })
        {
            foreach (string key in keys)
            {
                Assert.Equal(Get(separate, key, snapshot), Get(sorted, key, snapshot));
                Assert.Equal(
                    tables.Max(table => table.CoveringTombstone(B(key), snapshot)),
                    sorted.Level6.CoveringTombstone(B(key), snapshot));
            }

            Assert.Equal(Forward(separate, snapshot, null), Forward(sorted, snapshot, null));
            Assert.Equal(Backward(separate, snapshot), Backward(sorted, snapshot));
            foreach (string key in keys)
                Assert.Equal(Forward(separate, snapshot, key), Forward(sorted, snapshot, key));
        }

        foreach (TableReader table in tables)
            table.Dispose();
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_database_reads_its_level_6_files_after_a_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            foreach (string prefix in new[] { "c", "a", "b" })
            {
                for (int i = 0; i < 50; i++)
                    db.Put(B($"{prefix}{i:D2}"), B($"{prefix}{i}"));

                db.Flush();
            }
        }

        // Make the three files of disjoint ranges level 6, in key order, as a move would.
        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!;
        ManifestFamily record = manifest.ColumnFamilies.Single(f => f.Name == ColumnFamily.DefaultName);
        List<ManifestTable> moved = [.. record.Tables.OrderBy(t => t.Smallest, ByteOrder.Instance).Select(t => AtLevel(t, Manifest.BottomLevel))];
        record.Tables.Clear();
        record.Tables.AddRange(moved);
        manifest.Save(DefaultStorageEnvironment.Instance, DbPath);

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            FamilyVersion version = db.DefaultColumnFamily.Version;
            Assert.Empty(version.Level0);
            Assert.Equal(3, version.Level6.Tables.Length);
            Assert.Equal("b7", S(db.Get(B("b07"))));
            Assert.Null(db.Get(B("b50")));

            // A newer level 0 file comes before level 6 for the keys it holds.
            db.Put(B("b07"), B("new"));
            db.Delete(B("c00"));
            db.Flush();
            Assert.Equal("new", S(db.Get(B("b07"))));
            Assert.Null(db.Get(B("c00")));
            Assert.Equal(149, Forward(db).Count);
        }

        using (SolariumDb db = SolariumDb.Open(DbPath, scope.Options()))
        {
            Assert.Single(db.DefaultColumnFamily.Version.Level0);
            Assert.Equal(3, db.DefaultColumnFamily.Version.Level6.Tables.Length);
            Assert.Equal("new", S(db.Get(B("b07"))));
            Assert.Null(db.Get(B("c00")));

            List<string> forward = Forward(db);
            Assert.Equal(149, forward.Count);
            Assert.Equal("a00", forward[0]);
            Assert.Equal("c49", forward[^1]);

            using Iterator iterator = db.NewIterator();
            List<string> backward = [];
            for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
                backward.Add(S(iterator.Key()));

            Assert.Equal(forward.AsEnumerable().Reverse(), backward);
        }
    }

    [Fact]
    public void A_version_1_manifest_opens_and_is_saved_as_the_current_version()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            for (int flush = 0; flush < 3; flush++)
            {
                db.Put(B("k"), B($"v{flush}"));
                db.Put(B($"only{flush}"), B("x"));
                db.Flush();
            }
        }

        string path = Path.Combine(DbPath, Manifest.FileName);
        RewriteAsVersion(DbPath, 1);
        Assert.Equal(1u, ManifestVersion(path));

        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            Assert.Equal(3, db.DefaultColumnFamily.Version.Level0.Length);
            Assert.Equal("v2", S(db.Get(B("k"))));
            Assert.Equal("x", S(db.Get(B("only0"))));
        }

        Assert.Equal(3u, ManifestVersion(path));
    }

    [Fact]
    public void A_manifest_whose_column_family_id_is_past_the_cap_is_corrupt()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath))
            db.Put(B("a"), B("1"));

        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!;
        manifest.ColumnFamilies.Add(new ManifestFamily { Id = Manifest.MaxFamilyId + 1, Name = "far" });
        manifest.Save(DefaultStorageEnvironment.Instance, DbPath);

        SolariumException e = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains($"column family id {Manifest.MaxFamilyId + 1} is above", e.Message);
    }

    [Fact]
    public void A_manifest_with_level_6_files_that_overlap_or_follow_no_version_2_does_not_open()
    {
        using (SolariumDb db = SolariumDb.Open(DbPath))
        {
            for (int flush = 0; flush < 2; flush++)
            {
                db.Put(B("a"), B("1"));
                db.Put(B("z"), B("1"));
                db.Flush();
            }
        }

        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, DbPath)!;
        ManifestFamily record = manifest.ColumnFamilies.Single(f => f.Name == ColumnFamily.DefaultName);
        List<ManifestTable> overlapping = [.. record.Tables.Select(t => AtLevel(t, Manifest.BottomLevel))];
        record.Tables.Clear();
        record.Tables.AddRange(overlapping);
        manifest.Save(DefaultStorageEnvironment.Instance, DbPath);

        SolariumException overlap = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains("overlap at level 6", overlap.Message);

        // Version 1 has level 0 files only.
        record.Tables.RemoveAt(1);
        manifest.Save(DefaultStorageEnvironment.Instance, DbPath);
        RewriteAsVersion(DbPath, 1);
        SolariumException level = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains("at level 6", level.Message);

        // Version 2 has levels 0 and 6 only.
        record.Tables[0] = AtLevel(record.Tables[0], 3);
        manifest.Save(DefaultStorageEnvironment.Instance, DbPath);
        RewriteAsVersion(DbPath, 2);
        SolariumException middle = Assert.Throws<SolariumException>(() => SolariumDb.Open(DbPath));
        Assert.Contains("at level 3", middle.Message);
    }

    private static MemTable Puts(string key, long sequence)
    {
        MemTable memTable = new();
        memTable.Add(B(key), sequence, OperationKind.Put, B("value"));
        return memTable;
    }

    private TableReader BuildTable(IStorageEnvironment environment, MemTable memTable)
    {
        IStorageFile file = environment.OpenFile(_directory.Combine($"{Guid.NewGuid():N}.sst"));
        TableBuilder.Build(file, memTable.Entries(), memTable.Tombstones);
        return TableReader.Open(1, file);
    }

    internal static ManifestTable AtLevel(ManifestTable table, int level) => new()
    {
        Number = table.Number,
        Level = level,
        Size = table.Size,
        EntryCount = table.EntryCount,
        MinSequence = table.MinSequence,
        MaxSequence = table.MaxSequence,
        Smallest = table.Smallest,
        Largest = table.Largest,
        LargestIsExclusive = table.LargestIsExclusive,
    };

    // The manifest of directory in the layout of version 1 or 2, which have no table flags.
    internal static void RewriteAsVersion(string directory, uint version)
    {
        Manifest manifest = Manifest.Load(DefaultStorageEnvironment.Instance, directory)!;
        ArrayBufferWriter<byte> payload = new();
        void Put32(uint value) => BinaryPrimitives.WriteUInt32LittleEndian(payload.GetSpan(4), value);
        void Put64(long value) => BinaryPrimitives.WriteInt64LittleEndian(payload.GetSpan(8), value);
        void PutBytes(byte[] bytes)
        {
            Put32((uint)bytes.Length);
            payload.Advance(4);
            payload.Write(bytes);
        }

        Put32(version);
        payload.Advance(4);
        Put64(manifest.NextFileNumber);
        payload.Advance(8);
        Put32((uint)manifest.WalNumbers.Count);
        payload.Advance(4);
        foreach (long number in manifest.WalNumbers)
        {
            Put64(number);
            payload.Advance(8);
        }

        Put32((uint)manifest.ColumnFamilies.Count);
        payload.Advance(4);
        foreach (ManifestFamily family in manifest.ColumnFamilies)
        {
            Put32((uint)family.Id);
            payload.Advance(4);
            PutBytes(Encoding.UTF8.GetBytes(family.Name));
            Put64(family.FlushedSequence);
            payload.Advance(8);
            Put32((uint)family.Tables.Count);
            payload.Advance(4);
            foreach (ManifestTable table in family.Tables)
            {
                Put64(table.Number);
                payload.Advance(8);
                Put32((uint)table.Level);
                payload.Advance(4);
                foreach (long value in (long[])[table.Size, table.EntryCount, table.MinSequence, table.MaxSequence])
                {
                    Put64(value);
                    payload.Advance(8);
                }

                PutBytes(table.Smallest);
                PutBytes(table.Largest);
            }
        }

        byte[] file = new byte[8 + payload.WrittenCount];
        BinaryPrimitives.WriteUInt32LittleEndian(file, (uint)payload.WrittenCount);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(4), Crc32.HashToUInt32(payload.WrittenSpan));
        payload.WrittenSpan.CopyTo(file.AsSpan(8));
        File.WriteAllBytes(Path.Combine(directory, Manifest.FileName), file);
    }

    // The version field is the first four bytes of the payload.
    private static uint ManifestVersion(string path) => BinaryPrimitives.ReadUInt32LittleEndian(File.ReadAllBytes(path).AsSpan(8));

    // Levels with level at the bottom and the others empty.
    private static SortedLevel[] AtBottom(SortedLevel level)
    {
        SortedLevel[] levels = [.. Enumerable.Repeat(SortedLevel.Empty, Manifest.LevelCount)];
        levels[Manifest.BottomLevel] = level;
        return levels;
    }

    private static string? Get(FamilyVersion version, string key, long snapshot)
    {
        return ReadPath.TryGet(version.Sources, B(key), snapshot, true, out byte[]? value) ? S(value) : null;
    }

    private static List<string> Forward(FamilyVersion version, long snapshot, string? from)
    {
        using Iterator iterator = new(version, version.Enter(), snapshot, null, fillCache: true);
        List<string> keys = [];
        if (from is null)
            iterator.SeekToFirst();
        else
            iterator.Seek(B(from));

        for (; iterator.Valid(); iterator.Next())
            keys.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        return keys;
    }

    private static List<string> Backward(FamilyVersion version, long snapshot)
    {
        using Iterator iterator = new(version, version.Enter(), snapshot, null, fillCache: true);
        List<string> keys = [];
        for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
            keys.Add($"{S(iterator.Key())}={S(iterator.Value())}");

        return keys;
    }

    private static List<string> Forward(SolariumDb db)
    {
        using Iterator iterator = db.NewIterator();
        List<string> keys = [];
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            keys.Add(S(iterator.Key()));

        return keys;
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    private sealed class ByteOrder : IComparer<byte[]>
    {
        public static ByteOrder Instance { get; } = new();

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}
