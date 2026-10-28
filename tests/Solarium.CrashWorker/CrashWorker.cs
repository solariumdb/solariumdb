using System.Globalization;
using System.Text;
using Solarium.IoUring;
using Solarium.Storage;

namespace Solarium.CrashWorker;

/// <summary>
/// The process that <c>KillTests</c> kills: <c>dotnet Solarium.CrashWorker.dll directory environment seed</c>
/// writes a random workload to the database in <c>directory</c> until it is killed, and tells the
/// test on standard output what it writes. Before each batch, a line <c>S</c> with the batch's
/// sync flag and operations; after the write returns, a line <c>A</c> with the database's latest
/// sequence number. Fields are separated by tabs, and the last one is <c>E</c>, so a line that a kill
/// cut short shows it; keys and values hold no tab or line break.
/// </summary>
/// <remarks>
/// The workload is that of <c>CrashTests</c>: batches across the families <c>default</c>, <c>a</c>
/// and <c>b</c>, synced and not, Kommander's log rows and floor pass in <c>a</c>, flushes and
/// compactions, with small memtables. It starts from what the database holds, so a run can follow
/// a killed one in the same directory.
/// </remarks>
public static class CrashWorker
{
    private const int MaxSteps = 50_000;

    private static readonly string[] FamilyNames = ["default", "a", "b"];

    /// <summary>Runs the workload; returns 0 when it ends without being killed, 2 on wrong arguments.</summary>
    public static int Main(string[] args)
    {
        if (args.Length != 3 || !int.TryParse(args[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out int seed))
        {
            Console.Error.WriteLine("Usage: Solarium.CrashWorker directory default|io_uring seed");
            return 2;
        }

        using IoUringStorageEnvironment? owned = args[1] == "io_uring" ? new IoUringStorageEnvironment() : null;
        Cache? cache = seed % 64 >= 32 ? new Cache(4 * 1024) : null;
        DbOptions options = new()
        {
            Environment = owned is null ? DefaultStorageEnvironment.Instance : owned,
            CreateMissingColumnFamilies = true,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = 8 * 1024,
                MaxWriteBufferNumber = 3,
                Level0FileNumCompactionTrigger = 2,
                TargetFileSizeBase = seed % 4 >= 2 ? 4 * 1024 : 16 * 1024,

                // Odd seeds compress, so a kill cuts compressed tables too; seeds 2 and 3 of each four
                // have small level targets, so a kill cuts compactions into levels 1 to 5. Seeds 4 to 7
                // of each eight run four compaction threads, seeds 8 to 15 of each sixteen pace their
                // tables and start their writeback every 16 KB, seeds 16 to 31 of each thirty-two cap
                // their memtables across families at 24 KB (below), and seeds 32 to 63 of each sixty-four
                // keep index and filter blocks in a 4 KB cache, pinned at level 0 when the seed is odd.
                Compression = seed % 2 == 0 ? CompressionType.None : CompressionType.Lz4,
                MaxBytesForLevelBase = seed % 4 >= 2 ? 8 * 1024 : 256L * 1024 * 1024,
                MaxBytesForLevelMultiplier = seed % 4 >= 2 ? 2 : 10,
                BlockCache = cache,
                CacheIndexAndFilterBlocks = cache is not null,
                PinL0FilterAndIndexBlocksInCache = seed % 2 == 1,
            },
            MaxTotalWalSize = 32 * 1024,
            MaxBackgroundCompactions = seed % 8 >= 4 ? 4 : 1,
            RateLimiter = seed % 16 >= 8 ? new RateLimiter(256L * 1024 * 1024) : null,
            BytesPerSync = seed % 16 >= 8 ? 16 * 1024 : 0,
            DbWriteBufferSize = seed % 32 >= 16 ? 24 * 1024 : 0,
        };

        using StreamWriter output = new(Console.OpenStandardOutput(), new UTF8Encoding(false)) { AutoFlush = true };
        using SolariumDb db = SolariumDb.Open(args[0], options, FamilyNames);
        Random random = new(seed);
        long floor = long.Parse(Encoding.ASCII.GetString(db.Get("floor"u8) ?? "0"u8.ToArray()), CultureInfo.InvariantCulture);
        long nextRow = NextRow(db);

        for (int step = 0; step < MaxSteps; step++)
        {
            int choice = random.Next(100);
            if (choice < 75)
                Write(db, output, RandomBatch(random, step), random.Next(100) < 15);
            else if (choice < 87)
            {
                List<string[]> rows = [];
                for (int count = random.Next(1, 20); count > 0; count--, nextRow++)
                    rows.Add(["P", "a", $"log{nextRow:D8}", $"row {nextRow}"]);

                Write(db, output, rows, sync: false);
            }
            else if (choice < 92 && nextRow - 30 > floor)
            {
                // Kommander's floor pass: the floor and a range delete below it, synced, then the drop.
                floor = nextRow - 30;
                Write(db, output, [["P", "default", "floor", floor.ToString(CultureInfo.InvariantCulture)], ["R", "a", "log00000000", $"log{floor:D8}"]], sync: true);
                db.DeleteFilesInRange(Encoding.ASCII.GetBytes("log00000000"), Encoding.ASCII.GetBytes($"log{floor - 1:D8}"), db.GetColumnFamily("a"));
            }
            else if (choice < 97)
                db.Flush(db.GetColumnFamily(FamilyNames[random.Next(FamilyNames.Length)]), wait: random.Next(2) == 0);
            else
                db.CompactRange(null, null, db.GetColumnFamily(FamilyNames[random.Next(FamilyNames.Length)]));
        }

        return 0;
    }

    // Puts, deletes and range deletes in order of their ends; an empty range writes nothing, so it is left out.
    private static List<string[]> RandomBatch(Random random, int step)
    {
        List<string[]> operations = [];
        for (int i = random.Next(1, 5); i > 0; i--)
        {
            string family = FamilyNames[random.Next(FamilyNames.Length)];
            string key = $"k{random.Next(200):D3}";
            int kind = random.Next(100);
            if (kind < 80)
                operations.Add(["P", family, key, $"{step}.{i}:" + new string('v', random.Next(300))]);
            else if (kind < 92)
                operations.Add(["D", family, key, ""]);
            else
            {
                string other = $"k{random.Next(200):D3}";
                if (other != key)
                    operations.Add(["R", family, string.CompareOrdinal(key, other) < 0 ? key : other, string.CompareOrdinal(key, other) < 0 ? other : key]);
            }
        }

        return operations;
    }

    // Tells the batch, writes it, and tells the sequence number it ended at.
    private static void Write(SolariumDb db, StreamWriter output, List<string[]> operations, bool sync)
    {
        if (operations.Count == 0)
            return;

        WriteBatch batch = new();
        foreach (string[] operation in operations)
        {
            ColumnFamily family = db.GetColumnFamily(operation[1]);
            byte[] key = Encoding.ASCII.GetBytes(operation[2]);
            if (operation[0] == "P")
                batch.Put(key, Encoding.ASCII.GetBytes(operation[3]), family);
            else if (operation[0] == "D")
                batch.Delete(key, family);
            else
                batch.DeleteRange(key, Encoding.ASCII.GetBytes(operation[3]), family);
        }

        output.WriteLine($"S\t{(sync ? 1 : 0)}\t{string.Join('\t', operations.SelectMany(o => o))}\tE");
        db.Write(batch, sync ? WriteOptions.Synced : null);
        output.WriteLine($"A\t{db.LatestSequenceNumber}\tE");
    }

    // The log id after the newest row in family a: rows only grow, and the newest ones are never deleted.
    private static long NextRow(SolariumDb db)
    {
        using Iterator iterator = db.NewIterator(db.GetColumnFamily("a"));
        iterator.SeekToLast();
        for (; iterator.Valid(); iterator.Prev())
        {
            string key = Encoding.ASCII.GetString(iterator.Key());
            if (key.StartsWith("log", StringComparison.Ordinal))
                return long.Parse(key.AsSpan(3), CultureInfo.InvariantCulture) + 1;
        }

        return 1;
    }
}
