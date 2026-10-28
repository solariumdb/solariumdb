using System.Globalization;
using Solarium.Storage;
using Xunit.Abstractions;
using static Solarium.Tests.CompactionTests;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// Power losses at random operations of a random workload, through <see cref="PowerLossEnvironment"/>:
/// batches across families, synced and not, with flushes, WAL rotation, compactions, and Kommander's
/// floor pass with <c>DeleteFilesInRange</c>. Each run crashes and reopens several times in a row,
/// so recovery itself crashes too. <c>SOLARIUM_CRASH_ITERATIONS</c> and <c>SOLARIUM_CRASH_SEED</c>
/// change the number of runs and the first seed; a failure names its seed.
/// </summary>
public sealed class CrashTests(ITestOutputHelper output) : IDisposable
{
    private const int Generations = 3;
    private const int StepsPerGeneration = 400;

    private static readonly string[] FamilyNames = [ColumnFamily.DefaultName, "a", "b"];

    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void After_a_power_loss_at_any_operation_the_database_opens_with_every_synced_batch_and_no_part_of_a_later_one(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        int iterations = Setting("SOLARIUM_CRASH_ITERATIONS", 30);
        int firstSeed = Setting("SOLARIUM_CRASH_SEED", 20261022);
        Dictionary<string, int> outcomes = new(StringComparer.Ordinal);
        for (int seed = firstSeed; seed < firstSeed + iterations; seed++)
        {
            try
            {
                foreach (string outcome in Run(scope.Environment, seed))
                    outcomes[outcome] = outcomes.GetValueOrDefault(outcome) + 1;
            }
            catch (Exception e)
            {
                // The directories of the failed run, for a look at the files the crash left.
                string kept = Path.Combine(Path.GetTempPath(), $"solarium-crash-{seed}-{Guid.NewGuid():N}");
                Directory.CreateDirectory(kept);
                foreach (string directory in Directory.GetDirectories(_directory.Path, $"{seed}-*"))
                    CopyDatabase(directory, Path.Combine(kept, Path.GetFileName(directory)));

                Assert.Fail($"Seed {seed} ({environment}), directories in {kept}: {e}");
            }
        }

        output.WriteLine(string.Join(", ", outcomes.OrderBy(o => o.Key, StringComparer.Ordinal).Select(o => $"{o.Key}: {o.Value}")));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_flushed_table_never_survives_a_power_loss_without_the_earlier_writes_of_other_families(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string directory = _directory.Combine("db");
        string crashed = _directory.Combine("crashed");
        Directory.CreateDirectory(directory);
        PowerLossEnvironment powerLoss = new(scope.Environment, directory);

        using (SolariumDb db = SolariumDb.Open(directory, scope.Options(createMissingColumnFamilies: true) with { Environment = powerLoss }, ["a"]))
        {
            db.Put(B("x"), B("1"), db.GetColumnFamily("a"));
            db.Put(B("y"), B("2"));

            // The table holds y, sequence number 2; x, sequence number 1, is only in the WAL.
            db.Flush();
            powerLoss.CrashAfter(0, crashed, PowerLossEnvironment.Loss.Unflushed, new Random(1));
        }

        using SolariumDb reopened = SolariumDb.Open(crashed, scope.Options(), ["a"]);
        Assert.Equal("2", S(reopened.Get(B("y"))));
        Assert.Equal("1", S(reopened.Get(B("x"), reopened.GetColumnFamily("a"))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Every_write_before_a_clean_close_survives_a_power_loss_after_it(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        string directory = _directory.Combine("db");
        string crashed = _directory.Combine("crashed");
        Directory.CreateDirectory(directory);
        PowerLossEnvironment powerLoss = new(scope.Environment, directory);

        using (SolariumDb db = SolariumDb.Open(directory, scope.Options(createMissingColumnFamilies: true) with { Environment = powerLoss }, ["a"]))
        {
            for (int i = 0; i < 50; i++)
                db.Write(new WriteBatch().Put(B($"k{i:D2}"), B("1")).Put(B($"k{i:D2}"), B("2"), db.GetColumnFamily("a")));
        }

        powerLoss.CrashAfter(0, crashed, PowerLossEnvironment.Loss.Unflushed, new Random(1));
        using SolariumDb reopened = SolariumDb.Open(crashed, scope.Options(), ["a"]);
        Assert.Equal(100, reopened.LatestSequenceNumber);
        Assert.Equal("2", S(reopened.Get(B("k49"), reopened.GetColumnFamily("a"))));
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // One run: a workload on a new database, then a crash, a reopen and a check, Generations times.
    // Returns how each crash ended, for the summary.
    private List<string> Run(IStorageEnvironment inner, int seed)
    {
        Random random = new(seed);
        History history = new();
        List<string> outcomes = [];
        string directory = _directory.Combine($"{seed}-0");
        Directory.CreateDirectory(directory);

        for (int generation = 0; ; generation++)
        {
            PowerLossEnvironment environment = new(inner, directory);
            PowerLossEnvironment.Loss loss = (PowerLossEnvironment.Loss)random.Next(3);
            string crashed = _directory.Combine($"{seed}-{generation + 1}");
            bool last = generation == Generations;
            if (!last)
                environment.CrashAfter(random.Next(1, 6000), crashed, loss, new Random(random.Next()));

            using (SolariumDb db = SolariumDb.Open(directory, Options(environment, seed), FamilyNames))
            {
                if (generation > 0)
                    outcomes.Add(history.Recover(db, $"generation {generation}, after a {history.LastLoss} loss at operation {history.LastCrash}"));

                if (last)
                    return outcomes;

                Workload(db, environment, history, random);
                if (!environment.Crashed)
                    environment.CrashAfter(0, crashed, loss, new Random(random.Next()));

                history.Crashed(loss, environment.Operations);
            }

            directory = crashed;
        }
    }

    // Odd seeds compress their tables, so the crashes cut compressed tables too; seeds 2 and 3 of each
    // four have small level targets, so they cut compactions into levels 1 to 5; seeds 4 to 7 of each
    // eight run four compaction threads; seeds 8 to 15 of each sixteen pace their tables and start their
    // writeback every 16 KB; seeds 16 to 31 of each thirty-two cap their memtables across families at 24 KB;
    // seeds 32 to 63 of each sixty-four keep index and filter blocks in a 4 KB cache, which rereads them on
    // most lookups, and pin level 0's when the seed is odd.
    private static DbOptions Options(PowerLossEnvironment environment, int seed)
    {
        Cache? cache = seed % 64 >= 32 ? new Cache(4 * 1024) : null;
        return new DbOptions
        {
            Environment = environment,
            CreateMissingColumnFamilies = true,
            DefaultColumnFamilyOptions = new ColumnFamilyOptions
            {
                WriteBufferSize = 8 * 1024,
                MaxWriteBufferNumber = 3,
                Level0FileNumCompactionTrigger = 2,
                TargetFileSizeBase = seed % 4 >= 2 ? 4 * 1024 : 16 * 1024,
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
    }

    // Writes until the environment crashes or the steps run out, from this thread alone, so each
    // batch's sequence numbers are known; flushes and compactions run on their own threads.
    private static void Workload(SolariumDb db, PowerLossEnvironment environment, History history, Random random)
    {
        for (int step = 0; step < StepsPerGeneration && !environment.Crashed; step++)
        {
            int choice = random.Next(100);
            if (choice < 75)
                history.Write(db, environment, RandomBatch(history, random), random.Next(100) < 15);
            else if (choice < 87)
                history.Write(db, environment, history.LogRows(random.Next(1, 20)), sync: false);
            else if (choice < 92)
                history.FloorPass(db, environment);
            else if (choice < 97)
                db.Flush(db.GetColumnFamily(FamilyNames[random.Next(FamilyNames.Length)]), wait: random.Next(2) == 0);
            else
                db.CompactRange(null, null, db.GetColumnFamily(FamilyNames[random.Next(FamilyNames.Length)]));
        }
    }

    private static List<Operation> RandomBatch(History history, Random random)
    {
        List<Operation> operations = [];
        int count = random.Next(1, 5);
        for (int i = 0; i < count; i++)
        {
            string family = FamilyNames[random.Next(FamilyNames.Length)];
            string key = $"k{random.Next(200):D3}";
            int kind = random.Next(100);
            if (kind < 80)
                operations.Add(new Operation(family, key, $"{history.Batches}.{i}:" + new string('v', random.Next(300)), null));
            else if (kind < 92)
                operations.Add(new Operation(family, key, null, null));
            else
            {
                // A range with its ends in order; an empty one now and then.
                string other = $"k{random.Next(200):D3}";
                operations.Add(string.CompareOrdinal(key, other) <= 0 ? new Operation(family, key, null, other) : new Operation(family, other, null, key));
            }
        }

        return operations;
    }

    private static int Setting(string name, int fallback)
    {
        return int.TryParse(System.Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value) ? value : fallback;
    }

    /// <summary>A put (a value), a delete (neither), or a range delete up to <see cref="End"/>, exclusive.</summary>
    private sealed record Operation(string Family, string Key, string? Value, string? End);

    /// <summary>A written batch, with its last sequence number and whether the database acknowledged it before the crash.</summary>
    private sealed record Batch(List<Operation> Operations, long LastSequence, bool Synced, bool Acknowledged);

    /// <summary>
    /// The batches written so far, the ones that must survive a crash, and Kommander's log in family
    /// <c>a</c>: rows that only grow and a floor below which they are deleted, then dropped whole.
    /// </summary>
    private sealed class History
    {
        private readonly List<Batch> _batches = [];
        private int _mustSurvive;
        private long _nextRow = 1;
        private long _floor;

        /// <summary>The batches written, including one a crash may have cut.</summary>
        public int Batches => _batches.Count;

        /// <summary>How the last crash treated what was not flushed.</summary>
        public PowerLossEnvironment.Loss LastLoss { get; private set; }

        /// <summary>The operation the last crash happened at.</summary>
        public long LastCrash { get; private set; }

        /// <summary>Kommander's log rows: the next <paramref name="count"/> ids in family <c>a</c>.</summary>
        public List<Operation> LogRows(int count)
        {
            List<Operation> rows = [];
            for (int i = 0; i < count; i++)
                rows.Add(new Operation("a", $"log{_nextRow++:D8}", $"row {_nextRow - 1}", null));

            return rows;
        }

        /// <summary>
        /// Kommander's floor pass: one synced batch with the floor and a range delete below it, then
        /// <c>DeleteFilesInRange</c> below it. Nothing is written below the floor again, so the drop
        /// changes nothing a read sees, once the batch is durable.
        /// </summary>
        public void FloorPass(SolariumDb db, PowerLossEnvironment environment)
        {
            long floor = _nextRow - 30;
            if (floor <= _floor)
                return;

            List<Operation> pass =
            [
                new Operation(ColumnFamily.DefaultName, "floor", floor.ToString(CultureInfo.InvariantCulture), null),
                new Operation("a", "log00000000", null, $"log{floor:D8}"),
            ];
            if (!Write(db, environment, pass, sync: true))
                return;

            _floor = floor;
            db.DeleteFilesInRange(B("log00000000"), B($"log{floor - 1:D8}"), db.GetColumnFamily("a"));
        }

        /// <summary>Writes <paramref name="operations"/> as one batch and records it; true when it was acknowledged before a crash.</summary>
        public bool Write(SolariumDb db, PowerLossEnvironment environment, List<Operation> operations, bool sync)
        {
            WriteBatch batch = new();
            foreach (Operation operation in operations)
            {
                ColumnFamily family = db.GetColumnFamily(operation.Family);
                if (operation.Value is not null)
                    batch.Put(B(operation.Key), B(operation.Value), family);
                else if (operation.End is null)
                    batch.Delete(B(operation.Key), family);
                else
                    batch.DeleteRange(B(operation.Key), B(operation.End), family);
            }

            // Empty ranges add nothing, and a batch of only those writes nothing.
            if (batch.Count == 0)
                return !environment.Crashed;

            db.Write(batch, sync ? WriteOptions.Synced : null);

            // Acknowledged before the crash only when the crash had not happened when the write returned.
            bool acknowledged = !environment.Crashed;
            _batches.Add(new Batch(operations, db.LatestSequenceNumber, sync, acknowledged));
            if (sync && acknowledged)
                _mustSurvive = _batches.Count;

            return acknowledged;
        }

        /// <summary>Records the crash that ended the current generation.</summary>
        public void Crashed(PowerLossEnvironment.Loss loss, long operation)
        {
            LastLoss = loss;
            LastCrash = operation;
        }

        /// <summary>
        /// Checks the reopened database: it holds exactly the first k batches, for one k at or past
        /// the last synced acknowledgment, and nothing of a later batch. The later batches are gone
        /// from the history, and every batch recovered now must survive the next crash too.
        /// </summary>
        public string Recover(SolariumDb db, string context)
        {
            long sequence = db.LatestSequenceNumber;
            int recovered = sequence == 0 ? 0 : _batches.FindIndex(b => b.LastSequence == sequence) + 1;
            if (recovered == 0 && sequence != 0)
                throw new InvalidOperationException($"{context}: recovered to sequence number {sequence}, inside a batch: {Inside(db, sequence)}");

            if (recovered < _mustSurvive)
                throw new InvalidOperationException(
                    $"{context}: recovered {recovered} batches (sequence number {sequence}), but batch {_mustSurvive} was synced and acknowledged. Lost: "
                    + string.Join("; ", _batches.Skip(recovered).Take(_mustSurvive - recovered).Select((b, i) =>
                        $"batch {recovered + i + 1} to sequence number {b.LastSequence}, synced {b.Synced}, {string.Join(", ", b.Operations.Select(o => $"{o.Family}/{o.Key}"))}")));

            List<string> expected = Model(recovered);
            List<string> actual = Contents(db);
            if (!expected.SequenceEqual(actual))
            {
                string difference = expected.Except(actual).Concat(actual.Except(expected).Select(entry => "unexpected " + entry)).First();
                throw new InvalidOperationException(
                    $"{context}: the contents are not those of the first {recovered} batches (sequence number {sequence}): {difference}.");
            }

            string outcome = $"{LastLoss}: {(recovered == _batches.Count ? "all" : recovered == _mustSurvive ? "synced only" : "some unsynced")}";
            _batches.RemoveRange(recovered, _batches.Count - recovered);
            _mustSurvive = recovered;
            _nextRow = 1 + _batches.SelectMany(b => b.Operations).Count(o => o.Family == "a" && o.Value is not null && o.Key.StartsWith("log", StringComparison.Ordinal));
            _floor = _batches.SelectMany(b => b.Operations).Where(o => o.Key == "floor").Select(o => long.Parse(o.Value!, CultureInfo.InvariantCulture)).DefaultIfEmpty().Max();
            return outcome;
        }

        // The batch whose sequence numbers include sequence, with what a read finds of each operation.
        private string Inside(SolariumDb db, long sequence)
        {
            int index = _batches.FindIndex(b => b.LastSequence > sequence);
            if (index < 0)
                return $"past the last batch, {(_batches.Count == 0 ? 0 : _batches[^1].LastSequence)}";

            Batch batch = _batches[index];
            long first = index == 0 ? 1 : _batches[index - 1].LastSequence + 1;
            IEnumerable<string> operations = batch.Operations.Select((o, i) =>
            {
                string kind = o.Value is not null ? "put" : o.End is null ? "delete" : $"delete range to {o.End}";
                string found = S(db.Get(B(o.Key), db.GetColumnFamily(o.Family))) ?? "absent";
                return $"#{first + i} {kind} {o.Family}/{o.Key} (now {(found.Length > 12 ? found[..12] + "..." : found)})";
            });
            return $"batch {index + 1} of {_batches.Count}, sequence numbers {first} to {batch.LastSequence}, synced {batch.Synced}, "
                + $"acknowledged {batch.Acknowledged}, must survive up to batch {_mustSurvive}: {string.Join("; ", operations)}";
        }

        // Every family's entries after the first count batches, as "family/key=value", in order.
        private List<string> Model(int count)
        {
            Dictionary<string, SortedDictionary<string, string>> families = FamilyNames.ToDictionary(f => f, _ => new SortedDictionary<string, string>(StringComparer.Ordinal));
            foreach (Batch batch in _batches.Take(count))
                foreach (Operation operation in batch.Operations)
                {
                    SortedDictionary<string, string> family = families[operation.Family];
                    if (operation.Value is not null)
                        family[operation.Key] = operation.Value;
                    else if (operation.End is null)
                        family.Remove(operation.Key);
                    else
                        foreach (string key in family.Keys.Where(k => string.CompareOrdinal(k, operation.Key) >= 0 && string.CompareOrdinal(k, operation.End) < 0).ToList())
                            family.Remove(key);
                }

            return [.. FamilyNames.SelectMany(f => families[f].Select(entry => $"{f}/{entry.Key}={entry.Value}"))];
        }

        private static List<string> Contents(SolariumDb db)
        {
            List<string> entries = [];
            foreach (string name in FamilyNames)
            {
                using Iterator iterator = db.NewIterator(db.GetColumnFamily(name));
                for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                    entries.Add($"{name}/{S(iterator.Key())}={S(iterator.Value())}");
            }

            return entries;
        }
    }
}
