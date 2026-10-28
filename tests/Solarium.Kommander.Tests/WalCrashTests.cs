using System.Globalization;
using System.Text;
using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kommander.Tests;

/// <summary>
/// Crashes of <see cref="SolariumWAL"/> at random storage operations of a random Kommander workload,
/// through <see cref="PowerLossEnvironment"/>: appends, commit markers, backfilled rows, checkpoints,
/// compaction passes, truncations, snapshot boundaries, partition deletes and hard state, synced and
/// not, on small memtables so flushes, WAL rotation, compactions and file drops run too.
/// </summary>
/// <remarks>
/// <para>The oracle is a second <see cref="SolariumWAL"/> on an environment that never crashes, given
/// the same operations: after each one, the test records what Kommander can read of it. Each
/// operation is one atomic write, so a reopened WAL must read exactly as the reference did after some
/// operation k. After a power loss, k is at or past the last synced operation that changed what a read
/// sees; after a process crash (<see cref="PowerLossEnvironment.Loss.None"/>), past the last
/// operation that returned. Each run crashes and reopens several times, so recovery crashes too.</para>
/// <para><c>SOLARIUM_WAL_CRASH_ITERATIONS</c> and <c>SOLARIUM_WAL_CRASH_SEED</c> change the number of
/// runs and the first seed; a failure names its seed and keeps the run's directories.</para>
/// </remarks>
public sealed class WalCrashTests(ITestOutputHelper output) : IDisposable
{
    private const int Generations = 3;
    private const int StepsPerGeneration = 150;

    // Partitions 1 and 9 share shard 1; partition 2 owns shard 2.
    private static readonly int[] Partitions = [1, 9, 2];

    private static readonly SolariumWalOptions Small = new()
    {
        ShardWriteBufferSizeBytes = 16 * 1024,
        ShardMaxWriteBufferNumber = 3,
        ShardLevel0FileNumCompactionTrigger = 2,
        MaxTotalWalSizeFlushUnits = 2,
    };

    private readonly TempDirectory _directory = new();

    /// <summary>Deletes the test's directory.</summary>
    public void Dispose() => _directory.Dispose();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void After_a_crash_at_any_operation_the_wal_reads_as_after_one_operation_at_or_past_the_last_durable_one(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        int iterations = Setting("SOLARIUM_WAL_CRASH_ITERATIONS", 24);
        int firstSeed = Setting("SOLARIUM_WAL_CRASH_SEED", 20261023);
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
                string kept = Path.Combine(Path.GetTempPath(), $"solarium-wal-crash-{seed}-{Guid.NewGuid():N}");
                CopyTree(_directory.Path, kept);
                Assert.Fail($"Seed {seed} ({environment}), directories in {kept}: {e}");
            }
        }

        output.WriteLine(string.Join(", ", outcomes.OrderBy(o => o.Key, StringComparer.Ordinal).Select(o => $"{o.Key}: {o.Value}")));
    }

    // One run: a workload, then a crash, a reopen and a check, Generations times. Returns how each crash ended.
    private List<string> Run(IStorageEnvironment inner, int seed)
    {
        Random random = new(seed);
        List<Step> steps = [];
        List<string> states = [];   // states[k]: what a read sees after the first k steps
        Dictionary<int, long> terms = Partitions.ToDictionary(p => p, _ => 1L);
        List<string> outcomes = [];
        int mustSurvive = 0;        // the steps that a power loss must keep
        int returned = 0;           // the steps that a process crash must keep
        PowerLossEnvironment.Loss crashed = PowerLossEnvironment.Loss.None;   // how the crash that made root ended
        string root = _directory.Combine($"{seed}-0");
        Directory.CreateDirectory(Path.Combine(root, "wal"));

        for (int generation = 0; ; generation++)
        {
            PowerLossEnvironment environment = new(inner, Path.Combine(root, "wal"));
            PowerLossEnvironment.Loss loss = (PowerLossEnvironment.Loss)random.Next(4);
            string next = _directory.Combine($"{seed}-{generation + 1}");
            bool last = generation == Generations;
            if (!last)
                environment.CrashAfter(random.Next(1, 3000), Path.Combine(next, "wal"), loss, new Random(random.Next()));

            using (SolariumWAL wal = new(root, "wal", NullLogger<IRaft>.Instance, syncWrites: true, Small with { Environment = environment }))
            {
                if (generation > 0)
                {
                    string recovered = Read(wal);
                    int floor = crashed == PowerLossEnvironment.Loss.None ? returned : mustSurvive;
                    int match = states.FindIndex(floor, s => s == recovered);
                    if (match < 0)
                        throw new InvalidOperationException(
                            $"generation {generation}, after a {crashed} crash: the WAL reads as after none of steps {floor} to {steps.Count}.\n"
                            + $"Recovered:\n{recovered}\nAfter step {floor} ({(floor == 0 ? "start" : steps[floor - 1].Name)}):\n{states[floor]}\n"
                            + $"After step {steps.Count}:\n{states[^1]}\nThe last steps: {string.Join("; ", steps.Skip(Math.Max(0, steps.Count - 8)).Select(s => s.Name))}");

                    outcomes.Add($"{crashed}: {(match == steps.Count ? "all" : match == floor ? "durable only" : "some more")}");
                    steps.RemoveRange(match, steps.Count - match);
                    states.RemoveRange(match + 1, states.Count - match - 1);
                    mustSurvive = match;
                    returned = match;
                }

                if (last)
                    return outcomes;

                using SolariumWAL reference = Wals.Open(inner, _directory.Combine($"{seed}-reference-{generation}"), syncWrites: false, Small);
                foreach (Step step in steps)
                    step.Apply(reference);

                if (Read(reference) != states.DefaultIfEmpty("").Last() && states.Count > 0)
                    throw new InvalidOperationException($"generation {generation}: replaying {steps.Count} steps on a new reference does not read as before.");

                if (states.Count == 0)
                    states.Add(Read(reference));

                for (int i = 0; i < StepsPerGeneration && !environment.Crashed; i++)
                {
                    Step step = NextStep(reference, terms, random);
                    step.Apply(wal);
                    bool acknowledged = !environment.Crashed;
                    step.Apply(reference);
                    steps.Add(step);
                    states.Add(Read(reference));
                    if (!acknowledged)
                        break;

                    returned = steps.Count;
                    if (step.Synced && states[^1] != states[^2])
                        mustSurvive = steps.Count;
                }

                if (!environment.Crashed)
                    environment.CrashAfter(0, Path.Combine(next, "wal"), loss, new Random(random.Next()));
            }

            crashed = loss;
            root = next;
        }
    }

    // One Kommander operation, from the reference's state; the same parameters apply to both WALs.
    private static Step NextStep(SolariumWAL reference, Dictionary<int, long> terms, Random random)
    {
        int p = Partitions[random.Next(Partitions.Length)];
        long max = reference.GetMaxLog(p);
        if (random.Next(100) < 4)
            terms[p]++;

        long term = terms[p];
        bool sync = random.Next(100) < 35;
        int choice = random.Next(100);
        if (choice < 35)
        {
            List<RaftLog> rows = Rows(max + 1, random.Next(1, 9), term, RaftLogType.Proposed, random);
            return new Step($"append {p} {max + 1}+{rows.Count}{(sync ? " synced" : "")}", sync, w => Check(w.Write([(p, Copy(rows))], sync)));
        }

        if (choice < 50)
        {
            // Commit markers for the last ids; one of an older term rewrites its row.
            long first = Math.Max(1, max - random.Next(4));
            List<RaftLog> markers = [];
            for (long id = first; id <= max; id++)
                markers.Add(new RaftLog { Id = id, Term = random.Next(10) == 0 ? term + 1 : term, Type = RaftLogType.Committed, LogType = "op" });

            return new Step($"commit {p} {first}..{max}{(sync ? " synced" : "")}", sync, w => Check(w.Write([(p, Copy(markers))], sync)));
        }

        if (choice < 55)
        {
            List<RaftLog> rows = Rows(max + 1, random.Next(1, 6), term, RaftLogType.Committed, random);
            return new Step($"backfill {p} {max + 1}+{rows.Count}{(sync ? " synced" : "")}", sync, w => Check(w.Write([(p, Copy(rows))], sync)));
        }

        if (choice < 62)
        {
            RaftLog checkpoint = new() { Id = max + 1, Term = term, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" };
            return new Step($"checkpoint {p} {max + 1}{(sync ? " synced" : "")}", sync, w => Check(w.Write([(p, Copy([checkpoint]))], sync)));
        }

        if (choice < 70)
        {
            long checkpoint = reference.GetLastCheckpoint(p);
            int cap = random.Next(1, 50);
            return new Step($"compact {p} below {checkpoint}", true, w =>
            {
                if (checkpoint > 0)
                    Check(w.CompactLogsOlderThan(p, checkpoint, cap).Status);
            });
        }

        if (choice < 75)
        {
            long after = Math.Max(0, max - random.Next(5));
            return new Step($"truncate {p} after {after}", true, w => Check(w.TruncateLogsAfter(p, after)));
        }

        if (choice < 79)
        {
            long after = Math.Max(0, max - random.Next(5));
            return new Step($"truncate proposed {p} after {after}", true, w => Check(w.TruncateProposedLogsAfter(p, after)));
        }

        if (choice < 84)
        {
            long index = Math.Max(1, max - random.Next(-2, 5));
            long lastIncluded = random.Next(2) == 0 ? term : term + 1;
            return new Step($"boundary {p} at {index} term {lastIncluded}{(sync ? " synced" : "")}", sync, w => Check(w.InstallSnapshotBoundary(p, index, lastIncluded, sync).Status));
        }

        if (choice < 95)
        {
            string vote = $"node-{random.Next(3)}";
            return new Step($"hard state {p} {term} {vote}", false, w => Assert.True(((IWAL)w).PersistHardState(p, term, vote)));
        }

        if (choice < 97)
            return new Step($"delete partition {p}", true, w => Check(w.DeletePartitionWAL(p)));

        // One batch over two partitions that share a shard.
        long nine = reference.GetMaxLog(9);
        long one = reference.GetMaxLog(1);
        List<RaftLog> ones = Rows(one + 1, random.Next(1, 4), terms[1], RaftLogType.Proposed, random);
        List<RaftLog> nines = Rows(nine + 1, random.Next(1, 4), terms[9], RaftLogType.Proposed, random);
        return new Step($"append 1 {one + 1}+{ones.Count} and 9 {nine + 1}+{nines.Count}{(sync ? " synced" : "")}", sync,
            w => Check(w.Write([(1, Copy(ones)), (9, Copy(nines))], sync)));
    }

    private static List<RaftLog> Rows(long first, int count, long term, RaftLogType type, Random random)
    {
        List<RaftLog> rows = new(count);
        for (int i = 0; i < count; i++)
        {
            byte[] payload = new byte[random.Next(0, 400)];
            random.NextBytes(payload);
            rows.Add(new RaftLog { Id = first + i, Term = term, Type = type, LogType = "op", LogData = payload });
        }

        return rows;
    }

    // Each WAL gets its own entries, so nothing one of them keeps can change what the other was given.
    private static List<RaftLog> Copy(List<RaftLog> logs) =>
        [.. logs.Select(l => new RaftLog { Id = l.Id, Term = l.Term, Type = l.Type, LogType = l.LogType, LogData = l.LogData?.ToArray() })];

    private static void Check(RaftOperationStatus status) => Assert.Equal(RaftOperationStatus.Success, status);

    // Everything Kommander can read of the partitions, as text that compares whole.
    private static string Read(SolariumWAL wal)
    {
        StringBuilder text = new();
        foreach (int p in Partitions)
        {
            text.Append(CultureInfo.InvariantCulture,
                $"p{p}: checkpoint {wal.GetLastCheckpoint(p)}, max {wal.GetMaxLog(p)}, term {wal.GetCurrentTerm(p)}, floor {wal.GetCompactionFloorForTesting(p)}, ");
            text.Append(CultureInfo.InvariantCulture, $"persisted {wal.CountPersistedLogs(p)}, removable {wal.CountRemovableLogs(p)}, ");
            text.Append(((IWAL)wal).TryGetHardState(p, out long term, out string? vote) ? $"hard state {term}/{vote}" : "no hard state");
            text.Append("\n  rows");
            foreach (RaftLog log in wal.ReadLogsRange(p, 0))
                text.Append(CultureInfo.InvariantCulture, $" {log.Id}:{log.Term}:{log.Type}:{Hash(log.LogData)}");

            text.Append("\n  restore from");
            foreach (RaftLog log in wal.ReadLogs(p))
                text.Append(CultureInfo.InvariantCulture, $" {log.Id}");

            text.Append('\n');
        }

        return text.ToString();
    }

    private static string Hash(byte[]? data)
    {
        if (data is null)
            return "null";

        uint hash = 2166136261;
        foreach (byte b in data)
            hash = (hash ^ b) * 16777619;

        return $"{data.Length}/{hash:x8}";
    }

    private static int Setting(string name, int fallback) =>
        int.TryParse(System.Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value) ? value : fallback;

    private static void CopyTree(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source))
            if (Path.GetFileName(file) != "LOCK")
                File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));

        foreach (string directory in Directory.GetDirectories(source))
            CopyTree(directory, Path.Combine(destination, Path.GetFileName(directory)));
    }

    /// <summary>One operation of the workload: a name for failure messages, whether it is synced, and how to apply it to a WAL.</summary>
    private sealed record Step(string Name, bool Synced, Action<SolariumWAL> Apply);
}
