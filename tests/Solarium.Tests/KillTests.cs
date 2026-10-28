using System.Diagnostics;
using System.Globalization;
using Xunit.Abstractions;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>
/// The process that writes is killed with SIGKILL at random moments, as an OOM kill or a crash
/// does: <c>Solarium.CrashWorker</c> writes a random workload and reports each batch, and the test
/// kills it, reopens the directory, and checks it against the reported batches. A process crash
/// loses nothing that the OS holds, so every acknowledged batch must be there, synced or not.
/// Each run kills three workers in a row in one directory, some of them while they open it.
/// <c>SOLARIUM_KILL_ITERATIONS</c> and <c>SOLARIUM_KILL_SEED</c> change the number of runs and the
/// first seed; a failure names its seed.
/// </summary>
public sealed class KillTests(ITestOutputHelper output) : IDisposable
{
    private const int Generations = 3;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static readonly string[] FamilyNames = [ColumnFamily.DefaultName, "a", "b"];

    private readonly TempDirectory _directory = new();

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public async Task After_a_kill_at_any_moment_the_database_opens_with_every_acknowledged_batch_and_no_part_of_a_later_one(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        int iterations = Setting("SOLARIUM_KILL_ITERATIONS", 4);
        int firstSeed = Setting("SOLARIUM_KILL_SEED", 20261022);
        int kills = 0;
        int inFlight = 0;
        long batches = 0;
        for (int seed = firstSeed; seed < firstSeed + iterations; seed++)
        {
            Random random = new(seed);
            string directory = _directory.Combine(seed.ToString(CultureInfo.InvariantCulture));
            List<Batch> history = [];
            for (int generation = 0; generation < Generations; generation++)
            {
                string context = $"seed {seed} ({environment}), generation {generation}";
                (List<string> lines, bool killed) = await RunAndKill(directory, environment, random.Next(), random);
                kills += killed ? 1 : 0;
                (int acknowledged, bool started) = Read(lines, history, context);
                batches += acknowledged;

                using SolariumDb db = SolariumDb.Open(directory, scope.Options(createMissingColumnFamilies: true), FamilyNames);
                int recovered = Recover(db, history, acknowledged, started, context);
                inFlight += recovered > acknowledged ? 1 : 0;
                history.RemoveRange(recovered, history.Count - recovered);
            }
        }

        output.WriteLine($"{kills} kills, {batches} acknowledged batches checked, {inFlight} batches in flight at the kill survived whole");
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();

    // Starts a worker and kills it after a random number of acknowledged batches, or soon after it
    // starts, while it may still be opening the database. Returns its output and whether it was
    // killed rather than done.
    private static async Task<(List<string> Lines, bool Killed)> RunAndKill(string directory, string environment, int seed, Random random)
    {
        string worker = Path.Combine(AppContext.BaseDirectory, "Solarium.CrashWorker.dll");
        ProcessStartInfo start = new("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (string argument in new[] { worker, directory, environment, seed.ToString(CultureInfo.InvariantCulture) })
            start.ArgumentList.Add(argument);

        using Process process = Process.Start(start)!;
        List<string> lines = [];
        int acknowledged = 0;
        int killAfter = random.Next(5) == 0 ? 0 : random.Next(1, 400);
        Task<string> errors = process.StandardError.ReadToEndAsync();
        Task reading = Task.Run(async () =>
        {
            while (await process.StandardOutput.ReadLineAsync() is { } line)
            {
                lock (lines)
                    lines.Add(line);

                if (line.StartsWith('A') && line.EndsWith("\tE", StringComparison.Ordinal))
                    Interlocked.Increment(ref acknowledged);
            }
        });

        Stopwatch elapsed = Stopwatch.StartNew();
        if (killAfter == 0)
            await Task.Delay(random.Next(20, 400));
        else
            while (Volatile.Read(ref acknowledged) < killAfter && !process.HasExited && elapsed.Elapsed < Timeout)
                await Task.Delay(1);

        bool killed = !process.HasExited;
        if (killed)
            process.Kill(entireProcessTree: true);

        await process.WaitForExitAsync().WaitAsync(Timeout);
        await reading.WaitAsync(Timeout);
        if (!killed && process.ExitCode != 0)
            throw new InvalidOperationException($"The worker failed with exit code {process.ExitCode}: {await errors}");

        lock (lines)
            return ([.. lines], killed);
    }

    // Adds the batches a worker reported to the history: the acknowledged ones, and the one it had
    // started when it died, if any, which may or may not be in the database. Checks that each
    // acknowledgment names the sequence number the history gives the batch.
    private static (int Acknowledged, bool Started) Read(List<string> lines, List<Batch> history, string context)
    {
        int acknowledged = history.Count;
        bool started = false;
        foreach (string line in lines)
        {
            // A line the kill cut short: its batch was not written yet, or its acknowledgment is unknown.
            if (!line.EndsWith("\tE", StringComparison.Ordinal))
                continue;

            string[] fields = line[..^2].Split('\t');
            if (fields[0] == "S")
            {
                List<Operation> operations = [];
                for (int i = 2; i + 3 < fields.Length; i += 4)
                    operations.Add(new Operation(fields[i], fields[i + 1], fields[i + 2], fields[i + 3]));

                long previous = history.Count == 0 ? 0 : history[^1].LastSequence;
                history.Add(new Batch(operations, previous + operations.Count));
                started = true;
            }
            else if (fields[0] == "A")
            {
                long sequence = long.Parse(fields[1], CultureInfo.InvariantCulture);
                if (!started || sequence != history[^1].LastSequence)
                    throw new InvalidOperationException($"{context}: the worker acknowledged sequence number {sequence}, but the history has the batch end at {history[^1].LastSequence}.");

                acknowledged = history.Count;
                started = false;
            }
        }

        return (acknowledged, started);
    }

    // Checks the reopened database: it holds exactly the first k batches, with k at least every
    // acknowledged batch and at most the one in flight, and returns k.
    private static int Recover(SolariumDb db, List<Batch> history, int acknowledged, bool started, string context)
    {
        long sequence = db.LatestSequenceNumber;
        int recovered = sequence == 0 ? 0 : history.FindIndex(b => b.LastSequence == sequence) + 1;
        if (recovered == 0 && sequence != 0)
            throw new InvalidOperationException($"{context}: recovered to sequence number {sequence}, which ends no batch.");

        int most = acknowledged + (started ? 1 : 0);
        if (recovered < acknowledged || recovered > most)
            throw new InvalidOperationException(
                $"{context}: recovered {recovered} batches (sequence number {sequence}), but {acknowledged} were acknowledged and {most} written.");

        List<string> expected = Model(history, recovered);
        List<string> actual = Contents(db);
        if (!expected.SequenceEqual(actual))
        {
            string difference = expected.Except(actual).Concat(actual.Except(expected).Select(entry => "unexpected " + entry)).FirstOrDefault() ?? "order";
            throw new InvalidOperationException($"{context}: the contents are not those of the first {recovered} batches: {difference}.");
        }

        return recovered;
    }

    // Every family's entries after the first count batches, as "family/key=value", in order.
    private static List<string> Model(List<Batch> history, int count)
    {
        Dictionary<string, SortedDictionary<string, string>> families = FamilyNames.ToDictionary(f => f, _ => new SortedDictionary<string, string>(StringComparer.Ordinal));
        foreach (Batch batch in history.Take(count))
            foreach (Operation operation in batch.Operations)
            {
                SortedDictionary<string, string> family = families[operation.Family];
                if (operation.Kind == "P")
                    family[operation.Key] = operation.Value;
                else if (operation.Kind == "D")
                    family.Remove(operation.Key);
                else
                    foreach (string key in family.Keys.Where(k => string.CompareOrdinal(k, operation.Key) >= 0 && string.CompareOrdinal(k, operation.Value) < 0).ToList())
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

    private static int Setting(string name, int fallback)
    {
        return int.TryParse(System.Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value) ? value : fallback;
    }

    /// <summary>A put (P), a delete (D), or a range delete (R) up to <see cref="Value"/>, as the worker reports it.</summary>
    private sealed record Operation(string Kind, string Family, string Key, string Value);

    /// <summary>A batch the worker started, with the sequence number it ends at.</summary>
    private sealed record Batch(List<Operation> Operations, long LastSequence);
}
