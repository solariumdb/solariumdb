using System.Globalization;

namespace Solarium.Benchmarks;

/// <summary>
/// The time that the threads of this process spent on a CPU and in a run queue, waiting for one,
/// from <c>/proc/self/task/*/schedstat</c> on Linux. A thread that waits for a CPU is ready to run;
/// a thread that is blocked or asleep is not in a run queue, so its wait does not count. The
/// threads are grouped by the start of their names.
/// </summary>
internal sealed class ThreadSchedule
{
    private readonly Dictionary<int, (string Name, long OnCpuNanoseconds, long WaitNanoseconds)> _threads;

    private ThreadSchedule(Dictionary<int, (string Name, long OnCpuNanoseconds, long WaitNanoseconds)> threads) => _threads = threads;

    /// <summary>The threads of this process now, or null where <c>/proc</c> has no schedstat.</summary>
    public static ThreadSchedule? Read()
    {
        const string tasks = "/proc/self/task";
        if (!OperatingSystem.IsLinux() || !Directory.Exists(tasks))
            return null;

        Dictionary<int, (string, long, long)> threads = new();
        foreach (string directory in Directory.GetDirectories(tasks))
        {
            try
            {
                string name = File.ReadAllText(Path.Combine(directory, "comm")).TrimEnd('\n');
                string[] fields = File.ReadAllText(Path.Combine(directory, "schedstat")).Split(' ');
                threads[int.Parse(Path.GetFileName(directory), CultureInfo.InvariantCulture)] =
                    (name, long.Parse(fields[0], CultureInfo.InvariantCulture), long.Parse(fields[1], CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                // The thread ended between the directory listing and the read.
            }
        }

        return new ThreadSchedule(threads);
    }

    /// <summary>
    /// One line for each group in <paramref name="groups"/> (a label and the start of the thread
    /// names): the threads that ran in both reads, and their time on a CPU and in a run queue
    /// between <paramref name="before"/> and this read. A thread that started or ended between the
    /// reads is left out.
    /// </summary>
    public string Since(ThreadSchedule before, (string Label, string NamePrefix)[] groups)
    {
        List<string> parts = new(groups.Length);
        foreach ((string label, string prefix) in groups)
        {
            int count = 0;
            long onCpu = 0;
            long waiting = 0;
            foreach ((int id, (string name, long cpu, long wait)) in _threads)
            {
                if (!name.StartsWith(prefix, StringComparison.Ordinal) || !before._threads.TryGetValue(id, out (string Name, long OnCpuNanoseconds, long WaitNanoseconds) old))
                    continue;

                count++;
                onCpu += cpu - old.OnCpuNanoseconds;
                waiting += wait - old.WaitNanoseconds;
            }

            if (count > 0)
                parts.Add(string.Create(CultureInfo.InvariantCulture, $"{label} ({count}): {onCpu / 1e9:F3} s on a CPU, {waiting / 1e9:F3} s waiting for one"));
        }

        return string.Join("; ", parts);
    }
}
