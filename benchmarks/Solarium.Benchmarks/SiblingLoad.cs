using System.Diagnostics;
using System.Globalization;

namespace Solarium.Benchmarks;

/// <summary>
/// A load to run on the hyperthread siblings of the CPUs that a benchmark runs on, in a separate
/// process, so the benchmark measures what a busy sibling costs it:
/// <c>dotnet run -c Release --project benchmarks/Solarium.Benchmarks -- sibling-load writer-spin 2</c>.
/// It runs until it is stopped.
/// </summary>
internal static class SiblingLoad
{
    /// <summary>
    /// Runs the load that <paramref name="args"/> names on the given number of threads:
    /// <c>writer-spin</c>, the spin of a waiting writer, or <c>busy</c>, integer work
    /// with no pause, the worst case for a sibling.
    /// </summary>
    public static void Run(string[] args)
    {
        if (args.Length != 2 || args[0] is not ("writer-spin" or "busy") || !int.TryParse(args[1], CultureInfo.InvariantCulture, out int threads) || threads < 1)
            throw new ArgumentException("Usage: sibling-load writer-spin|busy <threads>");

        Action load = args[0] == "writer-spin" ? WriterSpin : Busy;
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++)
        {
            workers[i] = new Thread(() => load()) { Name = $"sibling-{i}" };
            workers[i].Start();
        }

        Console.WriteLine($"sibling-load: {args[0]} on {threads} threads; stop the process to end it.");
        foreach (Thread worker in workers)
            worker.Join();
    }

    // The spin of Writer.WaitForTurn: a pause and a clock read in each turn.
    private static void WriterSpin()
    {
        long sink = 0;
        while (true)
        {
            Thread.SpinWait(1);
            sink += Stopwatch.GetTimestamp();
            if (sink == 1)
                Console.WriteLine(sink);
        }
    }

    private static void Busy()
    {
        ulong x = 0x9E3779B97F4A7C15;
        while (true)
        {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if (x == 0)
                Console.WriteLine(x);
        }
    }
}
