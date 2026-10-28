using System.Numerics;

namespace Solarium.Kahuna.Benchmarks;

/// <summary>A latency histogram in nanoseconds: exact below 128, then 64 buckets per power of two (1.6% precision).</summary>
internal static class Histogram
{
    /// <summary>Buckets up to about 2^40 ns (18 minutes).</summary>
    public const int Buckets = 64 * 36;

    /// <summary>The bucket of <paramref name="nanoseconds"/>.</summary>
    public static int Bucket(long nanoseconds)
    {
        if (nanoseconds < 128)
            return (int)Math.Max(nanoseconds, 0);

        int shift = 63 - BitOperations.LeadingZeroCount((ulong)nanoseconds) - 6;
        return Math.Min(((shift + 1) * 64) + (int)((nanoseconds >> shift) - 64), Buckets - 1);
    }

    /// <summary>The lower bound, in nanoseconds, of the bucket that holds the <paramref name="quantile"/> of <paramref name="histogram"/>.</summary>
    public static long Percentile(long[] histogram, double quantile)
    {
        long total = 0;
        foreach (long count in histogram)
            total += count;

        if (total == 0)
            return 0;

        long rank = (long)Math.Ceiling(total * quantile);
        long seen = 0;
        for (int i = 0; i < histogram.Length; i++)
        {
            seen += histogram[i];
            if (seen >= rank)
                return i < 128 ? i : (long)((i % 64) + 64) << ((i / 64) - 1);
        }

        return 0;
    }

    /// <summary>The bucket-wise sum of <paramref name="histograms"/>.</summary>
    public static long[] Merge(IEnumerable<long[]> histograms)
    {
        long[] merged = new long[Buckets];
        foreach (long[] histogram in histograms)
            for (int i = 0; i < merged.Length; i++)
                merged[i] += histogram[i];

        return merged;
    }
}
