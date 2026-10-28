using System.Diagnostics;

namespace Solarium;

/// <summary>
/// Paces the bytes that flushes and compactions write, as RocksDB's <c>GenericRateLimiter</c>: each
/// refill period hands out <see cref="BytesPerSecond"/> times the period, and a write that finds it spent
/// waits for the next refill. Give it to <see cref="DbOptions.RateLimiter"/>; the host owns it, and one
/// limiter can pace several databases on one drive. WAL writes, the manifest and checkpoints are not
/// paced, as RocksDB paces only flushes and compactions by default.
/// </summary>
/// <remarks>
/// <para>Flush writes go before compaction writes, as RocksDB's <c>IO_HIGH</c> and <c>IO_LOW</c>, except
/// that every <c>fairness</c>th refill serves the compactions first, so they always progress. Each
/// write asks for its bytes in pieces of at most one period's budget (<see cref="SingleBurstBytes"/>).</para>
/// <para>An auto-tuned limiter, as RocksDB's <c>auto_tuned</c>, starts at half the rate it was given and
/// moves between a twentieth of it and all of it: every 100 refill periods, up by 5% when writes waited in
/// more than 90% of the periods, down by 5% when in fewer than half, and to the bottom when in none. It
/// smooths bursts under its maximum; it does not find what the drive can take.</para>
/// </remarks>
public sealed class RateLimiter
{
    private const int RefillsPerTune = 100;
    private const int AllowedRangeFactor = 20;
    private const int LowWatermarkPercent = 50;
    private const int HighWatermarkPercent = 90;
    private const int AdjustPercent = 5;

    private readonly object _lock = new();
    private readonly long _refillTicks;   // Stopwatch ticks
    private readonly int _fairness;
    private readonly Queue<Waiter> _high = new();   // under _lock
    private readonly Queue<Waiter> _low = new();    // under _lock
    private long _maxBytesPerSecond;   // under _lock: the rate given, the top of an auto-tuned one
    private long _bytesPerSecond;      // under _lock
    private long _refillBytes;         // under _lock: the budget of one period
    private long _available;           // under _lock
    private long _nextRefill;          // under _lock: Stopwatch ticks
    private long _refills;             // under _lock
    private long _tuneStart;           // under _lock: Stopwatch ticks
    private long _drainedTicks;        // under _lock: the time since the last tune in which writes waited
    private long _lastRefill;          // under _lock: Stopwatch ticks
    private bool _drainedThisPeriod;   // under _lock
    private long _bytesThrough;        // under _lock
    private long _requests;            // under _lock
    private long _waitTicks;           // under _lock

    /// <summary>
    /// A limiter of <paramref name="bytesPerSecond"/>, refilled every <paramref name="refillPeriod"/>
    /// (default 100 ms, as RocksDB's), auto-tuned or not, that serves the compactions first on every
    /// <paramref name="fairness"/>th refill (default 10, as RocksDB's).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">A value is not positive, or the period is under a millisecond.</exception>
    public RateLimiter(long bytesPerSecond, TimeSpan? refillPeriod = null, bool autoTuned = false, int fairness = 10)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bytesPerSecond, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(fairness, 1);
        TimeSpan period = refillPeriod ?? TimeSpan.FromMilliseconds(100);
        ArgumentOutOfRangeException.ThrowIfLessThan(period, TimeSpan.FromMilliseconds(1), nameof(refillPeriod));

        RefillPeriod = period;
        AutoTuned = autoTuned;
        _fairness = fairness;
        _refillTicks = (long)(period.TotalSeconds * Stopwatch.Frequency);
        _maxBytesPerSecond = bytesPerSecond;
        SetRate(autoTuned ? Math.Max(1, bytesPerSecond / 2) : bytesPerSecond);
        _available = _refillBytes;
        long now = Stopwatch.GetTimestamp();
        _nextRefill = now + _refillTicks;
        _lastRefill = now;
        _tuneStart = now;
    }

    /// <summary>How often the budget refills.</summary>
    public TimeSpan RefillPeriod { get; }

    /// <summary>True when the rate tunes itself under the one given.</summary>
    public bool AutoTuned { get; }

    /// <summary>The rate now: the one given, or where auto-tuning has moved it.</summary>
    public long BytesPerSecond
    {
        get
        {
            lock (_lock)
            {
                return _bytesPerSecond;
            }
        }
    }

    /// <summary>The budget of one refill period, the most one piece of a write takes, as RocksDB's <c>GetSingleBurstBytes</c>.</summary>
    public long SingleBurstBytes
    {
        get
        {
            lock (_lock)
            {
                return _refillBytes;
            }
        }
    }

    /// <summary>The bytes asked for so far, as RocksDB's <c>GetTotalBytesThrough</c>.</summary>
    public long TotalBytesThrough
    {
        get
        {
            lock (_lock)
            {
                return _bytesThrough;
            }
        }
    }

    /// <summary>The pieces asked for so far, as RocksDB's <c>GetTotalRequests</c>.</summary>
    public long TotalRequests
    {
        get
        {
            lock (_lock)
            {
                return _requests;
            }
        }
    }

    /// <summary>The time writes have waited for their bytes so far, summed over the waiting writes.</summary>
    public TimeSpan TotalWait
    {
        get
        {
            lock (_lock)
            {
                return Stopwatch.GetElapsedTime(0, _waitTicks);
            }
        }
    }

    /// <summary>
    /// Change the rate, as RocksDB's <c>SetBytesPerSecond</c>: for an auto-tuned limiter, the top of its
    /// range, which the rate now stays under. From the next refill on.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bytesPerSecond"/> is not positive.</exception>
    public void SetBytesPerSecond(long bytesPerSecond)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bytesPerSecond, 1);
        lock (_lock)
        {
            _maxBytesPerSecond = bytesPerSecond;
            SetRate(AutoTuned ? Math.Min(_bytesPerSecond, bytesPerSecond) : bytesPerSecond);
        }
    }

    /// <summary>
    /// Wait until <paramref name="bytes"/> are granted, in pieces of at most <see cref="SingleBurstBytes"/>;
    /// flushes ask with <paramref name="high"/>, compactions without.
    /// </summary>
    internal void Request(long bytes, bool high)
    {
        while (bytes > 0)
        {
            long piece;
            lock (_lock)
            {
                piece = Math.Min(bytes, _refillBytes);
            }

            Acquire(piece, high);
            bytes -= piece;
        }
    }

    private void Acquire(long bytes, bool high)
    {
        lock (_lock)
        {
            _requests++;
            _bytesThrough += bytes;
            long now = Stopwatch.GetTimestamp();
            if (now >= _nextRefill)
                Refill(now);

            // Waiters come first, so a new write does not take the budget from one that waits.
            if (_high.Count == 0 && _low.Count == 0 && _available >= bytes)
            {
                _available -= bytes;
                return;
            }

            Waiter waiter = new(bytes);
            (high ? _high : _low).Enqueue(waiter);
            _drainedThisPeriod = true;
            long start = now;
            while (!waiter.Granted)
            {
                now = Stopwatch.GetTimestamp();
                if (now >= _nextRefill)
                {
                    Refill(now);
                    continue;
                }

                Monitor.Wait(_lock, Stopwatch.GetElapsedTime(now, _nextRefill) + TimeSpan.FromMilliseconds(1));
            }

            _waitTicks += Stopwatch.GetTimestamp() - start;
        }
    }

    // Under the lock: a new period's budget, to the waiters in order, the flushes first except on every
    // fairness-th refill; then, for an auto-tuned limiter, a tune every RefillsPerTune periods.
    private void Refill(long now)
    {
        // A refill comes late when no write asks or a waiter wakes late, which a waiter does by design (it
        // sleeps a millisecond past the refill). The time since the last refill counts as drained when writes
        // waited in it, in clock ticks: counted in whole periods, a refill a little under a period late counted
        // one where two had passed, and steady demand read as half drained, so the rate never climbed.
        if (_drainedThisPeriod)
            _drainedTicks += now - _lastRefill;

        _lastRefill = now;
        _drainedThisPeriod = false;
        _nextRefill = now + _refillTicks;
        _available = _refillBytes;
        _refills++;
        if (_refills % _fairness == 0)
        {
            Grant(_low);
            Grant(_high);
        }
        else
        {
            Grant(_high);
            Grant(_low);
        }

        if (_high.Count > 0 || _low.Count > 0)
            _drainedThisPeriod = true;

        if (AutoTuned && now - _tuneStart >= RefillsPerTune * _refillTicks)
            Tune(now);

        Monitor.PulseAll(_lock);
    }

    // Under the lock: the waiters at the front, while the budget lasts; the first one it does not cover
    // takes what is left and waits for the rest.
    private void Grant(Queue<Waiter> queue)
    {
        while (queue.Count > 0 && _available > 0)
        {
            Waiter front = queue.Peek();
            long take = Math.Min(front.Remaining, _available);
            front.Remaining -= take;
            _available -= take;
            if (front.Remaining > 0)
                return;

            front.Granted = true;
            queue.Dequeue();
        }
    }

    // Under the lock: RocksDB's GenericRateLimiter::TuneLocked.
    private void Tune(long now)
    {
        long drainedPercent = _drainedTicks * 100 / Math.Max(1, now - _tuneStart);
        long floor = Math.Max(1, _maxBytesPerSecond / AllowedRangeFactor);
        long rate = _bytesPerSecond;
        if (drainedPercent == 0)
            rate = floor;
        else if (drainedPercent < LowWatermarkPercent)
            rate = Math.Max(floor, rate * 100 / (100 + AdjustPercent));
        else if (drainedPercent > HighWatermarkPercent)
            rate = Math.Min(_maxBytesPerSecond, rate * (100 + AdjustPercent) / 100);

        SetRate(rate);
        _tuneStart = now;
        _drainedTicks = 0;
    }

    // Under the lock, or in the constructor.
    private void SetRate(long bytesPerSecond)
    {
        _bytesPerSecond = bytesPerSecond;
        _refillBytes = Math.Max(1, (long)(bytesPerSecond * RefillPeriod.TotalSeconds));
    }

    // One write's piece that waits for its bytes.
    private sealed class Waiter(long bytes)
    {
        public long Remaining { get; set; } = bytes;

        public bool Granted { get; set; }
    }
}
