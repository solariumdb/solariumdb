namespace Solarium.Engine;

/// <summary>
/// Whether the long spin of a waiting writer pays, learned from how probes ended: a probe is a
/// turn that spun for the whole long spin, and it either got its turn in time or blocked. As
/// RocksDB's adaptive yield credit, the credit decays by 1/1024 at each probe, rises by a fixed
/// step when the probe ended in time, and falls by three such steps when it blocked. So it stays
/// at or above 0 while at most a quarter of the probes block. One database keeps one credit for
/// all its writers.
/// </summary>
/// <remarks>
/// The threshold of a quarter lies between the measured cases. Where the long spin was faster than
/// a short one, 13% or fewer of its spins blocked. Where it was slower, 40% or more blocked: the
/// spinning writers held CPUs that the leader, its hyperthread sibling or the storage environment's
/// I/O thread needed, and the groups took longer than the spin anyway.
/// </remarks>
internal sealed class WriterSpinCredit
{
    private const int Decay = 1024;
    // The credit stays between about -Loss × Decay and Gain × Decay, 3 × 2^27 at most, far from the
    // limits of an int.
    private const int Gain = 1 << 17;
    private const int Loss = 3 * Gain;

    /// <summary>
    /// The credit of a new database: 16 probes that block in a row take it below 0. RocksDB's credit
    /// starts at 0, where one block stops the yields. Here one early block would make the spin short,
    /// and while it is short, probes among the waits of short spins can keep blocking where the long
    /// spin pays: on Linux, with six writers on one thread per core on io_uring, a credit that
    /// started at 0 made 28% to 38% of the turns spin short, where a fixed long spin blocked on 12%
    /// of its turns. Where the long spin does not pay, the margin costs about 16 probes, some 4,000
    /// turns, once.
    /// </summary>
    public const int Start = 16 * Loss;

    // Read by every waiting writer, written only by probes. Two probes that end at once can lose one
    // update. That drops the outcome of one probe, as if it had not been sampled: RocksDB updates its
    // credit the same way.
    private int _credit = Start;

    /// <summary>The credit now: <see cref="Start"/> at the start, negative when too many probes blocked.</summary>
    public int Value => Volatile.Read(ref _credit);

    /// <summary>True while the long spin pays: the credit is 0 or more. A new database starts with the long spin.</summary>
    public bool LongSpinPays => Volatile.Read(ref _credit) >= 0;

    /// <summary>Record the outcome of one probe: <paramref name="inTime"/> when its turn came before the long spin ended.</summary>
    public void Record(bool inTime)
    {
        int credit = Volatile.Read(ref _credit);
        Volatile.Write(ref _credit, credit - credit / Decay + (inTime ? Gain : -Loss));
    }
}
