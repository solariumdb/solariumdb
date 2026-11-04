namespace Solarium;

/// <summary>
/// A failure of the engine itself: corruption, or a write after a storage error. <see cref="IsCorruption"/> tells
/// the two apart, as RocksDB's <c>Status::IsCorruption</c> does.
/// </summary>
public sealed class SolariumException : Exception
{
    /// <summary>
    /// Creates an exception whose <paramref name="message"/> names the directory, offset, or id involved. It is not
    /// a corruption; <see cref="Corruption(string, Exception?)"/> creates one.
    /// </summary>
    public SolariumException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Creates an exception that wraps the <paramref name="inner"/> failure that caused it. It is a corruption when
    /// <paramref name="inner"/> is one: a write refused after a read found damage still reports the damage.
    /// </summary>
    public SolariumException(string message, Exception inner)
        : base(message, inner)
    {
        IsCorruption = inner is SolariumException { IsCorruption: true };
    }

    private SolariumException(string message, Exception? inner, bool corruption)
        : base(message, inner)
    {
        IsCorruption = corruption;
    }

    /// <summary>
    /// True when the files of the database hold damage that no crash explains: a block or a record that fails its
    /// checksum or does not decode, a block at another block's place, or a file that is missing or is not the one the
    /// manifest records. Retrying the operation fails again; the data must come from another copy. False for a
    /// failure of the storage itself, such as a full disk, after which the database is opened again.
    /// </summary>
    public bool IsCorruption { get; }

    /// <summary>
    /// Creates a corruption (<see cref="IsCorruption"/>) whose <paramref name="message"/> names the file and the
    /// offset or id involved, caused by <paramref name="inner"/> when there is one.
    /// </summary>
    internal static SolariumException Corruption(string message, Exception? inner = null) => new(message, inner, corruption: true);
}
