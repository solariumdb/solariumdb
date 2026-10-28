namespace Solarium;

/// <summary>A failure of the engine itself: corruption, or a write after a storage error.</summary>
public sealed class SolariumException : Exception
{
    /// <summary>Creates an exception whose <paramref name="message"/> names the directory, offset, or id involved.</summary>
    public SolariumException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an exception that wraps the <paramref name="inner"/> failure that caused it.</summary>
    public SolariumException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
