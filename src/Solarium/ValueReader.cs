namespace Solarium;

/// <summary>
/// Reads a value in place, for <see cref="SolariumDb.TryGet{TResult}"/>: <paramref name="value"/> lies in the
/// engine's memory, and is valid only until the reader returns, so the reader copies what it keeps.
/// </summary>
/// <typeparam name="TResult">What the reader makes of the value.</typeparam>
/// <param name="value">The value, empty for a key that was set to an empty one.</param>
/// <returns>What <see cref="SolariumDb.TryGet{TResult}"/> gives its caller.</returns>
public delegate TResult ValueReader<out TResult>(ReadOnlySpan<byte> value);
