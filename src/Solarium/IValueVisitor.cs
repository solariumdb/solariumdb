namespace Solarium;

/// <summary>
/// Reads the values of a batched lookup in place, for <see cref="SolariumDb.MultiGet{TVisitor}"/>, as the
/// <c>IMultiGetValueVisitor</c> of <c>Kommander.RocksDB</c>. A caller implements it with a struct.
/// </summary>
/// <remarks>
/// A struct and not a delegate, so that the lookup calls the visitor directly, and so that no two visitors share one
/// compiled lookup. A delegate that takes a generic state shares it among all reference types, and the .NET 10 JIT
/// fails, and ends the process, when it inlines the delegate of one state, which stores references into an array, into
/// a caller whose state is an array of numbers.
/// </remarks>
public interface IValueVisitor
{
    /// <summary>
    /// Reads one value. <paramref name="value"/> lies in the engine's memory, and is valid only until this returns, so
    /// the visitor copies what it keeps. An exception ends the lookup and reaches its caller.
    /// </summary>
    /// <param name="index">The place of the value's key in the keys of the lookup.</param>
    /// <param name="value">The value, empty for a key that was set to an empty one.</param>
    void OnValue(int index, ReadOnlySpan<byte> value);
}
