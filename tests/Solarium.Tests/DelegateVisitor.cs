namespace Solarium.Tests;

/// <summary>A visitor that calls a delegate, for the tests whose visitors differ from call to call.</summary>
internal readonly struct DelegateVisitor(DelegateVisitor.Visit visit) : IValueVisitor
{
    /// <summary>What <see cref="OnValue"/> calls.</summary>
    public delegate void Visit(int index, ReadOnlySpan<byte> value);

    /// <inheritdoc/>
    public void OnValue(int index, ReadOnlySpan<byte> value) => visit(index, value);
}
