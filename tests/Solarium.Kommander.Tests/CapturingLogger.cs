using Kommander;
using Microsoft.Extensions.Logging;

namespace Solarium.Kommander.Tests;

/// <summary>A logger that keeps the warnings it gets, for the tests that check what the WAL warns about.</summary>
internal sealed class CapturingLogger : ILogger<IRaft>
{
    private readonly List<string> _warnings = [];

    /// <summary>The formatted warnings logged so far, oldest first.</summary>
    public IReadOnlyList<string> Warnings
    {
        get
        {
            lock (_warnings)
            {
                return [.. _warnings];
            }
        }
    }

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel) => true;

    /// <inheritdoc />
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (logLevel != LogLevel.Warning)
            return;

        lock (_warnings)
            _warnings.Add(formatter(state, exception));
    }
}
