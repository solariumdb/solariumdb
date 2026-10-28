using Solarium.Storage;

namespace Solarium.Tests;

/// <summary>Owns a storage environment for one test and disposes it afterwards, if it is disposable.</summary>
public sealed class EnvironmentScope(IStorageEnvironment environment) : IDisposable
{
    /// <summary>The environment the test's databases use.</summary>
    public IStorageEnvironment Environment { get; } = environment;

    /// <summary>Builds database options that use <see cref="Environment"/>.</summary>
    public DbOptions Options(bool createMissingColumnFamilies = false)
    {
        return new DbOptions { Environment = Environment, CreateMissingColumnFamilies = createMissingColumnFamilies };
    }

    /// <summary>Disposes the environment when it is disposable; the shared default environment is not.</summary>
    public void Dispose() => (Environment as IDisposable)?.Dispose();
}
