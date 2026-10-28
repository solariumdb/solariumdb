using Solarium.IoUring;
using Solarium.Storage;

namespace Solarium.Kahuna;

/// <summary>
/// The settings a Kahuna node gives the Solarium engine with <c>--extension-option</c>, which reach the providers in
/// the <c>ExtensionOptions</c> of their contexts. The keys start with <c>solarium.</c>; a provider refuses one it
/// does not know, so a misspelled setting fails the open instead of being ignored.
/// </summary>
internal static class SolariumExtensionOptions
{
    /// <summary>
    /// The storage environment of the node's Solarium WAL and store: <c>default</c> (the default) or <c>io_uring</c>,
    /// Linux only.
    /// </summary>
    public const string EnvironmentKey = "solarium.environment";

    private const string Prefix = "solarium.";

    // One io_uring environment, and so one reactor thread, for every Solarium database of the process. It lives as
    // long as the process: the databases close their files, and the reactor thread is a background thread.
    private static readonly Lazy<IoUringStorageEnvironment> IoUring = new(() => new IoUringStorageEnvironment(), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    /// The storage environment that <paramref name="options"/> choose: the default environment when they name none,
    /// or the process's io_uring environment for <c>io_uring</c>.
    /// </summary>
    /// <exception cref="ArgumentException">A <c>solarium.</c> key is unknown, or the environment is neither <c>default</c> nor <c>io_uring</c>.</exception>
    /// <exception cref="PlatformNotSupportedException">The options choose io_uring, and this machine cannot create an io_uring ring.</exception>
    public static IStorageEnvironment Environment(IReadOnlyDictionary<string, string> options)
    {
        foreach (string key in options.Keys)
            if (key.StartsWith(Prefix, StringComparison.Ordinal) && key != EnvironmentKey)
                throw new ArgumentException($"--extension-option {key} is not a Solarium setting; the only one is {EnvironmentKey}.", nameof(options));

        if (!options.TryGetValue(EnvironmentKey, out string? environment) || environment == "default")
            return DefaultStorageEnvironment.Instance;

        if (environment != "io_uring")
            throw new ArgumentException($"--extension-option {EnvironmentKey}={environment} is not an environment; use default or io_uring.", nameof(options));

        if (!IoUring.IsValueCreated && !IoUringStorageEnvironment.IsSupported())
            throw new PlatformNotSupportedException($"--extension-option {EnvironmentKey}=io_uring needs Linux with io_uring, and this machine cannot create an io_uring ring.");

        return IoUring.Value;
    }
}
