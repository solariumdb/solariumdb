using Solarium.IoUring;
using Solarium.Storage;

namespace Solarium.Tests;

/// <summary>
/// The storage environments this machine can run. The default one runs everywhere; io_uring joins
/// on Linux with io_uring, so the same tests cover both there.
/// </summary>
public static class Environments
{
    /// <summary>Lists the environment names theory tests run over: "default", plus "io_uring" where supported.</summary>
    public static TheoryData<string> Available()
    {
        TheoryData<string> names = new() { "default" };
        if (IoUringStorageEnvironment.IsSupported())
            names.Add("io_uring");

        return names;
    }

    /// <summary>
    /// Every name of <see cref="Available"/>, once with the commit of write groups pipelined
    /// (<see cref="DbOptions.EnablePipelinedWrite"/>) and once without.
    /// </summary>
    public static TheoryData<string, bool> AvailableWithCommits()
    {
        TheoryData<string, bool> cases = new();
        foreach (string name in IoUringStorageEnvironment.IsSupported() ? new[] { "default", "io_uring" } : ["default"])
        {
            cases.Add(name, false);
            cases.Add(name, true);
        }

        return cases;
    }

    /// <summary>Creates the environment called <paramref name="name"/>; the scope disposes it when the test ends.</summary>
    public static EnvironmentScope Create(string name)
    {
        return new EnvironmentScope(name == "io_uring" ? new IoUringStorageEnvironment() : DefaultStorageEnvironment.Instance);
    }
}
