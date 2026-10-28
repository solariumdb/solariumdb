using System.Diagnostics;
using System.Text.Json;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The folder that <c>dotnet publish</c> makes of the extension, the one a Kahuna server loads with
/// <c>--extension-assembly</c>. The server's loader takes the server's own copy of every assembly the server has,
/// and the server has Kahuna.Core and every package Kahuna.Core depends on. Any other package the extension uses
/// must be in the folder, or the server fails to load it at its first use: a folder without LZ4 failed the first
/// table a node wrote. The tests run on project references and never load the published folder, so this test
/// publishes it and checks it against the extension's dependency graph.
/// </summary>
public sealed class TestPublishedExtension : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Fact]
    public void The_published_folder_holds_every_package_the_server_lacks_and_none_it_has()
    {
        DirectoryInfo output = new(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        string framework = output.Name;
        string configuration = output.Parent!.Name;
        string root = RepositoryRoot(output);
        string project = Path.Combine(root, "src", "Solarium.Kahuna", "Solarium.Kahuna.csproj");
        string depsFile = Path.Combine(root, "src", "Solarium.Kahuna", "bin", configuration, framework, "Solarium.Kahuna.deps.json");

        Publish(project, configuration, _directory.Path);

        using JsonDocument deps = JsonDocument.Parse(File.ReadAllText(depsFile));
        Dictionary<string, JsonElement> targets = [];
        foreach (JsonProperty target in deps.RootElement.GetProperty("targets").EnumerateObject().First().Value.EnumerateObject())
            targets[target.Name.Split('/')[0]] = target.Value;

        HashSet<string> serverHas = Closure(targets, "Kahuna.Core");
        Assert.Contains("Kommander", serverHas);

        List<string> missing = [];
        List<string> extra = [];
        List<string> serverLacks = [];

        foreach (JsonProperty library in deps.RootElement.GetProperty("libraries").EnumerateObject())
        {
            string name = library.Name.Split('/')[0];
            if (library.Value.GetProperty("type").GetString() != "package" || !targets.TryGetValue(name, out JsonElement target))
                continue;

            bool lacked = !serverHas.Contains(name);
            if (lacked)
                serverLacks.Add(name);

            foreach (string file in RuntimeFiles(target))
            {
                bool published = File.Exists(Path.Combine(_directory.Path, file));
                if (lacked && !published)
                    missing.Add($"{name}: {file}");
                else if (!lacked && published)
                    extra.Add($"{name}: {file}");
            }
        }

        Assert.True(missing.Count == 0,
            "The published folder lacks files of packages the server does not have; keep the package in PublishOnlyWhatTheServerLacks:" +
            Environment.NewLine + string.Join(Environment.NewLine, missing));
        Assert.True(extra.Count == 0,
            "The published folder holds files of packages the server has, which the server's copy shadows:" +
            Environment.NewLine + string.Join(Environment.NewLine, extra));

        // The packages Solarium brings today; a new one shows here first, and must then be published.
        Assert.Equal(["K4os.Compression.LZ4", "ioxide"], serverLacks.Order(StringComparer.Ordinal));
    }

    // The server ships each package of Kahuna.Core's closure at the version the closure asks for, and its loader
    // gives the extension that copy, so a newer version in the extension's own graph is never loaded: the first
    // use of the package fails. System.IO.Hashing 10.0.14 against Kahuna.Core's 10.0.13 failed the first database
    // a node opened, while the published folder was right.
    [Fact]
    public void No_package_the_server_supplies_is_newer_in_the_extension_than_kahuna_core_asks_for()
    {
        // The restore's assets file keeps each package's dependencies as its package declares them, where the
        // deps file has only the versions this build resolved.
        DirectoryInfo output = new(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        string assetsFile = Path.Combine(RepositoryRoot(output), "src", "Solarium.Kahuna", "obj", "project.assets.json");

        using JsonDocument assets = JsonDocument.Parse(File.ReadAllText(assetsFile));
        Dictionary<string, JsonElement> targets = [];
        Dictionary<string, Version> resolved = new(StringComparer.Ordinal);
        foreach (JsonProperty target in assets.RootElement.GetProperty("targets").EnumerateObject().First().Value.EnumerateObject())
        {
            if (target.Value.GetProperty("type").GetString() != "package")
                continue;

            string[] parts = target.Name.Split('/');
            targets[parts[0]] = target.Value;
            resolved[parts[0]] = NumericVersion(parts[1]);
        }

        // The highest version that a package of Kahuna.Core's closure asks for, by package.
        Dictionary<string, Version> serverShips = new(StringComparer.Ordinal);
        foreach (string name in Closure(targets, "Kahuna.Core"))
        {
            if (!targets.TryGetValue(name, out JsonElement target) || !target.TryGetProperty("dependencies", out JsonElement dependencies))
                continue;

            foreach (JsonProperty dependency in dependencies.EnumerateObject())
            {
                Version asked = NumericVersion(dependency.Value.GetString()!.Trim('[', ']', '(', ')').Split(',')[0]);
                if (!serverShips.TryGetValue(dependency.Name, out Version? shipped) || asked > shipped)
                    serverShips[dependency.Name] = asked;
            }
        }

        Assert.Contains("System.IO.Hashing", serverShips.Keys);
        List<string> newer = [];
        foreach ((string name, Version version) in resolved)
            if (serverShips.TryGetValue(name, out Version? shipped) && version > shipped)
                newer.Add($"{name}: the extension resolves {version}, Kahuna.Core's packages ask for {shipped}");

        Assert.True(newer.Count == 0,
            "The extension needs newer versions of packages that the server supplies; reference Kahuna.Core's versions, or a Kahuna.Core that asks for these:" +
            Environment.NewLine + string.Join(Environment.NewLine, newer));
    }

    public void Dispose() => _directory.Dispose();

    // The numeric part of a package version, without a prerelease label.
    private static Version NumericVersion(string version) => Version.Parse(version.Split('-')[0]);

    // The packages that root depends on, itself included.
    private static HashSet<string> Closure(Dictionary<string, JsonElement> targets, string root)
    {
        HashSet<string> seen = new(StringComparer.Ordinal);
        Stack<string> pending = new([root]);

        while (pending.Count > 0)
        {
            string name = pending.Pop();
            if (!seen.Add(name) || !targets.TryGetValue(name, out JsonElement target))
                continue;

            if (target.TryGetProperty("dependencies", out JsonElement dependencies))
                foreach (JsonProperty dependency in dependencies.EnumerateObject())
                    pending.Push(dependency.Name);
        }

        return seen;
    }

    // Where publish puts a package's assets: assemblies at the root, native files under their runtimes/ path.
    private static IEnumerable<string> RuntimeFiles(JsonElement target)
    {
        if (target.TryGetProperty("runtime", out JsonElement runtime))
            foreach (JsonProperty asset in runtime.EnumerateObject())
                yield return Path.GetFileName(asset.Name);

        if (target.TryGetProperty("runtimeTargets", out JsonElement runtimeTargets))
            foreach (JsonProperty asset in runtimeTargets.EnumerateObject())
                yield return asset.Name.Replace('/', Path.DirectorySeparatorChar);
    }

    private static string RepositoryRoot(DirectoryInfo start)
    {
        for (DirectoryInfo? directory = start; directory is not null; directory = directory.Parent)
            if (File.Exists(Path.Combine(directory.FullName, "Solarium.slnx")))
                return directory.FullName;

        throw new InvalidOperationException($"No Solarium.slnx above {start.FullName}.");
    }

    // The build this test runs on already built the extension, so publish only lays out the folder. Without
    // node reuse and build servers: a node that outlives the publish inherits its output pipes and keeps them
    // open, so reading them to the end would wait for that node.
    private static void Publish(string project, string configuration, string folder)
    {
        ProcessStartInfo start = new(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (string argument in new[] { "publish", project, "-c", configuration, "--no-build", "--nologo", "-nodeReuse:false", "-p:UseSharedCompilation=false", "-o", folder })
            start.ArgumentList.Add(argument);

        start.Environment["MSBUILDDISABLENODEREUSE"] = "1";
        start.Environment["DOTNET_CLI_USE_MSBUILD_SERVER"] = "0";

        using Process process = Process.Start(start)!;
        Task<string> output = process.StandardOutput.ReadToEndAsync();
        Task<string> error = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(TimeSpan.FromMinutes(3)))
        {
            process.Kill(entireProcessTree: true);
            Assert.Fail("dotnet publish did not finish within 3 minutes.");
        }

        // Bounded: should anything still hold the pipes, the test fails with what it read instead of waiting.
        bool drained = Task.WaitAll([output, error], TimeSpan.FromSeconds(30));
        string log = drained ? output.Result + error.Result : "(the output did not close)";
        Assert.True(process.ExitCode == 0, $"dotnet publish failed with {process.ExitCode}:{Environment.NewLine}{log}");
    }
}
