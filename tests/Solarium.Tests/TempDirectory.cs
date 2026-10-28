namespace Solarium.Tests;

/// <summary>A unique temporary directory for one test, deleted on dispose.</summary>
public sealed class TempDirectory : IDisposable
{
    /// <summary>The absolute path of the directory.</summary>
    public string Path { get; } = Directory.CreateTempSubdirectory("solarium-").FullName;

    /// <summary>Returns the path of <paramref name="name"/> inside the directory.</summary>
    public string Combine(string name) => System.IO.Path.Combine(Path, name);

    /// <summary>Deletes the directory and its contents; an I/O failure while deleting is ignored.</summary>
    public void Dispose()
    {
        try
        {
            Directory.Delete(Path, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
