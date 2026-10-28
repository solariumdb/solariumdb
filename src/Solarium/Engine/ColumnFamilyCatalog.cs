using System.Text;
using Solarium.Storage;

namespace Solarium.Engine;

/// <summary>
/// Column family names, and the <c>COLUMN_FAMILIES</c> file of format 1 (one <c>id TAB name</c> line
/// each), which the manifest replaced. Open reads that file only to migrate a format-1 directory.
/// </summary>
internal static class ColumnFamilyCatalog
{
    /// <summary>The name of the format-1 catalog file inside the database directory.</summary>
    public const string FileName = "COLUMN_FAMILIES";

    /// <summary>Reads the catalog of <paramref name="directory"/> (id to name); a missing file yields only the default family. Throws <see cref="SolariumException"/> on a malformed line.</summary>
    public static SortedDictionary<int, string> Load(IStorageEnvironment environment, string directory)
    {
        SortedDictionary<int, string> families = new();
        string path = Path.Combine(directory, FileName);

        if (!environment.FileExists(path))
        {
            families[0] = ColumnFamily.DefaultName;
            return families;
        }

        // Lines as File.ReadAllLines splits them: a blank line inside the file is still corrupt.
        using StringReader reader = new(Encoding.UTF8.GetString(DurableFiles.ReadAllBytes(environment, path)));
        for (string? line = reader.ReadLine(); line is not null; line = reader.ReadLine())
        {
            string[] parts = line.Split('\t');
            if (parts.Length != 2 || !int.TryParse(parts[0], out int id) || id < 0 || id > Manifest.MaxFamilyId || !families.TryAdd(id, parts[1]))
                throw new SolariumException($"'{path}' is corrupt at line '{line}'.");
        }

        return families;
    }

    /// <summary>Throws <see cref="ArgumentException"/> unless <paramref name="name"/> is non-empty with no tab or line break.</summary>
    public static void Validate(string name)
    {
        if (string.IsNullOrEmpty(name) || name.AsSpan().IndexOfAny('\t', '\n', '\r') >= 0)
            throw new ArgumentException($"'{name}' is not a valid column family name: it must be non-empty, with no tab or line break.");
    }
}
