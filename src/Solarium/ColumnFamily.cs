using Solarium.Engine;

namespace Solarium;

/// <summary>
/// A named, ordered key space of a database, with its own memtables and SSTables. All column
/// families share the database's WAL and sequence numbers, so one <see cref="WriteBatch"/> can span
/// them atomically.
/// </summary>
public sealed class ColumnFamily
{
    /// <summary>The name of the column family every database has.</summary>
    public const string DefaultName = "default";

    private FamilyVersion _version;

    /// <summary>Wraps the manifest record, options, and first version of a family the database opened or created.</summary>
    internal ColumnFamily(SolariumDb owner, ManifestFamily record, ColumnFamilyOptions options, FamilyVersion version)
    {
        Owner = owner;
        AsArray = [this];
        Record = record;
        Options = options;
        _version = version;
    }

    /// <summary>The name this family was created with.</summary>
    public string Name => Record.Name;

    /// <summary>The id that WAL records and SSTables use to name this family.</summary>
    internal int Id => Record.Id;

    /// <summary>The database this family belongs to.</summary>
    internal SolariumDb Owner { get; }

    /// <summary>
    /// An array of this family alone: the families of a batch that writes to one family, with no
    /// allocation per batch. Never changed.
    /// </summary>
    internal ColumnFamily[] AsArray { get; }

    /// <summary>This family's entry in the manifest. Changed under the database's state lock only.</summary>
    internal ManifestFamily Record { get; }

    /// <summary>The options this family was opened with.</summary>
    internal ColumnFamilyOptions Options { get; }

    /// <summary>Memtables of this family flushed to SSTables since open. Changed and read under the database's state lock only.</summary>
    internal long FlushCount { get; set; }

    /// <summary>Compactions of this family since open that moved level 0 tables without a rewrite. Under the database's state lock only.</summary>
    internal long MoveCount { get; set; }

    /// <summary>Compactions of this family since open that merged tables into new ones. Under the database's state lock only.</summary>
    internal long MergeCount { get; set; }

    /// <summary>
    /// The current version, for the code that builds the next one under the database's state lock.
    /// A reader uses <see cref="EnterVersion"/>, which keeps the version's tables open.
    /// </summary>
    internal FamilyVersion Version => Volatile.Read(ref _version);

    /// <summary>
    /// The current version, with this reader counted in it; the caller passes <paramref name="slot"/>
    /// to <see cref="FamilyVersion.Leave"/> when it is done. Takes no lock.
    /// </summary>
    internal FamilyVersion EnterVersion(out int slot)
    {
        while (true)
        {
            FamilyVersion version = Volatile.Read(ref _version);
            slot = version.Enter();

            // Enter is a full fence, so this read comes after the count. If the version is still
            // current here, Install has not replaced it yet, and Retire, after Install, sees the
            // count. Otherwise the version may be closed already, so read the new one.
            if (ReferenceEquals(version, Volatile.Read(ref _version)))
                return version;

            version.Leave(slot);
        }
    }

    /// <summary>Publish a new version and retire the one it replaces. Under the database's state lock only.</summary>
    internal void Install(FamilyVersion version)
    {
        // The exchange is a full fence: a reader that counted itself in after it is sent to the new
        // version by its check in EnterVersion, and Retire sees every reader counted in before it.
        Interlocked.Exchange(ref _version, version).Retire();
    }

    /// <summary>The family name.</summary>
    public override string ToString() => Name;
}
