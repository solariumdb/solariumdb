using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Providers;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.IoUring;
using Solarium.Storage;

namespace Solarium.Kahuna.Benchmarks;

/// <summary>
/// One Kahuna persistence backend, opened as a Kahuna server opens it: through the provider registered
/// for its storage type, with Kahuna's default configuration, and with no resources shared with a WAL.
/// </summary>
internal sealed class BackendEngine : IDisposable
{
    /// <summary>The revision directory under the session's directory, as a server's <c>--storage-revision</c>.</summary>
    public const string Revision = "v1";

    /// <summary>
    /// The engines: <c>rocksdb</c> with Kahuna's defaults (direct reads, the process-wide 256 MiB block
    /// cache); <c>rocksdb-buffered</c>, the same with direct reads off, so its tables are read through the
    /// page cache as Solarium's are; and Solarium on the default and the io_uring environment.
    /// </summary>
    public static readonly string[] All = ["rocksdb", "rocksdb-buffered", "solarium", "solarium-io_uring"];

    private readonly IPersistenceBackendProvider _provider;
    private readonly KahunaConfiguration _configuration;
    private readonly IStorageEnvironment? _environment;

    private BackendEngine(string name, string directory, IPersistenceBackendProvider provider, KahunaConfiguration configuration, IStorageEnvironment? environment)
    {
        Name = name;
        Directory = directory;
        _provider = provider;
        _configuration = configuration;
        _environment = environment;
    }

    /// <summary>The engine's name, one of <see cref="All"/>.</summary>
    public string Name { get; }

    /// <summary>The storage root: the store lives in its <see cref="Revision"/> subdirectory.</summary>
    public string Directory { get; }

    /// <summary>The open backend, or null between <see cref="Close"/> and <see cref="Open"/>.</summary>
    public IPersistenceBackend? Backend { get; private set; }

    /// <summary>Creates the engine <paramref name="name"/> over <paramref name="directory"/>, without opening it.</summary>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not one of <see cref="All"/>.</exception>
    public static BackendEngine Create(string name, string directory)
    {
        KahunaConfiguration configuration = new();
        switch (name)
        {
            case "rocksdb":
                return new(name, directory, StorageProviders.CreateDefault().GetBackend("rocksdb"), configuration, null);

            case "rocksdb-buffered":
                configuration.RocksDbDirectReads = false;
                return new(name, directory, StorageProviders.CreateDefault().GetBackend("rocksdb"), configuration, null);

            case "solarium":
                return new(name, directory, new SolariumPersistenceBackendProvider(), configuration, null);

            case "solarium-io_uring":
                IoUringStorageEnvironment environment = new();
                return new(name, directory, new SolariumPersistenceBackendProvider(environment), configuration, environment);

            default:
                throw new ArgumentException($"Unknown engine '{name}'; the engines are {string.Join(", ", All)}.", nameof(name));
        }
    }

    /// <summary>Opens the backend, creating the storage root and the store if they do not exist.</summary>
    public void Open()
    {
        // A server creates the storage root before it opens the backend; RocksDB does not create parents.
        System.IO.Directory.CreateDirectory(Directory);
        Backend = _provider.Open(new PersistenceBackendContext
        {
            StoragePath = Directory,
            StorageRevision = Revision,
            Configuration = _configuration,
            Logger = NullLogger.Instance,
        });
    }

    /// <summary>Closes the backend; the store stays on disk.</summary>
    public void Close()
    {
        (Backend as IDisposable)?.Dispose();
        Backend = null;
    }

    /// <summary>
    /// The bytes of every file under the storage root. The engine keeps writing meanwhile, so a file
    /// listed can be gone when its size is read, a temporary file renamed or a compacted table deleted;
    /// it counts as nothing.
    /// </summary>
    public long BytesOnDisk()
    {
        long bytes = 0;
        foreach (FileInfo file in new DirectoryInfo(Directory).EnumerateFiles("*", SearchOption.AllDirectories))
        {
            try
            {
                bytes += file.Length;
            }
            catch (FileNotFoundException)
            {
            }
        }

        return bytes;
    }

    /// <summary>Closes the backend and the storage environment, and deletes the storage root.</summary>
    public void Dispose()
    {
        Close();
        (_environment as IDisposable)?.Dispose();
        if (System.IO.Directory.Exists(Directory))
            System.IO.Directory.Delete(Directory, recursive: true);
    }
}
