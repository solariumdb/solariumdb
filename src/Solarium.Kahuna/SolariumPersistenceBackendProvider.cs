using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Providers;
using Microsoft.Extensions.Logging;
using Solarium.Storage;

namespace Solarium.Kahuna;

/// <summary>
/// Kahuna's <c>--storage solarium</c>: the key-value and lock data in a Solarium database at
/// <c>{storage path}/{revision}</c>, with the column families <c>kv</c> and <c>locks</c> and the rows of
/// Kahuna's RocksDB backend. A checkpoint (<see cref="SolariumDb.CreateCheckpoint"/>) links the store's tables
/// where the file system allows it, so a live checkpoint is staged on the store's file system, as the RocksDB
/// backend's is.
/// </summary>
public sealed class SolariumPersistenceBackendProvider : IPersistenceBackendProvider
{
    /// <summary>The storage type name.</summary>
    public const string TypeName = "solarium";

    private readonly IStorageEnvironment? environment;   // null: the node's solarium.environment chooses

    /// <summary>
    /// A provider whose store does its file I/O through the environment the node chooses with
    /// <c>--extension-option solarium.environment=default|io_uring</c> (<see cref="SolariumExtensionOptions"/>); the
    /// constructor the extension loader calls.
    /// </summary>
    public SolariumPersistenceBackendProvider()
    {
    }

    /// <summary>A provider whose databases do their file I/O through <paramref name="environment"/>, whatever the node's settings.</summary>
    public SolariumPersistenceBackendProvider(IStorageEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(environment);
        this.environment = environment;
    }

    /// <inheritdoc />
    public string StorageType => TypeName;

    /// <inheritdoc />
    public bool KeepsDataOnDisk => true;

    /// <inheritdoc />
    /// <remarks>True: a checkpoint links the tables only on the store's own file system.</remarks>
    public bool StagesLiveCheckpointsInStore => true;

    /// <inheritdoc />
    /// <remarks>
    /// The node's <c>StorageCompactionRateBytesPerSec</c> and <c>StorageMaxBackgroundCompactions</c> pace the
    /// store's compactions (see <see cref="SolariumPersistenceBackend.CompactionThreads"/>). The node's shared
    /// memory (<see cref="SolariumSharedMemory"/>) gives the store the cache and the memtable budget that a
    /// Solarium WAL of the node uses too; without a shared cache, the store keeps its index and filter blocks in
    /// <see cref="SolariumPersistenceBackend.SharedBlockCache"/>. The node's <c>solarium.environment</c> chooses the
    /// storage environment, unless the provider was given one.
    /// </remarks>
    /// <exception cref="ArgumentException">The node's Solarium settings are unknown or invalid.</exception>
    /// <exception cref="PlatformNotSupportedException">The node chose io_uring, which this machine does not have.</exception>
    public IPersistenceBackend Open(PersistenceBackendContext context)
    {
        SolariumSharedMemory? shared = SolariumSharedMemory.For(context.SharedMemory);
        IStorageEnvironment chosen = environment ?? SolariumExtensionOptions.Environment(context.ExtensionOptions);
        return new SolariumPersistenceBackend(context.StoragePath, context.StorageRevision, context.Logger, chosen,
            context.Configuration.StorageCompactionRateBytesPerSec, context.Configuration.StorageMaxBackgroundCompactions,
            blockCache: shared?.Cache, writeBufferManager: shared?.WriteBufferManager);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Kahuna gives no settings here: a backup or restore opens the copy on the default environment unless the
    /// provider was given one. The environment changes only how the files are read and written, not what they hold.
    /// </remarks>
    public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) =>
        new SolariumPersistenceBackend(directory, revision, logger, environment ?? DefaultStorageEnvironment.Instance);

    /// <inheritdoc />
    /// <remarks>An empty revision opens the root itself, as the RocksDB backend does.</remarks>
    public string DataDirectory(string storageRoot, string revision) =>
        string.IsNullOrEmpty(revision) ? storageRoot : Path.Combine(storageRoot, revision);

    /// <inheritdoc />
    public bool HasData(string storageRoot, string revision)
    {
        string directory = DataDirectory(storageRoot, revision);
        return File.Exists(directory) || (Directory.Exists(directory) && Directory.EnumerateFileSystemEntries(directory).Any());
    }

    /// <inheritdoc />
    public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
        SolariumPersistenceBackend.MergeCheckpointInto(targetDirectory, sourceDirectory, environment ?? DefaultStorageEnvironment.Instance, ct);
}
