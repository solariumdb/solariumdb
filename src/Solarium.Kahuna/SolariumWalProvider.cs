using Kahuna.Server.Persistence.Providers;
using Kommander.WAL;
using Solarium.Kommander;
using Solarium.Storage;

namespace Solarium.Kahuna;

/// <summary>
/// Kahuna's <c>--wal-storage solarium</c>: the Raft log in a <see cref="SolariumWAL"/> at
/// <c>{WAL path}/{revision}</c>. The RocksDB WAL's tuning does not apply; the node's shared cache and memtable
/// budget do (<see cref="SolariumSharedMemory"/>), and a Solarium store of the node uses the same ones, and the
/// same storage environment (<see cref="SolariumExtensionOptions"/>).
/// </summary>
public sealed class SolariumWalProvider : IWalProvider
{
    private readonly IStorageEnvironment? environment;   // null: the node's solarium.environment chooses

    /// <summary>
    /// A provider whose WAL does its file I/O through the environment the node chooses with
    /// <c>--extension-option solarium.environment=default|io_uring</c> (<see cref="SolariumExtensionOptions"/>); the
    /// constructor the extension loader calls.
    /// </summary>
    public SolariumWalProvider()
    {
    }

    /// <summary>A provider whose WALs do their file I/O through <paramref name="environment"/>, whatever the node's settings.</summary>
    public SolariumWalProvider(IStorageEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(environment);
        this.environment = environment;
    }

    /// <inheritdoc />
    public string StorageType => SolariumPersistenceBackendProvider.TypeName;

    /// <inheritdoc />
    public bool KeepsDataOnDisk => true;

    /// <inheritdoc />
    /// <exception cref="ArgumentException">The node's Solarium settings are unknown or invalid.</exception>
    /// <exception cref="PlatformNotSupportedException">The node chose io_uring, which this machine does not have.</exception>
    public IWAL Open(WalContext context) =>
        new SolariumWAL(context.Path, context.Revision, context.Logger, context.SyncWrites, SolariumWalOptions.Default with
        {
            Environment = environment ?? SolariumExtensionOptions.Environment(context.ExtensionOptions),
            WriteBufferManager = SolariumSharedMemory.For(context.SharedMemory)?.WriteBufferManager,
            BlockCache = SolariumSharedMemory.For(context.SharedMemory)?.Cache,
        });
}
