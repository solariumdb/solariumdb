using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Providers;
using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Solarium.IoUring;
using Solarium.Storage;
using Solarium.Tests;

namespace Solarium.Kahuna.Tests;

/// <summary>
/// The node's Solarium settings, from <c>--extension-option</c>: <c>solarium.environment</c> chooses the storage
/// environment of the WAL and the store that the providers the extension loader creates open; io_uring is one
/// environment for both; and an unknown Solarium setting or environment fails the open, naming it, while another
/// extension's settings are left alone.
/// </summary>
public sealed class TestExtensionOptions : IDisposable
{
    private readonly TempDirectory _directory = new();

    [Theory]
    [InlineData(null, null)]
    [InlineData("solarium.environment", "default")]
    [InlineData("acme.mode", "fast")]
    public void Without_io_uring_the_store_and_the_wal_open_on_the_default_environment(string? key, string? value)
    {
        Dictionary<string, string> options = key is null ? [] : new() { [key] = value! };

        using SolariumPersistenceBackend store = OpenStore(options);
        Assert.Same(DefaultStorageEnvironment.Instance, store.Environment);
        Assert.Same(DefaultStorageEnvironment.Instance, SolariumExtensionOptions.Environment(options));
        ((IDisposable)OpenWal(options)).Dispose();
    }

    [Fact]
    public void Io_uring_opens_the_wal_and_the_store_on_one_io_uring_environment_and_they_read_back_after_a_reopen()
    {
        Dictionary<string, string> options = new() { [SolariumExtensionOptions.EnvironmentKey] = "io_uring" };

        // macOS and Windows have no io_uring: the open fails and says why.
        if (!IoUringStorageEnvironment.IsSupported())
        {
            Assert.Throws<PlatformNotSupportedException>(() => OpenStore(options));
            return;
        }

        IStorageEnvironment environment;
        using (SolariumPersistenceBackend store = OpenStore(options))
        {
            environment = Assert.IsType<IoUringStorageEnvironment>(store.Environment);
            Assert.True(store.StoreKeyValues([Item("io/1")]));
        }

        Assert.Same(environment, SolariumExtensionOptions.Environment(options));
        using (SolariumPersistenceBackend reopened = OpenStore(options))
        {
            Assert.Same(environment, reopened.Environment);
            Assert.NotNull(reopened.GetKeyValue("io/1"));
        }

        IWAL wal = OpenWal(options);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "op", LogData = [1, 2, 3] }])]));
        ((IDisposable)wal).Dispose();

        IWAL reopenedWal = OpenWal(options);
        Assert.Equal([1L], reopenedWal.ReadLogsRange(1, 0).Select(log => log.Id));
        ((IDisposable)reopenedWal).Dispose();
    }

    [Theory]
    [InlineData("solarium.enviroment", "io_uring", "solarium.enviroment")]
    [InlineData("solarium.environment", "uring", "solarium.environment=uring")]
    public void An_unknown_solarium_setting_or_environment_fails_both_opens_naming_it(string key, string value, string named)
    {
        Dictionary<string, string> options = new() { [key] = value };

        Assert.Contains(named, Assert.Throws<ArgumentException>(() => OpenStore(options)).Message, StringComparison.Ordinal);
        Assert.Contains(named, Assert.Throws<ArgumentException>(() => OpenWal(options)).Message, StringComparison.Ordinal);
    }

    public void Dispose() => _directory.Dispose();

    // The store and the WAL opened as the extension loader's providers open them, with the node's settings.
    private SolariumPersistenceBackend OpenStore(Dictionary<string, string> options) =>
        (SolariumPersistenceBackend)new SolariumPersistenceBackendProvider().Open(new PersistenceBackendContext
        {
            StoragePath = Path.Combine(_directory.Path, "store"),
            StorageRevision = "v1",
            Configuration = new KahunaConfiguration { ExtensionOptions = options },
            Logger = NullLogger.Instance,
            ExtensionOptions = options,
        });

    private IWAL OpenWal(Dictionary<string, string> options) =>
        new SolariumWalProvider().Open(new WalContext
        {
            Path = Path.Combine(_directory.Path, "wal"),
            Revision = "v1",
            SyncWrites = false,
            Logger = NullLogger<IRaft>.Instance,
            ExtensionOptions = options,
        });

    private static PersistenceRequestItem Item(string key) =>
        new(key,
            Encoding.UTF8.GetBytes("value of " + key),
            revision: 1,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 1,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);
}
