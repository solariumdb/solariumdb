using Kommander;
using Kommander.Tests.WAL;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Solarium.Kommander.Tests;

/// <summary>Runs Kommander's shared <see cref="IWAL"/> conformance suite against <see cref="SolariumWAL"/>.</summary>
public sealed class SolariumWalConformanceTests : WalConformanceTests
{
    // One pass advances the compaction floor over every row below the checkpoint, as RocksDbWAL does.
    protected override bool CapBoundsRemoval => false;

    protected override IWAL CreateWal(out Action cleanup)
    {
        string path = Path.Combine(Path.GetTempPath(), "solarium-kommander-" + Guid.NewGuid().ToString("N"));
        cleanup = () =>
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        };

        return new SolariumWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
    }
}
