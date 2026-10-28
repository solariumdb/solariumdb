using BenchmarkDotNet.Running;
using Solarium.Benchmarks;

if (args is ["sibling-load", .. string[] load])
{
    SiblingLoad.Run(load);
    return;
}

if (args is ["synced", .. string[] synced])
{
    SyncedLoad.Run(synced);
    return;
}

if (args is ["compaction", .. string[] scenario])
{
    CompactionLoad.Run(scenario);
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
