namespace Solarium.Kommander.Benchmarks;

/// <summary>
/// What one run of a case on one engine measured, over its measured seconds only: the entries
/// written per second, the latency of a <c>Write</c> call in microseconds (percentiles and the longest), the process CPU time and
/// the bytes allocated per entry, the reads per second of the mixed workload's readers, the time the
/// GC paused the process and its gen 2 collections, and for Solarium what its engine counted: the
/// memtable switches that waited for a flush, the time they waited, and the flushes.
/// </summary>
public sealed record WalRun(
    WalCase Case,
    string Engine,
    int Repeat,
    double EntriesPerSecond,
    double P50Microseconds,
    double P99Microseconds,
    double P999Microseconds,
    double MaxMicroseconds,
    double CpuMicrosecondsPerEntry,
    double AllocatedBytesPerEntry,
    double ReadsPerSecond,
    double GcPauseMilliseconds,
    int Gen2Collections,
    string EngineNotes);
