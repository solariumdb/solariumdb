namespace Solarium.Kahuna.Benchmarks;

/// <summary>
/// What one case of one session measured, over its measured time only: operations per second (rows for
/// writes and scans, keys for batched reads), the latency of one backend call in microseconds (percentiles
/// and the longest), the seconds it took (for the cases that run once: load, reopen, checkpoint, prune),
/// the process CPU time per operation, the bytes the calling threads allocated per operation, the bytes the
/// process read from and wrote to the storage device per operation (compactions and flushes included), the
/// time the GC paused the process and its gen 2 collections, and notes such as the writer's rate in a mixed
/// case.
/// </summary>
public sealed record BackendRun(
    Dataset Dataset,
    string Case,
    string Engine,
    int Repeat,
    double OperationsPerSecond,
    double P50Microseconds,
    double P99Microseconds,
    double P999Microseconds,
    double MaxMicroseconds,
    double Seconds,
    double CpuMicrosecondsPerOperation,
    double AllocatedBytesPerOperation,
    double StorageReadBytesPerOperation,
    double StorageWrittenBytesPerOperation,
    double GcPauseMilliseconds,
    int Gen2Collections,
    string Notes);
