# Testing

```sh
dotnet test
```

The storage, SSTable, and database tests take the environment as a parameter. On Linux with io_uring, they run once for each environment. To test the io_uring environment from macOS or Windows, use Docker:

```sh
docker build -t solarium-test .
docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1 solarium-test
```

Docker needs both flags. The default seccomp profile blocks io_uring, and each ring counts against the locked-memory limit.

Without io_uring, the suite runs the default environment only and still passes. CI (`.github/workflows/ci.yml`) runs on Linux and fails when a test ran on the default environment but not on io_uring. The `Windows` workflow (`.github/workflows/windows.yml`) runs the suite on Windows, on the default environment, when started by hand: Windows enforces file share modes and has no directory fsync, and runner minutes there count against the plan.

`CrashTests` cuts the power at random operations of a random workload: batches across column families, synced and not, with flushes, WAL rotation, compactions, and Kommander's floor pass with `DeleteFilesInRange`. `PowerLossEnvironment` keeps what each file and the directory held at their last flush, and a crash writes out what a power loss can leave: only that, or also part of the rest, written in order or sector by sector in any order. Each run crashes and reopens three times, so recovery itself is cut too. After each reopen, the database must hold exactly the first batches up to one point, at or after the last synced write that returned, and nothing of a later batch. The suite runs 30 runs on each environment; `SOLARIUM_CRASH_ITERATIONS` sets more, and `SOLARIUM_CRASH_SEED` the first seed. A failure names its seed and keeps the directories of the run.

`CheckpointTests` holds a checkpoint at its first write, with `HookedEnvironment`, and writes, flushes and compacts the source meanwhile; the copy must read as the source did when the checkpoint began. `PowerLossEnvironment` models one directory, so no test cuts the power during a checkpoint. Instead, a recording environment checks the order that makes a cut-short copy refuse to open: the tables and `MANIFEST`, each made durable, before `FORMAT`. A directory gate holds a checkpoint between its check that the directory does not exist and its creation, while another checkpoint, an open checkpoint or a new database takes the directory; the held checkpoint must fail and leave every file there as it was. `Dispose` runs while a checkpoint is held at a memtable table, a table copy, `MANIFEST` or `FORMAT`; it must stop the checkpoint, or wait for one that writes `FORMAT`.

`SolariumDbTests` deletes the `MANIFEST` of a checkpoint, of a flushed database and of one with WAL files only, and checks that `Open` refuses each and changes no byte of its files, and that each opens again with its manifest back. `MemTableTests` checks the memtable's runs of range tombstones against a scan of all of them, after every range delete and at every snapshot, while readers race the writer that builds them. In Kommander's tests, a read gate holds a commit marker at its first SSTable probe while a higher commit of the same partition, or a partition delete, races it; the persisted commit frontier must not move back.

`KillTests` kills a real process with SIGKILL instead. `tests/Solarium.CrashWorker` writes the same kind of workload to a directory and reports each batch on standard output before it writes it, and the sequence number after the write returns; the test kills it at a random moment, sometimes while it still opens the database, reopens the directory, and checks it against the reports. A killed process loses nothing that the OS holds, so every acknowledged batch must be there, synced or not, and the batch in flight whole or not at all. Each run kills three workers in a row in one directory. The suite runs 4 runs on each environment; `SOLARIUM_KILL_ITERATIONS` and `SOLARIUM_KILL_SEED` change that.

`tests/Solarium.Kommander.Tests` holds Kommander's conformance suite, the ported `RocksDbWAL` behavior tests, `WalCrashTests`, the crash test of the WAL, `ClusterTests`, three-node Kommander clusters on `SolariumWAL`, and `MetricsTests`, the package's instruments read through a `MeterListener`; [kommander.md](kommander.md) describes them. `PowerLossEnvironment`'s `None` loss is a process crash: it keeps every write and directory change, flushed or not.
