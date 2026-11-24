# Testing

```sh
dotnet test
```

The storage, SSTable and database tests take the storage environment as a parameter. On Linux with io_uring, they execute once for each environment. On other platforms, they cover the default environment only. You can test io_uring from macOS or Windows through Docker.

```sh
docker build -t solarium-test .
docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1 solarium-test
```

Docker needs both flags. The default seccomp profile blocks io_uring, and each ring counts against the locked-memory limit.

CI (`.github/workflows/ci.yml`) uses Linux. It fails when a test ran on the default environment but did not run on io_uring. The `Windows` workflow (`.github/workflows/windows.yml`) is started by hand. Windows matters because it enforces file share modes and has no directory fsync.

## Crash tests

| Test | What it simulates | Iterations per environment | Settings |
| --- | --- | ---: | --- |
| `CrashTests` | A power loss at random operations of a random workload | 30 | `SOLARIUM_CRASH_ITERATIONS`, `SOLARIUM_CRASH_SEED` |
| `KillTests` | SIGKILL of a real process at a random moment | 4 | `SOLARIUM_KILL_ITERATIONS`, `SOLARIUM_KILL_SEED` |
| `WalCrashTests` | Power losses and process crashes of a `SolariumWAL` (see [kommander.md](kommander.md)) | 24 | `SOLARIUM_WAL_CRASH_ITERATIONS`, `SOLARIUM_WAL_CRASH_SEED` |

### Power loss

`PowerLossEnvironment` records what each file and the directory contained at their last flush. A crash leaves that content, and can also leave part of the later writes. That part is either in order or sector by sector in any order. With its `None` loss, which models a process crash, all writes and directory changes remain.

The workload of `CrashTests` writes synced and unsynced batches across column families. It also covers flushes, compactions and Kommander's floor pass. Each run crashes and reopens three times, which means that recovery is cut too. After each reopen, the database must contain exactly the first batches up to one point, at or after the last synced write that returned. A failure prints its seed, and the directories of the run are preserved.

### Process kill

`tests/Solarium.CrashWorker` writes the same kind of workload. It prints each batch before the write, and the sequence number after the write returns. The test kills the worker and compares the reopened database with that output. All acknowledged batches must be there, synced or unsynced. The batch in flight must be complete or absent. Each run kills three workers in a row in one directory.

## Race and failure tests

- `CheckpointTests` pauses a checkpoint at its first write while the source takes writes, flushes and compactions. The copy must match the source as it was when the checkpoint began.
- `CheckpointTests` also checks the write order that makes an incomplete copy fail to open: the tables and `MANIFEST`, each made durable, before `FORMAT`. No test cuts the power during a checkpoint, since `PowerLossEnvironment` models one directory.
- Other checkpoint tests take the target directory during a checkpoint, or call `Dispose` at each of its stages.
- `SolariumDbTests` deletes a `MANIFEST`. `Open` must fail and must not change a byte, and it must succeed when the manifest is back.
- `MemTableTests` compares the index of range tombstones with a scan of all the tombstones, while readers race the writer.
- In Kommander's tests, a commit marker is paused while a higher commit or a partition delete races it. The persisted commit frontier must not move back.

For the tests of the Kommander and Kahuna packages, see [kommander.md](kommander.md) and [kahuna.md](kahuna.md).
