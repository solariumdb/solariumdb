# SolariumDb.IoUring

A storage environment for [Solarium](https://www.nuget.org/packages/SolariumDb) on Linux 6.1 or later.
`IoUringStorageEnvironment` writes the database files through io_uring, on the
[ioxide](https://github.com/MDA2AV/ioxide) reactor. Reads and fsync stay on the calling thread.

## Use it

```sh
dotnet add package SolariumDb.IoUring
```

```csharp
using Solarium;
using Solarium.IoUring;

using IoUringStorageEnvironment environment = new();
using SolariumDb db = SolariumDb.Open("./data", new DbOptions { Environment = environment });
```

Create one environment per process. The host owns it and disposes it after every database that uses it.

## License

MIT.
