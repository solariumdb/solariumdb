namespace Solarium.Engine;

/// <summary>
/// How a <see cref="TableBuilder"/> paces its writes: the <see cref="RateLimiter"/> it asks before each
/// write, at the priority of flushes (<paramref name="High"/>) or compactions, and every how many bytes it
/// starts the writeback of what it wrote (<see cref="Storage.IStorageFile.SyncRange"/>; 0: never).
/// The default paces nothing.
/// </summary>
internal readonly record struct TablePacing(RateLimiter? Limiter, bool High, long BytesPerSync);
