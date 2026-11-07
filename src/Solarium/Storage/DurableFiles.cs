using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Solarium.Storage;

/// <summary>
/// Durable file operations the BCL does not offer: a directory flush, a data flush (<c>fdatasync</c>), a hard
/// link and the writeback of a range, and an atomic replace and a whole read of a small file through an
/// environment.
/// </summary>
internal static partial class DurableFiles
{
    /// <summary>
    /// Make the directory's entries durable: a file created, renamed or deleted in it survives a
    /// crash only after this. A no-op on Windows, where NTFS journals metadata.
    /// </summary>
    public static void FlushDirectory(string directory)
    {
        if (OperatingSystem.IsWindows())
            return;

        // O_RDONLY is 0 on Linux and macOS; opening a directory read-only is allowed on both.
        int fd = open(directory, 0);
        if (fd < 0)
            throw Errno($"open directory '{directory}'");

        try
        {
            if (fsync(fd) != 0)
                throw Errno($"fsync directory '{directory}'");
        }
        finally
        {
            close(fd);
        }
    }

    /// <summary>
    /// Link <paramref name="target"/> to the file <paramref name="source"/>; see
    /// <see cref="IStorageEnvironment.TryLinkFile"/>. False when the file system cannot link them.
    /// </summary>
    public static bool TryLinkFile(string source, string target)
    {
        if (OperatingSystem.IsWindows())
        {
            if (CreateHardLinkW(target, source, IntPtr.Zero))
                return true;

            int error = Marshal.GetLastPInvokeError();
            if (error is ErrorInvalidFunction or ErrorNotSameDevice or ErrorNotSupported or ErrorTooManyLinks)
                return false;

            throw new IOException($"link '{target}' to '{source}' failed: {Marshal.GetPInvokeErrorMessage(error)} (error {error})");
        }

        if (link(source, target) == 0)
            return true;

        // EPERM is Linux's answer for a file system without hard links, and for a link that
        // protected_hardlinks refuses; either way a copy can do what the link cannot.
        int errno = Marshal.GetLastPInvokeError();
        bool unsupported = errno is EPERM or EXDEV or EMLINK
            || (OperatingSystem.IsLinux() && errno == LinuxEOPNOTSUPP)
            || (OperatingSystem.IsMacOS() && errno is MacENOTSUP or MacEOPNOTSUPP);
        if (unsupported)
            return false;

        throw Errno($"link '{target}' to '{source}'");
    }

    /// <summary>Replace <paramref name="path"/> with the UTF-8 bytes of <paramref name="contents"/>; see <see cref="ReplaceBytes"/>.</summary>
    public static void ReplaceText(IStorageEnvironment environment, string path, string contents)
    {
        ReplaceBytes(environment, path, System.Text.Encoding.UTF8.GetBytes(contents));
    }

    /// <summary>
    /// Replace <paramref name="path"/> with <paramref name="contents"/>, all or nothing, durably: write
    /// a temporary file, flush it, rename it over the target, flush the directory.
    /// </summary>
    public static void ReplaceBytes(IStorageEnvironment environment, string path, ReadOnlySpan<byte> contents)
    {
        string temporary = path + ".tmp";
        using (IStorageFile file = environment.OpenFile(temporary))
        {
            // A temporary file a crash left behind may be longer.
            file.SetLength(0);
            file.Write(contents, 0);
            file.Flush();
        }

        environment.RenameFile(temporary, path);
        environment.FlushDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
    }

    /// <summary>The whole of <paramref name="path"/>, which must exist.</summary>
    public static byte[] ReadAllBytes(IStorageEnvironment environment, string path)
    {
        using IStorageFile file = environment.OpenFile(path);
        byte[] bytes = new byte[file.GetLength()];
        int read = 0;
        while (read < bytes.Length)
        {
            int count = file.Read(bytes.AsSpan(read), read);
            if (count == 0)
                throw new IOException($"'{path}' ended at {read} of its {bytes.Length} bytes.");

            read += count;
        }

        return bytes;
    }

    /// <summary>
    /// <c>sync_file_range(SYNC_FILE_RANGE_WRITE)</c> on Linux: start the writeback of the range and do not
    /// wait for it. Nothing elsewhere, and nothing where the file system has no such call (ENOSYS).
    /// </summary>
    public static void SyncRange(SafeFileHandle handle, long offset, long length)
    {
        if (!OperatingSystem.IsLinux())
            return;

        if (sync_file_range((int)handle.DangerousGetHandle(), offset, length, SyncFileRangeWrite) != 0 && Marshal.GetLastPInvokeError() != LinuxENOSYS)
            throw Errno("sync_file_range");
    }

    /// <summary>
    /// Make the data and the length of the file durable: <c>fdatasync</c> on Linux, as RocksDB flushes by
    /// default (<c>use_fsync</c> off) and as the io_uring environment does, and
    /// <see cref="RandomAccess.FlushToDisk"/> elsewhere (<c>F_FULLFSYNC</c> on macOS, <c>FlushFileBuffers</c>
    /// on Windows). fdatasync commits a changed length but not a changed timestamp alone, so a write in place
    /// over allocated blocks, such as a slot of a small ledger, costs one device flush and no journal commit;
    /// nothing in the engine reads a timestamp back.
    /// </summary>
    /// <exception cref="IOException">The system call failed.</exception>
    /// <exception cref="ObjectDisposedException">The handle is closed.</exception>
    public static void Flush(SafeFileHandle handle)
    {
        if (!OperatingSystem.IsLinux())
        {
            RandomAccess.FlushToDisk(handle);
            return;
        }

        ObjectDisposedException.ThrowIf(handle.IsClosed, handle);
        if (fdatasync((int)handle.DangerousGetHandle()) != 0)
            throw Errno("fdatasync");
    }

    private static IOException Errno(string operation)
    {
        int errno = Marshal.GetLastPInvokeError();
        return new IOException($"{operation} failed: {Marshal.GetPInvokeErrorMessage(errno)} (errno {errno})");
    }

    // errno values that are the same on Linux and macOS, and the ones that differ.
    private const int EPERM = 1;
    private const int EXDEV = 18;
    private const int EMLINK = 31;
    private const int LinuxENOSYS = 38;
    private const int LinuxEOPNOTSUPP = 95;
    private const int MacENOTSUP = 45;
    private const int MacEOPNOTSUPP = 102;

    // sync_file_range's flag that starts the writeback of the range and does not wait.
    private const uint SyncFileRangeWrite = 2;

    // Win32 errors of CreateHardLinkW that mean the volume cannot link the two paths.
    private const int ErrorInvalidFunction = 1;
    private const int ErrorNotSameDevice = 17;
    private const int ErrorNotSupported = 50;
    private const int ErrorTooManyLinks = 1142;

    [LibraryImport("libc", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int open(string path, int flags);

    [LibraryImport("libc", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int link(string existing, string created);

    [LibraryImport("kernel32", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CreateHardLinkW(string created, string existing, IntPtr securityAttributes);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int fsync(int fd);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int fdatasync(int fd);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int sync_file_range(int fd, long offset, long length, uint flags);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int close(int fd);
}
