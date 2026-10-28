using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>The contract every storage environment must meet.</summary>
public sealed class StorageEnvironmentTests
{
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Writes_at_offsets_read_back_and_survive_reopen(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        string path = directory.Combine("file.dat");

        using (IStorageFile file = scope.Environment.OpenFile(path))
        {
            Assert.Equal(0, file.GetLength());

            file.Write(B("hello"), 0);
            file.Write(B("world"), 5);
            file.Flush();
            Assert.Equal(10, file.GetLength());

            byte[] buffer = new byte[16];
            Assert.Equal("loworld", S(buffer.AsSpan(0, file.Read(buffer, 3))));
            Assert.Equal(0, file.Read(buffer, 10));
        }

        using (IStorageFile file = scope.Environment.OpenFile(path))
        {
            Assert.Equal(10, file.GetLength());

            file.SetLength(4);
            file.Flush();

            byte[] buffer = new byte[16];
            Assert.Equal("hell", S(buffer.AsSpan(0, file.Read(buffer, 0))));
        }
    }

    // The writeback of a range starts and returns; the bytes read back, and a range past the end is allowed.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_range_sync_starts_the_writeback_and_changes_nothing_the_file_reads(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        byte[] bytes = new byte[3 << 20];
        new Random(5).NextBytes(bytes);

        using IStorageFile file = scope.Environment.OpenFile(directory.Combine("file.dat"));
        file.Write(bytes, 0);
        file.SyncRange(0, 1 << 20);
        file.SyncRange(1 << 20, 2 << 20);
        file.SyncRange(2 << 20, 4 << 20);
        file.Flush();

        byte[] read = new byte[bytes.Length];
        int total = 0;
        while (total < read.Length)
            total += file.Read(read.AsSpan(total), total);

        Assert.Equal(bytes, read);
    }

    // A hard link is a second name of one file: it reads the same bytes, a write through one name
    // shows through the other, and the file stays while either name does.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_link_names_the_same_file_and_keeps_it_when_the_source_is_deleted(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        string source = directory.Combine("source.sst");
        string target = directory.Combine("target.sst");
        using (IStorageFile file = scope.Environment.OpenFile(source))
        {
            file.Write(B("hello"), 0);
            file.Flush();
        }

        Assert.True(scope.Environment.TryLinkFile(source, target));
        scope.Environment.FlushDirectory(directory.Path);
        using (IStorageFile file = scope.Environment.OpenFile(source))
            file.Write(B("J"), 0);

        scope.Environment.DeleteFile(source);
        Assert.False(scope.Environment.FileExists(source));
        using IStorageFile linked = scope.Environment.OpenFile(target);
        byte[] buffer = new byte[16];
        Assert.Equal("Jello", S(buffer.AsSpan(0, linked.Read(buffer, 0))));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_link_onto_a_name_that_exists_or_from_one_that_does_not_throws(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        string source = directory.Combine("source.sst");
        string target = directory.Combine("target.sst");
        File.WriteAllText(source, "source");
        File.WriteAllText(target, "target");

        Assert.Throws<IOException>(() => scope.Environment.TryLinkFile(source, target));
        Assert.Equal("target", File.ReadAllText(target));
        Assert.Throws<IOException>(() => scope.Environment.TryLinkFile(directory.Combine("missing.sst"), directory.Combine("other.sst")));
        Assert.False(File.Exists(directory.Combine("other.sst")));
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Concurrent_writes_to_distinct_regions_all_land(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        using IStorageFile file = scope.Environment.OpenFile(directory.Combine("file.dat"));

        Parallel.For(0, 64, i => file.Write(Enumerable.Repeat((byte)i, 4096).ToArray(), i * 4096L));

        byte[] buffer = new byte[4096];
        for (int i = 0; i < 64; i++)
        {
            Assert.Equal(4096, file.Read(buffer, i * 4096L));
            Assert.All(buffer, b => Assert.Equal((byte)i, b));
        }
    }

    // Requests on several files, one of them short: each fills its buffer, or stops where its file ends.
    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void A_multi_read_fills_each_request_from_its_file_and_stops_at_the_end_of_a_file(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        using IStorageFile one = scope.Environment.OpenFile(directory.Combine("one.dat"));
        using IStorageFile two = scope.Environment.OpenFile(directory.Combine("two.dat"));
        byte[] data = new byte[64 * 1024];
        new Random(3).NextBytes(data);
        one.Write(data, 0);
        two.Write(B("short"), 0);

        byte[][] buffers = [new byte[4096], new byte[100], new byte[16], new byte[16], new byte[0], new byte[8192]];
        ReadRequest[] requests =
        [
            new(one, buffers[0], 8192),
            new(two, buffers[1], 0),
            new(two, buffers[2], 3),
            new(two, buffers[3], 100),
            new(one, buffers[4], 0),
            new(one, buffers[5], data.Length - 4096),
        ];

        scope.Environment.MultiRead(requests);

        Assert.Equal([4096, 5, 2, 0, 0, 4096], requests.Select(r => r.BytesRead));
        Assert.Equal(data.AsSpan(8192, 4096).ToArray(), buffers[0]);
        Assert.Equal("short", S(buffers[1].AsSpan(0, 5)));
        Assert.Equal("rt", S(buffers[2].AsSpan(0, 2)));
        Assert.Equal(data.AsSpan(data.Length - 4096).ToArray(), buffers[5].AsSpan(0, 4096).ToArray());
    }

    [Theory]
    [MemberData(nameof(Environments.Available), MemberType = typeof(Environments))]
    public void Concurrent_multi_reads_each_read_their_own_bytes(string environment)
    {
        using EnvironmentScope scope = Environments.Create(environment);
        using TempDirectory directory = new();
        using IStorageFile file = scope.Environment.OpenFile(directory.Combine("file.dat"));
        for (int i = 0; i < 256; i++)
            file.Write(Enumerable.Repeat((byte)i, 4096).ToArray(), i * 4096L);

        Parallel.For(0, 64, round =>
        {
            Random random = new(round);
            int[] pages = [.. Enumerable.Range(0, 32).Select(_ => random.Next(256))];
            byte[][] buffers = [.. pages.Select(_ => new byte[4096])];
            ReadRequest[] requests = [.. pages.Select((page, i) => new ReadRequest(file, buffers[i], page * 4096L))];

            scope.Environment.MultiRead(requests);

            for (int i = 0; i < pages.Length; i++)
            {
                Assert.Equal(4096, requests[i].BytesRead);
                Assert.All(buffers[i], b => Assert.Equal((byte)pages[i], b));
            }
        });
    }
}
