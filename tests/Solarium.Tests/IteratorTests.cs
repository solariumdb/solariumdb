using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>Iterator behavior: ordering, deletes, snapshots, and the upper bound.</summary>
public sealed class IteratorTests : IDisposable
{
    private readonly TempDirectory _directory = new();
    private readonly SolariumDb _db;

    /// <summary>Opens a fresh database in a temporary directory.</summary>
    public IteratorTests()
    {
        _db = SolariumDb.Open(_directory.Combine("db"));
    }

    [Fact]
    public void Forward_and_backward_skip_deleted_keys()
    {
        Put("a", "b", "c", "d", "e", "f");
        _db.Delete(B("c"));
        _db.DeleteRange(B("d"), B("f"));

        Assert.Equal(["a", "b", "f"], Forward(_db.NewIterator()));
        Assert.Equal(["f", "b", "a"], Backward(_db.NewIterator()));

        using Iterator iterator = _db.NewIterator();
        iterator.Seek(B("bb"));
        Assert.Equal("f", S(iterator.Key()));
        iterator.Prev();
        Assert.Equal("b", S(iterator.Key()));
    }

    [Fact]
    public void Only_the_newest_version_of_a_key_is_visible()
    {
        _db.Put(B("k"), B("1"));
        _db.Put(B("k"), B("2"));
        _db.Put(B("k"), B("3"));

        using Iterator iterator = _db.NewIterator();
        iterator.SeekToFirst();
        Assert.Equal("3", S(iterator.Value()));
        iterator.Next();
        Assert.False(iterator.Valid());
    }

    [Fact]
    public void An_iterator_reads_the_snapshot_of_its_creation()
    {
        Put("a", "b");
        using Iterator iterator = _db.NewIterator();

        _db.Put(B("aa"), B("new"));
        _db.Delete(B("a"));
        _db.Put(B("b"), B("changed"));

        iterator.SeekToFirst();
        iterator.Next();
        Assert.Equal("b", S(iterator.Value()));

        Assert.Equal(["a", "b"], Forward(iterator));
    }

    [Fact]
    public void The_upper_bound_confines_a_partition_scan()
    {
        // The Kommander layout: "<partition>:<id>", with ';' (after ':') closing the partition.
        Put("p1:001", "p1:002", "p1:003", "p1:004", "p2:001");
        ReadOptions bounded = new ReadOptions { IterateUpperBound = B("p1;") };

        Assert.Equal(["p1:001", "p1:002", "p1:003", "p1:004"], Forward(_db.NewIterator(options: bounded)));

        using Iterator iterator = _db.NewIterator(options: bounded);
        iterator.SeekToLast();
        Assert.Equal("p1:004", S(iterator.Key()));
        iterator.Prev();
        Assert.Equal("p1:003", S(iterator.Key()));

        _db.DeleteRange(B("p1:000"), B("p1:003"));
        Assert.Equal(["p1:003", "p1:004"], Forward(_db.NewIterator(options: bounded)));
    }

    [Fact]
    public void An_empty_column_family_has_no_positions()
    {
        using Iterator iterator = _db.NewIterator();
        iterator.SeekToFirst();
        Assert.False(iterator.Valid());
        iterator.SeekToLast();
        Assert.False(iterator.Valid());
        Assert.Throws<InvalidOperationException>(() => iterator.Next());
    }

    private void Put(params string[] keys)
    {
        foreach (string key in keys)
            _db.Put(B(key), B(key));
    }

    private static List<string> Forward(Iterator iterator)
    {
        using (iterator)
        {
            List<string> keys = new();
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
                keys.Add(S(iterator.Key()));

            return keys;
        }
    }

    private static List<string> Backward(Iterator iterator)
    {
        using (iterator)
        {
            List<string> keys = new();
            for (iterator.SeekToLast(); iterator.Valid(); iterator.Prev())
                keys.Add(S(iterator.Key()));

            return keys;
        }
    }

    /// <summary>Closes the database and deletes its directory.</summary>
    public void Dispose()
    {
        _db.Dispose();
        _directory.Dispose();
    }
}
