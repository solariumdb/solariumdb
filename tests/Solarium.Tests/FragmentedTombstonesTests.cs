using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>The fragmented range tombstones answer what a scan of every tombstone answers.</summary>
public sealed class FragmentedTombstonesTests
{
    [Fact]
    public void The_fragments_find_the_tombstone_that_a_scan_finds_for_every_key_and_snapshot()
    {
        Random random = new(20261022);
        for (int round = 0; round < 2000; round++)
        {
            // Ranges over a small key space, so they overlap, nest, repeat, touch, and are empty.
            List<RangeTombstone> tombstones = [];
            int count = random.Next(60);
            for (int i = 0; i < count; i++)
            {
                int begin = random.Next(30);
                int end = random.Next(4) == 0 ? begin : begin + random.Next(1, random.Next(3) == 0 ? 30 : 4);
                tombstones.Add(new RangeTombstone(Key(begin), Key(end), random.Next(1, 100)));
            }

            FragmentedTombstones fragments = FragmentedTombstones.Build(tombstones);
            for (int k = -1; k <= 61; k++)
            {
                byte[] key = k < 0 ? [] : Key(k);
                foreach (long snapshot in new long[] { 0, 1, random.Next(1, 100), 50, 99, long.MaxValue })
                {
                    long expected = 0;
                    foreach (RangeTombstone tombstone in tombstones)
                        if (tombstone.Covers(key, snapshot))
                            expected = Math.Max(expected, tombstone.Sequence);

                    Assert.Equal(expected, fragments.Covering(key, snapshot));
                }
            }
        }
    }

    [Fact]
    public void Tombstones_that_do_not_overlap_make_one_fragment_each()
    {
        List<RangeTombstone> tombstones = [];
        for (int i = 0; i < 100; i++)
            tombstones.Add(new RangeTombstone(Key(i * 10), Key((i * 10) + 5), i + 1));

        FragmentedTombstones fragments = FragmentedTombstones.Build(tombstones);

        Assert.Equal(100, fragments.FragmentCount);
        Assert.Equal(42, fragments.Covering(Key(413), long.MaxValue));
        Assert.Equal(0, fragments.Covering(Key(415), long.MaxValue));
        Assert.Equal(0, fragments.Covering(Key(413), 41));
    }

    [Fact]
    public void No_tombstones_cover_nothing()
    {
        Assert.Equal(0, FragmentedTombstones.Build([]).Covering(Key(1), long.MaxValue));
        Assert.Equal(0, FragmentedTombstones.Build([new RangeTombstone(Key(3), Key(3), 7)]).Covering(Key(3), long.MaxValue));
    }

    // Keys whose byte order is their number's order.
    private static byte[] Key(int i) => B($"k{i:D4}");
}
