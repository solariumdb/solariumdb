using Solarium.Engine;

namespace Solarium.Tests;

/// <summary>
/// The LRU cache on its own: what it evicts and when, the high-priority pool, what pins keep, the soft capacity,
/// erases of pinned entries, and the default shard count. One shard, unless the test is about shards, so the
/// order is exact.
/// </summary>
public sealed class CacheTests
{
    [Fact]
    public void An_insert_over_the_capacity_evicts_the_least_recently_used_entries_first()
    {
        Cache cache = new(300, shardBits: 0);
        cache.Insert(Key(1), "one", 100);
        cache.Insert(Key(2), "two", 100);
        cache.Insert(Key(3), "three", 100);

        // A lookup makes 1 the most recently used, so 2 is the oldest when 4 needs room.
        Assert.True(cache.TryGet(Key(1), out object? one));
        Assert.Equal("one", one);
        cache.Insert(Key(4), "four", 100);

        Assert.False(cache.TryGet(Key(2), out _));
        Assert.True(cache.TryGet(Key(1), out _));
        Assert.True(cache.TryGet(Key(3), out _));
        Assert.True(cache.TryGet(Key(4), out _));
        Assert.Equal(300, cache.Usage);
        Assert.Equal(0, cache.PinnedUsage);
    }

    [Fact]
    public void A_second_insert_of_a_key_keeps_the_first_value_and_its_charge()
    {
        Cache cache = new(1000, shardBits: 0);
        Assert.Equal("first", cache.Insert(Key(1), "first", 100));
        Assert.Equal("first", cache.Insert(Key(1), "second", 400));

        Assert.Equal(100, cache.Usage);
        Assert.Equal(1, cache.InsertCount);
    }

    [Fact]
    public void Pinned_entries_are_never_evicted_and_the_cache_stays_over_its_capacity_rather_than_drop_them()
    {
        Cache cache = new(200, shardBits: 0);
        CacheEntry pinned = cache.InsertPinned(Key(1), "pinned", 150);
        cache.Insert(Key(2), "loose", 100);

        // 250 bytes in a cache of 200: only the unpinned entry can go, and it does.
        Assert.False(cache.TryGet(Key(2), out _));
        Assert.True(cache.TryGet(Key(1), out _));
        Assert.Equal(150, cache.Usage);
        Assert.Equal(150, cache.PinnedUsage);

        // Pins alone may exceed the capacity: the cache is soft.
        cache.InsertPinned(Key(3), "also pinned", 150);
        Assert.Equal(300, cache.Usage);
        Assert.Equal(300, cache.PinnedUsage);

        // Unpinned, the entry is the most recently used, and evicting down to the capacity takes it
        // only if nothing older is left: here it is the only unpinned entry, so it goes.
        cache.Unpin(pinned);
        Assert.False(cache.TryGet(Key(1), out _));
        Assert.Equal(150, cache.Usage);
        Assert.Equal(150, cache.PinnedUsage);
    }

    [Fact]
    public void An_entry_larger_than_the_room_left_is_evicted_at_once_and_the_caller_keeps_its_value()
    {
        Cache cache = new(100, shardBits: 0);

        Assert.Equal("big", cache.Insert(Key(1), "big", 500));
        Assert.False(cache.TryGet(Key(1), out _));
        Assert.Equal(0, cache.Usage);
    }

    [Fact]
    public void Erasing_a_pinned_entry_removes_its_charge_and_a_later_unpin_changes_nothing()
    {
        Cache cache = new(1000, shardBits: 0);
        CacheEntry pinned = cache.InsertPinned(Key(1), "pinned", 100);
        cache.Insert(Key(2), "loose", 100);

        cache.Erase(Key(1));
        Assert.False(cache.TryGet(Key(1), out _));
        Assert.Equal(100, cache.Usage);
        Assert.Equal(0, cache.PinnedUsage);

        cache.Unpin(pinned);
        Assert.Equal(100, cache.Usage);
        Assert.Equal(0, cache.PinnedUsage);

        cache.Erase(Key(2));
        cache.Erase(Key(3));
        Assert.Equal(0, cache.Usage);
    }

    [Fact]
    public void A_pinned_entry_pinned_twice_stays_until_both_pins_go()
    {
        Cache cache = new(0, shardBits: 0);
        CacheEntry first = cache.InsertPinned(Key(1), "value", 100);
        CacheEntry second = cache.InsertPinned(Key(1), "other", 100);
        Assert.Same(first, second);
        Assert.Equal(100, cache.PinnedUsage);

        cache.Unpin(first);
        Assert.True(cache.TryGet(Key(1), out _));
        cache.Unpin(second);
        Assert.False(cache.TryGet(Key(1), out _));
        Assert.Equal(0, cache.Usage);
    }

    [Fact]
    public void A_smaller_capacity_evicts_at_once()
    {
        Cache cache = new(1000, shardBits: 0);
        for (int i = 0; i < 10; i++)
            cache.Insert(Key(i), i, 100);

        cache.SetCapacity(300);
        Assert.Equal(300, cache.Capacity);
        Assert.Equal(300, cache.Usage);
        Assert.True(cache.TryGet(Key(9), out _));
        Assert.False(cache.TryGet(Key(6), out _));
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1024 * 1024, 2)]
    [InlineData(8L * 1024 * 1024, 16)]
    [InlineData(1024L * 1024 * 1024, 64)]
    public void The_default_shard_count_keeps_each_shard_at_512_KiB_or_more_up_to_64(long capacity, int shards)
    {
        Assert.Equal(shards, new Cache(capacity).ShardCount);
    }

    [Fact]
    public void A_sharded_cache_finds_every_entry_and_its_totals_add_up_its_shards()
    {
        Cache cache = new(64L * 1024 * 1024, shardBits: 4);
        long owner = Cache.NewOwner();
        for (int i = 0; i < 1600; i++)
            cache.Insert(new CacheKey(owner, i), i, 1000);

        Assert.Equal(1_600_000, cache.Usage);
        for (int i = 0; i < 1600; i++)
            Assert.True(cache.TryGet(new CacheKey(owner, i), out _));

        // Another owner's table of the same number is another entry.
        Assert.False(cache.TryGet(new CacheKey(Cache.NewOwner(), 7), out _));
    }

    [Fact]
    public void Arguments_out_of_range_are_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(1, shardBits: -2));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(1, shardBits: 21));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(1).SetCapacity(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(1, highPriorityPoolRatio: -0.1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(1, highPriorityPoolRatio: 1.1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Cache(1, highPriorityPoolRatio: double.NaN));
        Assert.Equal(0.5, new Cache(1).HighPriorityPoolRatio);
    }

    [Fact]
    public void Entries_read_once_pass_through_without_evicting_high_priority_entries_or_entries_read_again()
    {
        Cache cache = new(1000, shardBits: 0);
        cache.Insert(Key(1), "index", 200, highPriority: true);
        cache.Insert(Key(2), "filter", 200, highPriority: true);
        cache.Insert(Key(3), "hot block", 100);
        Assert.True(cache.TryGet(Key(3), out _));
        Assert.Equal(500, cache.HighPriorityPoolUsage);

        // A scan of 50 blocks, each read once, five times the cache: they evict one another, and nothing else.
        for (int i = 100; i < 150; i++)
            cache.Insert(Key(i), "scanned block", 100);

        Assert.True(cache.TryGet(Key(1), out _));
        Assert.True(cache.TryGet(Key(2), out _));
        Assert.True(cache.TryGet(Key(3), out _));
        Assert.True(cache.TryGet(Key(149), out _));
        Assert.False(cache.TryGet(Key(100), out _));
        Assert.Equal(1000, cache.Usage);
    }

    [Fact]
    public void A_high_priority_pool_over_its_share_moves_its_oldest_entries_to_the_low_pool_and_eviction_keeps_their_order()
    {
        Cache cache = new(400, shardBits: 0);
        for (int i = 1; i <= 4; i++)
            cache.Insert(Key(i), i, 100, highPriority: true);

        // Half of 400 for the pool: 1 and 2 moved out of it, oldest first, and are the next to go.
        Assert.Equal(200, cache.HighPriorityPoolUsage);
        cache.Insert(Key(5), 5, 100);
        Assert.False(cache.TryGet(Key(1), out _));

        // A read brings 5 into the pool, which moves 3 out, newer than 2; then 2 and 3 go, in that order.
        Assert.True(cache.TryGet(Key(5), out _));
        cache.Insert(Key(6), 6, 100);
        Assert.False(cache.TryGet(Key(2), out _));
        Assert.Equal(200, cache.HighPriorityPoolUsage);
        cache.Insert(Key(7), 7, 100);
        Assert.False(cache.TryGet(Key(3), out _));
        Assert.True(cache.TryGet(Key(4), out _));
        Assert.True(cache.TryGet(Key(5), out _));
    }

    [Fact]
    public void With_no_high_priority_pool_every_entry_is_evicted_in_plain_lru_order()
    {
        Cache cache = new(300, shardBits: 0, highPriorityPoolRatio: 0);
        cache.Insert(Key(1), "index", 100, highPriority: true);
        cache.Insert(Key(2), "block", 100);
        cache.Insert(Key(3), "block", 100);
        Assert.True(cache.TryGet(Key(2), out _));

        cache.Insert(Key(4), "block", 100);
        Assert.False(cache.TryGet(Key(1), out _));
        cache.Insert(Key(5), "block", 100);
        Assert.False(cache.TryGet(Key(3), out _));
        Assert.Equal(0, cache.HighPriorityPoolUsage);
    }

    [Fact]
    public void A_pinned_entry_is_outside_both_pools_and_joins_its_own_when_unpinned()
    {
        Cache cache = new(1000, shardBits: 0);
        CacheEntry pinned = cache.InsertPinned(Key(1), "level 0 index", 300, highPriority: true);
        Assert.Equal(0, cache.HighPriorityPoolUsage);
        Assert.Equal(300, cache.PinnedUsage);

        cache.Unpin(pinned);
        Assert.Equal(300, cache.HighPriorityPoolUsage);
        Assert.Equal(0, cache.PinnedUsage);

        // Pinned again, it leaves the pool; erased while pinned, it leaves the cache.
        cache.InsertPinned(Key(1), "level 0 index", 300, highPriority: true);
        Assert.Equal(0, cache.HighPriorityPoolUsage);
        cache.Erase(Key(1));
        Assert.Equal(0, cache.Usage);
        Assert.Equal(0, cache.PinnedUsage);
    }

    [Fact]
    public void A_smaller_capacity_shrinks_the_high_priority_pool_too()
    {
        Cache cache = new(1000, shardBits: 0);
        for (int i = 1; i <= 5; i++)
            cache.Insert(Key(i), i, 100, highPriority: true);

        Assert.Equal(500, cache.HighPriorityPoolUsage);
        cache.SetCapacity(400);
        Assert.Equal(200, cache.HighPriorityPoolUsage);
        Assert.Equal(400, cache.Usage);
        Assert.False(cache.TryGet(Key(1), out _));
        Assert.True(cache.TryGet(Key(2), out _));
    }

    private static CacheKey Key(long number) => new(1, number);
}
