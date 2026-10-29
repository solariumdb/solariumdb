using Solarium.Engine;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>The memtable's skip list under concurrent inserts, with readers walking it at the same time.</summary>
public sealed class MemTableTests
{
    [Fact]
    public void A_range_with_no_key_in_it_adds_no_tombstone_but_keeps_its_sequence_number()
    {
        MemTable table = new();
        table.AddRangeTombstone(B("b"), B("a"), 1);
        table.AddRangeTombstone(B("a"), B("a"), 2);
        table.AddRangeTombstoneConcurrently(B("z"), B("y"), 3);

        Assert.Empty(table.Tombstones);
        Assert.Equal(2, table.MaxSequence);
    }

    [Fact]
    public async Task Concurrent_inserts_keep_every_entry_once_in_order_while_readers_walk()
    {
        const int threads = 8;
        const int keys = 500;
        const int versions = 4;
        MemTable table = new();
        using CancellationTokenSource stop = new();
        using Barrier start = new(threads);
        int disorders = 0;
        long walks = 0;

        // Readers walk the whole list and check that each entry sorts after the one before it.
        Task[] readers = [.. Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                Entry? previous = null;
                foreach (Entry entry in table.Entries())
                {
                    if (previous is { } before && InternalKey.Compare(before.Key, before.Sequence, entry.Key, entry.Sequence) >= 0)
                        Interlocked.Increment(ref disorders);

                    previous = entry;
                }

                Interlocked.Increment(ref walks);
            }
        }))];

        // Every thread inserts every key, each at its own sequence numbers, so the threads insert
        // next to each other in the list and race for the same links.
        Task[] writers = [.. Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
        {
            start.SignalAndWait();
            for (int version = 0; version < versions; version++)
            {
                for (int k = 0; k < keys; k++)
                {
                    long sequence = 1 + (((long)version * keys + k) * threads) + t;
                    byte[] key = B($"key{k:D4}");
                    byte[] value = B($"{sequence}");
                    table.AddConcurrently(key, sequence, OperationKind.Put, value, out long bytes);
                    table.AddCounts(bytes, 1, sequence, sequence);
                }
            }
        }, TaskCreationOptions.LongRunning))];

        await Task.WhenAll(writers);
        Assert.True(SpinWait.SpinUntil(() => Interlocked.Read(ref walks) > 0, TimeSpan.FromSeconds(10)));
        stop.Cancel();
        await Task.WhenAll(readers);

        const long total = (long)threads * keys * versions;
        Assert.Equal(0, disorders);
        Assert.Equal(total, table.EntryCount);
        Assert.Equal(1, table.MinSequence);
        Assert.Equal(total, table.MaxSequence);

        // Every entry once, in internal key order.
        Entry[] entries = [.. table.Entries()];
        Assert.Equal(total, entries.Length);
        for (int i = 1; i < entries.Length; i++)
            Assert.True(InternalKey.Compare(entries[i - 1].Key, entries[i - 1].Sequence, entries[i].Key, entries[i].Sequence) < 0);

        // A seek goes down from the top level, so a node linked out of order at any level above the
        // bottom one makes it miss the entry it looks for.
        foreach (Entry entry in entries)
        {
            Assert.True(table.TryGetNewest(entry.Key, entry.Sequence, true, out Entry found));
            Assert.Equal(entry.Sequence, found.Sequence);
            Assert.Equal($"{entry.Sequence}", S(found.Value));
        }
    }

    [Fact]
    public async Task Concurrent_range_tombstones_are_all_kept()
    {
        const int threads = 8;
        const int each = 200;
        MemTable table = new();
        using Barrier start = new(threads);

        Task[] writers = [.. Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
        {
            start.SignalAndWait();
            for (int i = 0; i < each; i++)
            {
                long sequence = 1 + ((long)i * threads) + t;
                byte[] begin = B($"b{sequence:D6}");
                byte[] end = B($"e{sequence:D6}");
                table.AddRangeTombstoneConcurrently(begin, end, sequence);
                table.AddCounts(MemTable.TombstoneBytes(begin, end), 0, sequence, sequence);
            }
        }, TaskCreationOptions.LongRunning))];

        await Task.WhenAll(writers);

        Assert.Equal(threads * each, table.Tombstones.Count);
        Assert.Equal(threads * each, table.Tombstones.Select(t => t.Sequence).Distinct().Count());
        Assert.Equal(0, table.EntryCount);
        Assert.Equal(threads * each, table.MaxSequence);
        Assert.False(table.IsEmpty);
    }

    // Overlapping, nested and repeated ranges over a small key space, so the runs that the writer
    // builds and merges share keys: after every range delete, by both writer paths, the highest
    // tombstone over each key at each snapshot is what a scan of all of them finds.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Covering_tombstones_match_a_scan_of_all_of_them_at_every_count_and_snapshot(bool concurrently)
    {
        Random random = new(7);
        MemTable table = new();
        List<RangeTombstone> added = [];
        for (int sequence = 1; sequence <= 300; sequence++)
        {
            int begin = random.Next(60);
            byte[] from = Key(begin);
            byte[] to = Key(begin + 1 + random.Next(random.Next(2) == 0 ? 3 : 40));
            if (concurrently)
                table.AddRangeTombstoneConcurrently(from, to, sequence);
            else
                table.AddRangeTombstone(from, to, sequence);

            added.Add(new RangeTombstone(from, to, sequence));
            for (int probe = 0; probe < 20; probe++)
            {
                byte[] key = Key(random.Next(-2, 104));
                long snapshot = random.Next(sequence + 2);
                Assert.Equal(Scan(added, key, snapshot), table.CoveringTombstone(key, snapshot));
            }
        }

        Assert.Equal(added.Select(t => t.Sequence), table.Tombstones.Select(t => t.Sequence));
    }

    // A writer adds range deletes while readers look up keys at the newest published sequence number;
    // each reader must find what a scan of the tombstones at or below its snapshot finds, never a
    // tombstone from a slot or a run that is not published yet, and never miss one that is.
    [Fact]
    public async Task Readers_find_the_tombstones_of_their_snapshot_while_a_writer_adds_and_merges_runs()
    {
        const int Count = 2000;
        Random random = new(11);
        RangeTombstone[] all = new RangeTombstone[Count];
        for (int i = 0; i < Count; i++)
        {
            int begin = random.Next(400);
            all[i] = new RangeTombstone(Key(begin), Key(begin + 1 + random.Next(30)), i + 1);
        }

        MemTable table = new();
        long published = 0;
        using CancellationTokenSource done = new();
        Task[] readers = [.. Enumerable.Range(0, 4).Select(r => Task.Factory.StartNew(() =>
        {
            Random own = new(r);
            int checks = 0;
            while (!done.IsCancellationRequested || checks == 0)
            {
                long snapshot = Volatile.Read(ref published);
                byte[] key = Key(own.Next(440));
                long expected = Scan(all.AsSpan(0, (int)snapshot).ToArray(), key, snapshot);
                Assert.Equal(expected, table.CoveringTombstone(key, snapshot));
                checks++;
            }
        }, TaskCreationOptions.LongRunning))];

        for (int i = 0; i < Count; i++)
        {
            table.AddRangeTombstone(all[i].Begin, all[i].End, all[i].Sequence);
            Volatile.Write(ref published, i + 1);
        }

        done.Cancel();
        await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(60));
    }

    // The fragments of the runs are memory the memtable holds, so they count toward its flush.
    [Fact]
    public void The_fragments_of_the_tombstones_count_in_the_approximate_bytes()
    {
        MemTable table = new();
        long tombstoneBytes = 0;
        for (int i = 0; i < 64; i++)
        {
            byte[] begin = Key(2 * i);
            byte[] end = Key((2 * i) + 1);
            table.AddRangeTombstone(begin, end, i + 1);
            tombstoneBytes += MemTable.TombstoneBytes(begin, end);
        }

        // 64 ranges that do not overlap: one run of 64 fragments, each two key references, an offset and a sequence number.
        Assert.Equal(tombstoneBytes + (64 * ((2 * IntPtr.Size) + sizeof(int) + sizeof(long))), table.ApproximateBytes);
    }

    private static byte[] Key(int i) => B($"k{i + 10:D4}");

    private static long Scan(IEnumerable<RangeTombstone> tombstones, byte[] key, long snapshot)
    {
        long highest = 0;
        foreach (RangeTombstone tombstone in tombstones)
            if (tombstone.Covers(key, snapshot))
                highest = Math.Max(highest, tombstone.Sequence);

        return highest;
    }
}
