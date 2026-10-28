using System.Buffers;
using System.IO.Hashing;
using Solarium.Engine;
using Solarium.Storage;
using static Solarium.Tests.Text;

namespace Solarium.Tests;

/// <summary>Replay, torn-tail handling, and damage detection of <see cref="WriteAheadLog"/>.</summary>
public sealed class WriteAheadLogTests : IDisposable
{
    private readonly TempDirectory _directory = new();
    private long _durable;

    private string WalPath => _directory.Combine("wal.log");

    [Fact]
    public void Replays_batches_in_order_with_their_sequence_numbers()
    {
        Append(1, Put(0, "a", "1"), Put(3, "b", "2"));
        Append(3, Delete(0, "a"), Range(3, "b", "c"));

        List<(long, string)> replayed = Replay();

        Assert.Equal([(1, "put 0 a=1"), (2, "put 3 b=2"), (3, "delete 0 a"), (4, "range 3 [b, c)")], replayed);
    }

    [Fact]
    public void A_torn_tail_is_cut_off_and_new_records_follow_the_last_intact_one()
    {
        Append(1, Put(0, "a", "1"));
        long intact = new FileInfo(WalPath).Length;

        ArrayBufferWriter<byte> torn = new();
        WriteAheadLog.EncodeBatch(torn, 2, [Put(0, "b", new string('x', 100))]);
        AppendRaw(torn.WrittenSpan[..60]);

        Assert.Equal([(1, "put 0 a=1")], Replay());
        Assert.Equal(intact, new FileInfo(WalPath).Length);

        Append(2, Put(0, "c", "3"));
        Assert.Equal([(1, "put 0 a=1"), (2, "put 0 c=3")], Replay());
    }

    [Fact]
    public void A_bad_checksum_on_the_last_record_is_a_torn_tail()
    {
        Append(1, Put(0, "a", "1"));
        Append(2, Put(0, "b", "2"));
        FlipByte(new FileInfo(WalPath).Length - 1);

        Assert.Equal([(1, "put 0 a=1")], Replay());
    }

    [Fact]
    public void Damage_that_a_later_record_shows_was_flushed_refuses_to_open()
    {
        Append(1, Put(0, "a", "1"));
        long secondRecord = new FileInfo(WalPath).Length;
        Append(2, Put(0, "b", "2"));
        long thirdRecord = new FileInfo(WalPath).Length;
        Append(3, Put(0, "c", "3"));
        FlipByte(secondRecord + 20);

        // The third record was appended after a flush that covered the second.
        SolariumException e = Assert.Throws<SolariumException>(Replay);
        Assert.Contains($"offset {secondRecord}, which the intact record at offset {thirdRecord} shows had been flushed", e.Message);
    }

    [Fact]
    public void Damage_in_unflushed_records_is_cut_off_with_the_intact_records_after_it()
    {
        Append(1, Put(0, "a", "1"));
        long secondRecord = new FileInfo(WalPath).Length;
        Append(2, sync: false, Put(0, "b", "2"));
        Append(3, sync: false, Put(0, "c", "3"));
        FlipByte(secondRecord + 20);

        // A power loss can lose a sector of what was never flushed and keep the ones after it.
        Assert.Equal([(1, "put 0 a=1")], Replay());
        Assert.Equal(secondRecord, new FileInfo(WalPath).Length);
    }

    [Fact]
    public void A_version_1_file_replays_refuses_damage_with_data_after_it_and_takes_no_append()
    {
        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, 1, [Put(0, "a", "1")], version: 1);
        int secondRecord = records.WrittenCount;
        WriteAheadLog.EncodeBatch(records, 2, [Delete(0, "a"), Range(1, "b", "c")], version: 1);
        WriteAheadLog.EncodeBatch(records, 4, [Put(0, "d", "4")], version: 1);
        File.WriteAllBytes(WalPath, records.WrittenSpan.ToArray());

        Assert.Equal([(1, "put 0 a=1"), (2, "delete 0 a"), (3, "range 1 [b, c)"), (4, "put 0 d=4")], Replay());
        using (WriteAheadLog wal = WriteAheadLog.Open(DefaultStorageEnvironment.Instance.OpenFile(WalPath), (_, _) => { }))
            Assert.Throws<InvalidOperationException>(() => wal.Append(records.WrittenSpan, sync: false));

        FlipByte(secondRecord + 20);
        SolariumException e = Assert.Throws<SolariumException>(Replay);
        Assert.Contains($"offset {secondRecord}, with intact data after it", e.Message);
    }

    [Fact]
    public void An_operation_count_that_a_valid_checksum_covers_but_does_not_fit_is_corruption()
    {
        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, 1, [Put(0, "a", "1")]);
        byte[] record = records.WrittenSpan.ToArray();

        // The count follows the first sequence number and the durable offset; the CRC covers it.
        BitConverter.TryWriteBytes(record.AsSpan(8 + 16, 4), int.MaxValue);
        BitConverter.TryWriteBytes(record.AsSpan(4, 4), Crc32.HashToUInt32(record.AsSpan(8)));
        using (WriteAheadLog wal = WriteAheadLog.Create(DefaultStorageEnvironment.Instance.OpenFile(WalPath)))
            wal.Append(record, sync: true);

        SolariumException e = Assert.Throws<SolariumException>(Replay);
        Assert.Contains("operation count", e.Message);
    }

    // The count check bounds the count by the smallest operation; a first operation that takes
    // every byte leaves the second one with none, not even its kind.
    [Fact]
    public void An_operation_that_starts_past_the_end_of_a_record_with_a_valid_checksum_is_corruption()
    {
        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, 1, [Put(0, "0123456789", "")]);
        byte[] record = records.WrittenSpan.ToArray();
        BitConverter.TryWriteBytes(record.AsSpan(8 + 16, 4), 2);
        BitConverter.TryWriteBytes(record.AsSpan(4, 4), Crc32.HashToUInt32(record.AsSpan(8)));
        using (WriteAheadLog wal = WriteAheadLog.Create(DefaultStorageEnvironment.Instance.OpenFile(WalPath)))
            wal.Append(record, sync: true);

        SolariumException e = Assert.Throws<SolariumException>(Replay);
        Assert.Contains("operation 1 of 2", e.Message);
        Assert.Contains("does not decode", e.Message);
    }

    // A record cut short at every byte after its batch header, with its length and checksum made to
    // match, so only the decoder can find the cut: every field of every kind of operation.
    [Fact]
    public void A_record_cut_short_at_any_field_with_a_valid_checksum_is_corruption()
    {
        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, 1, [Put(0, "a", "1"), Delete(2, "bb"), Range(3, "c", "d")]);
        byte[] whole = records.WrittenSpan.ToArray();
        const int BatchHeader = 20;
        for (int payload = BatchHeader; payload < whole.Length - 8; payload++)
        {
            File.Delete(WalPath);
            byte[] record = whole[..(8 + payload)];
            BitConverter.TryWriteBytes(record.AsSpan(0, 4), payload);
            BitConverter.TryWriteBytes(record.AsSpan(4, 4), Crc32.HashToUInt32(record.AsSpan(8)));
            using (WriteAheadLog wal = WriteAheadLog.Create(DefaultStorageEnvironment.Instance.OpenFile(WalPath)))
                wal.Append(record, sync: true);

            SolariumException e = Assert.Throws<SolariumException>(Replay);
            Assert.Contains("does not decode", e.Message);
        }
    }

    [Fact]
    public void A_tail_of_zeros_is_a_torn_tail()
    {
        Append(1, Put(0, "a", "1"));
        AppendRaw(new byte[4096]);

        Assert.Equal([(1, "put 0 a=1")], Replay());
    }

    [Fact]
    public void Records_larger_than_the_read_buffer_replay()
    {
        string large = new('x', 3 * 1024 * 1024);
        Append(1, Put(0, "small", "1"));
        Append(2, Put(0, "large", large));
        Append(3, Put(0, "after", "2"));

        Assert.Equal([(1, "put 0 small=1"), (2, $"put 0 large={large}"), (3, "put 0 after=2")], Replay());
    }

    private static WalOperation Put(int family, string key, string value) => new(OperationKind.Put, family, B(key), B(value));

    private static WalOperation Delete(int family, string key) => new(OperationKind.Delete, family, B(key), null);

    private static WalOperation Range(int family, string begin, string end) => new(OperationKind.DeleteRange, family, B(begin), B(end));

    private void Append(long firstSequence, params WalOperation[] operations) => Append(firstSequence, sync: true, operations);

    // Appends one record, with the durable offset that the log knows, as the engine does. A log
    // that is opened again knows only its replayed length as durable, so an unsynced record leaves
    // the offset where the last synced one did.
    private void Append(long firstSequence, bool sync, params WalOperation[] operations)
    {
        IStorageFile file = DefaultStorageEnvironment.Instance.OpenFile(WalPath);
        using WriteAheadLog wal = file.GetLength() == 0 ? WriteAheadLog.Create(file) : WriteAheadLog.Open(file, (_, _) => { });
        long durable = sync ? wal.DurableLength : Math.Min(wal.DurableLength, _durable);
        ArrayBufferWriter<byte> records = new();
        WriteAheadLog.EncodeBatch(records, firstSequence, [.. operations], durableOffset: durable);
        wal.Append(records.WrittenSpan, sync);
        if (sync)
            _durable = wal.DurableLength;
    }

    private List<(long, string)> Replay()
    {
        List<(long, string)> replayed = new();
        using WriteAheadLog wal = WriteAheadLog.Open(DefaultStorageEnvironment.Instance.OpenFile(WalPath), (first, operations) =>
        {
            foreach (WalOperation op in operations)
                replayed.Add((first++, op.Kind switch
                {
                    OperationKind.Put => $"put {op.FamilyId} {S(op.Key)}={S(op.Value)}",
                    OperationKind.Delete => $"delete {op.FamilyId} {S(op.Key)}",
                    _ => $"range {op.FamilyId} [{S(op.Key)}, {S(op.Value)})",
                }));
        });

        return replayed;
    }

    private void AppendRaw(ReadOnlySpan<byte> bytes)
    {
        using FileStream stream = new(WalPath, FileMode.Append);
        stream.Write(bytes);
    }

    private void FlipByte(long offset)
    {
        byte[] bytes = File.ReadAllBytes(WalPath);
        bytes[offset] ^= 0xFF;
        File.WriteAllBytes(WalPath, bytes);
    }

    /// <summary>Deletes the temporary directory.</summary>
    public void Dispose() => _directory.Dispose();
}
