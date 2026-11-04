# Durability and recovery

What a write survives, how the WAL is laid out on disk, and how an open recovers after a crash or a power loss.

## What a write survives

- A synced write (`WriteOptions.Sync`) survives a power loss once `Write` returns.
- A write that is not synced survives a process crash, but not a power loss, as in RocksDB. After a clean close it survives a power loss too: `Dispose` flushes the current WAL file.
- A WAL, flush or compaction failure stops all later writes until the database is opened again: the WAL tail is unknown after a failed append. Reads continue.

## WAL files

A new WAL file starts at the first write group after a memtable switch. The old file is flushed first, so only the newest file can have a torn tail. At open, every WAL file is replayed in order, and entries that an SSTable already holds are skipped. A memtable that fills during the replay is written as a level 0 table at once, as RocksDB's recovery does, so the WAL files need not fit in memory; the manifest saved at the end of the open records those tables. Then a new WAL file starts.

The WAL file and record format (all integers little-endian, one record for each batch):

```
file      = [u32 magic "SWAL"][u32 version = 2][record]...
record    = [u32 payload length][u32 CRC-32 of payload][payload]
payload   = [u64 first sequence number][u64 durable offset][u32 operation count][operation]...
operation = [u8 kind = 2][u32 family id][u32 key length][key]                          delete
          | [u8 kind = 1 or 3][u32 family id][u32 key length][key][u32 length][bytes]  put, delete range (bytes = end key)
```

The durable offset is how much of the file a completed flush had made durable when the record was appended: a synced group raises it for the records after it, and so does the flush of the WAL before a memtable flush. The header is written and flushed when the file is created, before any record. A WAL file of version 1, from format 2, has no header and no durable offset; it still replays.

A process crash can only cut the last write short. A power loss can also lose any sector written since the last flush, in any order, so the newest WAL file can end with damage that has intact records after it. Recovery cuts the newest file at its first damage, with everything after it, unless an intact record after the damage has a durable offset past it: that record proves the damaged bytes had been flushed, no crash explains the damage, and the open fails rather than drop data. Damage in an older file, a gap in the sequence numbers, or a missing WAL file that the manifest lists cannot come from a crash either, and fail the open too. A version 1 file keeps the rule it was written under: any damage with data after it, other than the end of a record or zeros, fails the open.

The cut can drop a synced write only when flushed data was damaged in a way no crash causes, and no record after it was written: damage inside the last synced group of the file, with nothing appended since. Then nothing in the file shows that the group had been flushed. RocksDB's two recovery modes for this case are `TolerateCorruptedTailRecords`, which Kommander uses and which fails the open after any power loss that left such a hole, and `PointInTimeRecovery`, which drops everything after the first damage whatever its cause. The power-loss test opens after every kind of loss it simulates.

## Damage to the SSTables

A table file is written once, flushed before the manifest names it, and never written again, so damage to it is never a crash's torn tail: whatever a read finds wrong is corruption, which throws `SolariumException` with `IsCorruption` set and names the file and the offset. A retry fails the same way; the data must come from another copy. What is checked:

- At open, the footer (its magic, version and CRC-32) and every block but the data blocks, each against its checksum, and each index partition against the key the top-level index holds for it. The table must then be the one the manifest records: the same table id (from table version 5, when the manifest records one), size, entry count, sequence numbers and key range. A table file that another, whole and valid one replaced, an older table of the same size or the same-numbered table of another database, fails here, before the open replays the WAL or saves the manifest, so every file the manifest lists stays as it was.
- At each read of a data block from the file, by `Get`, `MultiGet`, an iterator or a compaction: its checksum, bound in table version 5 to the table's id and the block's offset, so a block copied from another place of the table or from another table fails it; its keys against its index entry, the last one equal to it and the first after the block before it, which finds a misplaced block in a table of any version; and the decoding of every length, count and entry kind it reads. A version 5 data block names the offset of its last entry, so this check decodes its first and last entries only, and the last must end the block; the entries between them are decoded, and refused if they do not decode, when a read reaches them. In an older table the check walks every entry of the block. A block that a cache holds passed these checks before it went in.

A read that hits corruption fails, and nothing else changes: other keys still read. A compaction that hits it stops the database's writes, as any compaction failure does.

## How it is tested

[testing.md](testing.md) describes the power-loss and kill tests that check these rules.
