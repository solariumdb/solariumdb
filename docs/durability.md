# Durability and recovery

## What a write survives

| Write | Process crash | Power loss |
| --- | --- | --- |
| Synced (`WriteOptions.Sync`) | Yes | Yes, once `Write` returns |
| Unsynced | Yes | Only after a clean close: `Dispose` flushes the current WAL file |

After a WAL, flush or compaction failure, writes fail until you reopen the database. A failed append leaves the WAL tail unknown. Reads continue.

## WAL files

A new WAL file starts at the first write group after a memtable switch. The old file is flushed first, which means that only the newest file can have a torn tail.

At open, the WAL files are replayed in order, and entries that an SSTable already has are skipped. A memtable that fills during the replay is written as a level 0 table immediately, so the WAL files do not need to fit in memory. The manifest is saved at the end of the open and records those tables. Then a new WAL file starts.

All integers are little-endian, and each batch is one record.

```
file      = [u32 magic "SWAL"][u32 version = 2][record]...
record    = [u32 payload length][u32 CRC-32 of payload][payload]
payload   = [u64 first sequence number][u64 durable offset][u32 operation count][operation]...
operation = [u8 kind = 2][u32 family id][u32 key length][key]                          delete
          | [u8 kind = 1 or 3][u32 family id][u32 key length][key][u32 length][bytes]  put, delete range (bytes = end key)
```

The durable offset is how much of the file a completed flush had made durable when the record was appended. A synced group raises it for the records that follow, and so does the WAL flush before a memtable flush. The header is written and flushed when the file is created, before any record.

A version 1 file, from format 2, has neither a header nor durable offsets. These files can be replayed.

## Recovery after a crash

A process crash can only cut the last write short. A power loss can also lose any sector written since the last flush, in any order. The newest WAL file can therefore end with damage that has intact records after it.

Recovery cuts the newest file at its first damage, together with the bytes that follow. The open fails when there is proof that the damage did not come from a crash.

- An intact record after the damage has a durable offset past it. The damaged bytes had been flushed, and cutting them would drop data.
- The search for that record reaches its limit. It checksums at most four times the bytes after the damage plus 64 MiB. A crash does not leave that many records with bad checksums.
- An older file is damaged, the sequence numbers have a gap, or a WAL file in the manifest is missing.
- A version 1 file has any damage with data after it, other than the end of a record or zeros.

One case cannot be detected. If no crash caused the damage, and it lies in the last synced group of the file with no record after it, the file does not show that the group was flushed. The cut then drops a synced write. `SolariumWAL` detects this case with its sync ledger (see [kommander.md](kommander.md)).

In RocksDB, `TolerateCorruptedTailRecords`, which Kommander uses, fails the open after any power loss that left such a hole. `PointInTimeRecovery` drops everything after the first damage, whatever its cause.

## Damage to the SSTables

A table file is written once and flushed before the manifest names it. Damage to it is therefore always corruption. A read of the damage throws `SolariumException` with `IsCorruption` set, and the message has the file and the offset. A retry gives the same result, and the data must come from another copy.

### Checks at open

- The footer is checked by its magic number, version and CRC-32, and all blocks except the data blocks by their checksums. Each index partition is checked against its key in the top-level index.
- The table must match its manifest record: the size, the entry count, the sequence numbers and the key range. From table version 5, the table id must match too, when the manifest records one.

These checks come before the open replays the WAL or saves the manifest. If a valid table file replaced another one, the open fails and the files in the manifest are unchanged.

### Checks at each read of a data block

- The block's checksum. In table version 5 it includes the table's id and the block's offset, which detects a block copied from another place.
- The block's keys against its index entry. The last key must equal the entry, and the first key must come after the previous block. This detects a misplaced block in any table version.
- The lengths, counts and entry kinds that the read decodes.

In a version 5 table, the key check decodes the first and last entries only. The entries between them are checked when a read reaches them. In an older table, the check walks the complete block. A block in a cache passed these checks before it was cached.

A read that finds corruption fails, and other keys can be read. A compaction that finds it stops the database's writes.

For the power-loss and kill tests that check these rules, see [testing.md](testing.md).
