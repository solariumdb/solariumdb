# Design

This page describes how Solarium reads and writes data, and the files on disk. Three related topics have separate pages.

- [durability.md](durability.md): the WAL and recovery.
- [storage-environments.md](storage-environments.md): file I/O.
- [rocksdb-differences.md](rocksdb-differences.md): the deliberate differences from RocksDB.

## Column families

A column family is a named, ordered key space. All families share the WAL and one sequence of sequence numbers, which makes a batch across families atomic.

Each family has a **version**, like RocksDB's `SuperVersion`. It lists the active memtable, the immutable memtables that wait for a flush, and the SSTables. A version is immutable. A memtable switch publishes a new one, and so does the install of a flush or a compaction.

## Writes: group commit

1. A writer encodes its batch into a WAL record. The sequence numbers are filled in later. The writer joins the write queue, and becomes the leader if there is none.
2. The leader takes a group from the front of the queue, up to `MaxWriteBatchGroupSizeBytes`. If the leader's batch is at most an eighth of the cap, the group stops at that size plus an eighth of the cap, as in RocksDB. A batch larger than the cap is a group of one.
3. The leader switches the full memtables of the families in the group. If the `WriteBufferManager` is past its cap, it also switches the database's memtable with the oldest data (see [Flushes](#flushes)).
4. The leader copies the records into one buffer and gives each one its first sequence number and its checksum. It appends the buffer to the WAL in one write, and flushes the WAL once if a writer asked for `Sync`.
5. The leader offers leadership to the oldest writer in the queue. The next WAL append can then overlap with the memtable inserts of this group, as with RocksDB's `enable_pipelined_write`.
6. The group waits for its memtable stage. One stage is active at a time. Other groups wait in a memtable queue in WAL order, and the next stage takes all of them.
7. The batches go into the active memtables under the sequence numbers of their WAL records. The stage's leader tells the writers that are spinning to insert their own batches. It inserts the batches of the blocked writers itself and waits for the last insert.
8. The stage's leader publishes the last sequence number, which makes the groups visible in WAL order. Then it completes each writer.

The tests can turn pipelining off with an internal option. The leader then publishes its group before it offers leadership.

A record above 64 KiB is in an array of its writer. If it is alone in its group, the leader appends that array directly, and the batch is not in memory twice. After a group larger than twice the cap, the leader's buffer is replaced to release its memory.

### Invariants

- The stage's leader holds the lock that a memtable switch takes, from the inserts to the publication. `Flush` cannot switch a memtable while writers fill it.
- A WAL file stops being current after all the groups appended to it are published. A flush therefore cannot delete a WAL file whose data is not in a memtable yet.
- If an insert fails, its group fails. Writes then fail until the database is reopened. The WAL has the complete group, but part of it is in the memtables under sequence numbers that were not published.
- The offer of leadership is made under the write lock, while the writer is the oldest in the queue. A later offer could reach a writer that waits for its memtable stage, which would read it as the offer of that stage.
- The offer is not a hand-off as in RocksDB. A writer that arrives while there is no leader can lead the queue, and the writer with the offer waits for that group. A running writer starts the next group sooner than a blocked one can wake.

### Waiting writers

Each writer waits on a separate signal. The end of a group wakes its writers and the oldest writer in the queue.

A writer that waits for a WAL flush blocks at once. Other writers spin for up to 20 µs before they block. A long spin that ends in a block wastes a CPU that the leader or an I/O thread needs, and for this reason the spin adapts.

- One turn in 256 is a probe, which spins for 20 µs. Its outcome updates a credit. When more than about a quarter of the probes block, the other turns spin for 1 µs.
- While the spin is short, probes come in windows. Once in 8,192 turns, 256 turns spin for 20 µs and the last 128 of them are probes. A single probe would wait for the wakes of the writers that blocked before it.
- A new database starts with the long spin and a credit of 16 blocked probes.

## Reads

A read takes the family's version and then the last published sequence number as its snapshot, in that order. A version has all the data that its snapshot can see, but a newer version may not. The snapshot can be newer than the version's memtables. A memtable switch splits the sequence numbers at one point, and the read sees the state at one sequence number.

A read takes no locks. It searches the sources of the version from new to old.

1. The memtables.
2. The SSTables of level 0.
3. Levels 1 to 6, each as one source. The files of a level are in key order and do not overlap.

For each key, the versions and range tombstones in a newer source have higher sequence numbers than those in an older source. The first source with a version of the key decides the result.

### Values in place

`TryGet` gives a `ValueReader` the value where it lies: in the memtable's arena, in a cached block, or in a pooled buffer. `Get` is the same lookup with a reader that copies the value. Until the reader returns or throws, the read is counted in its version and has a reference on the cached block. The bytes cannot be freed by a flush, an eviction or the close of the database.

`MultiGet` works in rounds, over batches of up to 32 keys. A round searches each key's sources until a memtable answers or a table's index names a data block. It then reads the distinct blocks of the round in one `IStorageEnvironment.MultiRead` and settles the keys. A key that its block does not contain, after a Bloom filter false positive, continues in the next round.

Each value goes to an `IValueVisitor` when its key settles, in no set order. Values in memtables and cached blocks are visited at once. Values in blocks that the round read are visited after all those blocks are verified.

### File and memtable lifetime

A reader counts itself into its version and out when it is done. An iterator leaves at `Dispose`. A table closes, and a memtable is freed, once the last version that lists it is replaced and its readers are gone. A flush also has a reference on the memtable it will write. The counts are per CPU, so that readers do not share a cache line.

When a database closes, it retires its last versions and turns away new readers. An open iterator can use its memtables until it is disposed.

### Memtable

The memtable is a multi-version skip list. Readers take no lock, and it has one writer or the writers of one group. A concurrent insert links each level with a compare-and-swap from the bottom up, as in RocksDB's `InlineSkipList::InsertConcurrently`.

The nodes are in native memory, outside the GC heap. A node is one allocation with its metadata and links, followed by the key and the value. The arena allocates nodes from 64 KiB chunks, with one chunk per writer's processor during a concurrent insert. A value larger than 16 KiB gets a separate block.

The chunks of a flushed memtable return to a pool for the next memtables. The pool belongs to the `WriteBufferManager`, or is shared by the databases that do not have one. It retains chunks up to the memtable budget.

### Range tombstones

A range tombstone, from `DeleteRange`, hides older versions of the keys in its range, in the same source and in older ones. A source cuts its tombstones at each begin and end into fragments that do not overlap, as RocksDB's `FragmentedRangeTombstoneList` does. A read binary-searches for the fragment, and then for the highest sequence number in its snapshot.

A table builds the fragments when it opens. With 8 tombstones or fewer, it scans them, which costs less.

The memtable indexes its tombstones in runs, as a binary counter carries. When 8 tombstones are past the last run, the writer builds them into a new one, together with the previous runs that are no larger. Run sizes double from 8 tombstones, with at most one run of each size. A lookup searches each run and scans the fewer than 8 tombstones that remain. Readers do not build anything. Each tombstone takes part in one build each time the count doubles.

### Iterators

An iterator merges one cursor per source, with the snapshot and version of its creation. A later flush does not change what it sees.

A table cursor reads keys and values in place, in a cached block or in a buffer of the cursor. It releases the block when it moves to the next one or passes its last entry, and when the iterator is disposed. From the third file read in a row, it reads ahead: 8 KiB at first, doubling up to 256 KiB. A seek to another place starts again with single blocks.

## Flushes

1. A memtable that reaches `WriteBufferSize` becomes immutable at the next write group. `Flush` switches it at once.
2. A background thread writes the oldest immutable memtable to an SSTable and flushes the file. It writes the newest version of each key and drops keys that a newer range tombstone of the same memtable covers. Range tombstones and other point deletes remain, since they hide data in older tables.
3. The current WAL file is flushed. A batch can span families, and the rest of it, or an earlier batch, may be in the WAL alone. Without this step, a power loss could keep the table and lose those records.
4. One manifest save records the table and the new flush point of the family, and drops the WAL files that no memtable needs.
5. A new version replaces the memtable with the table. Then the dropped WAL files are deleted.

Readers do not need what a flush drops. A reader of the new version takes its snapshot after the install, above all entries of the memtable. A reader of the older version reads the memtable.

When a family has `MaxWriteBufferNumber` memtables, writes wait for a flush. When the live WAL files are larger than `MaxTotalWalSize`, the families that need the oldest WAL file are flushed.

A `WriteBufferManager` caps the memtables of all the databases that share it. The stage's leader charges the bytes of a group after its inserts and before its publication. Before each group, the leader asks the manager whether to flush, with the rule of RocksDB's `ShouldFlush`: the active memtables use more than 7/8 of the cap, or all memtables reach the cap and half of it is active. If so, it switches the memtable with the oldest data in the leader's database. A database does not flush the memtables of another one, and the memory of an idle database remains until it takes a write.

A flush failure stops writes until the database is reopened. Reads continue.

## Compactions

A family has seven levels. Level 0 has flushed memtables, whose files can overlap. The files of levels 1 to 6 are in key order and share no key within the level. For each key, the versions in a level are newer than those in the levels below.

Levels are sized as with RocksDB's `level_compaction_dynamic_level_bytes`. Each level above level 6 has a target of one `MaxBytesForLevelMultiplier`th (10) of the level below. Level 0 compacts into the **base level**: the highest level whose target is at most `MaxBytesForLevelBase` (256 MB), and not below the first level with files. A family smaller than that stores its sorted data in level 6. At 4.3 GB in level 6, the targets are 430 MB for level 5 and 256 MB for level 4, the base level.

Up to `MaxBackgroundCompactions` compactions are active at once across all families, each on a separate thread. The level with the highest score of at least 1 goes first. The score of level 0 is its file count over `Level0FileNumCompactionTrigger`. For the base level to level 5, it is bytes over target.

1. **Level 0 into the base level.** From the oldest file, the files that do not overlap each other or the base level move as they are, with one manifest save. The walk ends at the first file that overlaps. If no file can move, the oldest file is merged with the level 0 files that overlap it, directly or through other files, and with the base level's files in their range.
2. **A sorted level into the one below.** The file with the smallest overlap for its size is merged with the files it overlaps (`kMinOverlappingRatio`). A file that overlaps none moves as it is. An append-only key space, such as a Raft log, reaches level 6 this way and is not rewritten.
3. **The merge.** It writes the newest version of each key, unless a range tombstone of the inputs covers it. A point delete remains while a lower level has a file that holds its key, and a range tombstone while a lower file overlaps it. Output files end at `TargetFileSizeBase`, between two keys.
4. **Install.** One manifest save replaces the inputs with the outputs, and a new version is published. An input file is deleted when the last version that lists it closes, possibly on a reader's thread.

A range tombstone is cut at the boundaries between output files. The piece in a file ends at the next file's first key, which is the file's largest bound, excluded. The table records this in `TableProperties.LargestIsExclusive`, and two files of a level may meet at that key.

The merge reads its inputs in windows of 256 KiB and does not fill the cache. Values go from the input block to the output block as spans. Flushes take entries from the memtable in the same way.

### Concurrent compactions

- A file that a compaction reads is not picked again, and does not count in the scores.
- A family has one level 0 compaction at a time.
- A merge of level 0 waits while a base level file in its range is in another compaction.
- A compaction into a level waits while another one writes an overlapping range of that level.

These rules preserve the order of the levels: a merge has all the files of its output level in its range, and no other compaction can move a version of those keys past it. Each compaction installs in one manifest save under the state lock.

`CompactRange` compacts a range one level after the other on one thread. It returns when the last step is installed. A delete in the range reaches the bottom level and drops what it hides. Level 6 files alone are not rewritten. No automatic compaction starts while a manual one waits, and each of its steps waits until no other compaction is active.

After a compaction failure, no other compaction installs and writes fail until the database is reopened. `Dispose` stops an active merge and deletes its output. After a crash, the next open deletes the orphan files.

### DeleteFilesInRange

`DeleteFilesInRange` drops the files of levels 1 to 6 whose keys all lie in a range, in one manifest save. It skips level 0, the memtables, and the files in a running compaction. As in RocksDB, it ignores sequence numbers. A key of a dropped file is gone for later reads, and an older version of it in a lower file that reaches past the range can come back. Reads and iterators that started before are not affected.

Kommander reclaims the dead prefix of a Raft log with it. The files of one partition do not overlap, and each one can be dropped once its last entry is below the compaction floor.

## Pacing background writes

A table is written through a 1 MiB buffer. Before each write, the builder asks the `RateLimiter` for the bytes, if the database has one. Flushes have high priority and compactions low. Each refill period (100 ms by default) serves flushes first, and each tenth one serves compactions first. An auto-tuned limiter starts at half its rate. It moves between a twentieth of the rate and the full rate, by how often the writes waited. The WAL, the manifest, checkpoints and the flushes of a replay are not paced.

After each `BytesPerSync` bytes of a table, the builder calls `IStorageFile.SyncRange` to start the writeback. The table then reaches the drive gradually, and its last sync does not delay the WAL's syncs. A failed range sync fails the write of the table.

## Write stalls

Writes slow down and then stop when flushes add level 0 files faster than compactions remove them. One family past a trigger affects the writes to all families. Files that a compaction is reading count.

- **Delay.** It starts when a family has `Level0SlowdownWritesTrigger` files in level 0. Each WAL leader takes credit for the previous group's bytes from a bucket that refills at `DelayedWriteRate` once a millisecond. If the credit is too low, the leader sleeps in steps of 1 ms, outside the state lock.
- **Stop.** It starts at `Level0StopWritesTrigger` files. The leader waits until all families are below the trigger. A flush or compaction failure, or `Dispose`, fails the waiting group. Flushes continue, and level 0 can grow past the trigger.

A family with `DisableAutoCompactions` does not stall writes on level 0. The memtable count stalls writes separately, at `MaxWriteBufferNumber`.

## SSTables

An SSTable stores internal keys: the user key, the sequence number, and the kind. `TableFormat` in `Engine/TableFormat.cs` documents the layout.

| Part | Content |
| --- | --- |
| Data blocks | 4 KB of entries, raw or as LZ4 when that saves at least an eighth |
| Range tombstone block | The range tombstones |
| Index partitions | The last internal key of each data block, in partitions of about 4 KB |
| Filter partitions | A Bloom filter (10 bits per key) on the user keys of each index partition |
| Top-level index | The last internal key and the handles of each partition |
| Properties block, footer | Metadata of the table and its blocks |

All blocks and the footer have a CRC-32. From table version 5, it is combined with the table's id and the block's offset, so that a block written at the wrong place fails the check. A read also checks a data block's first and last keys against its index entry. A version 5 block stores the offset of its last entry, and the check decodes two entries. [durability.md](durability.md) lists the checks.

A lookup finds the partition in the top-level index, checks its filter, and searches its index for the block. Range tombstones and the top-level index are always in memory. Partitions are in memory or in a block cache. They take about 2% of the table's bytes (4% with LZ4), and the top-level index takes 0.04%. A decoded index is five objects, whatever its number of blocks.

### Block cache

The cache is an LRU of a byte capacity, split into shards. Each shard has a high-priority pool, up to `HighPriorityPoolRatio` (0.5), for index and filter partitions and for entries that were read again. The low-priority pool has the rest, and evictions take from it first. An entry that overflows the high-priority pool moves to the newest end of the low one. Pinned entries are not evicted, and the cache can exceed its capacity when only these are left.

- **`CacheIndexAndFilterBlocks`.** Each partition's index and filter are one entry, cached when the table opens. A miss reads and verifies two blocks, a few kilobytes. A reader that took an index can use it after an eviction. With `PinL0FilterAndIndexBlocksInCache`, the partitions of a level 0 table are pinned until it leaves level 0.
- **`CacheDataBlocks`.** A miss reads, verifies and decompresses the block and caches its entries in native memory. A read holds a reference until the end of its decode, and the last reader frees an evicted block. The option is off by default, since it slows reads on a store larger than the cache (see [benchmarks.md](benchmarks.md)).

With `ReadOptions.FillCache` false, a miss is not cached. Compactions read this way. A table that closes erases its partitions, and its data blocks are left to the LRU. A database that closes erases all its entries. A `WriteBufferManager` created with the cache charges the memtables to it as pinned entries of 256 KiB.

## Checkpoints

`CreateCheckpoint` copies the database to a new directory, as of one sequence number in all families.

1. Under the state lock, it reads the latest published sequence number and enters the current version of each family. The tables of those versions remain in place.
2. It creates the directory, which must not exist, and takes its `LOCK`. If another checkpoint or an `Open` took the directory first, the checkpoint fails and does not touch the files there.
3. Outside the lock, it writes each memtable to a level 0 table of the copy, as a read at that sequence number sees it.
4. It links the tables of all levels into the copy, with their numbers and levels. If the file system does not support a link, it copies that table and the later ones. A table file is not written after a manifest names it, which makes the sharing safe.
5. It writes the manifest, which flushes the directory. `FORMAT` comes last. The manifest lists no WAL file, and the copy opens with no replay.

A directory that lacks `FORMAT` is an unfinished copy, and `Open` fails on it. A failed checkpoint deletes its files and does not stop the source's writes.

`Dispose` stops a running checkpoint and waits for it. The checkpoint checks for this between its steps, and then deletes its files and throws `ObjectDisposedException`. If it is already past the check before `FORMAT`, it completes.

## Files in the data directory

| File | Content |
| --- | --- |
| `LOCK` | An exclusive lock. Only one open database can use the directory. |
| `FORMAT` | The format version, `solarium-format 6`. |
| `MANIFEST` | The live WAL files, the column families with their flush points, and their SSTables with levels and ids. A binary record with a CRC-32, replaced atomically. `Engine/Manifest.cs` documents the layout. |
| `NNNNNN.log` | A WAL file. |
| `NNNNNN.sst` | An SSTable. |
| `.*` | An application's file, which the database does not read or delete. `SolariumWAL` stores its ledgers there (see [kommander.md](kommander.md)). |

A first open writes `MANIFEST` after `FORMAT` and before any WAL or SST file. `Open` treats a directory that lacks a manifest as new only if it has no file other than `LOCK`, `FORMAT`, temporary files and application files. Otherwise the manifest was lost, and `Open` fails and changes no file.

### Format versions

Older directories move to format 6 at open. A format 1 directory is migrated. In the others, the existing WAL files are replayed, and the existing tables and manifest can be read. Compactions rewrite the tables over time.

| Format | Change |
| --- | --- |
| 3 | WAL file version 2, with a header and durable offsets |
| 4 | Table version 3: data blocks have a type and can be compressed |
| 5 | Table version 4: the index and filter are partitioned |
| 6 | Table version 5: checksums are bound to the table's id and the block's offset. Manifest version 4 records the ids. |
