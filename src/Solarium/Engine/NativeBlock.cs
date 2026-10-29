using System.Runtime.InteropServices;

namespace Solarium.Engine;

/// <summary>
/// The verified and decompressed entries of one data block in a <see cref="Cache"/>, in native memory, as RocksDB
/// keeps its cached blocks: the garbage collector sees only this small handle, not the block's bytes, so a block
/// that lives until its eviction is never copied by a collection and counts against no heap limit.
/// </summary>
/// <remarks>
/// <para>A reference count decides when the bytes are freed: the cache holds one while the block is cached, and
/// each reader holds one from the lookup that found the block to its <see cref="Release"/>, which every read does
/// in a <c>finally</c>. The last release frees the bytes, so an eviction never frees a block under a reader. A
/// reader takes its reference under the cache shard's lock, while the cache's own reference is still held, so the
/// count never rises from 0.</para>
/// <para>Each block is listed by the <see cref="BlockMemory"/> of its shard until it is freed; a cache that becomes
/// unreachable frees the blocks it still lists when the garbage collector finalizes that list.</para>
/// </remarks>
internal sealed unsafe class NativeBlock
{
    private readonly BlockMemory _owner;
    private byte* _data;
    private int _references = 1;   // Interlocked: the creator's, until it releases it

    /// <summary>A block of <paramref name="contents"/>, copied, with one reference, the caller's.</summary>
    internal NativeBlock(BlockMemory owner, ReadOnlySpan<byte> contents)
    {
        _owner = owner;
        Length = contents.Length;
        _data = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, contents.Length));
        contents.CopyTo(new Span<byte>(_data, contents.Length));
    }

    /// <summary>The bytes of the entries.</summary>
    public int Length { get; }

    /// <summary>The entries; valid only while the caller holds a reference.</summary>
    public ReadOnlySpan<byte> Span => new(_data, Length);

    // The neighbours in the owner's list, under its lock.
    internal NativeBlock? Previous { get; set; }

    internal NativeBlock? Next { get; set; }

    /// <summary>One more reference, from a holder that has one already or from the cache shard under its lock.</summary>
    public void AddReference() => Interlocked.Increment(ref _references);

    /// <summary>Drops a reference; the last one frees the bytes.</summary>
    public void Release()
    {
        int left = Interlocked.Decrement(ref _references);
        if (left == 0)
            _owner.Free(this);
        else if (left < 0)
            throw new InvalidOperationException("A cached block was released more times than it was referenced.");
    }

    // Under the owner's lock, once.
    internal void FreeBytes()
    {
#if DEBUG
        // A reader that kept the span past its release reads this pattern instead of the entries.
        new Span<byte>(_data, Length).Fill(0xDD);
#endif
        NativeMemory.Free(_data);
        _data = null;
    }
}

/// <summary>
/// The native blocks of one cache shard that are not freed yet, in a list, so that a cache the garbage collector
/// finds unreachable frees them: every block references its list, so the list is unreachable only when no reader
/// holds any of its blocks.
/// </summary>
internal sealed class BlockMemory
{
    private readonly object _lock = new();
    private NativeBlock? _first;   // under _lock
    private long _count;           // under _lock
    private long _bytes;           // under _lock

    ~BlockMemory()
    {
        lock (_lock)
        {
            for (NativeBlock? block = _first; block is not null; block = block.Next)
                block.FreeBytes();

            _first = null;
        }
    }

    /// <summary>The blocks allocated and not freed yet, cached or held by readers.</summary>
    public long Count
    {
        get
        {
            lock (_lock)
                return _count;
        }
    }

    /// <summary>The bytes of <see cref="Count"/> blocks.</summary>
    public long Bytes
    {
        get
        {
            lock (_lock)
                return _bytes;
        }
    }

    /// <summary>A new block of <paramref name="contents"/>, listed, with one reference, the caller's.</summary>
    public NativeBlock Allocate(ReadOnlySpan<byte> contents)
    {
        NativeBlock block = new(this, contents);
        lock (_lock)
        {
            block.Next = _first;
            if (_first is not null)
                _first.Previous = block;

            _first = block;
            _count++;
            _bytes += block.Length;
        }

        return block;
    }

    // The last release of a block: unlist it and free its bytes.
    internal void Free(NativeBlock block)
    {
        lock (_lock)
        {
            if (block.Previous is not null)
                block.Previous.Next = block.Next;
            else
                _first = block.Next;

            if (block.Next is not null)
                block.Next.Previous = block.Previous;

            block.Previous = null;
            block.Next = null;
            _count--;
            _bytes -= block.Length;
            block.FreeBytes();
        }
    }
}
