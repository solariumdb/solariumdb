using static System.Buffers.Binary.BinaryPrimitives;

namespace Solarium.Engine;

/// <summary>A Bloom filter over 64-bit key hashes: 10 bits per key and 7 probes, about 1% false positives.</summary>
internal sealed class BloomFilter
{
    private const int BitsPerKey = 10;
    private const int Probes = 7;

    private readonly byte[] _bits;
    private readonly uint _bitCount;
    private readonly int _probes;

    private BloomFilter(byte[] bits, uint bitCount, int probes)
    {
        _bits = bits;
        _bitCount = bitCount;
        _probes = probes;
    }

    /// <summary>Builds the filter block (bit count, probe count, bits) for the key hashes of one table.</summary>
    public static byte[] Build(List<ulong> hashes)
    {
        uint bitCount = (uint)Math.Max(64, hashes.Count * BitsPerKey);
        byte[] block = new byte[5 + ((bitCount + 7) / 8)];
        WriteUInt32LittleEndian(block, bitCount);
        block[4] = Probes;
        Span<byte> bits = block.AsSpan(5);

        foreach (ulong hash in hashes)
        {
            uint h1 = (uint)hash, h2 = (uint)(hash >> 32) | 1;
            for (int i = 0; i < Probes; i++)
            {
                uint bit = (uint)((h1 + ((ulong)i * h2)) % bitCount);
                bits[(int)(bit >> 3)] |= (byte)(1 << (int)(bit & 7));
            }
        }

        return block;
    }

    /// <summary>Decodes a filter block; throws <see cref="ArgumentOutOfRangeException"/> when its size does not match its header.</summary>
    public static BloomFilter Decode(byte[] block)
    {
        uint bitCount = ReadUInt32LittleEndian(block);
        if (bitCount == 0 || block.Length != 5 + ((bitCount + 7) / 8))
            throw new ArgumentOutOfRangeException(nameof(block), "the filter block has the wrong size");

        return new BloomFilter(block[5..], bitCount, block[4]);
    }

    /// <summary>The bytes of the filter's bits.</summary>
    public int ByteCount => _bits.Length;

    /// <summary>False when the key with this hash is definitely not in the table; true when it may be.</summary>
    public bool MayContain(ulong hash)
    {
        uint h1 = (uint)hash, h2 = (uint)(hash >> 32) | 1;
        for (int i = 0; i < _probes; i++)
        {
            uint bit = (uint)((h1 + ((ulong)i * h2)) % _bitCount);
            if ((_bits[bit >> 3] & (1 << (int)(bit & 7))) == 0)
                return false;
        }

        return true;
    }
}
