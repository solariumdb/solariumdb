using System.Text;

namespace Solarium.Tests;

/// <summary>UTF-8 conversions between strings and the byte keys and values the database stores.</summary>
public static class Text
{
    /// <summary>Encodes <paramref name="text"/> as UTF-8 bytes.</summary>
    public static byte[] B(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>Decodes UTF-8 bytes to a string; returns null when <paramref name="bytes"/> is null.</summary>
    public static string? S(byte[]? bytes) => bytes is null ? null : Encoding.UTF8.GetString(bytes);

    /// <summary>Decodes a span of UTF-8 bytes to a string.</summary>
    public static string S(ReadOnlySpan<byte> bytes) => Encoding.UTF8.GetString(bytes);
}
