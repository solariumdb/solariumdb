namespace Solarium.Tests;

/// <summary>Batched reads in place with a lambda for a visitor.</summary>
internal static class DelegateVisits
{
    /// <summary><see cref="SolariumDb.MultiGet{TVisitor}"/> with a visitor that calls <paramref name="visit"/>.</summary>
    public static void MultiGet(this SolariumDb db, IReadOnlyList<byte[]> keys, DelegateVisitor.Visit visit, ColumnFamily? family = null, ReadOptions? options = null)
    {
        DelegateVisitor visitor = new(visit);
        db.MultiGet(keys, ref visitor, family, options);
    }
}
