namespace Solarium.Engine;

/// <summary>One write as the WAL stores it: the family is its id.</summary>
internal readonly record struct WalOperation(OperationKind Kind, int FamilyId, byte[] Key, byte[]? Value);
