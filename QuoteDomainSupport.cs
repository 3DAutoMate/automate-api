using System.Security.Cryptography;
using System.Text;

namespace AutoMateApi;

/// <summary>Pure quote-domain rules. THREED catalogue rows remain the pricing authority.</summary>
public static class QuoteCalculator
{
    public static QuoteDraft Calculate(QuoteCalculationInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (input.TenantId == Guid.Empty) throw new ArgumentException("Tenant ID is required.");
        if (input.RequestId == Guid.Empty) throw new ArgumentException("Quote request ID is required.");
        if (input.CatalogueVersion < 1) throw new ArgumentException("An active THREED catalogue version is required.");
        if (string.IsNullOrWhiteSpace(input.Address)) throw new ArgumentException("Property address is required.");

        var candidates = input.Candidates ?? [];
        var duplicate = candidates.GroupBy(x => x.CatalogueItemId).FirstOrDefault(x => x.Key == Guid.Empty || x.Count() > 1);
        if (duplicate is not null) throw new ArgumentException("Each candidate must reference one unique THREED catalogue item.");

        var lines = new List<QuoteDraftLine>();
        var attention = new List<QuoteAttention>();
        foreach (var item in candidates.OrderBy(x => x.DisplayOrder).ThenBy(x => x.Label, StringComparer.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(item.Label)) throw new ArgumentException("Every catalogue candidate requires a label.");
            if (item.UnitPrice < 0m) throw new ArgumentException("Quote prices cannot be negative.");
            if (item.Quantity <= 0m) throw new ArgumentException("Quote quantities must be positive.");
            var amount = Money(item.Quantity * item.UnitPrice);
            switch (item.Decision)
            {
                case QuoteCandidateDecision.Confirmed:
                    lines.Add(new(item.CatalogueItemId, item.Label.Trim(), item.Quantity, Money(item.UnitPrice), amount,
                        item.IsBaseItem, item.Source, item.Evidence));
                    break;
                case QuoteCandidateDecision.Review:
                    attention.Add(new("modifier_review_required", item.Label.Trim(),
                        string.IsNullOrWhiteSpace(item.Evidence) ? "Confirm this suggested catalogue item before sending the quote." : item.Evidence.Trim(),
                        QuoteAttentionSeverity.Warning, item.CatalogueItemId));
                    break;
                case QuoteCandidateDecision.Excluded:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(item.Decision));
            }
        }

        if (!lines.Any(x => x.IsBaseItem))
            attention.Add(new("base_item_required", "Select a base service",
                "A quote cannot be approved until one active THREED base service is confirmed.", QuoteAttentionSeverity.Blocking, null));
        if (lines.Count(x => x.IsBaseItem) > 1)
            attention.Add(new("multiple_base_items", "Review base services",
                "More than one base service is selected. Confirm that this is intentional before sending.", QuoteAttentionSeverity.Blocking, null));
        if (!input.PropertyMatchConfirmed)
            attention.Add(new("property_match_required", "Review property match",
                "The submitted address has not been matched to the correct property with sufficient confidence.", QuoteAttentionSeverity.Blocking, null));

        return new(input.TenantId, input.RequestId, input.CatalogueVersion, input.Address.Trim(), lines,
            Money(lines.Sum(x => x.Amount)), attention, attention.All(x => x.Severity != QuoteAttentionSeverity.Blocking));
    }

    private static decimal Money(decimal value) => Math.Round(value, 2, MidpointRounding.AwayFromZero);
}

/// <summary>Creates purpose-bound public quote tokens. Persist only TokenHash.</summary>
public static class QuotePublicToken
{
    public const int MinimumSecretBytes = 32;

    public static QuoteBearerToken Create(string purpose, string pepper, int byteCount = MinimumSecretBytes)
    {
        if (byteCount < MinimumSecretBytes) throw new ArgumentOutOfRangeException(nameof(byteCount), "Tokens require at least 256 bits.");
        Validate(purpose, pepper);
        var secret = Base64Url(RandomNumberGenerator.GetBytes(byteCount));
        return new(secret, Hash(secret, purpose, pepper));
    }

    public static string Hash(string secret, string purpose, string pepper)
    {
        if (string.IsNullOrWhiteSpace(secret)) throw new ArgumentException("Token secret is required.", nameof(secret));
        Validate(purpose, pepper);
        var data = Encoding.UTF8.GetBytes($"automate-public-quote:v1:{purpose.Trim().ToLowerInvariant()}:{secret}");
        return Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(pepper), data)).ToLowerInvariant();
    }

    public static bool Verify(string secret, string expectedHash, string purpose, string pepper)
    {
        if (string.IsNullOrWhiteSpace(expectedHash) || expectedHash.Length != 64) return false;
        var actual = Hash(secret, purpose, pepper);
        return CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(actual), Encoding.ASCII.GetBytes(expectedHash.ToLowerInvariant()));
    }

    private static void Validate(string purpose, string pepper)
    {
        if (string.IsNullOrWhiteSpace(purpose)) throw new ArgumentException("Token purpose is required.", nameof(purpose));
        if (string.IsNullOrWhiteSpace(pepper)) throw new ArgumentException("A server-side token pepper is required.", nameof(pepper));
    }

    private static string Base64Url(byte[] value) => Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}

public sealed record QuoteCalculationInput(Guid TenantId, Guid RequestId, int CatalogueVersion, string Address,
    bool PropertyMatchConfirmed, IReadOnlyList<QuoteCandidate> Candidates);
public sealed record QuoteCandidate(Guid CatalogueItemId, string Label, decimal Quantity, decimal UnitPrice,
    bool IsBaseItem, QuoteCandidateDecision Decision, QuoteEvidenceSource Source, string Evidence = "", int DisplayOrder = 0);
public sealed record QuoteDraft(Guid TenantId, Guid RequestId, int CatalogueVersion, string Address,
    IReadOnlyList<QuoteDraftLine> Lines, decimal Total, IReadOnlyList<QuoteAttention> Attention, bool ReviewMayProceed);
public sealed record QuoteDraftLine(Guid CatalogueItemId, string Label, decimal Quantity, decimal UnitPrice,
    decimal Amount, bool IsBaseItem, QuoteEvidenceSource Source, string Evidence);
public sealed record QuoteAttention(string Code, string Title, string Detail, QuoteAttentionSeverity Severity, Guid? CatalogueItemId);
public sealed record QuoteBearerToken(string Secret, string TokenHash);
public enum QuoteCandidateDecision { Confirmed, Review, Excluded }
public enum QuoteEvidenceSource { ThreedCatalogue, UserSelection, PropertyStructuredData, PropertyImagery, FallbackResearch }
public enum QuoteAttentionSeverity { Warning, Blocking }
