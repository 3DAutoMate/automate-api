using System.Globalization;
using System.Text.Json;

namespace AutoMateApi;

public sealed record InvoiceAdjustmentLine(string Description, decimal Amount);
public sealed record InvoiceAdjustmentChange(string Field, string OldValue, string NewValue);

public static class InvoiceAdjustmentLineSupport
{
    public static IReadOnlyList<InvoiceAdjustmentLine> Build(
        IReadOnlyList<InvoiceAdjustmentChange> changes,
        decimal remainingDifference,
        string originalInvoiceNumber)
    {
        var amount = Math.Round(remainingDifference, 2);
        if (amount <= 0m) return [];
        var reference = string.IsNullOrWhiteSpace(originalInvoiceNumber) ? "the original invoice" : $"original invoice {originalInvoiceNumber.Trim()}";
        var invoiceLineChange = changes.LastOrDefault(change => string.Equals(change.Field, "invoiceLines", StringComparison.OrdinalIgnoreCase));
        var before = Parse(invoiceLineChange?.OldValue);
        var after = Parse(invoiceLineChange?.NewValue);
        var keys = before.Keys.Union(after.Keys, StringComparer.OrdinalIgnoreCase).ToArray();
        var deltas = new List<LineDelta>();
        foreach (var key in keys)
        {
            before.TryGetValue(key, out var oldLine);after.TryGetValue(key, out var newLine);
            var previous = oldLine?.Amount ?? 0m;var current = newLine?.Amount ?? 0m;var delta = Math.Round(current - previous, 2);
            if (Math.Abs(delta) <= 0.005m) continue;
            var description = newLine?.Description ?? oldLine?.Description ?? "THREED service";
            deltas.Add(new(description, delta, oldLine is null ? "added" : newLine is null ? "removed" : "changed"));
        }
        var positive = deltas.Where(delta => delta.Difference > 0m).ToArray();
        if (positive.Length > 0 && deltas.All(delta => delta.Difference > 0m) && Math.Abs(positive.Sum(delta => delta.Difference) - amount) <= 0.01m)
            return positive.Select(delta => new InvoiceAdjustmentLine($"Service changed from {reference}: {delta.Description}", delta.Difference)).ToArray();
        if (deltas.Count > 0)
        {
            var detail = string.Join("; ", deltas.Select(delta => $"{delta.Description} ({delta.Kind})"));
            return [new InvoiceAdjustmentLine($"Services changed from {reference}: {detail}", amount)];
        }
        var serviceNames = changes
            .Where(change => change.Field is "primaryService" or "additionalService1" or "additionalService2")
            .Select(change => change.NewValue)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var fallback = serviceNames.Length > 0 ? string.Join("; ", serviceNames) : "THREED invoice total adjustment";
        return [new InvoiceAdjustmentLine($"Services changed from {reference}: {fallback}", amount)];
    }

    public static IReadOnlyList<string> ChangedDescriptions(IReadOnlyList<InvoiceAdjustmentChange> changes)
    {
        var lineChange = changes.LastOrDefault(change => string.Equals(change.Field, "invoiceLines", StringComparison.OrdinalIgnoreCase));
        var before = Parse(lineChange?.OldValue);var after = Parse(lineChange?.NewValue);
        var result = new List<string>();
        foreach (var key in before.Keys.Union(after.Keys, StringComparer.OrdinalIgnoreCase))
        {
            before.TryGetValue(key, out var oldLine);after.TryGetValue(key, out var newLine);
            if (Math.Abs((newLine?.Amount ?? 0m) - (oldLine?.Amount ?? 0m)) <= 0.005m) continue;
            result.Add(newLine?.Description ?? oldLine?.Description ?? "THREED service");
        }
        return result.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static Dictionary<string, SnapshotLine> Parse(string? json)
    {
        var result = new Dictionary<string, SnapshotLine>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(json)) return result;
        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Array) return result;
            foreach (var element in document.RootElement.EnumerateArray())
            {
                var description = Read(element, "description").Trim();if (description.Length == 0) continue;
                var quantity = Decimal(Read(element, "quantity"), 1m);if (quantity <= 0m) quantity = 1m;
                var unitPrice = Decimal(Read(element, "unitPrice"), 0m);
                var key = Normalize(description);var amount = Math.Round(quantity * unitPrice, 2);
                if (result.TryGetValue(key, out var existing)) result[key] = existing with { Amount = existing.Amount + amount };
                else result[key] = new(description, amount);
            }
        }
        catch (JsonException) { }
        return result;
    }

    private static string Read(JsonElement element, string property) => element.TryGetProperty(property, out var value) ? (value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : value.ToString()) : "";
    private static decimal Decimal(string value, decimal fallback) => decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;
    private static string Normalize(string value) => string.Join(' ', value.Trim().ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries));
    private sealed record SnapshotLine(string Description, decimal Amount);
    private sealed record LineDelta(string Description, decimal Difference, string Kind);
}
