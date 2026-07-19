using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace AutoMateApi;

public static class AgreementWebhookContract
{
    public static AgreementWebhookEvent ParseAdobe(string payload)
    {
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;
        var agreement = root.TryGetProperty("agreement", out var value) && value.ValueKind == JsonValueKind.Object ? value : root;
        var documentId = First(agreement, "id", "agreementId", "agreement_id");
        var status = First(agreement, "status", "agreementStatus");
        var eventKey = First(root, "webhookNotificationId", "eventId");
        return new(documentId, status, eventKey);
    }

    public static AgreementWebhookEvent ParseDocuSign(string payload)
    {
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;
        var documentId = Find(root, "envelopeId", "envelope_id");
        var status = Find(root, "status", "envelopeStatus");
        if (string.IsNullOrWhiteSpace(status)) status = MapDocuSignEvent(First(root, "event", "eventType"));
        return new(documentId, status, "");
    }

    public static string MapDocuSignEvent(string eventName) => (eventName ?? "").Trim().ToLowerInvariant() switch
    {
        "envelope-sent" => "sent",
        "envelope-delivered" => "delivered",
        "envelope-completed" => "completed",
        "envelope-declined" => "declined",
        "envelope-voided" => "voided",
        var value when value.StartsWith("envelope-", StringComparison.Ordinal) => value["envelope-".Length..],
        var value => value
    };

    public static bool VerifyDocuSignHmac(string secret, ReadOnlySpan<byte> payload, string presented)
    {
        if (string.IsNullOrWhiteSpace(secret) || string.IsNullOrWhiteSpace(presented)) return false;
        var expected = Convert.ToBase64String(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload));
        var expectedBytes = Encoding.ASCII.GetBytes(expected);
        var presentedBytes = Encoding.ASCII.GetBytes(presented.Trim());
        return expectedBytes.Length == presentedBytes.Length && CryptographicOperations.FixedTimeEquals(expectedBytes, presentedBytes);
    }

    public static bool VerifyDocuSignHmac(string secret, ReadOnlySpan<byte> payload, IEnumerable<string> presented)
    {
        foreach (var signature in presented)
            if (VerifyDocuSignHmac(secret, payload, signature)) return true;
        return false;
    }

    private static string First(JsonElement element, params string[] names)
    {
        if (element.ValueKind != JsonValueKind.Object) return "";
        foreach (var name in names)
            if (element.TryGetProperty(name, out var value) && value.ValueKind is JsonValueKind.String or JsonValueKind.Number)
                return value.ToString();
        return "";
    }

    private static string Find(JsonElement element, params string[] names)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var direct = First(element, names);
            if (!string.IsNullOrWhiteSpace(direct)) return direct;
            foreach (var property in element.EnumerateObject())
            {
                var found = Find(property.Value, names);
                if (!string.IsNullOrWhiteSpace(found)) return found;
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
            foreach (var item in element.EnumerateArray())
            {
                var found = Find(item, names);
                if (!string.IsNullOrWhiteSpace(found)) return found;
            }
        return "";
    }
}

public sealed record AgreementWebhookEvent(string DocumentId, string Status, string EventKey);
