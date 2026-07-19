using System.Text.Json;
using System.Text.RegularExpressions;
using System.Security.Cryptography;
using System.Text;

public sealed record StructuredAddressMatch(int? PropertyId, string PageUrl, string MatchedAddress);

public static class StructuredAddressResolver
{
    private static readonly HttpClient Http = CreateHttpClient();

    public static async Task<StructuredAddressMatch?> ResolveAsync(string address)
    {
        if (string.IsNullOrWhiteSpace(address)) return null;
        var street = NormalizeStreet(address.Split(',')[0]);
        if (street.Length == 0) return null;
        var originalTokens = Tokens(address);

        foreach (var candidate in BuildCandidates(address))
        {
            var url = "https://www.propertyvalue.co.nz/api/public/clapi/suggestions?q=" + Uri.EscapeDataString(candidate) + "&suggestionTypes=address&limit=5";
            using var response = await Http.GetAsync(url);
            if (!response.IsSuccessStatusCode) continue;
            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            if (!doc.RootElement.TryGetProperty("suggestions", out var suggestions) || suggestions.ValueKind != JsonValueKind.Array) continue;

            var matches = new List<(int Score, StructuredAddressMatch Match)>();
            foreach (var suggestion in suggestions.EnumerateArray())
            {
                var label = Text(suggestion, "suggestion");
                var suggestedStreet = NormalizeStreet(label.Split(',')[0]);
                if (!suggestedStreet.Equals(street, StringComparison.OrdinalIgnoreCase)) continue;
                if (!IsAcceptableMatch(address, label)) continue;
                var pageUrl = Text(suggestion, "url");
                if (pageUrl.Length == 0) continue;
                if (pageUrl.StartsWith('/')) pageUrl = "https://www.propertyvalue.co.nz" + pageUrl;
                var suggestionTokens = Tokens(label);
                var score = originalTokens.Count(token => suggestionTokens.Contains(token));
                matches.Add((score, new StructuredAddressMatch(Integer(suggestion, "propertyId"), pageUrl, label)));
            }
            var best = matches.OrderByDescending(item => item.Score).FirstOrDefault();
            if (best.Match != null) return best.Match;
        }
        return null;
    }

    public static bool IsAcceptableMatch(string requestedAddress, string candidateAddress)
    {
        var requested = Parts(requestedAddress); var candidate = Parts(candidateAddress);
        if (requested.Street.Length == 0 || !requested.Street.Equals(candidate.Street, StringComparison.OrdinalIgnoreCase)) return false;
        if (requested.Postcode.Length > 0 && !requested.Postcode.Equals(candidate.Postcode, StringComparison.OrdinalIgnoreCase)) return false;
        if (requested.Localities.Count > 0 && !requested.Localities.Overlaps(candidate.Localities)) return false;
        return true;
    }

    private static IEnumerable<string> BuildCandidates(string address)
    {
        var normalized = Regex.Replace(address.Trim(), "\\s+", " ");
        var withoutPostcode = Regex.Replace(normalized, @"(?:,?\s*)\b\d{4}\b\s*$", "").Trim().TrimEnd(',');
        var parts = withoutPostcode.Split(',').Select(part => part.Trim()).Where(part => part.Length > 0).ToArray();
        var street = parts.Length > 0 ? parts[0] : withoutPostcode;
        var streetAndSuburb = parts.Length > 1 ? street + ", " + parts[1] : withoutPostcode;
        return new[] { normalized, withoutPostcode, streetAndSuburb, street }.Where(value => value.Length > 0).Distinct(StringComparer.OrdinalIgnoreCase);
    }

    private static string NormalizeStreet(string value) => Regex.Replace(value.Trim().ToUpperInvariant(), "[^A-Z0-9]", "");
    private static AddressParts Parts(string value)
    {
        var pieces=(value??"").Split(',').Select(x=>x.Trim()).Where(x=>x.Length>0).ToArray();
        var postcode=Regex.Match(value??"", @"\b\d{4}\b").Value;
        var localities=new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach(var piece in pieces.Skip(1))
        {
            var normalized=Regex.Replace(piece, @"\b\d{4}\b", "");
            foreach(var token in Tokens(normalized)) localities.Add(token);
        }
        return new AddressParts(pieces.Length==0?"":NormalizeStreet(pieces[0]),postcode,localities);
    }
    private static HashSet<string> Tokens(string value) => Regex.Matches(value.ToUpperInvariant(), "[A-Z0-9]+")
        .Select(match => match.Value).Where(token => token.Length > 1).ToHashSet(StringComparer.OrdinalIgnoreCase);
    private static string Text(JsonElement value, string name) => value.TryGetProperty(name, out var child) && child.ValueKind == JsonValueKind.String ? child.GetString() ?? "" : "";
    private static int? Integer(JsonElement value, string name) => value.TryGetProperty(name, out var child) && child.TryGetInt32(out var result) ? result : null;
    public static string Fingerprint(string address)
    {
        var value = (address ?? "").Trim().ToUpperInvariant();
        value = Regex.Replace(value, @"(?:,?\s*)\b\d{4}\b\s*$", "");
        value = Regex.Replace(value, @"[^A-Z0-9]", "");
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    }

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("3D-AutoMate/1.0");
        client.DefaultRequestHeaders.Accept.ParseAdd("application/json,text/html;q=0.9,*/*;q=0.8");
        return client;
    }
    private sealed record AddressParts(string Street,string Postcode,HashSet<string> Localities);
}
