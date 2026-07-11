using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

public sealed record PropertyFeaturesResult(
    string Status, string AddressFingerprint, DateTimeOffset RetrievedAt, string Error,
    int? PropertyId, string FormattedAddress, double? Latitude, double? Longitude,
    string PropertyType, string PropertySubType, int? Bedrooms, int? Bathrooms, int? CarSpaces,
    double? LandArea, double? FloorArea, string YearBuilt, string DecadeBuilt,
    string RoofMaterial, string WallMaterial, string TotalFloors, string LegalDescription,
    string CouncilArea, string Postcode);

public static class PropertyFeaturesLookupService
{
    private static readonly HttpClient Http = CreateHttpClient();
    private static readonly ConcurrentDictionary<string, PropertyFeaturesResult> Cache = new();

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("3D-AutoMate/1.0");
        client.DefaultRequestHeaders.Accept.ParseAdd("application/json,text/html;q=0.9,*/*;q=0.8");
        return client;
    }

    public static async Task<PropertyFeaturesResult> LookupAsync(string address, bool forceRefresh = false)
    {
        var fingerprint = Fingerprint(address);
        if (!forceRefresh && Cache.TryGetValue(fingerprint, out var cached) && cached.Status == "available") return cached;
        try
        {
            var match = await StructuredAddressResolver.ResolveAsync(address);
            if (match == null) return Failed(fingerprint, "PropertyValue did not return an exact street-address match.");
            var html = await Http.GetStringAsync(match.PageUrl);
            const string marker = "window.REDUX_DATA=";
            var start = html.IndexOf(marker, StringComparison.Ordinal);
            if (start < 0) return Failed(fingerprint, "PropertyValue structured property data was not present.");
            start += marker.Length;
            var end = html.IndexOf("</script>", start, StringComparison.OrdinalIgnoreCase);
            if (end < 0) return Failed(fingerprint, "PropertyValue structured property data was incomplete.");
            var json = html.Substring(start, end - start).Trim().TrimEnd(';');
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("PropertyDetails", out var details)) return Failed(fingerprint, "PropertyDetails was not present.");
            var core = Child(details, "core"); var additional = Child(details, "additional");
            var location = Child(details, "location"); var rating = Child(details, "ratingValuation");
            var totalFloors = "";
            var features = Child(Child(details, "features"), "featureAttributes");
            if (features.ValueKind == JsonValueKind.Array)
                foreach (var feature in features.EnumerateArray())
                    if (Text(feature, "name").Equals("Total Floors In Building", StringComparison.OrdinalIgnoreCase)) totalFloors = Text(feature, "value");
            var legal = "";
            var legalValues = Child(rating, "legalDescriptions");
            if (legalValues.ValueKind == JsonValueKind.Array) legal = string.Join("; ", legalValues.EnumerateArray().Select(v => v.GetString()).Where(v => !string.IsNullOrWhiteSpace(v)));
            var result = new PropertyFeaturesResult("available", fingerprint, DateTimeOffset.UtcNow, "",
                Integer(details, "propertyId"), Text(location, "locallyFormattedAddress"), Number(location, "latitude"), Number(location, "longitude"),
                Text(core, "propertyType"), Text(core, "propertySubType"), Integer(core, "beds"), Integer(core, "baths"), Integer(core, "carSpaces"),
                Number(core, "landArea"), Number(additional, "floorArea"), Text(additional, "yearBuilt"), Text(additional, "decadeBuilt"),
                Text(additional, "roofMaterial"), Text(additional, "wallMaterial"), totalFloors, legal,
                Text(location, "councilArea"), Text(Child(location, "postcode"), "name"));
            Cache[fingerprint] = result;
            return result;
        }
        catch (Exception ex) { return Failed(fingerprint, ex.Message); }
    }

    private static PropertyFeaturesResult Failed(string fingerprint, string error) => new("unavailable", fingerprint, DateTimeOffset.UtcNow, error,
        null, "", null, null, "", "", null, null, null, null, null, "", "", "", "", "", "", "", "");
    private static string Fingerprint(string address) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Regex.Replace((address ?? "").Trim().ToUpperInvariant(), "\\s+", " ")))).ToLowerInvariant();
    private static JsonElement Child(JsonElement value, string name) => value.ValueKind == JsonValueKind.Object && value.TryGetProperty(name, out var child) ? child : default;
    private static string Text(JsonElement value, string name) { var child = Child(value, name); return child.ValueKind == JsonValueKind.String ? child.GetString() ?? "" : child.ValueKind == JsonValueKind.Number ? child.ToString() : ""; }
    private static int? Integer(JsonElement value, string name) { var child = Child(value, name); return child.ValueKind == JsonValueKind.Number && child.TryGetInt32(out var result) ? result : null; }
    private static double? Number(JsonElement value, string name) { var child = Child(value, name); return child.ValueKind == JsonValueKind.Number && child.TryGetDouble(out var result) ? result : null; }
}
