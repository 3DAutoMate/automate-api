using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AutoMateApi;

public sealed record QuotePlaceAddress(
    string CanonicalAddress,
    string PlaceId,
    string StreetNumber,
    string Route,
    string Locality,
    string Region,
    string Postcode,
    string Country,
    double? Latitude,
    double? Longitude,
    string CoordinateSource,
    bool Confirmed);

public sealed record QuoteAddressPrediction(string PlaceId, string Address);

public static class RailwayPropertyMediaService
{
    private const int DisplayWidth = 640;
    private const int DisplayHeight = 420;
    private const int QuoteZoom = 19;
    private static readonly HttpClient Google = CreateClient(TimeSpan.FromSeconds(25));
    private static readonly HttpClient OpenAi = CreateClient(TimeSpan.FromSeconds(120));
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task<object> PredictAsync(string query, string? sessionToken, IConfiguration configuration, CancellationToken cancellationToken)
    {
        query = Normalize(query);
        if (query.Length < 3) return new { predictions = Array.Empty<object>(), source = "Google Places", warning = "" };
        if (query.Length > 180) throw new InvalidOperationException("The address search is too long.");
        var key = RequiredGoogleKey(configuration);
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://places.googleapis.com/v1/places:autocomplete");
        request.Headers.TryAddWithoutValidation("X-Goog-Api-Key", key);
        request.Headers.TryAddWithoutValidation("X-Goog-FieldMask", "suggestions.placePrediction.placeId,suggestions.placePrediction.text.text");
        request.Content = JsonContent(new
        {
            input = query,
            includedRegionCodes = new[] { "nz" },
            languageCode = "en",
            sessionToken = ValidSessionToken(sessionToken)
        });
        using var response = await Google.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"Google address prediction is temporarily unavailable (HTTP {(int)response.StatusCode}).");
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        var output = new List<QuoteAddressPrediction>();
        if (document.RootElement.TryGetProperty("suggestions", out var suggestions) && suggestions.ValueKind == JsonValueKind.Array)
        {
            foreach (var suggestion in suggestions.EnumerateArray())
            {
                if (!suggestion.TryGetProperty("placePrediction", out var prediction)) continue;
                var placeId = Text(prediction, "placeId");
                var address = prediction.TryGetProperty("text", out var text) ? Text(text, "text") : "";
                if (string.IsNullOrWhiteSpace(placeId) || string.IsNullOrWhiteSpace(address)) continue;
                output.Add(new QuoteAddressPrediction(placeId, address));
                if (output.Count == 6) break;
            }
        }
        return new { predictions = output, source = "Google Places", warning = "" };
    }

    public static async Task<object> EnrichAsync(QuoteAddressEnrichmentRequest request, IConfiguration configuration, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.CanonicalAddress)) throw new InvalidOperationException("Confirm a property address first.");
        var selectedAddress = await ResolveAddressAsync(request, configuration, cancellationToken);
        var property = await PropertyFeaturesLookupService.LookupAsync(selectedAddress.CanonicalAddress);
        var propertyStatus = ClassifyPropertyValue(property);
        var warnings = string.IsNullOrWhiteSpace(property.Error) ? Array.Empty<string>() : new[] { FriendlyPropertyValueMessage(propertyStatus) };

        var latitude = selectedAddress.Latitude ?? property.Latitude ?? request.Latitude;
        var longitude = selectedAddress.Longitude ?? property.Longitude ?? request.Longitude;
        var coordinateSource = selectedAddress.Latitude.HasValue && selectedAddress.Longitude.HasValue
            ? "Google Places"
            : property.Latitude.HasValue && property.Longitude.HasValue ? "PropertyValue" : "Provided address";
        var address = selectedAddress with { Latitude = latitude, Longitude = longitude, CoordinateSource = coordinateSource };
        var imagery = await LoadQuoteImageryAsync(address, request.VisualReviewConfirmed, configuration, cancellationToken);
        var suggestions = Suggestions(imagery.VisualReview);
        return new
        {
            address,
            property = new
            {
                status = propertyStatus,
                source = "PropertyValue",
                message = FriendlyPropertyValueMessage(propertyStatus),
                fields = new
                {
                    property.PropertyId,
                    property.FormattedAddress,
                    property.PropertyType,
                    property.PropertySubType,
                    property.Bedrooms,
                    property.Bathrooms,
                    property.CarSpaces,
                    property.LandArea,
                    property.FloorArea,
                    property.YearBuilt,
                    property.DecadeBuilt,
                    property.RoofMaterial,
                    property.WallMaterial,
                    property.TotalFloors,
                    property.LegalDescription,
                    property.CouncilArea,
                    property.Postcode
                },
                warnings
            },
            imagery = new
            {
                imagery.Status,
                imagery.Message,
                roadmapImage = imagery.RoadmapImage,
                satelliteImage = imagery.SatelliteImage,
                streetViewImage = imagery.StreetViewImage,
                roadmapUrl = imagery.RoadmapUrl,
                satelliteUrl = imagery.SatelliteUrl,
                streetViewUrl = imagery.StreetViewUrl,
                imagery.AiReviewStatus,
                visualReview = imagery.VisualReview
            },
            suggestions,
            manualReviewAvailable = true
        };
    }

    public static Task<QuotePlaceAddress> ResolvePlaceAsync(string canonicalAddress, string? placeId, string? sessionToken, IConfiguration configuration, CancellationToken cancellationToken)
        => ResolveAddressAsync(new QuoteAddressEnrichmentRequest(Guid.Empty, canonicalAddress, placeId, null, null, sessionToken, false), configuration, cancellationToken);

    public static async Task<object> LoadJobMediaAsync(string address, IConfiguration configuration, CancellationToken cancellationToken)
    {
        address = Normalize(address);
        if (string.IsNullOrWhiteSpace(address)) return new { address, status = "unavailable", message = "No property address is available.", mapImage = "", streetViewImage = "", mapsUrl = "" };
        var key = RequiredGoogleKey(configuration);
        var escapedAddress = Uri.EscapeDataString(address);
        var mapTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escapedAddress}&zoom=18&size={DisplayWidth}x360&scale=1&maptype=roadmap&markers=color:0x176B87%7C{escapedAddress}&key={Uri.EscapeDataString(key)}", cancellationToken);
        var streetTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/streetview?size={DisplayWidth}x360&location={escapedAddress}&fov=90&pitch=0&source=outdoor&key={Uri.EscapeDataString(key)}", cancellationToken);
        await Task.WhenAll(mapTask, streetTask);
        var map = await mapTask;
        var street = await streetTask;
        var available = !string.IsNullOrWhiteSpace(map) || !string.IsNullOrWhiteSpace(street);
        return new
        {
            address,
            status = available ? "available" : "unavailable",
            message = available ? "" : "Google imagery is unavailable for this address.",
            mapImage = map,
            streetViewImage = street,
            mapsUrl = "https://www.google.com/maps/search/?api=1&query=" + escapedAddress
        };
    }

    public static async Task<object> CheckCapabilitiesAsync(IConfiguration configuration, CancellationToken cancellationToken)
    {
        var googleKey = RequiredGoogleKey(configuration);
        var place = await ProbePlacesAsync(googleKey, cancellationToken);
        var map = await ProbeGetAsync($"https://maps.googleapis.com/maps/api/staticmap?center=-36.8485,174.7633&zoom=15&size=64x64&key={Uri.EscapeDataString(googleKey)}", cancellationToken);
        var street = await ProbeGetAsync($"https://maps.googleapis.com/maps/api/streetview?size=64x64&location=-36.8485,174.7633&key={Uri.EscapeDataString(googleKey)}", cancellationToken);
        var routes = await ProbeRoutesAsync(googleKey, cancellationToken);
        return new
        {
            googleConfigured = true,
            openAiConfigured = !string.IsNullOrWhiteSpace(configuration["OPENAI_API_KEY"]),
            capabilities = new
            {
                places = new { ok = place == 200, httpStatus = place },
                mapsStatic = new { ok = map == 200, httpStatus = map },
                streetViewStatic = new { ok = street == 200, httpStatus = street },
                routes = new { ok = routes == 200, httpStatus = routes }
            },
            ready = place == 200 && map == 200 && street == 200 && routes == 200
        };
    }

    private static async Task<QuotePlaceAddress> ResolveAddressAsync(QuoteAddressEnrichmentRequest request, IConfiguration configuration, CancellationToken cancellationToken)
    {
        var supplied = Normalize(request.CanonicalAddress);
        if (string.IsNullOrWhiteSpace(request.PlaceId) || request.PlaceId.StartsWith("pv:", StringComparison.OrdinalIgnoreCase))
            return new QuotePlaceAddress(supplied, request.PlaceId ?? "", StreetNumberFromText(supplied), "", "", "", "", "New Zealand", request.Latitude, request.Longitude, "Provided address", true);

        var key = RequiredGoogleKey(configuration);
        var placeId = request.PlaceId.Trim();
        using var googleRequest = new HttpRequestMessage(HttpMethod.Get, $"https://places.googleapis.com/v1/places/{Uri.EscapeDataString(placeId)}?languageCode=en&sessionToken={Uri.EscapeDataString(ValidSessionToken(request.SessionToken))}");
        googleRequest.Headers.TryAddWithoutValidation("X-Goog-Api-Key", key);
        googleRequest.Headers.TryAddWithoutValidation("X-Goog-FieldMask", "id,formattedAddress,addressComponents,location");
        using var response = await Google.SendAsync(googleRequest, cancellationToken);
        if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"Google could not confirm the selected address (HTTP {(int)response.StatusCode}). Select it again.");
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        var root = document.RootElement;
        var canonical = Normalize(Text(root, "formattedAddress"));
        var components = ReadComponents(root);
        var latitude = root.TryGetProperty("location", out var location) ? Number(location, "latitude") : null;
        var longitude = root.TryGetProperty("location", out location) ? Number(location, "longitude") : null;
        if (string.IsNullOrWhiteSpace(canonical) || !latitude.HasValue || !longitude.HasValue)
            throw new InvalidOperationException("Google returned incomplete details for the selected address. Select the address again or use manual entry.");
        return new QuotePlaceAddress(canonical, Text(root, "id"), components.GetValueOrDefault("street_number", StreetNumberFromText(canonical)), components.GetValueOrDefault("route", ""), components.GetValueOrDefault("locality", components.GetValueOrDefault("postal_town", "")), components.GetValueOrDefault("administrative_area_level_1", ""), components.GetValueOrDefault("postal_code", ""), components.GetValueOrDefault("country", "New Zealand"), latitude, longitude, "Google Places", true);
    }

    private static async Task<QuoteImagery> LoadQuoteImageryAsync(QuotePlaceAddress address, bool runAi, IConfiguration configuration, CancellationToken cancellationToken)
    {
        if (!address.Latitude.HasValue || !address.Longitude.HasValue)
            return new QuoteImagery("unavailable", "Coordinates are unavailable. Continue with manual property review.", "", "", "", "", "", "", "unavailable", null);

        var key = RequiredGoogleKey(configuration);
        var coordinates = FormattableString.Invariant($"{address.Latitude.Value},{address.Longitude.Value}");
        var escapedCoordinates = Uri.EscapeDataString(coordinates);
        var escapedKey = Uri.EscapeDataString(key);
        var escapedAddress = Uri.EscapeDataString(address.CanonicalAddress);
        var roadmapTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escapedCoordinates}&zoom={QuoteZoom}&size={DisplayWidth}x{DisplayHeight}&scale=1&maptype=roadmap&markers=color:0x176B87%7C{escapedCoordinates}&key={escapedKey}", cancellationToken);
        var satelliteTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escapedCoordinates}&zoom={QuoteZoom}&size={DisplayWidth}x{DisplayHeight}&scale=1&maptype=satellite&markers=color:0x176B87%7C{escapedCoordinates}&key={escapedKey}", cancellationToken);
        var streetTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/streetview?size={DisplayWidth}x{DisplayHeight}&location={escapedCoordinates}&fov=90&pitch=0&source=outdoor&key={escapedKey}", cancellationToken);
        await Task.WhenAll(roadmapTask, satelliteTask, streetTask);
        var roadmap = await roadmapTask;
        var satellite = await satelliteTask;
        var street = await streetTask;
        var available = !string.IsNullOrWhiteSpace(roadmap) || !string.IsNullOrWhiteSpace(satellite) || !string.IsNullOrWhiteSpace(street);
        if (!available) return new QuoteImagery("unavailable", "Google imagery could not be retrieved for this address.", "", "", "", "", "", "", "unavailable", null);

        var roadmapUrl = "https://www.google.com/maps/search/?api=1&query=" + escapedAddress;
        var satelliteUrl = "https://www.google.com/maps/search/?api=1&query=" + escapedCoordinates;
        var streetUrl = "https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=" + escapedCoordinates;
        if (!runAi)
            return new QuoteImagery("available", "Imagery loaded for office review. Select AI review to analyse storeys, garages and outbuildings.", roadmap, satellite, street, roadmapUrl, satelliteUrl, streetUrl, "confirmation_required", null);

        var openAiKey = configuration["OPENAI_API_KEY"]?.Trim();
        if (string.IsNullOrWhiteSpace(openAiKey))
            return new QuoteImagery("available", "Imagery loaded, but AI visual review is not configured.", roadmap, satellite, street, roadmapUrl, satelliteUrl, streetUrl, "not_configured", null);
        try
        {
            var review = await ReviewAsync(address, roadmap, satellite, street, googleKey: key, openAiKey, cancellationToken);
            return new QuoteImagery("available", "Street View, roadmap outlines, hybrid and satellite zooms reviewed. Confirm every suggested modifier before saving or sending.", roadmap, satellite, street, roadmapUrl, satelliteUrl, streetUrl, "completed", review);
        }
        catch (Exception exception)
        {
            return new QuoteImagery("available", "Imagery loaded, but visual review failed: " + SafeError(exception.Message), roadmap, satellite, street, roadmapUrl, satelliteUrl, streetUrl, "failed", null);
        }
    }

    private static async Task<JsonObject> ReviewAsync(QuotePlaceAddress address, string roadmap, string satellite, string street, string googleKey, string openAiKey, CancellationToken cancellationToken)
    {
        var coordinates = FormattableString.Invariant($"{address.Latitude},{address.Longitude}");
        var escaped = Uri.EscapeDataString(coordinates);
        var key = Uri.EscapeDataString(googleKey);
        var detailRoadmapTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escaped}&zoom=20&size=640x640&scale=1&maptype=roadmap&key={key}", cancellationToken);
        var hybridTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escaped}&zoom=19&size=640x640&scale=1&maptype=hybrid&key={key}", cancellationToken);
        var satelliteContextTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escaped}&zoom=18&size=640x640&scale=1&maptype=satellite&key={key}", cancellationToken);
        var satelliteDetailTask = ImageDataAsync($"https://maps.googleapis.com/maps/api/staticmap?center={escaped}&zoom=20&size=640x640&scale=1&maptype=satellite&key={key}", cancellationToken);
        await Task.WhenAll(detailRoadmapTask, hybridTask, satelliteContextTask, satelliteDetailTask);

        var content = new JsonArray
        {
            new JsonObject
            {
                ["type"] = "input_text",
                ["text"] = $"Review the supplied views of {address.CanonicalAddress}. Verify the same parcel, then independently determine main-dwelling storeys, garages/carports and every separate outbuilding. Scan the whole parcel and compare roadmap footprints with satellite/hybrid roofs. Do not stop after the first detached footprint. Count only clearly visible evidence. Use null when obstruction, image age or parcel ambiguity prevents a reliable count. Zero means confidently none visible. Return short evidence and independent confidence for every finding."
            }
        };
        foreach (var image in new[] { street, roadmap, await detailRoadmapTask, await hybridTask, await satelliteContextTask, satellite, await satelliteDetailTask }.Where(value => !string.IsNullOrWhiteSpace(value)))
            content.Add(new JsonObject { ["type"] = "input_image", ["image_url"] = image, ["detail"] = "high" });

        var nullableCount = new JsonObject { ["type"] = new JsonArray("integer", "null"), ["minimum"] = 0, ["maximum"] = 20 };
        var confidence = new JsonObject { ["type"] = "string", ["enum"] = new JsonArray("low", "medium", "high") };
        var schema = new JsonObject
        {
            ["type"] = "object",
            ["additionalProperties"] = false,
            ["properties"] = new JsonObject
            {
                ["sameProperty"] = new JsonObject { ["type"] = "boolean" },
                ["storeys"] = new JsonObject { ["type"] = new JsonArray("integer", "null"), ["minimum"] = 1, ["maximum"] = 10 },
                ["storeysConfidence"] = confidence.DeepClone(), ["storeysEvidence"] = new JsonObject { ["type"] = "string" },
                ["garageCount"] = nullableCount.DeepClone(), ["garageConfidence"] = confidence.DeepClone(), ["garageEvidence"] = new JsonObject { ["type"] = "string" },
                ["outbuildingCount"] = nullableCount.DeepClone(), ["outbuildingConfidence"] = confidence.DeepClone(), ["outbuildingEvidence"] = new JsonObject { ["type"] = "string" },
                ["limitations"] = new JsonObject { ["type"] = "string" }
            },
            ["required"] = new JsonArray("sameProperty", "storeys", "storeysConfidence", "storeysEvidence", "garageCount", "garageConfidence", "garageEvidence", "outbuildingCount", "outbuildingConfidence", "outbuildingEvidence", "limitations")
        };
        var payload = new JsonObject
        {
            ["model"] = "gpt-5.6",
            ["reasoning"] = new JsonObject { ["effort"] = "low" },
            ["input"] = new JsonArray(new JsonObject { ["role"] = "user", ["content"] = content }),
            ["text"] = new JsonObject { ["format"] = new JsonObject { ["type"] = "json_schema", ["name"] = "property_visual_review_v3", ["strict"] = true, ["schema"] = schema } },
            ["max_output_tokens"] = 5000
        };
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.openai.com/v1/responses");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", openAiKey);
        request.Content = new StringContent(payload.ToJsonString(JsonOptions), Encoding.UTF8, "application/json");
        using var response = await OpenAi.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"AI review returned HTTP {(int)response.StatusCode}.");
        using var responseDocument = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        if (responseDocument.RootElement.TryGetProperty("output_text", out var outputText) && outputText.ValueKind == JsonValueKind.String)
            return JsonNode.Parse(outputText.GetString() ?? "{}") as JsonObject ?? throw new InvalidOperationException("AI review returned invalid structured output.");
        if (responseDocument.RootElement.TryGetProperty("output", out var output) && output.ValueKind == JsonValueKind.Array)
            foreach (var item in output.EnumerateArray())
                if (item.TryGetProperty("content", out var parts) && parts.ValueKind == JsonValueKind.Array)
                    foreach (var part in parts.EnumerateArray())
                        if (part.TryGetProperty("text", out var text) && text.ValueKind == JsonValueKind.String)
                            return JsonNode.Parse(text.GetString() ?? "{}") as JsonObject ?? throw new InvalidOperationException("AI review returned invalid structured output.");
        throw new InvalidOperationException("AI review did not return structured output.");
    }

    private static object[] Suggestions(JsonObject? review)
    {
        if (review is null || review["sameProperty"]?.GetValue<bool>() != true) return Array.Empty<object>();
        var result = new List<object>();
        AddSuggestion(result, review, "storeys", "storeysConfidence", "storeysEvidence", "storeys", false);
        AddSuggestion(result, review, "garageCount", "garageConfidence", "garageEvidence", "garage", true);
        AddSuggestion(result, review, "outbuildingCount", "outbuildingConfidence", "outbuildingEvidence", "outbuilding", true);
        return result.ToArray();
    }

    private static void AddSuggestion(List<object> output, JsonObject review, string valueKey, string confidenceKey, string evidenceKey, string kind, bool requirePositive)
    {
        if (review[valueKey] is not JsonValue raw || !raw.TryGetValue<int>(out var value) || (requirePositive && value <= 0) || value < 1) return;
        var confidence = review[confidenceKey]?.GetValue<string>() ?? "low";
        if (!confidence.Equals("high", StringComparison.OrdinalIgnoreCase)) return;
        output.Add(new { kind, value, confidence, evidence = review[evidenceKey]?.GetValue<string>() ?? "" });
    }

    private static string ClassifyPropertyValue(PropertyFeaturesResult property)
    {
        if (property.Status == "available")
        {
            var facts = new object?[] { property.PropertyId, property.PropertyType, property.Bedrooms, property.Bathrooms, property.LandArea, property.FloorArea, property.YearBuilt };
            return facts.Count(value => value is not null && !string.IsNullOrWhiteSpace(Convert.ToString(value, CultureInfo.InvariantCulture))) >= 3 ? "found" : "partial";
        }
        var error = property.Error ?? "";
        return error.Contains("exact street-address match", StringComparison.OrdinalIgnoreCase)
            || error.Contains("structured property data was not present", StringComparison.OrdinalIgnoreCase)
            || error.Contains("PropertyDetails was not present", StringComparison.OrdinalIgnoreCase)
            ? "not_found"
            : "unavailable";
    }

    private static string FriendlyPropertyValueMessage(string status) => status switch
    {
        "found" => "Property details found.",
        "partial" => "Partial property details found. Confirm missing facts manually.",
        "not_found" => "No matching PropertyValue record was found. You can continue with manual property details.",
        _ => "PropertyValue is temporarily unavailable. You can continue with manual property details."
    };

    private static async Task<string> ImageDataAsync(string url, CancellationToken cancellationToken)
    {
        try
        {
            using var response = await Google.GetAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode) return "";
            var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
            if (bytes.Length == 0 || bytes.Length > 4_000_000) return "";
            var mediaType = response.Content.Headers.ContentType?.MediaType ?? "image/jpeg";
            return $"data:{mediaType};base64,{Convert.ToBase64String(bytes)}";
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) { return ""; }
        catch (HttpRequestException) { return ""; }
    }

    private static Dictionary<string, string> ReadComponents(JsonElement root)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (!root.TryGetProperty("addressComponents", out var components) || components.ValueKind != JsonValueKind.Array) return result;
        foreach (var component in components.EnumerateArray())
        {
            var value = Text(component, "longText");
            if (!component.TryGetProperty("types", out var types) || types.ValueKind != JsonValueKind.Array) continue;
            foreach (var type in types.EnumerateArray()) if (type.ValueKind == JsonValueKind.String && !result.ContainsKey(type.GetString()!)) result[type.GetString()!] = value;
        }
        return result;
    }

    private static async Task<int> ProbePlacesAsync(string key, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://places.googleapis.com/v1/places:autocomplete");
        request.Headers.TryAddWithoutValidation("X-Goog-Api-Key", key);
        request.Headers.TryAddWithoutValidation("X-Goog-FieldMask", "suggestions.placePrediction.placeId");
        request.Content = JsonContent(new { input = "1 Queen Street Auckland", includedRegionCodes = new[] { "nz" } });
        using var response = await Google.SendAsync(request, cancellationToken);
        return (int)response.StatusCode;
    }

    private static async Task<int> ProbeGetAsync(string url, CancellationToken cancellationToken)
    {
        using var response = await Google.GetAsync(url, cancellationToken);
        return (int)response.StatusCode;
    }

    private static async Task<int> ProbeRoutesAsync(string key, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://routes.googleapis.com/directions/v2:computeRoutes");
        request.Headers.TryAddWithoutValidation("X-Goog-Api-Key", key);
        request.Headers.TryAddWithoutValidation("X-Goog-FieldMask", "routes.duration,routes.distanceMeters");
        request.Content = JsonContent(new { origin = new { address = "1 Queen Street, Auckland, New Zealand" }, destination = new { address = "2 Queen Street, Auckland, New Zealand" }, travelMode = "DRIVE" });
        using var response = await Google.SendAsync(request, cancellationToken);
        return (int)response.StatusCode;
    }

    private static HttpClient CreateClient(TimeSpan timeout) => new() { Timeout = timeout };
    private static StringContent JsonContent(object value) => new(JsonSerializer.Serialize(value, JsonOptions), Encoding.UTF8, "application/json");
    private static string RequiredGoogleKey(IConfiguration configuration) => !string.IsNullOrWhiteSpace(configuration["GOOGLE_MAPS_API_KEY"]) ? configuration["GOOGLE_MAPS_API_KEY"]!.Trim() : throw new InvalidOperationException("Google property services are not configured on Railway.");
    private static string ValidSessionToken(string? value) => Guid.TryParse(value, out var token) && token != Guid.Empty ? token.ToString("D") : Guid.NewGuid().ToString("D");
    private static string Normalize(string? value) => string.Join(" ", (value ?? "").Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    private static string Text(JsonElement value, string name) => value.ValueKind == JsonValueKind.Object && value.TryGetProperty(name, out var child) && child.ValueKind == JsonValueKind.String ? child.GetString() ?? "" : "";
    private static double? Number(JsonElement value, string name) => value.ValueKind == JsonValueKind.Object && value.TryGetProperty(name, out var child) && child.TryGetDouble(out var number) ? number : null;
    private static string StreetNumberFromText(string value) => value.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault(part => part.Any(char.IsDigit)) ?? "";
    private static string SafeError(string value) => string.IsNullOrWhiteSpace(value) ? "The provider did not return an explanation." : value.Length > 300 ? value[..300] : value;

    private sealed record QuoteImagery(string Status, string Message, string RoadmapImage, string SatelliteImage, string StreetViewImage, string RoadmapUrl, string SatelliteUrl, string StreetViewUrl, string AiReviewStatus, JsonObject? VisualReview);
}
