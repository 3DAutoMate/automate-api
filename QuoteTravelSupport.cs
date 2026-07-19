using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class QuoteTravelSupport
{
    private static readonly HttpClient Google = new() { Timeout = TimeSpan.FromSeconds(30) };

    public static async Task<QuoteTravelResult> CalculateAsync(
        NpgsqlConnection connection,
        QuoteTravelRequest request,
        IConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        var settings = await TravelCalendarSupport.LoadAsync(connection, request.TenantId, cancellationToken);
        var inspector = settings.Inspectors.FirstOrDefault(value => value.InspectorId == request.InspectorId);
        if (inspector is null) throw new QuoteTravelException("inspector_not_found", "Select an active THREED inspector.");
        if (!inspector.Enabled || string.IsNullOrWhiteSpace(inspector.BaseAddressOverride) || string.IsNullOrWhiteSpace(inspector.BasePlaceId))
            throw new QuoteTravelException("inspector_base_required", $"Confirm {inspector.Name}'s travel base on the Inspectors page first.");
        if (string.IsNullOrWhiteSpace(request.CanonicalAddress))
            throw new QuoteTravelException("property_address_required", "Confirm the property address before calculating travel.");

        var key = configuration["GOOGLE_MAPS_API_KEY"]?.Trim();
        if (string.IsNullOrWhiteSpace(key)) throw new QuoteTravelException("routes_not_configured", "Google Routes is not configured.");

        var outbound = await LoadLegAsync(
            new RoutePoint(inspector.BaseAddressOverride, inspector.BasePlaceId, null, null),
            new RoutePoint(request.CanonicalAddress, request.PlaceId ?? "", request.Latitude, request.Longitude),
            key,
            cancellationToken);
        var inbound = await LoadLegAsync(
            new RoutePoint(request.CanonicalAddress, request.PlaceId ?? "", request.Latitude, request.Longitude),
            new RoutePoint(inspector.BaseAddressOverride, inspector.BasePlaceId, null, null),
            key,
            cancellationToken);

        var fingerprint = Fingerprint(inspector, request);
        return new QuoteTravelResult(
            request.InspectorId,
            inspector.Name,
            request.CanonicalAddress.Trim(),
            outbound.DistanceMetres,
            outbound.DurationSeconds,
            inbound.DistanceMetres,
            inbound.DurationSeconds,
            outbound.DistanceMetres + inbound.DistanceMetres,
            outbound.DurationSeconds + inbound.DurationSeconds,
            "traffic_unaware",
            DateTimeOffset.UtcNow,
            fingerprint);
    }

    public static async Task<string> CurrentFingerprintAsync(NpgsqlConnection connection, QuoteTravelRequest request, CancellationToken cancellationToken=default)
    {
        var settings=await TravelCalendarSupport.LoadAsync(connection,request.TenantId,cancellationToken);
        var inspector=settings.Inspectors.FirstOrDefault(value=>value.InspectorId==request.InspectorId);
        return inspector is null||!inspector.Enabled||string.IsNullOrWhiteSpace(inspector.BasePlaceId)?"":Fingerprint(inspector,request);
    }

    private static string Fingerprint(TravelInspectorView inspector,QuoteTravelRequest request)=>TravelCalendarSupport.Fingerprint(
        request.InspectorId.ToString("D"),inspector.BasePlaceId,inspector.BaseAddressOverride,request.PlaceId,request.CanonicalAddress,
        request.Latitude?.ToString("R",CultureInfo.InvariantCulture),request.Longitude?.ToString("R",CultureInfo.InvariantCulture));

    private static async Task<RouteLeg> LoadLegAsync(RoutePoint origin, RoutePoint destination, string key, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://routes.googleapis.com/directions/v2:computeRoutes");
        request.Headers.TryAddWithoutValidation("X-Goog-Api-Key", key);
        request.Headers.TryAddWithoutValidation("X-Goog-FieldMask", "routes.distanceMeters,routes.duration,routes.staticDuration");
        request.Content = JsonContent.Create(new
        {
            origin = Waypoint(origin),
            destination = Waypoint(destination),
            travelMode = "DRIVE",
            routingPreference = "TRAFFIC_UNAWARE",
            computeAlternativeRoutes = false,
            languageCode = "en-NZ",
            units = "METRIC"
        });
        using var response = await Google.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            throw new QuoteTravelException("route_unavailable", $"Google route calculation is temporarily unavailable (HTTP {(int)response.StatusCode}).");
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        if (!document.RootElement.TryGetProperty("routes", out var routes) || routes.ValueKind != JsonValueKind.Array || routes.GetArrayLength() == 0)
            throw new QuoteTravelException("route_unavailable", "Google returned no driving route for these addresses.");
        var route = routes[0];
        var distance = route.TryGetProperty("distanceMeters", out var value) && value.TryGetInt32(out var metres) ? metres : 0;
        var duration = route.TryGetProperty("duration", out value) ? ParseDuration(value.GetString() ?? "") : 0;
        if (distance <= 0 || duration <= 0) throw new QuoteTravelException("route_unavailable", "Google returned incomplete travel details.");
        return new RouteLeg(distance, duration);
    }

    private static object Waypoint(RoutePoint point)
    {
        if (!string.IsNullOrWhiteSpace(point.PlaceId)) return new { placeId = point.PlaceId.Trim() };
        if (point.Latitude.HasValue && point.Longitude.HasValue) return new { location = new { latLng = new { latitude = point.Latitude.Value, longitude = point.Longitude.Value } } };
        return new { address = point.Address.Trim() };
    }

    private static int ParseDuration(string value)
        => value.EndsWith('s') && double.TryParse(value[..^1], NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds)
            ? Math.Max(0, (int)Math.Ceiling(seconds))
            : 0;

    private sealed record RoutePoint(string Address, string PlaceId, double? Latitude, double? Longitude);
    private sealed record RouteLeg(int DistanceMetres, int DurationSeconds);
}

public sealed record QuotePlaceResolutionRequest(Guid TenantId, string CanonicalAddress, string? PlaceId, string? SessionToken);
public sealed record QuoteTravelRequest(Guid TenantId, Guid InspectorId, string CanonicalAddress, string? PlaceId, double? Latitude, double? Longitude);
public sealed record QuoteTravelResult(Guid InspectorId, string InspectorName, string PropertyAddress, int OutboundDistanceMetres, int OutboundDurationSeconds, int ReturnDistanceMetres, int ReturnDurationSeconds, int RoundTripDistanceMetres, int RoundTripDurationSeconds, string EstimateType, DateTimeOffset CalculatedAt, string Fingerprint);
public sealed class QuoteTravelException(string code, string message) : Exception(message) { public string Code { get; } = code; }
