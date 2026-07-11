using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using Mapbox.Vector.Tile;
using ProjNet.CoordinateSystems;
using ProjNet.CoordinateSystems.Transformations;

public sealed record BranzLookupResult(string WindZone, string ExposureZone, string Status,
    double? Latitude, double? Longitude, string AddressFingerprint, DateTimeOffset RetrievedAt, string Error);

public static class BranzLookupService
{
    private const int Zoom = 14;
    private const double Resolution = 4.7773142678170775;
    private const double OriginX = -4020900;
    private const double OriginY = 19998100;
    private const int TileSize = 512;
    private static readonly HttpClient Http = CreateHttpClient();
    private static readonly ConcurrentDictionary<string, BranzLookupResult> Cache = new();
    private static readonly Regex LatitudeRegex = new("(?:latitude|lat)[\\\"']?\\s*[:=]\\s*[\\\"']?(?<v>-?\\d{1,2}\\.\\d+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex LongitudeRegex = new("(?:longitude|lng|lon)[\\\"']?\\s*[:=]\\s*[\\\"']?(?<v>1\\d{2}\\.\\d+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("3D-AutoMate/1.0");
        return client;
    }

    public static string Fingerprint(string address)
        => StructuredAddressResolver.Fingerprint(address);

    public static async Task<BranzLookupResult> LookupAsync(string address, double? latitude = null, double? longitude = null, bool forceRefresh = false)
    {
        var fingerprint = Fingerprint(address);
        if (!forceRefresh && Cache.TryGetValue(fingerprint, out var cached) && cached.Status == "available") return cached;
        try
        {
            if (!latitude.HasValue || !longitude.HasValue)
                (latitude, longitude) = await ResolveCoordinatesAsync(address);
            if (!latitude.HasValue || !longitude.HasValue)
                return Failed(fingerprint, "The property address could not be located.");

            var nztm = ToNztm(longitude.Value, latitude.Value);
            var col = (int)Math.Floor((nztm[0] - OriginX) / (Resolution * TileSize));
            var row = (int)Math.Floor((OriginY - nztm[1]) / (Resolution * TileSize));
            var windTask = ReadZoneAsync("Wind_Zones", nztm[0], nztm[1], row, col);
            var exposureTask = ReadZoneAsync("Corrosion_Zones", nztm[0], nztm[1], row, col);
            await Task.WhenAll(windTask, exposureTask);

            var wind = MapWind(await windTask);
            var exposure = MapExposure(await exposureTask);
            if (wind.Length == 0 || exposure.Length == 0)
                return Failed(fingerprint, "BRANZ did not return both containing zones.", latitude, longitude);
            var result = new BranzLookupResult(wind, exposure, "available", latitude, longitude, fingerprint, DateTimeOffset.UtcNow, "");
            Cache[fingerprint] = result;
            return result;
        }
        catch (Exception ex)
        {
            return Failed(fingerprint, ex.Message, latitude, longitude);
        }
    }

    private static BranzLookupResult Failed(string fingerprint, string error, double? latitude = null, double? longitude = null) =>
        new("", "", "unavailable", latitude, longitude, fingerprint, DateTimeOffset.UtcNow, error);

    private static async Task<(double? Latitude, double? Longitude)> ResolveCoordinatesAsync(string address)
    {
        if (string.IsNullOrWhiteSpace(address)) return (null, null);
        var match = await StructuredAddressResolver.ResolveAsync(address);
        if (match == null) return (null, null);
        var html = await Http.GetStringAsync(match.PageUrl);
        var lat = LatitudeRegex.Match(html);
        var lon = LongitudeRegex.Match(html);
        return lat.Success && lon.Success
            ? (double.Parse(lat.Groups["v"].Value, System.Globalization.CultureInfo.InvariantCulture), double.Parse(lon.Groups["v"].Value, System.Globalization.CultureInfo.InvariantCulture))
            : (null, null);
    }

    private static double[] ToNztm(double longitude, double latitude)
    {
        const string wgs84 = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]]";
        const string nztm = "PROJCS[\"NZGD2000 / New Zealand Transverse Mercator 2000\",GEOGCS[\"NZGD2000\",DATUM[\"New_Zealand_Geodetic_Datum_2000\",SPHEROID[\"GRS 1980\",6378137,298.257222101]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0],PARAMETER[\"central_meridian\",173],PARAMETER[\"scale_factor\",0.9996],PARAMETER[\"false_easting\",1600000],PARAMETER[\"false_northing\",10000000],UNIT[\"metre\",1]]";
        var factory = new CoordinateSystemFactory();
        var transform = new CoordinateTransformationFactory().CreateFromCoordinateSystems(factory.CreateFromWkt(wgs84), factory.CreateFromWkt(nztm));
        return transform.MathTransform.Transform([longitude, latitude]);
    }

    private static async Task<int?> ReadZoneAsync(string service, double easting, double northing, int row, int col)
    {
        var url = $"https://tiles.arcgis.com/tiles/vkPf8weODt71Prmb/arcgis/rest/services/{service}/VectorTileServer/tile/{Zoom}/{row}/{col}.pbf";
        using var response = await Http.GetAsync(url);
        response.EnsureSuccessStatusCode();
        await using var raw = await response.Content.ReadAsStreamAsync();
        Stream tileStream = response.Content.Headers.ContentEncoding.Contains("gzip") ? new GZipStream(raw, CompressionMode.Decompress) : raw;
        var layers = VectorTileParser.Parse(tileStream);
        foreach (var feature in layers.SelectMany(layer => layer.VectorTileFeatures))
        {
            var px = (easting - (OriginX + col * TileSize * Resolution)) / (TileSize * Resolution) * feature.Extent;
            var py = ((OriginY - row * TileSize * Resolution) - northing) / (TileSize * Resolution) * feature.Extent;
            var inside = false;
            foreach (var segment in feature.Geometry)
                if (PointInRing(px, py, segment)) inside = !inside;
            if (!inside) continue;
            var symbol = feature.Attributes.FirstOrDefault(pair => pair.Key == "_symbol").Value;
            if (symbol != null && int.TryParse(Convert.ToString(symbol), out var value)) return value;
        }
        return null;
    }

    private static bool PointInRing(double x, double y, ArraySegment<Mapbox.Vector.Tile.Coordinate> ring)
    {
        var points = ring.Array!;
        var inside = false;
        for (int i = ring.Offset, j = ring.Offset + ring.Count - 1; i < ring.Offset + ring.Count; j = i++)
        {
            var a = points[i]; var b = points[j];
            if ((a.Y > y) != (b.Y > y) && x < (b.X - a.X) * (y - a.Y) / (b.Y - a.Y) + a.X) inside = !inside;
        }
        return inside;
    }

    private static string MapWind(int? value) => value switch { 0 => "Low", 1 => "Medium", 2 => "High", 3 => "Very High", 4 => "Extra High", 5 => "Specific Engineering Design", _ => "" };
    private static string MapExposure(int? value) => value switch { 0 => "Zone B", 1 => "Zone C", 2 => "Zone D", _ => "" };
}
