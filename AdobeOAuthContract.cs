namespace AutoMateApi;

public static class AdobeOAuthContract
{
    public const string GlobalAuthorizeUrl = "https://secure.adobesign.com/public/oauth/v2";
    public const string DevelopmentApiBase = "https://api.na1.adobesign.com";

    public static string ResolveApiBase(string? candidate, string fallback = DevelopmentApiBase)
    {
        if (TryNormalizeApiBase(candidate, out var normalized)) return normalized;
        if (TryNormalizeApiBase(fallback, out normalized)) return normalized;
        throw new InvalidOperationException("Adobe did not return a trusted API access point.");
    }

    public static bool TryNormalizeApiBase(string? value, out string normalized)
    {
        normalized = "";
        if (!Uri.TryCreate(value?.Trim(), UriKind.Absolute, out var uri) ||
            !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) ||
            !string.IsNullOrEmpty(uri.UserInfo) ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment) ||
            (uri.Port != 443 && !uri.IsDefaultPort)) return false;

        var host = uri.IdnHost.TrimEnd('.').ToLowerInvariant();
        var trusted = host == "api.adobesign.com" || host.EndsWith(".adobesign.com", StringComparison.Ordinal) ||
                      host == "api.echosign.com" || host.EndsWith(".echosign.com", StringComparison.Ordinal);
        if (!trusted || !host.StartsWith("api.", StringComparison.Ordinal)) return false;

        normalized = $"https://{host}";
        return true;
    }
}
