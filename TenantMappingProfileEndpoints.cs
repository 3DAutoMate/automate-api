using Npgsql;

/// <summary>
/// Routing module for mapping-profile configuration. The host supplies its existing authenticated
/// tenant check and actor resolver, preventing tenant IDs or actor names from being trusted from JSON.
/// </summary>
public static class TenantMappingProfileEndpoints
{
    public static IEndpointRouteBuilder MapTenantMappingProfileEndpoints(
        this IEndpointRouteBuilder endpoints,
        string connectionString,
        Func<HttpContext, Guid, CancellationToken, Task<bool>> authorizeTenant,
        Func<HttpContext, Guid, CancellationToken, Task<string>> resolveActor)
    {
        var group = endpoints.MapGroup("/automation/mapping-profile");

        group.MapGet("/catalog", () => Results.Ok(new
        {
            success = true,
            contractVersion = TenantMappingProfileSupport.ContractVersion,
            fields = TenantMappingProfileSupport.CanonicalFields.Values.OrderBy(x => x.Category).ThenBy(x => x.Key)
        }));

        group.MapGet("/current", async (HttpContext context, Guid tenantId, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await TenantMappingProfileSupport.EnsureAsync(connection, ct);
            var profile = await TenantMappingProfileSupport.LoadCurrentAsync(connection, tenantId, ct);
            return Results.Ok(new { success = true, profile, currentVersion = profile?.ProfileVersion ?? 0 });
        });

        group.MapPost("/validate", async (HttpContext context, Guid tenantId, TenantMappingProfileDraft draft, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            return Results.Ok(new { success = true, validation = TenantMappingProfileSupport.Validate(draft) });
        });

        group.MapPut("/current", async (HttpContext context, Guid tenantId, MappingProfileSaveRequest request, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            var actor = await resolveActor(context, tenantId, ct);
            if (string.IsNullOrWhiteSpace(actor))
                return Results.Json(new { success = false, code = "authenticated_actor_required", message = "An authenticated actor is required." }, statusCode: 401);
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await TenantMappingProfileSupport.EnsureAsync(connection, ct);
            var result = await TenantMappingProfileSupport.SaveVersionAsync(connection, tenantId, request.ExpectedVersion, request.Profile, actor, ct);
            return result.Status == "conflict"
                ? Results.Json(new { success = false, code = result.Status, result.Message, result.CurrentVersion, result.Validation }, statusCode: 409)
                : Results.Ok(new { success = true, result.Status, result.CurrentVersion, result.ProfileFingerprint, result.Validation, result.Message });
        });

        return endpoints;
    }
}

public sealed record MappingProfileSaveRequest(int ExpectedVersion, TenantMappingProfileDraft Profile);
