using AutoMateApi;
using Npgsql;

public static class TenantAgreementPolicyEndpoints
{
    public static void MapTenantAgreementPolicyEndpoints(
        this IEndpointRouteBuilder endpoints,
        string connectionString,
        Func<HttpContext, Guid, CancellationToken, Task<bool>> authorizeTenant,
        Func<HttpContext, Guid, CancellationToken, Task<string>> resolveActor)
    {
        endpoints.MapGet("/automation/agreements/policy", async (HttpContext context, Guid tenantId, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            var policy = await TenantAgreementPolicySupport.LoadAsync(connection, tenantId, ct);
            return Results.Ok(new { success = true, policy });
        });

        endpoints.MapPut("/automation/agreements/policy", async (HttpContext context, Guid tenantId, AgreementPolicySaveRequest request, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            var actor = await resolveActor(context, tenantId, ct);
            if (string.IsNullOrWhiteSpace(actor)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            var result = await TenantAgreementPolicySupport.SaveDraftAsync(connection, tenantId, request, actor, ct);
            return result.Success
                ? Results.Ok(new { success = true, result })
                : Results.Json(new { success = false, result.Status, result.Message, result }, statusCode: result.Status == "version_conflict" ? 409 : 400);
        });

        endpoints.MapPost("/automation/agreements/policy/activate", async (HttpContext context, Guid tenantId, AgreementPolicyActivateRequest request, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            var actor = await resolveActor(context, tenantId, ct);
            if (string.IsNullOrWhiteSpace(actor)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            var result = await TenantAgreementPolicySupport.ActivateAsync(connection, tenantId, request, actor, ct);
            return result.Success
                ? Results.Ok(new { success = true, result })
                : Results.Json(new { success = false, result.Status, result.Message, result }, statusCode: result.Status == "version_conflict" ? 409 : 400);
        });

        endpoints.MapGet("/jobs/{jobId:guid}/agreements/plan", async (HttpContext context, Guid jobId, Guid tenantId, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, tenantId, ct)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            var plan = await TenantAgreementPolicySupport.LoadJobPlanAsync(connection, tenantId, jobId, ct);
            return Results.Ok(new { success = true, status = plan is null ? "not_captured" : "captured", plan });
        });

        endpoints.MapPost("/jobs/{jobId:guid}/agreements/preview", async (HttpContext context, Guid jobId, TenantAgreementJobRequest request, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, request.TenantId, ct)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            var preview = await TenantAgreementPolicySupport.PreviewJobAsync(connection, request.TenantId, jobId, ct);
            return Results.Ok(new { success = true, preview, agreements = preview.Agreements, sideEffectsExecuted = false });
        });

        endpoints.MapPost("/jobs/{jobId:guid}/agreements/report-override", async (HttpContext context, Guid jobId, TenantAgreementReportOverrideRequest request, CancellationToken ct) =>
        {
            if (!await authorizeTenant(context, request.TenantId, ct)) return Results.Unauthorized();
            var actor = await resolveActor(context, request.TenantId, ct);
            if (string.IsNullOrWhiteSpace(actor)) return Results.Unauthorized();
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            var result = await TenantAgreementPolicySupport.CreateReportOverrideAsync(
                connection,
                request.TenantId,
                jobId,
                new AgreementReportOverrideRequest(request.ExpectedPlanFingerprint, request.Reason, request.ConfirmationText, request.Confirmed),
                actor,
                ct);
            return result.Success
                ? Results.Ok(new { success = true, result })
                : Results.Json(new { success = false, result.Status, result.Message, result }, statusCode: result.Status is "plan_conflict" or "plan_review_required" ? 409 : 400);
        });
    }
}

public sealed record TenantAgreementJobRequest(Guid TenantId);
public sealed record TenantAgreementReportOverrideRequest(Guid TenantId, string ExpectedPlanFingerprint, string Reason, string ConfirmationText, bool Confirmed);
