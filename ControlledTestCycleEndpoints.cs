using Npgsql;

namespace AutoMateApi;

public static class ControlledTestCycleEndpoints
{
    public static void MapControlledTestCycleEndpoints(
        this WebApplication app,
        string connectionString,
        Func<HttpContext, Guid, CancellationToken, Task<bool>> authorizeTenantAdmin,
        Func<HttpContext, Guid, CancellationToken, Task<string>> resolveActor)
    {
        app.MapGet("/jobs/{jobId:guid}/controlled-test", async (HttpContext context, Guid jobId, Guid tenantId) =>
            await Execute(context, tenantId, jobId, false, async connection =>
                await ControlledTestCycleSupport.LoadStateAsync(connection, tenantId, jobId, cancellationToken: context.RequestAborted)));

        app.MapPut("/jobs/{jobId:guid}/controlled-test", async (HttpContext context, Guid jobId, ControlledTestDesignationApiRequest request) =>
            await Execute(context, request.TenantId, jobId, true, async connection =>
            {
                var actor = await resolveActor(context, request.TenantId, context.RequestAborted);
                return await ControlledTestCycleSupport.SetDesignationAsync(connection,
                    new(request.TenantId, jobId, request.Controlled, request.ExpectedVersion,
                        request.Confirmed, request.Reason, actor, request.IdempotencyKey), context.RequestAborted);
            }));

        app.MapGet("/jobs/{jobId:guid}/test-cycle/readiness", async (HttpContext context, Guid jobId, Guid tenantId) =>
            await Execute(context, tenantId, jobId, false, async connection =>
                await ControlledTestCycleSupport.LoadReadinessAsync(connection, tenantId, jobId, context.RequestAborted)));

        app.MapPost("/jobs/{jobId:guid}/test-cycle/prepare", async (HttpContext context, Guid jobId, ControlledTestPrepareApiRequest request) =>
            await Execute(context, request.TenantId, jobId, true, async connection =>
            {
                var actor = await resolveActor(context, request.TenantId, context.RequestAborted);
                return await ControlledTestCycleSupport.PrepareAsync(connection,
                    new(request.TenantId, jobId, request.ExpectedDesignationVersion, request.ExpectedApprovedVersion,
                        request.Confirmed, request.Reason, request.IdempotencyKey, actor, NormalizeXeroPolicy(request.XeroPolicy)),
                    context.RequestAborted);
            }));

        app.MapPost("/jobs/{jobId:guid}/test-cycle/{cycleId:guid}/reconcile/{itemId:guid}", async (
            HttpContext context, Guid jobId, Guid cycleId, Guid itemId, ControlledTestReconcileApiRequest request) =>
            await Execute(context, request.TenantId, jobId, true, async connection =>
            {
                var actor = await resolveActor(context, request.TenantId, context.RequestAborted);
                return await ControlledTestCycleSupport.ReconcileAsync(connection,
                    new(request.TenantId, jobId, cycleId, itemId, request.Resolution, request.Confirmed,
                        request.Reason, request.IdempotencyKey, actor), context.RequestAborted);
            }));

        app.MapPost("/jobs/{jobId:guid}/test-cycle/{cycleId:guid}/start", async (
            HttpContext context, Guid jobId, Guid cycleId, ControlledTestStartApiRequest request) =>
        {
            if (!string.Equals(request.ConfirmationText, "START NEW TEST CYCLE", StringComparison.Ordinal))
                return Results.Json(new { success = false, status = "confirmation_required", message = "Type START NEW TEST CYCLE exactly." }, statusCode: 409);
            return await Execute(context, request.TenantId, jobId, true, async connection =>
            {
                var actor = await resolveActor(context, request.TenantId, context.RequestAborted);
                return await ControlledTestCycleSupport.StartAsync(connection,
                    new(request.TenantId, jobId, cycleId, request.ExpectedApprovedVersion,
                        request.ExpectedReconciliationFingerprint, request.Confirmed, request.Reason,
                        request.IdempotencyKey, actor, NormalizeXeroPolicy(request.XeroPolicy)), context.RequestAborted);
            });
        });

        async Task<IResult> Execute<T>(HttpContext context, Guid tenantId, Guid jobId, bool mutation, Func<NpgsqlConnection, Task<T>> action)
        {
            if (tenantId == Guid.Empty || jobId == Guid.Empty)
                return Results.BadRequest(new { success = false, status = "invalid_identity", message = "TenantId and JobID are required." });
            if (!await authorizeTenantAdmin(context, tenantId, context.RequestAborted))
                return Results.Json(new { success = false, status = mutation ? "tenant_admin_required" : "ownership_required", message = "An authenticated company administrator is required." }, statusCode: 403);
            try
            {
                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(context.RequestAborted);
                var result = await action(connection);
                return Results.Ok(new { success = true, data = result });
            }
            catch (ControlledTestCycleException exception)
            {
                var status = exception.Code == "cycle_not_found" ? 404 : 409;
                return Results.Json(new { success = false, status = exception.Code, message = exception.Message }, statusCode: status);
            }
            catch (ArgumentException exception)
            {
                return Results.BadRequest(new { success = false, status = "invalid_request", message = exception.Message });
            }
            catch (UnauthorizedAccessException)
            {
                return Results.Json(new { success = false, status = "not_found", message = "The controlled job or cycle was not found." }, statusCode: 404);
            }
        }
    }

    private static string NormalizeXeroPolicy(string? value) =>
        string.IsNullOrWhiteSpace(value) || value == "reuse_read_only" ? "retain_existing" : value.Trim();
}

public sealed class ControlledTestDesignationApiRequest
{
    public Guid TenantId { get; set; }
    public bool Controlled { get; set; }
    public int ExpectedVersion { get; set; }
    public bool Confirmed { get; set; }
    public string Reason { get; set; } = "Controlled disposable-job testing";
    public string IdempotencyKey { get; set; } = "";
}

public sealed class ControlledTestPrepareApiRequest
{
    public Guid TenantId { get; set; }
    public int ExpectedDesignationVersion { get; set; }
    public int ExpectedApprovedVersion { get; set; }
    public bool Confirmed { get; set; }
    public string Reason { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
    public string XeroPolicy { get; set; } = "retain_existing";
}

public sealed class ControlledTestReconcileApiRequest
{
    public Guid TenantId { get; set; }
    public string Resolution { get; set; } = "";
    public bool Confirmed { get; set; }
    public string Reason { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
}

public sealed class ControlledTestStartApiRequest
{
    public Guid TenantId { get; set; }
    public int ExpectedApprovedVersion { get; set; }
    public string ExpectedReconciliationFingerprint { get; set; } = "";
    public bool Confirmed { get; set; }
    public string ConfirmationText { get; set; } = "";
    public string Reason { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
    public string XeroPolicy { get; set; } = "retain_existing";
}
