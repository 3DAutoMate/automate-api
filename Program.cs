using System.Text.Json;
using System.Text.Json.Serialization;
using static OnlinePropertySupport;
using System.Text.RegularExpressions;
using System.Text;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using Npgsql;
using Microsoft.AspNetCore.RateLimiting;
using System.Threading.RateLimiting;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);
builder.Configuration.AddEnvironmentVariables();
builder.Services.AddCors(options =>
{
    options.AddPolicy("LocalTemplateMaker", policy =>
    {
        policy
            .WithOrigins("http://127.0.0.1:5000", "http://localhost:5000")
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});
builder.Services.AddRateLimiter(options =>
{
    options.AddPolicy("public-inspection", context => RateLimitPartition.GetFixedWindowLimiter(
        context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
        _ => new FixedWindowRateLimiterOptions
        {
            PermitLimit = 60,
            Window = TimeSpan.FromMinutes(1),
            QueueLimit = 0,
            AutoReplenishment = true
        }));
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
});

// Use Railway DATABASE_PUBLIC_URL if available, otherwise use local fallback for testing
var rawConnectionString = builder.Configuration["DATABASE_PUBLIC_URL"];

if (string.IsNullOrWhiteSpace(rawConnectionString))
{
    throw new Exception("DATABASE_PUBLIC_URL is missing.");
}

string connectionString;

if (rawConnectionString.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase) ||
    rawConnectionString.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase))
{
    var databaseUri = new Uri(rawConnectionString);
    var userInfo = databaseUri.UserInfo.Split(':', 2);

    var username = Uri.UnescapeDataString(userInfo[0]);
    var password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : "";

    connectionString =
        $"Host={databaseUri.Host};" +
        $"Port={databaseUri.Port};" +
        $"Database={databaseUri.AbsolutePath.TrimStart('/')};" +
        $"Username={username};" +
        $"Password={password};" +
        $"SSL Mode=Require;" +
        $"Trust Server Certificate=true;";
}
else
{
    connectionString = rawConnectionString;
}

var V1MappingFields = new List<V1MappingField>
{
    new("primary_service", "CustomText1", "Primary Service", true, true, "all_services", ""),
    new("additional_service_1", "CustomText2", "Additional Service 1", true, true, "all_services", ""),
    new("additional_service_2", "CustomText3", "Additional Service 2", true, true, "all_services", ""),
    new("age_of_building", "CustomText4", "Age of Building", true, true, "building_inspection", "Show for all services; affects pricing for building inspection only."),
    new("building_type", "CustomText5", "Building Type", true, true, "building_inspection", "Affects building inspection only."),
    new("number_of_stories", "CustomText6", "Number Of Stories", true, true, "building_inspection", "Affects building inspection only."),
    new("number_of_bedrooms", "CustomText7", "Number Of Bedrooms", true, true, "building_inspection", "Affects building inspection only."),
    new("number_of_bathrooms", "CustomText8", "Number Of Bathrooms", true, true, "building_inspection", "Affects building inspection only."),
    new("monolithic_or_plaster_cladding", "CustomText9", "Monolithic or Plaster Cladding?", true, true, "building_inspection", ""),
    new("inspect_separate_outbuildings", "CustomText10", "Inspect Separate Outbuilding(s)?", true, true, "building_inspection", "Inclusion/exclusion for building inspection."),
    new("house_occupied", "CustomText11", "House Occupied?", true, true, "building_inspection", ""),
    new("inspect_attached_flat", "CustomText12", "Inspect Attached Flat?", true, true, "building_inspection", "Inclusion/exclusion for building inspection."),
    new("travel_fee", "CustomText13", "Travel Fee?", true, true, "all_services", "Can affect all services."),
    new("healthy_homes_number_of_bedrooms", "CustomText14", "Healthy Homes Number Of Bedrooms", true, true, "healthy_homes", "Healthy Homes Assessment only."),
    new("meth_testing_number_of_samples", "CustomText15", "Meth Testing Number Of Samples", true, true, "meth_testing", "Meth testing only."),
    new("healthy_homes_reinspect_failed", "CustomText16", "Reinspect Failed Healthy Homes Assessment", true, true, "healthy_homes", "Healthy Homes Assessment only."),
    new("review_council_files", "CustomText17", "Review Council Files?", true, true, "building_inspection", "Inclusion/exclusion for building inspection."),
    new("foundation_space_to_inspect", "CustomText18", "Foundation Space To Inspect?", true, true, "building_inspection", "Affects building inspection only."),
    new("healthy_homes_reinspection_date", "CustomText19", "Reinspection Date For Healthy Homes", true, true, "healthy_homes", "Healthy Homes Assessment only."),
    new("property_access_by", "CustomText23", "Property Access By?", false, true, "all_services", "")
};

var app = builder.Build();
app.UseCors("LocalTemplateMaker");
app.UseRateLimiter();
app.MapTenantMappingProfileEndpoints(
    connectionString,
    async (context, tenantId, cancellationToken) =>
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var owner = await RequireAutomationOwnerAsync(context, connection, tenantId);
        return owner.Allowed;
    },
    async (context, tenantId, cancellationToken) =>
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        return await LoadAuthenticatedAutomationActorAsync(connection, tenantId, GetAuthenticatedInspectorId(context));
    });
const string FoundersTrialAccessCode = "PILOT";
var clientTokenPepper = builder.Configuration["AUTOMATE_CLIENT_PAGE_TOKEN_KEY"] ?? "";
var publicBaseUrl = (builder.Configuration["AUTOMATE_PUBLIC_BASE_URL"] ?? "https://automate-api-production.up.railway.app").TrimEnd('/');
var TransparentGif = Convert.FromBase64String("R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==");

// =============================
// ROOT
// =============================
app.MapGet("/", () => Results.Ok(new
{
    ok = true,
    service = "3D AutoMate API"
}));

// =============================
// DB TEST
// =============================
app.MapGet("/db-test", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand("SELECT NOW();", conn);
        var result = await cmd.ExecuteScalarAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Database connection successful.",
            serverTime = result?.ToString()
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Database connection failed",
            detail: ex.Message,
            statusCode: 500
        );
    }
});

// =============================
// ENSURE ACCOUNT TABLES
// inspectors + subscriptions
// =============================
app.MapPost("/accounts/ensure-tables", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureInspectorsTableAsync(conn);
        await EnsureSubscriptionsTableAsync(conn);

        return Results.Ok(new
        {
            success = true,
            message = "Account tables ensured"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Ensure account tables failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// CLIENT INSPECTION PAGE / EMAIL ENGAGEMENT
// =============================
app.MapGet("/inspection/{token}", async (HttpContext context, string token) =>
{
    if (string.IsNullOrWhiteSpace(clientTokenPepper)) return Results.Problem(statusCode: 503, title: "Inspection page unavailable");
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var access = await AutoMateApi.ClientEngagementSupport.ResolveAsync(conn, token, "inspection_page", clientTokenPepper, context.RequestAborted);
        if (access == null) return Results.Content(ExpiredClientPageHtml(), "text/html", Encoding.UTF8, 404);
        await AutoMateApi.ClientEngagementSupport.RecordViewAsync(conn, EngagementCommand(context, access, "view", EngagementEventKey(context, "view")), clientTokenPepper, context.RequestAborted);
        var display = await LoadClientInspectionDisplayAsync(conn, access, context.RequestAborted);
        return Results.Content(AutoMateApi.ClientInspectionPageRenderer.Render(access, display, token), "text/html", Encoding.UTF8);
    }
    catch { return Results.Content(ExpiredClientPageHtml(), "text/html", Encoding.UTF8, 404); }
}).RequireRateLimiting("public-inspection");

app.MapGet("/inspection/{token}/pixel.gif", async (HttpContext context, string token) =>
{
    try
    {
        if (!string.IsNullOrWhiteSpace(clientTokenPepper))
        {
            await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
            var access = await AutoMateApi.ClientEngagementSupport.ResolveForPixelAsync(conn, token, "inspection_page", clientTokenPepper, context.RequestAborted);
            if (access != null) await AutoMateApi.ClientEngagementSupport.RecordPixelAsync(conn, EngagementCommand(context, access, "pixel", EngagementEventKey(context, "pixel")), clientTokenPepper, context.RequestAborted);
        }
    }
    catch { }
    finally { context.Response.Headers.CacheControl = "no-store, private"; }
    return Results.Bytes(TransparentGif, "image/gif");
}).RequireRateLimiting("public-inspection");

app.MapPost("/inspection/{token}/confirm", async (HttpContext context, string token) =>
{
    if (string.IsNullOrWhiteSpace(clientTokenPepper)) return Results.NotFound();
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var access = await AutoMateApi.ClientEngagementSupport.ResolveAsync(conn, token, "inspection_page", clientTokenPepper, context.RequestAborted);
        if (access == null) return Results.NotFound();
        var result = await AutoMateApi.ClientEngagementSupport.RecordConfirmAsync(conn, EngagementCommand(context, access, "confirm", "confirmed"), clientTokenPepper, context.RequestAborted);
        return Results.Ok(new { success=true,status="confirmed",confirmedAt=result.OccurredAt,replayed=result.Replayed });
    }
    catch { return Results.NotFound(); }
}).RequireRateLimiting("public-inspection");

app.MapGet("/inspection/{token}/calendar.ics", async (HttpContext context, string token) =>
{
    if (string.IsNullOrWhiteSpace(clientTokenPepper)) return Results.NotFound();
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var access = await AutoMateApi.ClientEngagementSupport.ResolveAsync(conn, token, "inspection_page", clientTokenPepper, context.RequestAborted);
        if (access == null) return Results.NotFound();
        await AutoMateApi.ClientEngagementSupport.RecordCalendarAsync(conn, EngagementCommand(context, access, "calendar", "ics"), clientTokenPepper, context.RequestAborted);
        var display = await LoadClientInspectionDisplayAsync(conn, access, context.RequestAborted);
        context.Response.Headers.ContentDisposition = "attachment; filename=inspection.ics";
        return Results.Text(AutoMateApi.ClientInspectionPageRenderer.Calendar(access, display), "text/calendar", Encoding.UTF8);
    }
    catch { return Results.NotFound(); }
}).RequireRateLimiting("public-inspection");

app.MapGet("/automation/engagement/settings", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,tenantId); if(!owner.Allowed)return owner.Error!;
        var settings=await AutoMateApi.ClientEngagementSupport.LoadSettingsAsync(conn,tenantId,context.RequestAborted);
        return Results.Ok(new { success=true,openTrackingEnabled=settings.PixelEnabled,clientPageEnabled=settings.PageEnabled,expiresAfterDays=90,settings.Version,settings.UpdatedAt });
    }
    catch(Exception ex){return Results.Problem(title:"Load engagement settings failed",detail:ex.Message,statusCode:500);}
});

app.MapGet("/jobs/{jobId}/client-page/preview", async (HttpContext context, Guid jobId, Guid tenantId) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,tenantId);if(!owner.Allowed)return owner.Error!;
        const string sql=@"SELECT approved_snapshot_version,COALESCE(approved_snapshot_fingerprint,''),COALESCE(approved_snapshot_json::text,'') FROM public.jobs_staging WHERE job_id=@job AND tenant_id::text=@tenant LIMIT 1";
        await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("job",jobId);cmd.Parameters.AddWithValue("tenant",tenantId.ToString());
        await using var reader=await cmd.ExecuteReaderAsync(context.RequestAborted);if(!await reader.ReadAsync(context.RequestAborted))return Results.NotFound(new{success=false,message="Job not found for this company."});
        var version=reader.GetInt32(0);var fingerprint=reader.GetString(1);var snapshot=reader.GetString(2);await reader.DisposeAsync();
        if(version<1||string.IsNullOrWhiteSpace(fingerprint)||string.IsNullOrWhiteSpace(snapshot))return Results.Conflict(new{success=false,status="approved_snapshot_required",message="No approved client snapshot is available for this job."});
        var access=new AutoMateApi.ClientPageAccess(jobId,Guid.Empty,tenantId,jobId,"contact_1",DateTime.UtcNow.AddDays(1),version,fingerprint,snapshot);
        var display=await LoadClientInspectionDisplayAsync(conn,access,context.RequestAborted);
        return Results.Ok(new{success=true,jobId,approvedVersion=version,html=AutoMateApi.ClientInspectionPageRenderer.RenderPreview(access,display),recordsEngagement=false});
    }
    catch(Exception ex){return Results.Problem(title:"Preview client page failed",detail:ex.Message,statusCode:500);}
});

app.MapPut("/automation/engagement/settings", async (HttpContext context, ClientEngagementSettingsRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        var inspectorId=GetAuthenticatedInspectorId(context);var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,inspectorId);
        var result=await AutoMateApi.ClientEngagementSupport.SaveSettingsAsync(conn,new(request.TenantId,request.ClientPageEnabled,request.OpenTrackingEnabled,request.ExpectedVersion,request.IdempotencyKey,request.Confirmed,actor),context.RequestAborted);
        if(result.Status is "conflict" or "idempotency_conflict" or "confirmation_required")return Results.Json(new{success=false,status=result.Status,message=result.Message,settings=result.Settings},statusCode:409);
        return Results.Ok(new{success=true,status=result.Status,message=result.Message,settings=result.Settings,expiresAfterDays=90});
    }
    catch(AuthenticatedAutomationIdentityException ex){return Results.Json(new{success=false,status="authenticated_identity_required",message=ex.Message},statusCode:401);}
    catch(Exception ex){return Results.Problem(title:"Save engagement settings failed",detail:ex.Message,statusCode:500);}
});

app.MapGet("/jobs/{jobId}/communications", async (HttpContext context, Guid jobId, Guid tenantId) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,tenantId);if(!owner.Allowed)return owner.Error!;
        if(!await AutomationFoundationSupport.JobBelongsToTenantAsync(conn,tenantId,jobId))return Results.NotFound();
        var items=await AutoMateApi.ClientEngagementSupport.LoadCommunicationsAsync(conn,tenantId,jobId,context.RequestAborted);
        return Results.Ok(new{success=true,jobId,items});
    }
    catch(Exception ex){return Results.Problem(title:"Load job communications failed",detail:ex.Message,statusCode:500);}
});

app.MapGet("/communications", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,tenantId);if(!owner.Allowed)return owner.Error!;
        await AutoMateApi.ClientEngagementSupport.EnsureAsync(conn,context.RequestAborted);
        const string sql=@"SELECT c.communication_id,c.job_id,c.delivery_state,c.issued_at,c.provider,c.redacted_error,c.confirmed_at,
COUNT(e.event_id) FILTER(WHERE e.event_type='pixel') AS pixel_count,MAX(e.occurred_at) FILTER(WHERE e.event_type='pixel') AS last_pixel,
COUNT(e.event_id) FILTER(WHERE e.event_type='view') AS view_count,MAX(e.occurred_at) FILTER(WHERE e.event_type='view') AS last_view
FROM public.email_communications c LEFT JOIN public.email_engagement_events e ON e.communication_id=c.communication_id
WHERE c.tenant_id=@tenant GROUP BY c.communication_id,c.job_id,c.delivery_state,c.issued_at,c.provider,c.redacted_error,c.confirmed_at
ORDER BY c.issued_at DESC LIMIT 200";
        await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("tenant",tenantId);await using var reader=await cmd.ExecuteReaderAsync(context.RequestAborted);var items=new List<object>();
        while(await reader.ReadAsync(context.RequestAborted))items.Add(new{communicationId=reader.GetGuid(0),jobId=reader.GetGuid(1),deliveryState=reader.GetString(2),issuedAt=reader.GetDateTime(3),provider=reader.GetString(4),error=reader.GetString(5),confirmedAt=reader.IsDBNull(6)?null:(DateTime?)reader.GetDateTime(6),possibleOpenCount=reader.GetInt64(7),lastPossibleOpenAt=reader.IsDBNull(8)?null:(DateTime?)reader.GetDateTime(8),viewCount=reader.GetInt64(9),lastViewAt=reader.IsDBNull(10)?null:(DateTime?)reader.GetDateTime(10)});
        return Results.Ok(new{success=true,items});
    }
    catch(Exception ex){return Results.Problem(title:"Load communications failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/client-page/revoke", async (HttpContext context, Guid jobId, RevokeClientPageRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        if(!request.Confirmed)return Results.BadRequest(new{success=false,status="confirmation_required",message="Confirm client-page revocation."});
        var inspectorId=GetAuthenticatedInspectorId(context);var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,inspectorId);
        var count=await AutoMateApi.ClientEngagementSupport.RevokeJobAsync(conn,new(request.TenantId,jobId,string.IsNullOrWhiteSpace(request.Reason)?"revoked by tenant":request.Reason,actor),context.RequestAborted);
        return Results.Ok(new{success=true,jobId,revoked=count});
    }
    catch(Exception ex){return Results.Problem(title:"Revoke client page failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/accounts/register-trial", async (TrialRegistrationRequest request) =>
{
    try
    {
        if (request.TenantId == Guid.Empty)
            return Results.BadRequest(new { success = false, message = "Tenant ID is required." });

        if (request.InspectorId == Guid.Empty)
            return Results.BadRequest(new { success = false, message = "Inspector ID is required." });

        if (string.IsNullOrWhiteSpace(request.Email))
            return Results.BadRequest(new { success = false, message = "Email is required." });

        if (!string.Equals((request.AccessCode ?? "").Trim(), FoundersTrialAccessCode, StringComparison.OrdinalIgnoreCase))
        {
            return Results.BadRequest(new
            {
                success = false,
                allowed = false,
                status = "invalid_code",
                message = "Enter the valid 3D AutoMate founders trial access code."
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureInspectorsTableAsync(conn);
        await EnsureSubscriptionsTableAsync(conn);
        await EnsureInspectorIntegrationsTableAsync(conn);

        var existingAccount = await LoadCompanyAccountByTenantAsync(conn, request.TenantId);
        if (existingAccount != null)
        {
            var existingDaysRemaining = CalculateTrialDaysRemaining(existingAccount.TrialEndsAt);
            var existingStatus = existingAccount.Status;

            if (string.Equals(existingStatus, "active", StringComparison.OrdinalIgnoreCase))
            {
                await UpsertInspectorAsync(conn, request, "active_company");
                return Results.Ok(BuildAccountResponse(
                    success: true,
                    allowed: true,
                    status: "active",
                    message: "This company already has an active subscription.",
                    request: request,
                    trialEndsAt: existingAccount.TrialEndsAt,
                    companyStartAt: existingAccount.CompanyStartAt,
                    daysRemaining: existingDaysRemaining,
                    registeredEmail: existingAccount.Email,
                    registeredInspectorId: existingAccount.InspectorId));
            }

            if (string.Equals(existingStatus, "trialing", StringComparison.OrdinalIgnoreCase) && existingDaysRemaining > 0)
            {
                await UpsertInspectorAsync(conn, request, "trial_registered");
                return Results.Ok(BuildAccountResponse(
                    success: true,
                    allowed: true,
                    status: "trialing",
                    message: "This company already has a founders trial. The existing trial countdown was not reset.",
                    request: request,
                    trialEndsAt: existingAccount.TrialEndsAt,
                    companyStartAt: existingAccount.CompanyStartAt,
                    daysRemaining: existingDaysRemaining,
                    registeredEmail: existingAccount.Email,
                    registeredInspectorId: existingAccount.InspectorId));
            }

            if (!string.Equals(existingStatus, "not_registered", StringComparison.OrdinalIgnoreCase))
            {
                return Results.Ok(BuildAccountResponse(
                    success: false,
                    allowed: false,
                    status: "expired",
                    message: "This company trial has expired. Contact 3D AutoMate to activate your subscription.",
                    request: request,
                    trialEndsAt: existingAccount.TrialEndsAt,
                    companyStartAt: existingAccount.CompanyStartAt,
                    daysRemaining: 0,
                    registeredEmail: existingAccount.Email,
                    registeredInspectorId: existingAccount.InspectorId));
            }
        }

        await UpsertInspectorAsync(conn, request, "trial_registered");

        const string subscriptionSql = @"
INSERT INTO public.subscriptions
(
    inspector_id,
    status,
    plan_name,
    trial_ends_at,
    updated_at
)
VALUES
(
    @inspector_id,
    'trialing',
    'founders_trial',
    NOW() + INTERVAL '30 days',
    NOW()
)
ON CONFLICT (inspector_id) DO UPDATE
SET
    status = CASE
        WHEN public.subscriptions.status IN ('active', 'trialing') THEN public.subscriptions.status
        ELSE 'trialing'
    END,
    plan_name = COALESCE(public.subscriptions.plan_name, 'founders_trial'),
    trial_ends_at = COALESCE(public.subscriptions.trial_ends_at, NOW() + INTERVAL '30 days'),
    updated_at = NOW();";

        await using (var subscriptionCmd = new NpgsqlCommand(subscriptionSql, conn))
        {
            subscriptionCmd.Parameters.AddWithValue("inspector_id", request.InspectorId);
            await subscriptionCmd.ExecuteNonQueryAsync();
        }

        var trialEndsAt = await GetTrialEndsAtAsync(conn, request.InspectorId);
        var newAccount = await LoadCompanyAccountByTenantAsync(conn, request.TenantId);

        return Results.Ok(new
        {
            success = true,
            allowed = true,
            message = "Trial registered.",
            tenantId = request.TenantId,
            inspectorId = request.InspectorId,
            email = request.Email.Trim(),
            status = "trialing",
            trialEndsAt,
            companyStartAt = newAccount?.CompanyStartAt,
            daysRemaining = CalculateTrialDaysRemaining(trialEndsAt)
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Trial registration failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapGet("/accounts/trial-status", async (Guid inspectorId, Guid? tenantId) =>
{
    try
    {
        if (inspectorId == Guid.Empty && (!tenantId.HasValue || tenantId.Value == Guid.Empty))
            return Results.BadRequest(new { success = false, allowed = false, message = "Inspector ID or Tenant ID is required." });

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureInspectorsTableAsync(conn);
        await EnsureSubscriptionsTableAsync(conn);

        CompanyAccountStatus? account = null;
        if (tenantId.HasValue && tenantId.Value != Guid.Empty)
            account = await LoadCompanyAccountByTenantAsync(conn, tenantId.Value);

        if (account == null && inspectorId != Guid.Empty)
            account = await LoadCompanyAccountByInspectorAsync(conn, inspectorId);

        if (account == null)
        {
            return Results.Ok(new
            {
                success = true,
                allowed = false,
                registered = false,
                status = "not_registered",
                message = "This company is not registered.",
            trialEndsAt = (DateTime?)null,
            daysRemaining = 0,
            companyStartAt = (DateTime?)null
            });
        }

        var daysRemaining = CalculateTrialDaysRemaining(account.TrialEndsAt);
        var status = account.Status;
        var allowed = string.Equals(status, "active", StringComparison.OrdinalIgnoreCase)
            || (string.Equals(status, "trialing", StringComparison.OrdinalIgnoreCase) && daysRemaining > 0);

        var message = allowed
            ? (string.Equals(status, "active", StringComparison.OrdinalIgnoreCase)
                ? "This company has an active subscription."
                : "This company has an active founders trial.")
            : "This company trial has expired. Contact 3D AutoMate to activate your subscription.";

        return Results.Ok(new
        {
            success = true,
            allowed,
            registered = true,
            status,
            message,
            trialEndsAt = account.TrialEndsAt,
            daysRemaining = allowed ? daysRemaining : 0,
            companyStartAt = account.CompanyStartAt,
            inspectorName = account.InspectorName,
            companyName = account.CompanyName,
            email = account.Email,
            tenantId = account.TenantId,
            registeredInspectorId = account.InspectorId,
            planName = string.IsNullOrWhiteSpace(account.PlanName) ? "advanced" : account.PlanName,
            capabilities = new
            {
                basicAutomation = allowed,
                advancedWorkflows = allowed && !string.Equals(account.PlanName, "basic", StringComparison.OrdinalIgnoreCase),
                outgoingWebhooks = allowed && !string.Equals(account.PlanName, "basic", StringComparison.OrdinalIgnoreCase)
            }
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Trial status failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// ENSURE WORKFLOW + PAYLOAD COLUMNS
// =============================
app.MapPost("/jobs/ensure-columns", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS tenant_id uuid NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS inspector_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS age_of_building text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS job_date timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS inspection_duration_minutes integer NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS source_updated_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS date_added timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_sent boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_sent_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_retry_requested boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_retry_requested_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_last_attempt_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_last_error text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_sent boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_sent_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_retry_requested boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_retry_requested_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_last_attempt_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_last_error text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_signed boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_signed_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_document_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_invite_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_template_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_document_status text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_last_checked_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_signing_link text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_sent boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_sent_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_retry_requested boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_retry_requested_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_last_attempt_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_last_error text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS paid boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_created boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_created_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_retry_requested boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_retry_requested_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_last_attempt_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_last_error text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS primary_service text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS additional1 text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS additional2 text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS primary_service_key text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS additional1_service_key text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS additional2_service_key text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_template_key text NOT NULL DEFAULT 'general_booking';

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_required boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ALTER COLUMN terms_required SET DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS building_type text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS stories text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS bedrooms text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS bathrooms text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS monolithic text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS outbuilding text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS occupied text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS attached_flat text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS travel_fee text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_bedrooms text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS meth_samples text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_reinspect text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS council_files text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS foundation_space text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS weathertightness text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_reinspect_date text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS access_by text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_compliance text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS notes text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS directions text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS instructions text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_salutation text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_first_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_last_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_email text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_cellular text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact2_salutation text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact2_first_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact2_last_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact2_email text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact2_cellular text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS extracted_at_utc text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS connector_version text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS source_instance text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS raw_payload_json text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_workflow_sent boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_workflow_sent_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_retry_requested boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_retry_requested_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_last_attempt_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_last_error text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS marked_as_paid_override boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_available boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS job_total decimal(10,2) NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS amount_paid decimal(10,2) NOT NULL DEFAULT 0;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS amount_outstanding decimal(10,2) NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS payment_status text NOT NULL DEFAULT 'unpaid';

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS workflow_updated_at timestamptz NOT NULL DEFAULT NOW();

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT NOW();

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT NOW();
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Workflow and payload columns ensured"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Ensure columns failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// ENSURE INTEGRATION TABLES
// =============================
app.MapPost("/integrations/ensure-tables", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureInspectorIntegrationsTableAsync(conn);

        return Results.Ok(new
        {
            success = true,
            message = "Integration tables ensured"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Ensure integration tables failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// ENSURE MAPPING TABLES
// =============================
app.MapPost("/mappings/ensure-tables", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureMappingTablesAsync(conn);

        return Results.Ok(new
        {
            success = true,
            message = "Mapping tables ensured"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Ensure mapping tables failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// V1 MAPPING TEMPLATE
// =============================
app.MapGet("/mappings/v1-template", () => Results.Ok(new
{
    success = true,
    pricing_authority = "THREED tblItem",
    modifier_pricing = "capture_only",
    workflow_state_source = "Railway",
    fields = V1MappingFields
}));

app.MapGet("/integrations/branz/lookup", () => Results.Json(new { success = false, message = "Address-based lookup is disabled. Use the owned job refresh endpoint." }, statusCode: 410));
app.MapGet("/integrations/property-features/lookup", () => Results.Json(new { success = false, message = "Address-based lookup is disabled. Use the owned job refresh endpoint." }, statusCode: 410));

app.MapGet("/jobs/{jobId}/online-property-data", async (Guid jobId, Guid? tenantId) =>
{
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
    var current = await LoadOnlinePropertyDataAsync(conn, jobId, tenantId);
    return current == null ? Results.NotFound(new { success = false, message = "Job not found for this company." }) : Results.Ok(current);
});

app.MapPost("/jobs/{jobId}/online-property/refresh", async (Guid jobId, string source, bool force, Guid? tenantId) =>
{
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
    var job = await LoadOnlinePropertyJobAsync(conn, jobId, tenantId);
    if (job == null) return Results.NotFound(new { success = false, message = "Job not found for this company." });
    if (!await HasOnlinePropertyEntitlementAsync(conn, job.Value.TenantId, job.Value.InspectorId))
        return Results.Json(new { success = false, status = "subscription_required", message = "An active AutoMate subscription is required." }, statusCode: 403);

    var normalizedSource = (source ?? "").Trim().ToLowerInvariant();
    if (normalizedSource != "branz" && normalizedSource != "property-features")
        return Results.BadRequest(new { success = false, message = "source must be branz or property-features." });

    var fingerprint = StructuredAddressResolver.Fingerprint(job.Value.Address);
    var allowance = await RegisterOnlinePropertyAddressAsync(conn, jobId, job.Value.TenantId, fingerprint, job.Value.Address);
    if (!allowance.Allowed)
        return Results.Json(new { success = false, status = "limit_reached", message = "This inspection has already used its original address and two corrections." }, statusCode: 429);

    var cached = await LoadSuccessfulOnlinePropertyResultAsync(conn, jobId, normalizedSource, fingerprint);
    if (cached != null) return Results.Ok(cached);

    var retryGate = await GetOnlinePropertyFailureRetryGateAsync(conn, jobId, normalizedSource, fingerprint);
    if (!retryGate.Allowed)
        return Results.Json(new
        {
            success = false,
            status = retryGate.DailyLimitReached ? "failed_attempt_limit" : "retry_wait",
            retryAfterSeconds = retryGate.RetryAfterSeconds,
            failedAttemptsToday = retryGate.FailedAttemptsToday,
            message = retryGate.DailyLimitReached ? "This source has reached five failed attempts for this address today." : "The previous failed lookup can be retried after 60 seconds."
        }, statusCode: 429);

    if (normalizedSource == "property-features")
    {
        var result = await PropertyFeaturesLookupService.LookupAsync(job.Value.Address, false);
        await StorePropertyFeaturesResultAsync(conn, jobId, result);
        await AuditOnlinePropertyLookupAsync(conn, jobId, job.Value.TenantId, normalizedSource, fingerprint, "manual_retry", result.Status, result.Error);
        return result.Status == "available" ? Results.Ok(result) : Results.Json(result, statusCode: 422);
    }

    var branz = await BranzLookupService.LookupAsync(job.Value.Address, null, null, false);
    await StoreBranzResultAsync(conn, jobId, branz);
    await AuditOnlinePropertyLookupAsync(conn, jobId, job.Value.TenantId, normalizedSource, fingerprint, "manual_retry", branz.Status, branz.Error);
    return branz.Status == "available" ? Results.Ok(branz) : Results.Json(branz, statusCode: 422);
});

app.MapPost("/admin/jobs/{jobId}/online-property/force-refresh", async (HttpContext context, Guid jobId, string source, Guid? tenantId) =>
{
    var configuredKey = builder.Configuration["AUTOMATE_ADMIN_API_KEY"] ?? Environment.GetEnvironmentVariable("AUTOMATE_ADMIN_API_KEY") ?? "";
    var suppliedKey = context.Request.Headers["X-AutoMate-Admin-Key"].ToString();
    if (configuredKey.Length == 0) return Results.Json(new { success = false, message = "Administrator refresh is not configured." }, statusCode: 503);
    if (configuredKey.Length != suppliedKey.Length || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(configuredKey), Encoding.UTF8.GetBytes(suppliedKey)))
        return Results.Json(new { success = false, message = "Administrator authorization failed." }, statusCode: 403);

    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await EnsureOnlinePropertyTablesAsync(conn);
    var job = await LoadOnlinePropertyJobAsync(conn, jobId, tenantId);
    if (job == null) return Results.NotFound(new { success = false, message = "Job not found for this company." });
    var normalizedSource = (source ?? "").Trim().ToLowerInvariant();
    if (normalizedSource != "branz" && normalizedSource != "property-features") return Results.BadRequest(new { success = false, message = "source must be branz or property-features." });
    var fingerprint = StructuredAddressResolver.Fingerprint(job.Value.Address);
    if (!await IsRegisteredOnlinePropertyAddressAsync(conn, jobId, fingerprint))
        return Results.BadRequest(new { success = false, message = "Administrator refresh is limited to an already accepted job address." });

    if (normalizedSource == "property-features")
    {
        var result = await PropertyFeaturesLookupService.LookupAsync(job.Value.Address, true); await StorePropertyFeaturesResultAsync(conn, jobId, result);
        await AuditOnlinePropertyLookupAsync(conn, jobId, job.Value.TenantId, normalizedSource, fingerprint, "administrator_force", result.Status, result.Error);
        return result.Status == "available" ? Results.Ok(result) : Results.Json(result, statusCode: 422);
    }
    var branz = await BranzLookupService.LookupAsync(job.Value.Address, null, null, true); await StoreBranzResultAsync(conn, jobId, branz);
    await AuditOnlinePropertyLookupAsync(conn, jobId, job.Value.TenantId, normalizedSource, fingerprint, "administrator_force", branz.Status, branz.Error);
    return branz.Status == "available" ? Results.Ok(branz) : Results.Json(branz, statusCode: 422);
});

app.MapGet("/jobs/{jobId}/change-review", async (Guid jobId, Guid? tenantId) =>
{
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await JobChangeSupport.EnsureAsync(conn);
    await using var cmd = new NpgsqlCommand(@"SELECT tenant_id,change_review_pending,pending_change_json,pending_change_fingerprint,pending_change_reasons,
change_detected_at,approved_snapshot_version,xero_review_required,report_review_required,change_template_setup_required,source_missing,unscheduled
 ,approved_snapshot_json::text AS approved_snapshot_text,current_snapshot_json::text AS current_snapshot_text
FROM public.jobs_staging WHERE job_id=@job AND (@tenant IS NULL OR tenant_id::text=@tenant)", conn);
    cmd.Parameters.AddWithValue("job", jobId); cmd.Parameters.Add("tenant", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId?.ToString() ?? (object)DBNull.Value;
    await using var reader = await cmd.ExecuteReaderAsync(); if (!await reader.ReadAsync()) return Results.NotFound(new { success = false, message = "Job not found for this company." });
    object? changes = null;
    if (reader["approved_snapshot_text"] != DBNull.Value && reader["current_snapshot_text"] != DBNull.Value)
    {
        var reconstructed = JobChangeSupport.Diff(reader["approved_snapshot_text"].ToString() ?? "{}", reader["current_snapshot_text"].ToString() ?? "{}");
        if (reconstructed.Count > 0) changes = reconstructed.Select(change => new
        {
            field = change.field,
            oldValue = change.oldValue,
            newValue = change.newValue,
            category = change.category
        }).ToArray();
    }
    if (changes == null && reader["pending_change_json"] != DBNull.Value) changes = JsonSerializer.Deserialize<object>(reader["pending_change_json"].ToString() ?? "[]");
    return Results.Ok(new { success = true, jobId, changeReviewPending = (bool)reader["change_review_pending"], changes,
        revision = reader["pending_change_fingerprint"]?.ToString() ?? "", reasons = reader["pending_change_reasons"]?.ToString() ?? "",
        detectedAt = reader["change_detected_at"] == DBNull.Value ? null : reader["change_detected_at"], approvedVersion = reader["approved_snapshot_version"],
        xeroReviewRequired = (bool)reader["xero_review_required"], reportReviewRequired = (bool)reader["report_review_required"],
        templateSetupRequired = (bool)reader["change_template_setup_required"],
        sourceMissing = (bool)reader["source_missing"], unscheduled = (bool)reader["unscheduled"] });
});

app.MapPost("/jobs/{jobId}/confirm-change-review", async (Guid jobId, Guid? tenantId, string? confirmedBy) =>
{
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await JobChangeSupport.EnsureAsync(conn);
    await using var tx = await conn.BeginTransactionAsync(); string reasons = "", changes = "[]", fingerprint = "", snapshot = "{}"; int version = 0; Guid rowTenant = Guid.Empty;
    await using (var select = new NpgsqlCommand(@"SELECT tenant_id,change_review_pending,pending_change_reasons,pending_change_json::text,pending_change_fingerprint,
current_snapshot_json::text,approved_snapshot_version FROM public.jobs_staging WHERE job_id=@job AND (@tenant IS NULL OR tenant_id::text=@tenant) FOR UPDATE", conn, tx))
    {
        select.Parameters.AddWithValue("job", jobId); select.Parameters.Add("tenant", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId?.ToString() ?? (object)DBNull.Value;
        await using var reader = await select.ExecuteReaderAsync(); if (!await reader.ReadAsync()) return Results.NotFound(new { success = false, message = "Job not found for this company." });
        if (!(bool)reader["change_review_pending"]) return Results.Ok(new { success = true, alreadyConfirmed = true });
        Guid.TryParse(reader["tenant_id"]?.ToString(), out rowTenant); reasons = reader["pending_change_reasons"]?.ToString() ?? ""; changes = reader["pending_change_json"]?.ToString() ?? "[]";
        fingerprint = reader["pending_change_fingerprint"]?.ToString() ?? ""; snapshot = reader["current_snapshot_json"]?.ToString() ?? "{}"; version = Convert.ToInt32(reader["approved_snapshot_version"]) + 1;
    }
    var categories = reasons.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToHashSet(StringComparer.OrdinalIgnoreCase);
    bool email = categories.Overlaps(new[] { "services", "scope", "schedule" });
    bool unsupportedBasicEmailChange = categories.Overlaps(new[] { "address", "customer", "price" });
    bool terms = categories.Overlaps(new[] { "address", "services", "scope", "customer" });
    bool calendar = categories.Overlaps(new[] { "address", "services", "scope", "schedule", "customer", "operational" });
    await using (var update = new NpgsqlCommand(@"UPDATE public.jobs_staging SET
booking_email_sent=CASE WHEN @email AND booking_email_required THEN false ELSE booking_email_sent END,
booking_email_retry_requested=CASE WHEN @email AND booking_email_required THEN true ELSE booking_email_retry_requested END,
terms_sent=CASE WHEN @terms AND terms_required THEN false ELSE terms_sent END,terms_signed=CASE WHEN @terms AND terms_required THEN false ELSE terms_signed END,
terms_retry_requested=CASE WHEN @terms AND terms_required THEN true ELSE terms_retry_requested END,
signnow_document_status=CASE WHEN @terms AND terms_required THEN 'superseded-change-review' ELSE signnow_document_status END,
calendar_created=CASE WHEN @calendar AND calendar_required THEN false ELSE calendar_created END,
calendar_retry_requested=CASE WHEN @calendar AND calendar_required THEN true ELSE calendar_retry_requested END,
approved_snapshot_json=CAST(@snapshot AS jsonb),approved_snapshot_fingerprint=@fingerprint,approved_snapshot_version=@version,
change_review_pending=false,pending_change_json=NULL,pending_change_fingerprint=NULL,pending_change_reasons=NULL,
change_detected_at=NULL,xero_review_change_owned=false,report_review_change_owned=false,
change_template_setup_required=(change_template_setup_required OR @template_setup),
change_confirmed_at=NOW(),change_confirmed_by=@actor,address_change_pending=false,workflow_updated_at=NOW()
WHERE job_id=@job", conn, tx))
    {
        update.Parameters.AddWithValue("job", jobId); update.Parameters.AddWithValue("email", email); update.Parameters.AddWithValue("terms", terms); update.Parameters.AddWithValue("calendar", calendar); update.Parameters.AddWithValue("template_setup", unsupportedBasicEmailChange);
        update.Parameters.AddWithValue("snapshot", snapshot); update.Parameters.AddWithValue("fingerprint", fingerprint); update.Parameters.AddWithValue("version", version); update.Parameters.AddWithValue("actor", confirmedBy ?? "Connector user");
        await update.ExecuteNonQueryAsync();
    }
    if (email)
    {
        await using var actions = new NpgsqlCommand(@"UPDATE public.job_workflow_actions SET status='pending',retry_requested=true,retry_requested_at=NOW(),updated_at=NOW()
WHERE job_id=@job AND action_type='booking_email' AND status <> 'superseded'", conn, tx);
        actions.Parameters.AddWithValue("job", jobId); await actions.ExecuteNonQueryAsync();
    }
    await tx.CommitAsync();
    await JobChangeSupport.AuditAsync(conn, jobId, rowTenant, version, "confirmed", fingerprint, changes, reasons, confirmedBy ?? "Connector user");
    return Results.Ok(new { success = true, reasons, queued = new { email, terms, calendar }, xeroUnchanged = true });
});

app.MapPost("/jobs/{jobId}/manual-review/{reviewType}/complete", async (Guid jobId, string reviewType, Guid? tenantId, string? completedBy) =>
{
    string assignments = reviewType.Trim().ToLowerInvariant() switch { "xero" => "xero_review_required=false,xero_review_change_owned=false", "report" => "report_review_required=false,report_review_change_owned=false", "template" => "change_template_setup_required=false", _ => "" };
    if (assignments.Length == 0) return Results.BadRequest(new { success = false, message = "reviewType must be xero, report or template." });
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await JobChangeSupport.EnsureAsync(conn);
    await using var cmd = new NpgsqlCommand($"UPDATE public.jobs_staging SET {assignments} WHERE job_id=@job AND (@tenant IS NULL OR tenant_id::text=@tenant)", conn);
    cmd.Parameters.AddWithValue("job", jobId); cmd.Parameters.Add("tenant", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId?.ToString() ?? (object)DBNull.Value;
    int rows = await cmd.ExecuteNonQueryAsync(); return rows == 0 ? Results.NotFound() : Results.Ok(new { success = true, reviewType, completedBy });
});

app.MapPost("/jobs/{jobId}/cancel-unschedule", async (Guid jobId, Guid? tenantId, string? cancelledBy, bool sourceMissing) =>
{
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await JobChangeSupport.EnsureAsync(conn);
    await using (var owner = new NpgsqlCommand("SELECT 1 FROM public.jobs_staging WHERE job_id=@job AND (@tenant IS NULL OR tenant_id::text=@tenant)", conn))
    { owner.Parameters.AddWithValue("job", jobId); owner.Parameters.Add("tenant", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId?.ToString() ?? (object)DBNull.Value; if (await owner.ExecuteScalarAsync() == null) return Results.NotFound(new { success = false, message = "Job not found for this company." }); }
    var job = await LoadScheduleJobAsync(conn, jobId); if (job == null) return Results.NotFound();
    var calendar = await CancelGoogleCalendarEventForJobAsync(conn, job, builder.Configuration);
    await using (var cmd = new NpgsqlCommand(@"UPDATE public.jobs_staging SET unscheduled=true,source_missing=@missing,source_missing_at=CASE WHEN @missing THEN NOW() ELSE source_missing_at END,
change_review_pending=false,booking_email_retry_requested=false,terms_retry_requested=false,invoice_retry_requested=false,calendar_retry_requested=false,report_retry_requested=false,
xero_review_required=(xero_review_required OR invoice_sent),workflow_updated_at=NOW() WHERE job_id=@job", conn))
    { cmd.Parameters.AddWithValue("job", jobId); cmd.Parameters.AddWithValue("missing", sourceMissing); await cmd.ExecuteNonQueryAsync(); }
    await using (var actions = new NpgsqlCommand("UPDATE public.job_workflow_actions SET status='superseded',retry_requested=false,updated_at=NOW() WHERE job_id=@job AND status <> 'sent'", conn))
    { actions.Parameters.AddWithValue("job", jobId); await actions.ExecuteNonQueryAsync(); }
    return Results.Ok(new { success = calendar.Success, jobId, unscheduled = true, sourceMissing, calendar, xeroUnchanged = true, cancelledBy });
});

app.MapPost("/jobs/{jobId}/confirm-address-change", async (Guid jobId, Guid? tenantId, string? confirmedBy) =>
{
    await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
    await EnsureOnlinePropertyTablesAsync(conn); await EnsureSignNowJobColumnsAsync(conn);
    await using var tx = await conn.BeginTransactionAsync();
    const string selectSql = @"SELECT tenant_id,site_address,previous_site_address,address_change_pending,
booking_email_required,booking_email_sent,terms_required,terms_sent,terms_signed,signnow_document_id,
invoice_sent,paid,calendar_required,calendar_created,report_workflow_sent,report_sent
FROM public.jobs_staging WHERE job_id=@job_id AND (@tenant_id IS NULL OR tenant_id::text=@tenant_id) FOR UPDATE";
    var snapshot = new Dictionary<string, object?>();
    await using (var select = new NpgsqlCommand(selectSql, conn, tx))
    {
        select.Parameters.AddWithValue("job_id", jobId); select.Parameters.Add("tenant_id", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId.HasValue ? tenantId.Value.ToString() : DBNull.Value;
        await using var reader = await select.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return Results.NotFound(new { success = false, message = "Job not found for this company." });
        if (!(reader["address_change_pending"] != DBNull.Value && (bool)reader["address_change_pending"]))
            return Results.Ok(new { success = true, alreadyConfirmed = true, message = "This address change is already confirmed." });
        foreach (var name in new[] { "booking_email_sent", "terms_sent", "terms_signed", "signnow_document_id", "invoice_sent", "paid", "calendar_created", "report_workflow_sent", "report_sent" }) snapshot[name] = reader[name] == DBNull.Value ? null : reader[name];
        snapshot["tenant_id"] = reader["tenant_id"] == DBNull.Value ? null : reader["tenant_id"];
        snapshot["site_address"] = reader["site_address"]?.ToString() ?? ""; snapshot["previous_site_address"] = reader["previous_site_address"]?.ToString() ?? "";
        snapshot["booking_required"] = reader["booking_email_required"]; snapshot["terms_required"] = reader["terms_required"]; snapshot["calendar_required"] = reader["calendar_required"];
    }
    Guid.TryParse(Convert.ToString(snapshot["tenant_id"]), out var snapshotTenantId);
    await using (var audit = new NpgsqlCommand(@"INSERT INTO public.address_change_audit(job_id,tenant_id,previous_address,new_address,confirmed_by,prior_workflow_json)
VALUES(@job_id,@tenant_id,@previous,@new,@confirmed_by,CAST(@json AS jsonb))", conn, tx))
    {
        audit.Parameters.AddWithValue("job_id", jobId); audit.Parameters.AddWithValue("tenant_id", snapshotTenantId == Guid.Empty ? DBNull.Value : snapshotTenantId); audit.Parameters.AddWithValue("previous", snapshot["previous_site_address"] ?? ""); audit.Parameters.AddWithValue("new", snapshot["site_address"] ?? ""); audit.Parameters.AddWithValue("confirmed_by", confirmedBy ?? "Connector user"); audit.Parameters.AddWithValue("json", JsonSerializer.Serialize(snapshot)); await audit.ExecuteNonQueryAsync();
    }
    const string updateSql = @"UPDATE public.jobs_staging SET
booking_email_sent=CASE WHEN booking_email_required THEN false ELSE booking_email_sent END,
booking_email_sent_at=CASE WHEN booking_email_required THEN NULL ELSE booking_email_sent_at END,
booking_email_retry_requested=booking_email_required,booking_email_retry_requested_at=CASE WHEN booking_email_required THEN NOW() ELSE NULL END,
terms_sent=CASE WHEN terms_required THEN false ELSE terms_sent END,terms_sent_at=CASE WHEN terms_required THEN NULL ELSE terms_sent_at END,
terms_signed=CASE WHEN terms_required THEN false ELSE terms_signed END,terms_signed_at=CASE WHEN terms_required THEN NULL ELSE terms_signed_at END,
terms_retry_requested=terms_required,terms_retry_requested_at=CASE WHEN terms_required THEN NOW() ELSE NULL END,
signnow_document_id=CASE WHEN terms_required THEN NULL ELSE signnow_document_id END,
signnow_invite_id=CASE WHEN terms_required THEN NULL ELSE signnow_invite_id END,
signnow_document_status=CASE WHEN terms_required THEN 'superseded-address-change' ELSE signnow_document_status END,
calendar_created=CASE WHEN calendar_required THEN false ELSE calendar_created END,
calendar_retry_requested=calendar_required,calendar_retry_requested_at=CASE WHEN calendar_required THEN NOW() ELSE NULL END,
address_change_confirmed_at=NOW(),address_change_confirmed_by=@confirmed_by,workflow_updated_at=NOW()
WHERE job_id=@job_id";
    await using (var update = new NpgsqlCommand(updateSql, conn, tx)) { update.Parameters.AddWithValue("job_id", jobId); update.Parameters.AddWithValue("confirmed_by", confirmedBy ?? "Connector user"); await update.ExecuteNonQueryAsync(); }
    await tx.CommitAsync();
    return Results.Ok(new { success = true, message = "Corrected booking email, Terms and Calendar update queued. Xero was not changed.", invoiceUnchanged = true, reportNeedsReview = Convert.ToString(snapshot["report_sent"]) is not null and not "" });
});

// =============================
// CONNECTOR DISCOVERY SYNC
// =============================
app.MapPost("/inspectors/{inspectorId}/mappings/discovery", async (Guid inspectorId, MappingDiscoverySyncRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureMappingTablesAsync(conn);

        await using var tx = await conn.BeginTransactionAsync();

        int mappingCount = 0;
        var mappings = request.FieldMappings.Count > 0
            ? request.FieldMappings
            : V1MappingFields.Select(f => new MappingFieldInput
            {
                CanonicalFieldName = f.CanonicalFieldName,
                ThreedColumnName = f.ThreedColumnName,
                ThreedLabel = f.ThreedLabel,
                CanAffectPricing = f.CanAffectPricing,
                V1Enabled = f.V1Enabled,
                ServiceScope = f.ServiceScope,
                Notes = f.Notes
            }).ToList();

        foreach (var mapping in mappings)
        {
            await UpsertMappingFieldAsync(conn, tx, inspectorId, mapping, false);
            mappingCount++;
        }

        int catalogCount = 0;
        foreach (var item in request.ServiceCatalogItems)
        {
            await UpsertServiceCatalogItemAsync(conn, tx, inspectorId, item);
            catalogCount++;
        }

        const string syncSql = @"
INSERT INTO public.mapping_discovery_syncs
(
    inspector_id,
    connector_version,
    source_instance,
    field_mapping_count,
    service_catalog_count,
    raw_payload_json,
    created_at
)
VALUES
(
    @inspector_id,
    @connector_version,
    @source_instance,
    @field_mapping_count,
    @service_catalog_count,
    CAST(@raw_payload_json AS jsonb),
    NOW()
);";

        await using (var cmd = new NpgsqlCommand(syncSql, conn, tx))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            cmd.Parameters.AddWithValue("connector_version", request.ConnectorVersion ?? "");
            cmd.Parameters.AddWithValue("source_instance", request.SourceInstance ?? "");
            cmd.Parameters.AddWithValue("field_mapping_count", mappingCount);
            cmd.Parameters.AddWithValue("service_catalog_count", catalogCount);
            cmd.Parameters.AddWithValue("raw_payload_json", JsonSerializer.Serialize(request));
            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Mapping discovery synced",
            inspector_id = inspectorId,
            field_mapping_count = mappingCount,
            service_catalog_count = catalogCount
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mapping discovery sync failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// CONFIRM INSPECTOR MAPPINGS
// =============================
app.MapPost("/inspectors/{inspectorId}/mappings/confirm", async (Guid inspectorId, ConfirmMappingsRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureMappingTablesAsync(conn);

        await using var tx = await conn.BeginTransactionAsync();

        int mappingCount = 0;
        foreach (var mapping in request.FieldMappings)
        {
            await UpsertMappingFieldAsync(conn, tx, inspectorId, mapping, true);
            mappingCount++;
        }

        await tx.CommitAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Mappings confirmed",
            inspector_id = inspectorId,
            confirmed_mapping_count = mappingCount
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Confirm mappings failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// GET INSPECTOR MAPPING PROFILE
// =============================
app.MapGet("/inspectors/{inspectorId}/mappings/profile", async (Guid inspectorId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureMappingTablesAsync(conn);

        var mappings = new List<object>();
        const string mappingSql = @"
SELECT
    canonical_field_name,
    threed_column_name,
    threed_label,
    source_table_name,
    source_list_name,
    invoice_item_id,
    invoice_item_name,
    pricing_affects,
    v1_enabled,
    service_scope,
    notes,
    is_confirmed,
    updated_at
FROM public.inspector_field_mappings
WHERE inspector_id = @inspector_id
ORDER BY threed_column_name;";

        await using (var cmd = new NpgsqlCommand(mappingSql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                mappings.Add(new
                {
                    canonical_field_name = reader["canonical_field_name"]?.ToString(),
                    threed_column_name = reader["threed_column_name"]?.ToString(),
                    threed_label = reader["threed_label"]?.ToString(),
                    source_table_name = reader["source_table_name"]?.ToString(),
                    source_list_name = reader["source_list_name"]?.ToString(),
                    invoice_item_id = reader["invoice_item_id"]?.ToString(),
                    invoice_item_name = reader["invoice_item_name"]?.ToString(),
                    pricing_affects = reader["pricing_affects"]?.ToString(),
                    v1_enabled = reader["v1_enabled"]?.ToString(),
                    service_scope = reader["service_scope"]?.ToString(),
                    notes = reader["notes"]?.ToString(),
                    is_confirmed = reader["is_confirmed"]?.ToString(),
                    updated_at = reader["updated_at"]?.ToString()
                });
            }
        }

        var serviceCatalog = new List<object>();
        const string catalogSql = @"
SELECT
    catalog_item_key,
    list_item_id,
    list_item_name,
    list_name,
    invoice_item_id,
    invoice_item_name,
    unit_price,
    is_active,
    canonical_service_type,
    booking_template_key,
    pricing_affects,
    booking_email_required,
    terms_required,
    invoice_required,
    calendar_required,
    report_required,
    pricing_authority,
    last_synced_at
FROM public.inspector_service_catalog
WHERE inspector_id = @inspector_id
ORDER BY list_name, list_item_name, invoice_item_name;";

        await using (var cmd = new NpgsqlCommand(catalogSql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                serviceCatalog.Add(new
                {
                    catalog_item_key = reader["catalog_item_key"]?.ToString(),
                    list_item_id = reader["list_item_id"]?.ToString(),
                    list_item_name = reader["list_item_name"]?.ToString(),
                    list_name = reader["list_name"]?.ToString(),
                    invoice_item_id = reader["invoice_item_id"]?.ToString(),
                    invoice_item_name = reader["invoice_item_name"]?.ToString(),
                    unit_price = reader["unit_price"]?.ToString(),
                    is_active = reader["is_active"]?.ToString(),
                    canonical_service_type = reader["canonical_service_type"]?.ToString(),
                    booking_template_key = reader["booking_template_key"]?.ToString(),
                    pricing_affects = reader["pricing_affects"]?.ToString(),
                    booking_email_required = reader["booking_email_required"]?.ToString(),
                    terms_required = reader["terms_required"]?.ToString(),
                    invoice_required = reader["invoice_required"]?.ToString(),
                    calendar_required = reader["calendar_required"]?.ToString(),
                    report_required = reader["report_required"]?.ToString(),
                    pricing_authority = reader["pricing_authority"]?.ToString(),
                    last_synced_at = reader["last_synced_at"]?.ToString()
                });
            }
        }

        return Results.Ok(new
        {
            success = true,
            inspector_id = inspectorId,
            pricing_authority = "THREED tblItem",
            modifier_pricing = "capture_only",
            mappings,
            service_catalog = serviceCatalog
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Get mapping profile failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MICROSOFT CONNECT URL
// =============================
app.MapGet("/integrations/microsoft/connect-url", (string inspectorId) =>
{
    var clientId = builder.Configuration["MS_CLIENT_ID"];
    var redirectUri = builder.Configuration["MS_REDIRECT_URI"];

    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(redirectUri))
    {
        return Results.Problem(
            title: "Microsoft config missing",
            detail: "MS_CLIENT_ID and/or MS_REDIRECT_URI are missing from Railway variables.",
            statusCode: 500
        );
    }

    if (!Guid.TryParse(inspectorId.Trim(), out _))
    {
        return Results.BadRequest(new
        {
            success = false,
            message = "Invalid inspectorId"
        });
    }

    var scopes = "offline_access Mail.Send User.Read";

    var url =
        "https://login.microsoftonline.com/common/oauth2/v2.0/authorize" +
        $"?client_id={Uri.EscapeDataString(clientId)}" +
        $"&response_type=code" +
        $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
        $"&response_mode=query" +
        $"&scope={Uri.EscapeDataString(scopes)}" +
        $"&state={Uri.EscapeDataString(inspectorId.Trim())}";

    return Results.Ok(new
    {
        success = true,
        inspectorId = inspectorId.Trim(),
        url
    });
});

// =============================
// MICROSOFT CALLBACK
// =============================
app.MapGet("/api/integrations/microsoft/callback", async (HttpContext context) =>
{
    try
    {
        var code = context.Request.Query["code"].ToString();
        var state = context.Request.Query["state"].ToString();

        if (string.IsNullOrWhiteSpace(code))
        {
            return Results.BadRequest("Missing code");
        }

        if (!Guid.TryParse(state, out Guid inspectorId))
        {
            return Results.BadRequest("Invalid inspector ID in state");
        }

        var clientId = builder.Configuration["MS_CLIENT_ID"];
        var clientSecret = builder.Configuration["MS_CLIENT_SECRET"];
        var redirectUri = builder.Configuration["MS_REDIRECT_URI"];

        if (string.IsNullOrWhiteSpace(clientId) ||
            string.IsNullOrWhiteSpace(clientSecret) ||
            string.IsNullOrWhiteSpace(redirectUri))
        {
            return Results.Problem(
                title: "Microsoft config missing",
                detail: "MS_CLIENT_ID, MS_CLIENT_SECRET and/or MS_REDIRECT_URI are missing from Railway variables.",
                statusCode: 500
            );
        }

        using var httpClient = new HttpClient();

        var tokenResponse = await httpClient.PostAsync(
            "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["code"] = code,
                ["redirect_uri"] = redirectUri,
                ["grant_type"] = "authorization_code"
            }));

        var tokenJson = await tokenResponse.Content.ReadAsStringAsync();

        if (!tokenResponse.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "Microsoft token exchange failed",
                detail: tokenJson,
                statusCode: 500
            );
        }

        var tokenDoc = JsonDocument.Parse(tokenJson).RootElement;

        var accessToken = tokenDoc.GetProperty("access_token").GetString() ?? "";
        var refreshToken = tokenDoc.TryGetProperty("refresh_token", out var refreshTokenProp)
            ? refreshTokenProp.GetString() ?? ""
            : "";
        var expiresIn = tokenDoc.TryGetProperty("expires_in", out var expiresInProp)
            ? expiresInProp.GetInt32()
            : 3600;

        var expiresAt = DateTime.UtcNow.AddSeconds(expiresIn);

        string? externalAccountEmail = null;

        httpClient.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", accessToken);

        var meResponse = await httpClient.GetAsync("https://graph.microsoft.com/v1.0/me");
        if (meResponse.IsSuccessStatusCode)
        {
            var meJson = await meResponse.Content.ReadAsStringAsync();
            var meDoc = JsonDocument.Parse(meJson).RootElement;

            if (meDoc.TryGetProperty("mail", out var mailProp))
            {
                externalAccountEmail = mailProp.GetString();
            }

            if (string.IsNullOrWhiteSpace(externalAccountEmail) &&
                meDoc.TryGetProperty("userPrincipalName", out var upnProp))
            {
                externalAccountEmail = upnProp.GetString();
            }
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureInspectorIntegrationsTableAsync(conn);

        const string upsertSql = @"
INSERT INTO public.inspector_integrations
(
    inspector_id,
    provider,
    status,
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    external_tenant_id,
    created_at,
    updated_at
)
VALUES
(
    @inspector_id,
    'microsoft',
    'connected',
    @access_token,
    @refresh_token,
    @expires_at,
    @external_account_email,
    NULL,
    NOW(),
    NOW()
)
ON CONFLICT (inspector_id, provider)
DO UPDATE SET
    status = 'connected',
    access_token_encrypted = EXCLUDED.access_token_encrypted,
    refresh_token_encrypted = EXCLUDED.refresh_token_encrypted,
    expires_at = EXCLUDED.expires_at,
    external_account_email = EXCLUDED.external_account_email,
    updated_at = NOW();
";

        await using (var cmd = new NpgsqlCommand(upsertSql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            cmd.Parameters.AddWithValue("access_token", accessToken);
            cmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
            cmd.Parameters.AddWithValue("expires_at", expiresAt);
            cmd.Parameters.AddWithValue("external_account_email", (object?)externalAccountEmail ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();
        }

        return Results.Content("Microsoft connected successfully. You can close this window.");
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Microsoft callback failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MICROSOFT SEND TEST EMAIL
// =============================
app.MapPost("/integrations/microsoft/send-test-email", async (SendTestEmailRequest request) =>
{
    try
    {
        if (!Guid.TryParse(request.InspectorId, out Guid inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid InspectorId"
            });
        }

        if (string.IsNullOrWhiteSpace(request.ToEmail))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "ToEmail is required"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await EnsureInspectorIntegrationsTableAsync(conn);

        const string sql = @"
SELECT
    ii.access_token_encrypted,
    ii.refresh_token_encrypted,
    ii.expires_at,
    ii.external_account_email,
    ii.status,
    i.company_name,
    i.contact_name,
    i.email_from_name,
    i.email_from_address,
    i.phone,
    i.logo_url
FROM public.inspector_integrations ii
LEFT JOIN public.inspectors i
    ON i.inspector_id = ii.inspector_id
WHERE ii.inspector_id = @inspector_id
  AND ii.provider = 'microsoft'
LIMIT 1;";

        string? accessToken = null;
        string? refreshToken = null;
        DateTime? expiresAt = null;
        string? externalAccountEmail = null;
        string? status = null;
        string? companyName = null;
        string? contactName = null;
        string? emailFromName = null;
        string? emailFromAddress = null;
        string? phone = null;
        string? logoUrl = null;

        await using (var cmd = new NpgsqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);

            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                accessToken = reader["access_token_encrypted"]?.ToString();
                refreshToken = reader["refresh_token_encrypted"]?.ToString();
                externalAccountEmail = reader["external_account_email"]?.ToString();
                status = reader["status"]?.ToString();
                companyName = reader["company_name"]?.ToString();
                contactName = reader["contact_name"]?.ToString();
                emailFromName = reader["email_from_name"]?.ToString();
                emailFromAddress = reader["email_from_address"]?.ToString();
                phone = reader["phone"]?.ToString();
                logoUrl = reader["logo_url"]?.ToString();

                if (reader["expires_at"] != DBNull.Value)
                {
                    expiresAt = Convert.ToDateTime(reader["expires_at"]);
                }
            }
        }

        if (string.IsNullOrWhiteSpace(accessToken) || !string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Microsoft is not connected for this inspector."
            });
        }

        if (expiresAt.HasValue && expiresAt.Value <= DateTime.UtcNow.AddMinutes(5))
        {
            var clientId = builder.Configuration["MS_CLIENT_ID"];
            var clientSecret = builder.Configuration["MS_CLIENT_SECRET"];
            var redirectUri = builder.Configuration["MS_REDIRECT_URI"];

            if (string.IsNullOrWhiteSpace(clientId) ||
                string.IsNullOrWhiteSpace(clientSecret) ||
                string.IsNullOrWhiteSpace(redirectUri))
            {
                return Results.Problem(
                    title: "Microsoft config missing",
                    detail: "MS_CLIENT_ID, MS_CLIENT_SECRET and/or MS_REDIRECT_URI are missing from Railway variables.",
                    statusCode: 500
                );
            }

            if (string.IsNullOrWhiteSpace(refreshToken))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Access token expired and no refresh token is stored."
                });
            }

            using var refreshClient = new HttpClient();

            var refreshResponse = await refreshClient.PostAsync(
                "https://login.microsoftonline.com/common/oauth2/v2.0/token",
                new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["client_id"] = clientId,
                    ["client_secret"] = clientSecret,
                    ["refresh_token"] = refreshToken,
                    ["grant_type"] = "refresh_token",
                    ["redirect_uri"] = redirectUri,
                    ["scope"] = "offline_access Mail.Send User.Read"
                }));

            var refreshJson = await refreshResponse.Content.ReadAsStringAsync();

            if (!refreshResponse.IsSuccessStatusCode)
            {
                return Results.Problem(
                    title: "Microsoft token refresh failed",
                    detail: refreshJson,
                    statusCode: 500
                );
            }

            var refreshDoc = JsonDocument.Parse(refreshJson).RootElement;

            accessToken = refreshDoc.GetProperty("access_token").GetString() ?? accessToken;
            refreshToken = refreshDoc.TryGetProperty("refresh_token", out var refreshedTokenProp)
                ? refreshedTokenProp.GetString() ?? refreshToken
                : refreshToken;

            var refreshedExpiresIn = refreshDoc.TryGetProperty("expires_in", out var refreshedExpiresInProp)
                ? refreshedExpiresInProp.GetInt32()
                : 3600;

            expiresAt = DateTime.UtcNow.AddSeconds(refreshedExpiresIn);

            const string updateSql = @"
UPDATE public.inspector_integrations
SET
    access_token_encrypted = @access_token,
    refresh_token_encrypted = @refresh_token,
    expires_at = @expires_at,
    updated_at = NOW()
WHERE inspector_id = @inspector_id
  AND provider = 'microsoft';";

            await using var updateCmd = new NpgsqlCommand(updateSql, conn);
            updateCmd.Parameters.AddWithValue("access_token", accessToken);
            updateCmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("expires_at", expiresAt.Value);
            updateCmd.Parameters.AddWithValue("inspector_id", inspectorId);
            await updateCmd.ExecuteNonQueryAsync();
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", accessToken);

        var emailBody = new
        {
            message = new
            {
                subject = string.IsNullOrWhiteSpace(request.Subject)
                    ? "3D AutoMate Test Email"
                    : request.Subject,
                body = new
                {
                    contentType = "HTML",
                    content = string.IsNullOrWhiteSpace(request.Body)
                        ? "This is a test email from 3D AutoMate."
                        : RenderTestEmailBody(
                            request.Body,
                            companyName,
                            contactName,
                            emailFromName,
                            emailFromAddress,
                            phone,
                            logoUrl)
                },
                toRecipients = new[]
                {
                    new
                    {
                        emailAddress = new
                        {
                            address = request.ToEmail
                        }
                    }
                }
            }
        };

        var response = await httpClient.PostAsJsonAsync(
            "https://graph.microsoft.com/v1.0/me/sendMail",
            emailBody);

        var responseText = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "Microsoft send mail failed",
                detail: responseText,
                statusCode: 500
            );
        }

        return Results.Ok(new
        {
            success = true,
            message = "Test email sent.",
            inspectorId = request.InspectorId,
            toEmail = request.ToEmail,
            fromAccount = externalAccountEmail
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Send test email failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// XERO CONNECT URL
// =============================
app.MapGet("/integrations/xero/connect-url", (string inspectorId) =>
{
    var clientId = builder.Configuration["XERO_CLIENT_ID"];
    var redirectUri = builder.Configuration["XERO_REDIRECT_URI"];

    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(redirectUri))
    {
        return Results.Problem(
            title: "Xero config missing",
            detail: "XERO_CLIENT_ID and/or XERO_REDIRECT_URI are missing from Railway variables.",
            statusCode: 500
        );
    }

    if (!Guid.TryParse(inspectorId.Trim(), out _))
    {
        return Results.BadRequest(new
        {
            success = false,
            message = "Invalid inspectorId"
        });
    }

    var scopes = "offline_access accounting.settings.read accounting.contacts accounting.invoices";

    var url =
        "https://login.xero.com/identity/connect/authorize" +
        $"?response_type=code" +
        $"&client_id={Uri.EscapeDataString(clientId)}" +
        $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
        $"&scope={Uri.EscapeDataString(scopes)}" +
        $"&state={Uri.EscapeDataString(inspectorId.Trim())}";

    return Results.Ok(new
    {
        success = true,
        inspectorId = inspectorId.Trim(),
        url
    });
});

// =============================
// XERO CALLBACK
// =============================
app.MapGet("/api/integrations/xero/callback", async (HttpContext context) =>
{
    try
    {
        var code = context.Request.Query["code"].ToString();
        var state = context.Request.Query["state"].ToString();
        var error = context.Request.Query["error"].ToString();
        var errorDescription = context.Request.Query["error_description"].ToString();
        var errorUri = context.Request.Query["error_uri"].ToString();

        if (!string.IsNullOrWhiteSpace(error))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Xero authorization failed.",
                error,
                errorDescription,
                errorUri,
                state
            });
        }

        if (string.IsNullOrWhiteSpace(code))
            return Results.BadRequest("Missing code");

        if (!Guid.TryParse(state, out Guid inspectorId))
            return Results.BadRequest("Invalid inspector ID in state");

        var clientId = builder.Configuration["XERO_CLIENT_ID"];
        var clientSecret = builder.Configuration["XERO_CLIENT_SECRET"];
        var redirectUri = builder.Configuration["XERO_REDIRECT_URI"];

        if (string.IsNullOrWhiteSpace(clientId) ||
            string.IsNullOrWhiteSpace(clientSecret) ||
            string.IsNullOrWhiteSpace(redirectUri))
        {
            return Results.Problem(
                title: "Xero config missing",
                detail: "XERO_CLIENT_ID, XERO_CLIENT_SECRET and/or XERO_REDIRECT_URI are missing from Railway variables.",
                statusCode: 500
            );
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(clientId + ":" + clientSecret)));

        var tokenResponse = await httpClient.PostAsync(
            "https://identity.xero.com/connect/token",
            new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "authorization_code",
                ["code"] = code,
                ["redirect_uri"] = redirectUri
            }));

        var tokenJson = await tokenResponse.Content.ReadAsStringAsync();

        if (!tokenResponse.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "Xero token exchange failed",
                detail: tokenJson,
                statusCode: 500
            );
        }

        var tokenDoc = JsonDocument.Parse(tokenJson).RootElement;
        var accessToken = tokenDoc.GetProperty("access_token").GetString() ?? "";
        var refreshToken = tokenDoc.TryGetProperty("refresh_token", out var refreshTokenProp)
            ? refreshTokenProp.GetString() ?? ""
            : "";
        var expiresIn = tokenDoc.TryGetProperty("expires_in", out var expiresInProp)
            ? expiresInProp.GetInt32()
            : 1800;
        var expiresAt = DateTime.UtcNow.AddSeconds(expiresIn);

        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        var connectionsResponse = await httpClient.GetAsync("https://api.xero.com/connections");
        var connectionsJson = await connectionsResponse.Content.ReadAsStringAsync();

        if (!connectionsResponse.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "Xero connections lookup failed",
                detail: connectionsJson,
                statusCode: 500
            );
        }

        var connections = JsonDocument.Parse(connectionsJson).RootElement;
        if (connections.ValueKind != JsonValueKind.Array || connections.GetArrayLength() == 0)
        {
            return Results.Problem(
                title: "No Xero tenants found",
                detail: "Xero connected, but no organisation/tenant was returned for this account.",
                statusCode: 500
            );
        }

        var firstTenant = connections[0];
        string tenantId = firstTenant.TryGetProperty("tenantId", out var tenantIdProp)
            ? tenantIdProp.GetString() ?? ""
            : "";
        string tenantName = firstTenant.TryGetProperty("tenantName", out var tenantNameProp)
            ? tenantNameProp.GetString() ?? ""
            : "";
        int tenantCount = connections.GetArrayLength();

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string upsertSql = @"
INSERT INTO public.inspector_integrations
(
    inspector_id,
    provider,
    status,
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    external_tenant_id,
    created_at,
    updated_at
)
VALUES
(
    @inspector_id,
    'xero',
    'connected',
    @access_token,
    @refresh_token,
    @expires_at,
    @tenant_name,
    @tenant_id,
    NOW(),
    NOW()
)
ON CONFLICT (inspector_id, provider)
DO UPDATE SET
    status = 'connected',
    access_token_encrypted = EXCLUDED.access_token_encrypted,
    refresh_token_encrypted = EXCLUDED.refresh_token_encrypted,
    expires_at = EXCLUDED.expires_at,
    external_account_email = EXCLUDED.external_account_email,
    external_tenant_id = EXCLUDED.external_tenant_id,
    updated_at = NOW();";

        await using (var cmd = new NpgsqlCommand(upsertSql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            cmd.Parameters.AddWithValue("access_token", accessToken);
            cmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
            cmd.Parameters.AddWithValue("expires_at", expiresAt);
            cmd.Parameters.AddWithValue("tenant_name", (object?)tenantName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("tenant_id", (object?)tenantId ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        var multipleNote = tenantCount > 1
            ? $"<p>Multiple Xero organisations were returned. 3D AutoMate selected the first one for this first pass: <strong>{WebUtility.HtmlEncode(tenantName)}</strong>.</p>"
            : "";

        return Results.Content(
            "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;padding:32px;\">" +
            "<h2>Xero connected successfully</h2>" +
            $"<p>Connected organisation: <strong>{WebUtility.HtmlEncode(tenantName)}</strong></p>" +
            multipleNote +
            "<p>You can close this window and return to 3D AutoMate.</p>" +
            "</body></html>",
            "text/html");
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Xero callback failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// XERO STATUS
// =============================
app.MapGet("/integrations/xero/status", async (string inspectorId) =>
{
    try
    {
        if (!Guid.TryParse(inspectorId.Trim(), out Guid parsedInspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid inspectorId"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string sql = @"
SELECT
    status,
    external_account_email,
    external_tenant_id,
    expires_at,
    updated_at
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = 'xero'
LIMIT 1;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("inspector_id", parsedInspectorId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return Results.Ok(new
            {
                success = true,
                connected = false,
                status = "Disconnected",
                tenantName = "",
                tenantId = "",
                lastSync = "",
                expiresAt = ""
            });
        }

        string status = reader["status"]?.ToString() ?? "disconnected";

        return Results.Ok(new
        {
            success = true,
            connected = string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase),
            status,
            tenantName = reader["external_account_email"]?.ToString() ?? "",
            tenantId = reader["external_tenant_id"]?.ToString() ?? "",
            lastSync = reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O"),
            expiresAt = reader["expires_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["expires_at"]).ToString("O")
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Xero status failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// GOOGLE CONNECT URL
// =============================
app.MapGet("/integrations/google/connect-url", (string inspectorId) =>
{
    var clientId = builder.Configuration["GOOGLE_CLIENT_ID"];
    var redirectUri = builder.Configuration["GOOGLE_REDIRECT_URI"];

    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(redirectUri))
    {
        return Results.Problem(
            title: "Google config missing",
            detail: "GOOGLE_CLIENT_ID and/or GOOGLE_REDIRECT_URI are missing from Railway variables.",
            statusCode: 500
        );
    }

    if (!Guid.TryParse(inspectorId.Trim(), out _))
    {
        return Results.BadRequest(new
        {
            success = false,
            message = "Invalid inspectorId"
        });
    }

    var scopes = "https://www.googleapis.com/auth/calendar.events https://www.googleapis.com/auth/calendar.calendarlist.readonly https://www.googleapis.com/auth/userinfo.email";

    var url =
        "https://accounts.google.com/o/oauth2/v2/auth" +
        $"?client_id={Uri.EscapeDataString(clientId)}" +
        $"&response_type=code" +
        $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
        $"&scope={Uri.EscapeDataString(scopes)}" +
        $"&access_type=offline" +
        $"&prompt=consent" +
        $"&state={Uri.EscapeDataString(inspectorId.Trim())}";

    return Results.Ok(new
    {
        success = true,
        inspectorId = inspectorId.Trim(),
        url
    });
});

// =============================
// GOOGLE CALLBACK
// =============================
app.MapGet("/api/integrations/google/callback", async (HttpContext context) =>
{
    try
    {
        var code = context.Request.Query["code"].ToString();
        var state = context.Request.Query["state"].ToString();
        var error = context.Request.Query["error"].ToString();

        if (!string.IsNullOrWhiteSpace(error))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Google authorization failed.",
                error,
                state
            });
        }

        if (string.IsNullOrWhiteSpace(code))
            return Results.BadRequest("Missing code");

        if (!Guid.TryParse(state, out Guid inspectorId))
            return Results.BadRequest("Invalid inspector ID in state");

        var clientId = builder.Configuration["GOOGLE_CLIENT_ID"];
        var clientSecret = builder.Configuration["GOOGLE_CLIENT_SECRET"];
        var redirectUri = builder.Configuration["GOOGLE_REDIRECT_URI"];

        if (string.IsNullOrWhiteSpace(clientId) ||
            string.IsNullOrWhiteSpace(clientSecret) ||
            string.IsNullOrWhiteSpace(redirectUri))
        {
            return Results.Problem(
                title: "Google config missing",
                detail: "GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET and/or GOOGLE_REDIRECT_URI are missing from Railway variables.",
                statusCode: 500
            );
        }

        using var httpClient = new HttpClient();
        var tokenResponse = await httpClient.PostAsync(
            "https://oauth2.googleapis.com/token",
            new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["code"] = code,
                ["redirect_uri"] = redirectUri,
                ["grant_type"] = "authorization_code"
            }));

        var tokenJson = await tokenResponse.Content.ReadAsStringAsync();

        if (!tokenResponse.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "Google token exchange failed",
                detail: tokenJson,
                statusCode: 500
            );
        }

        var tokenDoc = JsonDocument.Parse(tokenJson).RootElement;
        var accessToken = tokenDoc.GetProperty("access_token").GetString() ?? "";
        var refreshToken = tokenDoc.TryGetProperty("refresh_token", out var refreshTokenProp)
            ? refreshTokenProp.GetString() ?? ""
            : "";
        var expiresIn = tokenDoc.TryGetProperty("expires_in", out var expiresInProp)
            ? expiresInProp.GetInt32()
            : 3600;
        var expiresAt = DateTime.UtcNow.AddSeconds(expiresIn);

        string? accountEmail = null;
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        var userInfoResponse = await httpClient.GetAsync("https://www.googleapis.com/oauth2/v2/userinfo");
        if (userInfoResponse.IsSuccessStatusCode)
        {
            var userInfoJson = await userInfoResponse.Content.ReadAsStringAsync();
            var userInfo = JsonDocument.Parse(userInfoJson).RootElement;
            accountEmail = userInfo.TryGetProperty("email", out var emailProp)
                ? emailProp.GetString()
                : null;
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string upsertSql = @"
INSERT INTO public.inspector_integrations
(
    inspector_id,
    provider,
    status,
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    external_tenant_id,
    created_at,
    updated_at
)
VALUES
(
    @inspector_id,
    'google',
    'connected',
    @access_token,
    @refresh_token,
    @expires_at,
    @external_account_email,
    'primary',
    NOW(),
    NOW()
)
ON CONFLICT (inspector_id, provider)
DO UPDATE SET
    status = 'connected',
    access_token_encrypted = EXCLUDED.access_token_encrypted,
    refresh_token_encrypted = COALESCE(NULLIF(EXCLUDED.refresh_token_encrypted, ''), inspector_integrations.refresh_token_encrypted),
    expires_at = EXCLUDED.expires_at,
    external_account_email = EXCLUDED.external_account_email,
    external_tenant_id = EXCLUDED.external_tenant_id,
    updated_at = NOW();";

        await using (var cmd = new NpgsqlCommand(upsertSql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            cmd.Parameters.AddWithValue("access_token", accessToken);
            cmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
            cmd.Parameters.AddWithValue("expires_at", expiresAt);
            cmd.Parameters.AddWithValue("external_account_email", (object?)accountEmail ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        return Results.Content(
            "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;padding:32px;\">" +
            "<h2>Google Calendar connected successfully</h2>" +
            $"<p>Connected account: <strong>{WebUtility.HtmlEncode(accountEmail ?? "Google Calendar")}</strong></p>" +
            "<p>You can close this window and return to 3D AutoMate.</p>" +
            "</body></html>",
            "text/html");
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Google callback failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// GOOGLE STATUS
// =============================
app.MapGet("/integrations/google/status", async (string inspectorId) =>
{
    try
    {
        if (!Guid.TryParse(inspectorId.Trim(), out Guid parsedInspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid inspectorId"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string sql = @"
SELECT
    status,
    external_account_email,
    external_tenant_id,
    expires_at,
    updated_at
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = 'google'
LIMIT 1;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("inspector_id", parsedInspectorId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return Results.Ok(new
            {
                success = true,
                connected = false,
                status = "Disconnected",
                accountEmail = "",
                calendarId = "primary",
                lastSync = "",
                expiresAt = ""
            });
        }

        string status = reader["status"]?.ToString() ?? "disconnected";

        return Results.Ok(new
        {
            success = true,
            connected = string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase),
            status,
            accountEmail = reader["external_account_email"]?.ToString() ?? "",
            calendarId = string.IsNullOrWhiteSpace(reader["external_tenant_id"]?.ToString()) ? "primary" : reader["external_tenant_id"]?.ToString(),
            lastSync = reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O"),
            expiresAt = reader["expires_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["expires_at"]).ToString("O")
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Google status failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapGet("/integrations/google/calendars", async (string inspectorId) =>
{
    try
    {
        if (!Guid.TryParse(inspectorId.Trim(), out Guid parsedInspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid inspectorId"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        var account = await GetGoogleCalendarAccountAsync(conn, parsedInspectorId, builder.Configuration);
        if (!account.Success)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = account.ErrorMessage ?? "Google Calendar is not connected."
            });
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);

        var response = await httpClient.GetAsync("https://www.googleapis.com/calendar/v3/users/me/calendarList?minAccessRole=writer");
        var body = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            var googleMessage = BuildGoogleApiFriendlyError(body);
            return Results.Problem(
                title: "Google calendars failed",
                detail: googleMessage,
                statusCode: (int)response.StatusCode
            );
        }

        var selectedCalendarId = string.IsNullOrWhiteSpace(account.CalendarId) ? "primary" : account.CalendarId;
        var doc = JsonDocument.Parse(body).RootElement;
        var calendars = new List<object>();

        if (doc.TryGetProperty("items", out var items) && items.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in items.EnumerateArray())
            {
                var id = GetJsonString(item, "id");
                if (string.IsNullOrWhiteSpace(id))
                    continue;

                calendars.Add(new
                {
                    id,
                    summary = GetJsonString(item, "summary"),
                    primary = item.TryGetProperty("primary", out var primaryProp) && primaryProp.ValueKind == JsonValueKind.True,
                    selected = string.Equals(id, selectedCalendarId, StringComparison.OrdinalIgnoreCase)
                });
            }
        }

        return Results.Ok(new
        {
            success = true,
            selectedCalendarId,
            calendars
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Google calendars failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapPost("/integrations/google/calendar", async (GoogleCalendarSelectionRequest request) =>
{
    try
    {
        if (!Guid.TryParse(request.InspectorId?.Trim(), out Guid parsedInspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid inspectorId"
            });
        }

        var calendarId = string.IsNullOrWhiteSpace(request.CalendarId) ? "primary" : request.CalendarId.Trim();

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string sql = @"
UPDATE public.inspector_integrations
SET
    external_tenant_id = @calendar_id,
    updated_at = NOW()
WHERE inspector_id = @inspector_id
  AND provider = 'google'
  AND status = 'connected';";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("inspector_id", parsedInspectorId);
        cmd.Parameters.AddWithValue("calendar_id", calendarId);
        var updated = await cmd.ExecuteNonQueryAsync();

        if (updated == 0)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Google Calendar is not connected for this inspector."
            });
        }

        return Results.Ok(new
        {
            success = true,
            calendarId
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Save Google calendar failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// SIGNNOW CONNECT URL
// =============================
app.MapGet("/integrations/signnow/connect-url", () =>
{
    var clientId = builder.Configuration["SIGNNOW_CLIENT_ID"];
    var redirectUri = builder.Configuration["SIGNNOW_REDIRECT_URI"];

    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(redirectUri))
    {
        return Results.Problem(
            title: "SignNow config missing",
            detail: "SIGNNOW_CLIENT_ID and/or SIGNNOW_REDIRECT_URI are missing from Railway variables.",
            statusCode: 500
        );
    }

    var url =
        "https://app.signnow.com/authorize" +
        $"?client_id={Uri.EscapeDataString(clientId)}" +
        $"&response_type=code" +
        $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
        $"&scope={Uri.EscapeDataString("*")}" +
        $"&state=company";

    return Results.Ok(new
    {
        success = true,
        url
    });
});

// =============================
// SIGNNOW CALLBACK
// =============================
app.MapGet("/api/integrations/signnow/callback", async (HttpContext context) =>
{
    try
    {
        var code = context.Request.Query["code"].ToString();
        var error = context.Request.Query["error"].ToString();

        if (!string.IsNullOrWhiteSpace(error))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "SignNow authorization failed.",
                error
            });
        }

        if (string.IsNullOrWhiteSpace(code))
            return Results.BadRequest("Missing code");

        var clientId = builder.Configuration["SIGNNOW_CLIENT_ID"];
        var clientSecret = builder.Configuration["SIGNNOW_CLIENT_SECRET"];
        var redirectUri = builder.Configuration["SIGNNOW_REDIRECT_URI"];

        if (string.IsNullOrWhiteSpace(clientId) ||
            string.IsNullOrWhiteSpace(clientSecret) ||
            string.IsNullOrWhiteSpace(redirectUri))
        {
            return Results.Problem(
                title: "SignNow config missing",
                detail: "SIGNNOW_CLIENT_ID, SIGNNOW_CLIENT_SECRET and/or SIGNNOW_REDIRECT_URI are missing from Railway variables.",
                statusCode: 500
            );
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(clientId + ":" + clientSecret)));

        var tokenResponse = await httpClient.PostAsync(
            "https://api.signnow.com/oauth2/token",
            new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "authorization_code",
                ["code"] = code,
                ["redirect_uri"] = redirectUri
            }));

        var tokenJson = await tokenResponse.Content.ReadAsStringAsync();

        if (!tokenResponse.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "SignNow token exchange failed",
                detail: tokenJson,
                statusCode: 500
            );
        }

        var tokenDoc = JsonDocument.Parse(tokenJson).RootElement;
        var accessToken = GetJsonString(tokenDoc, "access_token");
        var refreshToken = GetJsonString(tokenDoc, "refresh_token");
        var expiresIn = tokenDoc.TryGetProperty("expires_in", out var expiresInProp) &&
            expiresInProp.TryGetInt32(out var parsedExpiresIn)
                ? parsedExpiresIn
                : 3600;
        var expiresAt = DateTime.UtcNow.AddSeconds(expiresIn);

        string? accountEmail = null;
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        var userResponse = await httpClient.GetAsync("https://api.signnow.com/user");
        if (userResponse.IsSuccessStatusCode)
        {
            var userJson = await userResponse.Content.ReadAsStringAsync();
            var userDoc = JsonDocument.Parse(userJson).RootElement;
            accountEmail = FirstNonEmptyJsonString(userDoc, "email", "primary_email", "user_email");
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string upsertSql = @"
INSERT INTO public.inspector_integrations
(
    inspector_id,
    provider,
    status,
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    external_tenant_id,
    created_at,
    updated_at
)
VALUES
(
    @inspector_id,
    'signnow',
    'connected',
    @access_token,
    @refresh_token,
    @expires_at,
    @external_account_email,
    'company',
    NOW(),
    NOW()
)
ON CONFLICT (inspector_id, provider)
DO UPDATE SET
    status = 'connected',
    access_token_encrypted = EXCLUDED.access_token_encrypted,
    refresh_token_encrypted = COALESCE(NULLIF(EXCLUDED.refresh_token_encrypted, ''), inspector_integrations.refresh_token_encrypted),
    expires_at = EXCLUDED.expires_at,
    external_account_email = EXCLUDED.external_account_email,
    external_tenant_id = EXCLUDED.external_tenant_id,
    updated_at = NOW();";

        await using (var cmd = new NpgsqlCommand(upsertSql, conn))
        {
            cmd.Parameters.AddWithValue("inspector_id", Guid.Empty);
            cmd.Parameters.AddWithValue("access_token", accessToken);
            cmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
            cmd.Parameters.AddWithValue("expires_at", expiresAt);
            cmd.Parameters.AddWithValue("external_account_email", (object?)accountEmail ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        return Results.Content(
            "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;padding:32px;\">" +
            "<h2>SignNow connected successfully</h2>" +
            $"<p>Connected account: <strong>{WebUtility.HtmlEncode(accountEmail ?? "SignNow")}</strong></p>" +
            "<p>You can close this window and return to 3D AutoMate.</p>" +
            "</body></html>",
            "text/html");
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "SignNow callback failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// SIGNNOW STATUS
// =============================
app.MapGet("/integrations/signnow/status", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        const string sql = @"
SELECT
    status,
    external_account_email,
    expires_at,
    updated_at
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = 'signnow'
LIMIT 1;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("inspector_id", Guid.Empty);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return Results.Ok(new
            {
                success = true,
                connected = false,
                status = "Disconnected",
                accountEmail = "",
                lastSync = "",
                expiresAt = ""
            });
        }

        string status = reader["status"]?.ToString() ?? "disconnected";

        return Results.Ok(new
        {
            success = true,
            connected = string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase),
            status,
            accountEmail = reader["external_account_email"]?.ToString() ?? "",
            lastSync = reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O"),
            expiresAt = reader["expires_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["expires_at"]).ToString("O")
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "SignNow status failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// SIGNNOW TEMPLATES
// =============================
app.MapGet("/integrations/signnow/templates", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        var account = await GetSignNowAccountAsync(conn, builder.Configuration);
        if (!account.Success)
        {
            return Results.Problem(
                title: "SignNow is not connected",
                detail: account.ErrorMessage,
                statusCode: 400);
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        var lookup = await LookupSignNowTemplatesAsync(httpClient);
        if (lookup.SuccessfulEndpointCount == 0)
        {
            return Results.Problem(
                title: "SignNow templates lookup failed",
                detail: JsonSerializer.Serialize(new
                {
                    endpoint = lookup.LastEndpoint,
                    statusCode = lookup.LastStatusCode,
                    response = lookup.LastResponse,
                    diagnostics = lookup.Diagnostics
                }),
                statusCode: 500);
        }

        return Results.Ok(new
        {
            success = true,
            templates = lookup.Templates,
            diagnostics = lookup.Diagnostics
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "SignNow templates failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});


// =============================
// SIGNNOW TEMPLATE MAPPINGS
// =============================
app.MapGet("/integrations/signnow/template-mappings", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureSignNowTemplateMappingsTableAsync(conn);

        var mappings = await LoadSignNowTemplateMappingsAsync(conn);

        return Results.Ok(new
        {
            success = true,
            mappings
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "SignNow template mappings failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

app.MapPost("/integrations/signnow/template-mappings", async (SignNowTemplateMappingsRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureSignNowTemplateMappingsTableAsync(conn);

        var rows = 0;
        foreach (var mapping in request.Mappings ?? new List<SignNowTemplateMappingInput>())
        {
            if (string.IsNullOrWhiteSpace(mapping.TemplateKey))
                continue;

            await UpsertSignNowTemplateMappingAsync(
                conn,
                mapping.TemplateKey.Trim(),
                mapping.TemplateId?.Trim() ?? "",
                mapping.TemplateName?.Trim() ?? "");
            rows++;
        }

        return Results.Ok(new
        {
            success = true,
            message = "SignNow template mappings saved.",
            rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Save SignNow template mappings failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

// =============================
// SIGNNOW SEND TERMS
// =============================
app.MapPost("/integrations/signnow/jobs/{jobId}/ensure-webhook", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureSignNowJobColumnsAsync(conn);
        await using var lookup = new NpgsqlCommand("SELECT signnow_document_id FROM public.jobs_staging WHERE job_id = @job_id LIMIT 1", conn);
        lookup.Parameters.AddWithValue("job_id", jobId);
        var documentId = Convert.ToString(await lookup.ExecuteScalarAsync()) ?? "";
        if (string.IsNullOrWhiteSpace(documentId))
            return Results.NotFound(new { success = false, message = "No SignNow document is stored for this job." });

        var account = await GetSignNowAccountAsync(conn, builder.Configuration);
        if (!account.Success)
            return Results.BadRequest(new { success = false, message = account.ErrorMessage });
        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        var result = await CreateSignNowDocumentWebhookAsync(httpClient, documentId, builder.Configuration);
        await StoreSignNowWebhookResultAsync(conn, jobId, result);
        return Results.Ok(new { success = result.Success, documentId, subscriptionId = result.SubscriptionId, error = result.Error });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Ensure SignNow webhook failed", detail: ex.ToString(), statusCode: 500);
    }
});

app.MapPost("/integrations/signnow/jobs/{jobId}/send-terms", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await JobChangeSupport.EnsureAsync(conn);
        await EnsureSignNowTemplateMappingsTableAsync(conn);
        await EnsureInspectorIntegrationsTableAsync(conn);

        var job = await LoadScheduleJobAsync(conn, jobId);
        if (job == null)
        {
            return Results.NotFound(new
            {
                success = false,
                message = "Job was not found in Railway. Sync the selected job first.",
                jobId
            });
        }

        var result = await SendSignNowTermsForJobAsync(conn, job, builder.Configuration, forceResend: true);
        return Results.Ok(new
        {
            success = result.Success,
            message = result.Message,
            action = result.Action,
            skipped = result.Skipped,
            details = result.Details
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Send SignNow terms failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

// =============================
// SIGNNOW REFRESH STATUS
// =============================
app.MapPost("/integrations/signnow/jobs/{jobId}/refresh-status", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await EnsureInspectorIntegrationsTableAsync(conn);

        var result = await RefreshSignNowTermsStatusAsync(conn, jobId, builder.Configuration);
        return Results.Ok(new
        {
            success = result.Success,
            message = result.Message,
            action = result.Action,
            skipped = result.Skipped,
            details = result.Details
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Refresh SignNow terms status failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

// =============================
// SIGNNOW WEBHOOK
// =============================
app.MapPost("/api/integrations/signnow/webhook", async (HttpContext context) =>
{
    try
    {
        using var reader = new StreamReader(context.Request.Body);
        var body = await reader.ReadToEndAsync();
        if (string.IsNullOrWhiteSpace(body))
            return Results.BadRequest(new { success = false, message = "Missing webhook body." });

        var root = JsonDocument.Parse(body).RootElement;
        var documentId = FindJsonStringRecursive(root, "entity_id", "entityId", "document_id", "documentId", "document_unique_id");
        var eventName = FindJsonStringRecursive(root, "event", "event_type", "type", "status");
        if (!string.Equals(eventName, "document.complete", StringComparison.OrdinalIgnoreCase))
            return Results.Ok(new { success = true, ignored = true, eventName });

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureSignNowJobColumnsAsync(conn);

        var parsedJobId = string.IsNullOrWhiteSpace(documentId)
            ? Guid.Empty
            : await FindJobIdBySignNowDocumentAsync(conn, documentId);

        if (parsedJobId == Guid.Empty)
        {
            return Results.Ok(new
            {
                success = true,
                matched = false,
                message = "SignNow webhook received but no matching job was found."
            });
        }

        var verification = await RefreshSignNowTermsStatusAsync(conn, parsedJobId, builder.Configuration);

        return Results.Ok(new
        {
            success = verification.Success,
            matched = true,
            jobId = parsedJobId,
            documentId,
            eventName,
            verified = verification.Details
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "SignNow webhook failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

// =============================
// XERO TEST CONNECTION
// =============================
app.MapPost("/integrations/xero/test-connection", async (XeroTestConnectionRequest request) =>
{
    try
    {
        if (!Guid.TryParse(request.InspectorId, out Guid inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid InspectorId"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);

        var account = await GetXeroAccountAsync(conn, inspectorId, builder.Configuration);
        if (!account.Success)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = account.ErrorMessage
            });
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        var response = await httpClient.GetAsync("https://api.xero.com/connections");
        var responseText = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            return Results.Problem(
                title: "Xero connections lookup failed",
                detail: responseText,
                statusCode: 500
            );
        }

        var connectedTenant = false;
        var tenantName = account.TenantName;
        var connections = JsonDocument.Parse(responseText).RootElement;
        if (connections.ValueKind == JsonValueKind.Array)
        {
            foreach (var connection in connections.EnumerateArray())
            {
                var tenantId = connection.TryGetProperty("tenantId", out var tenantIdProp)
                    ? tenantIdProp.GetString() ?? ""
                    : "";

                if (!string.Equals(tenantId, account.TenantId, StringComparison.OrdinalIgnoreCase))
                    continue;

                connectedTenant = true;
                tenantName = connection.TryGetProperty("tenantName", out var tenantNameProp)
                    ? tenantNameProp.GetString() ?? tenantName
                    : tenantName;
                break;
            }
        }

        if (!connectedTenant)
        {
            return Results.Problem(
                title: "Xero tenant not found",
                detail: "The stored Xero tenant was not returned by Xero. Reconnect Xero and try again.",
                statusCode: 500
            );
        }

        return Results.Ok(new
        {
            success = true,
            message = "Xero connection verified.",
            tenantId = account.TenantId,
            tenantName
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Xero test connection failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// =============================
// EMAIL TEMPLATE PLACEHOLDERS
// =============================
app.MapGet("/email-templates/placeholders", () => Results.Ok(new
{
    success = true,
    placeholders = GetBasicEmailTemplatePlaceholders(),
    categories = GetBasicEmailTemplatePlaceholderCategories()
}));

// =============================
// EMAIL TEMPLATE SERVICE TYPES
// =============================
app.MapGet("/email-templates/service-types", () => Results.Ok(new
{
    success = true,
    serviceTypes = GetEmailTemplateServiceTypes()
}));

// =============================
// EMAIL TEMPLATE MAKER UI
// =============================
app.MapGet("/email-template-maker", () => Results.Content(GetEmailTemplateMakerHtml(), "text/html"));

// =============================
// EMAIL SENDER MODE
// =============================
app.MapGet("/integrations/email/status", async (Guid inspectorId) =>
{
    try
    {
        if (inspectorId == Guid.Empty)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "InspectorId is required."
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorsTableAsync(conn);

        const string sql = @"
SELECT COALESCE(email_sender_mode, 'microsoft') AS email_sender_mode
FROM public.inspectors
WHERE inspector_id = @inspector_id
LIMIT 1;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("inspector_id", inspectorId);

        var result = await cmd.ExecuteScalarAsync();
        var senderMode = NormalizeEmailSenderMode(result?.ToString());

        return Results.Ok(new
        {
            success = true,
            inspectorId,
            senderMode,
            senderModeLabel = GetEmailSenderModeLabel(senderMode),
            cloudCanSend = !IsSmtpEmailSenderMode(senderMode)
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Email sender status failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapPost("/integrations/email/sender-mode", async (EmailSenderModeRequest request) =>
{
    try
    {
        if (!Guid.TryParse(request.InspectorId, out var inspectorId) || inspectorId == Guid.Empty)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Valid InspectorId is required."
            });
        }

        var senderMode = NormalizeEmailSenderMode(request.SenderMode);

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorsTableAsync(conn);

        const string sql = @"
UPDATE public.inspectors
SET
    email_sender_mode = @email_sender_mode,
    updated_at = NOW()
WHERE inspector_id = @inspector_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("email_sender_mode", senderMode);
        cmd.Parameters.AddWithValue("inspector_id", inspectorId);
        var rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = rows > 0,
            updated = rows,
            inspectorId,
            senderMode,
            senderModeLabel = GetEmailSenderModeLabel(senderMode),
            cloudCanSend = !IsSmtpEmailSenderMode(senderMode)
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Email sender mode save failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// EMAIL TEMPLATE CRUD / RENDER / SEND
// =============================
app.MapGet("/inspectors/{inspectorId}/email-templates/{templateType}", async (Guid inspectorId, string templateType, string? serviceTypeKey) =>
{
    try
    {
        if (inspectorId == Guid.Empty)
            return Results.BadRequest(new { success = false, message = "InspectorId is required." });

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn);

        var normalizedTemplateType = NormalizeTemplateType(templateType);
        var normalizedServiceTypeKey = NormalizeServiceTypeKey(serviceTypeKey);
        var template = await LoadEmailTemplateAsync(conn, inspectorId, normalizedTemplateType, normalizedServiceTypeKey);

        return Results.Ok(new
        {
            success = true,
            template
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Load email template failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapPut("/inspectors/{inspectorId}/email-templates/{templateType}", async (Guid inspectorId, string templateType, EmailTemplateSaveRequest request) =>
{
    try
    {
        if (inspectorId == Guid.Empty)
            return Results.BadRequest(new { success = false, message = "InspectorId is required." });

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn);

        var normalizedTemplateType = NormalizeTemplateType(templateType);
        var normalizedServiceTypeKey = NormalizeServiceTypeKey(request.ServiceTypeKey);
        var template = new EmailTemplateResult(
            Guid.Empty,
            inspectorId,
            normalizedTemplateType,
            normalizedServiceTypeKey,
            string.IsNullOrWhiteSpace(request.Name) ? GetEmailTemplateServiceLabel(normalizedServiceTypeKey) : request.Name.Trim(),
            string.IsNullOrWhiteSpace(request.Subject) ? BuildDefaultBookingTemplateSubject(normalizedServiceTypeKey) : request.Subject,
            CleanEditorHtml(string.IsNullOrWhiteSpace(request.HtmlBody) ? BuildDefaultBookingTemplateHtml() : request.HtmlBody),
            request.IsActive,
            "",
            "");

        template = await UpsertEmailTemplateAsync(conn, template);

        return Results.Ok(new
        {
            success = true,
            message = "Template saved.",
            template
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Save email template failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapGet("/automation/templates", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        const string sql = @"SELECT template_id,inspector_id,template_type,service_type_key,name,subject,html_body,is_active,archived_at,created_at,updated_at
FROM public.email_templates WHERE tenant_id=@tenant ORDER BY archived_at NULLS FIRST,updated_at DESC,name;";
        await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await cmd.ExecuteReaderAsync(); var templates = new List<object>();
        while (await reader.ReadAsync()) templates.Add(new { templateId=reader.GetGuid(0), inspectorId=reader.GetGuid(1), templateType=reader.GetString(2), serviceTypeKey=reader.GetString(3), name=reader.GetString(4), subject=reader.GetString(5), htmlBody=reader.GetString(6), isActive=reader.GetBoolean(7), archived= !reader.IsDBNull(8), createdAt=reader.GetDateTime(9), updatedAt=reader.GetDateTime(10) });
        return Results.Ok(new { success = true, templates });
    }
    catch (Exception ex) { return Results.Problem(title: "Load company templates failed", detail: ex.Message, statusCode: 500); }
});

app.MapPost("/automation/templates", async (HttpContext context, AutomationTemplateSaveRequest request) =>
{
    if (request.TenantId == Guid.Empty || string.IsNullOrWhiteSpace(request.Name)) return Results.BadRequest(new { success=false, message="TenantId and template name are required." });
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var inspectorId = Guid.Parse(context.Request.Headers["X-AutoMate-Inspector-ID"].First()!);
        var templateId = request.TemplateId == Guid.Empty ? Guid.NewGuid() : request.TemplateId;
        var type = NormalizeTemplateType(string.IsNullOrWhiteSpace(request.TemplateType) ? "advanced-workflow" : request.TemplateType);
        var serviceKey = string.IsNullOrWhiteSpace(request.ServiceTypeKey) ? "advanced_" + templateId.ToString("N") : NormalizeServiceTypeKey(request.ServiceTypeKey);
        const string sql = @"INSERT INTO public.email_templates(template_id,tenant_id,inspector_id,template_type,service_type_key,email_type,name,subject,html_body,is_active,archived_at,created_at,updated_at)
VALUES(@id,@tenant,@inspector,@type,@service,'transactional',@name,@subject,@html,@active,NULL,NOW(),NOW())
ON CONFLICT(template_id) DO UPDATE SET name=EXCLUDED.name,subject=EXCLUDED.subject,html_body=EXCLUDED.html_body,is_active=EXCLUDED.is_active,updated_at=NOW()
WHERE public.email_templates.tenant_id=EXCLUDED.tenant_id RETURNING template_id,updated_at;";
        await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("id",templateId); cmd.Parameters.AddWithValue("tenant",request.TenantId); cmd.Parameters.AddWithValue("inspector",inspectorId); cmd.Parameters.AddWithValue("type",type); cmd.Parameters.AddWithValue("service",serviceKey); cmd.Parameters.AddWithValue("name",request.Name.Trim()); cmd.Parameters.AddWithValue("subject",request.Subject??""); cmd.Parameters.AddWithValue("html",CleanEditorHtml(request.HtmlBody??"")); cmd.Parameters.AddWithValue("active",request.IsActive);
        await using var reader = await cmd.ExecuteReaderAsync(); if (!await reader.ReadAsync()) return Results.NotFound(new { success=false,message="Template was not found for this company." });
        return Results.Ok(new { success=true,templateId=reader.GetGuid(0),updatedAt=reader.GetDateTime(1),serviceTypeKey=serviceKey });
    }
    catch (Exception ex) { return Results.Problem(title:"Save company template failed",detail:ex.Message,statusCode:500); }
});

app.MapPost("/automation/templates/{templateId}/archive", async (HttpContext context, Guid templateId, AutomationTemplateArchiveRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId); if(!owner.Allowed)return owner.Error!;
        const string referenceSql=@"SELECT EXISTS(SELECT 1 FROM public.automation_rules WHERE tenant_id=@tenant AND (template_id=@id OR actions_json::text LIKE '%' || @idtext || '%'));";
        await using(var reference=new NpgsqlCommand(referenceSql,conn)){reference.Parameters.AddWithValue("tenant",request.TenantId);reference.Parameters.AddWithValue("id",templateId);reference.Parameters.AddWithValue("idtext",templateId.ToString());if(Convert.ToBoolean(await reference.ExecuteScalarAsync()))return Results.Conflict(new {success=false,message="This template is referenced by a workflow and cannot be archived."});}
        const string sql=@"UPDATE public.email_templates SET archived_at=NOW(),is_active=false,updated_at=NOW() WHERE template_id=@id AND tenant_id=@tenant AND archived_at IS NULL;";
        await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("id",templateId);cmd.Parameters.AddWithValue("tenant",request.TenantId);var rows=await cmd.ExecuteNonQueryAsync();return rows==0?Results.NotFound(new{success=false,message="Template was not found or is already archived."}):Results.Ok(new{success=true});
    }
    catch(Exception ex){return Results.Problem(title:"Archive company template failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/automation/templates/{templateId}/duplicate", async (HttpContext context, Guid templateId, AutomationTemplateArchiveRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();await EnsureEmailTemplatesTableAsync(conn);await EnsureAdvancedActionsTablesAsync(conn);await AutomationFoundationSupport.EnsureAsync(conn);
        var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;var inspectorId=Guid.Parse(context.Request.Headers["X-AutoMate-Inspector-ID"].First()!);var newId=Guid.NewGuid();
        const string sql=@"INSERT INTO public.email_templates(template_id,tenant_id,inspector_id,template_type,service_type_key,email_type,name,subject,html_body,is_active,created_at,updated_at)
SELECT @newid,tenant_id,@inspector,'advanced-workflow','advanced_'||replace(@newid::text,'-',''),email_type,name||' Copy',subject,html_body,false,NOW(),NOW() FROM public.email_templates WHERE template_id=@id AND tenant_id=@tenant RETURNING template_id;";
        await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("newid",newId);cmd.Parameters.AddWithValue("inspector",inspectorId);cmd.Parameters.AddWithValue("id",templateId);cmd.Parameters.AddWithValue("tenant",request.TenantId);var result=await cmd.ExecuteScalarAsync();return result==null?Results.NotFound(new{success=false,message="Template was not found for this company."}):Results.Ok(new{success=true,templateId=newId});
    }
    catch(Exception ex){return Results.Problem(title:"Duplicate company template failed",detail:ex.Message,statusCode:500);}
});

app.MapGet("/automation/basic", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureBasicJobProfileColumnsAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn); await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        var inspectorId = Guid.Parse(context.Request.Headers["X-AutoMate-Inspector-ID"].First()!);
        var labels = await LoadBasicContactLabelsAsync(conn, tenantId);
        await AutoMateApi.BasicAutomationSupport.SeedSchedulingContactOneAsync(conn, tenantId, inspectorId, labels.Contact1, context.RequestAborted);
        var slots = await AutoMateApi.BasicAutomationSupport.LoadAsync(conn, tenantId, context.RequestAborted);
        return Results.Ok(new
        {
            success = true,
            contactLabels = new { contact1Name = labels.Contact1, contact2Name = labels.Contact2 },
            settings = slots.Select(slot => new { slot.EventKey, slot.RecipientKey, slot.Enabled, slot.SettingVersion, hasTemplate = slot.TemplateId.HasValue, slot.TemplateName, slot.UpdatedAt })
        });
    }
    catch (Exception ex) { return Results.Problem(title: "Load Basic Automations failed", detail: ex.Message, statusCode: 500); }
});

app.MapPut("/automation/basic/settings/{eventKey}/{recipientKey}", async (HttpContext context, string eventKey, string recipientKey, BasicAutomationSettingRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await AutomationFoundationSupport.EnsureAsync(conn); await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn); await AutoMateApi.BasicSettingCommandSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        if (!request.Confirmed) return Results.BadRequest(new { success=false,status="confirmation_required",message="Confirm the Basic setting change." });
        var inspectorId = GetAuthenticatedInspectorId(context);
        var actor = await LoadAuthenticatedAutomationActorAsync(conn, request.TenantId, inspectorId);
        var result = await AutoMateApi.BasicSettingCommandSupport.SaveAsync(conn, new(request.TenantId, inspectorId, eventKey, recipientKey, request.Enabled, request.ExpectedVersion, request.IdempotencyKey, actor, context.TraceIdentifier), context.RequestAborted);
        if (result.Status is "conflict" or "idempotency_conflict" or "template_required") return Results.Json(new { success=false,status=result.Status,code=result.Status,result.Enabled,result.SettingVersion,result.AuditId,message=result.Message }, statusCode:409);
        return Results.Ok(new { success=true,status=result.Status,result.Enabled,result.SettingVersion,result.Replayed,result.AuditId,message=result.Message,automaticSendingActive=false });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (ArgumentException ex) { return Results.BadRequest(new { success=false,message=ex.Message }); }
    catch (Exception ex) { return Results.Problem(title:"Save Basic setting failed",detail:ex.Message,statusCode:500); }
});

app.MapGet("/automation/basic/templates/{eventKey}/{recipientKey}", async (HttpContext context, string eventKey, string recipientKey, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn); await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        var labels = await LoadBasicContactLabelsAsync(conn, tenantId);
        var inspectorId = GetAuthenticatedInspectorId(context);
        await AutoMateApi.BasicAutomationSupport.SeedSchedulingContactOneAsync(conn, tenantId, inspectorId, labels.Contact1, context.RequestAborted);
        var slot = (await AutoMateApi.BasicAutomationSupport.LoadAsync(conn, tenantId, context.RequestAborted)).FirstOrDefault(item => item.EventKey == eventKey && item.RecipientKey == recipientKey);
        if (slot == null) return Results.BadRequest(new { success = false, message = "Unsupported Basic template slot." });
        var recipientLabel = recipientKey == "contact_2" ? labels.Contact2 : labels.Contact1;
        var defaults = BuildDefaultBasicTemplate(eventKey, recipientKey, recipientLabel);
        Guid? templateId = null; var templateVersion = 0; DateTimeOffset? updatedAt = null;
        if (slot.TemplateId.HasValue)
        {
            const string metadataSql = "SELECT template_id,template_version,updated_at FROM public.email_templates WHERE tenant_id=@tenant AND template_id=@template AND archived_at IS NULL";
            await using var metadata = new NpgsqlCommand(metadataSql, conn);
            metadata.Parameters.AddWithValue("tenant", tenantId); metadata.Parameters.AddWithValue("template", slot.TemplateId.Value);
            await using var reader = await metadata.ExecuteReaderAsync(context.RequestAborted);
            if (await reader.ReadAsync(context.RequestAborted)) { templateId = reader.GetGuid(0); templateVersion = reader.GetInt32(1); updatedAt = reader.GetFieldValue<DateTimeOffset>(2); }
        }
        return Results.Ok(new { success = true, eventKey, recipientKey, recipientLabel, templateName = AutoMateApi.BasicAutomationSupport.BuildDisplayName(eventKey, recipientLabel), subject = string.IsNullOrWhiteSpace(slot.Subject) ? defaults.Subject : slot.Subject, htmlBody = string.IsNullOrWhiteSpace(slot.HtmlBody) ? defaults.HtmlBody : slot.HtmlBody, hasTemplate = slot.TemplateId.HasValue, templateId, templateVersion, updatedAt, defaultSubject = defaults.Subject, defaultHtmlBody = defaults.HtmlBody });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (ArgumentException ex) { return Results.BadRequest(new { success = false, message = ex.Message }); }
    catch (Exception ex) { return Results.Problem(title: "Load Basic template failed", detail: ex.Message, statusCode: 500); }
});

app.MapPut("/automation/basic/templates/{eventKey}/{recipientKey}", async (HttpContext context, string eventKey, string recipientKey, BasicAutomationTemplateRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn); await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn); await AutoMateApi.BasicTemplateCommandSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        if (!request.Confirmed) return Results.BadRequest(new { success = false, message = "Template save confirmation is required." });
        if (string.IsNullOrWhiteSpace(request.Subject) || string.IsNullOrWhiteSpace(request.HtmlBody)) return Results.BadRequest(new { success = false, message = "Subject and email body are required." });
        if (request.Subject.Length > 500 || request.HtmlBody.Length > 250000) return Results.BadRequest(new { success = false, message = "The template exceeds the supported size." });
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey)) return Results.BadRequest(new { success = false, message = "IdempotencyKey is required." });
        var labels = await LoadBasicContactLabelsAsync(conn, request.TenantId); var label = recipientKey == "contact_2" ? labels.Contact2 : labels.Contact1;
        var inspectorId = GetAuthenticatedInspectorId(context);
        var actor = await LoadAuthenticatedAutomationActorAsync(conn, request.TenantId, inspectorId);
        var requestId = string.IsNullOrWhiteSpace(request.RequestId) ? context.TraceIdentifier : request.RequestId.Trim();
        var result = await AutoMateApi.BasicTemplateCommandSupport.SaveAsync(conn, new AutoMateApi.BasicTemplateSaveCommand(request.TenantId, inspectorId, eventKey, recipientKey, label, request.Subject.Trim(), SanitizeBasicTemplateHtml(request.HtmlBody), request.ExpectedVersion, request.IdempotencyKey, actor, requestId), context.RequestAborted);
        if (result.Status is "conflict" or "idempotency_conflict") return Results.Json(new { success=false, status=result.Status, code=result.Status, message=result.Message, templateId=result.TemplateId, templateVersion=result.TemplateVersion, auditId=result.AuditId }, statusCode:409);
        return Results.Ok(new { success = true, status=result.Status, templateId=result.TemplateId, templateVersion=result.TemplateVersion, updatedAt=result.UpdatedAt, auditId=result.AuditId, replayed=result.Replayed, requestId, templateName = AutoMateApi.BasicAutomationSupport.BuildDisplayName(eventKey, label) });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (ArgumentException ex) { return Results.BadRequest(new { success = false, message = ex.Message }); }
    catch (Exception ex) { return Results.Problem(title: "Save Basic template failed", detail: ex.Message, statusCode: 500); }
});

app.MapGet("/automation/basic/audit", async (HttpContext context, Guid tenantId, Guid? templateId, int? limit) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        var items = await AutoMateApi.BasicTemplateCommandSupport.LoadTemplateAuditAsync(conn, tenantId, templateId, Math.Clamp(limit ?? 100, 1, 250), context.RequestAborted);
        return Results.Ok(new { success=true, items });
    }
    catch (Exception ex) { return Results.Problem(title:"Load Basic template audit failed",detail:ex.Message,statusCode:500); }
});

app.MapGet("/jobs/{jobId}/audit", async (HttpContext context, Guid jobId, Guid tenantId, int? limit) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        if (!await AutomationFoundationSupport.JobBelongsToTenantAsync(conn, tenantId, jobId)) return Results.NotFound(new { success=false,message="Job not found for this company." });
        var items = await AutoMateApi.BasicTemplateCommandSupport.LoadJobAuditAsync(conn, tenantId, jobId, Math.Clamp(limit ?? 150, 1, 300), context.RequestAborted);
        return Results.Ok(new { success=true, jobId, items });
    }
    catch (Exception ex) { return Results.Problem(title:"Load job audit failed",detail:ex.Message,statusCode:500); }
});

app.MapPost("/jobs/{jobId}/automation/basic-render", async (HttpContext context, Guid jobId, BasicAutomationRenderRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureJobInvoiceLinesTableAsync(conn); await EnsureEmailTemplatesTableAsync(conn); await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn);
        var job = await LoadScheduleJobAsync(conn, jobId); if (job == null || job.TenantId != request.TenantId) return Results.NotFound(new { success=false,message="Job not found for this company." });
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var rendered = await RenderBasicEmailAsync(conn, job, request.EventKey, request.RecipientKey, request.Subject, request.HtmlBody);
        return Results.Ok(new { success=true, rendered.ToEmail, rendered.Subject, rendered.HtmlBody, rendered.RecipientLabel });
    }
    catch (ArgumentException ex) { return Results.BadRequest(new { success=false,message=ex.Message }); }
    catch (Exception ex) { return Results.Problem(title:"Render Basic email failed",detail:ex.Message,statusCode:500); }
});

app.MapPut("/automation/basic/test-jobs/{jobId}", async (HttpContext context, Guid jobId, BasicTestJobSelectionRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await AutomationFoundationSupport.EnsureAsync(conn); await AutoMateApi.BasicTestExecutionSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var inspectorId = GetAuthenticatedInspectorId(context);
        var actor = await LoadAuthenticatedAutomationActorAsync(conn, request.TenantId, inspectorId);
        var result = await AutoMateApi.BasicTestExecutionSupport.SetOptInAsync(conn,
            new(request.TenantId, jobId, request.Enabled, request.DisposableConfirmed, request.Confirmed, request.ExpectedVersion, actor), context.RequestAborted);
        if (result.Status is "conflict" or "confirmation_required") return Results.Json(new { success=false,status=result.Status,code=result.Status,result.Enabled,result.Version,message=result.Message }, statusCode:409);
        return Results.Ok(new { success=true,status=result.Status,result.Enabled,result.Version,message=result.Message,automaticSendingActive=false });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (UnauthorizedAccessException) { return Results.NotFound(new { success=false,message="Job not found for this company." }); }
    catch (Exception ex) { return Results.Problem(title:"Save Basic test-job selection failed",detail:ex.Message,statusCode:500); }
});

app.MapPost("/jobs/{jobId}/automation/basic/queue/prepare", async (HttpContext context, Guid jobId, BasicTestPrepareRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureJobInvoiceLinesTableAsync(conn); await EnsureEmailTemplatesTableAsync(conn); await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn); await AutoMateApi.BasicTestExecutionSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var job = await LoadScheduleJobAsync(conn, jobId); if (job == null || job.TenantId != request.TenantId) return Results.NotFound(new { success=false,message="Job not found for this company." });
        if (!request.Confirmed) return Results.BadRequest(new { success=false,status="confirmation_required",message="Confirm preparation of this disposable-job test action." });
        var rendered = await RenderBasicEmailAsync(conn, job, "scheduling", request.RecipientKey, null, null);
        var inspectorId = GetAuthenticatedInspectorId(context); var actor = await LoadAuthenticatedAutomationActorAsync(conn, request.TenantId, inspectorId);
        var result = await AutoMateApi.BasicTestExecutionSupport.PrepareAsync(conn,
            new(request.TenantId, jobId, request.RevisionKey, request.RecipientKey, rendered.Subject, rendered.HtmlBody, actor), context.RequestAborted);
        if (result.Status is "not_selected" or "slot_disabled" or "template_required" or "recipient_required" or "revision_conflict") return Results.Json(new { success=false,status=result.Status,code=result.Status,result.ActionId,result.State,message=result.Message }, statusCode:409);
        return Results.Ok(new { success=true,status=result.Status,queueId=result.ActionId,state=result.State,result.Replayed,message=result.Message,automaticSendingActive=false });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (ArgumentException ex) { return Results.BadRequest(new { success=false,message=ex.Message }); }
    catch (Exception ex) { return Results.Problem(title:"Prepare Basic test action failed",detail:ex.Message,statusCode:500); }
});

app.MapGet("/jobs/{jobId}/automation/basic/queue", async (HttpContext context, Guid jobId, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        var selection = await AutoMateApi.BasicTestExecutionSupport.LoadOptInAsync(conn, tenantId, jobId, context.RequestAborted);
        var items = await AutoMateApi.BasicTestExecutionSupport.LoadQueueAsync(conn, tenantId, jobId, context.RequestAborted);
        return Results.Ok(new { success=true,jobId,selected=selection.Enabled,selectionVersion=selection.Version,items,automaticSendingActive=false });
    }
    catch (UnauthorizedAccessException) { return Results.NotFound(new { success=false,message="Job not found for this company." }); }
    catch (Exception ex) { return Results.Problem(title:"Load Basic test queue failed",detail:ex.Message,statusCode:500); }
});

app.MapPost("/jobs/{jobId}/automation/basic/queue/{queueId}/approve", async (HttpContext context, Guid jobId, Guid queueId, BasicTestApproveRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var inspectorId = GetAuthenticatedInspectorId(context); var actor = await LoadAuthenticatedAutomationActorAsync(conn, request.TenantId, inspectorId);
        var result = await AutoMateApi.BasicTestExecutionSupport.ApproveAsync(conn, new(request.TenantId, jobId, queueId, request.Confirmed, actor), context.RequestAborted);
        if (result.Status is "confirmation_required" or "invalid_state" or "not_found") return Results.Json(new { success=false,status=result.Status,code=result.Status,message=result.Message }, statusCode:409);
        return Results.Ok(new { success=true,status=result.Status,queueId=result.ActionId,state=result.State,result.Replayed,message=result.Message });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (Exception ex) { return Results.Problem(title:"Approve Basic test action failed",detail:ex.Message,statusCode:500); }
});

app.MapPost("/jobs/{jobId}/automation/basic/queue/{queueId}/complete", async (HttpContext context, Guid jobId, Guid queueId, BasicTestCompleteRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var inspectorId = GetAuthenticatedInspectorId(context); var actor = await LoadAuthenticatedAutomationActorAsync(conn, request.TenantId, inspectorId);
        var result = await AutoMateApi.BasicTestExecutionSupport.CompleteAsync(conn,
            new(request.TenantId, jobId, queueId, request.TestRecipientEmail, request.Confirmed, request.Succeeded, request.ProviderMessageId, request.Error, actor), context.RequestAborted);
        if (result.Status is "confirmation_required" or "invalid_state" or "not_found" or "completion_conflict") return Results.Json(new { success=false,status=result.Status,code=result.Status,message=result.Message }, statusCode:409);
        return Results.Ok(new { success=true,status=result.Status,queueId=result.ActionId,state=result.State,result.Replayed,message=result.Message });
    }
    catch (AuthenticatedAutomationIdentityException ex) { return Results.Json(new { success=false,status="authenticated_identity_required",code="authenticated_identity_required",message=ex.Message }, statusCode:401); }
    catch (Exception ex) { return Results.Problem(title:"Complete Basic test action failed",detail:ex.Message,statusCode:500); }
});

app.MapGet("/jobs/{jobId}/automation/basic/production", async (HttpContext context, Guid jobId, Guid tenantId) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        var owner=await RequireAutomationOwnerAsync(context,conn,tenantId);if(!owner.Allowed)return owner.Error!;
        var status=await AutoMateApi.BasicProductionSchedulingSupport.LoadStatusAsync(conn,tenantId,jobId,context.RequestAborted);
        var latest=status.Actions.FirstOrDefault();
        return Results.Ok(new{success=true,jobId,armed=status.Armed,armVersion=status.ArmVersion,approvedRevision=status.ApprovedVersion.ToString(),approvedFingerprint=status.ApprovedFingerprint,
            recipientAvailable=status.RecipientAvailable,recipientName=status.RecipientName,recipientEmail=status.RecipientEmail,templateEnabled=status.SlotEnabled,templateSaved=status.TemplateSaved,
            bookingEmailState=status.BookingEmailSent?"completed":status.BookingEmailRequired?"pending":"not_required",changeReviewPending=status.ChangeReviewPending,unscheduled=status.Unscheduled,
            action=latest==null?null:new{actionId=latest.ActionId,jobId,approvedRevision=latest.ApprovedVersion.ToString(),recipientKey="contact_1",latest.RecipientName,latest.RecipientEmail,
                latest.RenderedSubject,latest.TemplateVersion,state=latest.State,createdAt=latest.PreparedAt,claimedAt=latest.ClaimedAt,completedAt=latest.CompletedAt,error=latest.CompletionError}});
    }
    catch(UnauthorizedAccessException){return Results.NotFound(new{success=false,message="Job or Basic entitlement was not found."});}
    catch(Exception ex){return Results.Problem(title:"Load production Basic Scheduling failed",detail:ex.Message,statusCode:500);}
});

app.MapPut("/jobs/{jobId}/automation/basic/production/arm", async (HttpContext context, Guid jobId, BasicProductionArmRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,GetAuthenticatedInspectorId(context));
        var result=await AutoMateApi.BasicProductionSchedulingSupport.SetArmAsync(conn,new(request.TenantId,jobId,request.Armed,request.DisposableConfirmed,request.Confirmed,request.ExpectedVersion,actor),context.RequestAborted);
        if(result.Status is "confirmation_required" or "conflict")return Results.Json(new{success=false,status=result.Status,message=result.Message,armed=result.Armed,armVersion=result.Version},statusCode:409);
        return Results.Ok(new{success=true,status=result.Status,armed=result.Armed,armVersion=result.Version,message=result.Message});
    }
    catch(AuthenticatedAutomationIdentityException ex){return Results.Json(new{success=false,status="authenticated_identity_required",message=ex.Message},statusCode:401);}
    catch(Exception ex){return Results.Problem(title:"Arm production Basic Scheduling failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/automation/basic/production/prepare", async (HttpContext context, Guid jobId, BasicProductionCommandRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        var job=await LoadScheduleJobAsync(conn,jobId);if(job==null||job.TenantId!=request.TenantId)return Results.NotFound();
        var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,GetAuthenticatedInspectorId(context));
        var rendered=await RenderBasicEmailAsync(conn,job,"scheduling","contact_1",null,null);
        var result=await AutoMateApi.BasicProductionSchedulingSupport.PrepareAsync(conn,new(request.TenantId,jobId,rendered.Subject,rendered.HtmlBody,request.Confirmed,actor),context.RequestAborted);
        if(result.ActionId==null||result.Status is not ("prepared" or "replayed"))return Results.Json(new{success=false,status=result.Status,message=result.Message,actionId=result.ActionId,state=result.State},statusCode:409);
        return Results.Ok(new{success=true,status=result.Status,actionId=result.ActionId,state=result.State,replayed=result.Replayed,message=result.Message});
    }
    catch(AuthenticatedAutomationIdentityException ex){return Results.Json(new{success=false,status="authenticated_identity_required",message=ex.Message},statusCode:401);}
    catch(Exception ex){return Results.Problem(title:"Prepare production Basic Scheduling failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/automation/basic/production/{actionId}/approve", async (HttpContext context, Guid jobId, Guid actionId, BasicProductionCommandRequest request) =>
{
    try{await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,GetAuthenticatedInspectorId(context));var result=await AutoMateApi.BasicProductionSchedulingSupport.ApproveAsync(conn,new(request.TenantId,jobId,actionId,request.Confirmed,actor),context.RequestAborted);return result.State=="approved"?Results.Ok(new{success=true,status=result.Status,actionId,state=result.State,message=result.Message}):Results.Json(new{success=false,status=result.Status,message=result.Message},statusCode:409);}
    catch(Exception ex){return Results.Problem(title:"Approve production Basic Scheduling failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/automation/basic/production/{actionId}/claim", async (HttpContext context, Guid jobId, Guid actionId, BasicProductionCommandRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,GetAuthenticatedInspectorId(context));
        var claim=await AutoMateApi.BasicProductionSchedulingSupport.ClaimForDeliveryAsync(conn,new(request.TenantId,jobId,actionId,request.Confirmed,actor),context.RequestAborted);
        if(claim.Status!="claimed")return Results.Json(new{success=false,status=claim.Status,message=claim.Message,state=claim.State},statusCode:409);
        var html=claim.HtmlBody;Guid? communicationId=null;
        try
        {
            var settings=await AutoMateApi.ClientEngagementSupport.LoadSettingsAsync(conn,request.TenantId,context.RequestAborted);
            if((settings.PageEnabled||settings.PixelEnabled)&&!string.IsNullOrWhiteSpace(clientTokenPepper))
            {
                var publication=await AutoMateApi.ClientEngagementSupport.PublishApprovedSnapshotAsync(conn,new(request.TenantId,jobId,actor),context.RequestAborted);
                var job=await LoadScheduleJobAsync(conn,jobId);var expiry=(job?.JobDate??DateTime.UtcNow).ToUniversalTime().AddDays(90);
                var token=AutoMateApi.ClientEngagementSupport.CreateToken("inspection_page",clientTokenPepper);
                var issued=await AutoMateApi.ClientEngagementSupport.IssueCommunicationAsync(conn,new(request.TenantId,jobId,publication.PublicationId,"contact_1",claim.ToEmail,"inspection_page",$"basic-production|{actionId:N}|v{publication.ApprovedVersion}",token.Secret,expiry,claim.Subject,false,false,actor),clientTokenPepper,context.RequestAborted);
                if(issued.RawToken==null)throw new InvalidOperationException("The engagement token was already issued; delivery will not be retried.");
                communicationId=issued.CommunicationId;var url=$"{publicBaseUrl}/inspection/{Uri.EscapeDataString(issued.RawToken)}";html+=BuildClientEngagementFooter(url,settings.PageEnabled,settings.PixelEnabled);
            }
        }
        catch(Exception)
        {
            await AutoMateApi.BasicProductionSchedulingSupport.CompleteAsync(conn,new(request.TenantId,jobId,actionId,true,"unknown",null,"Engagement preparation failed after the one-time claim; manual reconciliation required.",actor),context.RequestAborted);
            return Results.Json(new{success=false,status="reconciliation_required",message="Email preparation became uncertain after the one-time claim. It will not retry automatically."},statusCode:409);
        }
        return Results.Ok(new{success=true,status="claimed",actionId,state="sending",toEmail=claim.ToEmail,subject=claim.Subject,htmlBody=html,communicationId,message=claim.Message});
    }
    catch(Exception ex){return Results.Problem(title:"Claim production Basic Scheduling failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/automation/basic/production/{actionId}/complete", async (HttpContext context, Guid jobId, Guid actionId, BasicProductionCompleteRequest request) =>
{
    try{await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,GetAuthenticatedInspectorId(context));var result=await AutoMateApi.BasicProductionSchedulingSupport.CompleteAsync(conn,new(request.TenantId,jobId,actionId,request.Confirmed,request.Outcome,request.ProviderMessageId,request.Error,actor),context.RequestAborted);return result.Status is "smtp_accepted" or "failed" or "reconciliation_required"?Results.Ok(new{success=true,status=result.Status,actionId,state=result.State,message=result.Message}):Results.Json(new{success=false,status=result.Status,message=result.Message},statusCode:409);}
    catch(Exception ex){return Results.Problem(title:"Complete production Basic Scheduling failed",detail:ex.Message,statusCode:500);}
});

app.MapGet("/jobs/{jobId}/email-template-context", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureJobInvoiceLinesTableAsync(conn);
        await EnsureInspectorsTableAsync(conn);

        var job = await LoadScheduleJobAsync(conn, jobId);
        if (job == null)
            return Results.NotFound(new { success = false, message = "Job was not found in Railway.", jobId });

        var fields = BuildEmailTemplateFields(job, null);
        var invoiceContext = await AutoMateApi.EmailInvoiceTemplateContext.LoadAsync(conn, jobId);
        if (invoiceContext != null)
            MergeEmailTemplateFields(fields, invoiceContext.Tokens);

        return Results.Ok(new
        {
            success = true,
            jobId,
            inspectorId = job.InspectorId,
            inspectorName = job.InspectorName,
            clientEmail = job.ClientEmail,
            serviceTypeKey = job.BookingTemplateKey,
            fields
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Email template context failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapPost("/jobs/{jobId}/communications/client-email/prepare", async (HttpContext context, Guid jobId, PrepareClientEmailRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);await EnsureInspectorsTableAsync(conn);await EnsureEmailTemplatesTableAsync(conn);await AutoMateApi.ClientEngagementSupport.EnsureAsync(conn);
        var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        var job=await LoadScheduleJobAsync(conn,jobId);if(job==null||job.TenantId!=request.TenantId)return Results.NotFound(new{success=false,message="Job not found for this company."});
        if(!string.Equals(request.RecipientKey,"contact_1",StringComparison.Ordinal))return Results.BadRequest(new{success=false,status="client_only",message="Client engagement is available only for THREED Contact 1."});
        var controlledTest=request.ControlledClientPageTest;
        var deliveryAddress=job.ClientEmail;
        if(controlledTest)
        {
            if(!request.Confirmed)return Results.BadRequest(new{success=false,status="confirmation_required",message="Confirm the controlled client-page test."});
            try{deliveryAddress=new System.Net.Mail.MailAddress(request.DeliveryAddressOverride).Address;}catch{return Results.BadRequest(new{success=false,status="invalid_test_recipient",message="Enter a valid tester-owned email address."});}
            if(string.Equals(deliveryAddress,job.ClientEmail,StringComparison.OrdinalIgnoreCase))return Results.BadRequest(new{success=false,status="customer_recipient_forbidden",message="The tester address must differ from the THREED Client email."});
            if(string.IsNullOrWhiteSpace(request.IdempotencyKey))return Results.BadRequest(new{success=false,status="idempotency_required",message="A test idempotency key is required."});
        }
        var rendered=await RenderBookingEmailTemplateAsync(conn,jobId,new EmailTemplateRenderRequest{ServiceTypeKey=request.ServiceTypeKey,ToEmail=deliveryAddress,ActionKey=request.ActionKey},false);
        if(rendered==null)return Results.NotFound();
        var settings=await AutoMateApi.ClientEngagementSupport.LoadSettingsAsync(conn,request.TenantId,context.RequestAborted);
        if(!settings.PageEnabled&&!settings.PixelEnabled)return Results.Ok(new{success=true,rendered.Subject,rendered.HtmlBody,rendered.ToEmail,rendered.ActionKey,trackingApplied=false,communicationId=(Guid?)null});
        if(string.IsNullOrWhiteSpace(clientTokenPepper))return Results.Ok(new{success=true,rendered.Subject,rendered.HtmlBody,rendered.ToEmail,rendered.ActionKey,trackingApplied=false,engagementWarning="Client engagement is not configured on the server.",communicationId=(Guid?)null});
        var inspectorId=GetAuthenticatedInspectorId(context);var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,inspectorId);
        AutoMateApi.ClientPagePublicationResult publication;
        try{publication=await AutoMateApi.ClientEngagementSupport.PublishApprovedSnapshotAsync(conn,new(request.TenantId,jobId,actor),context.RequestAborted);}
        catch(InvalidOperationException ex){return Results.Ok(new{success=true,rendered.Subject,rendered.HtmlBody,rendered.ToEmail,rendered.ActionKey,trackingApplied=false,engagementWarning=ex.Message,communicationId=(Guid?)null});}
        var expiry=(job.JobDate??DateTime.UtcNow).ToUniversalTime().AddDays(90);if(expiry<=DateTime.UtcNow)return Results.Ok(new{success=true,rendered.Subject,rendered.HtmlBody,rendered.ToEmail,rendered.ActionKey,trackingApplied=false,engagementWarning="The inspection-page expiry has already passed.",communicationId=(Guid?)null});
        var token=AutoMateApi.ClientEngagementSupport.CreateToken("inspection_page",clientTokenPepper);
        var idempotency=controlledTest?$"client-page-test|{request.IdempotencyKey.Trim()}|v{publication.ApprovedVersion}":$"{request.EventKey}|{rendered.ActionKey}|v{publication.ApprovedVersion}";
        AutoMateApi.ClientCommunicationIssueResult issued;
        try{issued=await AutoMateApi.ClientEngagementSupport.IssueCommunicationAsync(conn,new(request.TenantId,jobId,publication.PublicationId,"contact_1",deliveryAddress,"inspection_page",idempotency,token.Secret,expiry,rendered.Subject,false,request.IsPreview,actor),clientTokenPepper,context.RequestAborted);}
        catch(InvalidOperationException ex) when(ex.Message.Contains("idempotency",StringComparison.OrdinalIgnoreCase)){return Results.Conflict(new{success=false,status="delivery_already_prepared",message="This email delivery was already prepared and will not be sent again automatically."});}
        if(issued.RawToken==null)return Results.Conflict(new{success=false,status="delivery_already_prepared",message="This email delivery was already prepared and will not be sent again automatically."});
        var url=$"{publicBaseUrl}/inspection/{Uri.EscapeDataString(issued.RawToken)}";
        var footer=BuildClientEngagementFooter(url,settings.PageEnabled,settings.PixelEnabled);
        var subject=controlledTest?"[CLIENT PAGE TEST] "+rendered.Subject:rendered.Subject;
        return Results.Ok(new{success=true,Subject=subject,HtmlBody=rendered.HtmlBody+footer,ToEmail=deliveryAddress,rendered.ActionKey,trackingApplied=true,controlledClientPageTest=controlledTest,settings.PageEnabled,settings.PixelEnabled,communicationId=issued.CommunicationId,expiresAt=issued.ExpiresAt});
    }
    catch(AuthenticatedAutomationIdentityException ex){return Results.Json(new{success=false,status="authenticated_identity_required",message=ex.Message},statusCode:401);}
    catch(Exception ex){return Results.Problem(title:"Prepare tracked Client email failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/communications/{communicationId}/delivery", async (HttpContext context, Guid jobId, Guid communicationId, ClientDeliveryRequest request) =>
{
    try
    {
        await using var conn=new NpgsqlConnection(connectionString);await conn.OpenAsync();var owner=await RequireAutomationOwnerAsync(context,conn,request.TenantId);if(!owner.Allowed)return owner.Error!;
        var inspectorId=GetAuthenticatedInspectorId(context);var actor=await LoadAuthenticatedAutomationActorAsync(conn,request.TenantId,inspectorId);
        var result=await AutoMateApi.ClientEngagementSupport.MarkDeliveryAsync(conn,new(request.TenantId,jobId,communicationId,request.Accepted,request.Provider,request.ConnectorVersion,request.Error,actor),context.RequestAborted);
        if(result.Status=="conflict")return Results.Conflict(new{success=false,status=result.Status,message=result.Message});return Results.Ok(new{success=true,status=result.Status,state=result.DeliveryState,replayed=result.Replayed});
    }
    catch(Exception ex){return Results.Problem(title:"Record Client email delivery failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/jobs/{jobId}/email-templates/booking-email/preview", async (Guid jobId, EmailTemplateRenderRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureInspectorsTableAsync(conn);

        var rendered = await RenderBookingEmailTemplateAsync(conn, jobId, request, preferDraft: true);
        if (rendered == null)
            return Results.NotFound(new { success = false, message = "Job was not found in Railway.", jobId });

        return Results.Ok(new
        {
            success = true,
            rendered.Subject,
            rendered.HtmlBody,
            rendered.ToEmail,
            rendered.ActionKey,
            rendered.ServiceTypeKey,
            rendered.ServiceLabel
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Preview email template failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapPost("/jobs/{jobId}/email-templates/booking-email/render", async (Guid jobId, EmailTemplateRenderRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureInspectorsTableAsync(conn);
        await EnsureEmailTemplatesTableAsync(conn);

        var rendered = await RenderBookingEmailTemplateAsync(conn, jobId, request, preferDraft: false);
        if (rendered == null)
            return Results.NotFound(new { success = false, message = "Job was not found in Railway.", jobId });

        return Results.Ok(new
        {
            success = true,
            rendered.Subject,
            rendered.HtmlBody,
            rendered.ToEmail,
            rendered.ActionKey,
            rendered.ServiceTypeKey,
            rendered.ServiceLabel,
            provider = "api-render"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Render email template failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

app.MapPost("/jobs/{jobId}/email-templates/booking-email/send", async (Guid jobId, EmailTemplateSendRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);
        await EnsureInspectorsTableAsync(conn);
        await EnsureEmailTemplatesTableAsync(conn);

        var rendered = await RenderBookingEmailTemplateAsync(conn, jobId, request, preferDraft: false);
        if (rendered == null)
            return Results.NotFound(new { success = false, message = "Job was not found in Railway.", jobId });

        if (string.IsNullOrWhiteSpace(rendered.ToEmail))
            return Results.BadRequest(new { success = false, message = "Recipient email is required." });

        if (builder.Configuration.GetValue("AUTOMATE_SMTP_ONLY", true))
            return Results.BadRequest(new
            {
                success = false,
                message = "AutoMate email is SMTP-only. Send from the desktop connector so the company's SMTP credentials remain local.",
                senderMode = "customer-smtp",
                provider = "Customer SMTP"
            });

        if (IsSmtpEmailSenderMode(rendered.EmailSenderMode))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "This inspector is set to SMTP. Send from the desktop connector so SMTP credentials stay local.",
                senderMode = rendered.EmailSenderMode,
                provider = GetEmailSenderModeLabel(rendered.EmailSenderMode)
            });
        }

        var account = await GetMicrosoftMailAccountAsync(conn, rendered.InspectorId, builder.Configuration);
        if (!account.Success)
        {
            await MarkBookingEmailFailedAsync(conn, jobId, account.ErrorMessage ?? "Microsoft email is not connected.");
            return Results.BadRequest(new
            {
                success = false,
                message = account.ErrorMessage ?? "Microsoft email is not connected."
            });
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
        var response = await SendMicrosoftMailAsync(httpClient, rendered.ToEmail, rendered.Subject, rendered.HtmlBody);
        if (!response.Success)
        {
            await MarkBookingEmailFailedAsync(conn, jobId, response.Message);
            await MarkWorkflowActionFailedAsync(conn, jobId, rendered.ActionKey, response.Message);
            return Results.Problem(
                title: "Microsoft send mail failed",
                detail: response.Message,
                statusCode: 500);
        }

        if (request.MarkWorkflowComplete)
        {
            await MarkWorkflowActionSentAsync(conn, jobId, rendered.ActionKey);
            await MarkBookingEmailSentIfNoPendingActionsAsync(conn, jobId);
        }

        return Results.Ok(new
        {
            success = true,
            message = "Email sent via Microsoft Test Mode.",
            provider = "Microsoft Test Mode",
            toEmail = rendered.ToEmail,
            actionKey = rendered.ActionKey
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Send email template failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});
// XERO CREATE DRAFT INVOICE
// =============================
app.MapPost("/integrations/xero/jobs/{jobId}/create-draft-invoice", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureJobInvoiceLinesTableAsync(conn);

        var job = await LoadXeroInvoiceJobAsync(conn, jobId);
        if (job == null)
        {
            return Results.NotFound(new
            {
                success = false,
                message = "Job was not found in Railway. Sync the selected job first."
            });
        }

        if (!string.IsNullOrWhiteSpace(job.XeroInvoiceId))
        {
            return Results.Ok(new
            {
                success = true,
                message = "Xero draft invoice already exists for this job.",
                invoiceId = job.XeroInvoiceId,
                invoiceNumber = job.XeroInvoiceNumber,
                invoiceStatus = job.XeroInvoiceStatus,
                contactId = job.XeroContactId,
                duplicatePrevented = true
            });
        }

        var account = await GetXeroAccountAsync(conn, job.InspectorId, builder.Configuration);
        if (!account.Success)
        {
            await StoreXeroJobErrorAsync(conn, jobId, account.ErrorMessage ?? "Xero is not connected.");
            return Results.BadRequest(new
            {
                success = false,
                message = account.ErrorMessage
            });
        }

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
        httpClient.DefaultRequestHeaders.Add("xero-tenant-id", account.TenantId);
        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        var contactId = job.XeroContactId;
        if (string.IsNullOrWhiteSpace(contactId))
        {
            contactId = await FindXeroContactIdByEmailAsync(httpClient, job.ContactEmail);
        }

        if (string.IsNullOrWhiteSpace(contactId))
        {
            contactId = await CreateXeroContactAsync(httpClient, job.ContactName, job.ContactEmail, job.ContactPhone);
        }

        if (string.IsNullOrWhiteSpace(contactId))
            throw new InvalidOperationException("Xero did not return a contact ID.");

        var invoiceLines = await LoadXeroInvoiceLinesAsync(conn, jobId);
        if (invoiceLines.Count == 0)
        {
            invoiceLines.Add(new XeroInvoiceLineInput(
                BuildFallbackInvoiceDescription(job.PrimaryService, job.SiteAddress),
                1m,
                job.JobTotal ?? 0m,
                1));
        }

        if (invoiceLines.All(line => line.UnitAmount == 0m))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "No invoice amount was found. Check the THREED invoice total/lines and sync the job again."
            });
        }

        var invoicePayload = new
        {
            Invoices = new[]
            {
                new
                {
                    Type = "ACCREC",
                    Contact = new { ContactID = contactId },
                    DateString = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                    DueDateString = (job.JobDate ?? DateTime.UtcNow).ToString("yyyy-MM-dd"),
                    Reference = string.IsNullOrWhiteSpace(job.SiteAddress) ? job.JobName : job.SiteAddress,
                    Status = "DRAFT",
                    SentToContact = false,
                    LineItems = invoiceLines.Select(line => new
                    {
                        Description = line.Description,
                        Quantity = line.Quantity <= 0m ? 1m : line.Quantity,
                        UnitAmount = line.UnitAmount
                    }).ToArray()
                }
            }
        };

        var invoiceResponse = await httpClient.PostAsJsonAsync(
            "https://api.xero.com/api.xro/2.0/Invoices",
            invoicePayload);
        var invoiceJson = await invoiceResponse.Content.ReadAsStringAsync();

        if (!invoiceResponse.IsSuccessStatusCode)
        {
            if (invoiceResponse.StatusCode == HttpStatusCode.Unauthorized)
            {
                var reconnectMessage = "Xero rejected invoice creation. Reconnect Xero from Setup / Settings > Integrations > Xero so 3D AutoMate can request the new contact and invoice permissions.";
                await StoreXeroJobErrorAsync(conn, jobId, reconnectMessage);
                return Results.BadRequest(new
                {
                    success = false,
                    message = reconnectMessage,
                    xeroStatus = (int)invoiceResponse.StatusCode,
                    xeroResponse = invoiceJson
                });
            }

            await StoreXeroJobErrorAsync(conn, jobId, invoiceJson);
            return Results.Problem(
                title: "Xero draft invoice creation failed",
                detail: invoiceJson,
                statusCode: 500);
        }

        var invoiceDoc = JsonDocument.Parse(invoiceJson).RootElement;
        var invoice = invoiceDoc.TryGetProperty("Invoices", out var invoicesProp) &&
                      invoicesProp.ValueKind == JsonValueKind.Array &&
                      invoicesProp.GetArrayLength() > 0
            ? invoicesProp[0]
            : invoiceDoc;

        var invoiceId = GetJsonString(invoice, "InvoiceID");
        var invoiceNumber = GetJsonString(invoice, "InvoiceNumber");
        var invoiceStatus = GetJsonString(invoice, "Status");

        await StoreXeroInvoiceResultAsync(conn, jobId, contactId, invoiceId, invoiceNumber, invoiceStatus);

        return Results.Ok(new
        {
            success = true,
            message = "Xero draft invoice created.",
            contactId,
            invoiceId,
            invoiceNumber,
            invoiceStatus,
            sentToContact = false
        });
    }
    catch (Exception ex)
    {
        try
        {
            await using var errConn = new NpgsqlConnection(connectionString);
            await errConn.OpenAsync();
            await EnsureJobPaymentColumnsAsync(errConn);
            await StoreXeroJobErrorAsync(errConn, jobId, ex.Message);
        }
        catch
        {
        }

        return Results.Problem(
            title: "Create Xero draft invoice failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

// =============================
// SCHEDULE JOB
// =============================
app.MapPost("/jobs/{jobId}/schedule", async (Guid jobId) =>
{
    var results = new List<ScheduleActionResult>();

    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorIntegrationsTableAsync(conn);
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await EnsureSignNowTemplateMappingsTableAsync(conn);
        await EnsureJobInvoiceLinesTableAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);
        await JobChangeSupport.EnsureAsync(conn);

        await using (var gate = new NpgsqlCommand("SELECT change_review_pending,unscheduled FROM public.jobs_staging WHERE job_id=@job", conn))
        {
            gate.Parameters.AddWithValue("job", jobId);
            await using var gateReader = await gate.ExecuteReaderAsync();
            if (await gateReader.ReadAsync() && (gateReader.GetBoolean(0) || gateReader.GetBoolean(1)))
                return Results.Conflict(new { success = false, status = "change_review_required", message = "Review the detected 3D job changes before running customer workflows." });
        }

        var job = await LoadScheduleJobAsync(conn, jobId);
        if (job == null)
        {
            return Results.NotFound(new
            {
                success = false,
                message = "Job was not found in Railway. Sync the selected job first.",
                jobId
            });
        }

        if (job.BookingEmailRequired)
        {
            var bookingResult = await SendScheduleBookingEmailsAsync(conn, job, builder.Configuration);
            results.Add(bookingResult);
        }
        else
        {
            results.Add(ScheduleActionResult.Skip("booking-email", "Booking email is not required for this job."));
        }

        if (job.InvoiceRequired)
        {
            var invoiceResult = await CreateXeroDraftInvoiceForJobAsync(conn, jobId, builder.Configuration);
            results.Add(invoiceResult);
        }
        else
        {
            results.Add(ScheduleActionResult.Skip("invoice", "Invoice is not required for this job."));
        }

        if (job.TermsRequired)
        {
            var termsResult = await SendSignNowTermsForJobAsync(conn, job, builder.Configuration, forceResend: false);
            results.Add(termsResult);
        }
        else
        {
            results.Add(ScheduleActionResult.Skip("terms", "Terms are not required for this job."));
        }

        if (job.CalendarRequired)
        {
            var calendarResult = await CreateGoogleCalendarEventForJobAsync(conn, job, builder.Configuration);
            results.Add(calendarResult);
        }
        else
        {
            results.Add(ScheduleActionResult.Skip("calendar", "Calendar event is not required for this job."));
        }

        var failed = results.Where(result => !result.Success && !result.Skipped).ToArray();

        return Results.Ok(new
        {
            success = failed.Length == 0,
            message = failed.Length == 0
                ? "Schedule Job completed."
                : "Schedule Job completed with setup or action errors.",
            jobId,
            actions = results
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Schedule Job failed",
            detail: ex.ToString(),
            statusCode: 500);
    }
});

// =============================
// GET PENDING WORKFLOWS
// Full shared JSON for all current/future zaps
// Enriched with inspectors + subscriptions
// =============================
app.MapGet("/jobs/pending-workflows", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);

        const string sql = @"
SELECT
    j.job_id,
    j.tenant_id,
    j.inspector_id,
    j.inspector_name,
    j.source_system,
    j.job_name,
    j.site_address,
    j.age_of_building,
    j.job_date,
    j.inspection_duration_minutes,
    j.source_updated_at,
    j.date_added,
    j.status,
    j.zap_processed,
    j.report_sent,
    j.booking_email_sent,
    j.booking_email_sent_at,
    j.booking_email_retry_requested,
    j.booking_email_retry_requested_at,
    j.booking_email_last_attempt_at,
    j.booking_email_last_error,
    j.terms_sent,
    j.terms_sent_at,
    j.terms_retry_requested,
    j.terms_retry_requested_at,
    j.terms_last_attempt_at,
    j.terms_last_error,
    j.terms_signed,
    j.terms_signed_at,
    j.signnow_document_id,
    j.signnow_invite_id,
    j.signnow_template_id,
    j.signnow_document_status,
    j.signnow_last_checked_at,
    j.signnow_signing_link,
    j.invoice_sent,
    j.invoice_sent_at,
    j.invoice_retry_requested,
    j.invoice_retry_requested_at,
    j.invoice_last_attempt_at,
    j.invoice_last_error,
    j.paid,
    j.marked_as_paid_override,
    j.report_available,
    j.job_total,
    j.amount_paid,
    j.amount_outstanding,
    j.payment_status,
    j.calendar_created,
    j.calendar_created_at,
    j.calendar_retry_requested,
    j.calendar_retry_requested_at,
    j.calendar_last_attempt_at,
    j.calendar_last_error,
    j.primary_service,
    j.additional1,
    j.additional2,
    j.primary_service_key,
    j.additional1_service_key,
    j.additional2_service_key,
    j.booking_template_key,
    j.booking_email_required,
    j.terms_required,
    j.invoice_required,
    j.calendar_required,
    j.report_required,
    j.building_type,
    j.stories,
    j.bedrooms,
    j.bathrooms,
    j.monolithic,
    j.outbuilding,
    j.occupied,
    j.attached_flat,
    j.travel_fee,
    j.hhs_bedrooms,
    j.meth_samples,
    j.hhs_reinspect,
    j.council_files,
    j.foundation_space,
    j.weathertightness,
    j.hhs_reinspect_date,
    j.access_by,
    j.hhs_compliance,
    j.contact1_salutation,
    j.contact1_first_name,
    j.contact1_last_name,
    j.contact1_email,
    j.contact1_cellular,
    j.contact2_salutation,
    j.contact2_first_name,
    j.contact2_last_name,
    j.contact2_email,
    j.contact2_cellular,
    j.extracted_at_utc,
    j.connector_version,
    j.source_instance,
    j.report_workflow_sent,
    j.report_workflow_sent_at,
    j.report_retry_requested,
    j.report_retry_requested_at,
    j.report_last_attempt_at,
    j.report_last_error,
    j.workflow_updated_at,
    j.created_at,
    j.updated_at,

    i.company_name,
    i.contact_name,
    i.email_from_name,
    i.email_from_address,
    i.phone,
    i.timezone,
    i.allow_report_release_before_payment,
    i.onboarding_status,
    i.logo_url,
    i.is_active AS inspector_is_active,

    s.status AS subscription_status,
    s.plan_name,
    s.billing_interval,
    s.trial_ends_at,
    s.current_period_end,

    CASE
        WHEN COALESCE(i.is_active, false) = true
         AND COALESCE(i.onboarding_status, '') IN ('complete', 'in_progress')
         AND COALESCE(s.status, '') IN ('active', 'trialing')
        THEN true
        ELSE false
    END AS account_can_run_automation

FROM public.jobs_staging j
LEFT JOIN public.inspectors i
    ON i.tenant_id::text = j.tenant_id::text
LEFT JOIN LATERAL (
    SELECT *
    FROM public.subscriptions s
    WHERE s.inspector_id::text = i.inspector_id::text
    ORDER BY
        CASE
            WHEN s.status IN ('active', 'trialing', 'past_due') THEN 0
            ELSE 1
        END,
        s.current_period_end DESC NULLS LAST,
        s.created_at DESC
    LIMIT 1
) s ON TRUE
WHERE
NOT j.change_review_pending AND NOT j.unscheduled AND
(
    (j.booking_email_required = true AND (j.booking_email_sent = false OR j.booking_email_retry_requested = true))
    OR (j.terms_required = true AND (j.terms_sent = false OR j.terms_retry_requested = true))
    OR (j.invoice_required = true AND (j.invoice_sent = false OR j.invoice_retry_requested = true))
    OR (j.calendar_required = true AND (j.calendar_created = false OR j.calendar_retry_requested = true))
    OR (j.report_required = true AND (j.report_workflow_sent = false OR j.report_retry_requested = true))
)
ORDER BY j.updated_at ASC
LIMIT 100;";

        var rows = new List<object>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            rows.Add(new
            {
                job_id = reader["job_id"]?.ToString(),
                tenant_id = reader["tenant_id"]?.ToString(),
                inspector_id = reader["inspector_id"]?.ToString(),
                inspector_name = reader["inspector_name"]?.ToString(),
                source_system = reader["source_system"]?.ToString(),
                job_name = reader["job_name"]?.ToString(),
                site_address = reader["site_address"]?.ToString(),
                age_of_building = reader["age_of_building"]?.ToString(),
                job_date = reader["job_date"]?.ToString(),
                inspection_duration_minutes = reader["inspection_duration_minutes"]?.ToString(),
                source_updated_at = reader["source_updated_at"]?.ToString(),
                date_added = reader["date_added"]?.ToString(),
                status = reader["status"]?.ToString(),
                zap_processed = reader["zap_processed"]?.ToString(),
                report_sent = reader["report_sent"]?.ToString(),

                booking_email_sent = reader["booking_email_sent"]?.ToString(),
                booking_email_sent_at = reader["booking_email_sent_at"]?.ToString(),
                booking_email_retry_requested = reader["booking_email_retry_requested"]?.ToString(),
                booking_email_retry_requested_at = reader["booking_email_retry_requested_at"]?.ToString(),
                booking_email_last_attempt_at = reader["booking_email_last_attempt_at"]?.ToString(),
                booking_email_last_error = reader["booking_email_last_error"]?.ToString(),

                terms_sent = reader["terms_sent"]?.ToString(),
                terms_sent_at = reader["terms_sent_at"]?.ToString(),
                terms_retry_requested = reader["terms_retry_requested"]?.ToString(),
                terms_retry_requested_at = reader["terms_retry_requested_at"]?.ToString(),
                terms_last_attempt_at = reader["terms_last_attempt_at"]?.ToString(),
                terms_last_error = reader["terms_last_error"]?.ToString(),
                terms_signed = reader["terms_signed"]?.ToString(),
                terms_signed_at = reader["terms_signed_at"]?.ToString(),
                signnow_document_id = reader["signnow_document_id"]?.ToString(),
                signnow_invite_id = reader["signnow_invite_id"]?.ToString(),
                signnow_template_id = reader["signnow_template_id"]?.ToString(),
                signnow_document_status = reader["signnow_document_status"]?.ToString(),
                signnow_last_checked_at = reader["signnow_last_checked_at"]?.ToString(),
                signnow_signing_link = reader["signnow_signing_link"]?.ToString(),

                invoice_sent = reader["invoice_sent"]?.ToString(),
                invoice_sent_at = reader["invoice_sent_at"]?.ToString(),
                invoice_retry_requested = reader["invoice_retry_requested"]?.ToString(),
                invoice_retry_requested_at = reader["invoice_retry_requested_at"]?.ToString(),
                invoice_last_attempt_at = reader["invoice_last_attempt_at"]?.ToString(),
                invoice_last_error = reader["invoice_last_error"]?.ToString(),
                job_total = reader["job_total"]?.ToString(),
                invoice_total = reader["job_total"]?.ToString(),

                paid = reader["paid"]?.ToString(),

                calendar_created = reader["calendar_created"]?.ToString(),
                calendar_created_at = reader["calendar_created_at"]?.ToString(),
                calendar_retry_requested = reader["calendar_retry_requested"]?.ToString(),
                calendar_retry_requested_at = reader["calendar_retry_requested_at"]?.ToString(),
                calendar_last_attempt_at = reader["calendar_last_attempt_at"]?.ToString(),
                calendar_last_error = reader["calendar_last_error"]?.ToString(),

                primary_service = reader["primary_service"]?.ToString(),
                additional1 = reader["additional1"]?.ToString(),
                additional2 = reader["additional2"]?.ToString(),
                primary_service_key = reader["primary_service_key"]?.ToString(),
                additional1_service_key = reader["additional1_service_key"]?.ToString(),
                additional2_service_key = reader["additional2_service_key"]?.ToString(),
                additional_service_keys = new[]
                {
                    reader["additional1_service_key"]?.ToString(),
                    reader["additional2_service_key"]?.ToString()
                }.Where(k => !string.IsNullOrWhiteSpace(k)).ToArray(),
                booking_template_key = reader["booking_template_key"]?.ToString(),
                booking_email_required = reader["booking_email_required"]?.ToString(),
                terms_required = reader["terms_required"]?.ToString(),
                invoice_required = reader["invoice_required"]?.ToString(),
                calendar_required = reader["calendar_required"]?.ToString(),
                report_required = reader["report_required"]?.ToString(),
                building_type = reader["building_type"]?.ToString(),
                stories = reader["stories"]?.ToString(),
                bedrooms = reader["bedrooms"]?.ToString(),
                bathrooms = reader["bathrooms"]?.ToString(),
                monolithic = reader["monolithic"]?.ToString(),
                outbuilding = reader["outbuilding"]?.ToString(),
                occupied = reader["occupied"]?.ToString(),
                attached_flat = reader["attached_flat"]?.ToString(),
                travel_fee = reader["travel_fee"]?.ToString(),
                hhs_bedrooms = reader["hhs_bedrooms"]?.ToString(),
                meth_samples = reader["meth_samples"]?.ToString(),
                hhs_reinspect = reader["hhs_reinspect"]?.ToString(),
                council_files = reader["council_files"]?.ToString(),
                foundation_space = reader["foundation_space"]?.ToString(),
                weathertightness = reader["weathertightness"]?.ToString(),
                hhs_reinspect_date = reader["hhs_reinspect_date"]?.ToString(),
                access_by = reader["access_by"]?.ToString(),
                hhs_compliance = reader["hhs_compliance"]?.ToString(),
                outbuilding_scope_label = ToScopeLabel(reader["outbuilding"]?.ToString()),
                attached_flat_scope_label = ToScopeLabel(reader["attached_flat"]?.ToString()),
                council_file_review_scope_label = ToScopeLabel(reader["council_files"]?.ToString()),
                weathertightness_scope_label = ToScopeLabel(reader["weathertightness"]?.ToString()),
                outbuilding_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include any separate garages or outbuildings?",
                    reader["outbuilding"]?.ToString()),
                attached_flat_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include any self-contained flats?",
                    reader["attached_flat"]?.ToString()),
                council_file_review_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include review of the Council Property File?",
                    reader["council_files"]?.ToString()),
                weathertightness_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include a non-invasive weathertightness assessment suitable for lender review?",
                    reader["weathertightness"]?.ToString()),
                additional_services_text = BuildAdditionalServicesText(reader["additional1"]?.ToString(), reader["additional2"]?.ToString()),
                additional_services_html = BuildAdditionalServicesHtml(reader["additional1"]?.ToString(), reader["additional2"]?.ToString()),

                contact1_salutation = reader["contact1_salutation"]?.ToString(),
                contact1_first_name = reader["contact1_first_name"]?.ToString(),
                contact1_last_name = reader["contact1_last_name"]?.ToString(),
                contact1_email = reader["contact1_email"]?.ToString(),
                contact1_cellular = reader["contact1_cellular"]?.ToString(),

                contact2_salutation = reader["contact2_salutation"]?.ToString(),
                contact2_first_name = reader["contact2_first_name"]?.ToString(),
                contact2_last_name = reader["contact2_last_name"]?.ToString(),
                contact2_email = reader["contact2_email"]?.ToString(),
                contact2_cellular = reader["contact2_cellular"]?.ToString(),

                extracted_at_utc = reader["extracted_at_utc"]?.ToString(),
                connector_version = reader["connector_version"]?.ToString(),
                source_instance = reader["source_instance"]?.ToString(),

                report_workflow_sent = reader["report_workflow_sent"]?.ToString(),
                report_workflow_sent_at = reader["report_workflow_sent_at"]?.ToString(),
                report_retry_requested = reader["report_retry_requested"]?.ToString(),
                report_retry_requested_at = reader["report_retry_requested_at"]?.ToString(),
                report_last_attempt_at = reader["report_last_attempt_at"]?.ToString(),
                report_last_error = reader["report_last_error"]?.ToString(),

                workflow_updated_at = reader["workflow_updated_at"]?.ToString(),
                created_at = reader["created_at"]?.ToString(),
                updated_at = reader["updated_at"]?.ToString(),

                company_name = reader["company_name"]?.ToString(),
                contact_name = reader["contact_name"]?.ToString(),
                email_from_name = reader["email_from_name"]?.ToString(),
                email_from_address = reader["email_from_address"]?.ToString(),
                phone = reader["phone"]?.ToString(),
                timezone = reader["timezone"]?.ToString(),
                allow_report_release_before_payment = reader["allow_report_release_before_payment"]?.ToString(),
                onboarding_status = reader["onboarding_status"]?.ToString(),
                logo_url = reader["logo_url"]?.ToString(),
                inspector_is_active = reader["inspector_is_active"]?.ToString(),

                subscription_status = reader["subscription_status"]?.ToString(),
                plan_name = reader["plan_name"]?.ToString(),
                billing_interval = reader["billing_interval"]?.ToString(),
                trial_ends_at = reader["trial_ends_at"]?.ToString(),
                current_period_end = reader["current_period_end"]?.ToString(),

                account_can_run_automation = reader["account_can_run_automation"]?.ToString()
            });
        }

        return Results.Ok(rows);
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Pending workflows query failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// GET JOB WORKFLOW STATUS
// Read-only connector view of Railway-owned state
// =============================
app.MapGet("/jobs/workflow-status", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureInspectorsTableAsync(conn);
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);
        await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn);
        await EnsureEmailTemplatesTableAsync(conn);
        await EnsureAdvancedActionsTablesAsync(conn);
        await AutomationFoundationSupport.EnsureAsync(conn);
        await JobChangeSupport.EnsureAsync(conn);

        const string sql = @"
SELECT
    j.job_id,
    j.job_name,
    j.site_address,
    j.job_date,
    j.date_added,
    j.source_updated_at,
    j.status,
    j.inspector_name,
    j.job_total,
    j.primary_service,
    j.additional1,
    j.additional2,
    j.booking_template_key,
    j.booking_email_required,
    j.booking_email_sent,
    j.booking_email_retry_requested,
    j.terms_required,
    j.terms_sent,
    j.terms_retry_requested,
    j.terms_signed,
    j.terms_signed_at,
    j.signnow_document_status,
    j.signnow_document_id,
    j.invoice_required,
    j.invoice_sent,
    j.invoice_retry_requested,
    j.calendar_required,
    j.calendar_created,
    j.calendar_retry_requested,
    j.report_required,
    j.report_workflow_sent,
    j.report_retry_requested,
    j.paid,
    j.contact1_first_name,
    j.contact1_last_name,
    j.contact1_display_name,
    j.contact1_salutation,
    j.contact1_role_label,
    j.contact1_email,
    j.weathertightness,
    j.workflow_updated_at,
    j.change_review_pending,
    j.address_change_pending,
    j.previous_site_address,
    j.address_change_detected_at,
    j.pending_change_json,
    j.pending_change_fingerprint,
    j.pending_change_reasons,
    j.change_detected_at,
    j.approved_snapshot_version,
    j.current_snapshot_fingerprint,
    j.xero_review_required,
    j.report_review_required,
    j.change_template_setup_required,
    j.source_missing,
    j.unscheduled,
    COALESCE(a.pending_action_count, 0) AS pending_action_count,
    COALESCE(a.sent_action_count, 0) AS sent_action_count,
    COALESCE(a.failed_action_count, 0) AS failed_action_count,
    COALESCE(a.pending_action_keys, '') AS pending_action_keys
FROM public.jobs_staging j
LEFT JOIN (
    SELECT
        job_id,
        COUNT(*) FILTER (WHERE status = 'pending' OR retry_requested = true) AS pending_action_count,
        COUNT(*) FILTER (WHERE status = 'sent') AS sent_action_count,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed_action_count,
        string_agg(action_key, ', ' ORDER BY action_key) FILTER (WHERE status = 'pending' OR retry_requested = true) AS pending_action_keys
    FROM public.job_workflow_actions
    GROUP BY job_id
) a
    ON a.job_id = j.job_id
ORDER BY j.job_date DESC NULLS LAST, COALESCE(j.workflow_updated_at, j.updated_at, j.created_at) DESC
LIMIT 500;";

        var rows = new List<object>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            rows.Add(new
            {
                job_id = reader["job_id"]?.ToString(),
                job_name = reader["job_name"]?.ToString(),
                site_address = reader["site_address"]?.ToString(),
                job_date = reader["job_date"]?.ToString(),
                date_added = reader["date_added"]?.ToString(),
                source_updated_at = reader["source_updated_at"]?.ToString(),
                status = reader["status"]?.ToString(),
                inspector_name = reader["inspector_name"]?.ToString(),
                job_total = reader["job_total"]?.ToString(),
                invoice_total = reader["job_total"]?.ToString(),
                primary_service = reader["primary_service"]?.ToString(),
                additional1 = reader["additional1"]?.ToString(),
                additional2 = reader["additional2"]?.ToString(),
                booking_template_key = reader["booking_template_key"]?.ToString(),
                booking_email_required = reader["booking_email_required"]?.ToString(),
                booking_email_sent = reader["booking_email_sent"]?.ToString(),
                booking_email_retry_requested = reader["booking_email_retry_requested"]?.ToString(),
                terms_required = reader["terms_required"]?.ToString(),
                terms_sent = reader["terms_sent"]?.ToString(),
                terms_retry_requested = reader["terms_retry_requested"]?.ToString(),
                terms_signed = reader["terms_signed"]?.ToString(),
                terms_signed_at = reader["terms_signed_at"]?.ToString(),
                signnow_document_status = reader["signnow_document_status"]?.ToString(),
                signnow_document_id = reader["signnow_document_id"]?.ToString(),
                invoice_required = reader["invoice_required"]?.ToString(),
                invoice_sent = reader["invoice_sent"]?.ToString(),
                invoice_retry_requested = reader["invoice_retry_requested"]?.ToString(),
                calendar_required = reader["calendar_required"]?.ToString(),
                calendar_created = reader["calendar_created"]?.ToString(),
                calendar_retry_requested = reader["calendar_retry_requested"]?.ToString(),
                report_required = reader["report_required"]?.ToString(),
                report_workflow_sent = reader["report_workflow_sent"]?.ToString(),
                report_retry_requested = reader["report_retry_requested"]?.ToString(),
                paid = reader["paid"]?.ToString(),
                contact1_first_name = reader["contact1_first_name"]?.ToString(),
                contact1_last_name = reader["contact1_last_name"]?.ToString(),
                contact1_email = reader["contact1_email"]?.ToString(),
                weathertightness = reader["weathertightness"]?.ToString(),
                weathertightness_scope_label = ToScopeLabel(reader["weathertightness"]?.ToString()),
                workflow_updated_at = reader["workflow_updated_at"]?.ToString(),
                change_review_pending = reader["change_review_pending"]?.ToString(),
                address_change_pending = reader["address_change_pending"]?.ToString(),
                previous_site_address = reader["previous_site_address"]?.ToString(),
                address_change_detected_at = reader["address_change_detected_at"]?.ToString(),
                pending_change_json = reader["pending_change_json"]?.ToString(),
                pending_change_fingerprint = reader["pending_change_fingerprint"]?.ToString(),
                pending_change_reasons = reader["pending_change_reasons"]?.ToString(),
                change_detected_at = reader["change_detected_at"]?.ToString(),
                approved_snapshot_version = reader["approved_snapshot_version"]?.ToString(),
                current_snapshot_fingerprint = reader["current_snapshot_fingerprint"]?.ToString(),
                xero_review_required = reader["xero_review_required"]?.ToString(),
                report_review_required = reader["report_review_required"]?.ToString(),
                change_template_setup_required = reader["change_template_setup_required"]?.ToString(),
                source_missing = reader["source_missing"]?.ToString(),
                unscheduled = reader["unscheduled"]?.ToString(),
                pending_action_count = reader["pending_action_count"]?.ToString(),
                sent_action_count = reader["sent_action_count"]?.ToString(),
                failed_action_count = reader["failed_action_count"]?.ToString(),
                pending_action_keys = reader["pending_action_keys"]?.ToString()
            });
        }

        return Results.Ok(rows);
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Workflow status query failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// GET PENDING WORKFLOW ACTIONS
// One row per service-level action for new V1 Zaps
// =============================
app.MapGet("/workflow-actions/pending", async (Guid? inspectorId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);
        await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn);

        const string sql = @"
SELECT
    a.job_id,
    a.tenant_id,
    a.inspector_id,
    a.action_key,
    a.action_type,
    a.service_key,
    a.service_label,
    a.service_slot,
    a.status AS action_status,
    a.retry_requested AS action_retry_requested,
    a.sent_at AS action_sent_at,
    a.last_attempt_at AS action_last_attempt_at,
    a.last_error AS action_last_error,
    a.created_at AS action_created_at,
    a.updated_at AS action_updated_at,

    j.inspector_name,
    j.source_system,
    j.job_name,
    j.site_address,
    j.age_of_building,
    j.job_date,
    j.inspection_duration_minutes,
    j.source_updated_at,
    j.date_added,
    j.status,
    j.job_total,
    j.primary_service,
    j.additional1,
    j.additional2,
    j.primary_service_key,
    j.additional1_service_key,
    j.additional2_service_key,
    j.booking_template_key,
    j.building_type,
    j.stories,
    j.bedrooms,
    j.bathrooms,
    j.monolithic,
    j.outbuilding,
    j.occupied,
    j.attached_flat,
    j.travel_fee,
    j.hhs_bedrooms,
    j.meth_samples,
    j.hhs_reinspect,
    j.council_files,
    j.foundation_space,
    j.weathertightness,
    j.hhs_reinspect_date,
    j.access_by,
    j.hhs_compliance,
    j.contact1_salutation,
    j.contact1_first_name,
    j.contact1_last_name,
    j.contact1_email,
    j.contact1_cellular,
    j.contact2_salutation,
    j.contact2_first_name,
    j.contact2_last_name,
    j.contact2_display_name,
    j.contact2_salutation,
    j.contact2_role_label,
    j.contact2_email,
    j.contact2_cellular,
    j.extracted_at_utc,
    j.connector_version,
    j.source_instance,
    j.workflow_updated_at,
    j.created_at,
    j.updated_at,

    i.company_name,
    i.contact_name,
    i.email_from_name,
    COALESCE(NULLIF(j.inspector_email,''),i.email_from_address) AS email_from_address,
    COALESCE(NULLIF(j.inspector_phone,''),i.phone) AS phone,
    i.timezone,
    COALESCE(i.email_sender_mode, 'microsoft') AS email_sender_mode,
    i.allow_report_release_before_payment,
    i.onboarding_status,
    i.logo_url,
    i.is_active AS inspector_is_active,

    s.status AS subscription_status,
    s.plan_name,
    s.billing_interval,
    s.trial_ends_at,
    s.current_period_end,

    CASE
        WHEN COALESCE(i.is_active, false) = true
         AND COALESCE(i.onboarding_status, '') IN ('complete', 'in_progress')
         AND COALESCE(s.status, '') IN ('active', 'trialing')
        THEN true
        ELSE false
    END AS account_can_run_automation

FROM public.job_workflow_actions a
JOIN public.jobs_staging j
    ON j.job_id = a.job_id
LEFT JOIN public.inspectors i
    ON i.tenant_id::text = j.tenant_id::text
LEFT JOIN LATERAL (
    SELECT *
    FROM public.subscriptions s
    WHERE s.inspector_id::text = i.inspector_id::text
    ORDER BY
        CASE
            WHEN s.status IN ('active', 'trialing', 'past_due') THEN 0
            ELSE 1
        END,
        s.current_period_end DESC NULLS LAST,
        s.created_at DESC
    LIMIT 1
) s ON TRUE
WHERE a.action_type = 'booking_email'
  AND (a.status = 'pending' OR a.retry_requested = true)
  AND NOT j.change_review_pending AND NOT j.unscheduled
  AND NOT EXISTS (SELECT 1 FROM public.automation_tenant_settings ats WHERE ats.tenant_id::text=j.tenant_id::text AND ats.activation_mode='all_jobs')
  AND NOT EXISTS (SELECT 1 FROM public.automation_job_selections ajs WHERE ajs.tenant_id::text=j.tenant_id::text AND ajs.job_id=j.job_id AND ajs.use_advanced_workflows=true)
  AND NOT EXISTS (SELECT 1 FROM public.basic_automation_settings bas WHERE bas.tenant_id::text=j.tenant_id::text AND bas.event_key='scheduling')
  AND (@inspector_id IS NULL OR a.inspector_id = @inspector_id)
ORDER BY a.updated_at ASC
LIMIT 100;";

        var rows = new List<object>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("inspector_id", inspectorId.HasValue && inspectorId.Value != Guid.Empty
            ? inspectorId.Value
            : DBNull.Value);
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            rows.Add(new
            {
                job_id = reader["job_id"]?.ToString(),
                tenant_id = reader["tenant_id"]?.ToString(),
                inspector_id = reader["inspector_id"]?.ToString(),
                action_key = reader["action_key"]?.ToString(),
                action_type = reader["action_type"]?.ToString(),
                service_key = reader["service_key"]?.ToString(),
                service_label = reader["service_label"]?.ToString(),
                service_slot = reader["service_slot"]?.ToString(),
                action_status = reader["action_status"]?.ToString(),
                action_retry_requested = reader["action_retry_requested"]?.ToString(),
                action_sent_at = reader["action_sent_at"]?.ToString(),
                action_last_attempt_at = reader["action_last_attempt_at"]?.ToString(),
                action_last_error = reader["action_last_error"]?.ToString(),

                inspector_name = reader["inspector_name"]?.ToString(),
                source_system = reader["source_system"]?.ToString(),
                job_name = reader["job_name"]?.ToString(),
                site_address = reader["site_address"]?.ToString(),
                age_of_building = reader["age_of_building"]?.ToString(),
                job_date = reader["job_date"]?.ToString(),
                inspection_duration_minutes = reader["inspection_duration_minutes"]?.ToString(),
                source_updated_at = reader["source_updated_at"]?.ToString(),
                date_added = reader["date_added"]?.ToString(),
                status = reader["status"]?.ToString(),
                job_total = reader["job_total"]?.ToString(),
                invoice_total = reader["job_total"]?.ToString(),
                primary_service = reader["primary_service"]?.ToString(),
                additional1 = reader["additional1"]?.ToString(),
                additional2 = reader["additional2"]?.ToString(),
                primary_service_key = reader["primary_service_key"]?.ToString(),
                additional1_service_key = reader["additional1_service_key"]?.ToString(),
                additional2_service_key = reader["additional2_service_key"]?.ToString(),
                additional_service_keys = new[]
                {
                    reader["additional1_service_key"]?.ToString(),
                    reader["additional2_service_key"]?.ToString()
                }.Where(k => !string.IsNullOrWhiteSpace(k)).ToArray(),
                booking_template_key = reader["booking_template_key"]?.ToString(),

                building_type = reader["building_type"]?.ToString(),
                stories = reader["stories"]?.ToString(),
                bedrooms = reader["bedrooms"]?.ToString(),
                bathrooms = reader["bathrooms"]?.ToString(),
                monolithic = reader["monolithic"]?.ToString(),
                outbuilding = reader["outbuilding"]?.ToString(),
                occupied = reader["occupied"]?.ToString(),
                attached_flat = reader["attached_flat"]?.ToString(),
                travel_fee = reader["travel_fee"]?.ToString(),
                hhs_bedrooms = reader["hhs_bedrooms"]?.ToString(),
                meth_samples = reader["meth_samples"]?.ToString(),
                hhs_reinspect = reader["hhs_reinspect"]?.ToString(),
                council_files = reader["council_files"]?.ToString(),
                foundation_space = reader["foundation_space"]?.ToString(),
                weathertightness = reader["weathertightness"]?.ToString(),
                hhs_reinspect_date = reader["hhs_reinspect_date"]?.ToString(),
                access_by = reader["access_by"]?.ToString(),
                hhs_compliance = reader["hhs_compliance"]?.ToString(),
                outbuilding_scope_label = ToScopeLabel(reader["outbuilding"]?.ToString()),
                attached_flat_scope_label = ToScopeLabel(reader["attached_flat"]?.ToString()),
                council_file_review_scope_label = ToScopeLabel(reader["council_files"]?.ToString()),
                weathertightness_scope_label = ToScopeLabel(reader["weathertightness"]?.ToString()),
                outbuilding_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include any separate garages or outbuildings?",
                    reader["outbuilding"]?.ToString()),
                attached_flat_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include any self-contained flats?",
                    reader["attached_flat"]?.ToString()),
                council_file_review_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include review of the Council Property File?",
                    reader["council_files"]?.ToString()),
                weathertightness_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include a non-invasive weathertightness assessment suitable for lender review?",
                    reader["weathertightness"]?.ToString()),
                additional_services_text = BuildAdditionalServicesText(reader["additional1"]?.ToString(), reader["additional2"]?.ToString()),
                additional_services_html = BuildAdditionalServicesHtml(reader["additional1"]?.ToString(), reader["additional2"]?.ToString()),

                contact1_salutation = reader["contact1_salutation"]?.ToString(),
                contact1_first_name = reader["contact1_first_name"]?.ToString(),
                contact1_last_name = reader["contact1_last_name"]?.ToString(),
                contact1_email = reader["contact1_email"]?.ToString(),
                contact1_cellular = reader["contact1_cellular"]?.ToString(),
                contact2_salutation = reader["contact2_salutation"]?.ToString(),
                contact2_first_name = reader["contact2_first_name"]?.ToString(),
                contact2_last_name = reader["contact2_last_name"]?.ToString(),
                contact2_email = reader["contact2_email"]?.ToString(),
                contact2_cellular = reader["contact2_cellular"]?.ToString(),

                extracted_at_utc = reader["extracted_at_utc"]?.ToString(),
                connector_version = reader["connector_version"]?.ToString(),
                source_instance = reader["source_instance"]?.ToString(),

                company_name = reader["company_name"]?.ToString(),
                contact_name = reader["contact_name"]?.ToString(),
                email_from_name = reader["email_from_name"]?.ToString(),
                email_from_address = reader["email_from_address"]?.ToString(),
                phone = reader["phone"]?.ToString(),
                timezone = reader["timezone"]?.ToString(),
                email_sender_mode = reader["email_sender_mode"]?.ToString(),
                allow_report_release_before_payment = reader["allow_report_release_before_payment"]?.ToString(),
                onboarding_status = reader["onboarding_status"]?.ToString(),
                logo_url = reader["logo_url"]?.ToString(),
                inspector_is_active = reader["inspector_is_active"]?.ToString(),
                subscription_status = reader["subscription_status"]?.ToString(),
                plan_name = reader["plan_name"]?.ToString(),
                billing_interval = reader["billing_interval"]?.ToString(),
                trial_ends_at = reader["trial_ends_at"]?.ToString(),
                current_period_end = reader["current_period_end"]?.ToString(),
                account_can_run_automation = reader["account_can_run_automation"]?.ToString()
            });
        }

        return Results.Ok(rows);
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Pending workflow actions query failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK WORKFLOW ACTION SENT
// =============================
app.MapPost("/jobs/{jobId}/workflow-actions/{actionKey}/sent", async (Guid jobId, string actionKey) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
UPDATE public.job_workflow_actions
SET
    status = 'sent',
    retry_requested = false,
    sent_at = NOW(),
    last_attempt_at = NOW(),
    last_error = NULL,
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_key = @action_key;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("action_key", actionKey);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId,
            actionKey
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark workflow action sent failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK WORKFLOW ACTION FAILED
// =============================
app.MapPost("/jobs/{jobId}/workflow-actions/{actionKey}/failed", async (Guid jobId, string actionKey, WorkflowActionFailureRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
UPDATE public.job_workflow_actions
SET
    status = 'failed',
    retry_requested = false,
    last_attempt_at = NOW(),
    last_error = @error_message,
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_key = @action_key;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("action_key", actionKey);
        cmd.Parameters.AddWithValue("error_message", request.ErrorMessage ?? "");

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId,
            actionKey
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark workflow action failed failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST WORKFLOW ACTION RETRY
// =============================
app.MapPost("/jobs/{jobId}/workflow-actions/{actionKey}/retry", async (Guid jobId, string actionKey) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
UPDATE public.job_workflow_actions
SET
    status = 'pending',
    retry_requested = true,
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_key = @action_key;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("action_key", actionKey);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId,
            actionKey
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request workflow action retry failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST ALL WORKFLOW ACTIONS RETRY
// Testing/admin helper for connector Reset Workflow
// =============================
app.MapPost("/jobs/{jobId}/workflow-actions/retry-all", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
UPDATE public.job_workflow_actions
SET
    status = 'pending',
    retry_requested = true,
    sent_at = NULL,
    last_attempt_at = NULL,
    last_error = NULL,
    updated_at = NOW()
WHERE job_id = @job_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request all workflow action retries failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// UPDATE JOB WORKFLOW REQUIREMENTS
// Connector Automation screen checkboxes for per-job workflow steps.
// =============================
app.MapPatch("/jobs/{jobId}/workflow-requirements", async (Guid jobId, JobWorkflowRequirementsRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
UPDATE public.jobs_staging
SET
    booking_email_required = COALESCE(@booking_email_required, booking_email_required),
    terms_required = COALESCE(@terms_required, terms_required),
    invoice_required = COALESCE(@invoice_required, invoice_required),
    calendar_required = COALESCE(@calendar_required, calendar_required),
    report_required = COALESCE(@report_required, report_required),
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

        await using (var cmd = new NpgsqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("job_id", jobId);
            cmd.Parameters.AddWithValue("booking_email_required", request.BookingEmailRequired.HasValue ? request.BookingEmailRequired.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("terms_required", request.TermsRequired.HasValue ? request.TermsRequired.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("invoice_required", request.InvoiceRequired.HasValue ? request.InvoiceRequired.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("calendar_required", request.CalendarRequired.HasValue ? request.CalendarRequired.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("report_required", request.ReportRequired.HasValue ? request.ReportRequired.Value : (object)DBNull.Value);

            var updated = await cmd.ExecuteNonQueryAsync();
            if (updated == 0)
            {
                return Results.NotFound(new
                {
                    success = false,
                    message = "Job was not found.",
                    jobId
                });
            }
        }

        if (request.BookingEmailRequired == false)
        {
            await using var disableActionsCmd = new NpgsqlCommand(@"
UPDATE public.job_workflow_actions
SET
    status = 'disabled',
    retry_requested = false,
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_type = 'booking_email'
  AND status <> 'sent';", conn);
            disableActionsCmd.Parameters.AddWithValue("job_id", jobId);
            await disableActionsCmd.ExecuteNonQueryAsync();
        }
        else if (request.BookingEmailRequired == true)
        {
            await using var enableActionsCmd = new NpgsqlCommand(@"
UPDATE public.job_workflow_actions
SET
    status = 'pending',
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_type = 'booking_email'
  AND status = 'disabled';", conn);
            enableActionsCmd.Parameters.AddWithValue("job_id", jobId);
            await enableActionsCmd.ExecuteNonQueryAsync();
        }

        return Results.Ok(new
        {
            success = true,
            message = "Workflow requirements updated.",
            jobId
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Update workflow requirements failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// HARD RESET JOB FOR TESTING
// Railway-only reset; external Xero/Google/email artifacts are not deleted.
// =============================
app.MapPost("/jobs/{jobId}/hard-reset", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await using var tx = await conn.BeginTransactionAsync();

        const string resetJobSql = @"
UPDATE public.jobs_staging
SET
    status = 'Pending',

    booking_email_sent = false,
    booking_email_sent_at = NULL,
    booking_email_retry_requested = false,
    booking_email_retry_requested_at = NULL,
    booking_email_last_attempt_at = NULL,
    booking_email_last_error = NULL,

    terms_sent = false,
    terms_sent_at = NULL,
    terms_retry_requested = false,
    terms_retry_requested_at = NULL,
    terms_last_attempt_at = NULL,
    terms_last_error = NULL,
    terms_signed = false,
    terms_signed_at = NULL,
    signnow_document_id = NULL,
    signnow_invite_id = NULL,
    signnow_template_id = NULL,
    signnow_document_status = NULL,
    signnow_last_checked_at = NULL,
    signnow_signing_link = NULL,
    signnow_webhook_subscription_id = NULL,
    signnow_webhook_status = NULL,
    signnow_webhook_last_error = NULL,

    invoice_sent = false,
    invoice_sent_at = NULL,
    invoice_retry_requested = false,
    invoice_retry_requested_at = NULL,
    invoice_last_attempt_at = NULL,
    invoice_last_error = NULL,

    calendar_created = false,
    calendar_created_at = NULL,
    calendar_retry_requested = false,
    calendar_retry_requested_at = NULL,
    calendar_last_attempt_at = NULL,
    calendar_last_error = NULL,

    report_workflow_sent = false,
    report_workflow_sent_at = NULL,
    report_retry_requested = false,
    report_retry_requested_at = NULL,
    report_last_attempt_at = NULL,
    report_last_error = NULL,
    report_available = false,

    paid = false,
    marked_as_paid_override = false,
    amount_paid = 0,
    amount_outstanding = job_total,
    payment_status = 'unpaid',

    xero_contact_id = NULL,
    xero_invoice_id = NULL,
    xero_invoice_number = NULL,
    xero_invoice_status = NULL,
    xero_invoice_created_at = NULL,
    xero_last_error = NULL,

    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

        int jobRows;
        await using (var resetJobCmd = new NpgsqlCommand(resetJobSql, conn, tx))
        {
            resetJobCmd.Parameters.AddWithValue("job_id", jobId);
            jobRows = await resetJobCmd.ExecuteNonQueryAsync();
        }

        if (jobRows == 0)
        {
            await tx.RollbackAsync();
            return Results.NotFound(new
            {
                success = false,
                message = "Job was not found in Railway. Sync the selected job first.",
                jobId
            });
        }

        await tx.CommitAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Job hard reset for testing. Railway state was cleared; external Xero, Google, and email artifacts were not deleted.",
            jobId,
            jobRows,
            workflowActionRows = 0
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Hard reset job failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK BOOKING EMAIL SENT
// =============================
app.MapPost("/jobs/{jobId}/mark-booking-email-sent", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    booking_email_sent = true,
    booking_email_sent_at = NOW(),
    booking_email_retry_requested = false,
    booking_email_retry_requested_at = NULL,
    booking_email_last_attempt_at = NOW(),
    booking_email_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId = jobId
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark booking email sent failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST BOOKING EMAIL RETRY
// =============================
app.MapPost("/jobs/{jobId}/request-booking-email-retry", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    booking_email_retry_requested = true,
    booking_email_retry_requested_at = NOW(),
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId = jobId
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request booking email retry failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK BOOKING EMAIL FAILED
// =============================
app.MapPost("/jobs/{jobId}/mark-booking-email-failed", async (Guid jobId, BookingEmailFailureRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    booking_email_last_attempt_at = NOW(),
    booking_email_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("error_message", request.ErrorMessage ?? "");

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            updated = rows,
            jobId = jobId
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark booking email failed failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK TERMS SENT
// =============================
app.MapPost("/jobs/{jobId}/mark-terms-sent", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    terms_sent = true,
    terms_sent_at = NOW(),
    terms_retry_requested = false,
    terms_retry_requested_at = NULL,
    terms_last_attempt_at = NOW(),
    terms_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked terms sent for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark terms sent failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST TERMS RETRY
// =============================
app.MapPost("/jobs/{jobId}/request-terms-retry", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    terms_retry_requested = true,
    terms_retry_requested_at = NOW(),
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Requested terms retry for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request terms retry failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// RESET SIGNNOW TERMS ONLY
// Preserves all non-terms workflow state and leaves the old document in SignNow.
// =============================
app.MapPost("/jobs/{jobId}/terms/reset", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureSignNowJobColumnsAsync(conn);

        const string sql = @"
UPDATE public.jobs_staging
SET
    terms_required = true,
    terms_sent = false,
    terms_sent_at = NULL,
    terms_signed = false,
    terms_signed_at = NULL,
    terms_retry_requested = false,
    terms_retry_requested_at = NULL,
    terms_last_attempt_at = NULL,
    terms_last_error = NULL,
    signnow_document_id = NULL,
    signnow_invite_id = NULL,
    signnow_template_id = NULL,
    signnow_document_status = NULL,
    signnow_last_checked_at = NULL,
    signnow_signing_link = NULL,
    signnow_webhook_subscription_id = NULL,
    signnow_webhook_status = NULL,
    signnow_webhook_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id
RETURNING job_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        var updated = await cmd.ExecuteScalarAsync();
        return updated == null
            ? Results.NotFound(new { success = false, message = "Job was not found.", jobId })
            : Results.Ok(new { success = true, message = "SignNow Terms state reset only.", jobId });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Reset SignNow Terms failed", detail: ex.ToString(), statusCode: 500);
    }
});

// =============================
// MARK TERMS FAILED
// =============================

app.MapPost("/jobs/{jobId}/mark-terms-failed", async (Guid jobId, TermsFailureRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    terms_last_attempt_at = NOW(),
    terms_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("error_message", request.ErrorMessage ?? "Unknown error");

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked terms failed for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark terms failed failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK INVOICE SENT
// =============================
app.MapPost("/jobs/{jobId}/mark-invoice-sent", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    invoice_sent = true,
    invoice_sent_at = NOW(),
    invoice_retry_requested = false,
    invoice_retry_requested_at = NULL,
    invoice_last_attempt_at = NOW(),
    invoice_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked invoice sent for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark invoice sent failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST INVOICE RETRY
// =============================
app.MapPost("/jobs/{jobId}/request-invoice-retry", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    invoice_retry_requested = true,
    invoice_retry_requested_at = NOW(),
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Requested invoice retry for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request invoice retry failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK INVOICE FAILED
// =============================

app.MapPost("/jobs/{jobId}/mark-invoice-failed", async (Guid jobId, InvoiceFailureRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    invoice_last_attempt_at = NOW(),
    invoice_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("error_message", request.ErrorMessage ?? "Unknown error");
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked invoice failed for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark invoice failed failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK CALENDAR CREATED
// =============================
app.MapPost("/jobs/{jobId}/mark-calendar-created", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    calendar_created = true,
    calendar_created_at = NOW(),
    calendar_retry_requested = false,
    calendar_retry_requested_at = NULL,
    calendar_last_attempt_at = NOW(),
    calendar_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked calendar created for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark calendar created failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST CALENDAR RETRY
// =============================
app.MapPost("/jobs/{jobId}/request-calendar-retry", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    calendar_retry_requested = true,
    calendar_retry_requested_at = NOW(),
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Requested calendar retry for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request calendar retry failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK CALENDAR FAILED
// =============================

app.MapPost("/jobs/{jobId}/mark-calendar-failed", async (Guid jobId, CalendarFailureRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    calendar_last_attempt_at = NOW(),
    calendar_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("error_message", request.ErrorMessage ?? "Unknown error");

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked calendar failed for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark calendar failed failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK REPORT AVAILABLE
// =============================
app.MapPost("/jobs/{jobId}/mark-report-available", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);

        const string sql = @"
UPDATE public.jobs_staging
SET
    report_available = true,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked report available for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark report available failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK REPORT SENT
// =============================
app.MapPost("/jobs/{jobId}/mark-report-sent", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    report_workflow_sent = true,
    report_workflow_sent_at = NOW(),
    report_retry_requested = false,
    report_retry_requested_at = NULL,
    report_last_attempt_at = NOW(),
    report_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked report sent for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark report sent failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// REQUEST REPORT RETRY
// =============================
app.MapPost("/jobs/{jobId}/request-report-retry", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    report_retry_requested = true,
    report_retry_requested_at = NOW(),
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Requested report retry for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Request report retry failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// MARK REPORT FAILED
// =============================

app.MapPost("/jobs/{jobId}/mark-report-failed", async (Guid jobId, ReportFailureRequest request) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
UPDATE public.jobs_staging
SET
    report_last_attempt_at = NOW(),
    report_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("error_message", request.ErrorMessage ?? "Unknown error");

        int rows = await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = $"Marked report failed for job {jobId}",
            rows_affected = rows
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Mark report failed failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// GET LATEST JOBS
// =============================
app.MapGet("/jobs/latest", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);

        const string sql = @"
SELECT
    job_id,
    tenant_id,
    inspector_id,
    inspector_name,
    source_system,
    job_name,
    site_address,
    age_of_building,
    job_date,
    inspection_duration_minutes,
    source_updated_at,
    date_added,
    status,
    zap_processed,
    report_sent,
    booking_email_sent,
    booking_email_sent_at,
    booking_email_retry_requested,
    booking_email_retry_requested_at,
    booking_email_last_attempt_at,
    booking_email_last_error,
    terms_sent,
    terms_sent_at,
    terms_retry_requested,
    terms_retry_requested_at,
    terms_last_attempt_at,
    terms_last_error,
    terms_signed,
    terms_signed_at,
    signnow_document_id,
    signnow_invite_id,
    signnow_template_id,
    signnow_document_status,
    signnow_last_checked_at,
    signnow_signing_link,
    invoice_sent,
    invoice_sent_at,
    invoice_retry_requested,
    invoice_retry_requested_at,
    invoice_last_attempt_at,
    invoice_last_error,
    paid,
    marked_as_paid_override,
    report_available,
    job_total,
    amount_paid,
    amount_outstanding,
    payment_status,
    calendar_created,
    calendar_created_at,
    calendar_retry_requested,
    calendar_retry_requested_at,
    calendar_last_attempt_at,
    calendar_last_error,
    primary_service,
    additional1,
    additional2,
    outbuilding,
    attached_flat,
    council_files,
    weathertightness,
    contact1_salutation,
    contact1_first_name,
    contact1_last_name,
    contact1_email,
    contact1_cellular,
    contact2_salutation,
    contact2_first_name,
    contact2_last_name,
    contact2_email,
    contact2_cellular,
    extracted_at_utc,
    connector_version,
    source_instance,
    report_workflow_sent,
    report_workflow_sent_at,
    report_retry_requested,
    report_retry_requested_at,
    report_last_attempt_at,
    report_last_error,
    workflow_updated_at,
    created_at,
    updated_at
FROM public.jobs_staging
ORDER BY updated_at DESC
LIMIT 20;";

        var rows = new List<object>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            rows.Add(new
            {
                job_id = reader["job_id"]?.ToString(),
                tenant_id = reader["tenant_id"]?.ToString(),
                inspector_id = reader["inspector_id"]?.ToString(),
                inspector_name = reader["inspector_name"]?.ToString(),
                source_system = reader["source_system"]?.ToString(),
                job_name = reader["job_name"]?.ToString(),
                site_address = reader["site_address"]?.ToString(),
                age_of_building = reader["age_of_building"]?.ToString(),
                job_date = reader["job_date"]?.ToString(),
                inspection_duration_minutes = reader["inspection_duration_minutes"]?.ToString(),
                source_updated_at = reader["source_updated_at"]?.ToString(),
                date_added = reader["date_added"]?.ToString(),
                status = reader["status"]?.ToString(),
                zap_processed = reader["zap_processed"]?.ToString(),
                report_sent = reader["report_sent"]?.ToString(),

                booking_email_sent = reader["booking_email_sent"]?.ToString(),
                booking_email_sent_at = reader["booking_email_sent_at"]?.ToString(),
                booking_email_retry_requested = reader["booking_email_retry_requested"]?.ToString(),
                booking_email_retry_requested_at = reader["booking_email_retry_requested_at"]?.ToString(),
                booking_email_last_attempt_at = reader["booking_email_last_attempt_at"]?.ToString(),
                booking_email_last_error = reader["booking_email_last_error"]?.ToString(),

                terms_sent = reader["terms_sent"]?.ToString(),
                terms_sent_at = reader["terms_sent_at"]?.ToString(),
                terms_retry_requested = reader["terms_retry_requested"]?.ToString(),
                terms_retry_requested_at = reader["terms_retry_requested_at"]?.ToString(),
                terms_last_attempt_at = reader["terms_last_attempt_at"]?.ToString(),
                terms_last_error = reader["terms_last_error"]?.ToString(),
                terms_signed = reader["terms_signed"]?.ToString(),
                terms_signed_at = reader["terms_signed_at"]?.ToString(),
                signnow_document_id = reader["signnow_document_id"]?.ToString(),
                signnow_invite_id = reader["signnow_invite_id"]?.ToString(),
                signnow_template_id = reader["signnow_template_id"]?.ToString(),
                signnow_document_status = reader["signnow_document_status"]?.ToString(),
                signnow_last_checked_at = reader["signnow_last_checked_at"]?.ToString(),
                signnow_signing_link = reader["signnow_signing_link"]?.ToString(),

                invoice_sent = reader["invoice_sent"]?.ToString(),
                invoice_sent_at = reader["invoice_sent_at"]?.ToString(),
                invoice_retry_requested = reader["invoice_retry_requested"]?.ToString(),
                invoice_retry_requested_at = reader["invoice_retry_requested_at"]?.ToString(),
                invoice_last_attempt_at = reader["invoice_last_attempt_at"]?.ToString(),
                invoice_last_error = reader["invoice_last_error"]?.ToString(),
                job_total = reader["job_total"]?.ToString(),
                invoice_total = reader["job_total"]?.ToString(),

                paid = reader["paid"]?.ToString(),

                calendar_created = reader["calendar_created"]?.ToString(),
                calendar_created_at = reader["calendar_created_at"]?.ToString(),
                calendar_retry_requested = reader["calendar_retry_requested"]?.ToString(),
                calendar_retry_requested_at = reader["calendar_retry_requested_at"]?.ToString(),
                calendar_last_attempt_at = reader["calendar_last_attempt_at"]?.ToString(),
                calendar_last_error = reader["calendar_last_error"]?.ToString(),

                primary_service = reader["primary_service"]?.ToString(),
                additional1 = reader["additional1"]?.ToString(),
                additional2 = reader["additional2"]?.ToString(),
                outbuilding = reader["outbuilding"]?.ToString(),
                attached_flat = reader["attached_flat"]?.ToString(),
                council_files = reader["council_files"]?.ToString(),
                weathertightness = reader["weathertightness"]?.ToString(),
                outbuilding_scope_label = ToScopeLabel(reader["outbuilding"]?.ToString()),
                attached_flat_scope_label = ToScopeLabel(reader["attached_flat"]?.ToString()),
                council_file_review_scope_label = ToScopeLabel(reader["council_files"]?.ToString()),
                weathertightness_scope_label = ToScopeLabel(reader["weathertightness"]?.ToString()),
                outbuilding_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include any separate garages or outbuildings?",
                    reader["outbuilding"]?.ToString()),
                attached_flat_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include any self-contained flats?",
                    reader["attached_flat"]?.ToString()),
                council_file_review_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include review of the Council Property File?",
                    reader["council_files"]?.ToString()),
                weathertightness_scope_html = BuildScopeHtml(
                    "Does the scope of this inspection include a non-invasive weathertightness assessment suitable for lender review?",
                    reader["weathertightness"]?.ToString()),

                contact1_salutation = reader["contact1_salutation"]?.ToString(),
                contact1_first_name = reader["contact1_first_name"]?.ToString(),
                contact1_last_name = reader["contact1_last_name"]?.ToString(),
                contact1_email = reader["contact1_email"]?.ToString(),
                contact1_cellular = reader["contact1_cellular"]?.ToString(),

                contact2_salutation = reader["contact2_salutation"]?.ToString(),
                contact2_first_name = reader["contact2_first_name"]?.ToString(),
                contact2_last_name = reader["contact2_last_name"]?.ToString(),
                contact2_email = reader["contact2_email"]?.ToString(),
                contact2_cellular = reader["contact2_cellular"]?.ToString(),

                extracted_at_utc = reader["extracted_at_utc"]?.ToString(),
                connector_version = reader["connector_version"]?.ToString(),
                source_instance = reader["source_instance"]?.ToString(),

                report_workflow_sent = reader["report_workflow_sent"]?.ToString(),
                report_workflow_sent_at = reader["report_workflow_sent_at"]?.ToString(),
                report_retry_requested = reader["report_retry_requested"]?.ToString(),
                report_retry_requested_at = reader["report_retry_requested_at"]?.ToString(),
                report_last_attempt_at = reader["report_last_attempt_at"]?.ToString(),
                report_last_error = reader["report_last_error"]?.ToString(),

                workflow_updated_at = reader["workflow_updated_at"]?.ToString(),
                created_at = reader["created_at"]?.ToString(),
                updated_at = reader["updated_at"]?.ToString()
            });
        }

        return Results.Ok(rows);
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Latest jobs failed",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// UPSERT JOB FROM CONNECTOR
// =============================
app.MapPost("/jobs/upsert", async (HttpContext context) =>
{
    try
    {
        using var reader = new StreamReader(context.Request.Body);
        var body = await reader.ReadToEndAsync();

        var payload = JsonSerializer.Deserialize<JobUploadRequest>(body, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        if (payload == null || payload.Job == null)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid payload"
            });
        }

        if (!Guid.TryParse(payload.Job.JobId, out Guid jobId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid Job.JobId"
            });
        }

        if (!Guid.TryParse(payload.TenantId, out Guid tenantId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid TenantId"
            });
        }

        if (!Guid.TryParse(payload.Job.InspectorId, out Guid inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid Job.InspectorId"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureWorkflowActionsTableAsync(conn);

        const string createTableSql = @"
CREATE TABLE IF NOT EXISTS public.jobs_staging
(
    job_id uuid PRIMARY KEY,
    tenant_id uuid NULL,
    inspector_id uuid NOT NULL,
    inspector_name text NULL,
    inspector_email text NULL,
    inspector_phone text NULL,
    source_system text,
    job_name text,
    site_address text,
    age_of_building text,
    job_date timestamptz NULL,
    inspection_duration_minutes integer NULL,
    source_updated_at timestamptz NULL,
    date_added timestamptz NULL,
    status text,
    zap_processed text,
    report_sent text,
    booking_email_sent boolean NOT NULL DEFAULT false,
    booking_email_sent_at timestamptz NULL,
    booking_email_retry_requested boolean NOT NULL DEFAULT false,
    booking_email_retry_requested_at timestamptz NULL,
    booking_email_last_attempt_at timestamptz NULL,
    booking_email_last_error text NULL,
    terms_sent boolean NOT NULL DEFAULT false,
    terms_sent_at timestamptz NULL,
    terms_retry_requested boolean NOT NULL DEFAULT false,
    terms_retry_requested_at timestamptz NULL,
    terms_last_attempt_at timestamptz NULL,
    terms_last_error text NULL,
    terms_signed boolean NOT NULL DEFAULT false,
    terms_signed_at timestamptz NULL,
    signnow_document_id text NULL,
    signnow_invite_id text NULL,
    signnow_template_id text NULL,
    signnow_document_status text NULL,
    signnow_last_checked_at timestamptz NULL,
    signnow_signing_link text NULL,
    invoice_sent boolean NOT NULL DEFAULT false,
    invoice_sent_at timestamptz NULL,
    invoice_retry_requested boolean NOT NULL DEFAULT false,
    invoice_retry_requested_at timestamptz NULL,
    invoice_last_attempt_at timestamptz NULL,
    invoice_last_error text NULL,
    paid boolean NOT NULL DEFAULT false,
    calendar_created boolean NOT NULL DEFAULT false,
    calendar_created_at timestamptz NULL,
    calendar_retry_requested boolean NOT NULL DEFAULT false,
    calendar_retry_requested_at timestamptz NULL,
    calendar_last_attempt_at timestamptz NULL,
    calendar_last_error text NULL,
    primary_service text,
    additional1 text,
    additional2 text,
    primary_service_key text,
    additional1_service_key text,
    additional2_service_key text,
    booking_template_key text NOT NULL DEFAULT 'general_booking',
    booking_email_required boolean NOT NULL DEFAULT true,
    terms_required boolean NOT NULL DEFAULT false,
    invoice_required boolean NOT NULL DEFAULT true,
    calendar_required boolean NOT NULL DEFAULT true,
    report_required boolean NOT NULL DEFAULT true,
    building_type text,
    stories text,
    bedrooms text,
    bathrooms text,
    monolithic text,
    outbuilding text,
    occupied text,
    attached_flat text,
    travel_fee text,
    hhs_bedrooms text,
    meth_samples text,
    hhs_reinspect text,
    council_files text,
    foundation_space text,
    weathertightness text,
    hhs_reinspect_date text,
    access_by text,
    hhs_compliance text,
    notes text,
    directions text,
    instructions text,
    contact1_salutation text,
    contact1_display_name text,
    contact1_role_label text,
    contact1_first_name text,
    contact1_last_name text,
    contact1_email text,
    contact1_cellular text,
    contact2_salutation text,
    contact2_display_name text,
    contact2_role_label text,
    contact2_first_name text,
    contact2_last_name text,
    contact2_email text,
    contact2_cellular text,
    extracted_at_utc text,
    connector_version text,
    source_instance text,
    raw_payload_json text,
    report_workflow_sent boolean NOT NULL DEFAULT false,
    report_workflow_sent_at timestamptz NULL,
    report_retry_requested boolean NOT NULL DEFAULT false,
    report_retry_requested_at timestamptz NULL,
    report_last_attempt_at timestamptz NULL,
    report_last_error text NULL,
    marked_as_paid_override boolean NOT NULL DEFAULT false,
    report_available boolean NOT NULL DEFAULT false,
    job_total decimal(10,2) NULL,
    amount_paid decimal(10,2) NOT NULL DEFAULT 0,
    amount_outstanding decimal(10,2) NULL,
    payment_status text NOT NULL DEFAULT 'unpaid',
    workflow_updated_at timestamptz NOT NULL DEFAULT NOW(),
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW()
);";

        await using (var createCmd = new NpgsqlCommand(createTableSql, conn))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var profileColumns = new NpgsqlCommand(@"
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS inspector_email text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS inspector_phone text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact1_display_name text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact1_role_label text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact2_display_name text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact2_role_label text NULL;", conn))
        {
            await profileColumns.ExecuteNonQueryAsync();
        }

        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureOnlinePropertyTablesAsync(conn);
        await JobChangeSupport.EnsureAsync(conn);
        var changePreparation = await JobChangeSupport.PrepareAsync(conn, jobId, payload);

        string previousAddress = "";
        bool workflowStartedBeforeAddressChange = false;
        await using (var previousCmd = new NpgsqlCommand(@"SELECT site_address,
(booking_email_sent OR terms_sent OR calendar_created OR invoice_sent) AS workflow_started
FROM public.jobs_staging WHERE job_id=@job_id LIMIT 1", conn))
        {
            previousCmd.Parameters.AddWithValue("job_id", jobId);
            await using var previousReader = await previousCmd.ExecuteReaderAsync();
            if (await previousReader.ReadAsync())
            {
                previousAddress = previousReader["site_address"]?.ToString() ?? "";
                workflowStartedBeforeAddressChange = previousReader["workflow_started"] != DBNull.Value && (bool)previousReader["workflow_started"];
            }
        }
        var incomingAddress = payload.Job.SiteAddress ?? "";
        var addressChanged = previousAddress.Length > 0 && StructuredAddressResolver.Fingerprint(previousAddress) != StructuredAddressResolver.Fingerprint(incomingAddress);

        const string upsertSql = @"
INSERT INTO public.jobs_staging
(
    tenant_id,
    job_id,
    inspector_id,
    inspector_name,
    inspector_email,
    inspector_phone,
    source_system,
    job_name,
    site_address,
    age_of_building,
    job_date,
    inspection_duration_minutes,
    source_updated_at,
    date_added,
    status,
    zap_processed,
    report_sent,
    job_total,
    primary_service,
    additional1,
    additional2,
    primary_service_key,
    additional1_service_key,
    additional2_service_key,
    booking_template_key,
    booking_email_required,
    terms_required,
    invoice_required,
    calendar_required,
    report_required,
    building_type,
    stories,
    bedrooms,
    bathrooms,
    monolithic,
    outbuilding,
    occupied,
    attached_flat,
    travel_fee,
    hhs_bedrooms,
    meth_samples,
    hhs_reinspect,
    council_files,
    foundation_space,
    weathertightness,
    hhs_reinspect_date,
    access_by,
    hhs_compliance,
    notes,
    directions,
    instructions,
    contact1_salutation,
    contact1_display_name,
    contact1_role_label,
    contact1_first_name,
    contact1_last_name,
    contact1_email,
    contact1_cellular,
    contact2_salutation,
    contact2_display_name,
    contact2_role_label,
    contact2_first_name,
    contact2_last_name,
    contact2_email,
    contact2_cellular,
    extracted_at_utc,
    connector_version,
    source_instance,
    raw_payload_json,
    updated_at
)
VALUES
(
    @tenant_id,
    @job_id,
    @inspector_id,
    @inspector_name,
    @inspector_email,
    @inspector_phone,
    @source_system,
    @job_name,
    @site_address,
    @age_of_building,
    @job_date,
    @inspection_duration_minutes,
    @source_updated_at,
    @date_added,
    @status,
    @zap_processed,
    @report_sent,
    @job_total,
    @primary_service,
    @additional1,
    @additional2,
    @primary_service_key,
    @additional1_service_key,
    @additional2_service_key,
    @booking_template_key,
    @booking_email_required,
    @terms_required,
    @invoice_required,
    @calendar_required,
    @report_required,
    @building_type,
    @stories,
    @bedrooms,
    @bathrooms,
    @monolithic,
    @outbuilding,
    @occupied,
    @attached_flat,
    @travel_fee,
    @hhs_bedrooms,
    @meth_samples,
    @hhs_reinspect,
    @council_files,
    @foundation_space,
    @weathertightness,
    @hhs_reinspect_date,
    @access_by,
    @hhs_compliance,
    @notes,
    @directions,
    @instructions,
    @contact1_salutation,
    @contact1_display_name,
    @contact1_role_label,
    @contact1_first_name,
    @contact1_last_name,
    @contact1_email,
    @contact1_cellular,
    @contact2_salutation,
    @contact2_display_name,
    @contact2_role_label,
    @contact2_first_name,
    @contact2_last_name,
    @contact2_email,
    @contact2_cellular,
    @extracted_at_utc,
    @connector_version,
    @source_instance,
    @raw_payload_json,
    NOW()
)
ON CONFLICT (job_id)
DO UPDATE SET
    tenant_id                    = EXCLUDED.tenant_id,
    inspector_id                 = EXCLUDED.inspector_id,
    inspector_name               = EXCLUDED.inspector_name,
    inspector_email              = EXCLUDED.inspector_email,
    inspector_phone              = EXCLUDED.inspector_phone,
    source_system                = EXCLUDED.source_system,
    job_name                     = EXCLUDED.job_name,
    site_address                 = EXCLUDED.site_address,
    age_of_building              = EXCLUDED.age_of_building,
    job_date                     = EXCLUDED.job_date,
    inspection_duration_minutes  = EXCLUDED.inspection_duration_minutes,
    source_updated_at            = EXCLUDED.source_updated_at,
    date_added                   = EXCLUDED.date_added,
    status                       = EXCLUDED.status,
    zap_processed                = EXCLUDED.zap_processed,
    report_sent                  = EXCLUDED.report_sent,
    job_total                    = EXCLUDED.job_total,
    primary_service              = EXCLUDED.primary_service,
    additional1                  = EXCLUDED.additional1,
    additional2                  = EXCLUDED.additional2,
    primary_service_key          = EXCLUDED.primary_service_key,
    additional1_service_key      = EXCLUDED.additional1_service_key,
    additional2_service_key      = EXCLUDED.additional2_service_key,
    booking_template_key         = EXCLUDED.booking_template_key,
    booking_email_required       = EXCLUDED.booking_email_required,
    terms_required               = EXCLUDED.terms_required,
    invoice_required             = EXCLUDED.invoice_required,
    calendar_required            = EXCLUDED.calendar_required,
    report_required              = EXCLUDED.report_required,
    building_type                = EXCLUDED.building_type,
    stories                      = EXCLUDED.stories,
    bedrooms                     = EXCLUDED.bedrooms,
    bathrooms                    = EXCLUDED.bathrooms,
    monolithic                   = EXCLUDED.monolithic,
    outbuilding                  = EXCLUDED.outbuilding,
    occupied                     = EXCLUDED.occupied,
    attached_flat                = EXCLUDED.attached_flat,
    travel_fee                   = EXCLUDED.travel_fee,
    hhs_bedrooms                 = EXCLUDED.hhs_bedrooms,
    meth_samples                 = EXCLUDED.meth_samples,
    hhs_reinspect                = EXCLUDED.hhs_reinspect,
    council_files                = EXCLUDED.council_files,
    foundation_space             = EXCLUDED.foundation_space,
    weathertightness             = EXCLUDED.weathertightness,
    hhs_reinspect_date           = EXCLUDED.hhs_reinspect_date,
    access_by                    = EXCLUDED.access_by,
    hhs_compliance               = EXCLUDED.hhs_compliance,
    notes                        = EXCLUDED.notes,
    directions                   = EXCLUDED.directions,
    instructions                 = EXCLUDED.instructions,
    contact1_salutation          = EXCLUDED.contact1_salutation,
    contact1_display_name        = EXCLUDED.contact1_display_name,
    contact1_role_label          = EXCLUDED.contact1_role_label,
    contact1_first_name          = EXCLUDED.contact1_first_name,
    contact1_last_name           = EXCLUDED.contact1_last_name,
    contact1_email               = EXCLUDED.contact1_email,
    contact1_cellular            = EXCLUDED.contact1_cellular,
    contact2_salutation          = EXCLUDED.contact2_salutation,
    contact2_display_name        = EXCLUDED.contact2_display_name,
    contact2_role_label          = EXCLUDED.contact2_role_label,
    contact2_first_name          = EXCLUDED.contact2_first_name,
    contact2_last_name           = EXCLUDED.contact2_last_name,
    contact2_email               = EXCLUDED.contact2_email,
    contact2_cellular            = EXCLUDED.contact2_cellular,
    extracted_at_utc             = EXCLUDED.extracted_at_utc,
    connector_version            = EXCLUDED.connector_version,
    source_instance              = EXCLUDED.source_instance,
    raw_payload_json             = EXCLUDED.raw_payload_json,
    updated_at                   = NOW();";

        await using (var cmd = new NpgsqlCommand(upsertSql, conn))
        {
            cmd.Parameters.AddWithValue("tenant_id", tenantId);
            cmd.Parameters.AddWithValue("job_id", jobId);
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            cmd.Parameters.AddWithValue("inspector_name", payload.Job.InspectorName ?? "");
            cmd.Parameters.AddWithValue("inspector_email", payload.Job.InspectorEmail ?? "");
            cmd.Parameters.AddWithValue("inspector_phone", payload.Job.InspectorPhone ?? "");
            cmd.Parameters.AddWithValue("source_system", payload.SourceSystem ?? "");
            cmd.Parameters.AddWithValue("job_name", payload.Job.JobName ?? "");
            cmd.Parameters.AddWithValue("site_address", payload.Job.SiteAddress ?? "");
            cmd.Parameters.AddWithValue("age_of_building", payload.Job.GetAgeOfBuilding());

            var jobDate = ParseNullableDateTime(payload.Job.JobDate);
            var sourceUpdatedAt = ParseNullableDateTime(payload.Job.SourceUpdatedAtUtc);
            var dateAdded = ParseNullableDateTime(payload.Job.DateAddedUtc);
            var invoiceTotal = ParseNullableDecimal(payload.Job.InvoiceTotal);

            cmd.Parameters.AddWithValue("job_date", jobDate.HasValue ? jobDate.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("inspection_duration_minutes", payload.Job.InspectionDurationMinutes);
            cmd.Parameters.AddWithValue("source_updated_at", sourceUpdatedAt.HasValue ? sourceUpdatedAt.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("date_added", dateAdded.HasValue ? dateAdded.Value : (object)DBNull.Value);

            cmd.Parameters.AddWithValue("status", payload.Job.Status ?? "");
            cmd.Parameters.AddWithValue("zap_processed", payload.Job.ZapProcessed ?? "");
            cmd.Parameters.AddWithValue("report_sent", payload.Job.ReportSent ?? "");
            cmd.Parameters.AddWithValue("job_total", invoiceTotal.HasValue ? invoiceTotal.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("primary_service", payload.Services?.Primary ?? "");
            cmd.Parameters.AddWithValue("additional1", payload.Services?.Additional1 ?? "");
            cmd.Parameters.AddWithValue("additional2", payload.Services?.Additional2 ?? "");
            cmd.Parameters.AddWithValue("primary_service_key", payload.Services?.PrimaryServiceKey ?? InferCanonicalServiceType(payload.Services?.Primary));
            cmd.Parameters.AddWithValue("additional1_service_key", payload.Services?.Additional1ServiceKey ?? InferCanonicalServiceType(payload.Services?.Additional1));
            cmd.Parameters.AddWithValue("additional2_service_key", payload.Services?.Additional2ServiceKey ?? InferCanonicalServiceType(payload.Services?.Additional2));
            cmd.Parameters.AddWithValue("booking_template_key", BuildBookingTemplateKey(payload.Services));
            cmd.Parameters.AddWithValue("booking_email_required", payload.Services?.BookingEmailRequired ?? true);
            cmd.Parameters.AddWithValue("terms_required", payload.Services?.TermsRequired ?? ShouldRequireTermsForBooking(payload.Services));
            cmd.Parameters.AddWithValue("invoice_required", payload.Services?.InvoiceRequired ?? true);
            cmd.Parameters.AddWithValue("calendar_required", payload.Services?.CalendarRequired ?? true);
            cmd.Parameters.AddWithValue("report_required", payload.Services?.ReportRequired ?? true);
            cmd.Parameters.AddWithValue("building_type", payload.JobDetails?.BuildingType ?? "");
            cmd.Parameters.AddWithValue("stories", payload.JobDetails?.Stories ?? "");
            cmd.Parameters.AddWithValue("bedrooms", payload.JobDetails?.Bedrooms ?? "");
            cmd.Parameters.AddWithValue("bathrooms", payload.JobDetails?.Bathrooms ?? "");
            cmd.Parameters.AddWithValue("monolithic", payload.JobDetails?.Monolithic ?? "");
            cmd.Parameters.AddWithValue("outbuilding", payload.JobDetails?.Outbuilding ?? "");
            cmd.Parameters.AddWithValue("occupied", payload.JobDetails?.Occupied ?? "");
            cmd.Parameters.AddWithValue("attached_flat", payload.JobDetails?.AttachedFlat ?? "");
            cmd.Parameters.AddWithValue("travel_fee", payload.JobDetails?.TravelFee ?? "");
            cmd.Parameters.AddWithValue("hhs_bedrooms", payload.JobDetails?.HhsBedrooms ?? "");
            cmd.Parameters.AddWithValue("meth_samples", payload.JobDetails?.MethSamples ?? "");
            cmd.Parameters.AddWithValue("hhs_reinspect", payload.JobDetails?.HhsReinspect ?? "");
            cmd.Parameters.AddWithValue("council_files", payload.JobDetails?.CouncilFiles ?? "");
            cmd.Parameters.AddWithValue("foundation_space", payload.JobDetails?.FoundationSpace ?? "");
            cmd.Parameters.AddWithValue("weathertightness", payload.JobDetails?.Weathertightness ?? "");
            cmd.Parameters.AddWithValue("hhs_reinspect_date", payload.JobDetails?.HhsReinspectDate ?? "");
            cmd.Parameters.AddWithValue("access_by", payload.JobDetails?.AccessBy ?? "");
            cmd.Parameters.AddWithValue("hhs_compliance", payload.JobDetails?.HhsCompliance ?? "");
            cmd.Parameters.AddWithValue("notes", payload.Job.Notes ?? "");
            cmd.Parameters.AddWithValue("directions", payload.Job.Directions ?? "");
            cmd.Parameters.AddWithValue("instructions", payload.Job.Instructions ?? "");
            cmd.Parameters.AddWithValue("contact1_salutation", payload.Contact1?.Salutation ?? "");
            cmd.Parameters.AddWithValue("contact1_display_name", payload.Contact1?.DisplayName ?? "");
            cmd.Parameters.AddWithValue("contact1_role_label", payload.Contact1?.RoleLabel ?? "");
            cmd.Parameters.AddWithValue("contact1_first_name", payload.Contact1?.FirstName ?? "");
            cmd.Parameters.AddWithValue("contact1_last_name", payload.Contact1?.LastName ?? "");
            cmd.Parameters.AddWithValue("contact1_email", payload.Contact1?.Email ?? "");
            cmd.Parameters.AddWithValue("contact1_cellular", payload.Contact1?.Cellular ?? "");
            cmd.Parameters.AddWithValue("contact2_salutation", payload.Contact2?.Salutation ?? "");
            cmd.Parameters.AddWithValue("contact2_display_name", payload.Contact2?.DisplayName ?? "");
            cmd.Parameters.AddWithValue("contact2_role_label", payload.Contact2?.RoleLabel ?? "");
            cmd.Parameters.AddWithValue("contact2_first_name", payload.Contact2?.FirstName ?? "");
            cmd.Parameters.AddWithValue("contact2_last_name", payload.Contact2?.LastName ?? "");
            cmd.Parameters.AddWithValue("contact2_email", payload.Contact2?.Email ?? "");
            cmd.Parameters.AddWithValue("contact2_cellular", payload.Contact2?.Cellular ?? "");
            cmd.Parameters.AddWithValue("extracted_at_utc", payload.Meta?.ExtractedAtUtc ?? "");
            cmd.Parameters.AddWithValue("connector_version", payload.Meta?.ConnectorVersion ?? "");
            cmd.Parameters.AddWithValue("source_instance", payload.Meta?.SourceInstance ?? "");
            cmd.Parameters.AddWithValue("raw_payload_json", body);

            await cmd.ExecuteNonQueryAsync();
        }

        if (addressChanged)
        {
            await using var addressCmd = new NpgsqlCommand(@"UPDATE public.jobs_staging SET
previous_site_address=@previous_address,address_change_pending=@pending,address_change_detected_at=NOW(),address_change_confirmed_at=NULL,address_change_confirmed_by=NULL,
property_features_status='stale',property_features_error='Address changed in 3D; refresh required.',
branz_lookup_status='stale',branz_lookup_error='Address changed in 3D; refresh required.'
WHERE job_id=@job_id", conn);
            addressCmd.Parameters.AddWithValue("job_id", jobId); addressCmd.Parameters.AddWithValue("previous_address", previousAddress); addressCmd.Parameters.AddWithValue("pending", workflowStartedBeforeAddressChange); await addressCmd.ExecuteNonQueryAsync();
        }

        var currentFingerprint = StructuredAddressResolver.Fingerprint(incomingAddress);
        var needsFeatures = addressChanged;
        var needsBranz = addressChanged;
        await using (var needsCmd = new NpgsqlCommand("SELECT property_features_address_fingerprint,branz_address_fingerprint,property_features_status,branz_lookup_status FROM public.jobs_staging WHERE job_id=@job_id", conn))
        {
            needsCmd.Parameters.AddWithValue("job_id", jobId); await using var needsReader = await needsCmd.ExecuteReaderAsync();
            if (await needsReader.ReadAsync())
            {
                needsFeatures = needsFeatures || !string.Equals(needsReader[0]?.ToString(), currentFingerprint, StringComparison.OrdinalIgnoreCase);
                needsBranz = needsBranz || !string.Equals(needsReader[1]?.ToString(), currentFingerprint, StringComparison.OrdinalIgnoreCase);
                needsFeatures = needsFeatures || !string.Equals(needsReader[2]?.ToString(), "available", StringComparison.OrdinalIgnoreCase);
                needsBranz = needsBranz || !string.Equals(needsReader[3]?.ToString(), "available", StringComparison.OrdinalIgnoreCase);
            }
        }
        if ((needsFeatures || needsBranz) && await HasOnlinePropertyEntitlementAsync(conn, tenantId, inspectorId))
        {
            var allowance = await RegisterOnlinePropertyAddressAsync(conn, jobId, tenantId, currentFingerprint, incomingAddress);
            if (allowance.Allowed)
            {
                if (needsFeatures) needsFeatures = (await GetOnlinePropertyFailureRetryGateAsync(conn, jobId, "property-features", currentFingerprint)).Allowed;
                if (needsBranz) needsBranz = (await GetOnlinePropertyFailureRetryGateAsync(conn, jobId, "branz", currentFingerprint)).Allowed;
                var featuresTask = needsFeatures ? PropertyFeaturesLookupService.LookupAsync(incomingAddress) : null;
                var branzTask = needsBranz ? BranzLookupService.LookupAsync(incomingAddress) : null;
                var pendingLookups = new List<Task>();
                if (featuresTask != null) pendingLookups.Add(featuresTask);
                if (branzTask != null) pendingLookups.Add(branzTask);
                await Task.WhenAll(pendingLookups);
                if (featuresTask != null) { var result = await featuresTask; await StorePropertyFeaturesResultAsync(conn, jobId, result); await AuditOnlinePropertyLookupAsync(conn, jobId, tenantId, "property-features", currentFingerprint, addressChanged ? "address_change" : "initial_sync", result.Status, result.Error); }
                if (branzTask != null) { var result = await branzTask; await StoreBranzResultAsync(conn, jobId, result); await AuditOnlinePropertyLookupAsync(conn, jobId, tenantId, "branz", currentFingerprint, addressChanged ? "address_change" : "initial_sync", result.Status, result.Error); }
            }
        }

        await RefreshBookingWorkflowActionsAsync(conn, payload, jobId, tenantId, inspectorId);
        await RefreshJobInvoiceLinesAsync(conn, payload, jobId);
        await JobChangeSupport.ApplyAsync(conn, jobId, tenantId, changePreparation);

        return Results.Ok(new
        {
            success = true,
            message = "Job staged successfully",
            jobId = payload.Job.JobId,
            tenantId = payload.TenantId,
            inspectorId = payload.Job.InspectorId,
            changeReviewPending = changePreparation.Pending,
            changeReasons = changePreparation.Reasons,
            currentSnapshotFingerprint = changePreparation.Fingerprint
        });
    }
    catch (PostgresException pgEx)
    {
        return Results.Problem(
            title: "Database error",
            detail: pgEx.ToString(),
            statusCode: 500
        );
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Server error",
            detail: ex.ToString(),
            statusCode: 500
        );
    }
});

// =============================
// ADVANCED ACTIONS
// Review-first rule definitions and side-effect-free preview evaluation.
// =============================
app.MapGet("/automation/foundation", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn);
        await EnsureAdvancedActionsTablesAsync(conn);
        await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId);
        if (!owner.Allowed) return owner.Error!;
        var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(conn, tenantId);
        var mode = await AutomationFoundationSupport.GetActivationModeAsync(conn, tenantId);
        return Results.Ok(new
        {
            success = true,
            tenantId,
            entitlement.Status,
            entitlement.PlanName,
            capabilities = new
            {
                basicAutomation = entitlement.BasicAutomation,
                advancedWorkflows = entitlement.AdvancedWorkflows,
                outgoingWebhooks = entitlement.OutgoingWebhooks
            },
            activationMode = mode,
            basicExecutionActive = mode != "all_jobs",
            customerFacingExecutionEnabled = false
        });
    }
    catch (Exception ex) { return Results.Problem(title: "Load automation foundation failed", detail: ex.Message, statusCode: 500); }
});

app.MapPut("/automation/foundation/activation", async (HttpContext context, AutomationActivationRequest request) =>
{
    var mode = NormalizeAutomationKey(request.ActivationMode);
    if (request.TenantId == Guid.Empty || (mode != "selected_jobs" && mode != "all_jobs"))
        return Results.BadRequest(new { success = false, message = "TenantId and a valid activation mode are required." });
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(conn, request.TenantId);
        if (!entitlement.AdvancedWorkflows) return Results.Json(new { success = false, message = "Advanced Workflows are not available for this company." }, statusCode: 403);
        var previous = await AutomationFoundationSupport.GetActivationModeAsync(conn, request.TenantId);
        const string sql = @"INSERT INTO public.automation_tenant_settings(tenant_id,activation_mode,updated_by) VALUES(@tenant,@mode,@by)
ON CONFLICT(tenant_id) DO UPDATE SET activation_mode=EXCLUDED.activation_mode,updated_by=EXCLUDED.updated_by,updated_at=NOW();";
        await using (var cmd = new NpgsqlCommand(sql, conn)) { cmd.Parameters.AddWithValue("tenant", request.TenantId); cmd.Parameters.AddWithValue("mode", mode); cmd.Parameters.AddWithValue("by", request.ChangedBy ?? "Connector user"); await cmd.ExecuteNonQueryAsync(); }
        if (previous != mode) await AutomationFoundationSupport.AuditAsync(conn, request.TenantId, null, "activation_mode_changed", previous, mode, request.ChangedBy ?? "Connector user");
        return Results.Ok(new { success = true, activationMode = mode, basicExecutionActive = mode != "all_jobs", customerFacingExecutionEnabled = false });
    }
    catch (Exception ex) { return Results.Problem(title: "Save automation activation failed", detail: ex.Message, statusCode: 500); }
});

app.MapPut("/automation/jobs/{jobId}/selection", async (HttpContext context, Guid jobId, AutomationJobSelectionRequest request) =>
{
    if (request.TenantId == Guid.Empty) return Results.BadRequest(new { success = false, message = "TenantId is required." });
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId); if (!owner.Allowed) return owner.Error!;
        if (!await AutomationFoundationSupport.JobBelongsToTenantAsync(conn, request.TenantId, jobId)) return Results.NotFound(new { success = false, message = "Job was not found for this company." });
        var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(conn, request.TenantId);
        if (!entitlement.AdvancedWorkflows) return Results.Json(new { success = false, message = "Advanced Workflows are not available for this company." }, statusCode: 403);
        var mode = await AutomationFoundationSupport.GetActivationModeAsync(conn, request.TenantId);
        if (mode == "all_jobs") return Results.BadRequest(new { success = false, message = "All Jobs mode already uses Advanced Workflows for every job." });
        var previous = await AutomationFoundationSupport.JobUsesAdvancedAsync(conn, request.TenantId, jobId, mode);
        const string sql = @"INSERT INTO public.automation_job_selections(tenant_id,job_id,use_advanced_workflows,updated_by) VALUES(@tenant,@job,@enabled,@by)
ON CONFLICT(tenant_id,job_id) DO UPDATE SET use_advanced_workflows=EXCLUDED.use_advanced_workflows,updated_by=EXCLUDED.updated_by,updated_at=NOW();";
        await using (var cmd = new NpgsqlCommand(sql, conn)) { cmd.Parameters.AddWithValue("tenant", request.TenantId); cmd.Parameters.AddWithValue("job", jobId); cmd.Parameters.AddWithValue("enabled", request.UseAdvancedWorkflows); cmd.Parameters.AddWithValue("by", request.ChangedBy ?? "Connector user"); await cmd.ExecuteNonQueryAsync(); }
        if (previous != request.UseAdvancedWorkflows) await AutomationFoundationSupport.AuditAsync(conn, request.TenantId, jobId, "job_workflow_engine_changed", previous ? "advanced" : "basic", request.UseAdvancedWorkflows ? "advanced" : "basic", request.ChangedBy ?? "Connector user");
        return Results.Ok(new { success = true, jobId, useAdvancedWorkflows = request.UseAdvancedWorkflows, basicAutomationApplies = !request.UseAdvancedWorkflows, customerFacingExecutionEnabled = false });
    }
    catch (Exception ex) { return Results.Problem(title: "Save job workflow selection failed", detail: ex.Message, statusCode: 500); }
});

app.MapGet("/automation/jobs/{jobId}/selection", async (HttpContext context, Guid jobId, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        if (!await AutomationFoundationSupport.JobBelongsToTenantAsync(conn, tenantId, jobId)) return Results.NotFound(new { success = false, message = "Job was not found for this company." });
        var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(conn, tenantId); var mode = await AutomationFoundationSupport.GetActivationModeAsync(conn, tenantId);
        var advanced = entitlement.AdvancedWorkflows && await AutomationFoundationSupport.JobUsesAdvancedAsync(conn, tenantId, jobId, mode);
        return Results.Ok(new { success = true, jobId, activationMode = mode, advancedEntitled = entitlement.AdvancedWorkflows, useAdvancedWorkflows = advanced, basicAutomationApplies = !advanced, customerFacingExecutionEnabled = false });
    }
    catch (Exception ex) { return Results.Problem(title: "Load job workflow selection failed", detail: ex.Message, statusCode: 500); }
});

app.MapGet("/automation/health", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync();
        await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        return Results.Ok(new { success = true, database = "available", rules = "available", templates = "available", activation = "available" });
    }
    catch (Exception ex) { return Results.Problem(title: "Automation health check failed", detail: ex.Message, statusCode: 500); }
});

app.MapGet("/automation/service-catalog", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await EnsureMappingTablesAsync(conn); await EnsureEmailTemplatesTableAsync(conn); await EnsureAdvancedActionsTablesAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        const string sql = @"SELECT DISTINCT ON (c.catalog_item_key)
c.catalog_item_key,c.list_item_id,c.list_item_name,c.list_name,c.invoice_item_id,c.invoice_item_name,c.is_active,c.canonical_service_type,c.booking_template_key,c.last_synced_at
FROM public.inspector_service_catalog c LEFT JOIN public.inspectors i ON i.inspector_id=c.inspector_id
WHERE i.tenant_id=@tenant
   OR EXISTS(SELECT 1 FROM public.jobs_staging j WHERE j.tenant_id::text=@tenant_text AND j.inspector_id=c.inspector_id)
ORDER BY c.catalog_item_key,c.last_synced_at DESC;";
        await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("tenant", tenantId); cmd.Parameters.Add("tenant_text", NpgsqlTypes.NpgsqlDbType.Text).Value=tenantId.ToString(); await using var reader = await cmd.ExecuteReaderAsync(); var serviceCatalog = new List<object>();
        while (await reader.ReadAsync()) serviceCatalog.Add(new { catalog_item_key=reader["catalog_item_key"]?.ToString(),list_item_id=reader["list_item_id"]?.ToString(),list_item_name=reader["list_item_name"]?.ToString(),list_name=reader["list_name"]?.ToString(),invoice_item_id=reader["invoice_item_id"]?.ToString(),invoice_item_name=reader["invoice_item_name"]?.ToString(),is_active=reader["is_active"]?.ToString(),canonical_service_type=reader["canonical_service_type"]?.ToString(),booking_template_key=reader["booking_template_key"]?.ToString(),last_synced_at=reader["last_synced_at"]?.ToString() });
        return Results.Ok(new { success=true, tenantId, service_catalog=serviceCatalog });
    }
    catch(Exception ex){return Results.Problem(title:"Load tenant service catalogue failed",detail:ex.Message,statusCode:500);}
});

app.MapPost("/actions/ensure-tables", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureAdvancedActionsTablesAsync(conn);
        return Results.Ok(new { success = true, message = "Advanced Actions tables are ready." });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Ensure Advanced Actions tables failed", detail: ex.Message, statusCode: 500);
    }
});

app.MapGet("/actions/catalog", () => Results.Ok(new
{
    events = new[] { "inspection_scheduled", "inspection_rescheduled", "pre_inspection_due", "inspection_cancelled", "price_changed", "service_changed" },
    fields = new[] { "lifecycle", "status", "primary_service", "all_services", "site_address", "client_name", "inspector_name", "invoice_total", "change_categories" },
    operators = new[] { "includes", "does_not_include" },
    actions = new[] { "send_email", "send_webhook", "upsert_calendar", "create_xero_draft", "send_signnow_agreement", "queue_report_communication", "set_workflow_state" },
    modes = new[] { "disabled", "review" }
}));

app.MapGet("/actions/rules", async (HttpContext context, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureAdvancedActionsTablesAsync(conn);
        await EnsureEmailTemplatesTableAsync(conn);
        await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId);
        if (!owner.Allowed) return owner.Error!;
        const string sql = @"
SELECT rule_id, tenant_id, name, event_key, mode, enabled, conditions_json, actions_json, created_at, updated_at
FROM public.automation_rules
WHERE tenant_id = @tenantId
ORDER BY updated_at DESC, name;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await cmd.ExecuteReaderAsync();
        var rules = new List<object>();
        while (await reader.ReadAsync())
        {
            var loadWarning = ""; List<AutomationCondition> loadedConditions; List<AutomationActionDefinition> loadedActions;
            try { loadedConditions = JsonSerializer.Deserialize<List<AutomationCondition>>(reader.GetString(6)) ?? new(); }
            catch (Exception ex) { loadedConditions = new(); loadWarning = "Conditions could not be read: " + ex.Message; }
            try { loadedActions = JsonSerializer.Deserialize<List<AutomationActionDefinition>>(reader.GetString(7)) ?? new(); }
            catch (Exception ex) { loadedActions = new(); loadWarning = (loadWarning + " Actions could not be read: " + ex.Message).Trim(); }
            foreach (var loadedAction in loadedActions)
                if (NormalizeAutomationKey(loadedAction.ActionKey) == "send_webhook" && loadedAction.Settings.TryGetValue("headers", out var protectedHeaders))
                    loadedAction.Settings["headers"] = AutomationSecretProtector.Unprotect(protectedHeaders, builder.Configuration["AUTOMATE_AUTOMATION_SECRET_KEY"]);
            rules.Add(new
            {
                ruleId = reader.GetGuid(0), tenantId = reader.GetGuid(1), name = reader.GetString(2),
                eventKey = reader.GetString(3), mode = reader.GetString(4), enabled = reader.GetBoolean(5),
                conditions = loadedConditions,
                actions = loadedActions,
                createdAt = reader.GetDateTime(8), updatedAt = reader.GetDateTime(9), loadWarning
            });
        }
        return Results.Ok(rules);
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Load Advanced Actions rules failed", detail: ex.Message, statusCode: 500);
    }
});

app.MapPost("/actions/rules", async (HttpContext context, AutomationRuleSaveRequest request) =>
{
    var validation = ValidateAutomationRule(request);
    if (validation.Count > 0)
        return Results.BadRequest(new { success = false, errors = validation });

    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureAdvancedActionsTablesAsync(conn);
        await EnsureEmailTemplatesTableAsync(conn);
        await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, request.TenantId);
        if (!owner.Allowed) return owner.Error!;
        var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(conn, request.TenantId);
        if (!entitlement.AdvancedWorkflows)
            return Results.Json(new { success = false, message = "Advanced Workflows are not available for this company." }, statusCode: 403);
        var ruleId = request.RuleId == Guid.Empty ? Guid.NewGuid() : request.RuleId;
        foreach (var action in request.Actions)
            if (NormalizeAutomationKey(action.ActionKey) == "send_webhook" && action.Settings.TryGetValue("headers", out var webhookHeaders) && !string.IsNullOrWhiteSpace(webhookHeaders))
                action.Settings["headers"] = AutomationSecretProtector.Protect(webhookHeaders, builder.Configuration["AUTOMATE_AUTOMATION_SECRET_KEY"]);
        const string sql = @"
INSERT INTO public.automation_rules
    (rule_id, tenant_id, name, event_key, mode, enabled, conditions_json, actions_json, created_at, updated_at)
VALUES
    (@ruleId, @tenantId, @name, @eventKey, @mode, @enabled, CAST(@conditions AS jsonb), CAST(@actions AS jsonb), NOW(), NOW())
ON CONFLICT (rule_id) DO UPDATE SET
    name = EXCLUDED.name, event_key = EXCLUDED.event_key, mode = EXCLUDED.mode,
    enabled = EXCLUDED.enabled, conditions_json = EXCLUDED.conditions_json,
    actions_json = EXCLUDED.actions_json, updated_at = NOW()
WHERE public.automation_rules.tenant_id = EXCLUDED.tenant_id
RETURNING rule_id, updated_at;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("ruleId", ruleId);
        cmd.Parameters.AddWithValue("tenantId", request.TenantId);
        cmd.Parameters.AddWithValue("name", request.Name.Trim());
        cmd.Parameters.AddWithValue("eventKey", NormalizeAutomationKey(request.EventKey));
        cmd.Parameters.AddWithValue("mode", request.Enabled ? "review" : "disabled");
        cmd.Parameters.AddWithValue("enabled", request.Enabled);
        cmd.Parameters.AddWithValue("conditions", JsonSerializer.Serialize(request.Conditions));
        cmd.Parameters.AddWithValue("actions", JsonSerializer.Serialize(request.Actions));
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return Results.NotFound(new { success = false, message = "Rule was not found for this tenant." });
        return Results.Ok(new { success = true, ruleId = reader.GetGuid(0), updatedAt = reader.GetDateTime(1), mode = request.Enabled ? "review" : "disabled" });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Save Advanced Actions rule failed", detail: ex.Message, statusCode: 500);
    }
});

app.MapDelete("/actions/rules/{ruleId}", async (HttpContext context, Guid ruleId, Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString); await conn.OpenAsync(); await EnsureAdvancedActionsTablesAsync(conn); await EnsureEmailTemplatesTableAsync(conn); await AutomationFoundationSupport.EnsureAsync(conn);
        var owner = await RequireAutomationOwnerAsync(context, conn, tenantId); if (!owner.Allowed) return owner.Error!;
        await using var cmd = new NpgsqlCommand("DELETE FROM public.automation_rules WHERE rule_id=@rule AND tenant_id=@tenant", conn);
        cmd.Parameters.AddWithValue("rule", ruleId); cmd.Parameters.AddWithValue("tenant", tenantId);
        int rows = await cmd.ExecuteNonQueryAsync();
        return rows == 0 ? Results.NotFound(new { success = false, message = "Workflow action was not found for this company." }) : Results.Ok(new { success = true });
    }
    catch (Exception ex) { return Results.Problem(title: "Delete workflow action failed", detail: ex.Message, statusCode: 500); }
});

app.MapPost("/actions/rules/preview", (AutomationRulePreviewRequest request) =>
{
    var validation = ValidateAutomationRule(request.Rule);
    if (validation.Count > 0)
        return Results.BadRequest(new { success = false, errors = validation });

    var evaluations = request.Rule.Conditions.Select(condition =>
    {
        request.Fields.TryGetValue(condition.FieldKey ?? "", out var actual);
        actual ??= "";
        var expected = condition.Value ?? "";
        bool contains = actual.IndexOf(expected, StringComparison.OrdinalIgnoreCase) >= 0;
        bool matched = NormalizeAutomationKey(condition.Operator) == "includes" ? contains : !contains;
        return new { fieldKey = condition.FieldKey, condition.Operator, expected, actual, matched };
    }).ToList();
    bool ruleMatched = evaluations.All(item => item.matched);
    return Results.Ok(new
    {
        success = true,
        matched = ruleMatched,
        sideEffectsExecuted = false,
        conditions = evaluations,
        proposedActions = ruleMatched ? request.Rule.Actions : new List<AutomationActionDefinition>(),
        message = ruleMatched ? "Rule matched. Actions require review before execution." : "Rule did not match this job."
    });
});

app.MapPost("/actions/events", async (AutomationEventRequest request) =>
{
    var eventKey = NormalizeAutomationKey(request.EventKey);
    var validEvents = new HashSet<string> { "inspection_scheduled", "inspection_rescheduled", "pre_inspection_due", "inspection_cancelled", "price_changed", "service_changed" };
    if (request.TenantId == Guid.Empty || !validEvents.Contains(eventKey) || string.IsNullOrWhiteSpace(request.IdempotencyKey))
        return Results.BadRequest(new { success = false, message = "TenantId, a supported EventKey, and IdempotencyKey are required." });
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureAdvancedActionsTablesAsync(conn);
        const string loadSql = @"
SELECT rule_id, name, conditions_json, actions_json
FROM public.automation_rules
WHERE tenant_id = @tenantId AND event_key = @eventKey AND enabled = true AND mode = 'review';";
        await using var load = new NpgsqlCommand(loadSql, conn);
        load.Parameters.AddWithValue("tenantId", request.TenantId);
        load.Parameters.AddWithValue("eventKey", eventKey);
        await using var reader = await load.ExecuteReaderAsync();
        var matches = new List<AutomationRuleMatch>();
        while (await reader.ReadAsync())
        {
            var conditions = JsonSerializer.Deserialize<List<AutomationCondition>>(reader.GetString(2)) ?? new();
            var actions = JsonSerializer.Deserialize<List<AutomationActionDefinition>>(reader.GetString(3)) ?? new();
            var evaluations = EvaluateAutomationConditions(conditions, request.Fields);
            if (evaluations.All(item => item.Matched))
                matches.Add(new AutomationRuleMatch(reader.GetGuid(0), reader.GetString(1), evaluations, actions));
        }
        await reader.CloseAsync();

        int queued = 0;
        foreach (var match in matches)
        {
            const string insertSql = @"
INSERT INTO public.automation_rule_executions
    (tenant_id, rule_id, job_id, event_key, event_idempotency_key, status, matched_conditions_json, proposed_actions_json, created_at, updated_at)
VALUES
    (@tenantId, @ruleId, @jobId, @eventKey, @idempotencyKey, 'awaiting_review', CAST(@conditions AS jsonb), CAST(@actions AS jsonb), NOW(), NOW())
ON CONFLICT (tenant_id, rule_id, event_idempotency_key) DO NOTHING;";
            await using var insert = new NpgsqlCommand(insertSql, conn);
            insert.Parameters.AddWithValue("tenantId", request.TenantId);
            insert.Parameters.AddWithValue("ruleId", match.RuleId);
            insert.Parameters.AddWithValue("jobId", request.JobId.HasValue ? request.JobId.Value : DBNull.Value);
            insert.Parameters.AddWithValue("eventKey", eventKey);
            insert.Parameters.AddWithValue("idempotencyKey", request.IdempotencyKey.Trim());
            insert.Parameters.AddWithValue("conditions", JsonSerializer.Serialize(match.Conditions));
            insert.Parameters.AddWithValue("actions", JsonSerializer.Serialize(match.Actions));
            queued += await insert.ExecuteNonQueryAsync();
        }
        return Results.Ok(new { success = true, matchedRules = matches.Count, queuedForReview = queued, duplicateMatchesSkipped = matches.Count - queued, sideEffectsExecuted = false });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Evaluate Advanced Actions event failed", detail: ex.Message, statusCode: 500);
    }
});

app.MapGet("/actions/review-queue", async (Guid tenantId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureAdvancedActionsTablesAsync(conn);
        const string sql = @"
SELECT e.execution_id, e.rule_id, r.name, e.job_id, e.event_key, e.status,
       e.matched_conditions_json, e.proposed_actions_json, e.last_error, e.created_at
FROM public.automation_rule_executions e
JOIN public.automation_rules r ON r.rule_id = e.rule_id
WHERE e.tenant_id = @tenantId AND e.status IN ('awaiting_review', 'failed')
ORDER BY e.created_at DESC;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await cmd.ExecuteReaderAsync();
        var items = new List<object>();
        while (await reader.ReadAsync())
            items.Add(new { executionId = reader.GetGuid(0), ruleId = reader.GetGuid(1), ruleName = reader.GetString(2), jobId = reader.IsDBNull(3) ? (Guid?)null : reader.GetGuid(3), eventKey = reader.GetString(4), status = reader.GetString(5), conditions = JsonSerializer.Deserialize<object>(reader.GetString(6)), proposedActions = JsonSerializer.Deserialize<object>(reader.GetString(7)), lastError = reader.IsDBNull(8) ? null : reader.GetString(8), createdAt = reader.GetDateTime(9) });
        return Results.Ok(items);
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Load Advanced Actions review queue failed", detail: ex.Message, statusCode: 500);
    }
});

await using (var startupMigrationConnection = new NpgsqlConnection(connectionString))
{
    await startupMigrationConnection.OpenAsync();
    await EnsureOnlinePropertyTablesAsync(startupMigrationConnection);
    await JobChangeSupport.EnsureAsync(startupMigrationConnection);
    await JobChangeSupport.BackfillApprovedSnapshotsAsync(startupMigrationConnection);
    await JobChangeSupport.RepairPendingChangesAsync(startupMigrationConnection);
    await EnsureEmailTemplatesTableAsync(startupMigrationConnection);
    await EnsureAdvancedActionsTablesAsync(startupMigrationConnection);
    await AutomationFoundationSupport.EnsureAsync(startupMigrationConnection);
    await AutoMateApi.BasicAutomationSupport.EnsureAsync(startupMigrationConnection);
    await AutoMateApi.BasicTemplateCommandSupport.EnsureAsync(startupMigrationConnection);
    await AutoMateApi.BasicSettingCommandSupport.EnsureAsync(startupMigrationConnection);
    await AutoMateApi.BasicTestExecutionSupport.EnsureAsync(startupMigrationConnection);
    await AutoMateApi.BasicProductionSchedulingSupport.EnsureAsync(startupMigrationConnection);
    await AutoMateApi.ClientEngagementSupport.EnsureAsync(startupMigrationConnection);
    await EnsureBasicJobProfileColumnsAsync(startupMigrationConnection);
}

app.Run();

static AutoMateApi.ClientEngagementCommand EngagementCommand(HttpContext context, AutoMateApi.ClientPageAccess access, string eventType, string eventKey) =>
    new(access.CommunicationId,access.TenantId,access.JobId,eventType,eventKey,
        context.Connection.RemoteIpAddress?.ToString(),context.Request.Headers.UserAgent.ToString(),context.Request.Headers.Referer.ToString(),"{}");

static string EngagementEventKey(HttpContext context,string type)
{
    var bucket=DateTime.UtcNow.ToString("yyyyMMddHHmm");
    var agent=context.Request.Headers.UserAgent.ToString();
    var family=agent.Contains("GoogleImageProxy",StringComparison.OrdinalIgnoreCase)?"google-proxy":agent.Contains("Apple",StringComparison.OrdinalIgnoreCase)?"apple":agent.Contains("bot",StringComparison.OrdinalIgnoreCase)||agent.Contains("scanner",StringComparison.OrdinalIgnoreCase)?"scanner":"client";
    return $"{type}:{bucket}:{family}";
}

static string BuildClientEngagementFooter(string url,bool pageEnabled,bool pixelEnabled)
{
    var safe=WebUtility.HtmlEncode(url);
    var button=pageEnabled?$"<div style=\"margin:24px 0;text-align:center\"><a href=\"{safe}\" style=\"display:inline-block;background:#0b5f86;color:#fff;text-decoration:none;padding:12px 20px;border-radius:8px;font-family:Segoe UI,Arial,sans-serif;font-weight:700\">View Inspection Details</a></div>":"";
    var pixel=pixelEnabled?$"<img src=\"{safe}/pixel.gif\" width=\"1\" height=\"1\" alt=\"\" style=\"display:block;width:1px;height:1px;border:0;opacity:0\">":"";
    return $"<!-- AutoMate client engagement -->{button}{pixel}";
}

static string ExpiredClientPageHtml()=>"<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"robots\" content=\"noindex,nofollow\"><title>Inspection page unavailable</title></head><body style=\"margin:0;background:#f5f7f9;color:#17212b;font:16px/1.5 Segoe UI,Arial,sans-serif\"><main style=\"max-width:620px;margin:12vh auto;padding:32px\"><div style=\"background:white;border:1px solid #dfe5e9;border-radius:14px;padding:32px\"><h1>Inspection page unavailable</h1><p>This secure link has expired, was replaced, or is no longer available. Contact your inspection company for assistance.</p></div></main></body></html>";

static async Task<AutoMateApi.ClientInspectionDisplay> LoadClientInspectionDisplayAsync(NpgsqlConnection conn,AutoMateApi.ClientPageAccess access,CancellationToken cancellationToken)
{
    const string sql=@"SELECT COALESCE((SELECT i2.company_name FROM public.inspectors i2 WHERE i2.tenant_id::text=j.tenant_id::text ORDER BY i2.created_at LIMIT 1),''),
COALESCE(NULLIF(j.inspector_name,''),i.email_from_name,''),COALESCE(NULLIF(j.inspector_phone,''),i.phone,''),COALESCE(NULLIF(j.inspector_email,''),i.email_from_address,''),
COALESCE(j.contact1_display_name,''),COALESCE(j.amount_paid,0),j.terms_required,j.terms_signed,COALESCE(j.signnow_signing_link,''),COALESCE(j.unscheduled,false)
FROM public.jobs_staging j LEFT JOIN public.inspectors i ON i.inspector_id=j.inspector_id WHERE j.job_id=@job AND j.tenant_id::text=@tenant LIMIT 1";
    await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("job",access.JobId);cmd.Parameters.AddWithValue("tenant",access.TenantId.ToString());
    await using var reader=await cmd.ExecuteReaderAsync(cancellationToken);if(!await reader.ReadAsync(cancellationToken))throw new UnauthorizedAccessException();
    return new(reader.GetString(0),"","",reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetDecimal(5),reader.GetBoolean(6),reader.GetBoolean(7),reader.GetString(8),reader.GetBoolean(9));
}

static async Task<(bool Allowed, IResult? Error)> RequireAutomationOwnerAsync(HttpContext context, NpgsqlConnection conn, Guid tenantId)
{
    if (tenantId == Guid.Empty)
        return (false, Results.BadRequest(new { success = false, message = "TenantId is required." }));
    var inspectorHeader = context.Request.Headers["X-AutoMate-Inspector-ID"].FirstOrDefault();
    if (!Guid.TryParse(inspectorHeader, out var inspectorId))
        return (false, Results.Json(new { success = false, message = "Registered AutoMate company identity is required." }, statusCode: 401));
    if (!await AutomationFoundationSupport.InspectorBelongsToTenantAsync(conn, tenantId, inspectorId))
        return (false, Results.Json(new { success = false, message = "This inspector does not belong to the requested company." }, statusCode: 403));
    var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(conn, tenantId);
    if (!entitlement.Allowed)
        return (false, Results.Json(new { success = false, message = "An active AutoMate subscription or trial is required." }, statusCode: 403));
    return (true, null);
}

static async Task EnsureAdvancedActionsTablesAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.automation_rules
(
    rule_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name text NOT NULL,
    event_key text NOT NULL,
    mode text NOT NULL DEFAULT 'disabled',
    enabled boolean NOT NULL DEFAULT false,
    conditions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    actions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_automation_rules_tenant_event
ON public.automation_rules(tenant_id, event_key, enabled);

CREATE TABLE IF NOT EXISTS public.automation_rule_executions
(
    execution_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    rule_id uuid NOT NULL REFERENCES public.automation_rules(rule_id) ON DELETE CASCADE,
    job_id uuid NULL,
    event_key text NOT NULL,
    event_idempotency_key text NOT NULL,
    status text NOT NULL DEFAULT 'awaiting_review',
    matched_conditions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    proposed_actions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    action_results_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, rule_id, event_idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_automation_rule_executions_review
ON public.automation_rule_executions(tenant_id, status, created_at DESC);";
    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static string NormalizeAutomationKey(string? value)
{
    return (value ?? "").Trim().ToLowerInvariant().Replace('-', '_').Replace(' ', '_');
}

static List<string> ValidateAutomationRule(AutomationRuleSaveRequest request)
{
    var errors = new List<string>();
    var validEvents = new HashSet<string> { "inspection_scheduled", "inspection_rescheduled", "pre_inspection_due", "inspection_cancelled", "price_changed", "service_changed" };
    var validFields = new HashSet<string> { "lifecycle", "status", "primary_service", "all_services", "site_address", "client_name", "inspector_name", "invoice_total", "change_categories" };
    var validOperators = new HashSet<string> { "includes", "does_not_include" };
    var validActions = new HashSet<string> { "send_email", "send_webhook", "upsert_calendar", "create_xero_draft", "send_signnow_agreement", "queue_report_communication", "set_workflow_state" };
    if (request.TenantId == Guid.Empty) errors.Add("TenantId is required.");
    if (string.IsNullOrWhiteSpace(request.Name)) errors.Add("Rule name is required.");
    if (!validEvents.Contains(NormalizeAutomationKey(request.EventKey))) errors.Add("Event is not supported.");
    if (request.Conditions != null) foreach (var condition in request.Conditions)
    {
        if (!validFields.Contains(NormalizeAutomationKey(condition.FieldKey))) errors.Add("Condition field is not supported: " + condition.FieldKey);
        if (!validOperators.Contains(NormalizeAutomationKey(condition.Operator))) errors.Add("Condition operator is not supported: " + condition.Operator);
        if (string.IsNullOrWhiteSpace(condition.Value)) errors.Add("Condition value is required.");
    }
    if (request.Actions == null || request.Actions.Count == 0) errors.Add("At least one action is required.");
    else foreach (var action in request.Actions)
    {
        var actionKey = NormalizeAutomationKey(action.ActionKey);
        if (!validActions.Contains(actionKey)) errors.Add("Action is not supported: " + action.ActionKey);
        if (actionKey == "send_webhook")
        {
            action.Settings.TryGetValue("url", out var endpoint);
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || (uri.Scheme != "https" && uri.Scheme != "http")) errors.Add("Webhook URL is required in the URL field.");
            action.Settings.TryGetValue("method", out var method);
            if (!new[] { "POST", "PUT", "PATCH" }.Contains((method ?? "POST").Trim().ToUpperInvariant())) errors.Add("Webhook method must be POST, PUT, or PATCH.");
        }
    }
    return errors.Distinct().ToList();
}

static List<AutomationConditionEvaluation> EvaluateAutomationConditions(List<AutomationCondition> conditions, Dictionary<string, string> fields)
{
    return conditions.Select(condition =>
    {
        fields.TryGetValue(condition.FieldKey ?? "", out var actual);
        actual ??= "";
        var expected = condition.Value ?? "";
        bool contains = actual.IndexOf(expected, StringComparison.OrdinalIgnoreCase) >= 0;
        bool matched = NormalizeAutomationKey(condition.Operator) == "includes" ? contains : !contains;
        return new AutomationConditionEvaluation(condition.FieldKey ?? "", condition.Operator ?? "", expected, actual, matched);
    }).ToList();
}

static async Task EnsureWorkflowActionsTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.job_workflow_actions
(
    job_id uuid NOT NULL,
    tenant_id uuid NULL,
    inspector_id uuid NOT NULL,
    action_key text NOT NULL,
    action_type text NOT NULL,
    service_key text NULL,
    service_label text NULL,
    service_slot text NULL,
    status text NOT NULL DEFAULT 'pending',
    retry_requested boolean NOT NULL DEFAULT false,
    retry_requested_at timestamptz NULL,
    sent_at timestamptz NULL,
    last_attempt_at timestamptz NULL,
    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, action_key)
);

ALTER TABLE public.job_workflow_actions
ADD COLUMN IF NOT EXISTS retry_requested_at timestamptz NULL;

CREATE INDEX IF NOT EXISTS idx_job_workflow_actions_pending
ON public.job_workflow_actions(status, retry_requested, action_type);

CREATE INDEX IF NOT EXISTS idx_job_workflow_actions_job
ON public.job_workflow_actions(job_id);";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task RefreshBookingWorkflowActionsAsync(NpgsqlConnection conn, JobUploadRequest payload, Guid jobId, Guid tenantId, Guid inspectorId)
{
    var actions = BuildBookingWorkflowActions(payload, jobId, tenantId, inspectorId);
    var actionKeys = actions.Select(action => action.ActionKey).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

    await using (var deleteCmd = new NpgsqlCommand(@"
DELETE FROM public.job_workflow_actions
WHERE job_id = @job_id
  AND action_type = 'booking_email'
  AND status <> 'sent'
  AND NOT (action_key = ANY(@action_keys));", conn))
    {
        deleteCmd.Parameters.AddWithValue("job_id", jobId);
        deleteCmd.Parameters.AddWithValue("action_keys", actionKeys);
        await deleteCmd.ExecuteNonQueryAsync();
    }

    foreach (var action in actions)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO public.job_workflow_actions
(
    job_id,
    tenant_id,
    inspector_id,
    action_key,
    action_type,
    service_key,
    service_label,
    service_slot,
    status,
    retry_requested,
    updated_at
)
VALUES
(
    @job_id,
    @tenant_id,
    @inspector_id,
    @action_key,
    @action_type,
    @service_key,
    @service_label,
    @service_slot,
    'pending',
    false,
    NOW()
)
ON CONFLICT (job_id, action_key)
DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    inspector_id = EXCLUDED.inspector_id,
    action_type = EXCLUDED.action_type,
    service_key = EXCLUDED.service_key,
    service_label = EXCLUDED.service_label,
    service_slot = EXCLUDED.service_slot,
    updated_at = NOW();", conn);

        cmd.Parameters.AddWithValue("job_id", action.JobId);
        cmd.Parameters.AddWithValue("tenant_id", action.TenantId);
        cmd.Parameters.AddWithValue("inspector_id", action.InspectorId);
        cmd.Parameters.AddWithValue("action_key", action.ActionKey);
        cmd.Parameters.AddWithValue("action_type", action.ActionType);
        cmd.Parameters.AddWithValue("service_key", action.ServiceKey);
        cmd.Parameters.AddWithValue("service_label", action.ServiceLabel);
        cmd.Parameters.AddWithValue("service_slot", action.ServiceSlot);

        await cmd.ExecuteNonQueryAsync();
    }
}

static List<WorkflowActionSeed> BuildBookingWorkflowActions(JobUploadRequest payload, Guid jobId, Guid tenantId, Guid inspectorId)
{
    if (!(payload.Services?.BookingEmailRequired ?? true))
        return new List<WorkflowActionSeed>();

    var labels = new[] { payload.Services?.Primary, payload.Services?.Additional1, payload.Services?.Additional2 }
        .Where(value => !string.IsNullOrWhiteSpace(value))
        .Select(value => value!.Trim())
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();
    if (labels.Length == 0)
        return new List<WorkflowActionSeed>();

    var templateKey = string.IsNullOrWhiteSpace(payload.Services?.BookingTemplateKey)
        ? "general_booking"
        : payload.Services.BookingTemplateKey.Trim();
    return new List<WorkflowActionSeed>
    {
        new(jobId, tenantId, inspectorId, "booking_job_confirmation", "booking_email", templateKey, string.Join(", ", labels), "job")
    };
}

static string NormalizeServiceKey(string? serviceKey, string? serviceLabel)
{
    var key = string.IsNullOrWhiteSpace(serviceKey) ? InferCanonicalServiceType(serviceLabel) : serviceKey.Trim();

    return key switch
    {
        "healthy_homes" => "healthy_homes_assessment",
        "meth_test" => "meth_field_composite",
        "custom" => "custom_service",
        "other" => "other_service",
        _ => key
    };
}

static string BuildBookingActionKey(string serviceKey, string? serviceLabel)
{
    if (string.IsNullOrWhiteSpace(serviceKey))
        return "";

    if (serviceKey == "custom_service" || serviceKey == "other_service")
        return "booking_custom_" + Slugify(serviceLabel);

    return "booking_" + Slugify(serviceKey);
}

static string Slugify(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return "unknown";

    var sb = new System.Text.StringBuilder();
    var previousUnderscore = false;

    foreach (var ch in value.Trim().ToLowerInvariant())
    {
        if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9'))
        {
            sb.Append(ch);
            previousUnderscore = false;
        }
        else if (!previousUnderscore)
        {
            sb.Append('_');
            previousUnderscore = true;
        }
    }

    return sb.ToString().Trim('_');
}

static bool IsModifierServiceKey(string key)
{
    return key == "additional_outbuilding"
        || key == "attached_flat"
        || key == "foundation_space"
        || key == "weathertightness"
        || key == "occupied_house"
        || key == "property_access"
        || key == "travel_fee"
        || key == "age_of_building"
        || key == "building_type"
        || key == "number_of_stories"
        || key == "number_of_bedrooms"
        || key == "number_of_bathrooms"
        || key == "monolithic_cladding"
        || key == "healthy_homes_bedrooms"
        || key == "meth_sample_count"
        || key == "custom_modifier"
        || key == "other_modifier";
}

static async Task EnsureInspectorsTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.inspectors
(
    inspector_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_name text NOT NULL,
    api_key text NULL,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS tenant_id uuid NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS company_name text NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS contact_name text NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS email_from_name text NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS email_from_address text NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS phone text NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS timezone text NOT NULL DEFAULT 'Pacific/Auckland';

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS allow_report_release_before_payment boolean NOT NULL DEFAULT false;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS onboarding_status text NOT NULL DEFAULT 'not_started';

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS logo_url text NULL;

ALTER TABLE public.inspectors
ADD COLUMN IF NOT EXISTS email_sender_mode text NOT NULL DEFAULT 'microsoft';

UPDATE public.inspectors
SET email_sender_mode = 'microsoft'
WHERE email_sender_mode IS NULL
   OR email_sender_mode NOT IN ('microsoft', 'threed-smtp', 'manual-smtp');

ALTER TABLE public.inspectors
DROP CONSTRAINT IF EXISTS inspectors_tenant_id_unique;
";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task EnsureSubscriptionsTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.subscriptions
(
    subscription_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_id uuid NOT NULL REFERENCES public.inspectors(inspector_id) ON DELETE CASCADE,
    status text NOT NULL DEFAULT 'trialing',
    plan_name text NULL,
    billing_interval text NULL,
    stripe_customer_id text NULL,
    stripe_subscription_id text NULL,
    trial_ends_at timestamptz NULL,
    current_period_end timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_inspector_id
ON public.subscriptions(inspector_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_subscriptions_inspector_id
ON public.subscriptions(inspector_id);
";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task EnsureInspectorIntegrationsTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.inspector_integrations
(
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_id uuid NOT NULL,
    provider text NOT NULL,
    status text DEFAULT 'disconnected',
    access_token_encrypted text NULL,
    refresh_token_encrypted text NULL,
    expires_at timestamptz NULL,
    external_account_email text NULL,
    external_tenant_id text NULL,
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW(),
    CONSTRAINT uq_inspector_integrations_inspector_provider UNIQUE (inspector_id, provider)
);";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task EnsureSignNowJobColumnsAsync(NpgsqlConnection conn)
{
    const string sql = @"
ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_signed boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_signed_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_document_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_invite_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_template_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_document_status text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_last_checked_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_signing_link text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_webhook_subscription_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_webhook_status text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS signnow_webhook_last_error text NULL;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task EnsureSignNowTemplateMappingsTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.signnow_template_mappings
(
    template_key text PRIMARY KEY,
    template_id text NOT NULL DEFAULT '',
    template_name text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task<List<SignNowTemplateMappingResult>> LoadSignNowTemplateMappingsAsync(NpgsqlConnection conn)
{
    const string sql = @"
SELECT template_key, template_id, template_name, updated_at
FROM public.signnow_template_mappings
ORDER BY template_key;";

    var mappings = new List<SignNowTemplateMappingResult>();

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        mappings.Add(new SignNowTemplateMappingResult(
            reader["template_key"]?.ToString() ?? "",
            reader["template_id"]?.ToString() ?? "",
            reader["template_name"]?.ToString() ?? "",
            reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O")));
    }

    return mappings;
}

static async Task UpsertSignNowTemplateMappingAsync(NpgsqlConnection conn, string templateKey, string templateId, string templateName)
{
    const string sql = @"
INSERT INTO public.signnow_template_mappings
(
    template_key,
    template_id,
    template_name,
    created_at,
    updated_at
)
VALUES
(
    @template_key,
    @template_id,
    @template_name,
    NOW(),
    NOW()
)
ON CONFLICT (template_key)
DO UPDATE SET
    template_id = EXCLUDED.template_id,
    template_name = EXCLUDED.template_name,
    updated_at = NOW();";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("template_key", templateKey);
    cmd.Parameters.AddWithValue("template_id", templateId);
    cmd.Parameters.AddWithValue("template_name", templateName);
    await cmd.ExecuteNonQueryAsync();
}

static async Task<SignNowTemplateMappingResult?> GetSignNowTemplateMappingAsync(NpgsqlConnection conn, string templateKey)
{
    const string sql = @"
SELECT template_key, template_id, template_name, updated_at
FROM public.signnow_template_mappings
WHERE template_key = @template_key
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("template_key", templateKey);

    await using var reader = await cmd.ExecuteReaderAsync();
    if (!await reader.ReadAsync())
        return null;

    return new SignNowTemplateMappingResult(
        reader["template_key"]?.ToString() ?? "",
        reader["template_id"]?.ToString() ?? "",
        reader["template_name"]?.ToString() ?? "",
        reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O"));
}

static async Task<IntegrationAccountResult> GetSignNowAccountAsync(
    NpgsqlConnection conn,
    IConfiguration configuration)
{
    const string sql = @"
SELECT
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    status
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = 'signnow'
LIMIT 1;";

    string? accessToken = null;
    string? refreshToken = null;
    DateTime? expiresAt = null;
    string? accountName = null;
    string? status = null;

    await using (var cmd = new NpgsqlCommand(sql, conn))
    {
        cmd.Parameters.AddWithValue("inspector_id", Guid.Empty);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            accessToken = reader["access_token_encrypted"]?.ToString();
            refreshToken = reader["refresh_token_encrypted"]?.ToString();
            accountName = reader["external_account_email"]?.ToString();
            status = reader["status"]?.ToString();
            if (reader["expires_at"] != DBNull.Value)
                expiresAt = Convert.ToDateTime(reader["expires_at"]);
        }
    }

    if (string.IsNullOrWhiteSpace(accessToken) ||
        !string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase))
    {
        return IntegrationAccountResult.Failure("SignNow is not connected.");
    }

    if (expiresAt.HasValue && expiresAt.Value > DateTime.UtcNow.AddMinutes(5))
        return IntegrationAccountResult.Ok(accessToken, refreshToken, "company", accountName);

    var clientId = configuration["SIGNNOW_CLIENT_ID"];
    var clientSecret = configuration["SIGNNOW_CLIENT_SECRET"];
    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
        return IntegrationAccountResult.Failure("SIGNNOW_CLIENT_ID and/or SIGNNOW_CLIENT_SECRET are missing.");

    if (string.IsNullOrWhiteSpace(refreshToken))
        return IntegrationAccountResult.Failure("SignNow access token expired and no refresh token is stored.");

    using var refreshClient = new HttpClient();
    refreshClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
        "Basic",
        Convert.ToBase64String(Encoding.UTF8.GetBytes(clientId + ":" + clientSecret)));

    var refreshResponse = await refreshClient.PostAsync(
        "https://api.signnow.com/oauth2/token",
        new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "refresh_token",
            ["refresh_token"] = refreshToken
        }));

    var refreshJson = await refreshResponse.Content.ReadAsStringAsync();
    if (!refreshResponse.IsSuccessStatusCode)
        return IntegrationAccountResult.Failure("SignNow token refresh failed: " + refreshJson);

    var refreshDoc = JsonDocument.Parse(refreshJson).RootElement;
    accessToken = GetJsonString(refreshDoc, "access_token");
    refreshToken = string.IsNullOrWhiteSpace(GetJsonString(refreshDoc, "refresh_token"))
        ? refreshToken
        : GetJsonString(refreshDoc, "refresh_token");

    var refreshedExpiresIn = refreshDoc.TryGetProperty("expires_in", out var refreshedExpiresInProp) &&
        refreshedExpiresInProp.TryGetInt32(out var parsedExpiresIn)
            ? parsedExpiresIn
            : 3600;
    expiresAt = DateTime.UtcNow.AddSeconds(refreshedExpiresIn);

    const string updateSql = @"
UPDATE public.inspector_integrations
SET
    access_token_encrypted = @access_token,
    refresh_token_encrypted = @refresh_token,
    expires_at = @expires_at,
    updated_at = NOW()
WHERE inspector_id = @inspector_id
  AND provider = 'signnow';";

    await using var updateCmd = new NpgsqlCommand(updateSql, conn);
    updateCmd.Parameters.AddWithValue("access_token", accessToken);
    updateCmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
    updateCmd.Parameters.AddWithValue("expires_at", expiresAt.Value);
    updateCmd.Parameters.AddWithValue("inspector_id", Guid.Empty);
    await updateCmd.ExecuteNonQueryAsync();

    return IntegrationAccountResult.Ok(accessToken, refreshToken, "company", accountName);
}

static async Task<XeroAccountResult> GetXeroAccountAsync(
    NpgsqlConnection conn,
    Guid inspectorId,
    IConfiguration configuration)
{
    const string sql = @"
SELECT
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    external_tenant_id,
    status
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = 'xero'
LIMIT 1;";

    string? accessToken = null;
    string? refreshToken = null;
    DateTime? expiresAt = null;
    string? tenantName = null;
    string? tenantId = null;
    string? status = null;

    await using (var cmd = new NpgsqlCommand(sql, conn))
    {
        cmd.Parameters.AddWithValue("inspector_id", inspectorId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            accessToken = reader["access_token_encrypted"]?.ToString();
            refreshToken = reader["refresh_token_encrypted"]?.ToString();
            tenantName = reader["external_account_email"]?.ToString();
            tenantId = reader["external_tenant_id"]?.ToString();
            status = reader["status"]?.ToString();

            if (reader["expires_at"] != DBNull.Value)
                expiresAt = Convert.ToDateTime(reader["expires_at"]);
        }
    }

    if (string.IsNullOrWhiteSpace(accessToken) ||
        string.IsNullOrWhiteSpace(tenantId) ||
        !string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase))
    {
        return XeroAccountResult.Failure("Xero is not connected for this inspector.");
    }

    if (expiresAt.HasValue && expiresAt.Value > DateTime.UtcNow.AddMinutes(5))
        return XeroAccountResult.Ok(accessToken, refreshToken, tenantId, tenantName);

    var clientId = configuration["XERO_CLIENT_ID"];
    var clientSecret = configuration["XERO_CLIENT_SECRET"];

    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
        return XeroAccountResult.Failure("XERO_CLIENT_ID and/or XERO_CLIENT_SECRET are missing.");

    if (string.IsNullOrWhiteSpace(refreshToken))
        return XeroAccountResult.Failure("Access token expired and no refresh token is stored.");

    using var refreshClient = new HttpClient();
    refreshClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
        "Basic",
        Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(clientId + ":" + clientSecret)));

    var refreshResponse = await refreshClient.PostAsync(
        "https://identity.xero.com/connect/token",
        new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "refresh_token",
            ["refresh_token"] = refreshToken
        }));

    var refreshJson = await refreshResponse.Content.ReadAsStringAsync();

    if (!refreshResponse.IsSuccessStatusCode)
        return XeroAccountResult.Failure($"Xero token refresh failed: {refreshJson}");

    var refreshDoc = JsonDocument.Parse(refreshJson).RootElement;
    accessToken = refreshDoc.GetProperty("access_token").GetString() ?? accessToken;
    refreshToken = refreshDoc.TryGetProperty("refresh_token", out var refreshedTokenProp)
        ? refreshedTokenProp.GetString() ?? refreshToken
        : refreshToken;

    var refreshedExpiresIn = refreshDoc.TryGetProperty("expires_in", out var refreshedExpiresInProp)
        ? refreshedExpiresInProp.GetInt32()
        : 1800;
    expiresAt = DateTime.UtcNow.AddSeconds(refreshedExpiresIn);

    const string updateSql = @"
UPDATE public.inspector_integrations
SET
    access_token_encrypted = @access_token,
    refresh_token_encrypted = @refresh_token,
    expires_at = @expires_at,
    updated_at = NOW()
WHERE inspector_id = @inspector_id
  AND provider = 'xero';";

    await using var updateCmd = new NpgsqlCommand(updateSql, conn);
    updateCmd.Parameters.AddWithValue("access_token", accessToken);
    updateCmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
    updateCmd.Parameters.AddWithValue("expires_at", expiresAt.Value);
    updateCmd.Parameters.AddWithValue("inspector_id", inspectorId);
    await updateCmd.ExecuteNonQueryAsync();

    return XeroAccountResult.Ok(accessToken, refreshToken, tenantId, tenantName);
}

static async Task<XeroInvoiceJobInput?> LoadXeroInvoiceJobAsync(NpgsqlConnection conn, Guid jobId)
{
    const string sql = @"
SELECT
    job_id,
    inspector_id,
    job_name,
    site_address,
    job_date,
    job_total,
    primary_service,
    contact1_first_name,
    contact1_last_name,
    contact1_email,
    contact1_cellular,
    xero_contact_id,
    xero_invoice_id,
    xero_invoice_number,
    xero_invoice_status
FROM public.jobs_staging
WHERE job_id = @job_id
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);

    await using var reader = await cmd.ExecuteReaderAsync();
    if (!await reader.ReadAsync())
        return null;

    var contactName = BuildPersonName(
        reader["contact1_first_name"]?.ToString(),
        reader["contact1_last_name"]?.ToString());
    var contactEmail = reader["contact1_email"]?.ToString() ?? "";
    if (string.IsNullOrWhiteSpace(contactName))
        contactName = string.IsNullOrWhiteSpace(contactEmail) ? "3D AutoMate Client" : contactEmail;

    return new XeroInvoiceJobInput(
        jobId,
        (Guid)reader["inspector_id"],
        reader["job_name"]?.ToString() ?? "",
        reader["site_address"]?.ToString() ?? "",
        reader["job_date"] == DBNull.Value ? null : Convert.ToDateTime(reader["job_date"]),
        reader["job_total"] == DBNull.Value ? null : Convert.ToDecimal(reader["job_total"]),
        reader["primary_service"]?.ToString() ?? "",
        contactName,
        contactEmail,
        reader["contact1_cellular"]?.ToString() ?? "",
        reader["xero_contact_id"]?.ToString() ?? "",
        reader["xero_invoice_id"]?.ToString() ?? "",
        reader["xero_invoice_number"]?.ToString() ?? "",
        reader["xero_invoice_status"]?.ToString() ?? "");
}

static async Task<List<XeroInvoiceLineInput>> LoadXeroInvoiceLinesAsync(NpgsqlConnection conn, Guid jobId)
{
    const string sql = @"
SELECT
    line_index,
    description,
    quantity,
    unit_price
FROM public.job_invoice_lines
WHERE job_id = @job_id
ORDER BY line_index;";

    var lines = new List<XeroInvoiceLineInput>();
    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);

    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        var description = reader["description"]?.ToString() ?? "";
        if (string.IsNullOrWhiteSpace(description))
            description = "Inspection service";

        lines.Add(new XeroInvoiceLineInput(
            description,
            reader["quantity"] == DBNull.Value ? 1m : Convert.ToDecimal(reader["quantity"]),
            reader["unit_price"] == DBNull.Value ? 0m : Convert.ToDecimal(reader["unit_price"]),
            reader["line_index"] == DBNull.Value ? 0 : Convert.ToInt32(reader["line_index"])));
    }

    return lines;
}

static async Task<string> FindXeroContactIdByEmailAsync(HttpClient httpClient, string email)
{
    if (string.IsNullOrWhiteSpace(email))
        return "";

    var where = Uri.EscapeDataString($"EmailAddress==\"{email.Trim()}\"");
    var response = await httpClient.GetAsync($"https://api.xero.com/api.xro/2.0/Contacts?where={where}");
    var body = await response.Content.ReadAsStringAsync();

    if (!response.IsSuccessStatusCode)
        return "";

    var doc = JsonDocument.Parse(body).RootElement;
    if (!doc.TryGetProperty("Contacts", out var contacts) ||
        contacts.ValueKind != JsonValueKind.Array ||
        contacts.GetArrayLength() == 0)
    {
        return "";
    }

    return GetJsonString(contacts[0], "ContactID");
}

static async Task<string> CreateXeroContactAsync(HttpClient httpClient, string name, string email, string phone)
{
    if (string.IsNullOrWhiteSpace(name) && string.IsNullOrWhiteSpace(email))
        throw new InvalidOperationException("Client contact name or email is required before creating a Xero contact.");

    var contact = new Dictionary<string, object?>
    {
        ["Name"] = string.IsNullOrWhiteSpace(name) ? email.Trim() : name.Trim()
    };

    if (!string.IsNullOrWhiteSpace(email))
        contact["EmailAddress"] = email.Trim();

    if (!string.IsNullOrWhiteSpace(phone))
    {
        contact["Phones"] = new[]
        {
            new
            {
                PhoneType = "MOBILE",
                PhoneNumber = phone.Trim()
            }
        };
    }

    var response = await httpClient.PostAsJsonAsync(
        "https://api.xero.com/api.xro/2.0/Contacts",
        new { Contacts = new[] { contact } });
    var body = await response.Content.ReadAsStringAsync();

    if (!response.IsSuccessStatusCode)
    {
        if (response.StatusCode == HttpStatusCode.Unauthorized)
        {
            throw new InvalidOperationException(
                "Xero rejected contact creation. Reconnect Xero from Setup / Settings > Integrations > Xero so 3D AutoMate can request the new contact and invoice permissions.");
        }

        throw new InvalidOperationException("Xero contact creation failed: " + body);
    }

    var doc = JsonDocument.Parse(body).RootElement;
    if (!doc.TryGetProperty("Contacts", out var contacts) ||
        contacts.ValueKind != JsonValueKind.Array ||
        contacts.GetArrayLength() == 0)
    {
        return "";
    }

    return GetJsonString(contacts[0], "ContactID");
}

static async Task StoreXeroInvoiceResultAsync(
    NpgsqlConnection conn,
    Guid jobId,
    string contactId,
    string invoiceId,
    string invoiceNumber,
    string invoiceStatus)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    xero_contact_id = @xero_contact_id,
    xero_invoice_id = @xero_invoice_id,
    xero_invoice_number = @xero_invoice_number,
    xero_invoice_status = @xero_invoice_status,
    xero_invoice_created_at = NOW(),
    xero_last_error = NULL,
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("xero_contact_id", contactId);
    cmd.Parameters.AddWithValue("xero_invoice_id", invoiceId);
    cmd.Parameters.AddWithValue("xero_invoice_number", invoiceNumber);
    cmd.Parameters.AddWithValue("xero_invoice_status", invoiceStatus);
    await cmd.ExecuteNonQueryAsync();
}

static async Task StoreXeroJobErrorAsync(NpgsqlConnection conn, Guid jobId, string error)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    xero_last_error = @xero_last_error,
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("xero_last_error", string.IsNullOrWhiteSpace(error) ? "Unknown Xero error" : error);
    await cmd.ExecuteNonQueryAsync();
}

static async Task<ScheduleJobInput?> LoadScheduleJobAsync(NpgsqlConnection conn, Guid jobId)
{
    await EnsureBasicJobProfileColumnsAsync(conn);
    const string sql = @"
SELECT
    j.job_id,
    j.tenant_id,
    j.inspector_id,
    j.inspector_name,
    j.job_name,
    j.site_address,
    j.job_date,
    j.inspection_duration_minutes,
    j.primary_service,
    j.additional1,
    j.additional2,
    j.primary_service_key,
    j.additional1_service_key,
    j.additional2_service_key,
    j.booking_template_key,
    j.booking_email_required,
    j.booking_email_sent,
    j.terms_required,
    j.terms_sent,
    j.terms_retry_requested,
    j.terms_signed,
    j.signnow_document_id,
    j.invoice_required,
    j.calendar_required,
    j.calendar_created,
    j.notes,
    j.directions,
    j.instructions,
    j.age_of_building,
    j.stories,
    j.bedrooms,
    j.bathrooms,
    j.monolithic,
    j.foundation_space,
    j.access_by,
    j.contact1_first_name,
    j.contact1_last_name,
    j.contact1_display_name,
    j.contact1_salutation,
    j.contact1_role_label,
    j.contact1_email,
    j.contact1_cellular,
    j.contact2_first_name,
    j.contact2_last_name,
    j.contact2_display_name,
    j.contact2_salutation,
    j.contact2_role_label,
    j.contact2_email,
    j.contact2_cellular,
    COALESCE(i.timezone, 'Pacific/Auckland') AS timezone,
    COALESCE((
        SELECT i2.company_name
        FROM public.inspectors i2
        LEFT JOIN public.subscriptions s2 ON s2.inspector_id=i2.inspector_id
        WHERE i2.tenant_id::text=j.tenant_id::text
        ORDER BY CASE WHEN s2.status='active' THEN 0 WHEN s2.status='trialing' AND s2.trial_ends_at>NOW() THEN 1 ELSE 2 END,
                 i2.created_at
        LIMIT 1
    ), '') AS company_name,
    i.email_from_name,
    COALESCE(NULLIF(j.inspector_email,''),i.email_from_address) AS email_from_address,
    COALESCE(NULLIF(j.inspector_phone,''),i.phone) AS phone,
    COALESCE(i.email_sender_mode, 'microsoft') AS email_sender_mode
FROM public.jobs_staging j
LEFT JOIN public.inspectors i
    ON i.inspector_id = j.inspector_id
WHERE j.job_id = @job_id
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);

    await using var reader = await cmd.ExecuteReaderAsync();
    if (!await reader.ReadAsync())
        return null;

    return new ScheduleJobInput(
        ReadDatabaseGuid(reader["job_id"]),
        ReadDatabaseGuid(reader["tenant_id"]),
        ReadDatabaseGuid(reader["inspector_id"]),
        reader["inspector_name"]?.ToString() ?? "",
        reader["job_name"]?.ToString() ?? "",
        reader["site_address"]?.ToString() ?? "",
        reader["job_date"] == DBNull.Value ? null : Convert.ToDateTime(reader["job_date"]),
        reader["inspection_duration_minutes"] == DBNull.Value ? 60 : Convert.ToInt32(reader["inspection_duration_minutes"]),
        reader["primary_service"]?.ToString() ?? "",
        reader["additional1"]?.ToString() ?? "",
        reader["additional2"]?.ToString() ?? "",
        reader["primary_service_key"]?.ToString() ?? "",
        reader["additional1_service_key"]?.ToString() ?? "",
        reader["additional2_service_key"]?.ToString() ?? "",
        reader["booking_template_key"]?.ToString() ?? "general_booking",
        reader["booking_email_required"] != DBNull.Value && Convert.ToBoolean(reader["booking_email_required"]),
        reader["booking_email_sent"] != DBNull.Value && Convert.ToBoolean(reader["booking_email_sent"]),
        reader["terms_required"] != DBNull.Value && Convert.ToBoolean(reader["terms_required"]),
        reader["terms_sent"] != DBNull.Value && Convert.ToBoolean(reader["terms_sent"]),
        reader["terms_retry_requested"] != DBNull.Value && Convert.ToBoolean(reader["terms_retry_requested"]),
        reader["terms_signed"] != DBNull.Value && Convert.ToBoolean(reader["terms_signed"]),
        reader["signnow_document_id"]?.ToString() ?? "",
        reader["invoice_required"] != DBNull.Value && Convert.ToBoolean(reader["invoice_required"]),
        reader["calendar_required"] != DBNull.Value && Convert.ToBoolean(reader["calendar_required"]),
        reader["calendar_created"] != DBNull.Value && Convert.ToBoolean(reader["calendar_created"]),
        reader["notes"]?.ToString() ?? "",
        reader["directions"]?.ToString() ?? "",
        reader["instructions"]?.ToString() ?? "",
        reader["age_of_building"]?.ToString() ?? "",
        reader["stories"]?.ToString() ?? "",
        reader["bedrooms"]?.ToString() ?? "",
        reader["bathrooms"]?.ToString() ?? "",
        reader["monolithic"]?.ToString() ?? "",
        reader["foundation_space"]?.ToString() ?? "",
        reader["access_by"]?.ToString() ?? "",
        BuildPersonName(reader["contact1_first_name"]?.ToString(), reader["contact1_last_name"]?.ToString()),
        reader["contact1_first_name"]?.ToString() ?? "",
        reader["contact1_last_name"]?.ToString() ?? "",
        reader["contact1_display_name"]?.ToString() ?? "",
        reader["contact1_salutation"]?.ToString() ?? "",
        reader["contact1_role_label"]?.ToString() ?? "Client",
        reader["contact1_email"]?.ToString() ?? "",
        reader["contact1_cellular"]?.ToString() ?? "",
        BuildPersonName(reader["contact2_first_name"]?.ToString(), reader["contact2_last_name"]?.ToString()),
        reader["contact2_first_name"]?.ToString() ?? "",
        reader["contact2_last_name"]?.ToString() ?? "",
        reader["contact2_display_name"]?.ToString() ?? "",
        reader["contact2_salutation"]?.ToString() ?? "",
        reader["contact2_role_label"]?.ToString() ?? "Buyers Agent",
        reader["contact2_email"]?.ToString() ?? "",
        reader["contact2_cellular"]?.ToString() ?? "",
        reader["timezone"]?.ToString() ?? "Pacific/Auckland",
        reader["company_name"]?.ToString() ?? "",
        reader["email_from_name"]?.ToString() ?? "",
        reader["email_from_address"]?.ToString() ?? "",
        reader["phone"]?.ToString() ?? "",
        NormalizeEmailSenderMode(reader["email_sender_mode"]?.ToString()));
}

static async Task EnsureBasicJobProfileColumnsAsync(NpgsqlConnection conn)
{
    const string sql=@"ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS inspector_email text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS inspector_phone text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact1_display_name text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact1_role_label text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact2_display_name text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact2_role_label text NULL;";
    await using var cmd=new NpgsqlCommand(sql,conn);await cmd.ExecuteNonQueryAsync();
}

static Guid ReadDatabaseGuid(object? value)
{
    if (value == null || value == DBNull.Value)
        return Guid.Empty;
    if (value is Guid guid)
        return guid;
    return Guid.TryParse(Convert.ToString(value), out var parsed) ? parsed : Guid.Empty;
}

static async Task<ScheduleActionResult> SendScheduleBookingEmailsAsync(
    NpgsqlConnection conn,
    ScheduleJobInput job,
    IConfiguration configuration)
{
    if (job.BookingEmailSent)
        return ScheduleActionResult.Skip("booking-email", "Booking email was already marked sent.");

    if (string.IsNullOrWhiteSpace(job.ClientEmail))
    {
        await MarkBookingEmailFailedAsync(conn, job.JobId, "Client email is missing.");
        return ScheduleActionResult.Failed("booking-email", "Client email is missing.");
    }

    var services = GetSchedulableServices(job).ToArray();
    if (services.Length == 0)
        return ScheduleActionResult.Skip("booking-email", "No schedulable services were found.");

    if (IsSmtpEmailSenderMode(job.EmailSenderMode))
    {
        return ScheduleActionResult.Skip(
            "booking-email",
            "Booking email is pending for local SMTP sending in the desktop connector.",
            new
            {
                senderMode = job.EmailSenderMode,
                provider = GetEmailSenderModeLabel(job.EmailSenderMode),
                pending = services.Select(service => service.Label).ToArray()
            });
    }

    var account = await GetMicrosoftMailAccountAsync(conn, job.InspectorId, configuration);
    if (!account.Success)
    {
        await MarkBookingEmailFailedAsync(conn, job.JobId, account.ErrorMessage ?? "Microsoft email is not connected.");
        return ScheduleActionResult.Failed("booking-email", account.ErrorMessage ?? "Microsoft email is not connected.");
    }

    using var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);

    var sent = new List<string>();
    foreach (var service in services)
    {
        var subject = $"Booking confirmation - {service.Label}";
        var body = BuildScheduleBookingEmailHtml(job, service);
        var response = await SendMicrosoftMailAsync(httpClient, job.ClientEmail, subject, body);
        if (!response.Success)
        {
            await MarkBookingEmailFailedAsync(conn, job.JobId, response.Message);
            return ScheduleActionResult.Failed("booking-email", response.Message, new { sent });
        }

        await MarkWorkflowActionSentAsync(conn, job.JobId, BuildBookingActionKey(service.ServiceKey, service.Label));
        sent.Add(service.Label);
    }

    await MarkBookingEmailSentAsync(conn, job.JobId);
    return ScheduleActionResult.Ok("booking-email", $"Sent {sent.Count} booking email(s).", new { sent });
}

static IEnumerable<ScheduleServiceInput> GetSchedulableServices(ScheduleJobInput job)
{
    foreach (var service in new[]
    {
        new ScheduleServiceInput(job.PrimaryService, NormalizeServiceKey(job.PrimaryServiceKey, job.PrimaryService), "primary"),
        new ScheduleServiceInput(job.Additional1, NormalizeServiceKey(job.Additional1ServiceKey, job.Additional1), "additional1"),
        new ScheduleServiceInput(job.Additional2, NormalizeServiceKey(job.Additional2ServiceKey, job.Additional2), "additional2")
    })
    {
        if (string.IsNullOrWhiteSpace(service.Label))
            continue;

        if (string.IsNullOrWhiteSpace(service.ServiceKey) || IsModifierServiceKey(service.ServiceKey))
            continue;

        yield return service;
    }
}

#pragma warning disable CS8321
static string BuildScheduleBookingEmailHtml(ScheduleJobInput job, ScheduleServiceInput service)
{
    var company = job.CompanyName?.Trim() ?? "";
    var inspector = !string.IsNullOrWhiteSpace(job.EmailFromName) ? job.EmailFromName.Trim() : job.InspectorName;
    var start = job.JobDate.HasValue ? job.JobDate.Value.ToLocalTime().ToString("f") : "To be confirmed";

    return
        "<div style=\"font-family:Segoe UI,Arial,sans-serif;font-size:15px;line-height:1.5;color:#1f2937;\">" +
        $"<p>Hi {WebUtility.HtmlEncode(FirstWord(job.ClientName))},</p>" +
        $"<p>Your <strong>{WebUtility.HtmlEncode(service.Label)}</strong> booking has been scheduled.</p>" +
        "<table style=\"border-collapse:collapse;margin:16px 0;\">" +
        $"<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Address</td><td>{WebUtility.HtmlEncode(job.SiteAddress)}</td></tr>" +
        $"<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Date/time</td><td>{WebUtility.HtmlEncode(start)}</td></tr>" +
        $"<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Inspector</td><td>{WebUtility.HtmlEncode(inspector)}</td></tr>" +
        "</table>" +
        "<p>Terms and any agreement documents will follow where required.</p>" +
        $"<p>Regards,<br>{WebUtility.HtmlEncode(company)}</p>" +
        "</div>";
}
#pragma warning restore CS8321

static async Task<ScheduleActionResult> CreateXeroDraftInvoiceForJobAsync(
    NpgsqlConnection conn,
    Guid jobId,
    IConfiguration configuration)
{
    var job = await LoadXeroInvoiceJobAsync(conn, jobId);
    if (job == null)
        return ScheduleActionResult.Failed("invoice", "Job was not found in Railway. Sync the selected job first.");

    if (!string.IsNullOrWhiteSpace(job.XeroInvoiceId))
    {
        await MarkInvoiceSentAsync(conn, jobId);
        return ScheduleActionResult.Skip("invoice", "Xero draft invoice already exists.", new
        {
            invoiceId = job.XeroInvoiceId,
            invoiceNumber = job.XeroInvoiceNumber,
            invoiceStatus = job.XeroInvoiceStatus
        });
    }

    var account = await GetXeroAccountAsync(conn, job.InspectorId, configuration);
    if (!account.Success)
    {
        await StoreXeroJobErrorAsync(conn, jobId, account.ErrorMessage ?? "Xero is not connected.");
        return ScheduleActionResult.Failed("invoice", account.ErrorMessage ?? "Xero is not connected.");
    }

    using var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
    httpClient.DefaultRequestHeaders.Add("xero-tenant-id", account.TenantId);
    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

    var contactId = job.XeroContactId;
    if (string.IsNullOrWhiteSpace(contactId))
        contactId = await FindXeroContactIdByEmailAsync(httpClient, job.ContactEmail);

    if (string.IsNullOrWhiteSpace(contactId))
        contactId = await CreateXeroContactAsync(httpClient, job.ContactName, job.ContactEmail, job.ContactPhone);

    if (string.IsNullOrWhiteSpace(contactId))
        return ScheduleActionResult.Failed("invoice", "Xero did not return a contact ID.");

    var invoiceLines = await LoadXeroInvoiceLinesAsync(conn, jobId);
    if (invoiceLines.Count == 0)
    {
        invoiceLines.Add(new XeroInvoiceLineInput(
            BuildFallbackInvoiceDescription(job.PrimaryService, job.SiteAddress),
            1m,
            job.JobTotal ?? 0m,
            1));
    }

    if (invoiceLines.All(line => line.UnitAmount == 0m))
    {
        var message = "No invoice amount was found. Check the THREED invoice total/lines and sync the job again.";
        await StoreXeroJobErrorAsync(conn, jobId, message);
        return ScheduleActionResult.Failed("invoice", message);
    }

    var invoicePayload = new
    {
        Invoices = new[]
        {
            new
            {
                Type = "ACCREC",
                Contact = new { ContactID = contactId },
                DateString = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                DueDateString = (job.JobDate ?? DateTime.UtcNow).ToString("yyyy-MM-dd"),
                Reference = string.IsNullOrWhiteSpace(job.SiteAddress) ? job.JobName : job.SiteAddress,
                Status = "DRAFT",
                SentToContact = false,
                LineItems = invoiceLines.Select(line => new
                {
                    Description = line.Description,
                    Quantity = line.Quantity <= 0m ? 1m : line.Quantity,
                    UnitAmount = line.UnitAmount
                }).ToArray()
            }
        }
    };

    var invoiceResponse = await httpClient.PostAsJsonAsync(
        "https://api.xero.com/api.xro/2.0/Invoices",
        invoicePayload);
    var invoiceJson = await invoiceResponse.Content.ReadAsStringAsync();

    if (!invoiceResponse.IsSuccessStatusCode)
    {
        await StoreXeroJobErrorAsync(conn, jobId, invoiceJson);
        return ScheduleActionResult.Failed("invoice", "Xero draft invoice creation failed: " + invoiceJson);
    }

    var invoiceDoc = JsonDocument.Parse(invoiceJson).RootElement;
    var invoice = invoiceDoc.TryGetProperty("Invoices", out var invoicesProp) &&
                  invoicesProp.ValueKind == JsonValueKind.Array &&
                  invoicesProp.GetArrayLength() > 0
        ? invoicesProp[0]
        : invoiceDoc;

    var invoiceId = GetJsonString(invoice, "InvoiceID");
    var invoiceNumber = GetJsonString(invoice, "InvoiceNumber");
    var invoiceStatus = GetJsonString(invoice, "Status");

    await StoreXeroInvoiceResultAsync(conn, jobId, contactId, invoiceId, invoiceNumber, invoiceStatus);
    await MarkInvoiceSentAsync(conn, jobId);

    return ScheduleActionResult.Ok("invoice", "Xero draft invoice created.", new
    {
        invoiceId,
        invoiceNumber,
        invoiceStatus,
        sentToContact = false
    });
}

static async Task<ScheduleActionResult> SendSignNowTermsForJobAsync(
    NpgsqlConnection conn,
    ScheduleJobInput job,
    IConfiguration configuration,
    bool forceResend)
{
    if (!job.TermsRequired)
        return ScheduleActionResult.Skip("terms", "Terms are not required for this job.");

    if (job.TermsSigned && !forceResend)
        return ScheduleActionResult.Skip("terms", "Terms are already signed.", new { documentId = job.SignNowDocumentId });

    if (job.TermsSent && !job.TermsRetryRequested && !forceResend)
        return ScheduleActionResult.Skip("terms", "Terms have already been sent.", new { documentId = job.SignNowDocumentId });

    if (string.IsNullOrWhiteSpace(job.ClientEmail))
    {
        await MarkTermsFailedAsync(conn, job.JobId, "Client email is missing.");
        return ScheduleActionResult.Failed("terms", "Client email is missing.");
    }

    var templateKey = ResolveSignNowTermsTemplateKey(job.BookingTemplateKey);

    var mapping = await GetSignNowTemplateMappingAsync(conn, templateKey);
    if (mapping == null || string.IsNullOrWhiteSpace(mapping.TemplateId))
    {
        var message = $"No SignNow template is mapped for service/template key '{templateKey}'. Open Setup / Settings > SignNow Templates and choose a template.";
        await MarkTermsFailedAsync(conn, job.JobId, message);
        return ScheduleActionResult.Failed("terms", message);
    }

    var account = await GetSignNowAccountAsync(conn, configuration);
    if (!account.Success)
    {
        await MarkTermsFailedAsync(conn, job.JobId, account.ErrorMessage ?? "SignNow is not connected.");
        return ScheduleActionResult.Failed("terms", account.ErrorMessage ?? "SignNow is not connected.");
    }

    using var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

    var documentName = BuildSignNowDocumentName(job);
    var copyResponse = await httpClient.PostAsJsonAsync(
        $"https://api.signnow.com/template/{Uri.EscapeDataString(mapping.TemplateId)}/copy",
        new
        {
            document_name = documentName,
            name = documentName
        });
    var copyJson = await copyResponse.Content.ReadAsStringAsync();

    if (!copyResponse.IsSuccessStatusCode)
    {
        copyResponse = await httpClient.PostAsJsonAsync(
            $"https://api.signnow.com/document/{Uri.EscapeDataString(mapping.TemplateId)}/copy",
            new
            {
                document_name = documentName,
                name = documentName
            });
        copyJson = await copyResponse.Content.ReadAsStringAsync();
    }

    if (!copyResponse.IsSuccessStatusCode)
    {
        await MarkTermsFailedAsync(conn, job.JobId, copyJson);
        return ScheduleActionResult.Failed("terms", "SignNow template/document copy failed: " + copyJson);
    }

    var copyDoc = JsonDocument.Parse(copyJson).RootElement;
    var documentId = FirstNonEmptyJsonString(copyDoc, "id", "document_id", "unique_id");
    if (string.IsNullOrWhiteSpace(documentId))
    {
        await MarkTermsFailedAsync(conn, job.JobId, "SignNow did not return a document ID after copying the template.");
        return ScheduleActionResult.Failed("terms", "SignNow did not return a document ID after copying the template.");
    }

    var webhook = await CreateSignNowDocumentWebhookAsync(httpClient, documentId, configuration);

    var documentResponse = await httpClient.GetAsync($"https://api.signnow.com/document/{Uri.EscapeDataString(documentId)}");
    var documentJson = await documentResponse.Content.ReadAsStringAsync();
    JsonElement? documentRoot = null;
    if (documentResponse.IsSuccessStatusCode && !string.IsNullOrWhiteSpace(documentJson))
        documentRoot = JsonDocument.Parse(documentJson).RootElement.Clone();

    await TryPrefillSignNowDocumentAsync(httpClient, documentId, job);

    var signerRole = documentRoot.HasValue ? ExtractSignNowSignerRole(documentRoot.Value) : "";
    if (string.IsNullOrWhiteSpace(signerRole))
        signerRole = "Signer 1";

    var invitePayload = new
    {
        document_id = documentId,
        subject = "Terms and conditions for your inspection",
        message = "Please review and sign the terms and conditions for your inspection.",
        from = account.AccountName,
        to = new[]
        {
            new
            {
                email = job.ClientEmail.Trim(),
                role = signerRole,
                role_id = ExtractSignNowSignerRoleId(documentRoot),
                order = 1,
                subject = "Terms and conditions for your inspection",
                message = "Please review and sign the terms and conditions for your inspection."
            }
        }
    };

    var inviteResponse = await httpClient.PostAsJsonAsync(
        $"https://api.signnow.com/document/{Uri.EscapeDataString(documentId)}/invite",
        invitePayload);
    var inviteJson = await inviteResponse.Content.ReadAsStringAsync();

    if (!inviteResponse.IsSuccessStatusCode)
    {
        await MarkTermsFailedAsync(conn, job.JobId, inviteJson);
        return ScheduleActionResult.Failed("terms", "SignNow invite failed: " + inviteJson);
    }

    var inviteId = "";
    var signingLink = "";
    if (!string.IsNullOrWhiteSpace(inviteJson))
    {
        var inviteDoc = JsonDocument.Parse(inviteJson).RootElement;
        inviteId = FirstNonEmptyJsonString(inviteDoc, "id", "invite_id", "field_invite_id");
        signingLink = FirstNonEmptyJsonString(inviteDoc, "signing_link", "signingLink", "link");
    }

    await StoreSignNowTermsSentAsync(
        conn,
        job.JobId,
        documentId,
        inviteId,
        mapping.TemplateId,
        "sent",
        signingLink);
    await StoreSignNowWebhookResultAsync(conn, job.JobId, webhook);

    return ScheduleActionResult.Ok("terms", "SignNow terms sent to client.", new
    {
        documentId,
        inviteId,
        templateKey,
        templateId = mapping.TemplateId,
        templateName = mapping.TemplateName,
        sentTo = job.ClientEmail,
        webhookCreated = webhook.Success,
        webhookSubscriptionId = webhook.SubscriptionId,
        webhookError = webhook.Error
    });
}

static async Task<SignNowWebhookRegistrationResult> CreateSignNowDocumentWebhookAsync(HttpClient httpClient, string documentId, IConfiguration configuration)
{
    var callbackUrl = configuration["SIGNNOW_WEBHOOK_URL"];
    if (string.IsNullOrWhiteSpace(callbackUrl))
        callbackUrl = "https://automate-api-production.up.railway.app/api/integrations/signnow/webhook";

    var payload = new
    {
        @event = "document.complete",
        entity_id = documentId,
        action = "callback",
        attributes = new
        {
            callback = callbackUrl,
            use_tls_12 = true
        },
        secret_key = ""
    };

    try
    {
        var response = await httpClient.PostAsJsonAsync("https://api.signnow.com/api/v2/events", payload);
        var json = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
            return new SignNowWebhookRegistrationResult(false, "", json);

        var subscriptionId = "";
        if (!string.IsNullOrWhiteSpace(json))
            subscriptionId = FindJsonStringRecursive(JsonDocument.Parse(json).RootElement, "event_subscription_id", "subscription_id", "id");
        return new SignNowWebhookRegistrationResult(true, subscriptionId, "");
    }
    catch (Exception ex)
    {
        return new SignNowWebhookRegistrationResult(false, "", ex.Message);
    }
}

static async Task StoreSignNowWebhookResultAsync(NpgsqlConnection conn, Guid jobId, SignNowWebhookRegistrationResult result)
{
    const string sql = @"
UPDATE public.jobs_staging
SET signnow_webhook_subscription_id = NULLIF(@subscription_id, ''),
    signnow_webhook_status = @status,
    signnow_webhook_last_error = NULLIF(@last_error, ''),
    updated_at = NOW()
WHERE job_id = @job_id;";
    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("subscription_id", result.SubscriptionId ?? "");
    cmd.Parameters.AddWithValue("status", result.Success ? "active" : "failed");
    cmd.Parameters.AddWithValue("last_error", result.Error ?? "");
    await cmd.ExecuteNonQueryAsync();
}

static async Task<ScheduleActionResult> RefreshSignNowTermsStatusAsync(
    NpgsqlConnection conn,
    Guid jobId,
    IConfiguration configuration)
{
    const string sql = @"
SELECT signnow_document_id
FROM public.jobs_staging
WHERE job_id = @job_id
LIMIT 1;";

    string documentId;
    await using (var cmd = new NpgsqlCommand(sql, conn))
    {
        cmd.Parameters.AddWithValue("job_id", jobId);
        var result = await cmd.ExecuteScalarAsync();
        documentId = result?.ToString() ?? "";
    }

    if (string.IsNullOrWhiteSpace(documentId))
        return ScheduleActionResult.Failed("terms", "No SignNow document ID is stored for this job.");

    var account = await GetSignNowAccountAsync(conn, configuration);
    if (!account.Success)
        return ScheduleActionResult.Failed("terms", account.ErrorMessage ?? "SignNow is not connected.");

    using var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

    var response = await httpClient.GetAsync($"https://api.signnow.com/document/{Uri.EscapeDataString(documentId)}");
    var json = await response.Content.ReadAsStringAsync();
    if (!response.IsSuccessStatusCode)
    {
        await MarkTermsFailedAsync(conn, jobId, json);
        return ScheduleActionResult.Failed("terms", "SignNow status lookup failed: " + json);
    }

    var root = JsonDocument.Parse(json).RootElement;
    var status = FirstNonEmptyJsonString(root, "status", "state", "invite_status");
    if (string.IsNullOrWhiteSpace(status))
        status = FindJsonStringRecursive(root, "status", "state", "invite_status");
    var signed = LooksLikeSignNowCompleted(status);
    var signedAt = signed ? DateTime.UtcNow : (DateTime?)null;

    await StoreSignNowStatusAsync(conn, jobId, documentId, null, null, status, null, signed, signedAt);

    return ScheduleActionResult.Ok("terms", signed ? "SignNow terms are signed." : "SignNow terms status refreshed.", new
    {
        documentId,
        status,
        signed
    });
}

static async Task TryPrefillSignNowDocumentAsync(HttpClient httpClient, string documentId, ScheduleJobInput job)
{
    var fields = new Dictionary<string, string>
    {
        ["JobID"] = job.JobId.ToString(),
        ["Address of Property to be inspected"] = job.SiteAddress ?? "",
        ["Full name"] = job.ClientName ?? ""
    };

    var payload = new
    {
        fields = fields.Select(field => new
        {
            field_name = field.Key,
            prefilled_text = field.Value,
            value = field.Value
        }).ToArray(),
        prefill_texts = fields.Select(field => new
        {
            field_name = field.Key,
            prefilled_text = field.Value
        }).ToArray()
    };

    try
    {
        using var content = JsonContent.Create(payload);
        await httpClient.PutAsync($"https://api.signnow.com/document/{Uri.EscapeDataString(documentId)}", content);
    }
    catch
    {
        // Prefill support varies by template shape; sending still proceeds and SignNow returns validation errors if required fields block signing.
    }
}

static async Task StoreSignNowTermsSentAsync(
    NpgsqlConnection conn,
    Guid jobId,
    string documentId,
    string? inviteId,
    string templateId,
    string status,
    string? signingLink)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    terms_sent = true,
    terms_sent_at = COALESCE(terms_sent_at, NOW()),
    terms_retry_requested = false,
    terms_retry_requested_at = NULL,
    terms_last_attempt_at = NOW(),
    terms_last_error = NULL,
    signnow_document_id = @document_id,
    signnow_invite_id = @invite_id,
    signnow_template_id = @template_id,
    signnow_document_status = @status,
    signnow_last_checked_at = NOW(),
    signnow_signing_link = @signing_link,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("document_id", documentId);
    cmd.Parameters.AddWithValue("invite_id", (object?)inviteId ?? DBNull.Value);
    cmd.Parameters.AddWithValue("template_id", templateId);
    cmd.Parameters.AddWithValue("status", string.IsNullOrWhiteSpace(status) ? "sent" : status);
    cmd.Parameters.AddWithValue("signing_link", (object?)signingLink ?? DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
}

static async Task StoreSignNowStatusAsync(
    NpgsqlConnection conn,
    Guid jobId,
    string? documentId,
    string? inviteId,
    string? templateId,
    string? status,
    string? signingLink,
    bool signed,
    DateTime? signedAt)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    signnow_document_id = COALESCE(NULLIF(@document_id, ''), signnow_document_id),
    signnow_invite_id = COALESCE(NULLIF(@invite_id, ''), signnow_invite_id),
    signnow_template_id = COALESCE(NULLIF(@template_id, ''), signnow_template_id),
    signnow_document_status = COALESCE(NULLIF(@status, ''), signnow_document_status),
    signnow_last_checked_at = NOW(),
    signnow_signing_link = COALESCE(NULLIF(@signing_link, ''), signnow_signing_link),
    terms_signed = CASE WHEN @signed THEN true ELSE terms_signed END,
    terms_signed_at = CASE WHEN @signed THEN COALESCE(@signed_at, NOW()) ELSE terms_signed_at END,
    terms_last_error = CASE WHEN @signed THEN NULL ELSE terms_last_error END,
    terms_retry_requested = CASE WHEN @signed THEN false ELSE terms_retry_requested END,
    terms_retry_requested_at = CASE WHEN @signed THEN NULL ELSE terms_retry_requested_at END,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("document_id", documentId ?? "");
    cmd.Parameters.AddWithValue("invite_id", inviteId ?? "");
    cmd.Parameters.AddWithValue("template_id", templateId ?? "");
    cmd.Parameters.AddWithValue("status", status ?? "");
    cmd.Parameters.AddWithValue("signing_link", signingLink ?? "");
    cmd.Parameters.AddWithValue("signed", signed);
    cmd.Parameters.AddWithValue("signed_at", signedAt.HasValue ? signedAt.Value : DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
}

static async Task<ScheduleActionResult> CreateGoogleCalendarEventForJobAsync(
    NpgsqlConnection conn,
    ScheduleJobInput job,
    IConfiguration configuration)
{
    if (job.CalendarCreated)
        return ScheduleActionResult.Skip("calendar", "Calendar event was already marked created.");

    if (!job.JobDate.HasValue)
    {
        await MarkCalendarFailedAsync(conn, job.JobId, "Inspection date/time is missing.");
        return ScheduleActionResult.Failed("calendar", "Inspection date/time is missing.");
    }

    var account = await GetGoogleCalendarAccountAsync(conn, job.InspectorId, configuration);
    if (!account.Success)
    {
        await MarkCalendarFailedAsync(conn, job.JobId, account.ErrorMessage ?? "Google Calendar is not connected.");
        return ScheduleActionResult.Failed("calendar", account.ErrorMessage ?? "Google Calendar is not connected.");
    }

    using var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);

    var calendarId = string.IsNullOrWhiteSpace(account.CalendarId) ? "primary" : account.CalendarId;
    var privateProperty = Uri.EscapeDataString("automateJobId=" + job.JobId);
    var lookupUrl =
        $"https://www.googleapis.com/calendar/v3/calendars/{Uri.EscapeDataString(calendarId)}/events" +
        $"?privateExtendedProperty={privateProperty}&singleEvents=true&maxResults=1";
    var lookupResponse = await httpClient.GetAsync(lookupUrl);
    var lookupJson = await lookupResponse.Content.ReadAsStringAsync();
    if (lookupResponse.IsSuccessStatusCode)
    {
        var lookupDoc = JsonDocument.Parse(lookupJson).RootElement;
        if (lookupDoc.TryGetProperty("items", out var items) &&
            items.ValueKind == JsonValueKind.Array &&
            items.GetArrayLength() > 0)
        {
            var existingEventId = GetJsonString(items[0], "id");
            var existingInvoiceLines = await LoadXeroInvoiceLinesAsync(conn, job.JobId);
            var existingSummary = string.IsNullOrWhiteSpace(job.PrimaryService) ? "Inspection" : job.PrimaryService.Trim();
            if (!string.IsNullOrWhiteSpace(job.SiteAddress)) existingSummary += " - " + job.SiteAddress.Trim();
            var patchResponse = await httpClient.PatchAsJsonAsync(
                $"https://www.googleapis.com/calendar/v3/calendars/{Uri.EscapeDataString(calendarId)}/events/{Uri.EscapeDataString(existingEventId)}",
                new { summary = existingSummary, location = job.SiteAddress, description = BuildGoogleCalendarDescription(job, existingInvoiceLines) });
            var patchJson = await patchResponse.Content.ReadAsStringAsync();
            if (!patchResponse.IsSuccessStatusCode)
            {
                await MarkCalendarFailedAsync(conn, job.JobId, patchJson);
                return ScheduleActionResult.Failed("calendar", "Google Calendar event update failed: " + patchJson);
            }
            await MarkCalendarCreatedAsync(conn, job.JobId);
            return ScheduleActionResult.Ok("calendar", "Google Calendar event updated.", new
            {
                eventId = existingEventId,
                htmlLink = GetJsonString(items[0], "htmlLink")
            });
        }
    }

    var start = job.JobDate.Value;
    var duration = job.InspectionDurationMinutes <= 0 ? 60 : job.InspectionDurationMinutes;
    var end = start.AddMinutes(duration);
    var summary = string.IsNullOrWhiteSpace(job.PrimaryService)
        ? "Inspection"
        : job.PrimaryService.Trim();
    if (!string.IsNullOrWhiteSpace(job.SiteAddress))
        summary += " - " + job.SiteAddress.Trim();

    var invoiceLines = await LoadXeroInvoiceLinesAsync(conn, job.JobId);

    var eventPayload = new
    {
        summary,
        location = job.SiteAddress,
        description = BuildGoogleCalendarDescription(job, invoiceLines),
        start = new
        {
            dateTime = start.ToUniversalTime().ToString("O"),
            timeZone = string.IsNullOrWhiteSpace(job.Timezone) ? "Pacific/Auckland" : job.Timezone
        },
        end = new
        {
            dateTime = end.ToUniversalTime().ToString("O"),
            timeZone = string.IsNullOrWhiteSpace(job.Timezone) ? "Pacific/Auckland" : job.Timezone
        },
        extendedProperties = new
        {
            @private = new Dictionary<string, string>
            {
                ["automateJobId"] = job.JobId.ToString()
            }
        }
    };

    var createResponse = await httpClient.PostAsJsonAsync(
        $"https://www.googleapis.com/calendar/v3/calendars/{Uri.EscapeDataString(calendarId)}/events",
        eventPayload);
    var createJson = await createResponse.Content.ReadAsStringAsync();

    if (!createResponse.IsSuccessStatusCode)
    {
        await MarkCalendarFailedAsync(conn, job.JobId, createJson);
        return ScheduleActionResult.Failed("calendar", "Google Calendar event creation failed: " + createJson);
    }

    var created = JsonDocument.Parse(createJson).RootElement;
    await MarkCalendarCreatedAsync(conn, job.JobId);
    return ScheduleActionResult.Ok("calendar", "Google Calendar event created.", new
    {
        eventId = GetJsonString(created, "id"),
        htmlLink = GetJsonString(created, "htmlLink")
    });
}

static async Task<ScheduleActionResult> CancelGoogleCalendarEventForJobAsync(
    NpgsqlConnection conn,
    ScheduleJobInput job,
    IConfiguration configuration)
{
    if (!job.CalendarCreated) return ScheduleActionResult.Skip("calendar", "No active Calendar event was recorded.");
    var account = await GetGoogleCalendarAccountAsync(conn, job.InspectorId, configuration);
    if (!account.Success) return ScheduleActionResult.Failed("calendar", account.ErrorMessage ?? "Google Calendar is not connected.");
    using var httpClient = new HttpClient(); httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", account.AccessToken);
    var calendarId = string.IsNullOrWhiteSpace(account.CalendarId) ? "primary" : account.CalendarId;
    var privateProperty = Uri.EscapeDataString("automateJobId=" + job.JobId);
    var lookupUrl = $"https://www.googleapis.com/calendar/v3/calendars/{Uri.EscapeDataString(calendarId)}/events?privateExtendedProperty={privateProperty}&singleEvents=true&maxResults=10";
    var lookupResponse = await httpClient.GetAsync(lookupUrl); var lookupJson = await lookupResponse.Content.ReadAsStringAsync();
    if (!lookupResponse.IsSuccessStatusCode) return ScheduleActionResult.Failed("calendar", "Calendar lookup failed: " + lookupJson);
    var root = JsonDocument.Parse(lookupJson).RootElement;
    if (!root.TryGetProperty("items", out var items) || items.ValueKind != JsonValueKind.Array || items.GetArrayLength() == 0)
        return ScheduleActionResult.Skip("calendar", "Calendar event was already absent.");
    foreach (var item in items.EnumerateArray())
    {
        var id = GetJsonString(item, "id"); if (id.Length == 0) continue;
        var response = await httpClient.DeleteAsync($"https://www.googleapis.com/calendar/v3/calendars/{Uri.EscapeDataString(calendarId)}/events/{Uri.EscapeDataString(id)}");
        if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.Gone && response.StatusCode != System.Net.HttpStatusCode.NotFound)
            return ScheduleActionResult.Failed("calendar", "Calendar cancellation failed: " + await response.Content.ReadAsStringAsync());
    }
    await using var cmd = new NpgsqlCommand("UPDATE public.jobs_staging SET calendar_created=false,calendar_created_at=NULL WHERE job_id=@job", conn);
    cmd.Parameters.AddWithValue("job", job.JobId); await cmd.ExecuteNonQueryAsync();
    return ScheduleActionResult.Ok("calendar", "Calendar event cancelled.");
}

static string BuildGoogleCalendarDescription(ScheduleJobInput job, List<XeroInvoiceLineInput> invoiceLines)
{
    var sb = new StringBuilder();
    var addOns = BuildCalendarAddOns(job, invoiceLines);
    var additionalServices = JoinNonEmpty(job.Additional1, job.Additional2);

    AppendDescriptionLine(sb, "Client Name", job.ClientName);
    AppendDescriptionLine(sb, "Client Phone", job.ClientPhone);
    AppendDescriptionLine(sb, "Service", job.PrimaryService);
    AppendDescriptionLine(sb, "Service Add-ons", addOns);
    AppendDescriptionLine(sb, "Additional Service(s)", additionalServices);
    AppendDescriptionLine(sb, "Client Concerns", job.Notes);
    AppendDescriptionLine(sb, "Directions", job.Directions);
    AppendDescriptionLine(sb, "Access Instructions", job.Instructions);

    var agentDetails = JoinNonEmpty(job.AgentName, job.AgentPhone, job.AgentEmail);
    AppendDescriptionLine(sb, "Agent Details", agentDetails);
    AppendDescriptionLine(sb, "Access By", job.AccessBy);

    if (sb.Length > 0)
        sb.AppendLine();

    sb.AppendLine("Building Details:");
    AppendDescriptionLine(sb, "Age", job.AgeOfBuilding);
    AppendDescriptionLine(sb, "Levels", job.Stories);
    AppendDescriptionLine(sb, "Bedrooms", job.Bedrooms);
    AppendDescriptionLine(sb, "Bathrooms", job.Bathrooms);
    AppendDescriptionLine(sb, "Plaster Cladding?", job.Monolithic);
    AppendDescriptionLine(sb, "Foundation Space?", job.FoundationSpace);

    if (sb.Length > 0)
        sb.AppendLine();

    sb.Append("Created by 3D AutoMate.");
    return sb.ToString().Trim();
}

static string BuildCalendarAddOns(ScheduleJobInput job, List<XeroInvoiceLineInput> invoiceLines)
{
    var excluded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    AddIfNotEmpty(excluded, job.PrimaryService);
    AddIfNotEmpty(excluded, job.Additional1);
    AddIfNotEmpty(excluded, job.Additional2);

    var values = invoiceLines
        .Select(line => (line.Description ?? "").Trim())
        .Where(description => !string.IsNullOrWhiteSpace(description))
        .Where(description => !excluded.Contains(description))
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();

    return values.Length == 0 ? "" : string.Join(", ", values);
}

static void AddIfNotEmpty(HashSet<string> values, string? value)
{
    if (!string.IsNullOrWhiteSpace(value))
        values.Add(value.Trim());
}

static void AppendDescriptionLine(StringBuilder sb, string label, string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return;

    sb.Append(label);
    sb.Append(": ");
    sb.AppendLine(value.Trim());
}

static string JoinNonEmpty(params string?[] values)
{
    return string.Join(", ", values
        .Select(value => value?.Trim() ?? "")
        .Where(value => !string.IsNullOrWhiteSpace(value)));
}

static async Task<IntegrationAccountResult> GetMicrosoftMailAccountAsync(
    NpgsqlConnection conn,
    Guid inspectorId,
    IConfiguration configuration)
{
    return await GetOAuthAccountAsync(
        conn,
        inspectorId,
        "microsoft",
        configuration["MS_CLIENT_ID"],
        configuration["MS_CLIENT_SECRET"],
        "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        new Dictionary<string, string>
        {
            ["scope"] = "offline_access Mail.Send User.Read"
        },
        null);
}

static async Task<IntegrationAccountResult> GetGoogleCalendarAccountAsync(
    NpgsqlConnection conn,
    Guid inspectorId,
    IConfiguration configuration)
{
    return await GetOAuthAccountAsync(
        conn,
        inspectorId,
        "google",
        configuration["GOOGLE_CLIENT_ID"],
        configuration["GOOGLE_CLIENT_SECRET"],
        "https://oauth2.googleapis.com/token",
        null,
        "primary");
}

static async Task<IntegrationAccountResult> GetOAuthAccountAsync(
    NpgsqlConnection conn,
    Guid inspectorId,
    string provider,
    string? clientId,
    string? clientSecret,
    string tokenUrl,
    Dictionary<string, string>? extraRefreshFields,
    string? defaultTenantId)
{
    const string sql = @"
SELECT
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    external_tenant_id,
    status
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = @provider
LIMIT 1;";

    string? accessToken = null;
    string? refreshToken = null;
    DateTime? expiresAt = null;
    string? accountName = null;
    string? tenantId = null;
    string? status = null;

    await using (var cmd = new NpgsqlCommand(sql, conn))
    {
        cmd.Parameters.AddWithValue("inspector_id", inspectorId);
        cmd.Parameters.AddWithValue("provider", provider);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            accessToken = reader["access_token_encrypted"]?.ToString();
            refreshToken = reader["refresh_token_encrypted"]?.ToString();
            accountName = reader["external_account_email"]?.ToString();
            tenantId = reader["external_tenant_id"]?.ToString();
            status = reader["status"]?.ToString();
            if (reader["expires_at"] != DBNull.Value)
                expiresAt = Convert.ToDateTime(reader["expires_at"]);
        }
    }

    if (string.IsNullOrWhiteSpace(accessToken) ||
        !string.Equals(status, "connected", StringComparison.OrdinalIgnoreCase))
    {
        return IntegrationAccountResult.Failure($"{ToTitle(provider)} is not connected for this inspector.");
    }

    if (expiresAt.HasValue && expiresAt.Value > DateTime.UtcNow.AddMinutes(5))
        return IntegrationAccountResult.Ok(accessToken, refreshToken, tenantId ?? defaultTenantId, accountName);

    if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
        return IntegrationAccountResult.Failure($"{provider.ToUpperInvariant()} client ID and/or secret are missing.");

    if (string.IsNullOrWhiteSpace(refreshToken))
        return IntegrationAccountResult.Failure($"{ToTitle(provider)} access token expired and no refresh token is stored.");

    using var refreshClient = new HttpClient();
    var refreshFields = new Dictionary<string, string>
    {
        ["client_id"] = clientId,
        ["client_secret"] = clientSecret,
        ["refresh_token"] = refreshToken,
        ["grant_type"] = "refresh_token"
    };

    if (extraRefreshFields != null)
    {
        foreach (var field in extraRefreshFields)
            refreshFields[field.Key] = field.Value;
    }

    var refreshResponse = await refreshClient.PostAsync(tokenUrl, new FormUrlEncodedContent(refreshFields));
    var refreshJson = await refreshResponse.Content.ReadAsStringAsync();

    if (!refreshResponse.IsSuccessStatusCode)
        return IntegrationAccountResult.Failure($"{ToTitle(provider)} token refresh failed: {refreshJson}");

    var refreshDoc = JsonDocument.Parse(refreshJson).RootElement;
    accessToken = refreshDoc.GetProperty("access_token").GetString() ?? accessToken;
    refreshToken = refreshDoc.TryGetProperty("refresh_token", out var refreshedTokenProp)
        ? refreshedTokenProp.GetString() ?? refreshToken
        : refreshToken;

    var refreshedExpiresIn = refreshDoc.TryGetProperty("expires_in", out var refreshedExpiresInProp)
        ? refreshedExpiresInProp.GetInt32()
        : 3600;
    expiresAt = DateTime.UtcNow.AddSeconds(refreshedExpiresIn);

    const string updateSql = @"
UPDATE public.inspector_integrations
SET
    access_token_encrypted = @access_token,
    refresh_token_encrypted = @refresh_token,
    expires_at = @expires_at,
    updated_at = NOW()
WHERE inspector_id = @inspector_id
  AND provider = @provider;";

    await using var updateCmd = new NpgsqlCommand(updateSql, conn);
    updateCmd.Parameters.AddWithValue("access_token", accessToken);
    updateCmd.Parameters.AddWithValue("refresh_token", (object?)refreshToken ?? DBNull.Value);
    updateCmd.Parameters.AddWithValue("expires_at", expiresAt.Value);
    updateCmd.Parameters.AddWithValue("inspector_id", inspectorId);
    updateCmd.Parameters.AddWithValue("provider", provider);
    await updateCmd.ExecuteNonQueryAsync();

    return IntegrationAccountResult.Ok(accessToken, refreshToken, tenantId ?? defaultTenantId, accountName);
}

static async Task<ScheduleActionResult> SendMicrosoftMailAsync(
    HttpClient httpClient,
    string toEmail,
    string subject,
    string htmlBody)
{
    var emailBody = new
    {
        message = new
        {
            subject,
            body = new
            {
                contentType = "HTML",
                content = htmlBody
            },
            toRecipients = new[]
            {
                new
                {
                    emailAddress = new
                    {
                        address = toEmail
                    }
                }
            }
        }
    };

    var response = await httpClient.PostAsJsonAsync(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        emailBody);
    var responseText = await response.Content.ReadAsStringAsync();

    return response.IsSuccessStatusCode
        ? ScheduleActionResult.Ok("booking-email", "Email sent.")
        : ScheduleActionResult.Failed("booking-email", "Microsoft send mail failed: " + responseText);
}

static async Task MarkBookingEmailSentAsync(NpgsqlConnection conn, Guid jobId)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    booking_email_sent = true,
    booking_email_sent_at = NOW(),
    booking_email_retry_requested = false,
    booking_email_retry_requested_at = NULL,
    booking_email_last_attempt_at = NOW(),
    booking_email_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkBookingEmailFailedAsync(NpgsqlConnection conn, Guid jobId, string error)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    booking_email_last_attempt_at = NOW(),
    booking_email_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("error_message", error);
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkTermsFailedAsync(NpgsqlConnection conn, Guid jobId, string error)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    terms_last_attempt_at = NOW(),
    terms_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("error_message", error);
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkInvoiceSentAsync(NpgsqlConnection conn, Guid jobId)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    invoice_sent = true,
    invoice_sent_at = NOW(),
    invoice_retry_requested = false,
    invoice_retry_requested_at = NULL,
    invoice_last_attempt_at = NOW(),
    invoice_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkCalendarCreatedAsync(NpgsqlConnection conn, Guid jobId)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    calendar_created = true,
    calendar_created_at = NOW(),
    calendar_retry_requested = false,
    calendar_retry_requested_at = NULL,
    calendar_last_attempt_at = NOW(),
    calendar_last_error = NULL,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkCalendarFailedAsync(NpgsqlConnection conn, Guid jobId, string error)
{
    const string sql = @"
UPDATE public.jobs_staging
SET
    calendar_last_attempt_at = NOW(),
    calendar_last_error = @error_message,
    workflow_updated_at = NOW(),
    updated_at = NOW()
WHERE job_id = @job_id;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("error_message", error);
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkWorkflowActionSentAsync(NpgsqlConnection conn, Guid jobId, string actionKey)
{
    if (string.IsNullOrWhiteSpace(actionKey))
        return;

    const string sql = @"
UPDATE public.job_workflow_actions
SET
    status = 'sent',
    retry_requested = false,
    retry_requested_at = NULL,
    sent_at = NOW(),
    last_attempt_at = NOW(),
    last_error = NULL,
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_key = @action_key;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("action_key", actionKey);
    await cmd.ExecuteNonQueryAsync();
}

static string ToTitle(string value)
{
    if (string.IsNullOrWhiteSpace(value))
        return "";

    return char.ToUpperInvariant(value[0]) + value.Substring(1);
}

static string GetJsonString(JsonElement element, string propertyName)
{
    return element.TryGetProperty(propertyName, out var value) && value.ValueKind != JsonValueKind.Null
        ? value.ToString()
        : "";
}

static string BuildGoogleApiFriendlyError(string body)
{
    if (string.IsNullOrWhiteSpace(body))
        return "Google Calendar returned an empty error response.";

    try
    {
        var root = JsonDocument.Parse(body).RootElement;
        if (!root.TryGetProperty("error", out var error))
            return body;

        var message = GetJsonString(error, "message");
        var status = GetJsonString(error, "status");
        var service = FindJsonStringRecursive(error, "service");
        var consumer = FindJsonStringRecursive(error, "consumer");
        var reason = FindJsonStringRecursive(error, "reason");
        var activationUrl = FindJsonStringRecursive(error, "activationUrl");

        if (string.Equals(status, "PERMISSION_DENIED", StringComparison.OrdinalIgnoreCase) &&
            (string.Equals(reason, "SERVICE_DISABLED", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(reason, "accessNotConfigured", StringComparison.OrdinalIgnoreCase) ||
             message.Contains("has not been used", StringComparison.OrdinalIgnoreCase) ||
             message.Contains("is disabled", StringComparison.OrdinalIgnoreCase)))
        {
            var project = "";
            if (!string.IsNullOrWhiteSpace(consumer))
            {
                var match = Regex.Match(consumer, "projects/(\\d+)");
                if (match.Success)
                    project = match.Groups[1].Value;
            }

            var target = string.IsNullOrWhiteSpace(project) ? "the Google Cloud project used by AutoMate" : "Google Cloud project " + project;
            var apiName = string.IsNullOrWhiteSpace(service) ? "Google Calendar API" : service;

            return apiName + " is disabled for " + target + ". Enable Google Calendar API, wait a few minutes, then reconnect or refresh calendars."
                + (string.IsNullOrWhiteSpace(activationUrl) ? "" : " Enable it here: " + activationUrl);
        }

        return string.IsNullOrWhiteSpace(message) ? body : message;
    }
    catch
    {
        return body;
    }
}

static string FirstNonEmptyJsonString(JsonElement element, params string[] propertyNames)
{
    foreach (var propertyName in propertyNames)
    {
        var value = GetJsonString(element, propertyName);
        if (!string.IsNullOrWhiteSpace(value))
            return value;
    }

    return "";
}

static string FindJsonStringRecursive(JsonElement element, params string[] propertyNames)
{
    if (element.ValueKind == JsonValueKind.Object)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (propertyNames.Any(name => string.Equals(name, property.Name, StringComparison.OrdinalIgnoreCase)) &&
                property.Value.ValueKind != JsonValueKind.Null &&
                property.Value.ValueKind != JsonValueKind.Object &&
                property.Value.ValueKind != JsonValueKind.Array)
            {
                return property.Value.ToString();
            }

            var nested = FindJsonStringRecursive(property.Value, propertyNames);
            if (!string.IsNullOrWhiteSpace(nested))
                return nested;
        }
    }
    else if (element.ValueKind == JsonValueKind.Array)
    {
        foreach (var item in element.EnumerateArray())
        {
            var nested = FindJsonStringRecursive(item, propertyNames);
            if (!string.IsNullOrWhiteSpace(nested))
                return nested;
        }
    }

    return "";
}

static async Task<SignNowTemplateLookupResult> LookupSignNowTemplatesAsync(HttpClient httpClient)
{
    var signNowTermsParentId = "274f64b07a6e4d3bb14a0e7f6b51086d983d4240";
    var templates = new List<SignNowTemplateResult>();
    var diagnostics = new List<object>();
    var successfulEndpointCount = 0;
    string lastJson = "";
    string lastEndpoint = "";
    int lastStatusCode = 0;

    const int pageSize = 100;
    const int maxActiveFolderPages = 2;
    for (var page = 1; page <= maxActiveFolderPages; page++)
    {
        var endpoint = new SignNowTemplateEndpoint(
            "terms_parent_documentsv2",
            $"https://api.signnow.com/user/documentsv2?parent_id={Uri.EscapeDataString(signNowTermsParentId)}&page={page}&per_page={pageSize}");

        lastEndpoint = endpoint.Url;
        var lastResponse = await httpClient.GetAsync(endpoint.Url);
        lastJson = await lastResponse.Content.ReadAsStringAsync();
        lastStatusCode = (int)lastResponse.StatusCode;

        if (lastResponse.IsSuccessStatusCode)
        {
            successfulEndpointCount++;
            var root = JsonDocument.Parse(lastJson).RootElement;
            var endpointTemplates = ExtractSignNowTemplates(root, endpoint.Url, endpoint.SourceType);
            var rawCount = CountSignNowArrayItems(root);
            templates.AddRange(endpointTemplates);
            diagnostics.Add(new
            {
                endpoint = endpoint.Url,
                sourceType = endpoint.SourceType,
                statusCode = lastStatusCode,
                success = true,
                count = endpointTemplates.Count,
                rawCount
            });

            if (rawCount < pageSize)
                break;
        }
        else
        {
            diagnostics.Add(new
            {
                endpoint = endpoint.Url,
                sourceType = endpoint.SourceType,
                statusCode = lastStatusCode,
                success = false,
                count = 0,
                response = TruncateForDiagnostics(lastJson, 1000)
            });

            break;
        }
    }

    if (templates.Count == 0)
    {
        var fallback = await LookupSignNowTemplateFallbacksAsync(httpClient);
        templates.AddRange(fallback.Templates);
        diagnostics.AddRange(fallback.Diagnostics);
        successfulEndpointCount += fallback.SuccessfulEndpointCount;
        lastEndpoint = fallback.LastEndpoint;
        lastStatusCode = fallback.LastStatusCode;
        lastJson = fallback.LastResponse;
    }

    return new SignNowTemplateLookupResult(
        GroupSignNowTemplatesByName(templates),
        diagnostics.ToArray(),
        successfulEndpointCount,
        lastEndpoint,
        lastStatusCode,
        TruncateForDiagnostics(lastJson, 2000));
}


static async Task<SignNowTemplateLookupResult> LookupSignNowTemplateFallbacksAsync(HttpClient httpClient)
{
    var endpoints = new[]
    {
        new SignNowTemplateEndpoint("template", "https://api.signnow.com/template"),
        new SignNowTemplateEndpoint("user_documents_template", "https://api.signnow.com/user/documents?type=template"),
        new SignNowTemplateEndpoint("user_documentsv2_template", "https://api.signnow.com/user/documentsv2?type=template")
    };

    var templates = new List<SignNowTemplateResult>();
    var diagnostics = new List<object>();
    var successfulEndpointCount = 0;
    string lastJson = "";
    string lastEndpoint = "";
    int lastStatusCode = 0;

    foreach (var endpoint in endpoints)
    {
        lastEndpoint = endpoint.Url;
        var response = await httpClient.GetAsync(endpoint.Url);
        lastJson = await response.Content.ReadAsStringAsync();
        lastStatusCode = (int)response.StatusCode;

        if (response.IsSuccessStatusCode)
        {
            successfulEndpointCount++;
            var root = JsonDocument.Parse(lastJson).RootElement;
            var endpointTemplates = ExtractSignNowTemplates(root, endpoint.Url, endpoint.SourceType);
            templates.AddRange(endpointTemplates);
            diagnostics.Add(new
            {
                endpoint = endpoint.Url,
                sourceType = endpoint.SourceType,
                statusCode = lastStatusCode,
                success = true,
                count = endpointTemplates.Count,
                rawCount = CountSignNowArrayItems(root)
            });
        }
        else
        {
            diagnostics.Add(new
            {
                endpoint = endpoint.Url,
                sourceType = endpoint.SourceType,
                statusCode = lastStatusCode,
                success = false,
                count = 0,
                rawCount = 0,
                response = TruncateForDiagnostics(lastJson, 1000)
            });
        }
    }

    return new SignNowTemplateLookupResult(
        templates,
        diagnostics.ToArray(),
        successfulEndpointCount,
        lastEndpoint,
        lastStatusCode,
        TruncateForDiagnostics(lastJson, 2000));
}

static int CountSignNowArrayItems(JsonElement root)
{
    if (root.ValueKind == JsonValueKind.Array)
        return root.GetArrayLength();

    if (root.ValueKind == JsonValueKind.Object)
    {
        foreach (var name in new[] { "templates", "data", "documents" })
        {
            if (root.TryGetProperty(name, out var array) && array.ValueKind == JsonValueKind.Array)
                return array.GetArrayLength();
        }
    }

    return 0;
}

static List<SignNowTemplateResult> ExtractSignNowTemplates(JsonElement root, string sourceEndpoint, string sourceType)
{
    var templates = new List<SignNowTemplateResult>();
    var arrays = new List<JsonElement>();

    if (root.ValueKind == JsonValueKind.Array)
        arrays.Add(root);
    else if (root.ValueKind == JsonValueKind.Object)
    {
        foreach (var name in new[] { "templates", "data", "documents" })
        {
            if (root.TryGetProperty(name, out var array) && array.ValueKind == JsonValueKind.Array)
                arrays.Add(array);
        }
    }

    foreach (var array in arrays)
    {
        foreach (var item in array.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object)
                continue;

            if (sourceType.StartsWith("terms_parent_", StringComparison.OrdinalIgnoreCase) &&
                item.TryGetProperty("template", out var templateFlag) &&
                templateFlag.ValueKind == JsonValueKind.False)
            {
                continue;
            }

            var id = FirstNonEmptyJsonString(item, "id", "template_id", "document_id", "unique_id");
            if (string.IsNullOrWhiteSpace(id))
                continue;

            templates.Add(new SignNowTemplateResult(
                id,
                FirstNonEmptyJsonString(item, "name", "document_name", "template_name"),
                FirstNonEmptyJsonString(item, "updated", "updated_at", "last_updated"),
                sourceEndpoint,
                sourceType));
        }
    }

    return templates
        .GroupBy(template => template.Id, StringComparer.OrdinalIgnoreCase)
        .Select(group => group.First())
        .OrderBy(template => template.Name)
        .ToList();
}

static List<SignNowTemplateResult> GroupSignNowTemplatesByName(IEnumerable<SignNowTemplateResult> templates)
{
    return templates
        .Where(template => !string.IsNullOrWhiteSpace(template.Id))
        .Where(template => !LooksLikePropertySpecificSignNowTemplate(template.Name))
        .GroupBy(template => NormalizeSignNowTemplateName(template.Name), StringComparer.OrdinalIgnoreCase)
        .Select(group => group
            .OrderByDescending(template => ParseSignNowUpdatedAt(template.UpdatedAt))
            .ThenBy(template => template.Id)
            .First())
        .OrderBy(template => template.Name)
        .ToList();
}

static string NormalizeSignNowTemplateName(string? name)
{
    if (string.IsNullOrWhiteSpace(name))
        return "";

    var normalized = Regex.Replace(name.Trim(), "\\s+", " ");
    if (string.Equals(normalized, "Terms and Conditions - Pro-Spect", StringComparison.OrdinalIgnoreCase))
        return "Pro-Spect Terms and Conditions";

    return normalized;
}

static long ParseSignNowUpdatedAt(string? value)
{
    return long.TryParse(value, out var parsed) ? parsed : 0;
}

static bool LooksLikePropertySpecificSignNowTemplate(string? name)
{
    if (string.IsNullOrWhiteSpace(name))
        return false;

    var trimmed = name.Trim();
    var separatorIndex = trimmed.IndexOf(" - ", StringComparison.Ordinal);
    if (separatorIndex <= 0)
        return false;

    var prefix = trimmed.Substring(0, separatorIndex).Trim();
    if (prefix.Length == 0 || !char.IsDigit(prefix[0]))
        return false;

    return Regex.IsMatch(prefix, "[A-Za-z]") && Regex.IsMatch(prefix, "\\d");
}

static string ResolveSignNowTermsTemplateKey(string? value)
{
    var normalized = NormalizeServiceTypeKey(value);

    if (normalized.StartsWith("building_inspection_", StringComparison.OrdinalIgnoreCase))
        return "building_inspection";

    return normalized switch
    {
        "meth_field_composite" => "meth_testing",
        "meth_lab_composite" => "meth_testing",
        "meth_individual_sample" => "meth_testing",
        _ => normalized
    };
}

static string TruncateForDiagnostics(string? value, int maxLength)
{
    if (string.IsNullOrEmpty(value) || value.Length <= maxLength)
        return value ?? "";

    return value.Substring(0, maxLength) + "...";
}

static string ExtractSignNowSignerRole(JsonElement document)
{
    var role = FindJsonStringRecursive(document, "role", "role_name");
    if (!string.IsNullOrWhiteSpace(role))
        return role;

    return "";
}

static string ExtractSignNowSignerRoleId(JsonElement? document)
{
    if (!document.HasValue)
        return "";

    return FindJsonStringRecursive(document.Value, "role_id", "roleId");
}

static bool LooksLikeSignNowCompleted(string? status)
{
    if (string.IsNullOrWhiteSpace(status))
        return false;

    var value = status.Trim().ToLowerInvariant();
    return value.Contains("complete") ||
        value.Contains("completed") ||
        value.Contains("signed") ||
        value.Contains("fulfilled");
}

static string BuildSignNowDocumentName(ScheduleJobInput job)
{
    var name = string.IsNullOrWhiteSpace(job.JobName) ? "Inspection terms" : job.JobName.Trim();
    if (!string.IsNullOrWhiteSpace(job.SiteAddress))
        name += " - " + job.SiteAddress.Trim();

    return name.Length <= 180 ? name : name.Substring(0, 180);
}

static async Task<Guid> FindJobIdBySignNowDocumentAsync(NpgsqlConnection conn, string documentId)
{
    const string sql = @"
SELECT job_id
FROM public.jobs_staging
WHERE signnow_document_id = @document_id
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("document_id", documentId);

    var result = await cmd.ExecuteScalarAsync();
    return Guid.TryParse(result?.ToString(), out var jobId) ? jobId : Guid.Empty;
}

static string BuildPersonName(string? firstName, string? lastName)
{
    return string.Join(" ", new[] { firstName, lastName }
        .Where(part => !string.IsNullOrWhiteSpace(part))
        .Select(part => part!.Trim()));
}

static string BuildFallbackInvoiceDescription(string? primaryService, string? siteAddress)
{
    var service = string.IsNullOrWhiteSpace(primaryService) ? "Inspection service" : primaryService.Trim();
    return string.IsNullOrWhiteSpace(siteAddress)
        ? service
        : service + " - " + siteAddress.Trim();
}

static async Task EnsureMappingTablesAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.inspector_field_mappings
(
    inspector_field_mapping_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_id uuid NOT NULL,
    canonical_field_name text NOT NULL,
    threed_column_name text NOT NULL,
    threed_label text NULL,
    source_table_name text NULL,
    source_list_name text NULL,
    invoice_item_id text NULL,
    invoice_item_name text NULL,
    pricing_affects boolean NOT NULL DEFAULT false,
    v1_enabled boolean NOT NULL DEFAULT true,
    service_scope text NULL,
    notes text NULL,
    is_confirmed boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_inspector_field_mappings_field UNIQUE (inspector_id, canonical_field_name)
);

CREATE INDEX IF NOT EXISTS idx_inspector_field_mappings_inspector_id
ON public.inspector_field_mappings(inspector_id);

CREATE TABLE IF NOT EXISTS public.inspector_service_catalog
(
    service_catalog_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_id uuid NOT NULL,
    catalog_item_key text NOT NULL,
    list_item_id text NULL,
    list_item_name text NULL,
    list_name text NULL,
    invoice_item_id text NULL,
    invoice_item_name text NULL,
    unit_price numeric(10,2) NULL,
    is_active boolean NOT NULL DEFAULT true,
    canonical_service_type text NOT NULL DEFAULT 'other',
    booking_template_key text NOT NULL DEFAULT 'general_booking',
    pricing_affects boolean NOT NULL DEFAULT true,
    booking_email_required boolean NOT NULL DEFAULT true,
    terms_required boolean NOT NULL DEFAULT false,
    invoice_required boolean NOT NULL DEFAULT true,
    calendar_required boolean NOT NULL DEFAULT true,
    report_required boolean NOT NULL DEFAULT true,
    pricing_authority text NOT NULL DEFAULT 'THREED tblItem',
    raw_payload_json jsonb NULL,
    last_synced_at timestamptz NOT NULL DEFAULT NOW(),
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_inspector_service_catalog_key UNIQUE (inspector_id, catalog_item_key)
);

CREATE INDEX IF NOT EXISTS idx_inspector_service_catalog_inspector_id
ON public.inspector_service_catalog(inspector_id);

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS canonical_service_type text NOT NULL DEFAULT 'other';

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS booking_template_key text NOT NULL DEFAULT 'general_booking';

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS pricing_affects boolean NOT NULL DEFAULT true;

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS booking_email_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS terms_required boolean NOT NULL DEFAULT false;

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS invoice_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS calendar_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.inspector_service_catalog
ADD COLUMN IF NOT EXISTS report_required boolean NOT NULL DEFAULT true;

CREATE TABLE IF NOT EXISTS public.mapping_discovery_syncs
(
    mapping_discovery_sync_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_id uuid NOT NULL,
    connector_version text NULL,
    source_instance text NULL,
    field_mapping_count integer NOT NULL DEFAULT 0,
    service_catalog_count integer NOT NULL DEFAULT 0,
    raw_payload_json jsonb NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mapping_discovery_syncs_inspector_id
ON public.mapping_discovery_syncs(inspector_id);
";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task UpsertMappingFieldAsync(
    NpgsqlConnection conn,
    NpgsqlTransaction tx,
    Guid inspectorId,
    MappingFieldInput mapping,
    bool isConfirmed)
{
    const string sql = @"
INSERT INTO public.inspector_field_mappings
(
    inspector_id,
    canonical_field_name,
    threed_column_name,
    threed_label,
    source_table_name,
    source_list_name,
    invoice_item_id,
    invoice_item_name,
    pricing_affects,
    v1_enabled,
    service_scope,
    notes,
    is_confirmed,
    updated_at
)
VALUES
(
    @inspector_id,
    @canonical_field_name,
    @threed_column_name,
    @threed_label,
    @source_table_name,
    @source_list_name,
    @invoice_item_id,
    @invoice_item_name,
    @pricing_affects,
    @v1_enabled,
    @service_scope,
    @notes,
    @is_confirmed,
    NOW()
)
ON CONFLICT (inspector_id, canonical_field_name)
DO UPDATE SET
    threed_column_name = EXCLUDED.threed_column_name,
    threed_label = EXCLUDED.threed_label,
    source_table_name = EXCLUDED.source_table_name,
    source_list_name = EXCLUDED.source_list_name,
    invoice_item_id = EXCLUDED.invoice_item_id,
    invoice_item_name = EXCLUDED.invoice_item_name,
    pricing_affects = EXCLUDED.pricing_affects,
    v1_enabled = EXCLUDED.v1_enabled,
    service_scope = EXCLUDED.service_scope,
    notes = EXCLUDED.notes,
    is_confirmed = public.inspector_field_mappings.is_confirmed OR EXCLUDED.is_confirmed,
    updated_at = NOW();";

    await using var cmd = new NpgsqlCommand(sql, conn, tx);
    cmd.Parameters.AddWithValue("inspector_id", inspectorId);
    cmd.Parameters.AddWithValue("canonical_field_name", mapping.CanonicalFieldName ?? "");
    cmd.Parameters.AddWithValue("threed_column_name", mapping.ThreedColumnName ?? "");
    cmd.Parameters.AddWithValue("threed_label", mapping.ThreedLabel ?? "");
    cmd.Parameters.AddWithValue("source_table_name", mapping.SourceTableName ?? "dbo.tblListItem");
    cmd.Parameters.AddWithValue("source_list_name", mapping.SourceListName ?? "");
    cmd.Parameters.AddWithValue("invoice_item_id", mapping.InvoiceItemId ?? "");
    cmd.Parameters.AddWithValue("invoice_item_name", mapping.InvoiceItemName ?? "");
    cmd.Parameters.AddWithValue("pricing_affects", mapping.CanAffectPricing);
    cmd.Parameters.AddWithValue("v1_enabled", mapping.V1Enabled);
    cmd.Parameters.AddWithValue("service_scope", mapping.ServiceScope ?? "");
    cmd.Parameters.AddWithValue("notes", mapping.Notes ?? "");
    cmd.Parameters.AddWithValue("is_confirmed", isConfirmed);
    await cmd.ExecuteNonQueryAsync();
}

static async Task UpsertServiceCatalogItemAsync(
    NpgsqlConnection conn,
    NpgsqlTransaction tx,
    Guid inspectorId,
    ServiceCatalogItemInput item)
{
    const string sql = @"
INSERT INTO public.inspector_service_catalog
(
    inspector_id,
    catalog_item_key,
    list_item_id,
    list_item_name,
    list_name,
    invoice_item_id,
    invoice_item_name,
    unit_price,
    is_active,
    canonical_service_type,
    booking_template_key,
    pricing_affects,
    booking_email_required,
    terms_required,
    invoice_required,
    calendar_required,
    report_required,
    pricing_authority,
    raw_payload_json,
    last_synced_at,
    updated_at
)
VALUES
(
    @inspector_id,
    @catalog_item_key,
    @list_item_id,
    @list_item_name,
    @list_name,
    @invoice_item_id,
    @invoice_item_name,
    @unit_price,
    @is_active,
    @canonical_service_type,
    @booking_template_key,
    @pricing_affects,
    @booking_email_required,
    @terms_required,
    @invoice_required,
    @calendar_required,
    @report_required,
    'THREED tblItem',
    CAST(@raw_payload_json AS jsonb),
    NOW(),
    NOW()
)
ON CONFLICT (inspector_id, catalog_item_key)
DO UPDATE SET
    list_item_id = EXCLUDED.list_item_id,
    list_item_name = EXCLUDED.list_item_name,
    list_name = EXCLUDED.list_name,
    invoice_item_id = EXCLUDED.invoice_item_id,
    invoice_item_name = EXCLUDED.invoice_item_name,
    unit_price = EXCLUDED.unit_price,
    is_active = EXCLUDED.is_active,
    canonical_service_type = EXCLUDED.canonical_service_type,
    booking_template_key = EXCLUDED.booking_template_key,
    pricing_affects = EXCLUDED.pricing_affects,
    booking_email_required = EXCLUDED.booking_email_required,
    terms_required = EXCLUDED.terms_required,
    invoice_required = EXCLUDED.invoice_required,
    calendar_required = EXCLUDED.calendar_required,
    report_required = EXCLUDED.report_required,
    pricing_authority = EXCLUDED.pricing_authority,
    raw_payload_json = EXCLUDED.raw_payload_json,
    last_synced_at = NOW(),
    updated_at = NOW();";

    var key = !string.IsNullOrWhiteSpace(item.CatalogItemKey)
        ? item.CatalogItemKey
        : $"{item.ListName}|{item.ListItemId}|{item.InvoiceItemId}|{item.ListItemName}|{item.InvoiceItemName}";

    await using var cmd = new NpgsqlCommand(sql, conn, tx);
    cmd.Parameters.AddWithValue("inspector_id", inspectorId);
    cmd.Parameters.AddWithValue("catalog_item_key", key);
    cmd.Parameters.AddWithValue("list_item_id", item.ListItemId ?? "");
    cmd.Parameters.AddWithValue("list_item_name", item.ListItemName ?? "");
    cmd.Parameters.AddWithValue("list_name", item.ListName ?? "");
    cmd.Parameters.AddWithValue("invoice_item_id", item.InvoiceItemId ?? "");
    cmd.Parameters.AddWithValue("invoice_item_name", item.InvoiceItemName ?? "");
    cmd.Parameters.AddWithValue("unit_price", item.UnitPrice.HasValue ? item.UnitPrice.Value : (object)DBNull.Value);
    cmd.Parameters.AddWithValue("is_active", item.IsActive);
    cmd.Parameters.AddWithValue("canonical_service_type", string.IsNullOrWhiteSpace(item.CanonicalServiceType) ? "other" : item.CanonicalServiceType);
    cmd.Parameters.AddWithValue("booking_template_key", string.IsNullOrWhiteSpace(item.BookingTemplateKey) ? "general_booking" : item.BookingTemplateKey);
    cmd.Parameters.AddWithValue("pricing_affects", item.PricingAffects);
    cmd.Parameters.AddWithValue("booking_email_required", item.BookingEmailRequired);
    cmd.Parameters.AddWithValue("terms_required", item.TermsRequired ?? ShouldRequireTermsForService(item));
    cmd.Parameters.AddWithValue("invoice_required", item.InvoiceRequired);
    cmd.Parameters.AddWithValue("calendar_required", item.CalendarRequired);
    cmd.Parameters.AddWithValue("report_required", item.ReportRequired);
    cmd.Parameters.AddWithValue("raw_payload_json", JsonSerializer.Serialize(item));
    await cmd.ExecuteNonQueryAsync();
}

static string NormalizeServiceTypeKey(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return "general_booking";

    var normalized = value.Trim().ToLowerInvariant().Replace(" ", "_").Replace("-", "_");

    return normalized switch
    {
        "investigation" => "building_investigation",
        "pre_purchase" => "building_inspection",
        "pre_sale" => "building_inspection",
        "meth_test" => "meth_testing",
        "additional_outbuilding" => "garage_outbuilding",
        "council_file_review" => "property_file_review",
        _ => normalized
    };
}

static string BuildAddOnPlaceholderKey(string serviceTypeKey)
{
    return "HAS_" + NormalizeServiceTypeKey(serviceTypeKey).ToUpperInvariant();
}

static string NormalizeEmailSenderMode(string? mode)
{
    if (string.IsNullOrWhiteSpace(mode))
        return "microsoft";

    var normalized = mode.Trim().ToLowerInvariant()
        .Replace(" ", "-")
        .Replace("_", "-");

    return normalized switch
    {
        "microsoft" or "microsoft-test" or "microsoft-test-mode" or "test" => "microsoft",
        "threed" or "3d" or "threed-smtp" or "3d-smtp" or "smtp-threed" => "threed-smtp",
        "manual" or "manual-smtp" or "smtp" => "manual-smtp",
        _ => "microsoft"
    };
}

static async Task<string> LoadAuthenticatedAutomationActorAsync(NpgsqlConnection conn, Guid tenantId, Guid inspectorId)
{
    await using var command = new NpgsqlCommand("SELECT COALESCE(NULLIF(inspector_name,''),inspector_id::text) FROM public.inspectors WHERE inspector_id=@id AND tenant_id=@tenant LIMIT 1", conn);
    command.Parameters.AddWithValue("id", inspectorId); command.Parameters.AddWithValue("tenant", tenantId);
    var storedActor = Convert.ToString(await command.ExecuteScalarAsync());
    return AutoMateApi.AutomationActorSupport.Resolve(storedActor, inspectorId);
}

static Guid GetAuthenticatedInspectorId(HttpContext context)
{
    var raw = context.Request.Headers["X-AutoMate-Inspector-ID"].FirstOrDefault();
    if (!Guid.TryParse(raw, out var inspectorId) || inspectorId == Guid.Empty)
        throw new AuthenticatedAutomationIdentityException("A valid authenticated AutoMate/THREED user is required.");
    return inspectorId;
}

static bool IsSmtpEmailSenderMode(string? mode)
{
    var normalized = NormalizeEmailSenderMode(mode);
    return normalized == "threed-smtp" || normalized == "manual-smtp";
}

static string GetEmailSenderModeLabel(string? mode)
{
    return NormalizeEmailSenderMode(mode) switch
    {
        "threed-smtp" => "THREED SMTP",
        "manual-smtp" => "Manual SMTP",
        _ => "Microsoft Test Mode"
    };
}

static string NormalizeTemplateType(string? templateType)
{
    return string.IsNullOrWhiteSpace(templateType)
        ? "booking-email"
        : templateType.Trim().ToLowerInvariant().Replace("_", "-");
}

static async Task EnsureEmailTemplatesTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.email_templates
(
    template_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    inspector_id uuid NOT NULL,
    template_type text NOT NULL DEFAULT 'booking-email',
    service_type_key text NOT NULL DEFAULT 'general_booking',
    email_type text NOT NULL DEFAULT 'transactional',
    name text NOT NULL DEFAULT '',
    subject text NOT NULL DEFAULT '',
    html_body text NOT NULL DEFAULT '',
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_email_templates_inspector_type_service UNIQUE (inspector_id, template_type, service_type_key)
);

CREATE INDEX IF NOT EXISTS idx_email_templates_inspector
ON public.email_templates(inspector_id);";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task<EmailTemplateResult> LoadEmailTemplateAsync(
    NpgsqlConnection conn,
    Guid inspectorId,
    string templateType,
    string serviceTypeKey)
{
    await EnsureEmailTemplatesTableAsync(conn);

    const string sql = @"
SELECT
    template_id,
    inspector_id,
    template_type,
    service_type_key,
    name,
    subject,
    html_body,
    is_active,
    created_at,
    updated_at
FROM public.email_templates
WHERE inspector_id = @inspector_id
  AND template_type = @template_type
  AND service_type_key = @service_type_key
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("inspector_id", inspectorId);
    cmd.Parameters.AddWithValue("template_type", NormalizeTemplateType(templateType));
    cmd.Parameters.AddWithValue("service_type_key", NormalizeServiceTypeKey(serviceTypeKey));

    await using var reader = await cmd.ExecuteReaderAsync();
    if (await reader.ReadAsync())
    {
        return new EmailTemplateResult(
            (Guid)reader["template_id"],
            (Guid)reader["inspector_id"],
            reader["template_type"]?.ToString() ?? "booking-email",
            reader["service_type_key"]?.ToString() ?? "general_booking",
            reader["name"]?.ToString() ?? "",
            reader["subject"]?.ToString() ?? "",
            reader["html_body"]?.ToString() ?? "",
            reader["is_active"] != DBNull.Value && Convert.ToBoolean(reader["is_active"]),
            reader["created_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["created_at"]).ToString("O"),
            reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O"));
    }

    return new EmailTemplateResult(
        Guid.Empty,
        inspectorId,
        NormalizeTemplateType(templateType),
        NormalizeServiceTypeKey(serviceTypeKey),
        GetEmailTemplateServiceLabel(serviceTypeKey),
        BuildDefaultBookingTemplateSubject(serviceTypeKey),
        BuildDefaultBookingTemplateHtml(),
        true,
        "",
        "");
}

static async Task<EmailTemplateResult> UpsertEmailTemplateAsync(NpgsqlConnection conn, EmailTemplateResult template)
{
    await EnsureEmailTemplatesTableAsync(conn);

    const string sql = @"
INSERT INTO public.email_templates
(
    inspector_id,
    template_type,
    service_type_key,
    email_type,
    name,
    subject,
    html_body,
    is_active,
    created_at,
    updated_at
)
VALUES
(
    @inspector_id,
    @template_type,
    @service_type_key,
    'transactional',
    @name,
    @subject,
    @html_body,
    @is_active,
    NOW(),
    NOW()
)
ON CONFLICT (inspector_id, template_type, service_type_key)
DO UPDATE SET
    name = EXCLUDED.name,
    subject = EXCLUDED.subject,
    html_body = EXCLUDED.html_body,
    is_active = EXCLUDED.is_active,
    updated_at = NOW()
RETURNING template_id, created_at, updated_at;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("inspector_id", template.InspectorId);
    cmd.Parameters.AddWithValue("template_type", NormalizeTemplateType(template.TemplateType));
    cmd.Parameters.AddWithValue("service_type_key", NormalizeServiceTypeKey(template.ServiceTypeKey));
    cmd.Parameters.AddWithValue("name", template.Name ?? "");
    cmd.Parameters.AddWithValue("subject", template.Subject ?? "");
    cmd.Parameters.AddWithValue("html_body", CleanEditorHtml(template.HtmlBody ?? ""));
    cmd.Parameters.AddWithValue("is_active", template.IsActive);

    await using var reader = await cmd.ExecuteReaderAsync();
    if (await reader.ReadAsync())
    {
        return template with
        {
            TemplateId = (Guid)reader["template_id"],
            CreatedAt = reader["created_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["created_at"]).ToString("O"),
            UpdatedAt = reader["updated_at"] == DBNull.Value ? "" : Convert.ToDateTime(reader["updated_at"]).ToString("O")
        };
    }

    return template;
}

static string GetEmailTemplateServiceLabel(string? serviceTypeKey)
{
    var normalized = NormalizeServiceTypeKey(serviceTypeKey);
    var serviceType = GetEmailTemplateServiceTypeRecords()
        .FirstOrDefault(service => string.Equals(service.Key, normalized, StringComparison.OrdinalIgnoreCase));
    return serviceType?.Label ?? ToTitle(normalized.Replace("_", " "));
}

static string BuildDefaultBookingTemplateSubject(string? serviceTypeKey)
{
    return "Booking confirmation - {{PRIMARY_SERVICE}}";
}

static string BuildDefaultBookingTemplateHtml()
{
    return
        "<div style=\"font-family:Segoe UI,Arial,sans-serif;font-size:15px;line-height:1.5;color:#1f2937;\">" +
        "<p>Hi {{CLIENT_NAME}},</p>" +
        "<p>Your booking has been scheduled. The confirmed details are below.</p>" +
        "<table style=\"border-collapse:collapse;margin:16px 0;\">" +
        "<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Service</td><td>{{PRIMARY_SERVICE}}</td></tr>" +
        "<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Address</td><td>{{PROPERTY_ADDRESS}}</td></tr>" +
        "<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Date/time</td><td>{{INSPECTION_DATE}} {{INSPECTION_TIME}}</td></tr>" +
        "<tr><td style=\"font-weight:600;padding:4px 16px 4px 0;\">Inspector</td><td>{{INSPECTOR_NAME}}</td></tr>" +
        "</table>" +
        "<p>If any of these details need to change, please contact {{COMPANY_NAME}}.</p>" +
        "<p>Regards,<br>{{COMPANY_NAME}}</p>" +
        "</div>";
}

static async Task<(string Contact1, string Contact2)> LoadBasicContactLabelsAsync(NpgsqlConnection conn, Guid tenantId)
{
    const string sql = @"SELECT COALESCE(NULLIF(contact1_role_label,''),'Client'),COALESCE(NULLIF(contact2_role_label,''),'Buyers Agent')
FROM public.jobs_staging WHERE tenant_id::text=@tenant
ORDER BY updated_at DESC LIMIT 1";
    await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("tenant", tenantId.ToString());
    await using var reader = await cmd.ExecuteReaderAsync();
    if (!await reader.ReadAsync()) return ("Client", "Buyers Agent");
    return (reader.GetString(0), reader.GetString(1));
}

static (string Subject, string HtmlBody) BuildDefaultBasicTemplate(string eventKey, string recipientKey, string recipientLabel)
{
    if (!AutoMateApi.BasicAutomationSupport.IsValidEvent(eventKey) || !AutoMateApi.BasicAutomationSupport.IsValidRecipient(recipientKey))
        throw new ArgumentException("Unsupported Basic template slot.");
    var greeting = recipientKey == "contact_2" ? "{{AGENT_FIRST_NAME}}" : "{{CLIENT_FIRST_NAME}}";
    var eventText = eventKey switch
    {
        "scheduling" => "has been scheduled",
        "rescheduling" => "has been rescheduled",
        "cancellation" => "has been cancelled",
        "service_change" => "has updated services",
        _ => "has been updated"
    };
    var subject = eventKey switch
    {
        "scheduling" => "Inspection scheduled - {{PROPERTY_ADDRESS}}",
        "rescheduling" => "Inspection rescheduled - {{PROPERTY_ADDRESS}}",
        "cancellation" => "Inspection cancelled - {{PROPERTY_ADDRESS}}",
        _ => "Inspection services updated - {{PROPERTY_ADDRESS}}"
    };
    var html = $"<div style=\"font-family:Segoe UI,Arial,sans-serif;font-size:15px;line-height:1.5;color:#1f2937\"><p>Hi {greeting},</p><p>The inspection at <strong>{{{{PROPERTY_ADDRESS}}}}</strong> {eventText}.</p><p><strong>Date:</strong> {{{{INSPECTION_DATE}}}} {{{{INSPECTION_TIME}}}}<br><strong>Services:</strong> {{{{SERVICES}}}}</p><p>Regards,<br>{{{{COMPANY_NAME}}}}</p></div>";
    return (subject, html);
}

static async Task<RenderedEmailTemplate?> RenderBookingEmailTemplateAsync(
    NpgsqlConnection conn,
    Guid jobId,
    EmailTemplateRenderRequest request,
    bool preferDraft)
{
    var job = await LoadScheduleJobAsync(conn, jobId);
    if (job == null)
        return null;

    var serviceTypeKey = NormalizeServiceTypeKey(request.ServiceTypeKey);
    if (string.IsNullOrWhiteSpace(request.ServiceTypeKey))
        serviceTypeKey = NormalizeServiceTypeKey(job.BookingTemplateKey);

    var service = ResolveTemplateService(job, serviceTypeKey);
    if (!string.IsNullOrWhiteSpace(service.ServiceKey))
        serviceTypeKey = NormalizeServiceTypeKey(service.ServiceKey);

    var template = await LoadEmailTemplateAsync(conn, job.InspectorId, "booking-email", serviceTypeKey);

    var subjectTemplate = preferDraft && !string.IsNullOrWhiteSpace(request.Subject)
        ? request.Subject
        : template.Subject;
    var htmlTemplate = preferDraft && !string.IsNullOrWhiteSpace(request.HtmlBody)
        ? request.HtmlBody
        : template.HtmlBody;

    if (string.IsNullOrWhiteSpace(subjectTemplate))
        subjectTemplate = BuildDefaultBookingTemplateSubject(serviceTypeKey);

    if (string.IsNullOrWhiteSpace(htmlTemplate))
        htmlTemplate = BuildDefaultBookingTemplateHtml();

    var fields = BuildEmailTemplateFields(job, service);
    await EnsureJobInvoiceLinesTableAsync(conn);
    var invoiceContext = await AutoMateApi.EmailInvoiceTemplateContext.LoadAsync(conn, jobId);
    if (invoiceContext != null)
        MergeEmailTemplateFields(fields, invoiceContext.Tokens);
    var subject = RenderTemplateTokens(subjectTemplate, fields, htmlEncode: false);
    var htmlBody = RenderTemplateTokens(CleanEditorHtml(htmlTemplate), fields, htmlEncode: true);
    var toEmail = string.IsNullOrWhiteSpace(request.ToEmail) ? job.ClientEmail : request.ToEmail.Trim();
    var actionKey = !string.IsNullOrWhiteSpace(request.ActionKey)
        ? request.ActionKey.Trim()
        : BuildBookingActionKey(service.ServiceKey, service.Label);

    return new RenderedEmailTemplate(
        job.JobId,
        job.InspectorId,
        job.EmailSenderMode,
        toEmail,
        subject,
        htmlBody,
        serviceTypeKey,
        service.Label,
        actionKey,
        fields);
}

static async Task<RenderedBasicEmail> RenderBasicEmailAsync(NpgsqlConnection conn, ScheduleJobInput job, string eventKey, string recipientKey, string? draftSubject=null, string? draftHtml=null)
{
    if (!AutoMateApi.BasicAutomationSupport.IsValidEvent(eventKey) || !AutoMateApi.BasicAutomationSupport.IsValidRecipient(recipientKey)) throw new ArgumentException("Unsupported Basic template slot.");
    await AutoMateApi.BasicAutomationSupport.EnsureAsync(conn);
    var slot=(await AutoMateApi.BasicAutomationSupport.LoadAsync(conn,job.TenantId)).First(item=>item.EventKey==eventKey&&item.RecipientKey==recipientKey);
    var label=recipientKey=="contact_2" ? (string.IsNullOrWhiteSpace(job.AgentRoleLabel)?"Buyers Agent":job.AgentRoleLabel) : (string.IsNullOrWhiteSpace(job.ClientRoleLabel)?"Client":job.ClientRoleLabel);
    var defaults=BuildDefaultBasicTemplate(eventKey,recipientKey,label);
    var subjectTemplate=!string.IsNullOrWhiteSpace(draftSubject)?draftSubject:(!string.IsNullOrWhiteSpace(slot.Subject)?slot.Subject:defaults.Subject);
    var htmlTemplate=!string.IsNullOrWhiteSpace(draftHtml)?draftHtml:(!string.IsNullOrWhiteSpace(slot.HtmlBody)?slot.HtmlBody:defaults.HtmlBody);
    var fields=BuildEmailTemplateFields(job,null); var invoice=await AutoMateApi.EmailInvoiceTemplateContext.LoadAsync(conn,job.JobId); if(invoice!=null) MergeEmailTemplateFields(fields,invoice.Tokens);
    return new RenderedBasicEmail(recipientKey=="contact_2"?job.AgentEmail:job.ClientEmail,RenderTemplateTokens(subjectTemplate,fields,false),RenderTemplateTokens(CleanEditorHtml(htmlTemplate),fields,true),label);
}

static ScheduleServiceInput ResolveTemplateService(ScheduleJobInput job, string serviceTypeKey)
{
    var services = GetSchedulableServices(job).ToArray();
    var normalized = NormalizeServiceTypeKey(serviceTypeKey);

    foreach (var service in services)
    {
        if (string.Equals(NormalizeServiceTypeKey(service.ServiceKey), normalized, StringComparison.OrdinalIgnoreCase))
            return service;
    }

    if (services.Length > 0)
    {
        var bookingTemplateKey = NormalizeServiceTypeKey(job.BookingTemplateKey);
        if (string.Equals(bookingTemplateKey, normalized, StringComparison.OrdinalIgnoreCase))
            return services[0];

        return services[0];
    }

    var fallbackLabel = string.IsNullOrWhiteSpace(job.PrimaryService)
        ? GetEmailTemplateServiceLabel(normalized)
        : job.PrimaryService;

    return new ScheduleServiceInput(fallbackLabel, normalized, "primary");
}

static Dictionary<string, string> BuildEmailTemplateFields(ScheduleJobInput job, ScheduleServiceInput? service)
{
    var resolvedService = service ?? ResolveTemplateService(job, job.BookingTemplateKey);
    var start = job.JobDate;
    var end = start.HasValue ? start.Value.AddMinutes(Math.Max(1, job.InspectionDurationMinutes)) : (DateTime?)null;
    var services = GetSchedulableServices(job).Select(item => item.Label).Where(value => !string.IsNullOrWhiteSpace(value)).ToArray();
    var additionalServices = new[] { job.Additional1, job.Additional2 }.Where(value => !string.IsNullOrWhiteSpace(value)).Select(value => value.Trim()).ToArray();
    var addOnKeys = GetSchedulableServices(job)
        .Where(item => !string.Equals(item.Slot, "primary", StringComparison.OrdinalIgnoreCase))
        .Select(item => NormalizeServiceTypeKey(item.ServiceKey))
        .Where(value => !string.IsNullOrWhiteSpace(value))
        .ToArray();
    var company = job.CompanyName?.Trim() ?? "";
    var inspector = !string.IsNullOrWhiteSpace(job.EmailFromName) ? job.EmailFromName.Trim() : job.InspectorName;

    var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["SERVICES"] = string.Join(", ", services.Length == 0 ? new[] { resolvedService.Label } : services),
        ["PRIMARY_SERVICE"] = string.IsNullOrWhiteSpace(job.PrimaryService) ? resolvedService.Label : job.PrimaryService,
        ["ADDITIONAL_SERVICES"] = additionalServices.Length == 0 ? "None" : string.Join(", ", additionalServices),
        ["PRIMARY_SERVICE_KEY"] = NormalizeServiceTypeKey(job.PrimaryServiceKey),
        ["ADDITIONAL1_SERVICE_KEY"] = NormalizeServiceTypeKey(job.Additional1ServiceKey),
        ["ADDITIONAL2_SERVICE_KEY"] = NormalizeServiceTypeKey(job.Additional2ServiceKey),
        ["CANONICAL_SERVICES"] = string.Join(", ", GetSchedulableServices(job).Select(item => NormalizeServiceTypeKey(item.ServiceKey))),
        ["ADD_ON_SERVICE_KEYS"] = string.Join(", ", addOnKeys),
        ["BOOKING_TEMPLATE_KEY"] = NormalizeServiceTypeKey(job.BookingTemplateKey),
        ["BOOKING_EMAIL_REQUIRED"] = job.BookingEmailRequired ? "Yes" : "No",
        ["TERMS_REQUIRED"] = job.TermsRequired ? "Yes" : "No",
        ["INVOICE_REQUIRED"] = job.InvoiceRequired ? "Yes" : "No",
        ["CALENDAR_REQUIRED"] = job.CalendarRequired ? "Yes" : "No",
        ["REPORT_REQUIRED"] = "Yes",
        ["PROPERTY_ADDRESS"] = job.SiteAddress,
        ["ADDRESS"] = job.SiteAddress,
        ["PROPERTY_COUNTY"] = "",
        ["STREET_ADDRESS"] = job.SiteAddress,
        ["INSPECTION_DATE"] = start.HasValue ? start.Value.ToString("dd MMM yyyy") : "To be confirmed",
        ["INSPECTION_TIME"] = start.HasValue ? start.Value.ToString("h:mm tt") : "",
        ["INSPECTION_END_TIME"] = end.HasValue ? end.Value.ToString("h:mm tt") : "",
        ["CLIENT_NAME"] = string.IsNullOrWhiteSpace(job.ClientDisplayName) ? job.ClientName : job.ClientDisplayName,
        ["CLIENT_DISPLAY_NAME"] = string.IsNullOrWhiteSpace(job.ClientDisplayName) ? job.ClientName : job.ClientDisplayName,
        ["CLIENT_FIRST_NAME"] = job.ClientFirstName,
        ["CLIENT_LAST_NAME"] = job.ClientLastName,
        ["CLIENT_SALUTATION"] = job.ClientSalutation,
        ["CLIENT_ADDRESS"] = "",
        ["CLIENT_EMAIL"] = job.ClientEmail,
        ["CLIENT_PHONE"] = job.ClientPhone,
        ["AGENT_NAME"] = string.IsNullOrWhiteSpace(job.AgentDisplayName) ? job.AgentName : job.AgentDisplayName,
        ["AGENT_DISPLAY_NAME"] = string.IsNullOrWhiteSpace(job.AgentDisplayName) ? job.AgentName : job.AgentDisplayName,
        ["AGENT_FIRST_NAME"] = job.AgentFirstName,
        ["AGENT_LAST_NAME"] = job.AgentLastName,
        ["AGENT_SALUTATION"] = job.AgentSalutation,
        ["AGENT_FULL_ADDRESS"] = "",
        ["AGENT_ADDRESS"] = "",
        ["AGENT_CITY"] = "",
        ["AGENT_STATE"] = "",
        ["AGENT_ZIP"] = "",
        ["LISTING_AGENT_NAME"] = job.AgentName,
        ["LISTING_AGENT_FIRST_NAME"] = FirstWord(job.AgentName),
        ["LISTING_AGENT_FULL_ADDRESS"] = "",
        ["LISTING_AGENT_ADDRESS"] = "",
        ["LISTING_AGENT_CITY"] = "",
        ["LISTING_AGENT_STATE"] = "",
        ["LISTING_AGENT_ZIP"] = "",
        ["INSPECTOR_NAME"] = inspector,
        ["INSPECTOR_FIRST_NAME"] = FirstWord(inspector),
        ["INSPECTOR_PHONE"] = string.IsNullOrWhiteSpace(job.Phone) ? job.ClientPhone : job.Phone,
        ["INSPECTOR_EMAIL"] = job.EmailFromAddress,
        ["INSPECTORS_NAMES"] = inspector,
        ["COMPANY_NAME"] = company,
        ["LOGO_URL"] = "",
        ["COMPANY_LOGO_URL"] = "",
        ["JOB_NAME"] = job.JobName,
        ["INSPECTION_LINK"] = "",
        ["REPORT_LINK"] = "",
        ["INVOICE_LINK"] = ""
    };

    foreach (var serviceType in GetEmailTemplateAddOnServiceTypes())
    {
        var key = BuildAddOnPlaceholderKey(serviceType.Key);
        fields[key] = addOnKeys.Any(value => string.Equals(value, NormalizeServiceTypeKey(serviceType.Key), StringComparison.OrdinalIgnoreCase))
            ? "Yes"
            : "No";
    }

    return fields;
}

static string RenderTemplateTokens(string template, Dictionary<string, string> fields, bool htmlEncode)
{
    return Regex.Replace(
        template ?? "",
        "\\{\\{\\s*([A-Z0-9_]+)\\s*\\}\\}",
        match =>
        {
            var key = match.Groups[1].Value;
            if (!fields.TryGetValue(key, out var replacement))
                return match.Value;

            if (htmlEncode && string.Equals(key, "INVOICE_LINE_ITEMS", StringComparison.OrdinalIgnoreCase))
                return replacement;

            return htmlEncode ? SafeHtml(replacement) : replacement;
        },
        RegexOptions.IgnoreCase);
}

static void MergeEmailTemplateFields(Dictionary<string, string> fields, IReadOnlyDictionary<string, string> additions)
{
    foreach (var pair in additions)
        fields[pair.Key] = pair.Value;
}

static async Task MarkWorkflowActionFailedAsync(NpgsqlConnection conn, Guid jobId, string actionKey, string error)
{
    if (string.IsNullOrWhiteSpace(actionKey))
        return;

    const string sql = @"
UPDATE public.job_workflow_actions
SET
    status = 'failed',
    retry_requested = false,
    retry_requested_at = NULL,
    last_attempt_at = NOW(),
    last_error = @error_message,
    updated_at = NOW()
WHERE job_id = @job_id
  AND action_key = @action_key;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("action_key", actionKey);
    cmd.Parameters.AddWithValue("error_message", error ?? "");
    await cmd.ExecuteNonQueryAsync();
}

static async Task MarkBookingEmailSentIfNoPendingActionsAsync(NpgsqlConnection conn, Guid jobId)
{
    const string pendingSql = @"
SELECT COUNT(*)
FROM public.job_workflow_actions
WHERE job_id = @job_id
  AND action_type = 'booking_email'
  AND (status = 'pending' OR retry_requested = true);";

    await using (var pendingCmd = new NpgsqlCommand(pendingSql, conn))
    {
        pendingCmd.Parameters.AddWithValue("job_id", jobId);
        var pending = Convert.ToInt32(await pendingCmd.ExecuteScalarAsync());
        if (pending > 0)
            return;
    }

    await MarkBookingEmailSentAsync(conn, jobId);
}

static object BuildAddOnPlaceholder(EmailTemplateServiceType serviceType)
{
    var key = BuildAddOnPlaceholderKey(serviceType.Key);
    return new { key, token = "{{" + key + "}}", label = "Has " + serviceType.Label };
}

static object P(string key, string label) => new { key, token = "{{" + key + "}}", label };

static object[] GetBasicEmailTemplatePlaceholders() => new[]
{
    P("PROPERTY_ADDRESS","Property Address"), P("INSPECTION_DATE","Inspection Date"), P("INSPECTION_TIME","Inspection Time"), P("INSPECTION_END_TIME","Inspection End Time"),
    P("SERVICES","Services"), P("PRIMARY_SERVICE","Primary Service"), P("ADDITIONAL_SERVICES","Additional Services"),
    P("CLIENT_FIRST_NAME","Client First Name"), P("CLIENT_LAST_NAME","Client Last Name"), P("CLIENT_DISPLAY_NAME","Client Display Name"), P("CLIENT_SALUTATION","Client Salutation"), P("CLIENT_EMAIL","Client Email"), P("CLIENT_PHONE","Client Phone"),
    P("AGENT_FIRST_NAME","Agent First Name"), P("AGENT_LAST_NAME","Agent Last Name"), P("AGENT_DISPLAY_NAME","Agent Display Name"), P("AGENT_SALUTATION","Agent Salutation"), P("AGENT_EMAIL","Agent Email"), P("AGENT_PHONE","Agent Phone"),
    P("INSPECTOR_NAME","Inspector Name"), P("INSPECTOR_FIRST_NAME","Inspector First Name"), P("INSPECTOR_EMAIL","Inspector Email"), P("INSPECTOR_PHONE","Inspector Phone"), P("COMPANY_NAME","Company Name"), P("COMPANY_LOGO_URL","Company Logo"),
    P("INVOICE_TOTAL","Invoice Total"), P("AMOUNT_PAID","Amount Paid"), P("BALANCE_DUE","Balance Due"), P("INVOICE_LINE_ITEMS","Invoice Line Items")
};

static object[] GetBasicEmailTemplatePlaceholderCategories() => new object[]
{
    new { category="Job Details", placeholders=new[]{ P("PROPERTY_ADDRESS","Property Address"),P("INSPECTION_DATE","Inspection Date"),P("INSPECTION_TIME","Inspection Time"),P("INSPECTION_END_TIME","Inspection End Time") } },
    new { category="Services", placeholders=new[]{ P("SERVICES","Services"),P("PRIMARY_SERVICE","Primary Service"),P("ADDITIONAL_SERVICES","Additional Services") } },
    new { category="Client Contact", placeholders=new[]{ P("CLIENT_FIRST_NAME","First Name"),P("CLIENT_LAST_NAME","Last Name"),P("CLIENT_DISPLAY_NAME","Display Name"),P("CLIENT_SALUTATION","Salutation"),P("CLIENT_EMAIL","Email"),P("CLIENT_PHONE","Phone") } },
    new { category="Agent Contact", placeholders=new[]{ P("AGENT_FIRST_NAME","First Name"),P("AGENT_LAST_NAME","Last Name"),P("AGENT_DISPLAY_NAME","Display Name"),P("AGENT_SALUTATION","Salutation"),P("AGENT_EMAIL","Email"),P("AGENT_PHONE","Phone") } },
    new { category="Inspector and Company", placeholders=new[]{ P("INSPECTOR_NAME","Inspector Name"),P("INSPECTOR_FIRST_NAME","Inspector First Name"),P("INSPECTOR_EMAIL","Inspector Email"),P("INSPECTOR_PHONE","Inspector Phone"),P("COMPANY_NAME","Company Name"),P("COMPANY_LOGO_URL","Company Logo") } },
    new { category="Invoice Details", placeholders=new[]{ P("INVOICE_TOTAL","Invoice Total"),P("AMOUNT_PAID","Amount Paid"),P("BALANCE_DUE","Balance Due"),P("INVOICE_LINE_ITEMS","Invoice Line Items") } }
};

#pragma warning disable CS8321
static object[] GetEmailTemplatePlaceholders()
{
    var basePlaceholders = new object[]
    {
        new { key = "SERVICES", token = "{{SERVICES}}", label = "Services" },
        new { key = "PRIMARY_SERVICE", token = "{{PRIMARY_SERVICE}}", label = "Primary Service" },
        new { key = "ADDITIONAL_SERVICES", token = "{{ADDITIONAL_SERVICES}}", label = "Additional Services" },
        new { key = "PRIMARY_SERVICE_KEY", token = "{{PRIMARY_SERVICE_KEY}}", label = "Primary Service Key" },
        new { key = "ADDITIONAL1_SERVICE_KEY", token = "{{ADDITIONAL1_SERVICE_KEY}}", label = "Additional 1 Service Key" },
        new { key = "ADDITIONAL2_SERVICE_KEY", token = "{{ADDITIONAL2_SERVICE_KEY}}", label = "Additional 2 Service Key" },
        new { key = "CANONICAL_SERVICES", token = "{{CANONICAL_SERVICES}}", label = "Canonical Services" },
        new { key = "ADD_ON_SERVICE_KEYS", token = "{{ADD_ON_SERVICE_KEYS}}", label = "Add On Service Keys" },
        new { key = "BOOKING_TEMPLATE_KEY", token = "{{BOOKING_TEMPLATE_KEY}}", label = "Booking Template Key" },
        new { key = "BOOKING_EMAIL_REQUIRED", token = "{{BOOKING_EMAIL_REQUIRED}}", label = "Booking Email Required" },
        new { key = "TERMS_REQUIRED", token = "{{TERMS_REQUIRED}}", label = "Terms Required" },
        new { key = "INVOICE_REQUIRED", token = "{{INVOICE_REQUIRED}}", label = "Invoice Required" },
        new { key = "CALENDAR_REQUIRED", token = "{{CALENDAR_REQUIRED}}", label = "Calendar Required" },
        new { key = "REPORT_REQUIRED", token = "{{REPORT_REQUIRED}}", label = "Report Required" },
        new { key = "PROPERTY_ADDRESS", token = "{{PROPERTY_ADDRESS}}", label = "Property Address" },
        new { key = "ADDRESS", token = "{{ADDRESS}}", label = "Address" },
        new { key = "PROPERTY_COUNTY", token = "{{PROPERTY_COUNTY}}", label = "Property County" },
        new { key = "STREET_ADDRESS", token = "{{STREET_ADDRESS}}", label = "Street Address" },
        new { key = "INSPECTION_DATE", token = "{{INSPECTION_DATE}}", label = "Inspection Date" },
        new { key = "INSPECTION_TIME", token = "{{INSPECTION_TIME}}", label = "Inspection Time" },
        new { key = "INSPECTION_END_TIME", token = "{{INSPECTION_END_TIME}}", label = "Inspection End Time" },
        new { key = "CLIENT_NAME", token = "{{CLIENT_NAME}}", label = "Client Name" },
        new { key = "CLIENT_FIRST_NAME", token = "{{CLIENT_FIRST_NAME}}", label = "Client First Name" },
        new { key = "CLIENT_ADDRESS", token = "{{CLIENT_ADDRESS}}", label = "Client Address" },
        new { key = "CLIENT_EMAIL", token = "{{CLIENT_EMAIL}}", label = "Client Email" },
        new { key = "CLIENT_PHONE", token = "{{CLIENT_PHONE}}", label = "Client Phone" },
        new { key = "AGENT_NAME", token = "{{AGENT_NAME}}", label = "Agent Name" },
        new { key = "AGENT_FIRST_NAME", token = "{{AGENT_FIRST_NAME}}", label = "Agent First Name" },
        new { key = "AGENT_FULL_ADDRESS", token = "{{AGENT_FULL_ADDRESS}}", label = "Agent Full Address" },
        new { key = "AGENT_ADDRESS", token = "{{AGENT_ADDRESS}}", label = "Agent Address" },
        new { key = "AGENT_CITY", token = "{{AGENT_CITY}}", label = "Agent City" },
        new { key = "AGENT_STATE", token = "{{AGENT_STATE}}", label = "Agent State" },
        new { key = "AGENT_ZIP", token = "{{AGENT_ZIP}}", label = "Agent Zip" },
        new { key = "LISTING_AGENT_NAME", token = "{{LISTING_AGENT_NAME}}", label = "Listing Agent Name" },
        new { key = "LISTING_AGENT_FIRST_NAME", token = "{{LISTING_AGENT_FIRST_NAME}}", label = "Listing Agent First Name" },
        new { key = "LISTING_AGENT_FULL_ADDRESS", token = "{{LISTING_AGENT_FULL_ADDRESS}}", label = "Listing Agent Full Address" },
        new { key = "LISTING_AGENT_ADDRESS", token = "{{LISTING_AGENT_ADDRESS}}", label = "Listing Agent Address" },
        new { key = "LISTING_AGENT_CITY", token = "{{LISTING_AGENT_CITY}}", label = "Listing Agent City" },
        new { key = "LISTING_AGENT_STATE", token = "{{LISTING_AGENT_STATE}}", label = "Listing Agent State" },
        new { key = "LISTING_AGENT_ZIP", token = "{{LISTING_AGENT_ZIP}}", label = "Listing Agent Zip" },
        new { key = "INSPECTOR_NAME", token = "{{INSPECTOR_NAME}}", label = "Inspector Name" },
        new { key = "INSPECTOR_FIRST_NAME", token = "{{INSPECTOR_FIRST_NAME}}", label = "Inspector First Name" },
        new { key = "INSPECTOR_PHONE", token = "{{INSPECTOR_PHONE}}", label = "Inspector Phone" },
        new { key = "INSPECTOR_EMAIL", token = "{{INSPECTOR_EMAIL}}", label = "Inspector Email" },
        new { key = "INSPECTORS_NAMES", token = "{{INSPECTORS_NAMES}}", label = "Inspectors Names" },
        new { key = "COMPANY_NAME", token = "{{COMPANY_NAME}}", label = "Company Name" },
        new { key = "LOGO_URL", token = "{{LOGO_URL}}", label = "Logo URL" },
        new { key = "COMPANY_LOGO_URL", token = "{{COMPANY_LOGO_URL}}", label = "Company Logo URL" },
        new { key = "JOB_NAME", token = "{{JOB_NAME}}", label = "Job Name" },
        new { key = "INSPECTION_LINK", token = "{{INSPECTION_LINK}}", label = "Inspection Link" },
        new { key = "REPORT_LINK", token = "{{REPORT_LINK}}", label = "Report Link" },
        new { key = "INVOICE_LINK", token = "{{INVOICE_LINK}}", label = "Invoice Link" }
    };

    return basePlaceholders
        .Take(8)
        .Concat(GetEmailTemplateAddOnServiceTypes().Select(BuildAddOnPlaceholder))
        .Concat(basePlaceholders.Skip(8))
        .Concat(GetInvoiceEmailTemplatePlaceholders())
        .ToArray();
}
#pragma warning restore CS8321

static object[] GetInvoiceEmailTemplatePlaceholders()
{
    var placeholders = new List<object>
    {
        new { key = "INVOICE_TOTAL", token = "{{INVOICE_TOTAL}}", label = "Invoice Total" },
        new { key = "AMOUNT_PAID", token = "{{AMOUNT_PAID}}", label = "Amount Paid" },
        new { key = "BALANCE_DUE", token = "{{BALANCE_DUE}}", label = "Balance Due" },
        new { key = "INVOICE_LINE_ITEMS", token = "{{INVOICE_LINE_ITEMS}}", label = "Invoice Line Items Table" }
    };

    for (var line = 1; line <= AutoMateApi.EmailInvoiceTemplateContext.IndexedLineLimit; line++)
    {
        placeholders.Add(new { key = $"INVOICE_LINE_{line}_DESCRIPTION", token = $"{{{{INVOICE_LINE_{line}_DESCRIPTION}}}}", label = $"Invoice Line {line} Description" });
        placeholders.Add(new { key = $"INVOICE_LINE_{line}_QUANTITY", token = $"{{{{INVOICE_LINE_{line}_QUANTITY}}}}", label = $"Invoice Line {line} Quantity" });
        placeholders.Add(new { key = $"INVOICE_LINE_{line}_UNIT_PRICE", token = $"{{{{INVOICE_LINE_{line}_UNIT_PRICE}}}}", label = $"Invoice Line {line} Unit Price" });
        placeholders.Add(new { key = $"INVOICE_LINE_{line}_TOTAL", token = $"{{{{INVOICE_LINE_{line}_TOTAL}}}}", label = $"Invoice Line {line} Total" });
    }

    return placeholders.ToArray();
}

#pragma warning disable CS8321
static object[] GetEmailTemplatePlaceholderCategories()
{
    return new object[]
    {
        new
        {
            category = "Job Details",
            placeholders = new object[]
            {
                new { key = "JOB_NAME", token = "{{JOB_NAME}}", label = "Job Name" },
                new { key = "PROPERTY_ADDRESS", token = "{{PROPERTY_ADDRESS}}", label = "Property Address" },
                new { key = "ADDRESS", token = "{{ADDRESS}}", label = "Address" },
                new { key = "INSPECTION_DATE", token = "{{INSPECTION_DATE}}", label = "Inspection Date" },
                new { key = "INSPECTION_TIME", token = "{{INSPECTION_TIME}}", label = "Inspection Time" },
                new { key = "INSPECTION_END_TIME", token = "{{INSPECTION_END_TIME}}", label = "Inspection End Time" }
            }
        },
        new
        {
            category = "Service Items",
            placeholders = new object[]
            {
                new { key = "SERVICES", token = "{{SERVICES}}", label = "Services" },
                new { key = "PRIMARY_SERVICE", token = "{{PRIMARY_SERVICE}}", label = "Primary Service" },
                new { key = "ADDITIONAL_SERVICES", token = "{{ADDITIONAL_SERVICES}}", label = "Additional Services" },
                new { key = "PRIMARY_SERVICE_KEY", token = "{{PRIMARY_SERVICE_KEY}}", label = "Primary Service Key" },
                new { key = "ADDITIONAL1_SERVICE_KEY", token = "{{ADDITIONAL1_SERVICE_KEY}}", label = "Additional 1 Service Key" },
                new { key = "ADDITIONAL2_SERVICE_KEY", token = "{{ADDITIONAL2_SERVICE_KEY}}", label = "Additional 2 Service Key" },
                new { key = "CANONICAL_SERVICES", token = "{{CANONICAL_SERVICES}}", label = "Canonical Services" },
                new { key = "ADD_ON_SERVICE_KEYS", token = "{{ADD_ON_SERVICE_KEYS}}", label = "Add On Service Keys" },
                new { key = "BOOKING_TEMPLATE_KEY", token = "{{BOOKING_TEMPLATE_KEY}}", label = "Booking Template Key" }
            }
            .Take(8)
            .Concat(GetEmailTemplateAddOnServiceTypes().Select(BuildAddOnPlaceholder))
            .Concat(new object[]
            {
                new { key = "BOOKING_TEMPLATE_KEY", token = "{{BOOKING_TEMPLATE_KEY}}", label = "Booking Template Key" }
            })
            .ToArray()
        },
        new
        {
            category = "Client Details",
            placeholders = new object[]
            {
                new { key = "CLIENT_NAME", token = "{{CLIENT_NAME}}", label = "Client Name" },
                new { key = "CLIENT_FIRST_NAME", token = "{{CLIENT_FIRST_NAME}}", label = "Client First Name" },
                new { key = "CLIENT_EMAIL", token = "{{CLIENT_EMAIL}}", label = "Client Email" },
                new { key = "CLIENT_PHONE", token = "{{CLIENT_PHONE}}", label = "Client Phone" }
            }
        },
        new
        {
            category = "Agent Details",
            placeholders = new object[]
            {
                new { key = "AGENT_NAME", token = "{{AGENT_NAME}}", label = "Agent Name" },
                new { key = "AGENT_FIRST_NAME", token = "{{AGENT_FIRST_NAME}}", label = "Agent First Name" }
            }
        },
        new
        {
            category = "Inspector Details",
            placeholders = new object[]
            {
                new { key = "INSPECTOR_NAME", token = "{{INSPECTOR_NAME}}", label = "Inspector Name" },
                new { key = "INSPECTOR_FIRST_NAME", token = "{{INSPECTOR_FIRST_NAME}}", label = "Inspector First Name" },
                new { key = "INSPECTOR_PHONE", token = "{{INSPECTOR_PHONE}}", label = "Inspector Phone" },
                new { key = "INSPECTOR_EMAIL", token = "{{INSPECTOR_EMAIL}}", label = "Inspector Email" },
                new { key = "COMPANY_NAME", token = "{{COMPANY_NAME}}", label = "Company Name" },
                new { key = "LOGO_URL", token = "{{LOGO_URL}}", label = "Logo URL" },
                new { key = "COMPANY_LOGO_URL", token = "{{COMPANY_LOGO_URL}}", label = "Company Logo URL" }
            }
        },
        new
        {
            category = "Invoice Details",
            placeholders = GetInvoiceEmailTemplatePlaceholders()
        },
        new
        {
            category = "Links",
            placeholders = new object[]
            {
                new { key = "INSPECTION_LINK", token = "{{INSPECTION_LINK}}", label = "Inspection Link" },
                new { key = "REPORT_LINK", token = "{{REPORT_LINK}}", label = "Report Link" },
                new { key = "INVOICE_LINK", token = "{{INVOICE_LINK}}", label = "Invoice Link" }
            }
        }
    };
}
#pragma warning restore CS8321

static object[] GetEmailTemplateServiceTypes()
{
    return GetEmailTemplateServiceTypeRecords()
        .Select(type => new { key = type.Key, label = type.Label, group = type.Group })
        .ToArray();
}

static EmailTemplateServiceType[] GetEmailTemplateAddOnServiceTypes()
{
    return GetEmailTemplateServiceTypeRecords()
        .Where(type => string.Equals(type.Group, "Add-on service", StringComparison.OrdinalIgnoreCase))
        .ToArray();
}

static EmailTemplateServiceType[] GetEmailTemplateServiceTypeRecords()
{
    return new[]
    {
        new EmailTemplateServiceType("general_booking", "General booking", "Primary service"),
        new EmailTemplateServiceType("building_inspection", "Building inspection", "Primary service"),
        new EmailTemplateServiceType("building_investigation", "Building investigation", "Primary service"),
        new EmailTemplateServiceType("healthy_homes_assessment", "Healthy Homes assessment", "Primary service"),
        new EmailTemplateServiceType("meth_field_composite", "Meth field composite", "Primary service"),
        new EmailTemplateServiceType("meth_lab_composite", "Meth lab composite", "Primary service"),
        new EmailTemplateServiceType("building_inspection_weathertightness", "Building inspection + weathertightness", "Building inspection combinations"),
        new EmailTemplateServiceType("building_inspection_garage_outbuilding", "Building inspection + garage/outbuilding", "Building inspection combinations"),
        new EmailTemplateServiceType("building_inspection_attached_flat", "Building inspection + attached flat", "Building inspection combinations"),
        new EmailTemplateServiceType("building_inspection_property_file_review", "Building inspection + property file review", "Building inspection combinations"),
        new EmailTemplateServiceType("building_inspection_weathertightness_garage_outbuilding", "Building inspection + weathertightness + garage/outbuilding", "Building inspection combinations"),
        new EmailTemplateServiceType("building_inspection_weathertightness_attached_flat", "Building inspection + weathertightness + attached flat", "Building inspection combinations"),
        new EmailTemplateServiceType("building_inspection_weathertightness_property_file_review", "Building inspection + weathertightness + property file review", "Building inspection combinations"),
        new EmailTemplateServiceType("weathertightness", "Weathertightness", "Add-on service"),
        new EmailTemplateServiceType("garage_outbuilding", "Garage/outbuilding", "Add-on service"),
        new EmailTemplateServiceType("attached_flat", "Attached flat", "Add-on service"),
        new EmailTemplateServiceType("property_file_review", "Property file review", "Add-on service"),
        new EmailTemplateServiceType("reinspection", "Reinspection", "Additional service / modifier"),
        new EmailTemplateServiceType("asbestos_test", "Asbestos test", "Additional service / modifier"),
        new EmailTemplateServiceType("moisture_check", "Moisture check", "Additional service / modifier"),
        new EmailTemplateServiceType("thermal_imaging", "Thermal imaging", "Additional service / modifier"),
        new EmailTemplateServiceType("pool_inspection", "Pool inspection", "Additional service / modifier"),
        new EmailTemplateServiceType("custom_service", "Custom service", "Additional service / modifier"),
        new EmailTemplateServiceType("other_service", "Other service", "Additional service / modifier")
    };
}

static string GetEmailTemplateMakerHtml()
{
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AutoMate Email Templates</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #1d252c;
      --muted: #64717d;
      --line: #d8dee4;
      --panel: #f6f7f8;
      --accent: #c9662a;
      --accent-dark: #a84f1c;
      --ok: #1f7a4d;
      --bad: #a4362d;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Segoe UI, Arial, sans-serif;
      color: var(--ink);
      background: #fff;
    }
    header {
      padding: 16px 20px 10px;
      border-bottom: 1px solid var(--line);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      margin: 0;
      font-size: 20px;
      font-weight: 650;
    }
    .status {
      min-height: 22px;
      font-size: 13px;
      color: var(--muted);
      text-align: right;
    }
    main {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 20px;
      padding: 18px 20px 28px;
    }
    .field-grid {
      display: none;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 5px;
    }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 4px;
      padding: 9px 10px;
      font: inherit;
      color: var(--ink);
      background: #fff;
    }
    #htmlBody { display: none; }
    .editor-tools {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      padding: 8px;
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 4px 4px 0 0;
      background: var(--panel);
    }
    .editor-tools button {
      min-height: 30px;
      padding: 4px 9px;
      font-weight: 650;
    }
    .editor-frame {
      width: 100%;
      height: 560px;
      border: 1px solid var(--line);
      border-radius: 0 0 4px 4px;
      background: #fff;
    }
    .subject-row {
      display: grid;
      grid-template-columns: 1fr 170px;
      gap: 12px;
      margin-bottom: 14px;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 14px 0;
    }
    button {
      border: 1px solid var(--line);
      border-radius: 4px;
      padding: 9px 13px;
      background: #fff;
      color: var(--ink);
      cursor: pointer;
      font: inherit;
      min-height: 38px;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
      font-weight: 650;
    }
    button.primary:hover { background: var(--accent-dark); }
    button:disabled {
      opacity: .48;
      cursor: not-allowed;
    }
    aside {
      border-left: 1px solid var(--line);
      padding-left: 20px;
    }
    aside h2 {
      margin: 0 0 10px;
      font-size: 14px;
      color: var(--muted);
      font-weight: 600;
    }
    .placeholder-list {
      display: grid;
      gap: 7px;
      max-height: calc(100vh - 148px);
      overflow: auto;
      padding-right: 4px;
    }
    .placeholder-list button {
      width: 100%;
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
      text-align: center;
      font-size: 13px;
      font-weight: 650;
      padding: 7px 9px;
      min-height: 30px;
    }
    .preview-wrap {
      margin-top: 14px;
      border: 1px solid var(--line);
      min-height: 260px;
    }
    .preview-head {
      padding: 9px 10px;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      font-size: 13px;
      color: var(--muted);
    }
    .preview-wrap iframe {
      width: 100%;
      height: 260px;
      border: 0;
      background: #fff;
    }
    .send-grid {
      display: grid;
      grid-template-columns: 1fr 1fr auto;
      gap: 12px;
      align-items: end;
      margin-top: 14px;
    }
    .good { color: var(--ok); }
    .bad { color: var(--bad); }
    @media (max-width: 980px) {
      main { grid-template-columns: 1fr; }
      aside { border-left: 0; padding-left: 0; }
      .field-grid { grid-template-columns: 1fr 1fr; }
      .subject-row, .send-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1 id="pageTitle">AutoMate Basic Email Template</h1>
    <div id="status" class="status">Ready</div>
  </header>

  <main>
    <section>
      <div class="field-grid">
        <div>
          <label for="emailTypeName">Email Type</label>
          <input id="emailTypeName" value="Booking email" disabled>
        </div>
        <div>
          <label for="serviceType">Service Type</label>
          <select id="serviceType"></select>
        </div>
        <div>
          <label for="templateName">Template Name</label>
          <input id="templateName" value="Booking email">
        </div>
        <div>
          <label for="active">Status</label>
          <select id="active">
            <option value="true">Active</option>
            <option value="false">Inactive</option>
          </select>
        </div>
      </div>

      <div class="subject-row">
        <div>
          <label for="subject">Subject</label>
          <input id="subject" class="insert-target" value="Please Complete Your Inspection Booking">
        </div>
        <div>
          <label for="signedInInspector">Inspector</label>
          <input id="signedInInspector" value="Auto-filled from selected job" disabled>
        </div>
      </div>

      <input id="inspectorId" type="hidden">

      <label for="editor">Email Body</label>
      <div class="editor-tools">
        <button type="button" data-command="bold">B</button>
        <button type="button" data-command="italic"><em>I</em></button>
        <button type="button" data-command="underline"><u>U</u></button>
        <button type="button" data-command="insertUnorderedList">List</button>
        <button type="button" data-command="justifyLeft">Left</button>
        <button type="button" data-command="justifyCenter">Center</button>
        <button type="button" data-command="createLink">Link</button>
        <button type="button" data-command="removeFormat">Clear</button>
      </div>
      <iframe id="editor" class="editor-frame" title="Email body editor"></iframe>
      <textarea id="htmlBody"><!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Booking Confirmation</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  body { font-family: Arial, Helvetica, sans-serif; background-color: #f5f5f5; margin: 0; padding: 0; }
  .container { max-width: 900px; margin: 20px auto; padding: 5%; background-color: #ffffff; border: 1px solid #5c9ccf; border-radius: 4px; }
  .header img { max-width: 50%; height: auto; display: block; margin: 0 auto 18px; }
  h1 { text-align: center; font-weight: bold; margin: 10px 0 20px; }
  p { line-height: 1.5; margin: 10px 0; }
  ul { margin: 10px 0 10px 20px; padding-left: 20px; }
  li { line-height: 1.5; margin: 10px 0; }
  .footer { margin-top: 20px; font-size: 12px; color: #555555; line-height: 1.4; }
  @media only screen and (max-width: 600px) {
    .container { width: 90%; padding: 15px; }
    .header img { max-width: 80%; }
  }
</style>
</head>
<body>
  <div class="container">
    <div class="header">
      <img src="{{LOGO_URL}}" alt="{{COMPANY_NAME}}" style="width:50%; height:auto; display:block; margin:0 auto 18px;">
    </div>

    <h1>Letter of Engagement</h1>

    <p>Hi {{CLIENT_FIRST_NAME}},</p>

    <p>Thank you for choosing {{COMPANY_NAME}} to carry out your property inspection at {{ADDRESS}}.</p>

    <p>As part of our compliance with New Zealand Property Inspection Standard NZS 4306:2005, we must supply you with a letter of engagement containing the following information:</p>

    <ul>
      <li><strong>Inspector:</strong> Your scheduled Inspector is {{INSPECTOR_NAME}}. Please note this may be subject to change at the discretion of {{COMPANY_NAME}}.</li>
      <li><strong>Date &amp; Time:</strong> The inspection has been scheduled for {{INSPECTION_TIME}} on {{INSPECTION_DATE}}.</li>
      <li><strong>Scope:</strong> The scope of the report is limited to the inspection services listed for this booking and any agreed inclusions or exclusions.</li>
      <li><strong>Services:</strong><br>{{SERVICES}}</li>
      <li><strong>Additional Services / Add-ons:</strong><br>{{ADDITIONAL_SERVICES}}</li>
      <li><strong>Terms:</strong> The relevant terms and conditions have been sent with this booking email and must be accepted where required to confirm your booking.</li>
    </ul>

    <p>{{INSPECTION_LINK}}</p>

    <p>We look forward to providing this service for you.</p>

    <p>Best Regards,<br>{{COMPANY_NAME}}</p>

    <div class="footer">
      <p>IMPORTANT: The contents of this email and any attachments are confidential. They are intended for the named recipient(s) only. If you have received this email by mistake, please notify the sender immediately and do not disclose the contents to anyone or make copies thereof.</p>
      <p>Warning: Although taking reasonable precautions to ensure no viruses or malicious software are present in this email, the sender cannot accept responsibility for any loss or damage arising from the use of this email or attachments.</p>
    </div>
  </div>
</body>
</html></textarea>

      <div class="toolbar">
        <button id="saveBtn" class="primary">Save Template</button>
        <button id="previewBtn">Preview With Job</button>
        <button id="restoreBtn">Restore Default</button>
      </div>

      <div class="send-grid">
        <div>
          <label for="jobId">Preview / Send Job ID</label>
          <input id="jobId" autocomplete="off">
        </div>
        <div>
          <label for="toEmail">Send To Override</label>
          <input id="toEmail" autocomplete="off" placeholder="Optional">
        </div>
        <button id="sendTestBtn">Send Test Email</button>
      </div>

      <div class="preview-wrap">
        <div id="previewSubject" class="preview-head">Preview subject will appear here</div>
        <iframe id="preview"></iframe>
      </div>
    </section>

    <aside>
      <h2>Insert Placeholders</h2>
      <div id="placeholders" class="placeholder-list"></div>
    </aside>
  </main>

  <script>
    const query = new URLSearchParams(window.location.search);
    const state = { lastTarget: null, placeholders: [], editorReady: false, previewTimer: null, previewRevision: 0, defaultSubject: "", defaultHtmlBody: "" };
    const eventKey = query.get("eventKey") || "scheduling";
    const recipientKey = query.get("recipientKey") || "contact_1";
    const tenantId = query.get("tenantId") || "";
    const DEFAULT_TEST_INSPECTOR_ID = "dea3f71c-b8ca-4cbb-bbe3-3de48d380ec5";
    const $ = id => document.getElementById(id);

    function setStatus(message, kind) {
      const el = $("status");
      el.textContent = message;
      el.className = "status " + (kind || "");
    }

    function updateJobActionState() {
      const hasJob = $("jobId").value.trim() !== "";
      $("previewBtn").disabled = !hasJob;
    }

    function getInspectorIdForTest() {
      const fromHidden = $("inspectorId").value.trim();
      if (fromHidden) return fromHidden;
      const fromUrl = new URLSearchParams(window.location.search).get("inspectorId");
      if (fromUrl) return fromUrl.trim();
      return DEFAULT_TEST_INSPECTOR_ID;
    }

    function friendlyError(message) {
      const text = message || "";
      if (text.includes("28P01") || text.toLowerCase().includes("password authentication failed")) {
        return "Local database connection needs refreshing before job preview/save can run.";
      }
      if (text.toLowerCase().includes("failed to fetch")) {
        return "API is unavailable. Check that AutoMate API is running.";
      }
      return text.split("\n")[0].split("\r")[0] || "Something went wrong.";
    }

    function templateUrl() {
      if (!tenantId) throw new Error("Company tenant is required.");
      return `/automation/basic/templates/${encodeURIComponent(eventKey)}/${encodeURIComponent(recipientKey)}?tenantId=${encodeURIComponent(tenantId)}`;
    }

    function bodyPayload() {
      return {
        tenantId,
        subject: $("subject").value,
        htmlBody: getEditorHtml()
      };
    }

    function editorDocument() {
      return $("editor").contentDocument || $("editor").contentWindow.document;
    }

    function setEditorHtml(html) {
      const value = html || $("htmlBody").value || "";
      $("htmlBody").value = value;
      state.editorReady = false;
      $("editor").srcdoc = value;
    }

    function prepareEditor() {
      const doc = editorDocument();
      if (!doc || !doc.body) return;
      doc.designMode = "on";
      doc.body.contentEditable = "true";
      doc.body.addEventListener("focus", () => state.lastTarget = $("editor"));
      doc.body.addEventListener("click", () => state.lastTarget = $("editor"));
      doc.body.addEventListener("keyup", () => state.lastTarget = $("editor"));
      doc.body.addEventListener("input", scheduleLivePreview);
      state.editorReady = true;
    }

    function getEditorHtml() {
      try {
        const doc = editorDocument();
        if (doc && doc.documentElement) {
          const html = "<!DOCTYPE html>\n" + doc.documentElement.outerHTML;
          $("htmlBody").value = html;
          return html;
        }
      } catch {}
      return $("htmlBody").value;
    }

    function renderDraftForTest(value) {
      const sampleFields = {
        CLIENT_FIRST_NAME: "Test Client",
        CLIENT_NAME: "Test Client",
        ADDRESS: "123 Test Street, Papamoa",
        PROPERTY_ADDRESS: "123 Test Street, Papamoa",
        INSPECTION_TIME: "10:00 AM",
        INSPECTION_DATE: "14 Jun 2026",
        INSPECTION_END_TIME: "11:30 AM",
        SERVICES: "Building inspection",
        PRIMARY_SERVICE: "Building inspection",
        ADDITIONAL_SERVICES: "Weathertightness Report",
        INSPECTION_LINK: "",
        REPORT_LINK: "",
        INVOICE_LINK: "",
        TERMS_REQUIRED: "Yes",
        BOOKING_TEMPLATE_KEY: "building_inspection"
      };

      return (value || "").replace(/\{\{\s*([A-Z0-9_]+)\s*\}\}/gi, (match, key) => {
        const upper = key.toUpperCase();
        return Object.prototype.hasOwnProperty.call(sampleFields, upper) ? sampleFields[upper] : match;
      });
    }

    function runEditorCommand(command) {
      const doc = editorDocument();
      if (!doc) return;
      let value = null;
      if (command === "createLink") {
        value = window.prompt("Link URL");
        if (!value) return;
      }
      doc.execCommand(command, false, value);
      $("editor").contentWindow.focus();
      getEditorHtml();
    }

    function labelFromKey(value) {
      return (value || "")
        .split("_")
        .filter(Boolean)
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(" ");
    }

    function ensureServiceTypeOption(value) {
      if (!value) return;
      const select = $("serviceType");
      if ([...select.options].some(option => option.value === value)) return;

      let group = [...select.children].find(child => child.tagName === "OPTGROUP" && child.label === "Job-specific templates");
      if (!group) {
        group = document.createElement("optgroup");
        group.label = "Job-specific templates";
        select.appendChild(group);
      }

      const option = document.createElement("option");
      option.value = value;
      option.textContent = labelFromKey(value);
      group.appendChild(option);
    }

    async function api(url, options) {
      const response = await fetch(url, {
        headers: { "Content-Type": "application/json", "X-AutoMate-Inspector-ID": getInspectorIdForTest() },
        ...options
      });
      const text = await response.text();
      let data = null;
      try { data = text ? JSON.parse(text) : null; } catch { data = text; }
      if (!response.ok) {
        const detail = data && (data.detail || data.message || data.title) ? (data.detail || data.message || data.title) : text;
        throw new Error(detail || `Request failed: ${response.status}`);
      }
      return data;
    }

    function insertToken(token) {
      if (state.lastTarget === $("subject")) {
        const target = $("subject");
        const start = target.selectionStart ?? target.value.length;
        const end = target.selectionEnd ?? target.value.length;
        target.value = target.value.slice(0, start) + token + target.value.slice(end);
        target.focus();
        target.selectionStart = target.selectionEnd = start + token.length;
        return;
      }

      const doc = editorDocument();
      if (doc && state.editorReady) {
        $("editor").contentWindow.focus();
        doc.execCommand("insertText", false, token);
        getEditorHtml();
        return;
      }

      const target = $("htmlBody");
      const start = target.selectionStart ?? target.value.length;
      const end = target.selectionEnd ?? target.value.length;
      target.value = target.value.slice(0, start) + token + target.value.slice(end);
      setEditorHtml(target.value);
    }

    async function loadPlaceholders() {
      const data = await api("/email-templates/placeholders");
      state.placeholders = data.placeholders || [];
      $("placeholders").innerHTML = "";
      for (const group of data.categories || []) {
        const heading = document.createElement("h2");
        heading.textContent = group.category;
        $("placeholders").appendChild(heading);

        for (const placeholder of group.placeholders || []) {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.textContent = "+ " + placeholder.label.toUpperCase();
          btn.title = placeholder.token;
          btn.addEventListener("click", () => insertToken(placeholder.token));
          $("placeholders").appendChild(btn);
        }
      }
    }

    async function loadServiceTypes() {
      const data = await api("/email-templates/service-types");
      $("serviceType").innerHTML = "";
      let currentGroup = "";
      let groupElement = null;

      for (const item of data.serviceTypes || []) {
        if (item.group !== currentGroup) {
          currentGroup = item.group;
          groupElement = document.createElement("optgroup");
          groupElement.label = currentGroup;
          $("serviceType").appendChild(groupElement);
        }

        const option = document.createElement("option");
        option.value = item.key;
        option.textContent = item.label;
        groupElement.appendChild(option);
      }
    }

    async function loadTemplate() {
      await resolveInspectorFromJob(false);
      setStatus("Loading...");
      const data = await api(templateUrl());
      $("templateName").value = data.templateName || "";
      $("pageTitle").textContent = data.templateName || "Basic Email Template";
      $("subject").value = data.subject || "";
      setEditorHtml(data.htmlBody || "");
      state.defaultSubject = data.defaultSubject || "";
      state.defaultHtmlBody = data.defaultHtmlBody || "";
      setStatus("Template loaded.", "good");
    }

    async function saveTemplate() {
      await resolveInspectorFromJob(false);
      setStatus("Saving...");
      const data = await api(templateUrl(), {
        method: "PUT",
        body: JSON.stringify(bodyPayload())
      });
      setStatus("Template saved.", data.success ? "good" : "");
    }

    function restoreDefault() {
      if (!state.defaultSubject && !state.defaultHtmlBody) throw new Error("Default template is unavailable.");
      $("subject").value = state.defaultSubject;
      setEditorHtml(state.defaultHtmlBody);
      scheduleLivePreview();
      setStatus("Default loaded as an unsaved draft. Click Save Template to keep it.", "");
    }

    function scheduleLivePreview() {
      clearTimeout(state.previewTimer);
      if (!$('jobId').value.trim()) return;
      state.previewTimer = setTimeout(() => {
        previewTemplate(true).catch(err => setStatus(friendlyError(err.message), "bad"));
      }, 650);
    }

    async function previewTemplate(isAutomatic) {
      const jobId = $("jobId").value.trim();
      if (!jobId) throw new Error("No test job is loaded yet. Refresh the local database connection or paste a Job ID.");
      await resolveInspectorFromJob(false);
      const revision = ++state.previewRevision;
      if (!isAutomatic) setStatus("Rendering preview...");
      const data = await api(`/jobs/${encodeURIComponent(jobId)}/automation/basic-render`, {
        method: "POST",
        body: JSON.stringify({
          tenantId,
          eventKey,
          recipientKey,
          subject: $("subject").value,
          htmlBody: getEditorHtml()
        })
      });
      if (revision !== state.previewRevision) return;
      $("previewSubject").textContent = data.subject || "";
      $("preview").srcdoc = data.htmlBody || "";
      setStatus(isAutomatic ? "Live preview updated." : "Preview rendered.", "good");
    }

    async function sendTestEmail() {
      const toEmail = $("toEmail").value.trim();
      if (!toEmail) throw new Error("Enter your email address in Send To Override first.");
      const jobId = $("jobId").value.trim();
      if (!jobId) throw new Error("A valid preview Job ID is required before sending a test.");

      setStatus("Sending test email through company SMTP...");
      const data = await api("/connector/email-template-test", {
        method: "POST",
        body: JSON.stringify({
          jobId,
          tenantId,
          eventKey,
          recipientKey,
          toEmail,
          serviceTypeKey: $("serviceType").value || null,
          subject: $("subject").value || "Booking email",
          htmlBody: getEditorHtml()
        })
      });
      setStatus(data.message || "Test email sent.", data.success ? "good" : "");
    }

    async function resolveInspectorFromJob(showSuccess) {
      const jobId = $("jobId").value.trim();
      if (!jobId) {
        if (!$("inspectorId").value.trim()) throw new Error("Inspector ID or Job ID is required.");
        return null;
      }

      const data = await api(`/jobs/${encodeURIComponent(jobId)}/email-template-context`);
      if (data.inspectorId) $("inspectorId").value = data.inspectorId;
      $("signedInInspector").value = data.inspectorName || data.inspectorId || "Selected job inspector";
      if (data.clientEmail && !$("toEmail").value.trim()) $("toEmail").value = data.clientEmail;
      if (data.fields && data.fields.BOOKING_TEMPLATE_KEY && data.fields.BOOKING_TEMPLATE_KEY !== "" && $("serviceType").value === "general_booking") {
        ensureServiceTypeOption(data.fields.BOOKING_TEMPLATE_KEY);
        $("serviceType").value = data.fields.BOOKING_TEMPLATE_KEY;
      }
      if (showSuccess) setStatus(`Using inspector ${data.inspectorName || data.inspectorId}`, "good");
      return data;
    }

    async function loadTopJobForTesting() {
      try {
        const jobs = await api("/jobs/latest");
        const first = Array.isArray(jobs) ? jobs[0] : null;
        const jobId = first && (first.job_id || first.jobId || first.JobId);
        if (!jobId) return;
        $("jobId").value = jobId;
        updateJobActionState();
        await resolveInspectorFromJob(false);
        setStatus("Loaded top job for testing.", "good");
      } catch (err) {
        setStatus("Test email mode ready. Job preview needs a working database connection.", "");
        updateJobActionState();
      }
    }

    for (const el of document.querySelectorAll(".insert-target")) {
      el.addEventListener("focus", () => state.lastTarget = el);
      el.addEventListener("click", () => state.lastTarget = el);
      el.addEventListener("keyup", () => state.lastTarget = el);
    }

    $("editor").addEventListener("load", prepareEditor);
    setEditorHtml($("htmlBody").value);
    $("jobId").addEventListener("input", () => { updateJobActionState(); scheduleLivePreview(); });
    $("subject").addEventListener("input", scheduleLivePreview);
    $("htmlBody").addEventListener("input", scheduleLivePreview);
    $("serviceType").addEventListener("change", scheduleLivePreview);
    updateJobActionState();

    for (const btn of document.querySelectorAll(".editor-tools button")) {
      btn.addEventListener("click", () => runEditorCommand(btn.dataset.command));
    }

    $("saveBtn").addEventListener("click", () => saveTemplate().catch(err => setStatus(friendlyError(err.message), "bad")));
    $("restoreBtn").addEventListener("click", () => { try { restoreDefault(); } catch (err) { setStatus(friendlyError(err.message), "bad"); } });
    $("previewBtn").addEventListener("click", () => previewTemplate(false).catch(err => setStatus(friendlyError(err.message), "bad")));
    $("sendTestBtn").addEventListener("click", () => sendTestEmail().catch(err => setStatus(friendlyError(err.message), "bad")));

    Promise.all([loadServiceTypes(), loadPlaceholders(), loadTemplate()])
      .then(() => loadTopJobForTesting())
      .catch(err => setStatus(friendlyError(err.message), "bad"));
  </script>
</body>
</html>
""";
}

static DateTime? ParseNullableDateTime(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    if (DateTime.TryParse(value, out var parsed))
        return parsed;

    return null;
}

static decimal? ParseNullableDecimal(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    var cleaned = value.Trim().Replace("$", "").Replace(",", "");

    if (decimal.TryParse(cleaned, out var parsed))
        return parsed;

    return null;
}

static string BuildBookingTemplateKey(ServicesSection? services)
{
    if (services == null)
        return "general_booking";

    if (!string.IsNullOrWhiteSpace(services.BookingTemplateKey))
        return services.BookingTemplateKey;

    var keys = new[]
    {
        string.IsNullOrWhiteSpace(services.PrimaryServiceKey) ? InferCanonicalServiceType(services.Primary) : services.PrimaryServiceKey,
        string.IsNullOrWhiteSpace(services.Additional1ServiceKey) ? InferCanonicalServiceType(services.Additional1) : services.Additional1ServiceKey,
        string.IsNullOrWhiteSpace(services.Additional2ServiceKey) ? InferCanonicalServiceType(services.Additional2) : services.Additional2ServiceKey
    }
    .Where(k => !string.IsNullOrWhiteSpace(k) && k != "other" && !IsModifierServiceKey(k))
    .Distinct()
    .ToList();

    return keys.Count == 0 ? "general_booking" : string.Join("_", keys);
}

static bool ShouldRequireTermsForBooking(ServicesSection? services)
{
    if (services == null)
        return false;

    var bookingTemplateKey = BuildBookingTemplateKey(services);
    if (IsBuildingInspectionTermsKey(bookingTemplateKey))
        return true;

    return IsBuildingInspectionTermsKey(services.PrimaryServiceKey)
        || IsBuildingInspectionTermsKey(InferCanonicalServiceType(services.Primary));
}

static bool ShouldRequireTermsForService(ServiceCatalogItemInput item)
{
    return IsBuildingInspectionTermsKey(item.BookingTemplateKey)
        || IsBuildingInspectionTermsKey(item.CanonicalServiceType)
        || IsBuildingInspectionTermsKey(item.ListItemName)
        || IsBuildingInspectionTermsKey(item.InvoiceItemName);
}

static bool IsBuildingInspectionTermsKey(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return false;

    var normalized = NormalizeServiceTypeKey(value);
    if (normalized == "building_inspection"
        || normalized.StartsWith("building_inspection_", StringComparison.OrdinalIgnoreCase))
        return true;

    var inferred = NormalizeServiceTypeKey(InferCanonicalServiceType(value));
    return inferred == "building_inspection"
        || inferred.StartsWith("building_inspection_", StringComparison.OrdinalIgnoreCase);
}

static string InferCanonicalServiceType(string? serviceName)
{
    if (string.IsNullOrWhiteSpace(serviceName))
        return "";

    var value = serviceName.Trim().ToLowerInvariant();

    if (value.Contains("healthy") || value.Contains("hhs"))
        return "healthy_homes";

    if (value.Contains("meth"))
        return "meth_test";

    if (value.Contains("weathertight") || value.Contains("weather tight") || value.Contains("weather-tight"))
        return "weathertightness";

    if (value.Contains("pre-purchase") || value.Contains("pre purchase") || value.Contains("ppi") || value.Contains("building report") || value.Contains("property inspection"))
        return "pre_purchase";

    if (value.Contains("pre-sale") || value.Contains("pre sale"))
        return "pre_sale";

    if (value.Contains("reinspect") || value.Contains("re-inspect") || value.Contains("reinspection"))
        return "reinspection";

    if (value.Contains("travel"))
        return "travel_fee";

    if (value.Contains("council") || value.Contains("file"))
        return "council_file_review";

    if (value.Contains("asbestos"))
        return "asbestos_test";

    if (value.Contains("moisture"))
        return "moisture_check";

    if (value.Contains("thermal"))
        return "thermal_imaging";

    if (value.Contains("pool"))
        return "pool_inspection";

    return "other";
}

static string ToScopeLabel(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return "Not specified";

    return NormalizeScopeValue(value);
}

static string BuildScopeHtml(string question, string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return "";

    var label = NormalizeScopeValue(value);
    return "<br>" +
        System.Net.WebUtility.HtmlEncode(question) +
        " - " +
        System.Net.WebUtility.HtmlEncode(label);
}

static string NormalizeScopeValue(string value)
{
    var trimmed = value.Trim();
    var normalized = trimmed.ToLowerInvariant();

    if (normalized == "yes" || normalized == "true" || normalized == "included" || normalized == "include")
        return "Yes";

    if (normalized == "no" || normalized == "false" || normalized == "excluded" || normalized == "exclude")
        return "No";

    return trimmed;
}

static string BuildAdditionalServicesText(string? additional1, string? additional2)
{
    var services = new[] { additional1, additional2 }
        .Where(value => !string.IsNullOrWhiteSpace(value))
        .Select(value => value!.Trim())
        .ToList();

    return services.Count == 0 ? "None" : string.Join(Environment.NewLine, services);
}

static string BuildAdditionalServicesHtml(string? additional1, string? additional2)
{
    var services = new[] { additional1, additional2 }
        .Where(value => !string.IsNullOrWhiteSpace(value))
        .Select(value => System.Net.WebUtility.HtmlEncode(value!.Trim()))
        .ToList();

    return services.Count == 0 ? "None" : string.Join("<br>", services);
}

static async Task EnsureJobPaymentColumnsAsync(NpgsqlConnection conn)
{
    const string sql = @"
ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS age_of_building text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS marked_as_paid_override boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_available boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS job_total decimal(10,2) NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS amount_paid decimal(10,2) NOT NULL DEFAULT 0;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS amount_outstanding decimal(10,2) NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS payment_status text NOT NULL DEFAULT 'unpaid';

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS primary_service_key text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS additional1_service_key text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS additional2_service_key text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_template_key text NOT NULL DEFAULT 'general_booking';

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS booking_email_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS terms_required boolean NOT NULL DEFAULT false;

ALTER TABLE public.jobs_staging
ALTER COLUMN terms_required SET DEFAULT false;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS invoice_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS calendar_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS report_required boolean NOT NULL DEFAULT true;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS building_type text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS stories text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS bedrooms text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS bathrooms text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS monolithic text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS outbuilding text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS occupied text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS attached_flat text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS travel_fee text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_bedrooms text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS meth_samples text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_reinspect text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS council_files text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS foundation_space text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS weathertightness text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_reinspect_date text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS access_by text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_compliance text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS xero_contact_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS xero_invoice_id text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS xero_invoice_number text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS xero_invoice_status text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS xero_invoice_created_at timestamptz NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS xero_last_error text NULL;
";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task EnsureJobInvoiceLinesTableAsync(NpgsqlConnection conn)
{
    const string sql = @"
CREATE TABLE IF NOT EXISTS public.job_invoice_lines
(
    job_id uuid NOT NULL,
    line_index integer NOT NULL,
    description text NULL,
    quantity decimal(10,2) NOT NULL DEFAULT 1,
    unit_price decimal(10,2) NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, line_index)
);

CREATE INDEX IF NOT EXISTS idx_job_invoice_lines_job_id
ON public.job_invoice_lines(job_id);";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

static async Task RefreshJobInvoiceLinesAsync(NpgsqlConnection conn, JobUploadRequest payload, Guid jobId)
{
    await EnsureJobInvoiceLinesTableAsync(conn);

    await using (var deleteCmd = new NpgsqlCommand("DELETE FROM public.job_invoice_lines WHERE job_id = @job_id;", conn))
    {
        deleteCmd.Parameters.AddWithValue("job_id", jobId);
        await deleteCmd.ExecuteNonQueryAsync();
    }

    if (payload.InvoiceLines == null || payload.InvoiceLines.Count == 0)
        return;

    const string insertSql = @"
INSERT INTO public.job_invoice_lines
(
    job_id,
    line_index,
    description,
    quantity,
    unit_price,
    created_at,
    updated_at
)
VALUES
(
    @job_id,
    @line_index,
    @description,
    @quantity,
    @unit_price,
    NOW(),
    NOW()
)
ON CONFLICT (job_id, line_index)
DO UPDATE SET
    description = EXCLUDED.description,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    updated_at = NOW();";

    foreach (var line in payload.InvoiceLines.OrderBy(line => line.LineIndex))
    {
        var lineIndex = line.LineIndex <= 0 ? payload.InvoiceLines.IndexOf(line) + 1 : line.LineIndex;
        var quantity = line.Quantity <= 0m ? 1m : line.Quantity;

        await using var cmd = new NpgsqlCommand(insertSql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("line_index", lineIndex);
        cmd.Parameters.AddWithValue("description", line.Description ?? "");
        cmd.Parameters.AddWithValue("quantity", quantity);
        cmd.Parameters.AddWithValue("unit_price", line.UnitPrice);
        await cmd.ExecuteNonQueryAsync();
    }
}

static string CleanEditorHtml(string? html)
{
    if (string.IsNullOrWhiteSpace(html))
        return "";

    return Regex.Replace(
        html,
        "\\scontenteditable=(\"true\"|'true'|true)",
        "",
        RegexOptions.IgnoreCase);
}

static string SanitizeBasicTemplateHtml(string? html)
{
    var clean = CleanEditorHtml(html);
    clean = Regex.Replace(clean, @"<(script|iframe|object|embed|form|meta|link|base)\b[^>]*>.*?</\1\s*>", "", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    clean = Regex.Replace(clean, @"<(script|iframe|object|embed|form|meta|link|base)\b[^>]*/?>", "", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    clean = Regex.Replace(clean, """(?is)@import\s+[^;]+;|expression\s*\([^)]*\)|behavior\s*:[^;]+;|url\s*\(\s*['"]?\s*(?:javascript:|data:text/html)[^)]*\)""", "");
    clean = Regex.Replace(clean, """\s+on[a-z0-9_-]+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+)""", "", RegexOptions.IgnoreCase);
    clean = Regex.Replace(clean, "(href|src)\\s*=\\s*\"\\s*(?:javascript:|data:text/html)[^\"]*\"", "$1=\"#\"", RegexOptions.IgnoreCase);
    clean = Regex.Replace(clean, """(href|src)\s*=\s*'\s*(?:javascript:|data:text/html)[^']*'""", "$1=\"#\"", RegexOptions.IgnoreCase);
    return clean;
}

static string RenderTestEmailBody(
    string htmlBody,
    string? companyName,
    string? contactName,
    string? emailFromName,
    string? emailFromAddress,
    string? phone,
    string? logoUrl)
{
    const string fallbackCompanyName = "Pro-Spect Building Reports Ltd";
    const string fallbackLogoUrl = "https://pro-spect.co.nz/wp-content/uploads/2023/11/Pro-Spect-report-transparent.png";

    var company = string.IsNullOrWhiteSpace(companyName) ? fallbackCompanyName : companyName.Trim();
    var logo = string.IsNullOrWhiteSpace(logoUrl) ? fallbackLogoUrl : logoUrl.Trim();
    var inspector = !string.IsNullOrWhiteSpace(emailFromName)
        ? emailFromName.Trim()
        : !string.IsNullOrWhiteSpace(contactName)
            ? contactName.Trim()
            : "Inspector";

    var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["LOGO_URL"] = SafeHtml(logo),
        ["COMPANY_LOGO_URL"] = SafeHtml(logo),
        ["COMPANY_NAME"] = SafeHtml(company),
        ["INSPECTOR_NAME"] = SafeHtml(inspector),
        ["INSPECTOR_FIRST_NAME"] = SafeHtml(FirstWord(inspector)),
        ["INSPECTOR_PHONE"] = SafeHtml(phone),
        ["INSPECTOR_EMAIL"] = SafeHtml(emailFromAddress)
    };

    return CleanEditorHtml(Regex.Replace(
        htmlBody ?? "",
        "\\{\\{\\s*([A-Z0-9_]+)\\s*\\}\\}",
        match =>
        {
            var key = match.Groups[1].Value;
            return fields.TryGetValue(key, out var replacement) ? replacement : match.Value;
        },
        RegexOptions.IgnoreCase));
}

static string SafeHtml(string? value)
{
    return System.Net.WebUtility.HtmlEncode(value ?? "");
}

static string FirstWord(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return "";

    return value.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? "";
}

static string FirstNonBlank(params string?[] values)
{
    foreach (var value in values)
    {
        if (!string.IsNullOrWhiteSpace(value))
            return value.Trim();
    }

    return "";
}

static async Task UpsertInspectorAsync(NpgsqlConnection conn, TrialRegistrationRequest request, string onboardingStatus)
{
    const string inspectorSql = @"
INSERT INTO public.inspectors
(
    inspector_id,
    tenant_id,
    inspector_name,
    company_name,
    contact_name,
    email_from_name,
    email_from_address,
    onboarding_status,
    updated_at
)
VALUES
(
    @inspector_id,
    @tenant_id,
    @inspector_name,
    @company_name,
    @contact_name,
    @email_from_name,
    @email_from_address,
    @onboarding_status,
    NOW()
)
ON CONFLICT (inspector_id) DO UPDATE
SET
    tenant_id = EXCLUDED.tenant_id,
    inspector_name = EXCLUDED.inspector_name,
    company_name = EXCLUDED.company_name,
    contact_name = EXCLUDED.contact_name,
    email_from_name = EXCLUDED.email_from_name,
    email_from_address = EXCLUDED.email_from_address,
    onboarding_status = EXCLUDED.onboarding_status,
    updated_at = NOW();";

    await using var inspectorCmd = new NpgsqlCommand(inspectorSql, conn);
    inspectorCmd.Parameters.AddWithValue("inspector_id", request.InspectorId);
    inspectorCmd.Parameters.AddWithValue("tenant_id", request.TenantId);
    inspectorCmd.Parameters.AddWithValue("inspector_name", FirstNonBlank(request.InspectorName, request.Email));
    inspectorCmd.Parameters.AddWithValue("company_name", request.CompanyName ?? "");
    inspectorCmd.Parameters.AddWithValue("contact_name", request.InspectorName ?? "");
    inspectorCmd.Parameters.AddWithValue("email_from_name", FirstNonBlank(request.InspectorName, request.CompanyName, request.Email));
    inspectorCmd.Parameters.AddWithValue("email_from_address", request.Email.Trim());
    inspectorCmd.Parameters.AddWithValue("onboarding_status", onboardingStatus);
    await inspectorCmd.ExecuteNonQueryAsync();
}

static async Task<CompanyAccountStatus?> LoadCompanyAccountByTenantAsync(NpgsqlConnection conn, Guid tenantId)
{
    const string sql = @"
SELECT
    i.inspector_id,
    i.tenant_id,
    i.inspector_name,
    i.company_name,
    i.email_from_address,
    i.created_at AS company_start_at,
    COALESCE(s.status, 'not_registered') AS status,
    s.trial_ends_at,
    COALESCE(s.plan_name, '') AS plan_name
FROM public.inspectors i
LEFT JOIN public.subscriptions s
    ON s.inspector_id = i.inspector_id
WHERE i.tenant_id = @tenant_id
ORDER BY
    CASE
        WHEN s.status = 'active' THEN 0
        WHEN s.status = 'trialing' AND s.trial_ends_at > NOW() THEN 1
        WHEN s.status = 'trialing' THEN 2
        ELSE 3
    END,
    s.trial_ends_at DESC NULLS LAST,
    i.created_at ASC
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("tenant_id", tenantId);
    return await ReadCompanyAccountStatusAsync(cmd);
}

static async Task<CompanyAccountStatus?> LoadCompanyAccountByInspectorAsync(NpgsqlConnection conn, Guid inspectorId)
{
    const string sql = @"
SELECT
    i.inspector_id,
    i.tenant_id,
    i.inspector_name,
    i.company_name,
    i.email_from_address,
    i.created_at AS company_start_at,
    COALESCE(s.status, 'not_registered') AS status,
    s.trial_ends_at,
    COALESCE(s.plan_name, '') AS plan_name
FROM public.inspectors i
LEFT JOIN public.subscriptions s
    ON s.inspector_id = i.inspector_id
WHERE i.inspector_id = @inspector_id
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("inspector_id", inspectorId);
    return await ReadCompanyAccountStatusAsync(cmd);
}

static async Task<CompanyAccountStatus?> ReadCompanyAccountStatusAsync(NpgsqlCommand cmd)
{
    await using var reader = await cmd.ExecuteReaderAsync();
    if (!await reader.ReadAsync())
        return null;

    return new CompanyAccountStatus
    {
        InspectorId = reader["inspector_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["inspector_id"],
        TenantId = reader["tenant_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["tenant_id"],
        InspectorName = reader["inspector_name"]?.ToString() ?? "",
        CompanyName = reader["company_name"]?.ToString() ?? "",
        Email = reader["email_from_address"]?.ToString() ?? "",
        Status = reader["status"]?.ToString() ?? "not_registered",
        PlanName = reader["plan_name"]?.ToString() ?? "",
        TrialEndsAt = reader["trial_ends_at"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["trial_ends_at"]),
        CompanyStartAt = reader["company_start_at"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["company_start_at"])
    };
}

static object BuildAccountResponse(
    bool success,
    bool allowed,
    string status,
    string message,
    TrialRegistrationRequest request,
    DateTime? trialEndsAt,
    DateTime? companyStartAt,
    int daysRemaining,
    string registeredEmail,
    Guid registeredInspectorId)
{
    return new
    {
        success,
        allowed,
        status,
        message,
        tenantId = request.TenantId,
        inspectorId = request.InspectorId,
        registeredInspectorId,
        email = request.Email.Trim(),
        registeredEmail,
        trialEndsAt,
        companyStartAt,
        daysRemaining
    };
}

static async Task<DateTime?> GetTrialEndsAtAsync(NpgsqlConnection conn, Guid inspectorId)
{
    const string sql = @"
SELECT trial_ends_at
FROM public.subscriptions
WHERE inspector_id = @inspector_id
ORDER BY created_at DESC
LIMIT 1;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("inspector_id", inspectorId);
    var result = await cmd.ExecuteScalarAsync();

    return result == null || result == DBNull.Value
        ? (DateTime?)null
        : Convert.ToDateTime(result);
}

static int CalculateTrialDaysRemaining(DateTime? trialEndsAt)
{
    if (!trialEndsAt.HasValue)
        return 0;

    var remaining = trialEndsAt.Value.ToUniversalTime() - DateTime.UtcNow;
    if (remaining <= TimeSpan.Zero)
        return 0;

    return Math.Max(1, (int)Math.Ceiling(remaining.TotalDays));
}

public class BookingEmailFailureRequest
{
    public string? ErrorMessage { get; set; }
}

public class TrialRegistrationRequest
{
    [JsonPropertyName("tenant_id")]
    public Guid TenantId { get; set; }

    [JsonPropertyName("inspector_id")]
    public Guid InspectorId { get; set; }

    [JsonPropertyName("inspector_name")]
    public string InspectorName { get; set; } = "";

    [JsonPropertyName("company_name")]
    public string CompanyName { get; set; } = "";

    [JsonPropertyName("email")]
    public string Email { get; set; } = "";

    [JsonPropertyName("access_code")]
    public string AccessCode { get; set; } = "";
}

public class CompanyAccountStatus
{
    public Guid InspectorId { get; set; }
    public Guid TenantId { get; set; }
    public string InspectorName { get; set; } = "";
    public string CompanyName { get; set; } = "";
    public string Email { get; set; } = "";
    public string Status { get; set; } = "";
    public string PlanName { get; set; } = "";
    public DateTime? TrialEndsAt { get; set; }
    public DateTime? CompanyStartAt { get; set; }
}

public class AutomationActivationRequest
{
    public Guid TenantId { get; set; }
    public string ActivationMode { get; set; } = "selected_jobs";
    public string? ChangedBy { get; set; }
}

public class AutomationJobSelectionRequest
{
    public Guid TenantId { get; set; }
    public bool UseAdvancedWorkflows { get; set; }
    public string? ChangedBy { get; set; }
}

public class AutomationTemplateSaveRequest
{
    public Guid TenantId { get; set; }
    public Guid TemplateId { get; set; }
    public string? TemplateType { get; set; }
    public string? ServiceTypeKey { get; set; }
    public string Name { get; set; } = "";
    public string? Subject { get; set; }
    public string? HtmlBody { get; set; }
    public bool IsActive { get; set; } = true;
}

public class AutomationTemplateArchiveRequest
{
    public Guid TenantId { get; set; }
}

public record WorkflowActionFailureRequest(string ErrorMessage);
public record TermsFailureRequest(string ErrorMessage);
public record InvoiceFailureRequest(string ErrorMessage);
public record CalendarFailureRequest(string ErrorMessage);
public record ReportFailureRequest(string ErrorMessage);
public record GoogleCalendarSelectionRequest(string InspectorId, string CalendarId);
public record EmailSenderModeRequest(string InspectorId, string SenderMode);
public class EmailTemplateSaveRequest
{
    public string? EmailType { get; set; }
    public string? ServiceTypeKey { get; set; }
    public string? Name { get; set; }
    public string? Subject { get; set; }
    public string? HtmlBody { get; set; }
    public bool IsActive { get; set; } = true;
}

public class EmailTemplateRenderRequest
{
    public string? EmailType { get; set; }
    public string? ServiceTypeKey { get; set; }
    public string? Subject { get; set; }
    public string? HtmlBody { get; set; }
    public string? ToEmail { get; set; }
    public string? ActionKey { get; set; }
}

public class EmailTemplateSendRequest : EmailTemplateRenderRequest
{
    public bool MarkWorkflowComplete { get; set; } = true;
}

public record EmailTemplateResult(
    Guid TemplateId,
    Guid InspectorId,
    string TemplateType,
    string ServiceTypeKey,
    string Name,
    string Subject,
    string HtmlBody,
    bool IsActive,
    string CreatedAt,
    string UpdatedAt);

public record RenderedEmailTemplate(
    Guid JobId,
    Guid InspectorId,
    string EmailSenderMode,
    string ToEmail,
    string Subject,
    string HtmlBody,
    string ServiceTypeKey,
    string ServiceLabel,
    string ActionKey,
    Dictionary<string, string> Fields);
public class JobWorkflowRequirementsRequest
{
    public bool? BookingEmailRequired { get; set; }
    public bool? TermsRequired { get; set; }
    public bool? InvoiceRequired { get; set; }
    public bool? CalendarRequired { get; set; }
    public bool? ReportRequired { get; set; }
}

public record WorkflowActionSeed(
    Guid JobId,
    Guid TenantId,
    Guid InspectorId,
    string ActionKey,
    string ActionType,
    string ServiceKey,
    string ServiceLabel,
    string ServiceSlot);
public record V1MappingField(
    string CanonicalFieldName,
    string ThreedColumnName,
    string ThreedLabel,
    bool CanAffectPricing,
    bool V1Enabled,
    string ServiceScope,
    string Notes);

public class MappingDiscoverySyncRequest
{
    public string ConnectorVersion { get; set; } = "";
    public string SourceInstance { get; set; } = "";
    public List<MappingFieldInput> FieldMappings { get; set; } = new();
    public List<ServiceCatalogItemInput> ServiceCatalogItems { get; set; } = new();
}

public class ConfirmMappingsRequest
{
    public List<MappingFieldInput> FieldMappings { get; set; } = new();
}

public class MappingFieldInput
{
    public string CanonicalFieldName { get; set; } = "";
    public string ThreedColumnName { get; set; } = "";
    public string ThreedLabel { get; set; } = "";
    public string SourceTableName { get; set; } = "";
    public string SourceListName { get; set; } = "";
    public string InvoiceItemId { get; set; } = "";
    public string InvoiceItemName { get; set; } = "";
    public bool CanAffectPricing { get; set; } = false;
    public bool V1Enabled { get; set; } = true;
    public string ServiceScope { get; set; } = "";
    public string Notes { get; set; } = "";
}

public class ServiceCatalogItemInput
{
    public string CatalogItemKey { get; set; } = "";
    public string ListItemId { get; set; } = "";
    public string ListItemName { get; set; } = "";
    public string ListName { get; set; } = "";
    public string InvoiceItemId { get; set; } = "";
    public string InvoiceItemName { get; set; } = "";
    public decimal? UnitPrice { get; set; }
    public bool IsActive { get; set; } = true;
    public string CanonicalServiceType { get; set; } = "other";
    public string BookingTemplateKey { get; set; } = "general_booking";
    public bool PricingAffects { get; set; } = true;
    public bool BookingEmailRequired { get; set; } = true;
    public bool? TermsRequired { get; set; }
    public bool InvoiceRequired { get; set; } = true;
    public bool CalendarRequired { get; set; } = true;
    public bool ReportRequired { get; set; } = true;
}

public class SendTestEmailRequest
{
    public string InspectorId { get; set; } = "";
    public string ToEmail { get; set; } = "";
    public string Subject { get; set; } = "";
    public string Body { get; set; } = "";
}

public record EmailTemplateServiceType(string Key, string Label, string Group);

public class XeroTestConnectionRequest
{
    public string InspectorId { get; set; } = "";
}

public record XeroAccountResult(
    bool Success,
    string? AccessToken,
    string? RefreshToken,
    string? TenantId,
    string? TenantName,
    string? ErrorMessage)
{
    public static XeroAccountResult Ok(string accessToken, string? refreshToken, string? tenantId, string? tenantName)
    {
        return new XeroAccountResult(true, accessToken, refreshToken, tenantId, tenantName, null);
    }

    public static XeroAccountResult Failure(string errorMessage)
    {
        return new XeroAccountResult(false, null, null, null, null, errorMessage);
    }
}

public record XeroInvoiceJobInput(
    Guid JobId,
    Guid InspectorId,
    string JobName,
    string SiteAddress,
    DateTime? JobDate,
    decimal? JobTotal,
    string PrimaryService,
    string ContactName,
    string ContactEmail,
    string ContactPhone,
    string XeroContactId,
    string XeroInvoiceId,
    string XeroInvoiceNumber,
    string XeroInvoiceStatus);

public record XeroInvoiceLineInput(
    string Description,
    decimal Quantity,
    decimal UnitAmount,
    int LineIndex);

public record ScheduleActionResult(
    string Action,
    bool Success,
    bool Skipped,
    string Message,
    object? Details = null)
{
    public static ScheduleActionResult Ok(string action, string message, object? details = null)
    {
        return new ScheduleActionResult(action, true, false, message, details);
    }

    public static ScheduleActionResult Skip(string action, string message, object? details = null)
    {
        return new ScheduleActionResult(action, true, true, message, details);
    }

    public static ScheduleActionResult Failed(string action, string message, object? details = null)
    {
        return new ScheduleActionResult(action, false, false, message, details);
    }
}

public record IntegrationAccountResult(
    bool Success,
    string? AccessToken,
    string? RefreshToken,
    string? TenantId,
    string? AccountName,
    string? ErrorMessage)
{
    public string? CalendarId => TenantId;

    public static IntegrationAccountResult Ok(string accessToken, string? refreshToken, string? tenantId, string? accountName)
    {
        return new IntegrationAccountResult(true, accessToken, refreshToken, tenantId, accountName, null);
    }

    public static IntegrationAccountResult Failure(string errorMessage)
    {
        return new IntegrationAccountResult(false, null, null, null, null, errorMessage);
    }
}

public class SignNowTemplateMappingsRequest
{
    public List<SignNowTemplateMappingInput> Mappings { get; set; } = new();
}

public class SignNowTemplateMappingInput
{
    public string TemplateKey { get; set; } = "";
    public string TemplateId { get; set; } = "";
    public string TemplateName { get; set; } = "";
}

public record SignNowTemplateMappingResult(
    string TemplateKey,
    string TemplateId,
    string TemplateName,
    string UpdatedAt);

public record SignNowTemplateResult(
    string Id,
    string Name,
    string UpdatedAt,
    string SourceEndpoint,
    string SourceType);

public record SignNowTemplateEndpoint(
    string SourceType,
    string Url);

public record SignNowTemplateLookupResult(
    List<SignNowTemplateResult> Templates,
    object[] Diagnostics,
    int SuccessfulEndpointCount,
    string LastEndpoint,
    int LastStatusCode,
    string LastResponse);

public static class OnlinePropertySupport
{
public static async Task EnsureOnlinePropertyTablesAsync(NpgsqlConnection conn)
{
    const string sql = @"
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_wind_zone text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_exposure_zone text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_lookup_status text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_latitude double precision NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_longitude double precision NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_address_fingerprint text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_retrieved_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS branz_lookup_error text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS property_features_json jsonb NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS property_features_status text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS property_features_address_fingerprint text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS property_features_retrieved_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS property_features_error text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS previous_site_address text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS address_change_pending boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS address_change_detected_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS address_change_confirmed_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS address_change_confirmed_by text NULL;
CREATE TABLE IF NOT EXISTS public.online_property_addresses
(
 job_id uuid NOT NULL, tenant_id uuid NULL, address_fingerprint text NOT NULL, address_snapshot text NOT NULL,
 first_seen_at timestamptz NOT NULL DEFAULT NOW(), PRIMARY KEY(job_id, address_fingerprint)
);
CREATE TABLE IF NOT EXISTS public.online_property_lookup_audit
(
 audit_id bigserial PRIMARY KEY, job_id uuid NOT NULL, tenant_id uuid NULL, source text NOT NULL,
 address_fingerprint text NOT NULL, reason text NOT NULL, outcome text NOT NULL, error text NULL,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.address_change_audit
(
 audit_id bigserial PRIMARY KEY, job_id uuid NOT NULL, tenant_id uuid NULL, previous_address text NULL,
 new_address text NOT NULL, confirmed_by text NULL, prior_workflow_json jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE OR REPLACE FUNCTION public.clear_completed_address_change() RETURNS trigger AS $$
BEGIN
 IF OLD.address_change_pending
    AND NEW.address_change_pending
    AND (NOT NEW.booking_email_required OR NEW.booking_email_sent)
    AND (NOT NEW.terms_required OR NEW.terms_sent)
    AND (NOT NEW.calendar_required OR NEW.calendar_created) THEN
   NEW.address_change_pending := false;
 END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_clear_completed_address_change ON public.jobs_staging;
CREATE TRIGGER trg_clear_completed_address_change BEFORE UPDATE ON public.jobs_staging
FOR EACH ROW EXECUTE FUNCTION public.clear_completed_address_change();
UPDATE public.jobs_staging
SET address_change_pending = true
WHERE address_change_pending = false
  AND address_change_detected_at IS NOT NULL
  AND address_change_detected_at >= TIMESTAMPTZ '2026-07-11 00:00:00+00'
  AND address_change_confirmed_at IS NULL
  AND COALESCE(previous_site_address, '') <> ''
  AND (
       booking_email_sent
    OR terms_sent
    OR calendar_created
    OR invoice_sent
  );";
    await using var cmd = new NpgsqlCommand(sql, conn); await cmd.ExecuteNonQueryAsync();
}

public static async Task<(Guid TenantId, Guid InspectorId, string Address)?> LoadOnlinePropertyJobAsync(NpgsqlConnection conn, Guid jobId, Guid? tenantId)
{
    const string sql = "SELECT tenant_id, inspector_id, site_address FROM public.jobs_staging WHERE job_id=@job_id AND (@tenant_id IS NULL OR tenant_id::text=@tenant_id) LIMIT 1";
    await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.Add("tenant_id", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId.HasValue ? tenantId.Value.ToString() : DBNull.Value;
    await using var reader = await cmd.ExecuteReaderAsync(); if (!await reader.ReadAsync()) return null;
    Guid.TryParse(reader["tenant_id"]?.ToString(), out var parsedTenantId);
    return (parsedTenantId, (Guid)reader["inspector_id"], reader["site_address"]?.ToString() ?? "");
}

public static async Task<Dictionary<string, object?>?> LoadOnlinePropertyDataAsync(NpgsqlConnection conn, Guid jobId, Guid? tenantId)
{
    const string sql = @"SELECT site_address, previous_site_address, address_change_pending, address_change_detected_at,
property_features_json, property_features_status, property_features_address_fingerprint, property_features_retrieved_at, property_features_error,
branz_wind_zone, branz_exposure_zone, branz_lookup_status, branz_address_fingerprint, branz_retrieved_at, branz_lookup_error
FROM public.jobs_staging WHERE job_id=@job_id AND (@tenant_id IS NULL OR tenant_id::text=@tenant_id) LIMIT 1";
    await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.Add("tenant_id", NpgsqlTypes.NpgsqlDbType.Text).Value = tenantId.HasValue ? tenantId.Value.ToString() : DBNull.Value;
    await using var reader = await cmd.ExecuteReaderAsync(); if (!await reader.ReadAsync()) return null;
    object? features = null;
    if (reader["property_features_json"] != DBNull.Value) features = JsonSerializer.Deserialize<object>(reader["property_features_json"].ToString() ?? "null");
    return new Dictionary<string, object?>
    {
        ["success"] = true, ["siteAddress"] = reader["site_address"]?.ToString() ?? "", ["previousSiteAddress"] = reader["previous_site_address"]?.ToString() ?? "",
        ["addressChangePending"] = reader["address_change_pending"] != DBNull.Value && (bool)reader["address_change_pending"], ["addressChangeDetectedAt"] = reader["address_change_detected_at"] == DBNull.Value ? null : reader["address_change_detected_at"],
        ["propertyFeatures"] = features, ["propertyFeaturesStatus"] = reader["property_features_status"]?.ToString() ?? "missing", ["propertyFeaturesFingerprint"] = reader["property_features_address_fingerprint"]?.ToString() ?? "",
        ["propertyFeaturesRetrievedAt"] = reader["property_features_retrieved_at"] == DBNull.Value ? null : reader["property_features_retrieved_at"], ["propertyFeaturesError"] = reader["property_features_error"]?.ToString() ?? "",
        ["windZone"] = reader["branz_wind_zone"]?.ToString() ?? "", ["exposureZone"] = reader["branz_exposure_zone"]?.ToString() ?? "", ["branzStatus"] = reader["branz_lookup_status"]?.ToString() ?? "missing",
        ["branzFingerprint"] = reader["branz_address_fingerprint"]?.ToString() ?? "", ["branzRetrievedAt"] = reader["branz_retrieved_at"] == DBNull.Value ? null : reader["branz_retrieved_at"], ["branzError"] = reader["branz_lookup_error"]?.ToString() ?? ""
    };
}

public static async Task<bool> HasOnlinePropertyEntitlementAsync(NpgsqlConnection conn, Guid tenantId, Guid inspectorId)
{
    const string tenantSql = @"
SELECT s.status, s.trial_ends_at
FROM public.inspectors i
LEFT JOIN public.subscriptions s ON s.inspector_id=i.inspector_id
WHERE i.tenant_id=@tenant_id
ORDER BY CASE WHEN s.status='active' THEN 0 WHEN s.status='trialing' AND s.trial_ends_at>NOW() THEN 1 ELSE 2 END,
         s.trial_ends_at DESC NULLS LAST, i.created_at ASC
LIMIT 1";
    if (tenantId != Guid.Empty)
    {
        await using var tenantCmd = new NpgsqlCommand(tenantSql, conn); tenantCmd.Parameters.AddWithValue("tenant_id", tenantId);
        await using var reader = await tenantCmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            var status = reader["status"]?.ToString() ?? "";
            var trialEndsAt = reader["trial_ends_at"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(reader["trial_ends_at"]);
            return status.Equals("active", StringComparison.OrdinalIgnoreCase)
                || (status.Equals("trialing", StringComparison.OrdinalIgnoreCase) && trialEndsAt.HasValue && trialEndsAt.Value.ToUniversalTime() > DateTime.UtcNow);
        }
    }

    const string inspectorSql = @"SELECT EXISTS(SELECT 1 FROM public.subscriptions WHERE inspector_id=@inspector_id AND (status='active' OR (status='trialing' AND trial_ends_at>NOW())))";
    await using var inspectorCmd = new NpgsqlCommand(inspectorSql, conn); inspectorCmd.Parameters.AddWithValue("inspector_id", inspectorId);
    return (bool)(await inspectorCmd.ExecuteScalarAsync() ?? false);
}

public static async Task<(bool Allowed, int Count)> RegisterOnlinePropertyAddressAsync(NpgsqlConnection conn, Guid jobId, Guid tenantId, string fingerprint, string address)
{
    await using var count = new NpgsqlCommand("SELECT COUNT(*) FROM public.online_property_addresses WHERE job_id=@job_id", conn); count.Parameters.AddWithValue("job_id", jobId);
    var used = Convert.ToInt32(await count.ExecuteScalarAsync());
    await using var exists = new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.online_property_addresses WHERE job_id=@job_id AND address_fingerprint=@fingerprint)", conn); exists.Parameters.AddWithValue("job_id", jobId); exists.Parameters.AddWithValue("fingerprint", fingerprint);
    if ((bool)(await exists.ExecuteScalarAsync() ?? false)) return (true, used);
    if (used >= 3) return (false, used);
    await using var insert = new NpgsqlCommand("INSERT INTO public.online_property_addresses(job_id,tenant_id,address_fingerprint,address_snapshot) VALUES(@job_id,@tenant_id,@fingerprint,@address) ON CONFLICT DO NOTHING", conn);
    insert.Parameters.AddWithValue("job_id", jobId); insert.Parameters.AddWithValue("tenant_id", tenantId == Guid.Empty ? DBNull.Value : tenantId); insert.Parameters.AddWithValue("fingerprint", fingerprint); insert.Parameters.AddWithValue("address", address ?? ""); await insert.ExecuteNonQueryAsync();
    return (true, used + 1);
}

public static async Task<bool> IsRegisteredOnlinePropertyAddressAsync(NpgsqlConnection conn, Guid jobId, string fingerprint)
{
    await using var cmd = new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.online_property_addresses WHERE job_id=@job_id AND address_fingerprint=@fingerprint)", conn);
    cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("fingerprint", fingerprint);
    return (bool)(await cmd.ExecuteScalarAsync() ?? false);
}

public static async Task<object?> LoadSuccessfulOnlinePropertyResultAsync(NpgsqlConnection conn, Guid jobId, string source, string fingerprint)
{
    if (source == "property-features")
    {
        await using var cmd = new NpgsqlCommand(@"SELECT property_features_json FROM public.jobs_staging
WHERE job_id=@job_id AND property_features_status='available' AND property_features_address_fingerprint=@fingerprint", conn);
        cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("fingerprint", fingerprint);
        var json = Convert.ToString(await cmd.ExecuteScalarAsync());
        if (string.IsNullOrWhiteSpace(json)) return null;
        var result = JsonSerializer.Deserialize<PropertyFeaturesResult>(json);
        if (result == null) return null;
        return new
        {
            result.Status, result.AddressFingerprint, result.RetrievedAt, result.Error, result.PropertyId, result.FormattedAddress,
            result.Latitude, result.Longitude, result.PropertyType, result.PropertySubType, result.Bedrooms, result.Bathrooms,
            result.CarSpaces, result.LandArea, result.FloorArea, result.YearBuilt, result.DecadeBuilt, result.RoofMaterial,
            result.WallMaterial, result.TotalFloors, result.LegalDescription, result.CouncilArea, result.Postcode,
            cacheStatus = "current_saved_data", message = "Current saved Property Features data."
        };
    }

    await using var branzCmd = new NpgsqlCommand(@"SELECT branz_wind_zone,branz_exposure_zone,branz_latitude,branz_longitude,branz_retrieved_at,branz_lookup_error
FROM public.jobs_staging WHERE job_id=@job_id AND branz_lookup_status='available' AND branz_address_fingerprint=@fingerprint", conn);
    branzCmd.Parameters.AddWithValue("job_id", jobId); branzCmd.Parameters.AddWithValue("fingerprint", fingerprint);
    await using var reader = await branzCmd.ExecuteReaderAsync(); if (!await reader.ReadAsync()) return null;
    return new
    {
        windZone = reader["branz_wind_zone"]?.ToString() ?? "", exposureZone = reader["branz_exposure_zone"]?.ToString() ?? "",
        status = "available", latitude = reader["branz_latitude"] == DBNull.Value ? null : (double?)Convert.ToDouble(reader["branz_latitude"]),
        longitude = reader["branz_longitude"] == DBNull.Value ? null : (double?)Convert.ToDouble(reader["branz_longitude"]),
        addressFingerprint = fingerprint, retrievedAt = reader["branz_retrieved_at"] == DBNull.Value ? null : reader["branz_retrieved_at"],
        error = reader["branz_lookup_error"]?.ToString() ?? "", cacheStatus = "current_saved_data", message = "Current saved BRANZ data."
    };
}

public static async Task<(bool Allowed, bool DailyLimitReached, int RetryAfterSeconds, int FailedAttemptsToday)> GetOnlinePropertyFailureRetryGateAsync(NpgsqlConnection conn, Guid jobId, string source, string fingerprint)
{
    const string sql = @"SELECT COUNT(*)::int AS failed_count, MAX(created_at) AS last_failed_at
FROM public.online_property_lookup_audit
WHERE job_id=@job_id AND source=@source AND address_fingerprint=@fingerprint
  AND outcome<>'available' AND reason<>'administrator_force' AND created_at >= date_trunc('day', NOW())";
    await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("source", source); cmd.Parameters.AddWithValue("fingerprint", fingerprint);
    await using var reader = await cmd.ExecuteReaderAsync(); await reader.ReadAsync();
    var count = reader["failed_count"] == DBNull.Value ? 0 : Convert.ToInt32(reader["failed_count"]);
    if (count >= 5) return (false, true, 0, count);
    if (reader["last_failed_at"] == DBNull.Value) return (true, false, 0, count);
    var last = new DateTimeOffset(Convert.ToDateTime(reader["last_failed_at"]));
    var remaining = 60 - (int)(DateTimeOffset.UtcNow - last).TotalSeconds;
    return remaining > 0 ? (false, false, remaining, count) : (true, false, 0, count);
}

public static async Task StorePropertyFeaturesResultAsync(NpgsqlConnection conn, Guid jobId, PropertyFeaturesResult result)
{
    const string sql = @"UPDATE public.jobs_staging SET property_features_json=CASE WHEN @status='available' THEN CAST(@json AS jsonb) ELSE property_features_json END,
property_features_status=@status,property_features_address_fingerprint=@fingerprint,property_features_retrieved_at=@retrieved,property_features_error=@error WHERE job_id=@job_id";
    await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("status", result.Status); cmd.Parameters.AddWithValue("json", JsonSerializer.Serialize(result)); cmd.Parameters.AddWithValue("fingerprint", result.AddressFingerprint); cmd.Parameters.AddWithValue("retrieved", result.RetrievedAt); cmd.Parameters.AddWithValue("error", result.Error); await cmd.ExecuteNonQueryAsync();
}

public static async Task StoreBranzResultAsync(NpgsqlConnection conn, Guid jobId, BranzLookupResult result)
{
    const string sql = @"UPDATE public.jobs_staging SET branz_wind_zone=CASE WHEN @status='available' THEN @wind ELSE branz_wind_zone END,
branz_exposure_zone=CASE WHEN @status='available' THEN @exposure ELSE branz_exposure_zone END,branz_lookup_status=@status,branz_latitude=@latitude,branz_longitude=@longitude,
branz_address_fingerprint=@fingerprint,branz_retrieved_at=@retrieved,branz_lookup_error=@error WHERE job_id=@job_id";
    await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("status", result.Status); cmd.Parameters.AddWithValue("wind", result.WindZone); cmd.Parameters.AddWithValue("exposure", result.ExposureZone); cmd.Parameters.AddWithValue("latitude", result.Latitude.HasValue ? result.Latitude.Value : DBNull.Value); cmd.Parameters.AddWithValue("longitude", result.Longitude.HasValue ? result.Longitude.Value : DBNull.Value); cmd.Parameters.AddWithValue("fingerprint", result.AddressFingerprint); cmd.Parameters.AddWithValue("retrieved", result.RetrievedAt); cmd.Parameters.AddWithValue("error", result.Error); await cmd.ExecuteNonQueryAsync();
}

public static async Task AuditOnlinePropertyLookupAsync(NpgsqlConnection conn, Guid jobId, Guid tenantId, string source, string fingerprint, string reason, string outcome, string error)
{
    await using var cmd = new NpgsqlCommand("INSERT INTO public.online_property_lookup_audit(job_id,tenant_id,source,address_fingerprint,reason,outcome,error) VALUES(@job_id,@tenant_id,@source,@fingerprint,@reason,@outcome,@error)", conn);
    cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("tenant_id", tenantId == Guid.Empty ? DBNull.Value : tenantId); cmd.Parameters.AddWithValue("source", source); cmd.Parameters.AddWithValue("fingerprint", fingerprint); cmd.Parameters.AddWithValue("reason", reason); cmd.Parameters.AddWithValue("outcome", outcome); cmd.Parameters.AddWithValue("error", error ?? ""); await cmd.ExecuteNonQueryAsync();
}
}

public record ScheduleServiceInput(
    string Label,
    string ServiceKey,
    string Slot);

public record ScheduleJobInput(
    Guid JobId,
    Guid TenantId,
    Guid InspectorId,
    string InspectorName,
    string JobName,
    string SiteAddress,
    DateTime? JobDate,
    int InspectionDurationMinutes,
    string PrimaryService,
    string Additional1,
    string Additional2,
    string PrimaryServiceKey,
    string Additional1ServiceKey,
    string Additional2ServiceKey,
    string BookingTemplateKey,
    bool BookingEmailRequired,
    bool BookingEmailSent,
    bool TermsRequired,
    bool TermsSent,
    bool TermsRetryRequested,
    bool TermsSigned,
    string SignNowDocumentId,
    bool InvoiceRequired,
    bool CalendarRequired,
    bool CalendarCreated,
    string Notes,
    string Directions,
    string Instructions,
    string AgeOfBuilding,
    string Stories,
    string Bedrooms,
    string Bathrooms,
    string Monolithic,
    string FoundationSpace,
    string AccessBy,
    string ClientName,
    string ClientFirstName,
    string ClientLastName,
    string ClientDisplayName,
    string ClientSalutation,
    string ClientRoleLabel,
    string ClientEmail,
    string ClientPhone,
    string AgentName,
    string AgentFirstName,
    string AgentLastName,
    string AgentDisplayName,
    string AgentSalutation,
    string AgentRoleLabel,
    string AgentEmail,
    string AgentPhone,
    string Timezone,
    string CompanyName,
    string EmailFromName,
    string EmailFromAddress,
    string Phone,
    string EmailSenderMode);

public class JobUploadRequest
{
    public string SourceSystem { get; set; } = "";
    public string TenantId { get; set; } = "";
    public JobSection Job { get; set; } = new JobSection();
    public ServicesSection Services { get; set; } = new ServicesSection();
    public JobDetailsSection JobDetails { get; set; } = new JobDetailsSection();
    public List<InvoiceLineSection> InvoiceLines { get; set; } = new();
    public ContactFlat Contact1 { get; set; } = new ContactFlat();
    public ContactFlat Contact2 { get; set; } = new ContactFlat();
    public MetaSection Meta { get; set; } = new MetaSection();
}

public sealed class AuthenticatedAutomationIdentityException : Exception
{
    public AuthenticatedAutomationIdentityException(string message) : base(message) { }
}

public sealed class BasicAutomationSettingRequest
{
    public Guid TenantId { get; set; }
    public string EventKey { get; set; } = "";
    public string RecipientKey { get; set; } = "";
    public bool Enabled { get; set; }
    public int ExpectedVersion { get; set; }
    public string IdempotencyKey { get; set; } = "";
    public bool Confirmed { get; set; }
}

public sealed class ClientEngagementSettingsRequest
{
    public Guid TenantId { get; set; }
    public bool OpenTrackingEnabled { get; set; }
    public bool ClientPageEnabled { get; set; }
    public int ExpectedVersion { get; set; }
    public string IdempotencyKey { get; set; } = "";
    public bool Confirmed { get; set; }
}

public sealed class RevokeClientPageRequest
{
    public Guid TenantId { get; set; }
    public string Reason { get; set; } = "";
    public bool Confirmed { get; set; }
}

public sealed class PrepareClientEmailRequest
{
    public Guid TenantId { get; set; }
    public string RecipientKey { get; set; } = "contact_1";
    public string EventKey { get; set; } = "scheduling";
    public string ServiceTypeKey { get; set; } = "";
    public string ActionKey { get; set; } = "";
    public string ConnectorVersion { get; set; } = "";
    public bool IsTest { get; set; }
    public bool IsPreview { get; set; }
    public bool ControlledClientPageTest { get; set; }
    public string DeliveryAddressOverride { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
    public bool Confirmed { get; set; }
}

public sealed class ClientDeliveryRequest
{
    public Guid TenantId { get; set; }
    public bool Accepted { get; set; }
    public string Provider { get; set; } = "";
    public string ConnectorVersion { get; set; } = "";
    public string Error { get; set; } = "";
}

public class BasicAutomationTemplateRequest
{
    public Guid TenantId { get; set; }
    public string Subject { get; set; } = "";
    public string HtmlBody { get; set; } = "";
    public int ExpectedVersion { get; set; }
    public string IdempotencyKey { get; set; } = "";
    public string RequestId { get; set; } = "";
    public bool Confirmed { get; set; }
}

public sealed class BasicAutomationRenderRequest : BasicAutomationTemplateRequest
{
    public string EventKey { get; set; } = "";
    public string RecipientKey { get; set; } = "";
}

public sealed class BasicProductionArmRequest
{
    public Guid TenantId { get; set; }
    public bool Armed { get; set; }
    public bool DisposableConfirmed { get; set; }
    public bool Confirmed { get; set; }
    public int ExpectedVersion { get; set; }
}

public class BasicProductionCommandRequest
{
    public Guid TenantId { get; set; }
    public bool Confirmed { get; set; }
}

public sealed class BasicProductionCompleteRequest : BasicProductionCommandRequest
{
    public string Outcome { get; set; } = "";
    public string ProviderMessageId { get; set; } = "";
    public string Error { get; set; } = "";
}

public sealed class BasicTestJobSelectionRequest
{
    public Guid TenantId { get; set; }
    public bool Enabled { get; set; }
    public bool DisposableConfirmed { get; set; }
    public bool Confirmed { get; set; }
    public int ExpectedVersion { get; set; }
}

public sealed class BasicTestPrepareRequest
{
    public Guid TenantId { get; set; }
    public string RevisionKey { get; set; } = "";
    public string RecipientKey { get; set; } = "";
    public bool Confirmed { get; set; }
}

public class BasicTestApproveRequest
{
    public Guid TenantId { get; set; }
    public bool Confirmed { get; set; }
}

public sealed class BasicTestCompleteRequest : BasicTestApproveRequest
{
    public string TestRecipientEmail { get; set; } = "";
    public bool Succeeded { get; set; }
    public string? ProviderMessageId { get; set; }
    public string? Error { get; set; }
}

public sealed class BasicAutomationCompleteRequest
{
    public Guid TenantId { get; set; }
    public string RevisionKey { get; set; } = "";
    public string EventKey { get; set; } = "";
    public string RecipientKey { get; set; } = "";
    public bool Success { get; set; }
    public string Error { get; set; } = "";
}

public sealed record RenderedBasicEmail(string ToEmail,string Subject,string HtmlBody,string RecipientLabel);

public class JobSection
{
    public string JobId { get; set; } = "";
    public string InspectorId { get; set; } = "";
    public string InspectorName { get; set; } = "";
    public string InspectorEmail { get; set; } = "";
    public string InspectorPhone { get; set; } = "";
    public string JobName { get; set; } = "";
    public string SiteAddress { get; set; } = "";
    public string AgeOfBuilding { get; set; } = "";
    [JsonPropertyName("age_of_building")]
    public string AgeOfBuildingSnake { get; set; } = "";
    public string JobDate { get; set; } = "";
    public int InspectionDurationMinutes { get; set; } = 0;
    public string SourceUpdatedAtUtc { get; set; } = "";
    public string DateAddedUtc { get; set; } = "";
    public string Status { get; set; } = "";
    public string ZapProcessed { get; set; } = "";
    public string ReportSent { get; set; } = "";
    public string InvoiceTotal { get; set; } = "";
    public string Notes { get; set; } = "";
    public string Directions { get; set; } = "";
    public string Instructions { get; set; } = "";
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }

    public string GetAgeOfBuilding()
    {
        if (!string.IsNullOrWhiteSpace(AgeOfBuilding))
            return AgeOfBuilding;

        if (!string.IsNullOrWhiteSpace(AgeOfBuildingSnake))
            return AgeOfBuildingSnake;

        if (ExtraFields == null)
            return "";

        foreach (var key in new[] { "Age of Building", "age of building", "age-of-building", "building_age", "BuildingAge" })
        {
            if (ExtraFields.TryGetValue(key, out var value) && value.ValueKind == JsonValueKind.String)
                return value.GetString() ?? "";
        }

        return "";
    }
}

public class ServicesSection
{
    public string Primary { get; set; } = "";
    public string Additional1 { get; set; } = "";
    public string Additional2 { get; set; } = "";
    public string PrimaryServiceKey { get; set; } = "";
    public string Additional1ServiceKey { get; set; } = "";
    public string Additional2ServiceKey { get; set; } = "";
    public string BookingTemplateKey { get; set; } = "";
    public bool? BookingEmailRequired { get; set; }
    public bool? TermsRequired { get; set; }
    public bool? InvoiceRequired { get; set; }
    public bool? CalendarRequired { get; set; }
    public bool? ReportRequired { get; set; }
}

public class JobDetailsSection
{
    public string AgeOfBuilding { get; set; } = "";
    public string BuildingType { get; set; } = "";
    public string Stories { get; set; } = "";
    public string Bedrooms { get; set; } = "";
    public string Bathrooms { get; set; } = "";
    public string Monolithic { get; set; } = "";
    public string Outbuilding { get; set; } = "";
    public string Occupied { get; set; } = "";
    public string AttachedFlat { get; set; } = "";
    public string TravelFee { get; set; } = "";
    public string HhsBedrooms { get; set; } = "";
    public string MethSamples { get; set; } = "";
    public string HhsReinspect { get; set; } = "";
    public string CouncilFiles { get; set; } = "";
    public string FoundationSpace { get; set; } = "";
    public string Weathertightness { get; set; } = "";
    public string HhsReinspectDate { get; set; } = "";
    public string AccessBy { get; set; } = "";
    public string HhsCompliance { get; set; } = "";
}

public class ContactFlat
{
    public string ContactId { get; set; } = "";
    public int ContactIndex { get; set; } = -1;
    public string RoleLabel { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string Salutation { get; set; } = "";
    public string FirstName { get; set; } = "";
    public string LastName { get; set; } = "";
    public string Email { get; set; } = "";
    public string Cellular { get; set; } = "";
}

public class InvoiceLineSection
{
    public int LineIndex { get; set; }
    public string Description { get; set; } = "";
    public decimal Quantity { get; set; } = 1m;
    public decimal UnitPrice { get; set; }
}

public class MetaSection
{
    public string ExtractedAtUtc { get; set; } = "";
    public string ConnectorVersion { get; set; } = "";
    public string SourceInstance { get; set; } = "";
}

public class AutomationRuleSaveRequest
{
    public Guid RuleId { get; set; }
    public Guid TenantId { get; set; }
    public string Name { get; set; } = "";
    public string EventKey { get; set; } = "";
    public bool Enabled { get; set; }
    public List<AutomationCondition> Conditions { get; set; } = new();
    public List<AutomationActionDefinition> Actions { get; set; } = new();
}

public class AutomationCondition
{
    public string FieldKey { get; set; } = "";
    public string Operator { get; set; } = "";
    public string Value { get; set; } = "";
}

public class AutomationActionDefinition
{
    public string ActionKey { get; set; } = "";
    public string Timing { get; set; } = "immediate";
    public Dictionary<string, string> Settings { get; set; } = new();
}

public class AutomationRulePreviewRequest
{
    public AutomationRuleSaveRequest Rule { get; set; } = new();
    public Dictionary<string, string> Fields { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public class AutomationEventRequest
{
    public Guid TenantId { get; set; }
    public Guid? JobId { get; set; }
    public string EventKey { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
    public Dictionary<string, string> Fields { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public record AutomationConditionEvaluation(string FieldKey, string Operator, string Expected, string Actual, bool Matched);
public record AutomationRuleMatch(Guid RuleId, string RuleName, List<AutomationConditionEvaluation> Conditions, List<AutomationActionDefinition> Actions);
public record SignNowWebhookRegistrationResult(bool Success, string SubscriptionId, string Error);
