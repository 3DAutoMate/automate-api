using System.Text.Json;
using System.Text.Json.Serialization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);
builder.Configuration.AddEnvironmentVariables();

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
ADD COLUMN IF NOT EXISTS terms_required boolean NOT NULL DEFAULT true;

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
ADD COLUMN IF NOT EXISTS hhs_reinspect_date text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS access_by text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_compliance text NULL;

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
    access_token_encrypted,
    refresh_token_encrypted,
    expires_at,
    external_account_email,
    status
FROM public.inspector_integrations
WHERE inspector_id = @inspector_id
  AND provider = 'microsoft'
LIMIT 1;";

        string? accessToken = null;
        string? refreshToken = null;
        DateTime? expiresAt = null;
        string? externalAccountEmail = null;
        string? status = null;

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
                    contentType = "Text",
                    content = string.IsNullOrWhiteSpace(request.Body)
                        ? "This is a test email from 3D AutoMate."
                        : request.Body
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
                hhs_reinspect_date = reader["hhs_reinspect_date"]?.ToString(),
                access_by = reader["access_by"]?.ToString(),
                hhs_compliance = reader["hhs_compliance"]?.ToString(),
                outbuilding_scope_label = ToScopeLabel(reader["outbuilding"]?.ToString()),
                attached_flat_scope_label = ToScopeLabel(reader["attached_flat"]?.ToString()),
                council_file_review_scope_label = ToScopeLabel(reader["council_files"]?.ToString()),
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
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
SELECT
    j.job_id,
    j.job_name,
    j.site_address,
    j.job_date,
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
    j.workflow_updated_at,
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
ORDER BY COALESCE(j.workflow_updated_at, j.updated_at, j.created_at) DESC
LIMIT 100;";

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
                workflow_updated_at = reader["workflow_updated_at"]?.ToString(),
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
app.MapGet("/workflow-actions/pending", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);

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
ORDER BY a.updated_at ASC
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
                hhs_reinspect_date = reader["hhs_reinspect_date"]?.ToString(),
                access_by = reader["access_by"]?.ToString(),
                hhs_compliance = reader["hhs_compliance"]?.ToString(),
                outbuilding_scope_label = ToScopeLabel(reader["outbuilding"]?.ToString()),
                attached_flat_scope_label = ToScopeLabel(reader["attached_flat"]?.ToString()),
                council_file_review_scope_label = ToScopeLabel(reader["council_files"]?.ToString()),
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
    terms_required boolean NOT NULL DEFAULT true,
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
    hhs_reinspect_date text,
    access_by text,
    hhs_compliance text,
    contact1_salutation text,
    contact1_first_name text,
    contact1_last_name text,
    contact1_email text,
    contact1_cellular text,
    contact2_salutation text,
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

        await EnsureJobPaymentColumnsAsync(conn);

        const string upsertSql = @"
INSERT INTO public.jobs_staging
(
    tenant_id,
    job_id,
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
    hhs_reinspect_date,
    access_by,
    hhs_compliance,
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
    raw_payload_json,
    updated_at
)
VALUES
(
    @tenant_id,
    @job_id,
    @inspector_id,
    @inspector_name,
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
    @hhs_reinspect_date,
    @access_by,
    @hhs_compliance,
    @contact1_salutation,
    @contact1_first_name,
    @contact1_last_name,
    @contact1_email,
    @contact1_cellular,
    @contact2_salutation,
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
    hhs_reinspect_date           = EXCLUDED.hhs_reinspect_date,
    access_by                    = EXCLUDED.access_by,
    hhs_compliance               = EXCLUDED.hhs_compliance,
    contact1_salutation          = EXCLUDED.contact1_salutation,
    contact1_first_name          = EXCLUDED.contact1_first_name,
    contact1_last_name           = EXCLUDED.contact1_last_name,
    contact1_email               = EXCLUDED.contact1_email,
    contact1_cellular            = EXCLUDED.contact1_cellular,
    contact2_salutation          = EXCLUDED.contact2_salutation,
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
            cmd.Parameters.AddWithValue("terms_required", payload.Services?.TermsRequired ?? true);
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
            cmd.Parameters.AddWithValue("hhs_reinspect_date", payload.JobDetails?.HhsReinspectDate ?? "");
            cmd.Parameters.AddWithValue("access_by", payload.JobDetails?.AccessBy ?? "");
            cmd.Parameters.AddWithValue("hhs_compliance", payload.JobDetails?.HhsCompliance ?? "");
            cmd.Parameters.AddWithValue("contact1_salutation", payload.Contact1?.Salutation ?? "");
            cmd.Parameters.AddWithValue("contact1_first_name", payload.Contact1?.FirstName ?? "");
            cmd.Parameters.AddWithValue("contact1_last_name", payload.Contact1?.LastName ?? "");
            cmd.Parameters.AddWithValue("contact1_email", payload.Contact1?.Email ?? "");
            cmd.Parameters.AddWithValue("contact1_cellular", payload.Contact1?.Cellular ?? "");
            cmd.Parameters.AddWithValue("contact2_salutation", payload.Contact2?.Salutation ?? "");
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

        await RefreshBookingWorkflowActionsAsync(conn, payload, jobId, tenantId, inspectorId);

        return Results.Ok(new
        {
            success = true,
            message = "Job staged successfully",
            jobId = payload.Job.JobId,
            tenantId = payload.TenantId,
            inspectorId = payload.Job.InspectorId
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

app.Run();

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
    sent_at timestamptz NULL,
    last_attempt_at timestamptz NULL,
    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, action_key)
);

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
    var actions = new List<WorkflowActionSeed>();

    AddBookingWorkflowAction(actions, jobId, tenantId, inspectorId, "primary", payload.Services?.Primary, NormalizeServiceKey(payload.Services?.PrimaryServiceKey, payload.Services?.Primary), payload.Services?.BookingEmailRequired ?? true);
    AddBookingWorkflowAction(actions, jobId, tenantId, inspectorId, "additional1", payload.Services?.Additional1, NormalizeServiceKey(payload.Services?.Additional1ServiceKey, payload.Services?.Additional1), payload.Services?.BookingEmailRequired ?? true);
    AddBookingWorkflowAction(actions, jobId, tenantId, inspectorId, "additional2", payload.Services?.Additional2, NormalizeServiceKey(payload.Services?.Additional2ServiceKey, payload.Services?.Additional2), payload.Services?.BookingEmailRequired ?? true);

    return actions
        .GroupBy(action => action.ActionKey, StringComparer.OrdinalIgnoreCase)
        .Select(group => group.First())
        .ToList();
}

static void AddBookingWorkflowAction(List<WorkflowActionSeed> actions, Guid jobId, Guid tenantId, Guid inspectorId, string serviceSlot, string? serviceLabel, string serviceKey, bool bookingEmailRequired)
{
    if (!bookingEmailRequired || string.IsNullOrWhiteSpace(serviceLabel))
        return;

    if (string.IsNullOrWhiteSpace(serviceKey) || IsModifierServiceKey(serviceKey))
        return;

    var actionKey = BuildBookingActionKey(serviceKey, serviceLabel);

    if (string.IsNullOrWhiteSpace(actionKey))
        return;

    actions.Add(new WorkflowActionSeed(
        jobId,
        tenantId,
        inspectorId,
        actionKey,
        "booking_email",
        serviceKey,
        serviceLabel.Trim(),
        serviceSlot));
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

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'inspectors_tenant_id_unique'
    ) THEN
        ALTER TABLE public.inspectors
        ADD CONSTRAINT inspectors_tenant_id_unique UNIQUE (tenant_id);
    END IF;
END $$;
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
    terms_required boolean NOT NULL DEFAULT true,
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
ADD COLUMN IF NOT EXISTS terms_required boolean NOT NULL DEFAULT true;

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
    cmd.Parameters.AddWithValue("terms_required", item.TermsRequired);
    cmd.Parameters.AddWithValue("invoice_required", item.InvoiceRequired);
    cmd.Parameters.AddWithValue("calendar_required", item.CalendarRequired);
    cmd.Parameters.AddWithValue("report_required", item.ReportRequired);
    cmd.Parameters.AddWithValue("raw_payload_json", JsonSerializer.Serialize(item));
    await cmd.ExecuteNonQueryAsync();
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
    .Where(k => !string.IsNullOrWhiteSpace(k) && k != "other")
    .Distinct()
    .ToList();

    return keys.Count == 0 ? "general_booking" : string.Join("_", keys);
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

    var trimmed = value.Trim();
    var normalized = trimmed.ToLowerInvariant();

    if (normalized == "yes" || normalized == "true" || normalized == "included" || normalized == "include")
        return "Included";

    if (normalized == "no" || normalized == "false" || normalized == "excluded" || normalized == "exclude")
        return "Excluded";

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
ADD COLUMN IF NOT EXISTS terms_required boolean NOT NULL DEFAULT true;

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
ADD COLUMN IF NOT EXISTS hhs_reinspect_date text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS access_by text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS hhs_compliance text NULL;
";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}

public class BookingEmailFailureRequest
{
    public string? ErrorMessage { get; set; }
}

public record WorkflowActionFailureRequest(string ErrorMessage);
public record TermsFailureRequest(string ErrorMessage);
public record InvoiceFailureRequest(string ErrorMessage);
public record CalendarFailureRequest(string ErrorMessage);
public record ReportFailureRequest(string ErrorMessage);
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
    public bool TermsRequired { get; set; } = true;
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

public class JobUploadRequest
{
    public string SourceSystem { get; set; } = "";
    public string TenantId { get; set; } = "";
    public JobSection Job { get; set; } = new JobSection();
    public ServicesSection Services { get; set; } = new ServicesSection();
    public JobDetailsSection JobDetails { get; set; } = new JobDetailsSection();
    public ContactFlat Contact1 { get; set; } = new ContactFlat();
    public ContactFlat Contact2 { get; set; } = new ContactFlat();
    public MetaSection Meta { get; set; } = new MetaSection();
}

public class JobSection
{
    public string JobId { get; set; } = "";
    public string InspectorId { get; set; } = "";
    public string InspectorName { get; set; } = "";
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
    public string HhsReinspectDate { get; set; } = "";
    public string AccessBy { get; set; } = "";
    public string HhsCompliance { get; set; } = "";
}

public class ContactFlat
{
    public string ContactId { get; set; } = "";
    public string Salutation { get; set; } = "";
    public string FirstName { get; set; } = "";
    public string LastName { get; set; } = "";
    public string Email { get; set; } = "";
    public string Cellular { get; set; } = "";
}

public class MetaSection
{
    public string ExtractedAtUtc { get; set; } = "";
    public string ConnectorVersion { get; set; } = "";
    public string SourceInstance { get; set; } = "";
}
