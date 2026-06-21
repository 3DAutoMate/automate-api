using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Text;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Npgsql;

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

    var scopes = "https://www.googleapis.com/auth/calendar.events https://www.googleapis.com/auth/userinfo.email";

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
app.MapPost("/integrations/signnow/jobs/{jobId}/send-terms", async (Guid jobId) =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
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
        var documentId = FindJsonStringRecursive(root, "document_id", "documentId", "document_unique_id", "id");
        var eventName = FindJsonStringRecursive(root, "event", "event_type", "type", "status");
        var jobIdText = FindSignNowJobId(root);
        Guid.TryParse(jobIdText, out var parsedJobId);

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await EnsureSignNowJobColumnsAsync(conn);

        if (parsedJobId == Guid.Empty && !string.IsNullOrWhiteSpace(documentId))
            parsedJobId = await FindJobIdBySignNowDocumentAsync(conn, documentId);

        if (parsedJobId == Guid.Empty)
        {
            return Results.Ok(new
            {
                success = true,
                matched = false,
                message = "SignNow webhook received but no matching job was found."
            });
        }

        var signed = LooksLikeSignNowCompleted(eventName) || LooksLikeSignNowCompleted(FindJsonStringRecursive(root, "status"));
        await StoreSignNowStatusAsync(
            conn,
            parsedJobId,
            documentId,
            null,
            null,
            eventName,
            null,
            signed,
            signed ? DateTime.UtcNow : null);

        return Results.Ok(new
        {
            success = true,
            matched = true,
            jobId = parsedJobId,
            documentId,
            status = eventName,
            signed
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
    placeholders = GetEmailTemplatePlaceholders(),
    categories = GetEmailTemplatePlaceholderCategories()
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
        await EnsureJobPaymentColumnsAsync(conn);
        await EnsureSignNowJobColumnsAsync(conn);
        await EnsureWorkflowActionsTableAsync(conn);

        const string sql = @"
SELECT
    j.job_id,
    j.job_name,
    j.site_address,
    j.job_date,
    j.date_added,
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
    j.contact1_email,
    j.weathertightness,
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
        await EnsureSignNowJobColumnsAsync(conn);
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
    weathertightness,
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
    @weathertightness,
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
    weathertightness             = EXCLUDED.weathertightness,
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
        await RefreshJobInvoiceLinesAsync(conn, payload, jobId);

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
ADD COLUMN IF NOT EXISTS signnow_signing_link text NULL;";

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
    j.contact1_first_name,
    j.contact1_last_name,
    j.contact1_email,
    j.contact2_first_name,
    j.contact2_last_name,
    j.contact2_email,
    COALESCE(i.timezone, 'Pacific/Auckland') AS timezone,
    i.company_name,
    i.email_from_name,
    i.email_from_address,
    i.phone
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
        (Guid)reader["job_id"],
        reader["tenant_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["tenant_id"],
        (Guid)reader["inspector_id"],
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
        BuildPersonName(reader["contact1_first_name"]?.ToString(), reader["contact1_last_name"]?.ToString()),
        reader["contact1_email"]?.ToString() ?? "",
        BuildPersonName(reader["contact2_first_name"]?.ToString(), reader["contact2_last_name"]?.ToString()),
        reader["contact2_email"]?.ToString() ?? "",
        reader["timezone"]?.ToString() ?? "Pacific/Auckland",
        reader["company_name"]?.ToString() ?? "",
        reader["email_from_name"]?.ToString() ?? "",
        reader["email_from_address"]?.ToString() ?? "",
        reader["phone"]?.ToString() ?? "");
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

static string BuildScheduleBookingEmailHtml(ScheduleJobInput job, ScheduleServiceInput service)
{
    var company = string.IsNullOrWhiteSpace(job.CompanyName) ? "3D AutoMate" : job.CompanyName.Trim();
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
        await MarkTermsFailedAsync(conn, job.JobId, copyJson);
        return ScheduleActionResult.Failed("terms", "SignNow template copy failed: " + copyJson);
    }

    var copyDoc = JsonDocument.Parse(copyJson).RootElement;
    var documentId = FirstNonEmptyJsonString(copyDoc, "id", "document_id", "unique_id");
    if (string.IsNullOrWhiteSpace(documentId))
    {
        await MarkTermsFailedAsync(conn, job.JobId, "SignNow did not return a document ID after copying the template.");
        return ScheduleActionResult.Failed("terms", "SignNow did not return a document ID after copying the template.");
    }

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

    return ScheduleActionResult.Ok("terms", "SignNow terms sent to client.", new
    {
        documentId,
        inviteId,
        templateKey,
        templateId = mapping.TemplateId,
        templateName = mapping.TemplateName,
        sentTo = job.ClientEmail
    });
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
            await MarkCalendarCreatedAsync(conn, job.JobId);
            return ScheduleActionResult.Skip("calendar", "Google Calendar event already exists.", new
            {
                eventId = GetJsonString(items[0], "id"),
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

    var eventPayload = new
    {
        summary,
        location = job.SiteAddress,
        description =
            "Created by 3D AutoMate." +
            (string.IsNullOrWhiteSpace(job.ClientName) ? "" : "\nClient: " + job.ClientName) +
            (string.IsNullOrWhiteSpace(job.ClientEmail) ? "" : "\nClient email: " + job.ClientEmail),
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
    string lastEndpoint = endpoints[0].Url;
    int lastStatusCode = 0;

    foreach (var endpoint in endpoints)
    {
        lastEndpoint = endpoint.Url;
        var lastResponse = await httpClient.GetAsync(endpoint.Url);
        lastJson = await lastResponse.Content.ReadAsStringAsync();
        lastStatusCode = (int)lastResponse.StatusCode;

        if (lastResponse.IsSuccessStatusCode)
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
                count = endpointTemplates.Count
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
                response = TruncateForDiagnostics(lastJson, 1000)
            });
        }
    }

    var preferredTemplates = templates.Any(template => string.Equals(template.SourceType, "user_documentsv2_template", StringComparison.OrdinalIgnoreCase))
        ? templates.Where(template => string.Equals(template.SourceType, "user_documentsv2_template", StringComparison.OrdinalIgnoreCase))
        : templates;

    return new SignNowTemplateLookupResult(
        GroupSignNowTemplatesByName(preferredTemplates),
        diagnostics.ToArray(),
        successfulEndpointCount,
        lastEndpoint,
        lastStatusCode,
        TruncateForDiagnostics(lastJson, 2000));
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

static string FindSignNowJobId(JsonElement root)
{
    var direct = FindJsonStringRecursive(root, "JobID", "job_id", "jobId");
    if (!string.IsNullOrWhiteSpace(direct))
        return direct;

    if (root.ValueKind == JsonValueKind.Object)
    {
        foreach (var property in root.EnumerateObject())
        {
            if (string.Equals(property.Name, "field_name", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(property.Value.ToString(), "JobID", StringComparison.OrdinalIgnoreCase))
            {
                return FirstNonEmptyJsonString(root, "value", "text", "prefilled_text");
            }
        }
    }

    return "";
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

static object BuildAddOnPlaceholder(EmailTemplateServiceType serviceType)
{
    var key = BuildAddOnPlaceholderKey(serviceType.Key);
    return new { key, token = "{{" + key + "}}", label = "Has " + serviceType.Label };
}

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
        .ToArray();
}

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
      display: grid;
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
    <h1>AutoMate Email Templates</h1>
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
        <button id="loadBtn">Load Saved</button>
        <button id="saveBtn" class="primary">Save Template</button>
        <button id="previewBtn">Preview With Job</button>
        <button id="jobInspectorBtn">Use Job Inspector</button>
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
        <button id="sendBtn" class="primary">Send</button>
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
    const state = { lastTarget: null, placeholders: [], editorReady: false };
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
      $("sendBtn").disabled = !hasJob;
      $("jobInspectorBtn").disabled = !hasJob;
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
      const inspectorId = $("inspectorId").value.trim();
      const type = "booking-email";
      const serviceTypeKey = $("serviceType").value || "general_booking";
      if (!inspectorId) throw new Error("Inspector ID is required.");
      return `/inspectors/${encodeURIComponent(inspectorId)}/email-templates/${encodeURIComponent(type)}?serviceTypeKey=${encodeURIComponent(serviceTypeKey)}`;
    }

    function bodyPayload() {
      return {
        emailType: "transactional",
        serviceTypeKey: $("serviceType").value || "general_booking",
        name: $("templateName").value.trim(),
        subject: $("subject").value,
        htmlBody: getEditorHtml(),
        isActive: $("active").value === "true"
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
        headers: { "Content-Type": "application/json" },
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
      const t = data.template;
      const serviceTypeKey = t.serviceTypeKey || t.service_type_key;
      if (serviceTypeKey) {
        ensureServiceTypeOption(serviceTypeKey);
        $("serviceType").value = serviceTypeKey;
      }
      $("templateName").value = t.name || "";
      $("subject").value = t.subject || "";
      setEditorHtml(t.htmlBody || t.html_body || "");
      $("active").value = String(t.isActive ?? t.is_active ?? true);
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

    async function previewTemplate() {
      const jobId = $("jobId").value.trim();
      if (!jobId) throw new Error("No test job is loaded yet. Refresh the local database connection or paste a Job ID.");
      await resolveInspectorFromJob(false);
      setStatus("Rendering preview...");
      const data = await api(`/jobs/${encodeURIComponent(jobId)}/email-templates/booking-email/preview`, {
        method: "POST",
        body: JSON.stringify({
          emailType: "transactional",
          serviceTypeKey: $("serviceType").value || null,
          subject: $("subject").value,
          htmlBody: getEditorHtml()
        })
      });
      $("previewSubject").textContent = data.subject || "";
      $("preview").srcdoc = data.htmlBody || "";
      setStatus("Preview rendered.", "good");
    }

    async function sendTemplate() {
      const jobId = $("jobId").value.trim();
      if (!jobId) throw new Error("No test job is loaded yet. Refresh the local database connection or paste a Job ID.");
      await resolveInspectorFromJob(false);
      setStatus("Sending...");
      const data = await api(`/jobs/${encodeURIComponent(jobId)}/email-templates/booking-email/send`, {
        method: "POST",
        body: JSON.stringify({
          toEmail: $("toEmail").value.trim() || null,
          serviceTypeKey: $("serviceType").value || null,
          markWorkflowComplete: true
        })
      });
      setStatus(data.message || "Sent.", "good");
    }

    async function sendTestEmail() {
      const toEmail = $("toEmail").value.trim();
      if (!toEmail) throw new Error("Enter your email address in Send To Override first.");

      setStatus("Sending test email...");
      const isLocal = location.hostname === "127.0.0.1" || location.hostname === "localhost";
      const sendUrl = isLocal
        ? "https://automate-api-production.up.railway.app/integrations/microsoft/send-test-email"
        : "/integrations/microsoft/send-test-email";
      const data = await api(sendUrl, {
        method: "POST",
        body: JSON.stringify({
          inspectorId: getInspectorIdForTest(),
          toEmail,
          subject: "[TEST] " + ($("subject").value || "Booking email"),
          body: renderDraftForTest(getEditorHtml())
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
    $("jobId").addEventListener("input", updateJobActionState);
    updateJobActionState();

    for (const btn of document.querySelectorAll(".editor-tools button")) {
      btn.addEventListener("click", () => runEditorCommand(btn.dataset.command));
    }

    $("loadBtn").addEventListener("click", () => loadTemplate().catch(err => setStatus(friendlyError(err.message), "bad")));
    $("saveBtn").addEventListener("click", () => saveTemplate().catch(err => setStatus(friendlyError(err.message), "bad")));
    $("previewBtn").addEventListener("click", () => previewTemplate().catch(err => setStatus(friendlyError(err.message), "bad")));
    $("sendTestBtn").addEventListener("click", () => sendTestEmail().catch(err => setStatus(friendlyError(err.message), "bad")));
    $("sendBtn").addEventListener("click", () => sendTemplate().catch(err => setStatus(friendlyError(err.message), "bad")));
    $("jobInspectorBtn").addEventListener("click", () => resolveInspectorFromJob(true).catch(err => setStatus(friendlyError(err.message), "bad")));

    Promise.all([loadServiceTypes(), loadPlaceholders()])
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

public class BookingEmailFailureRequest
{
    public string? ErrorMessage { get; set; }
}

public record WorkflowActionFailureRequest(string ErrorMessage);
public record TermsFailureRequest(string ErrorMessage);
public record InvoiceFailureRequest(string ErrorMessage);
public record CalendarFailureRequest(string ErrorMessage);
public record ReportFailureRequest(string ErrorMessage);
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
    string ClientName,
    string ClientEmail,
    string AgentName,
    string AgentEmail,
    string Timezone,
    string CompanyName,
    string EmailFromName,
    string EmailFromAddress,
    string Phone);

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
    public string Weathertightness { get; set; } = "";
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

