using System.Text.Json;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);

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

        const string sql = @"
SELECT
    j.job_id,
    j.tenant_id,
    j.inspector_id,
    j.inspector_name,
    j.source_system,
    j.job_name,
    j.site_address,
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
    j.calendar_created,
    j.calendar_created_at,
    j.calendar_retry_requested,
    j.calendar_retry_requested_at,
    j.calendar_last_attempt_at,
    j.calendar_last_error,
    j.primary_service,
    j.additional1,
    j.additional2,
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
    (j.booking_email_sent = false OR j.booking_email_retry_requested = true)
    OR (j.terms_sent = false OR j.terms_retry_requested = true)
    OR (j.invoice_sent = false OR j.invoice_retry_requested = true)
    OR (j.calendar_created = false OR j.calendar_retry_requested = true)
    OR (j.report_workflow_sent = false OR j.report_retry_requested = true)
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
// GET LATEST JOBS
// =============================
app.MapGet("/jobs/latest", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
SELECT
    job_id,
    tenant_id,
    inspector_id,
    inspector_name,
    source_system,
    job_name,
    site_address,
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
    workflow_updated_at timestamptz NOT NULL DEFAULT NOW(),
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW()
);";

        await using (var createCmd = new NpgsqlCommand(createTableSql, conn))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

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
    job_date,
    inspection_duration_minutes,
    source_updated_at,
    date_added,
    status,
    zap_processed,
    report_sent,
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
    @job_date,
    @inspection_duration_minutes,
    @source_updated_at,
    @date_added,
    @status,
    @zap_processed,
    @report_sent,
    @primary_service,
    @additional1,
    @additional2,
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
    job_date                     = EXCLUDED.job_date,
    inspection_duration_minutes  = EXCLUDED.inspection_duration_minutes,
    source_updated_at            = EXCLUDED.source_updated_at,
    date_added                   = EXCLUDED.date_added,
    status                       = EXCLUDED.status,
    zap_processed                = EXCLUDED.zap_processed,
    report_sent                  = EXCLUDED.report_sent,
    primary_service              = EXCLUDED.primary_service,
    additional1                  = EXCLUDED.additional1,
    additional2                  = EXCLUDED.additional2,
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

            var jobDate = ParseNullableDateTime(payload.Job.JobDate);
            var sourceUpdatedAt = ParseNullableDateTime(payload.Job.SourceUpdatedAtUtc);
            var dateAdded = ParseNullableDateTime(payload.Job.DateAddedUtc);

            cmd.Parameters.AddWithValue("job_date", jobDate.HasValue ? jobDate.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("inspection_duration_minutes", payload.Job.InspectionDurationMinutes);
            cmd.Parameters.AddWithValue("source_updated_at", sourceUpdatedAt.HasValue ? sourceUpdatedAt.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("date_added", dateAdded.HasValue ? dateAdded.Value : (object)DBNull.Value);

            cmd.Parameters.AddWithValue("status", payload.Job.Status ?? "");
            cmd.Parameters.AddWithValue("zap_processed", payload.Job.ZapProcessed ?? "");
            cmd.Parameters.AddWithValue("report_sent", payload.Job.ReportSent ?? "");
            cmd.Parameters.AddWithValue("primary_service", payload.Services?.Primary ?? "");
            cmd.Parameters.AddWithValue("additional1", payload.Services?.Additional1 ?? "");
            cmd.Parameters.AddWithValue("additional2", payload.Services?.Additional2 ?? "");
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

static DateTime? ParseNullableDateTime(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;

    if (DateTime.TryParse(value, out var parsed))
        return parsed;

    return null;
}

public class BookingEmailFailureRequest
{
    public string? ErrorMessage { get; set; }
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
    public string JobDate { get; set; } = "";
    public int InspectionDurationMinutes { get; set; } = 0;
    public string SourceUpdatedAtUtc { get; set; } = "";
    public string DateAddedUtc { get; set; } = "";
    public string Status { get; set; } = "";
    public string ZapProcessed { get; set; } = "";
    public string ReportSent { get; set; } = "";
}

public class ServicesSection
{
    public string Primary { get; set; } = "";
    public string Additional1 { get; set; } = "";
    public string Additional2 { get; set; } = "";
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