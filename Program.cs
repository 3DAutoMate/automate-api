using System.Text.Json;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// FORCE use of Railway public DB URL
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
ADD COLUMN IF NOT EXISTS contact1_first_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_last_name text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_email text NULL;

ALTER TABLE public.jobs_staging
ADD COLUMN IF NOT EXISTS contact1_cellular text NULL;

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
// GET PENDING WORKFLOWS
// Full shared JSON for all current/future zaps
// =============================
app.MapGet("/jobs/pending-workflows", async () =>
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
    contact1_first_name,
    contact1_last_name,
    contact1_email,
    contact1_cellular,
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
WHERE
(
    (booking_email_sent = false OR booking_email_retry_requested = true)
    OR (terms_sent = false OR terms_retry_requested = true)
    OR (invoice_sent = false OR invoice_retry_requested = true)
    OR (calendar_created = false OR calendar_retry_requested = true)
    OR (report_workflow_sent = false OR report_retry_requested = true)
)
ORDER BY updated_at ASC
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

                contact1_first_name = reader["contact1_first_name"]?.ToString(),
                contact1_last_name = reader["contact1_last_name"]?.ToString(),
                contact1_email = reader["contact1_email"]?.ToString(),
                contact1_cellular = reader["contact1_cellular"]?.ToString(),

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
    contact1_first_name,
    contact1_last_name,
    contact1_email,
    contact1_cellular,
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

                contact1_first_name = reader["contact1_first_name"]?.ToString(),
                contact1_last_name = reader["contact1_last_name"]?.ToString(),
                contact1_email = reader["contact1_email"]?.ToString(),
                contact1_cellular = reader["contact1_cellular"]?.ToString(),

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
    contact1_first_name text,
    contact1_last_name text,
    contact1_email text,
    contact1_cellular text,
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
    contact1_first_name,
    contact1_last_name,
    contact1_email,
    contact1_cellular,
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
    @contact1_first_name,
    @contact1_last_name,
    @contact1_email,
    @contact1_cellular,
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
    contact1_first_name          = EXCLUDED.contact1_first_name,
    contact1_last_name           = EXCLUDED.contact1_last_name,
    contact1_email               = EXCLUDED.contact1_email,
    contact1_cellular            = EXCLUDED.contact1_cellular,
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
            cmd.Parameters.AddWithValue("contact1_first_name", payload.Contact1?.FirstName ?? "");
            cmd.Parameters.AddWithValue("contact1_last_name", payload.Contact1?.LastName ?? "");
            cmd.Parameters.AddWithValue("contact1_email", payload.Contact1?.Email ?? "");
            cmd.Parameters.AddWithValue("contact1_cellular", payload.Contact1?.Cellular ?? "");
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
