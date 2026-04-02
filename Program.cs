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

app.MapGet("/", () => Results.Ok(new
{
    ok = true,
    service = "3D AutoMate API"
}));

app.MapGet("/jobs/latest", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
SELECT job_id, inspector_id, job_name, site_address, updated_at
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
                inspector_id = reader["inspector_id"]?.ToString(),
                job_name = reader["job_name"]?.ToString(),
                site_address = reader["site_address"]?.ToString(),
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
                message = "Invalid JobId"
            });
        }

        if (!Guid.TryParse(payload.TenantId, out Guid inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Invalid TenantId"
            });
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string createTableSql = @"
CREATE TABLE IF NOT EXISTS public.jobs_staging
(
    job_id uuid PRIMARY KEY,
    inspector_id uuid NOT NULL,
    source_system text,
    job_name text,
    site_address text,
    status text,
    zap_processed text,
    report_sent text,
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
    job_id,
    inspector_id,
    source_system,
    job_name,
    site_address,
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
    @job_id,
    @inspector_id,
    @source_system,
    @job_name,
    @site_address,
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
    inspector_id        = EXCLUDED.inspector_id,
    source_system       = EXCLUDED.source_system,
    job_name            = EXCLUDED.job_name,
    site_address        = EXCLUDED.site_address,
    status              = EXCLUDED.status,
    zap_processed       = EXCLUDED.zap_processed,
    report_sent         = EXCLUDED.report_sent,
    primary_service     = EXCLUDED.primary_service,
    additional1         = EXCLUDED.additional1,
    additional2         = EXCLUDED.additional2,
    contact1_first_name = EXCLUDED.contact1_first_name,
    contact1_last_name  = EXCLUDED.contact1_last_name,
    contact1_email      = EXCLUDED.contact1_email,
    contact1_cellular   = EXCLUDED.contact1_cellular,
    contact2_first_name = EXCLUDED.contact2_first_name,
    contact2_last_name  = EXCLUDED.contact2_last_name,
    contact2_email      = EXCLUDED.contact2_email,
    contact2_cellular   = EXCLUDED.contact2_cellular,
    extracted_at_utc    = EXCLUDED.extracted_at_utc,
    connector_version   = EXCLUDED.connector_version,
    source_instance     = EXCLUDED.source_instance,
    raw_payload_json    = EXCLUDED.raw_payload_json,
    updated_at          = NOW();";

        await using (var cmd = new NpgsqlCommand(upsertSql, conn))
        {
            cmd.Parameters.AddWithValue("job_id", jobId);
            cmd.Parameters.AddWithValue("inspector_id", inspectorId);
            cmd.Parameters.AddWithValue("source_system", payload.SourceSystem ?? "");
            cmd.Parameters.AddWithValue("job_name", payload.Job.JobName ?? "");
            cmd.Parameters.AddWithValue("site_address", payload.Job.SiteAddress ?? "");
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
            jobId = payload.Job.JobId
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
    public string JobName { get; set; } = "";
    public string SiteAddress { get; set; } = "";
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
