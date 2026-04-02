using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

var builder = WebApplication.CreateBuilder(args);

// Railway Postgres environment variables
var host = builder.Configuration["PGHOST"];
var port = builder.Configuration["PGPORT"];
var database = builder.Configuration["PGDATABASE"];
var username = builder.Configuration["PGUSER"];
var password = builder.Configuration["PGPASSWORD"];

if (string.IsNullOrWhiteSpace(host) ||
    string.IsNullOrWhiteSpace(port) ||
    string.IsNullOrWhiteSpace(database) ||
    string.IsNullOrWhiteSpace(username) ||
    string.IsNullOrWhiteSpace(password))
{
    throw new Exception("One or more required PG* database environment variables are missing.");
}

var connectionString =
    $"Host={host};" +
    $"Port={port};" +
    $"Database={database};" +
    $"Username={username};" +
    $"Password={password};" +
    $"SSL Mode=Require;" +
    $"Trust Server Certificate=true;";

var app = builder.Build();

app.MapGet("/env-check-2", (IConfiguration config) =>
{
    return Results.Ok(new
    {
        PGHOST = config["PGHOST"],
        DATABASE_URL = string.IsNullOrWhiteSpace(config["DATABASE_URL"]) ? "(missing)" : "(present)",
        DATABASE_PUBLIC_URL = string.IsNullOrWhiteSpace(config["DATABASE_PUBLIC_URL"]) ? "(missing)" : "(present)"
    });
});

app.MapGet("/env-check", (IConfiguration config) =>
{
    return Results.Ok(new
    {
        PGHOST = config["PGHOST"],
        PGPORT = config["PGPORT"],
        PGDATABASE = config["PGDATABASE"],
        PGUSER = config["PGUSER"],
        HasPassword = !string.IsNullOrWhiteSpace(config["PGPASSWORD"])
    });
});

app.MapGet("/", () => Results.Ok(new
{
    ok = true,
    service = "3D AutoMate API"
}));

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
        Console.WriteLine("===== DB TEST ERROR =====");
        Console.WriteLine(ex.ToString());
        Console.WriteLine("=========================");

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

        Console.WriteLine("===== UPSERT PAYLOAD =====");
        Console.WriteLine(body);
        Console.WriteLine("==========================");

        var payload = JsonSerializer.Deserialize<JobUploadRequest>(body, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        if (payload == null)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Could not deserialize request body."
            });
        }

        if (payload.Job == null || string.IsNullOrWhiteSpace(payload.Job.JobId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Job.JobId is required."
            });
        }

        if (string.IsNullOrWhiteSpace(payload.TenantId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "TenantId is required."
            });
        }

        if (!Guid.TryParse(payload.Job.JobId, out Guid jobId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Job.JobId is not a valid GUID."
            });
        }

        if (!Guid.TryParse(payload.TenantId, out Guid inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "TenantId is not a valid GUID."
            });
        }

        string sourceSystem = string.IsNullOrWhiteSpace(payload.SourceSystem) ? "THREED" : payload.SourceSystem;
        string jobName = payload.Job.JobName ?? "";
        string fileNumber = payload.Job.JobName ?? "";
        string payloadVersion = string.IsNullOrWhiteSpace(payload.Meta?.ConnectorVersion) ? "2.0" : payload.Meta.ConnectorVersion;
        string siteAddress = payload.Job.SiteAddress ?? "";
        string status = payload.Job.Status ?? "";
        string zapProcessed = payload.Job.ZapProcessed ?? "";
        string reportSent = payload.Job.ReportSent ?? "";
        string primaryService = payload.Services?.Primary ?? "";
        string additional1 = payload.Services?.Additional1 ?? "";
        string additional2 = payload.Services?.Additional2 ?? "";
        string contact1FirstName = payload.Contact1?.FirstName ?? "";
        string contact1LastName = payload.Contact1?.LastName ?? "";
        string contact1Email = payload.Contact1?.Email ?? "";
        string contact1Cellular = payload.Contact1?.Cellular ?? "";
        string contact2FirstName = payload.Contact2?.FirstName ?? "";
        string contact2LastName = payload.Contact2?.LastName ?? "";
        string contact2Email = payload.Contact2?.Email ?? "";
        string contact2Cellular = payload.Contact2?.Cellular ?? "";

        DateTime nowUtc = DateTime.UtcNow;

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string createTableSql = @"
CREATE TABLE IF NOT EXISTS public.jobs
(
    job_id uuid PRIMARY KEY,
    inspector_id uuid NOT NULL,
    source_system text NOT NULL DEFAULT '',
    job_name text NOT NULL DEFAULT '',
    file_number text NOT NULL DEFAULT '',
    payload_version text NOT NULL DEFAULT '',
    site_address text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT '',
    zap_processed text NOT NULL DEFAULT '',
    report_sent text NOT NULL DEFAULT '',
    primary_service text NOT NULL DEFAULT '',
    additional1 text NOT NULL DEFAULT '',
    additional2 text NOT NULL DEFAULT '',
    contact1_first_name text NOT NULL DEFAULT '',
    contact1_last_name text NOT NULL DEFAULT '',
    contact1_email text NOT NULL DEFAULT '',
    contact1_cellular text NOT NULL DEFAULT '',
    contact2_first_name text NOT NULL DEFAULT '',
    contact2_last_name text NOT NULL DEFAULT '',
    contact2_email text NOT NULL DEFAULT '',
    contact2_cellular text NOT NULL DEFAULT '',
    raw_payload_json jsonb,
    last_synced_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);";

        await using (var createCmd = new NpgsqlCommand(createTableSql, conn))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        const string upsertSql = @"
INSERT INTO public.jobs
(
    job_id,
    inspector_id,
    source_system,
    job_name,
    file_number,
    payload_version,
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
    raw_payload_json,
    last_synced_at,
    created_at,
    updated_at
)
VALUES
(
    @job_id,
    @inspector_id,
    @source_system,
    @job_name,
    @file_number,
    @payload_version,
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
    @raw_payload_json,
    @last_synced_at,
    @created_at,
    @updated_at
)
ON CONFLICT (job_id)
DO UPDATE SET
    inspector_id        = EXCLUDED.inspector_id,
    source_system       = EXCLUDED.source_system,
    job_name            = EXCLUDED.job_name,
    file_number         = EXCLUDED.file_number,
    payload_version     = EXCLUDED.payload_version,
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
    raw_payload_json    = EXCLUDED.raw_payload_json,
    last_synced_at      = EXCLUDED.last_synced_at,
    updated_at          = EXCLUDED.updated_at;";

        await using var cmd = new NpgsqlCommand(upsertSql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("inspector_id", inspectorId);
        cmd.Parameters.AddWithValue("source_system", sourceSystem);
        cmd.Parameters.AddWithValue("job_name", jobName);
        cmd.Parameters.AddWithValue("file_number", fileNumber);
        cmd.Parameters.AddWithValue("payload_version", payloadVersion);
        cmd.Parameters.AddWithValue("site_address", siteAddress);
        cmd.Parameters.AddWithValue("status", status);
        cmd.Parameters.AddWithValue("zap_processed", zapProcessed);
        cmd.Parameters.AddWithValue("report_sent", reportSent);
        cmd.Parameters.AddWithValue("primary_service", primaryService);
        cmd.Parameters.AddWithValue("additional1", additional1);
        cmd.Parameters.AddWithValue("additional2", additional2);
        cmd.Parameters.AddWithValue("contact1_first_name", contact1FirstName);
        cmd.Parameters.AddWithValue("contact1_last_name", contact1LastName);
        cmd.Parameters.AddWithValue("contact1_email", contact1Email);
        cmd.Parameters.AddWithValue("contact1_cellular", contact1Cellular);
        cmd.Parameters.AddWithValue("contact2_first_name", contact2FirstName);
        cmd.Parameters.AddWithValue("contact2_last_name", contact2LastName);
        cmd.Parameters.AddWithValue("contact2_email", contact2Email);
        cmd.Parameters.AddWithValue("contact2_cellular", contact2Cellular);
        cmd.Parameters.Add("raw_payload_json", NpgsqlDbType.Jsonb).Value = body;
        cmd.Parameters.AddWithValue("last_synced_at", nowUtc);
        cmd.Parameters.AddWithValue("created_at", nowUtc);
        cmd.Parameters.AddWithValue("updated_at", nowUtc);

        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Job upserted successfully.",
            jobId = payload.Job.JobId,
            syncedAtUtc = nowUtc.ToString("O")
        });
    }
    catch (PostgresException pgEx)
    {
        Console.WriteLine("===== POSTGRES ERROR =====");
        Console.WriteLine(pgEx.ToString());
        Console.WriteLine("==========================");

        return Results.Problem(
            title: "Database error",
            detail: $"Postgres: {pgEx.MessageText} | SQLSTATE: {pgEx.SqlState} | Table: {pgEx.TableName} | Constraint: {pgEx.ConstraintName}",
            statusCode: 500
        );
    }
    catch (Exception ex)
    {
        Console.WriteLine("===== SERVER ERROR =====");
        Console.WriteLine(ex.ToString());
        Console.WriteLine("========================");

        return Results.Problem(
            title: "Server error",
            detail: ex.Message,
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
