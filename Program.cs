using System.Text.Json.Serialization;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// Railway usually provides this as an environment variable.
// Use your actual env var name if different.

var rawConnectionString =
    builder.Configuration.GetConnectionString("Postgres")
    ?? builder.Configuration["DATABASE_URL"]
    ?? throw new Exception("Postgres connection string not found.");

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

app.MapGet("/", () => Results.Ok(new { ok = true, service = "3D AutoMate API" }));

app.MapPost("/jobs/upsert", async (JobUploadRequest payload) =>
{
    try
    {
        if (payload == null)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Request body was empty."
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

        // Your jobs table requires inspector_id, but your connector currently sends TenantId.
        // For now, this assumes inspectors.inspector_id = TenantId for your test tenant.
        // If not, we can add a lookup layer next.
        if (!Guid.TryParse(payload.TenantId, out var inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "TenantId is not a valid GUID."
            });
        }

        if (!Guid.TryParse(payload.Job.JobId, out var jobId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Job.JobId is not a valid GUID."
            });
        }

        var sourceSystem = payload.SourceSystem ?? "THREED";
        var jobName = payload.Job.JobName ?? "";
        var siteAddress = payload.Job.SiteAddress ?? "";
        var payloadVersion = "2.0";
        var nowUtc = DateTime.UtcNow;

        // Adjust these column names if your schema differs.
        // This version assumes:
        // jobs(job_id, inspector_id, source_system, job_name, file_number, payload_version,
        //      last_synced_at, created_at, updated_at)
        //
        // file_number is being populated from JobName for now because your current connector
        // payload does not send a separate file number yet.

        const string sql = @"
INSERT INTO jobs
(
    job_id,
    inspector_id,
    source_system,
    job_name,
    file_number,
    payload_version,
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
    @last_synced_at,
    @created_at,
    @updated_at
)
ON CONFLICT (job_id)
DO UPDATE SET
    inspector_id   = EXCLUDED.inspector_id,
    source_system  = EXCLUDED.source_system,
    job_name       = EXCLUDED.job_name,
    file_number    = EXCLUDED.file_number,
    payload_version= EXCLUDED.payload_version,
    last_synced_at = EXCLUDED.last_synced_at,
    updated_at     = EXCLUDED.updated_at;
";

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("inspector_id", inspectorId);
        cmd.Parameters.AddWithValue("source_system", sourceSystem);
        cmd.Parameters.AddWithValue("job_name", jobName);
        cmd.Parameters.AddWithValue("file_number", jobName); // temporary mapping
        cmd.Parameters.AddWithValue("payload_version", payloadVersion);
        cmd.Parameters.AddWithValue("last_synced_at", nowUtc);
        cmd.Parameters.AddWithValue("created_at", nowUtc);
        cmd.Parameters.AddWithValue("updated_at", nowUtc);

        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Job upserted successfully.",
            action = "upserted",
            jobId = payload.Job.JobId,
            syncedAtUtc = nowUtc.ToString("O")
        });
    }
    catch (PostgresException pgEx)
    {
        return Results.Problem(
            title: "Database error",
            detail: pgEx.MessageText,
            statusCode: 500
        );
    }
    catch (Exception ex)
    {
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
    public JobSection Job { get; set; } = new();
    public ServicesSection Services { get; set; } = new();
    public ContactFlat Contact1 { get; set; } = new();
    public ContactFlat Contact2 { get; set; } = new();
    public MetaSection Meta { get; set; } = new();
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
