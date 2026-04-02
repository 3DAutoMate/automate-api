using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

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

app.MapGet("/which-db-url", (IConfiguration config) =>
{
    var raw = config["DATABASE_PUBLIC_URL"] ?? "";

    return Results.Ok(new
    {
        hasDatabasePublicUrl = !string.IsNullOrWhiteSpace(raw),
        startsWith = raw.Length > 40 ? raw.Substring(0, 40) : raw,
        containsRailwayInternal = raw.Contains("railway.internal", StringComparison.OrdinalIgnoreCase),
        containsProxyRlwyNet = raw.Contains("proxy.rlwy.net", StringComparison.OrdinalIgnoreCase)
    });
});

app.MapGet("/db-test", async () =>
{
    try
    {
        await using var conn = new NpgsqlConnection(connectionString);
await conn.OpenAsync();

// 1. Ensure table exists
const string createTableSql = @"
CREATE TABLE IF NOT EXISTS public.jobs
(
    job_id uuid PRIMARY KEY,
    inspector_id uuid NOT NULL,
    job_name text,
    site_address text,
    created_at timestamptz DEFAULT NOW()
);";

await using (var createCmd = new NpgsqlCommand(createTableSql, conn))
{
    await createCmd.ExecuteNonQueryAsync();
}

// 2. SIMPLE INSERT (no upsert yet)
const string insertSql = @"
INSERT INTO public.jobs
(job_id, inspector_id, job_name, site_address)
VALUES
(@job_id, @inspector_id, @job_name, @site_address);";

await using (var cmd = new NpgsqlCommand(insertSql, conn))
{
    cmd.Parameters.AddWithValue("job_id", jobId);
    cmd.Parameters.AddWithValue("inspector_id", inspectorId);
    cmd.Parameters.AddWithValue("job_name", payload.Job.JobName ?? "");
    cmd.Parameters.AddWithValue("site_address", payload.Job.SiteAddress ?? "");

    await cmd.ExecuteNonQueryAsync();
}

return Results.Ok(new
{
    success = true,
    message = "Inserted successfully",
    jobId = payload.Job.JobId
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

        var payload = JsonSerializer.Deserialize<JobUploadRequest>(body, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        if (payload == null || payload.Job == null)
            return Results.BadRequest("Invalid payload");

        if (!Guid.TryParse(payload.Job.JobId, out Guid jobId))
            return Results.BadRequest("Invalid JobId");

        if (!Guid.TryParse(payload.TenantId, out Guid inspectorId))
            return Results.BadRequest("Invalid TenantId");

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // ONLY CREATE TABLE
        const string createTableSql = @"
CREATE TABLE IF NOT EXISTS public.jobs
(
    job_id uuid PRIMARY KEY,
    inspector_id uuid NOT NULL,
    job_name text,
    site_address text,
    created_at timestamptz DEFAULT NOW()
);";

        await using (var cmd = new NpgsqlCommand(createTableSql, conn))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        return Results.Ok(new
        {
            success = true,
            message = "Table creation OK"
        });
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
