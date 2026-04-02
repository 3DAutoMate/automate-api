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

        JobUploadRequest? payload = null;

        try
        {
            payload = JsonSerializer.Deserialize<JobUploadRequest>(body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(
                title: "Deserialize failed",
                detail: ex.ToString(),
                statusCode: 500
            );
        }

        if (payload == null)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Payload deserialized to null."
            });
        }

        if (payload.Job == null)
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Payload.Job is null."
            });
        }

        if (string.IsNullOrWhiteSpace(payload.Job.JobId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Job.JobId is blank."
            });
        }

        if (string.IsNullOrWhiteSpace(payload.TenantId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "TenantId is blank."
            });
        }

        if (!Guid.TryParse(payload.Job.JobId, out Guid jobId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "Job.JobId is not a valid GUID.",
                value = payload.Job.JobId
            });
        }

        if (!Guid.TryParse(payload.TenantId, out Guid inspectorId))
        {
            return Results.BadRequest(new
            {
                success = false,
                message = "TenantId is not a valid GUID.",
                value = payload.TenantId
            });
        }

        try
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();

            return Results.Ok(new
            {
                success = true,
                message = "Payload validated and DB connection opened successfully.",
                jobId = payload.Job.JobId,
                tenantId = payload.TenantId,
                jobName = payload.Job.JobName
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(
                title: "DB open failed",
                detail: ex.ToString(),
                statusCode: 500
            );
        }
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Unhandled server error",
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
