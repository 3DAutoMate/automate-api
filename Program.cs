using Npgsql;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/", () => Results.Ok("3D AutoMate API is running"));

app.MapGet("/debug-db", () =>
{
    var raw = Environment.GetEnvironmentVariable("DATABASE_URL");

    return Results.Ok(new
    {
        hasDatabaseUrl = !string.IsNullOrWhiteSpace(raw),
        databaseUrlPreview = string.IsNullOrWhiteSpace(raw)
            ? null
            : raw.Substring(0, Math.Min(raw.Length, 80))
    });
});

app.MapPost("/jobs/test-upload", async (Dictionary<string, object> payload) =>
{
    try
    {
        var raw = Environment.GetEnvironmentVariable("DATABASE_URL");

        if (string.IsNullOrWhiteSpace(raw))
        {
            return Results.Problem("DATABASE_URL is missing.");
        }

        var uri = new Uri(raw);
        var userInfo = uri.UserInfo.Split(':', 2);

        if (userInfo.Length < 2)
        {
            return Results.Problem("DATABASE_URL user info is invalid.");
        }

        var connectionString =
            $"Host={uri.Host};" +
            $"Port={uri.Port};" +
            $"Username={userInfo[0]};" +
            $"Password={userInfo[1]};" +
            $"Database={uri.AbsolutePath.TrimStart('/')};" +
            $"Ssl Mode=Require;" +
            $"Trust Server Certificate=true;";

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        var json = JsonSerializer.Serialize(payload);

        await using var cmd = new NpgsqlCommand(
            "INSERT INTO connector_test_jobs (raw_payload) VALUES (CAST(@p AS jsonb));",
            conn
        );

        cmd.Parameters.AddWithValue("p", json);

        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new
        {
            success = true,
            message = "Inserted"
        });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Server error: {ex.Message}");
    }
});

app.Run();
