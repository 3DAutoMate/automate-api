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
    var raw = Environment.GetEnvironmentVariable("DATABASE_PUBLIC_URL");

    return Results.Ok(new
    {
        hasDatabasePublicUrl = !string.IsNullOrWhiteSpace(raw),
        databasePublicUrlPreview = string.IsNullOrWhiteSpace(raw)
            ? null
            : raw.Substring(0, Math.Min(raw.Length, 100))
    });
});

app.MapPost("/jobs/test-upload", async (Dictionary<string, object> payload) =>
{
    try
    {
        var raw = Environment.GetEnvironmentVariable("DATABASE_PUBLIC_URL");

        if (string.IsNullOrWhiteSpace(raw))
        {
            return Results.Problem("DATABASE_PUBLIC_URL is missing.");
        }

        var connectionString = raw;

        if (!connectionString.Contains("SSL Mode=", StringComparison.OrdinalIgnoreCase))
        {
            connectionString += connectionString.Contains('?')
                ? "&sslmode=require"
                : "?sslmode=require";
        }

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
