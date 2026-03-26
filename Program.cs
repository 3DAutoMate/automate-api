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
    var host = Environment.GetEnvironmentVariable("PGHOST");
    var port = Environment.GetEnvironmentVariable("PGPORT");
    var db = Environment.GetEnvironmentVariable("PGDATABASE");
    var user = Environment.GetEnvironmentVariable("PGUSER");
    var password = Environment.GetEnvironmentVariable("PGPASSWORD");

    return Results.Ok(new
    {
        hasHost = !string.IsNullOrWhiteSpace(host),
        hasPort = !string.IsNullOrWhiteSpace(port),
        hasDatabase = !string.IsNullOrWhiteSpace(db),
        hasUser = !string.IsNullOrWhiteSpace(user),
        hasPassword = !string.IsNullOrWhiteSpace(password),
        hostPreview = string.IsNullOrWhiteSpace(host) ? null : host
    });
});

app.MapPost("/jobs/test-upload", async (Dictionary<string, object> payload) =>
{
    try
    {
        var host = Environment.GetEnvironmentVariable("PGHOST");
        var port = Environment.GetEnvironmentVariable("PGPORT");
        var database = Environment.GetEnvironmentVariable("PGDATABASE");
        var username = Environment.GetEnvironmentVariable("PGUSER");
        var password = Environment.GetEnvironmentVariable("PGPASSWORD");

        if (string.IsNullOrWhiteSpace(host) ||
            string.IsNullOrWhiteSpace(port) ||
            string.IsNullOrWhiteSpace(database) ||
            string.IsNullOrWhiteSpace(username) ||
            string.IsNullOrWhiteSpace(password))
        {
            return Results.Problem("One or more PG* environment variables are missing.");
        }

        var connectionString =
            $"Host={host};" +
            $"Port={port};" +
            $"Database={database};" +
            $"Username={username};" +
            $"Password={password};" +
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
