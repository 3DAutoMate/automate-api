using Npgsql;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/", () => "3D AutoMate API is running");

app.MapPost("/jobs/test-upload", async (HttpRequest request) =>
{
    using var reader = new StreamReader(request.Body);
    var body = await reader.ReadToEndAsync();

    var connectionString = Environment.GetEnvironmentVariable("DATABASE_URL");

    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    var cmd = new NpgsqlCommand(
        "INSERT INTO connector_test_jobs (raw_payload) VALUES (CAST(@p AS jsonb));",
        conn
    );

    cmd.Parameters.AddWithValue("p", body);

    await cmd.ExecuteNonQueryAsync();

    return Results.Ok("Inserted");
});

app.Run();