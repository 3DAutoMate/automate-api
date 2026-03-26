using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/", () => Results.Ok("3D AutoMate API is running"));

app.MapPost("/jobs/test-upload", (Dictionary<string, object> payload) =>
{
    string json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
    {
        WriteIndented = true
    });

    Console.WriteLine("===== PAYLOAD RECEIVED =====");
    Console.WriteLine(json);
    Console.WriteLine("============================");

    return Results.Ok(new
    {
        success = true,
        message = "Payload received",
        received = payload
    });
});

app.Run();
