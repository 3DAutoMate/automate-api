var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    ok = true,
    service = "3D AutoMate API"
}));

app.MapPost("/jobs/upsert", async (HttpContext context) =>
{
    using var reader = new StreamReader(context.Request.Body);
    var body = await reader.ReadToEndAsync();

    Console.WriteLine("===== UPSERT PAYLOAD =====");
    Console.WriteLine(body);
    Console.WriteLine("==========================");

    return Results.Ok(new
    {
        success = true,
        message = "Upsert received (no DB yet)."
    });
});

app.Run();
