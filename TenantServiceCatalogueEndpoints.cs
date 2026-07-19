using Npgsql;

public static class TenantServiceCatalogueEndpoints
{
    public static void MapTenantServiceCatalogueEndpoints(this IEndpointRouteBuilder endpoints,string connectionString,
        Func<HttpContext,Guid,CancellationToken,Task<bool>> authorizeTenant,
        Func<HttpContext,Guid,CancellationToken,Task<string>> resolveActor)
    {
        var group=endpoints.MapGroup("/automation/service-catalogue");
        group.MapGet("/current",async(HttpContext context,Guid tenantId,CancellationToken ct)=>
        {
            if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();
            await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);await TenantServiceCatalogueSupport.EnsureAsync(connection,ct);
            var state=await TenantServiceCatalogueSupport.LoadAsync(connection,tenantId,ct);var active=await TenantServiceCatalogueSupport.LoadActiveAsync(connection,tenantId,ct);return Results.Ok(new{success=true,state,active=new{version=active.Version,draft=active.Draft}});
        });
        group.MapPost("/validate",async(HttpContext context,Guid tenantId,ServiceCatalogueDraft draft,CancellationToken ct)=>
        {
            if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();return Results.Ok(new{success=true,validation=TenantServiceCatalogueSupport.Validate(draft)});
        });
        group.MapPut("/draft",async(HttpContext context,Guid tenantId,ServiceCatalogueDraftSaveRequest request,CancellationToken ct)=>
        {
            if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();var actor=await resolveActor(context,tenantId,ct);if(string.IsNullOrWhiteSpace(actor))return Results.Unauthorized();
            await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);await TenantServiceCatalogueSupport.EnsureAsync(connection,ct);
            var result=await TenantServiceCatalogueSupport.SaveDraftAsync(connection,tenantId,request.ExpectedDraftVersion,request.Draft,actor,ct);return result.Success?Results.Ok(result):Results.Json(result,statusCode:409);
        });
        group.MapPost("/activate",async(HttpContext context,Guid tenantId,ServiceCatalogueActivateRequest request,CancellationToken ct)=>
        {
            if(!request.Confirmed)return Results.BadRequest(new{success=false,code="confirmation_required",message="Confirm service catalogue activation."});
            if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();var actor=await resolveActor(context,tenantId,ct);if(string.IsNullOrWhiteSpace(actor))return Results.Unauthorized();
            await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);await TenantServiceCatalogueSupport.EnsureAsync(connection,ct);
            var result=await TenantServiceCatalogueSupport.ActivateAsync(connection,tenantId,request.ExpectedDraftVersion,actor,ct);return result.Success?Results.Ok(result):Results.Json(result,statusCode:409);
        });
        group.MapGet("/condition-values",async(HttpContext context,Guid tenantId,CancellationToken ct)=>
        {
            if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);await TenantServiceCatalogueSupport.EnsureAsync(connection,ct);var active=await TenantServiceCatalogueSupport.LoadActiveAsync(connection,tenantId,ct);
            return Results.Ok(new{success=true,catalogueVersion=active.Version,categories=active.Draft.Categories.Select(x=>new{x.Id,x.Name,x.Archived}),services=active.Draft.Services.Select(x=>new{x.Id,x.Name,x.CategoryId,x.Archived}),modifierGroups=active.Draft.ModifierGroups.Select(x=>new{x.Id,x.Name,x.Archived}),operators=new[]{"includes","does_not_include","equals","greater_than","less_than"}});
        });
        group.MapGet("/job-preview",async(HttpContext context,Guid tenantId,Guid jobId,CancellationToken ct)=>
        {
            if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();
            await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);await TenantServiceCatalogueSupport.EnsureAsync(connection,ct);
            const string sql="SELECT service_catalogue_version,service_catalogue_snapshot_json::text,service_catalogue_review_required FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job LIMIT 1";
            await using var command=new NpgsqlCommand(sql,connection);command.Parameters.AddWithValue("tenant",tenantId.ToString());command.Parameters.AddWithValue("job",jobId);await using var reader=await command.ExecuteReaderAsync(ct);
            if(!await reader.ReadAsync(ct))return Results.NotFound(new{success=false,message="The selected job was not found for this company."});
            return Results.Ok(new{success=true,jobId,catalogueVersion=reader.IsDBNull(0)?0:reader.GetInt32(0),snapshot=reader.IsDBNull(1)?"{}":reader.GetString(1),reviewRequired=!reader.IsDBNull(2)&&reader.GetBoolean(2)});
        });
    }
}
public sealed record ServiceCatalogueDraftSaveRequest(int ExpectedDraftVersion,ServiceCatalogueDraft Draft);
public sealed record ServiceCatalogueActivateRequest(int ExpectedDraftVersion,bool Confirmed);
