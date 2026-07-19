using Npgsql;

public static class TenantContactConfigurationEndpoints
{
    public static void MapTenantContactConfigurationEndpoints(this IEndpointRouteBuilder endpoints,string connectionString,Func<HttpContext,Guid,CancellationToken,Task<bool>> authorizeTenant,Func<HttpContext,Guid,CancellationToken,Task<string>> resolveActor)
    {
        var group=endpoints.MapGroup("/automation/contact-configuration");
        group.MapGet("/current",async(HttpContext context,Guid tenantId,CancellationToken ct)=>
        {if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);var state=await TenantContactConfigurationSupport.LoadAsync(connection,tenantId,ct);var settings=new List<object>();await using(var command=new NpgsqlCommand("SELECT recipient_binding_id,event_key,enabled,template_id,setting_version FROM public.recipient_binding_basic_settings WHERE tenant_id=@tenant ORDER BY event_key",connection)){command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))settings.Add(new{recipientBindingId=reader.GetGuid(0),eventKey=reader.GetString(1),enabled=reader.GetBoolean(2),templateId=reader.IsDBNull(3)?null:(Guid?)reader.GetGuid(3),settingVersion=reader.GetInt32(4)});}return Results.Ok(new{success=true,state,recipientSettings=settings});});
        group.MapPost("/discover",async(HttpContext context,Guid tenantId,ContactConfigurationDiscovery request,CancellationToken ct)=>
        {if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();var actor=await resolveActor(context,tenantId,ct);if(string.IsNullOrWhiteSpace(actor))return Results.Unauthorized();await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);try{return Results.Ok(new{success=true,state=await TenantContactConfigurationSupport.DiscoverAsync(connection,tenantId,request,actor,ct)});}catch(InvalidOperationException ex){return Results.BadRequest(new{success=false,status="invalid_contact_discovery",message=ex.Message});}});
        group.MapPost("/confirm",async(HttpContext context,Guid tenantId,ConfirmContactConfigurationRequest request,CancellationToken ct)=>
        {if(!request.Confirmed)return Results.BadRequest(new{success=false,status="confirmation_required",message="Confirm the Contacts & Recipients revision."});if(!await authorizeTenant(context,tenantId,ct))return Results.Unauthorized();var actor=await resolveActor(context,tenantId,ct);if(string.IsNullOrWhiteSpace(actor))return Results.Unauthorized();await using var connection=new NpgsqlConnection(connectionString);await connection.OpenAsync(ct);try{return Results.Ok(new{success=true,state=await TenantContactConfigurationSupport.ConfirmAsync(connection,tenantId,request.BillingRecipientConfirmed,actor,ct)});}catch(InvalidOperationException ex){return Results.Json(new{success=false,status="contact_review_required",message=ex.Message},statusCode:409);}});
    }
}
public sealed class ConfirmContactConfigurationRequest
{
    public bool Confirmed{get;set;} public bool BillingRecipientConfirmed{get;set;}
}
