using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class TravelCalendarSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        const string sql="""
CREATE TABLE IF NOT EXISTS public.tenant_travel_settings
(
 tenant_id uuid PRIMARY KEY,settings_version integer NOT NULL DEFAULT 1,
 company_base_address text NOT NULL DEFAULT '',company_base_place_id text NOT NULL DEFAULT '',
 updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.tenant_inspector_travel_settings
(
 tenant_id uuid NOT NULL,inspector_id uuid NOT NULL,inspector_name text NOT NULL DEFAULT '',
 inspector_email text NOT NULL DEFAULT '',inspector_phone text NOT NULL DEFAULT '',
 enabled boolean NOT NULL DEFAULT false,base_address_override text NOT NULL DEFAULT '',
 base_place_id text NOT NULL DEFAULT '',settings_version integer NOT NULL DEFAULT 1,
 updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,inspector_id)
);
ALTER TABLE public.tenant_inspector_travel_settings ADD COLUMN IF NOT EXISTS source_active boolean NOT NULL DEFAULT true;
ALTER TABLE public.tenant_inspector_travel_settings ADD COLUMN IF NOT EXISTS source_phone text NOT NULL DEFAULT '';
ALTER TABLE public.tenant_inspector_travel_settings ADD COLUMN IF NOT EXISTS source_data_json jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE TABLE IF NOT EXISTS public.job_travel_calendar_evidence
(
 tenant_id uuid NOT NULL,job_id uuid NOT NULL,inspector_id uuid NOT NULL,
 calendar_id text NOT NULL DEFAULT '',event_id text NOT NULL DEFAULT '',html_link text NOT NULL DEFAULT '',
 route_fingerprint text NOT NULL DEFAULT '',distance_metres integer NOT NULL DEFAULT 0,
 duration_seconds integer NOT NULL DEFAULT 0,calculated_at timestamptz NULL,event_status text NOT NULL DEFAULT '',
 updated_at timestamptz NOT NULL DEFAULT NOW(),PRIMARY KEY(tenant_id,job_id)
);
CREATE TABLE IF NOT EXISTS public.travel_settings_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,inspector_id uuid NULL,
 actor text NOT NULL,detail_json jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT NOW()
);
""";
        await using var command=new NpgsqlCommand(sql,connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task DiscoverAsync(NpgsqlConnection connection,Guid tenantId,IReadOnlyList<TravelInspectorDiscovery> inspectors,string actor,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);
        await using var transaction=await connection.BeginTransactionAsync(ct);
        await using(var deactivate=new NpgsqlCommand("UPDATE public.tenant_inspector_travel_settings SET source_active=false,updated_at=NOW() WHERE tenant_id=@tenant",connection,transaction))
        {deactivate.Parameters.AddWithValue("tenant",tenantId);await deactivate.ExecuteNonQueryAsync(ct);}
        foreach(var inspector in inspectors.Where(x=>x.InspectorId!=Guid.Empty).GroupBy(x=>x.InspectorId).Select(x=>x.First()))
        {
            await using var command=new NpgsqlCommand("""
INSERT INTO public.tenant_inspector_travel_settings(tenant_id,inspector_id,inspector_name,inspector_email,source_phone,source_active,source_data_json,updated_by)
VALUES(@tenant,@inspector,@name,@email,@phone,true,CAST(@source AS jsonb),@actor)
ON CONFLICT(tenant_id,inspector_id) DO UPDATE SET inspector_name=EXCLUDED.inspector_name,
 inspector_email=EXCLUDED.inspector_email,source_phone=EXCLUDED.source_phone,source_active=true,
 source_data_json=EXCLUDED.source_data_json,updated_by=EXCLUDED.updated_by,updated_at=NOW();
""",connection,transaction);
            command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("inspector",inspector.InspectorId);
            command.Parameters.AddWithValue("name",inspector.Name??"");command.Parameters.AddWithValue("email",inspector.Email??"");command.Parameters.AddWithValue("phone",inspector.Phone??"");command.Parameters.AddWithValue("source",JsonSerializer.Serialize(inspector));command.Parameters.AddWithValue("actor",actor);
            await command.ExecuteNonQueryAsync(ct);
        }
        await transaction.CommitAsync(ct);
    }

    public static async Task<TravelSettingsView> LoadAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);
        await using(var seed=new NpgsqlCommand("INSERT INTO public.tenant_travel_settings(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection)){seed.Parameters.AddWithValue("tenant",tenantId);await seed.ExecuteNonQueryAsync(ct);}
        int version=1;string address="",placeId="";DateTime updatedAt=DateTime.UtcNow;
        await using(var command=new NpgsqlCommand("SELECT settings_version,company_base_address,company_base_place_id,updated_at FROM public.tenant_travel_settings WHERE tenant_id=@tenant",connection))
        {command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);if(await reader.ReadAsync(ct)){version=reader.GetInt32(0);address=reader.GetString(1);placeId=reader.GetString(2);updatedAt=reader.GetDateTime(3);}}
        var inspectors=new List<TravelInspectorView>();
        await using(var command=new NpgsqlCommand("SELECT inspector_id,inspector_name,inspector_email,COALESCE(NULLIF(inspector_phone,''),source_phone),enabled,base_address_override,base_place_id,settings_version,updated_at,source_data_json::text FROM public.tenant_inspector_travel_settings WHERE tenant_id=@tenant AND source_active=true ORDER BY inspector_name",connection))
        {command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct)){var baseAddress=reader.GetString(5);inspectors.Add(new(reader.GetGuid(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetBoolean(4),baseAddress,reader.GetString(6),reader.GetInt32(7),reader.GetDateTime(8),baseAddress,JsonSerializer.Deserialize<JsonElement>(reader.GetString(9))));}}
        return new(version,address,placeId,updatedAt,inspectors);
    }

    public static async Task<TravelSettingsView> SaveAsync(NpgsqlConnection connection,TravelSettingsSaveRequest request,string actor,CancellationToken ct=default)
    {
        if(!request.Confirmed)throw new TravelCalendarException("confirmation_required","Confirm the travel settings change.");
        await EnsureAsync(connection,ct);var current=await LoadAsync(connection,request.TenantId,ct);
        if(current.Version!=request.ExpectedVersion)throw new TravelCalendarException("version_conflict","Travel settings changed elsewhere. Reload and try again.");
        await using var transaction=await connection.BeginTransactionAsync(ct);
        await using(var command=new NpgsqlCommand("UPDATE public.tenant_travel_settings SET company_base_address=@address,company_base_place_id=@place,settings_version=settings_version+1,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant",connection,transaction))
        {command.Parameters.AddWithValue("tenant",request.TenantId);command.Parameters.AddWithValue("address",Clean(request.CompanyBaseAddress,300));command.Parameters.AddWithValue("place",Clean(request.CompanyBasePlaceId,200));command.Parameters.AddWithValue("actor",actor);await command.ExecuteNonQueryAsync(ct);}
        foreach(var item in request.Inspectors)
        {
            await using var command=new NpgsqlCommand("UPDATE public.tenant_inspector_travel_settings SET inspector_phone=@phone,enabled=@enabled,base_address_override=@address,base_place_id=@place,settings_version=settings_version+1,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant AND inspector_id=@inspector",connection,transaction);
            command.Parameters.AddWithValue("tenant",request.TenantId);command.Parameters.AddWithValue("inspector",item.InspectorId);command.Parameters.AddWithValue("phone",Clean(item.Phone,60));command.Parameters.AddWithValue("enabled",item.Enabled);command.Parameters.AddWithValue("address",Clean(item.BaseAddressOverride,300));command.Parameters.AddWithValue("place",Clean(item.BasePlaceId,200));command.Parameters.AddWithValue("actor",actor);if(await command.ExecuteNonQueryAsync(ct)==0)throw new TravelCalendarException("inspector_not_found","The selected THREED inspector is not registered for this company.");
        }
        await using(var audit=new NpgsqlCommand("INSERT INTO public.travel_settings_audit(tenant_id,actor,detail_json) VALUES(@tenant,@actor,CAST(@detail AS jsonb))",connection,transaction)){audit.Parameters.AddWithValue("tenant",request.TenantId);audit.Parameters.AddWithValue("actor",actor);audit.Parameters.AddWithValue("detail",JsonSerializer.Serialize(new{request.CompanyBaseAddress,inspectors=request.Inspectors.Select(x=>new{x.InspectorId,x.Enabled,x.Phone,x.BaseAddressOverride})}));await audit.ExecuteNonQueryAsync(ct);}
        await transaction.CommitAsync(ct);return await LoadAsync(connection,request.TenantId,ct);
    }

    public static string Fingerprint(params string?[] values)
    {var text=string.Join("|",values.Select(x=>(x??"").Trim().ToLowerInvariant()));return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text))).ToLowerInvariant();}
    private static string Clean(string? value,int max){var text=(value??"").Trim();return text.Length<=max?text:text[..max];}
}

public sealed class TravelInspectorDiscovery
{
    public Guid TenantId{get;set;} public Guid InspectorId{get;set;} public string Name{get;set;}=""; public Guid? CompanyId{get;set;}
    public string WorkExtension{get;set;}="";public string HomePhone{get;set;}="";public string Pager{get;set;}="";public string Title{get;set;}="";
    public string Miscellaneous1{get;set;}="";public string Miscellaneous2{get;set;}="";public string Email{get;set;}="";public string Map{get;set;}="";
    public bool Inactive{get;set;} public string Address{get;set;}="";public string City{get;set;}="";public string State{get;set;}="";
    public string PostalCode{get;set;}="";public string Phone{get;set;}="";public string RawPhone{get;set;}="";public string Fax{get;set;}="";public string ExternalLinkId{get;set;}="";
}
public sealed record TravelInspectorView(Guid InspectorId,string Name,string Email,string Phone,bool Enabled,string BaseAddressOverride,string BasePlaceId,int Version,DateTime UpdatedAt,string EffectiveBaseAddress,JsonElement SourceData);
public sealed record TravelSettingsView(int Version,string CompanyBaseAddress,string CompanyBasePlaceId,DateTime UpdatedAt,List<TravelInspectorView> Inspectors);
public sealed record TravelInspectorSave(Guid InspectorId,bool Enabled,string Phone,string BaseAddressOverride,string BasePlaceId);
public sealed record TravelSettingsSaveRequest(Guid TenantId,int ExpectedVersion,string CompanyBaseAddress,string CompanyBasePlaceId,List<TravelInspectorSave> Inspectors,bool Confirmed);
public sealed record TravelInspectorDiscoveryRequest(Guid TenantId,List<TravelInspectorDiscovery> Inspectors);
public sealed class TravelCalendarException(string code,string message):Exception(message){public string Code{get;}=code;}
