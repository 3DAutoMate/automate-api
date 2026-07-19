using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class CompanyGoogleCalendarSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        await using var command=new NpgsqlCommand("""
CREATE TABLE IF NOT EXISTS public.tenant_google_calendar_settings
(tenant_id uuid PRIMARY KEY,company_account_inspector_id uuid NOT NULL,settings_version integer NOT NULL DEFAULT 1,updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS public.tenant_google_inspector_calendars
(tenant_id uuid NOT NULL,inspector_id uuid NOT NULL,calendar_id text NOT NULL,calendar_name text NOT NULL DEFAULT '',inspector_email text NOT NULL DEFAULT '',share_role text NOT NULL DEFAULT 'reader',sharing_status text NOT NULL DEFAULT 'pending',enabled boolean NOT NULL DEFAULT true,last_sync_at timestamptz NULL,last_error text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW(),PRIMARY KEY(tenant_id,inspector_id));
CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_google_calendar ON public.tenant_google_inspector_calendars(tenant_id,calendar_id) WHERE enabled;
CREATE TABLE IF NOT EXISTS public.tenant_google_calendar_audit
(audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,actor text NOT NULL,detail_json jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT NOW());
""",connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<CompanyGoogleSettings> LoadAsync(NpgsqlConnection connection,Guid tenantId,Guid accountInspectorId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using(var seed=new NpgsqlCommand("INSERT INTO public.tenant_google_calendar_settings(tenant_id,company_account_inspector_id) VALUES(@tenant,@account) ON CONFLICT(tenant_id) DO NOTHING",connection)){seed.Parameters.AddWithValue("tenant",tenantId);seed.Parameters.AddWithValue("account",accountInspectorId);await seed.ExecuteNonQueryAsync(ct);}
        int version=1;Guid account=accountInspectorId;DateTime updated=DateTime.UtcNow;await using(var command=new NpgsqlCommand("SELECT company_account_inspector_id,settings_version,updated_at FROM public.tenant_google_calendar_settings WHERE tenant_id=@tenant",connection)){command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);if(await reader.ReadAsync(ct)){account=reader.GetGuid(0);version=reader.GetInt32(1);updated=reader.GetDateTime(2);}}
        var mappings=new List<GoogleInspectorCalendarMapping>();await using(var command=new NpgsqlCommand("SELECT inspector_id,calendar_id,calendar_name,inspector_email,share_role,sharing_status,enabled,last_sync_at,last_error FROM public.tenant_google_inspector_calendars WHERE tenant_id=@tenant ORDER BY calendar_name",connection)){command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))mappings.Add(new(reader.GetGuid(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5),reader.GetBoolean(6),reader.IsDBNull(7)?null:reader.GetDateTime(7),reader.GetString(8)));}
        return new(version,account,updated,mappings);
    }

    public static async Task<CompanyGoogleSettings> SaveAsync(NpgsqlConnection connection,CompanyGoogleSaveRequest request,string actor,CancellationToken ct=default)
    {
        if(!request.Confirmed)throw new CompanyGoogleException("confirmation_required","Confirm the company Google calendar mappings.");if(request.Mappings.Where(x=>x.Enabled).GroupBy(x=>x.CalendarId,StringComparer.OrdinalIgnoreCase).Any(x=>x.Count()>1))throw new CompanyGoogleException("duplicate_calendar","One Google calendar cannot be assigned to multiple inspectors.");
        var current=await LoadAsync(connection,request.TenantId,request.CompanyAccountInspectorId,ct);if(current.Version!=request.ExpectedVersion)throw new CompanyGoogleException("version_conflict","Google calendar mappings changed elsewhere. Reload and review them.");await using var tx=await connection.BeginTransactionAsync(ct);
        await using(var update=new NpgsqlCommand("UPDATE public.tenant_google_calendar_settings SET company_account_inspector_id=@account,settings_version=settings_version+1,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant",connection,tx)){update.Parameters.AddWithValue("tenant",request.TenantId);update.Parameters.AddWithValue("account",request.CompanyAccountInspectorId);update.Parameters.AddWithValue("actor",actor);await update.ExecuteNonQueryAsync(ct);}
        foreach(var item in request.Mappings){await using var command=new NpgsqlCommand("""INSERT INTO public.tenant_google_inspector_calendars(tenant_id,inspector_id,calendar_id,calendar_name,inspector_email,share_role,sharing_status,enabled) VALUES(@tenant,@inspector,@calendar,@name,@email,@role,'pending',@enabled) ON CONFLICT(tenant_id,inspector_id) DO UPDATE SET calendar_id=EXCLUDED.calendar_id,calendar_name=EXCLUDED.calendar_name,inspector_email=EXCLUDED.inspector_email,share_role=EXCLUDED.share_role,sharing_status='pending',enabled=EXCLUDED.enabled,updated_at=NOW()""",connection,tx);command.Parameters.AddWithValue("tenant",request.TenantId);command.Parameters.AddWithValue("inspector",item.InspectorId);command.Parameters.AddWithValue("calendar",item.CalendarId.Trim());command.Parameters.AddWithValue("name",item.CalendarName??"");command.Parameters.AddWithValue("email",item.InspectorEmail??"");command.Parameters.AddWithValue("role",item.ShareRole=="writer"?"writer":"reader");command.Parameters.AddWithValue("enabled",item.Enabled);await command.ExecuteNonQueryAsync(ct);}
        await using(var audit=new NpgsqlCommand("INSERT INTO public.tenant_google_calendar_audit(tenant_id,actor,detail_json) VALUES(@tenant,@actor,CAST(@detail AS jsonb))",connection,tx)){audit.Parameters.AddWithValue("tenant",request.TenantId);audit.Parameters.AddWithValue("actor",actor);audit.Parameters.AddWithValue("detail",JsonSerializer.Serialize(new{request.CompanyAccountInspectorId,mappings=request.Mappings.Select(x=>new{x.InspectorId,x.CalendarId,x.Enabled,x.ShareRole})}));await audit.ExecuteNonQueryAsync(ct);}await tx.CommitAsync(ct);return await LoadAsync(connection,request.TenantId,request.CompanyAccountInspectorId,ct);
    }
}

public sealed record CompanyGoogleSettings(int Version,Guid CompanyAccountInspectorId,DateTime UpdatedAt,List<GoogleInspectorCalendarMapping> Mappings);
public sealed record GoogleInspectorCalendarMapping(Guid InspectorId,string CalendarId,string CalendarName,string InspectorEmail,string ShareRole,string SharingStatus,bool Enabled,DateTime? LastSyncAt,string LastError);
public sealed record GoogleInspectorCalendarSave(Guid InspectorId,string CalendarId,string CalendarName,string InspectorEmail,string ShareRole,bool Enabled);
public sealed record CompanyGoogleSaveRequest(Guid TenantId,Guid CompanyAccountInspectorId,int ExpectedVersion,List<GoogleInspectorCalendarSave> Mappings,bool Confirmed);
public sealed class CompanyGoogleException(string code,string message):Exception(message){public string Code{get;}=code;}
