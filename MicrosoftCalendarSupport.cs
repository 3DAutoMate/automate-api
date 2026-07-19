using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class MicrosoftCalendarSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        await using var command=new NpgsqlCommand("""
CREATE TABLE IF NOT EXISTS public.tenant_microsoft_calendar_settings
(tenant_id uuid PRIMARY KEY,settings_version integer NOT NULL DEFAULT 1,updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS public.tenant_microsoft_inspector_calendars
(tenant_id uuid NOT NULL,inspector_id uuid NOT NULL,calendar_id text NOT NULL,calendar_name text NOT NULL DEFAULT '',inspector_email text NOT NULL DEFAULT '',share_role text NOT NULL DEFAULT 'read',sharing_status text NOT NULL DEFAULT 'pending',enabled boolean NOT NULL DEFAULT true,last_sync_at timestamptz NULL,last_error text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW(),PRIMARY KEY(tenant_id,inspector_id));
CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_microsoft_calendar ON public.tenant_microsoft_inspector_calendars(tenant_id,calendar_id) WHERE enabled;
CREATE TABLE IF NOT EXISTS public.tenant_microsoft_calendar_audit
(audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,actor text NOT NULL,detail_json jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT NOW());
""",connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<MicrosoftCalendarSettings> LoadAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using(var seed=new NpgsqlCommand("INSERT INTO public.tenant_microsoft_calendar_settings(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection)){seed.Parameters.AddWithValue("tenant",tenantId);await seed.ExecuteNonQueryAsync(ct);}int version;DateTime updated;await using(var command=new NpgsqlCommand("SELECT settings_version,updated_at FROM public.tenant_microsoft_calendar_settings WHERE tenant_id=@tenant",connection)){command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);version=reader.GetInt32(0);updated=reader.GetDateTime(1);}var mappings=new List<MicrosoftInspectorCalendarMapping>();await using(var command=new NpgsqlCommand("SELECT inspector_id,calendar_id,calendar_name,inspector_email,share_role,sharing_status,enabled,last_sync_at,last_error FROM public.tenant_microsoft_inspector_calendars WHERE tenant_id=@tenant ORDER BY calendar_name",connection)){command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))mappings.Add(new(reader.GetGuid(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5),reader.GetBoolean(6),reader.IsDBNull(7)?null:reader.GetDateTime(7),reader.GetString(8)));}return new(version,updated,mappings);
    }

    public static async Task<MicrosoftCalendarSettings> SaveAsync(NpgsqlConnection connection,MicrosoftCalendarSaveRequest request,string actor,CancellationToken ct=default)
    {
        if(!request.Confirmed)throw new MicrosoftCalendarException("confirmation_required","Confirm the company Microsoft calendar mappings.");if(request.Mappings.Where(x=>x.Enabled).Any(x=>string.IsNullOrWhiteSpace(x.CalendarId)))throw new MicrosoftCalendarException("calendar_required","Every enabled inspector mapping requires a calendar.");if(request.Mappings.Where(x=>x.Enabled).GroupBy(x=>x.CalendarId,StringComparer.OrdinalIgnoreCase).Any(x=>x.Count()>1))throw new MicrosoftCalendarException("duplicate_calendar","One Microsoft calendar cannot be assigned to multiple inspectors.");var current=await LoadAsync(connection,request.TenantId,ct);if(current.Version!=request.ExpectedVersion)throw new MicrosoftCalendarException("version_conflict","Microsoft calendar mappings changed elsewhere. Reload and review them.");await using var tx=await connection.BeginTransactionAsync(ct);await using(var update=new NpgsqlCommand("UPDATE public.tenant_microsoft_calendar_settings SET settings_version=settings_version+1,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant",connection,tx)){update.Parameters.AddWithValue("tenant",request.TenantId);update.Parameters.AddWithValue("actor",actor??"");await update.ExecuteNonQueryAsync(ct);}foreach(var item in request.Mappings){await using var command=new NpgsqlCommand("""INSERT INTO public.tenant_microsoft_inspector_calendars(tenant_id,inspector_id,calendar_id,calendar_name,inspector_email,share_role,sharing_status,enabled) VALUES(@tenant,@inspector,@calendar,@name,@email,@role,'pending',@enabled) ON CONFLICT(tenant_id,inspector_id) DO UPDATE SET calendar_id=EXCLUDED.calendar_id,calendar_name=EXCLUDED.calendar_name,inspector_email=EXCLUDED.inspector_email,share_role=EXCLUDED.share_role,sharing_status='pending',enabled=EXCLUDED.enabled,updated_at=NOW()""",connection,tx);command.Parameters.AddWithValue("tenant",request.TenantId);command.Parameters.AddWithValue("inspector",item.InspectorId);command.Parameters.AddWithValue("calendar",item.CalendarId??"");command.Parameters.AddWithValue("name",item.CalendarName??"");command.Parameters.AddWithValue("email",item.InspectorEmail??"");command.Parameters.AddWithValue("role",item.ShareRole=="write"?"write":"read");command.Parameters.AddWithValue("enabled",item.Enabled);await command.ExecuteNonQueryAsync(ct);}await using(var audit=new NpgsqlCommand("INSERT INTO public.tenant_microsoft_calendar_audit(tenant_id,actor,detail_json) VALUES(@tenant,@actor,CAST(@detail AS jsonb))",connection,tx)){audit.Parameters.AddWithValue("tenant",request.TenantId);audit.Parameters.AddWithValue("actor",actor??"");audit.Parameters.AddWithValue("detail",JsonSerializer.Serialize(new{mappings=request.Mappings.Select(x=>new{x.InspectorId,x.CalendarId,x.Enabled,x.ShareRole})}));await audit.ExecuteNonQueryAsync(ct);}await tx.CommitAsync(ct);return await LoadAsync(connection,request.TenantId,ct);
    }

    public static async Task RecordSharingAsync(NpgsqlConnection connection,Guid tenantId,Guid inspectorId,string status,string error,CancellationToken ct=default)
    {
        await using var command=new NpgsqlCommand("UPDATE public.tenant_microsoft_inspector_calendars SET sharing_status=@status,last_error=@error,last_sync_at=CASE WHEN @status IN ('shared','owner') THEN NOW() ELSE last_sync_at END,updated_at=NOW() WHERE tenant_id=@tenant AND inspector_id=@inspector",connection);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("inspector",inspectorId);command.Parameters.AddWithValue("status",status);command.Parameters.AddWithValue("error",error??"");await command.ExecuteNonQueryAsync(ct);
    }
}

public sealed record MicrosoftCalendarSettings(int Version,DateTime UpdatedAt,List<MicrosoftInspectorCalendarMapping> Mappings);
public sealed record MicrosoftInspectorCalendarMapping(Guid InspectorId,string CalendarId,string CalendarName,string InspectorEmail,string ShareRole,string SharingStatus,bool Enabled,DateTime? LastSyncAt,string LastError);
public sealed record MicrosoftInspectorCalendarSave(Guid InspectorId,string CalendarId,string CalendarName,string InspectorEmail,string ShareRole,bool Enabled);
public sealed record MicrosoftCalendarSaveRequest(Guid TenantId,int ExpectedVersion,List<MicrosoftInspectorCalendarSave> Mappings,bool Confirmed);
public sealed class MicrosoftCalendarException(string code,string message):Exception(message){public string Code{get;}=code;}
