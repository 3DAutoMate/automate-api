using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class SharePointArchiveSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        const string sql="""
CREATE TABLE IF NOT EXISTS public.tenant_sharepoint_archive_settings
(
 tenant_id uuid PRIMARY KEY,settings_version integer NOT NULL DEFAULT 1,
 site_id text NOT NULL DEFAULT '',site_name text NOT NULL DEFAULT '',drive_id text NOT NULL DEFAULT '',
 library_name text NOT NULL DEFAULT '',root_item_id text NOT NULL DEFAULT '',root_name text NOT NULL DEFAULT '',
 local_sync_root text NOT NULL DEFAULT '',permission_tested boolean NOT NULL DEFAULT false,
 sync_mapping_tested boolean NOT NULL DEFAULT false,release_policy text NOT NULL DEFAULT 'terms_and_paid',
 status text NOT NULL DEFAULT 'not_configured',last_error text NOT NULL DEFAULT '',
 updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW(),
 CONSTRAINT ck_archive_release_policy CHECK(release_policy IN ('terms','paid','terms_and_paid','manual'))
);
ALTER TABLE public.tenant_sharepoint_archive_settings ADD COLUMN IF NOT EXISTS connector_id text NOT NULL DEFAULT '';
ALTER TABLE public.tenant_sharepoint_archive_settings ADD COLUMN IF NOT EXISTS permission_tested_at timestamptz NULL;
ALTER TABLE public.tenant_sharepoint_archive_settings ADD COLUMN IF NOT EXISTS sync_mapping_tested_at timestamptz NULL;
ALTER TABLE public.tenant_sharepoint_archive_settings ADD COLUMN IF NOT EXISTS test_settings_version integer NULL;
ALTER TABLE public.tenant_sharepoint_archive_settings ADD COLUMN IF NOT EXISTS cleanup_confirmed boolean NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS public.job_report_archive_items
(
 report_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,job_id uuid NOT NULL,
 revision integer NOT NULL DEFAULT 1,local_path text NOT NULL DEFAULT '',file_name text NOT NULL,
 content_hash text NOT NULL,size_bytes bigint NOT NULL DEFAULT 0,local_modified_at timestamptz NULL,
 site_id text NOT NULL DEFAULT '',drive_id text NOT NULL DEFAULT '',folder_item_id text NOT NULL DEFAULT '',
 file_item_id text NOT NULL DEFAULT '',provider_etag text NOT NULL DEFAULT '',provider_web_url text NOT NULL DEFAULT '',
 state text NOT NULL DEFAULT 'local_detected',release_gate_json jsonb NOT NULL DEFAULT '{}'::jsonb,
 share_permission_id text NOT NULL DEFAULT '',share_web_url text NOT NULL DEFAULT '',recipient_email text NOT NULL DEFAULT '',
 detected_at timestamptz NOT NULL DEFAULT NOW(),cloud_verified_at timestamptz NULL,published_at timestamptz NULL,
 last_error text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW(),
 UNIQUE(tenant_id,job_id,content_hash),UNIQUE(tenant_id,job_id,revision)
);
ALTER TABLE public.job_report_archive_items ADD COLUMN IF NOT EXISTS storage_provider text NOT NULL DEFAULT 'microsoft_documents';
CREATE INDEX IF NOT EXISTS ix_job_report_archive_attention ON public.job_report_archive_items(tenant_id,state,updated_at);
CREATE TABLE IF NOT EXISTS public.sharepoint_archive_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,job_id uuid NULL,
 action text NOT NULL,actor text NOT NULL,detail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.sharepoint_mapping_test_sessions
(
 test_id uuid PRIMARY KEY,tenant_id uuid NOT NULL,settings_version integer NOT NULL,
 marker_name text NOT NULL,marker_seen_at timestamptz NULL,cleanup_seen_at timestamptz NULL,
 created_by text NOT NULL DEFAULT '',created_at timestamptz NOT NULL DEFAULT NOW(),
 UNIQUE(tenant_id,settings_version,marker_name)
);
CREATE INDEX IF NOT EXISTS ix_sharepoint_mapping_test_tenant
 ON public.sharepoint_mapping_test_sessions(tenant_id,settings_version,created_at DESC);
""";
        await using var command=new NpgsqlCommand(sql,connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<SharePointArchiveView> LoadAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);
        await using(var seed=new NpgsqlCommand("INSERT INTO public.tenant_sharepoint_archive_settings(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection)){seed.Parameters.AddWithValue("tenant",tenantId);await seed.ExecuteNonQueryAsync(ct);}
        await using var command=new NpgsqlCommand("SELECT settings_version,site_id,site_name,drive_id,library_name,root_item_id,root_name,local_sync_root,permission_tested,sync_mapping_tested,release_policy,status,last_error,updated_at,connector_id,permission_tested_at,sync_mapping_tested_at,test_settings_version,cleanup_confirmed FROM public.tenant_sharepoint_archive_settings WHERE tenant_id=@tenant",connection);
        command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);
        return new(reader.GetInt32(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5),reader.GetString(6),reader.GetString(7),reader.GetBoolean(8),reader.GetBoolean(9),reader.GetString(10),reader.GetString(11),reader.GetString(12),reader.GetDateTime(13),reader.GetString(14),reader.IsDBNull(15)?null:reader.GetDateTime(15),reader.IsDBNull(16)?null:reader.GetDateTime(16),reader.IsDBNull(17)?null:reader.GetInt32(17),reader.GetBoolean(18));
    }

    public static async Task<SharePointArchiveView> SaveAsync(NpgsqlConnection connection,Guid tenantId,SharePointArchiveSaveRequest request,string actor,CancellationToken ct=default)
    {
        if(!request.Confirmed)throw new SharePointArchiveException("confirmation_required","Confirm the SharePoint report archive destination.");
        var current=await LoadAsync(connection,tenantId,ct);if(current.Version!=request.ExpectedVersion)throw new SharePointArchiveException("version_conflict","SharePoint archive settings changed elsewhere. Reload and review them.");
        var policy=(request.ReleasePolicy??"").Trim().ToLowerInvariant();if(!new[]{"terms","paid","terms_and_paid","manual"}.Contains(policy))throw new SharePointArchiveException("invalid_release_policy","Choose a supported report release policy.");
        foreach(var value in new[]{request.SiteId,request.DriveId,request.RootItemId,request.LocalSyncRoot})if(string.IsNullOrWhiteSpace(value))throw new SharePointArchiveException("destination_required","Select a granted SharePoint site, library, root folder and matching local sync folder.");
        var changed=current.SiteId!=request.SiteId.Trim()||current.DriveId!=request.DriveId.Trim()||current.RootItemId!=request.RootItemId.Trim()||current.LocalSyncRoot!=request.LocalSyncRoot.Trim()||current.ConnectorId!=(request.ConnectorId??"").Trim();
        await using var tx=await connection.BeginTransactionAsync(ct);var next=current.Version+1;
        await using(var update=new NpgsqlCommand("""UPDATE public.tenant_sharepoint_archive_settings SET settings_version=@version,site_id=@site,site_name=@site_name,drive_id=@drive,library_name=@library,root_item_id=@root,root_name=@root_name,local_sync_root=@local,connector_id=@connector,release_policy=@policy,permission_tested=CASE WHEN @changed THEN false ELSE permission_tested END,sync_mapping_tested=CASE WHEN @changed THEN false ELSE sync_mapping_tested END,permission_tested_at=CASE WHEN @changed THEN NULL ELSE permission_tested_at END,sync_mapping_tested_at=CASE WHEN @changed THEN NULL ELSE sync_mapping_tested_at END,test_settings_version=CASE WHEN @changed THEN NULL ELSE test_settings_version END,cleanup_confirmed=CASE WHEN @changed THEN false ELSE cleanup_confirmed END,status=CASE WHEN @changed THEN 'destination_selected' ELSE status END,last_error='',updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant""",connection,tx))
        {update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("version",next);update.Parameters.AddWithValue("site",request.SiteId.Trim());update.Parameters.AddWithValue("site_name",(request.SiteName??"").Trim());update.Parameters.AddWithValue("drive",request.DriveId.Trim());update.Parameters.AddWithValue("library",(request.LibraryName??"").Trim());update.Parameters.AddWithValue("root",request.RootItemId.Trim());update.Parameters.AddWithValue("root_name",(request.RootName??"").Trim());update.Parameters.AddWithValue("local",request.LocalSyncRoot.Trim());update.Parameters.AddWithValue("connector",(request.ConnectorId??"").Trim());update.Parameters.AddWithValue("policy",policy);update.Parameters.AddWithValue("changed",changed);update.Parameters.AddWithValue("actor",actor);await update.ExecuteNonQueryAsync(ct);}
        await AuditAsync(connection,tx,tenantId,null,"destination_saved",actor,new{request.SiteId,request.DriveId,request.RootItemId,request.LocalSyncRoot,policy,readinessInvalidated=changed},ct);await tx.CommitAsync(ct);return await LoadAsync(connection,tenantId,ct);
    }

    public static async Task<SharePointArchiveView> RecordTestAsync(NpgsqlConnection connection,Guid tenantId,int expectedVersion,string test,bool passed,bool cleanupConfirmed,string actor,string error,CancellationToken ct=default)
    {
        var current=await LoadAsync(connection,tenantId,ct);if(current.Version!=expectedVersion)throw new SharePointArchiveException("version_conflict","The destination changed. Reload it before testing.");
        if(test is not ("permission" or "mapping"))throw new SharePointArchiveException("invalid_test","Choose a supported SharePoint readiness test.");
        await using var tx=await connection.BeginTransactionAsync(ct);
        var permission=test=="permission"?passed:current.PermissionTested;var mapping=test=="mapping"?passed:current.SyncMappingTested;
        var healthy=permission&&mapping;
        var sql=test=="permission"?"UPDATE public.tenant_sharepoint_archive_settings SET permission_tested=@passed,permission_tested_at=CASE WHEN @passed THEN NOW() ELSE NULL END,test_settings_version=@version,status=@status,last_error=@error,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant":"UPDATE public.tenant_sharepoint_archive_settings SET sync_mapping_tested=@passed,sync_mapping_tested_at=CASE WHEN @passed THEN NOW() ELSE NULL END,cleanup_confirmed=@cleanup,test_settings_version=@version,status=@status,last_error=@error,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant";
        await using(var update=new NpgsqlCommand(sql,connection,tx)){update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("passed",test=="mapping"?mapping:passed);update.Parameters.AddWithValue("cleanup",cleanupConfirmed);update.Parameters.AddWithValue("version",current.Version);update.Parameters.AddWithValue("status",healthy?"healthy":passed?"verification_required":"test_failed");update.Parameters.AddWithValue("error",error??"");update.Parameters.AddWithValue("actor",actor);await update.ExecuteNonQueryAsync(ct);}
        await AuditAsync(connection,tx,tenantId,null,test+"_test",actor,new{settingsVersion=current.Version,passed,cleanupConfirmed,error},ct);await tx.CommitAsync(ct);return await LoadAsync(connection,tenantId,ct);
    }

    public static async Task RecordMappingMarkerSeenAsync(NpgsqlConnection connection,Guid tenantId,int expectedVersion,Guid testId,string markerName,string actor,CancellationToken ct=default)
    {
        var current=await LoadAsync(connection,tenantId,ct);if(current.Version!=expectedVersion)throw new SharePointArchiveException("version_conflict","The destination changed. Reload it before testing.");
        await using var tx=await connection.BeginTransactionAsync(ct);
        await using(var command=new NpgsqlCommand("""INSERT INTO public.sharepoint_mapping_test_sessions(test_id,tenant_id,settings_version,marker_name,marker_seen_at,created_by) VALUES(@test,@tenant,@version,@marker,NOW(),@actor) ON CONFLICT(test_id) DO UPDATE SET marker_seen_at=NOW() WHERE sharepoint_mapping_test_sessions.tenant_id=@tenant AND sharepoint_mapping_test_sessions.settings_version=@version AND sharepoint_mapping_test_sessions.marker_name=@marker""",connection,tx))
        {command.Parameters.AddWithValue("test",testId);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("version",expectedVersion);command.Parameters.AddWithValue("marker",markerName);command.Parameters.AddWithValue("actor",actor);await command.ExecuteNonQueryAsync(ct);}
        await AuditAsync(connection,tx,tenantId,null,"mapping_marker_seen",actor,new{testId,settingsVersion=expectedVersion,markerName},ct);await tx.CommitAsync(ct);
    }

    public static async Task<SharePointArchiveView> BindConnectorIfMissingAsync(NpgsqlConnection connection,Guid tenantId,int expectedVersion,string connectorId,string actor,CancellationToken ct=default)
    {
        var current=await LoadAsync(connection,tenantId,ct);if(current.Version!=expectedVersion)throw new SharePointArchiveException("version_conflict","The destination changed. Reload it before testing.");if(string.IsNullOrWhiteSpace(connectorId))throw new SharePointArchiveException("connector_required","The Windows connector identity is required.");if(!string.IsNullOrWhiteSpace(current.ConnectorId)&&!string.Equals(current.ConnectorId,connectorId,StringComparison.OrdinalIgnoreCase))throw new SharePointArchiveException("connector_mismatch","Run this test from the Windows connector that owns the saved local sync folder.");if(!string.IsNullOrWhiteSpace(current.ConnectorId))return current;
        await using var tx=await connection.BeginTransactionAsync(ct);await using(var update=new NpgsqlCommand("UPDATE public.tenant_sharepoint_archive_settings SET connector_id=@connector,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant AND settings_version=@version AND connector_id=''",connection,tx)){update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("version",expectedVersion);update.Parameters.AddWithValue("connector",connectorId.Trim());update.Parameters.AddWithValue("actor",actor);if(await update.ExecuteNonQueryAsync(ct)!=1)throw new SharePointArchiveException("connector_claim_conflict","The connector identity changed. Reload before testing.");}await AuditAsync(connection,tx,tenantId,null,"connector_claimed_for_mapping_test",actor,new{settingsVersion=expectedVersion,connectorId=connectorId.Trim()},ct);await tx.CommitAsync(ct);return await LoadAsync(connection,tenantId,ct);
    }

    public static async Task<bool> HasMappingMarkerBeenSeenAsync(NpgsqlConnection connection,Guid tenantId,int expectedVersion,Guid testId,string markerName,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using var command=new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.sharepoint_mapping_test_sessions WHERE test_id=@test AND tenant_id=@tenant AND settings_version=@version AND marker_name=@marker AND marker_seen_at IS NOT NULL)",connection);command.Parameters.AddWithValue("test",testId);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("version",expectedVersion);command.Parameters.AddWithValue("marker",markerName);return Convert.ToBoolean(await command.ExecuteScalarAsync(ct));
    }

    public static async Task<SharePointArchiveView> CompleteMappingTestAsync(NpgsqlConnection connection,Guid tenantId,int expectedVersion,Guid testId,string markerName,string actor,CancellationToken ct=default)
    {
        if(!await HasMappingMarkerBeenSeenAsync(connection,tenantId,expectedVersion,testId,markerName,ct))throw new SharePointArchiveException("mapping_marker_not_verified","The temporary folder has not yet been verified in the configured SharePoint root.");
        await using(var command=new NpgsqlCommand("UPDATE public.sharepoint_mapping_test_sessions SET cleanup_seen_at=NOW() WHERE test_id=@test AND tenant_id=@tenant",connection)){command.Parameters.AddWithValue("test",testId);command.Parameters.AddWithValue("tenant",tenantId);await command.ExecuteNonQueryAsync(ct);}
        return await RecordTestAsync(connection,tenantId,expectedVersion,"mapping",true,true,actor,"",ct);
    }

    public static async Task<SharePointArchiveView> CompleteMappingTestWithManualCleanupAsync(NpgsqlConnection connection,Guid tenantId,int expectedVersion,Guid testId,string markerName,string actor,CancellationToken ct=default)
    {
        if(!await HasMappingMarkerBeenSeenAsync(connection,tenantId,expectedVersion,testId,markerName,ct))throw new SharePointArchiveException("mapping_marker_not_verified","The temporary folder has not yet been verified in the configured SharePoint root.");
        return await RecordTestAsync(connection,tenantId,expectedVersion,"mapping",true,false,actor,"",ct);
    }

    public static async Task<ReportArchiveItemView> RegisterAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,ReportDetectedRequest request,string actor,CancellationToken ct=default)
    {
        if(string.IsNullOrWhiteSpace(request.FileName)||string.IsNullOrWhiteSpace(request.ContentHash)||request.SizeBytes<=0)throw new SharePointArchiveException("invalid_report","A stable PDF filename, SHA-256 hash and size are required.");
        if(!request.FileName.EndsWith(".pdf",StringComparison.OrdinalIgnoreCase))throw new SharePointArchiveException("pdf_required","Only completed PDF reports can be registered.");
        await EnsureAsync(connection,ct);await using var tx=await connection.BeginTransactionAsync(ct);
        int revision;await using(var count=new NpgsqlCommand("SELECT COALESCE(MAX(revision),0)+1 FROM public.job_report_archive_items WHERE tenant_id=@tenant AND job_id=@job",connection,tx)){count.Parameters.AddWithValue("tenant",tenantId);count.Parameters.AddWithValue("job",jobId);revision=Convert.ToInt32(await count.ExecuteScalarAsync(ct));}
        var provider=request.ProviderKey=="google_drive"?"google_drive":"microsoft_documents";var id=Guid.NewGuid();await using(var insert=new NpgsqlCommand("""INSERT INTO public.job_report_archive_items(report_id,tenant_id,job_id,revision,local_path,file_name,content_hash,size_bytes,local_modified_at,state,storage_provider) VALUES(@id,@tenant,@job,@revision,@path,@name,@hash,@size,@modified,'waiting_for_sync',@provider) ON CONFLICT(tenant_id,job_id,content_hash) DO UPDATE SET local_path=EXCLUDED.local_path,file_name=EXCLUDED.file_name,size_bytes=EXCLUDED.size_bytes,local_modified_at=EXCLUDED.local_modified_at,storage_provider=EXCLUDED.storage_provider,updated_at=NOW() RETURNING report_id,revision""",connection,tx)){insert.Parameters.AddWithValue("id",id);insert.Parameters.AddWithValue("tenant",tenantId);insert.Parameters.AddWithValue("job",jobId);insert.Parameters.AddWithValue("revision",revision);insert.Parameters.AddWithValue("path",request.LocalPath??"");insert.Parameters.AddWithValue("name",request.FileName.Trim());insert.Parameters.AddWithValue("hash",request.ContentHash.Trim().ToLowerInvariant());insert.Parameters.AddWithValue("size",request.SizeBytes);insert.Parameters.AddWithValue("modified",(object?)request.ModifiedAt??DBNull.Value);insert.Parameters.AddWithValue("provider",provider);await using var reader=await insert.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);id=reader.GetGuid(0);revision=reader.GetInt32(1);}
        await AuditAsync(connection,tx,tenantId,jobId,"report_detected",actor,new{reportId=id,revision,provider,request.FileName,request.ContentHash,request.SizeBytes},ct);await tx.CommitAsync(ct);return new(id,jobId,revision,request.FileName,request.ContentHash,request.SizeBytes,"waiting_for_sync","",DateTime.UtcNow,null,null,"","");
    }

    public static async Task<IReadOnlyList<ReportArchiveItemView>> LoadReportsAsync(NpgsqlConnection connection,Guid tenantId,Guid? jobId,CancellationToken ct=default)
    {
        var rows=new List<ReportArchiveItemView>();var sql=jobId.HasValue?"SELECT report_id,job_id,revision,file_name,content_hash,size_bytes,state,last_error,detected_at,cloud_verified_at,published_at,file_item_id,share_web_url FROM public.job_report_archive_items WHERE tenant_id=@tenant AND job_id=@job ORDER BY detected_at DESC":"SELECT report_id,job_id,revision,file_name,content_hash,size_bytes,state,last_error,detected_at,cloud_verified_at,published_at,file_item_id,share_web_url FROM public.job_report_archive_items WHERE tenant_id=@tenant ORDER BY detected_at DESC";await using var command=new NpgsqlCommand(sql,connection);command.Parameters.AddWithValue("tenant",tenantId);if(jobId.HasValue)command.Parameters.AddWithValue("job",jobId.Value);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))rows.Add(new(reader.GetGuid(0),reader.GetGuid(1),reader.GetInt32(2),reader.GetString(3),reader.GetString(4),reader.GetInt64(5),reader.GetString(6),reader.GetString(7),reader.GetDateTime(8),reader.IsDBNull(9)?null:reader.GetDateTime(9),reader.IsDBNull(10)?null:reader.GetDateTime(10),reader.GetString(11),reader.GetString(12)));return rows;
    }

    private static async Task AuditAsync(NpgsqlConnection connection,NpgsqlTransaction tx,Guid tenantId,Guid? jobId,string action,string actor,object detail,CancellationToken ct){await using var command=new NpgsqlCommand("INSERT INTO public.sharepoint_archive_audit(tenant_id,job_id,action,actor,detail_json) VALUES(@tenant,@job,@action,@actor,CAST(@detail AS jsonb))",connection,tx);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("job",(object?)jobId??DBNull.Value);command.Parameters.AddWithValue("action",action);command.Parameters.AddWithValue("actor",actor);command.Parameters.AddWithValue("detail",JsonSerializer.Serialize(detail));await command.ExecuteNonQueryAsync(ct);}
}

public sealed class SharePointArchiveSaveRequest{public Guid TenantId{get;set;}public int ExpectedVersion{get;set;}public string SiteId{get;set;}="";public string SiteName{get;set;}="";public string DriveId{get;set;}="";public string LibraryName{get;set;}="";public string RootItemId{get;set;}="";public string RootName{get;set;}="";public string LocalSyncRoot{get;set;}="";public string ConnectorId{get;set;}="";public string ReleasePolicy{get;set;}="terms_and_paid";public bool Confirmed{get;set;}}
public sealed class ReportDetectedRequest{public Guid TenantId{get;set;}public string ProviderKey{get;set;}="microsoft_documents";public string LocalPath{get;set;}="";public string FileName{get;set;}="";public string ContentHash{get;set;}="";public long SizeBytes{get;set;}public DateTime? ModifiedAt{get;set;}}
public sealed record SharePointArchiveView(int Version,string SiteId,string SiteName,string DriveId,string LibraryName,string RootItemId,string RootName,string LocalSyncRoot,bool PermissionTested,bool SyncMappingTested,string ReleasePolicy,string Status,string LastError,DateTime UpdatedAt,string ConnectorId,DateTime? PermissionTestedAt,DateTime? SyncMappingTestedAt,int? TestSettingsVersion,bool CleanupConfirmed);
public sealed class SharePointArchiveTestRequest{public Guid TenantId{get;set;}public int ExpectedVersion{get;set;}public string MarkerName{get;set;}="";public bool CleanupConfirmed{get;set;}public bool Confirmed{get;set;}}
public sealed class SharePointMappingObservationRequest{public Guid TenantId{get;set;}public int ExpectedVersion{get;set;}public Guid TestId{get;set;}public string MarkerName{get;set;}="";public string ConnectorId{get;set;}="";public bool ExpectedPresent{get;set;}public bool Confirmed{get;set;}}
public sealed record ReportArchiveItemView(Guid ReportId,Guid JobId,int Revision,string FileName,string ContentHash,long SizeBytes,string State,string LastError,DateTime DetectedAt,DateTime? CloudVerifiedAt,DateTime? PublishedAt,string FileItemId,string ShareWebUrl);
public sealed class SharePointArchiveException(string code,string message):Exception(message){public string Code{get;}=code;}
