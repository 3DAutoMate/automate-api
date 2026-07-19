using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class GoogleDriveArchiveSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.tenant_google_drive_archive_settings
(
 tenant_id uuid PRIMARY KEY,
 settings_version integer NOT NULL DEFAULT 1,
 root_folder_id text NOT NULL DEFAULT '',
 root_folder_name text NOT NULL DEFAULT 'AutoMate Reports',
 root_web_url text NOT NULL DEFAULT '',
 local_sync_root text NOT NULL DEFAULT '',
 connector_id text NOT NULL DEFAULT '',
 permission_tested boolean NOT NULL DEFAULT false,
 sync_mapping_tested boolean NOT NULL DEFAULT false,
 permission_tested_at timestamptz NULL,
 sync_mapping_tested_at timestamptz NULL,
 test_settings_version integer NULL,
 cleanup_confirmed boolean NOT NULL DEFAULT false,
 release_policy text NOT NULL DEFAULT 'terms_and_paid',
 status text NOT NULL DEFAULT 'not_configured',
 last_error text NOT NULL DEFAULT '',
 updated_by text NOT NULL DEFAULT '',
 updated_at timestamptz NOT NULL DEFAULT NOW(),
 CONSTRAINT ck_google_drive_release_policy CHECK(release_policy IN ('terms','paid','terms_and_paid','manual'))
);
CREATE TABLE IF NOT EXISTS public.google_drive_mapping_test_sessions
(
 test_id uuid PRIMARY KEY,
 tenant_id uuid NOT NULL,
 settings_version integer NOT NULL,
 marker_name text NOT NULL,
 marker_seen_at timestamptz NULL,
 cleanup_seen_at timestamptz NULL,
 created_by text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_google_drive_mapping_test_tenant
 ON public.google_drive_mapping_test_sessions(tenant_id,settings_version,created_at DESC);
CREATE TABLE IF NOT EXISTS public.google_drive_archive_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,
 action text NOT NULL,actor text NOT NULL DEFAULT '',detail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
""";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<GoogleDriveArchiveView> LoadAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using (var seed = new NpgsqlCommand("INSERT INTO public.tenant_google_drive_archive_settings(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING", connection))
        {
            seed.Parameters.AddWithValue("tenant", tenantId);
            await seed.ExecuteNonQueryAsync(ct);
        }
        await using var command = new NpgsqlCommand("""
SELECT settings_version,root_folder_id,root_folder_name,root_web_url,local_sync_root,connector_id,
 permission_tested,sync_mapping_tested,permission_tested_at,sync_mapping_tested_at,test_settings_version,
 cleanup_confirmed,release_policy,status,last_error,updated_at
FROM public.tenant_google_drive_archive_settings WHERE tenant_id=@tenant
""", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new(reader.GetInt32(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5),
            reader.GetBoolean(6), reader.GetBoolean(7), reader.IsDBNull(8) ? null : reader.GetDateTime(8), reader.IsDBNull(9) ? null : reader.GetDateTime(9),
            reader.IsDBNull(10) ? null : reader.GetInt32(10), reader.GetBoolean(11), reader.GetString(12), reader.GetString(13), reader.GetString(14), reader.GetDateTime(15));
    }

    public static async Task<GoogleDriveArchiveView> SaveAsync(NpgsqlConnection connection, Guid tenantId, GoogleDriveArchiveSaveRequest request, string actor, CancellationToken ct = default)
    {
        if (!request.Confirmed) throw new GoogleDriveArchiveException("confirmation_required", "Confirm the Google Drive report archive settings.");
        var current = await LoadAsync(connection, tenantId, ct);
        if (current.Version != request.ExpectedVersion) throw new GoogleDriveArchiveException("version_conflict", "Google Drive settings changed elsewhere. Reload and review them.");
        if (string.IsNullOrWhiteSpace(request.RootFolderId) || string.IsNullOrWhiteSpace(request.LocalSyncRoot))
            throw new GoogleDriveArchiveException("destination_required", "Connect Google Drive and choose the matching Google Drive for desktop folder.");
        var policy = (request.ReleasePolicy ?? "").Trim().ToLowerInvariant();
        if (!new[] { "terms", "paid", "terms_and_paid", "manual" }.Contains(policy))
            throw new GoogleDriveArchiveException("invalid_release_policy", "Choose a supported report release policy.");
        var local = request.LocalSyncRoot.Trim();
        var connector = (request.ConnectorId ?? "").Trim();
        var changed = current.RootFolderId != request.RootFolderId.Trim() || !string.Equals(current.LocalSyncRoot, local, StringComparison.OrdinalIgnoreCase) || !string.Equals(current.ConnectorId, connector, StringComparison.OrdinalIgnoreCase);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var update = new NpgsqlCommand("""
UPDATE public.tenant_google_drive_archive_settings SET settings_version=settings_version+1,
 root_folder_id=@root,root_folder_name=@name,root_web_url=@web,local_sync_root=@local,connector_id=@connector,
 release_policy=@policy,permission_tested=CASE WHEN @changed THEN false ELSE permission_tested END,
 sync_mapping_tested=CASE WHEN @changed THEN false ELSE sync_mapping_tested END,
 permission_tested_at=CASE WHEN @changed THEN NULL ELSE permission_tested_at END,
 sync_mapping_tested_at=CASE WHEN @changed THEN NULL ELSE sync_mapping_tested_at END,
 test_settings_version=CASE WHEN @changed THEN NULL ELSE test_settings_version END,
 cleanup_confirmed=CASE WHEN @changed THEN false ELSE cleanup_confirmed END,
 status=CASE WHEN @changed THEN 'destination_selected' ELSE status END,last_error='',updated_by=@actor,updated_at=NOW()
WHERE tenant_id=@tenant
""", connection, tx))
        {
            update.Parameters.AddWithValue("tenant", tenantId);
            update.Parameters.AddWithValue("root", request.RootFolderId.Trim());
            update.Parameters.AddWithValue("name", string.IsNullOrWhiteSpace(request.RootFolderName) ? "AutoMate Reports" : request.RootFolderName.Trim());
            update.Parameters.AddWithValue("web", request.RootWebUrl ?? "");
            update.Parameters.AddWithValue("local", local);
            update.Parameters.AddWithValue("connector", connector);
            update.Parameters.AddWithValue("policy", policy);
            update.Parameters.AddWithValue("changed", changed);
            update.Parameters.AddWithValue("actor", actor ?? "");
            await update.ExecuteNonQueryAsync(ct);
        }
        await AuditAsync(connection, tx, tenantId, "destination_saved", actor ?? "", new { request.RootFolderId, local, connector, policy, readinessInvalidated = changed }, ct);
        await tx.CommitAsync(ct);
        return await LoadAsync(connection, tenantId, ct);
    }

    public static async Task<GoogleDriveArchiveView> BindRootAsync(NpgsqlConnection connection, Guid tenantId, string folderId, string folderName, string webUrl, string actor, CancellationToken ct = default)
    {
        var current = await LoadAsync(connection, tenantId, ct);
        var changed = !string.Equals(current.RootFolderId, folderId, StringComparison.Ordinal);
        await using var command = new NpgsqlCommand("""
UPDATE public.tenant_google_drive_archive_settings SET root_folder_id=@root,root_folder_name=@name,root_web_url=@web,
 settings_version=CASE WHEN @changed THEN settings_version+1 ELSE settings_version END,
 permission_tested=CASE WHEN @changed THEN false ELSE permission_tested END,
 sync_mapping_tested=CASE WHEN @changed THEN false ELSE sync_mapping_tested END,
 permission_tested_at=CASE WHEN @changed THEN NULL ELSE permission_tested_at END,
 sync_mapping_tested_at=CASE WHEN @changed THEN NULL ELSE sync_mapping_tested_at END,
 test_settings_version=CASE WHEN @changed THEN NULL ELSE test_settings_version END,
 cleanup_confirmed=CASE WHEN @changed THEN false ELSE cleanup_confirmed END,
 status=CASE WHEN @changed THEN 'destination_selected' ELSE status END,last_error='',updated_by=@actor,updated_at=NOW()
WHERE tenant_id=@tenant
""", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("root", folderId);
        command.Parameters.AddWithValue("name", folderName);
        command.Parameters.AddWithValue("web", webUrl);
        command.Parameters.AddWithValue("changed", changed);
        command.Parameters.AddWithValue("actor", actor ?? "");
        await command.ExecuteNonQueryAsync(ct);
        return await LoadAsync(connection, tenantId, ct);
    }

    public static async Task<GoogleDriveArchiveView> RecordTestAsync(NpgsqlConnection connection, Guid tenantId, int expectedVersion, string test, bool passed, bool cleanupConfirmed, string actor, string error, CancellationToken ct = default)
    {
        var current = await LoadAsync(connection, tenantId, ct);
        if (current.Version != expectedVersion) throw new GoogleDriveArchiveException("version_conflict", "The Google Drive destination changed. Reload it before testing.");
        if (test is not ("permission" or "mapping")) throw new GoogleDriveArchiveException("invalid_test", "Choose a supported Google Drive readiness test.");
        var permission = test == "permission" ? passed : current.PermissionTested;
        var mapping = test == "mapping" ? passed : current.SyncMappingTested;
        var status = permission && mapping ? "healthy" : passed ? "verification_required" : "test_failed";
        var sql = test == "permission"
            ? "UPDATE public.tenant_google_drive_archive_settings SET permission_tested=@passed,permission_tested_at=CASE WHEN @passed THEN NOW() ELSE NULL END,test_settings_version=@version,status=@status,last_error=@error,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant"
            : "UPDATE public.tenant_google_drive_archive_settings SET sync_mapping_tested=@passed,sync_mapping_tested_at=CASE WHEN @passed THEN NOW() ELSE NULL END,cleanup_confirmed=@cleanup,test_settings_version=@version,status=@status,last_error=@error,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant";
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var update = new NpgsqlCommand(sql, connection, tx))
        {
            update.Parameters.AddWithValue("tenant", tenantId);
            update.Parameters.AddWithValue("passed", passed);
            update.Parameters.AddWithValue("cleanup", cleanupConfirmed);
            update.Parameters.AddWithValue("version", expectedVersion);
            update.Parameters.AddWithValue("status", status);
            update.Parameters.AddWithValue("error", error ?? "");
            update.Parameters.AddWithValue("actor", actor ?? "");
            await update.ExecuteNonQueryAsync(ct);
        }
        await AuditAsync(connection, tx, tenantId, test + "_test", actor ?? "", new { passed, cleanupConfirmed, error, expectedVersion }, ct);
        await tx.CommitAsync(ct);
        return await LoadAsync(connection, tenantId, ct);
    }

    public static async Task<GoogleDriveArchiveView> BindConnectorIfMissingAsync(NpgsqlConnection connection, Guid tenantId, int expectedVersion, string connectorId, string actor, CancellationToken ct = default)
    {
        var current = await LoadAsync(connection, tenantId, ct);
        if (current.Version != expectedVersion) throw new GoogleDriveArchiveException("version_conflict", "The Google Drive destination changed. Reload it before testing.");
        if (string.IsNullOrWhiteSpace(connectorId)) throw new GoogleDriveArchiveException("connector_required", "The Windows connector identity is required.");
        if (!string.IsNullOrWhiteSpace(current.ConnectorId) && !string.Equals(current.ConnectorId, connectorId, StringComparison.OrdinalIgnoreCase))
            throw new GoogleDriveArchiveException("connector_mismatch", "Run this test from the Windows connector that owns the saved Google Drive folder.");
        if (!string.IsNullOrWhiteSpace(current.ConnectorId)) return current;
        await using var command = new NpgsqlCommand("UPDATE public.tenant_google_drive_archive_settings SET connector_id=@connector,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant AND settings_version=@version AND connector_id=''", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("version", expectedVersion);
        command.Parameters.AddWithValue("connector", connectorId.Trim());
        command.Parameters.AddWithValue("actor", actor ?? "");
        if (await command.ExecuteNonQueryAsync(ct) != 1) throw new GoogleDriveArchiveException("connector_claim_conflict", "The connector identity changed. Reload before testing.");
        return await LoadAsync(connection, tenantId, ct);
    }

    public static async Task RecordMarkerSeenAsync(NpgsqlConnection connection, Guid tenantId, int expectedVersion, Guid testId, string markerName, string actor, CancellationToken ct = default)
    {
        var current = await LoadAsync(connection, tenantId, ct);
        if (current.Version != expectedVersion) throw new GoogleDriveArchiveException("version_conflict", "The Google Drive destination changed. Reload it before testing.");
        await using var command = new NpgsqlCommand("""
INSERT INTO public.google_drive_mapping_test_sessions(test_id,tenant_id,settings_version,marker_name,marker_seen_at,created_by)
VALUES(@test,@tenant,@version,@marker,NOW(),@actor)
ON CONFLICT(test_id) DO UPDATE SET marker_seen_at=NOW()
WHERE google_drive_mapping_test_sessions.tenant_id=@tenant AND google_drive_mapping_test_sessions.settings_version=@version AND google_drive_mapping_test_sessions.marker_name=@marker
""", connection);
        command.Parameters.AddWithValue("test", testId);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("version", expectedVersion);
        command.Parameters.AddWithValue("marker", markerName);
        command.Parameters.AddWithValue("actor", actor ?? "");
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<GoogleDriveArchiveView> CompleteMappingAsync(NpgsqlConnection connection, Guid tenantId, int expectedVersion, Guid testId, string markerName, bool cleanupConfirmed, string actor, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var check = new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.google_drive_mapping_test_sessions WHERE test_id=@test AND tenant_id=@tenant AND settings_version=@version AND marker_name=@marker AND marker_seen_at IS NOT NULL)", connection);
        check.Parameters.AddWithValue("test", testId);
        check.Parameters.AddWithValue("tenant", tenantId);
        check.Parameters.AddWithValue("version", expectedVersion);
        check.Parameters.AddWithValue("marker", markerName);
        if (!Convert.ToBoolean(await check.ExecuteScalarAsync(ct))) throw new GoogleDriveArchiveException("mapping_marker_not_verified", "The temporary folder has not yet been verified in the configured Google Drive root.");
        if (cleanupConfirmed)
        {
            await using var cleanup = new NpgsqlCommand("UPDATE public.google_drive_mapping_test_sessions SET cleanup_seen_at=NOW() WHERE test_id=@test AND tenant_id=@tenant", connection);
            cleanup.Parameters.AddWithValue("test", testId);
            cleanup.Parameters.AddWithValue("tenant", tenantId);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
        return await RecordTestAsync(connection, tenantId, expectedVersion, "mapping", true, cleanupConfirmed, actor, "", ct);
    }

    private static async Task AuditAsync(NpgsqlConnection connection, NpgsqlTransaction tx, Guid tenantId, string action, string actor, object detail, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("INSERT INTO public.google_drive_archive_audit(tenant_id,action,actor,detail_json) VALUES(@tenant,@action,@actor,CAST(@detail AS jsonb))", connection, tx);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("action", action);
        command.Parameters.AddWithValue("actor", actor ?? "");
        command.Parameters.AddWithValue("detail", JsonSerializer.Serialize(detail));
        await command.ExecuteNonQueryAsync(ct);
    }
}

public sealed class GoogleDriveArchiveSaveRequest
{
    public Guid TenantId { get; set; }
    public int ExpectedVersion { get; set; }
    public string RootFolderId { get; set; } = "";
    public string RootFolderName { get; set; } = "AutoMate Reports";
    public string RootWebUrl { get; set; } = "";
    public string LocalSyncRoot { get; set; } = "";
    public string ConnectorId { get; set; } = "";
    public string ReleasePolicy { get; set; } = "terms_and_paid";
    public bool Confirmed { get; set; }
}

public sealed record GoogleDriveArchiveView(int Version, string RootFolderId, string RootFolderName, string RootWebUrl, string LocalSyncRoot, string ConnectorId, bool PermissionTested, bool SyncMappingTested, DateTime? PermissionTestedAt, DateTime? SyncMappingTestedAt, int? TestSettingsVersion, bool CleanupConfirmed, string ReleasePolicy, string Status, string LastError, DateTime UpdatedAt);
public sealed class GoogleDriveArchiveException(string code, string message) : Exception(message) { public string Code { get; } = code; }
