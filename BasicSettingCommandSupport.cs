using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class BasicSettingCommandSupport
{
    public const string CommandType = "automations.basicSetting.save";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        await BasicTemplateCommandSupport.EnsureAsync(connection, cancellationToken);
        await using var command = new NpgsqlCommand("ALTER TABLE public.basic_automation_settings ADD COLUMN IF NOT EXISTS setting_version integer NOT NULL DEFAULT 1;", connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<BasicSettingSaveResult> SaveAsync(NpgsqlConnection connection, BasicSettingSaveCommand request, CancellationToken cancellationToken = default)
    {
        if (request.TenantId == Guid.Empty || request.InspectorId == Guid.Empty) throw new ArgumentException("Tenant and inspector IDs are required.");
        if (!BasicAutomationSupport.IsValidEvent(request.EventKey) || !BasicAutomationSupport.IsValidRecipient(request.RecipientKey)) throw new ArgumentException("Unsupported Basic automation slot.");
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey) || request.IdempotencyKey.Length > 200) throw new ArgumentException("A valid idempotency key is required.");
        await EnsureAsync(connection, cancellationToken);
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { request.TenantId, request.EventKey, request.RecipientKey, request.Enabled, request.ExpectedVersion })))).ToLowerInvariant();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string claimSql = "INSERT INTO public.automation_command_idempotency(tenant_id,command_type,idempotency_key,request_hash) VALUES(@tenant,@type,@key,@hash) ON CONFLICT DO NOTHING";
        await using (var claim = new NpgsqlCommand(claimSql, connection, transaction))
        {
            claim.Parameters.AddWithValue("tenant", request.TenantId); claim.Parameters.AddWithValue("type", CommandType); claim.Parameters.AddWithValue("key", request.IdempotencyKey); claim.Parameters.AddWithValue("hash", hash);
            if (await claim.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                await using var existing = new NpgsqlCommand("SELECT request_hash,result_status,COALESCE(result_json::text,'') FROM public.automation_command_idempotency WHERE tenant_id=@tenant AND command_type=@type AND idempotency_key=@key FOR UPDATE", connection, transaction);
                existing.Parameters.AddWithValue("tenant", request.TenantId); existing.Parameters.AddWithValue("type", CommandType); existing.Parameters.AddWithValue("key", request.IdempotencyKey);
                await using var reader = await existing.ExecuteReaderAsync(cancellationToken); await reader.ReadAsync(cancellationToken);
                if (reader.GetString(0) != hash) { await transaction.CommitAsync(cancellationToken); return new("idempotency_conflict", false, 0, false, null, "The idempotency key was already used for different content."); }
                var json = reader.GetString(2); if (!string.IsNullOrWhiteSpace(json)) { var replay = JsonSerializer.Deserialize<BasicSettingSaveResult>(json)! with { Replayed = true }; await transaction.CommitAsync(cancellationToken); return replay; }
                throw new InvalidOperationException("The same setting command is already being processed.");
            }
        }
        const string selectSql = "SELECT enabled,setting_version,template_id FROM public.basic_automation_settings WHERE tenant_id=@tenant AND event_key=@event AND recipient_key=@recipient FOR UPDATE";
        bool current; int version; Guid? template;
        await using (var select = new NpgsqlCommand(selectSql, connection, transaction))
        {
            select.Parameters.AddWithValue("tenant", request.TenantId); select.Parameters.AddWithValue("event", request.EventKey); select.Parameters.AddWithValue("recipient", request.RecipientKey);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken); if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Basic automation slot was not found.");
            current = reader.GetBoolean(0); version = reader.GetInt32(1); template = reader.IsDBNull(2) ? null : reader.GetGuid(2);
        }
        BasicSettingSaveResult result;
        if (version != request.ExpectedVersion) result = new("conflict", current, version, false, null, "The setting was changed by another user.");
        else if (request.Enabled && !template.HasValue) result = new("template_required", current, version, false, null, "Save a template before enabling this email.");
        else if (current == request.Enabled) result = new("unchanged", current, version, false, null, "Setting is already current.");
        else
        {
            version++;
            await using var update = new NpgsqlCommand("UPDATE public.basic_automation_settings SET enabled=@enabled,setting_version=@version,updated_at=NOW() WHERE tenant_id=@tenant AND event_key=@event AND recipient_key=@recipient", connection, transaction);
            update.Parameters.AddWithValue("enabled", request.Enabled); update.Parameters.AddWithValue("version", version); update.Parameters.AddWithValue("tenant", request.TenantId); update.Parameters.AddWithValue("event", request.EventKey); update.Parameters.AddWithValue("recipient", request.RecipientKey); await update.ExecuteNonQueryAsync(cancellationToken);
            var auditId = Guid.NewGuid();
            await using var audit = new NpgsqlCommand("INSERT INTO public.automation_foundation_audit(audit_id,tenant_id,action_key,previous_value,new_value,changed_by) VALUES(@id,@tenant,@action,@previous,@current,@actor)", connection, transaction);
            audit.Parameters.AddWithValue("id", auditId); audit.Parameters.AddWithValue("tenant", request.TenantId); audit.Parameters.AddWithValue("action", $"basic_setting:{request.EventKey}:{request.RecipientKey}"); audit.Parameters.AddWithValue("previous", current.ToString().ToLowerInvariant()); audit.Parameters.AddWithValue("current", request.Enabled.ToString().ToLowerInvariant()); audit.Parameters.AddWithValue("actor", request.Actor); await audit.ExecuteNonQueryAsync(cancellationToken);
            result = new("saved", request.Enabled, version, false, auditId, "Basic setting saved. Automatic sending remains inactive.");
        }
        await using (var complete = new NpgsqlCommand("UPDATE public.automation_command_idempotency SET result_status='completed',result_json=CAST(@result AS jsonb),template_version=@version,audit_id=@audit,completed_at=NOW() WHERE tenant_id=@tenant AND command_type=@type AND idempotency_key=@key AND request_hash=@hash", connection, transaction))
        {
            complete.Parameters.AddWithValue("result", JsonSerializer.Serialize(result)); complete.Parameters.AddWithValue("version", result.SettingVersion); complete.Parameters.AddWithValue("audit", result.AuditId.HasValue ? result.AuditId.Value : DBNull.Value); complete.Parameters.AddWithValue("tenant", request.TenantId); complete.Parameters.AddWithValue("type", CommandType); complete.Parameters.AddWithValue("key", request.IdempotencyKey); complete.Parameters.AddWithValue("hash", hash); await complete.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken); return result;
    }
}

public sealed record BasicSettingSaveCommand(Guid TenantId, Guid InspectorId, string EventKey, string RecipientKey, bool Enabled, int ExpectedVersion, string IdempotencyKey, string Actor, string RequestId);
public sealed record BasicSettingSaveResult(string Status, bool Enabled, int SettingVersion, bool Replayed, Guid? AuditId, string Message);
