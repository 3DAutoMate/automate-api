using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace AutoMateApi;

/// <summary>
/// Versioned, idempotent persistence for the first AutoMate Next write command.
/// Authentication, entitlement and HTML validation remain endpoint responsibilities.
/// This type never executes customer workflows.
/// </summary>
public static class BasicTemplateCommandSupport
{
    public const string CommandType = "automations.basicTemplate.save";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        const string sql = """
            ALTER TABLE public.email_templates
                ADD COLUMN IF NOT EXISTS template_version integer NOT NULL DEFAULT 1;

            CREATE TABLE IF NOT EXISTS public.automation_command_idempotency
            (
                tenant_id uuid NOT NULL,
                command_type text NOT NULL,
                idempotency_key text NOT NULL,
                request_hash text NOT NULL,
                result_status text NOT NULL DEFAULT 'processing',
                result_json jsonb NULL,
                template_id uuid NULL,
                template_version integer NULL,
                audit_id uuid NULL,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                completed_at timestamptz NULL,
                PRIMARY KEY (tenant_id, command_type, idempotency_key)
            );

            CREATE TABLE IF NOT EXISTS public.automation_template_audit
            (
                audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id uuid NOT NULL,
                template_id uuid NULL,
                event_key text NOT NULL,
                recipient_key text NOT NULL,
                previous_version integer NOT NULL,
                new_version integer NOT NULL,
                previous_subject_hash text NOT NULL DEFAULT '',
                new_subject_hash text NOT NULL DEFAULT '',
                previous_body_hash text NOT NULL DEFAULT '',
                new_body_hash text NOT NULL DEFAULT '',
                changed_fields text[] NOT NULL DEFAULT ARRAY[]::text[],
                actor text NOT NULL DEFAULT '',
                request_id text NOT NULL DEFAULT '',
                idempotency_key text NOT NULL,
                outcome text NOT NULL DEFAULT 'saved',
                created_at timestamptz NOT NULL DEFAULT NOW()
            );
            ALTER TABLE public.automation_template_audit ALTER COLUMN template_id DROP NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_automation_template_audit_tenant_created
                ON public.automation_template_audit(tenant_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_automation_template_audit_template_created
                ON public.automation_template_audit(tenant_id, template_id, created_at DESC);
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<BasicTemplateSaveResult> SaveAsync(
        NpgsqlConnection connection,
        BasicTemplateSaveCommand request,
        CancellationToken cancellationToken = default)
    {
        Validate(request);
        await EnsureAsync(connection, cancellationToken);
        await BasicAutomationSupport.EnsureAsync(connection, cancellationToken);
        await BasicAutomationSupport.EnsureTenantDefaultsAsync(connection, request.TenantId, cancellationToken);

        var eventKey = request.EventKey.Trim().ToLowerInvariant();
        var recipientKey = request.RecipientKey.Trim().ToLowerInvariant();
        var idempotencyKey = request.IdempotencyKey.Trim();
        var requestHash = Hash(JsonSerializer.Serialize(new
        {
            request.TenantId,
            request.InspectorId,
            eventKey,
            recipientKey,
            request.RecipientLabel,
            request.Subject,
            request.HtmlBody,
            request.ExpectedVersion
        }));

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            var replay = await ClaimOrReplayAsync(connection, transaction, request.TenantId,
                idempotencyKey, requestHash, cancellationToken);
            if (replay is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                return replay;
            }

            var current = await LoadCurrentForUpdateAsync(connection, transaction, request.TenantId,
                eventKey, recipientKey, cancellationToken);
            var currentVersion = current?.Version ?? 0;
            if (currentVersion != request.ExpectedVersion)
            {
                var conflictAuditId = Guid.NewGuid();
                await InsertConflictAuditAsync(connection, transaction, conflictAuditId, request,
                    current?.TemplateId, eventKey, recipientKey, currentVersion, cancellationToken);
                var conflict = BasicTemplateSaveResult.Conflict(current?.TemplateId, currentVersion,
                    conflictAuditId, "The template was changed by another user. Reload and compare before saving.");
                await CompleteClaimAsync(connection, transaction, request.TenantId, idempotencyKey,
                    requestHash, conflict, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return conflict;
            }

            var templateId = current?.TemplateId ?? Guid.NewGuid();
            var nextVersion = currentVersion + 1;
            var name = BasicAutomationSupport.BuildDisplayName(eventKey, request.RecipientLabel);
            await UpsertTemplateAsync(connection, transaction, request, templateId, nextVersion,
                eventKey, recipientKey, name, cancellationToken);
            await AssignTemplateAsync(connection, transaction, request.TenantId, templateId,
                eventKey, recipientKey, cancellationToken);

            var auditId = Guid.NewGuid();
            var subjectHash = Hash(request.Subject);
            var bodyHash = Hash(request.HtmlBody);
            var changed = new List<string>();
            if (!string.Equals(current?.Subject, request.Subject, StringComparison.Ordinal)) changed.Add("subject");
            if (!string.Equals(current?.HtmlBody, request.HtmlBody, StringComparison.Ordinal)) changed.Add("html_body");
            if (!string.Equals(current?.Name, name, StringComparison.Ordinal)) changed.Add("name");
            await InsertAuditAsync(connection, transaction, auditId, request, templateId,
                eventKey, recipientKey, currentVersion, nextVersion, current, subjectHash, bodyHash,
                changed, cancellationToken);

            var result = new BasicTemplateSaveResult("saved", templateId, nextVersion,
                DateTimeOffset.UtcNow, subjectHash, bodyHash, auditId, false, "Template saved.");
            await CompleteClaimAsync(connection, transaction, request.TenantId, idempotencyKey,
                requestHash, result, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return result;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    public static async Task<IReadOnlyList<TemplateAuditEntry>> LoadTemplateAuditAsync(
        NpgsqlConnection connection, Guid tenantId, Guid? templateId = null, int limit = 100,
        CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty) throw new ArgumentException("Tenant ID is required.", nameof(tenantId));
        limit = Math.Clamp(limit, 1, 250);
        const string sql = """
            SELECT audit_id,template_id,event_key,recipient_key,previous_version,new_version,
                   changed_fields,actor,request_id,idempotency_key,outcome,created_at
            FROM public.automation_template_audit
            WHERE tenant_id=@tenant AND (@template IS NULL OR template_id=@template)
            ORDER BY created_at DESC LIMIT @limit;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.Add("template", NpgsqlDbType.Uuid).Value = templateId.HasValue ? templateId.Value : DBNull.Value;
        command.Parameters.AddWithValue("limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var entries = new List<TemplateAuditEntry>();
        while (await reader.ReadAsync(cancellationToken))
            entries.Add(new(reader.GetGuid(0), reader.IsDBNull(1) ? null : reader.GetGuid(1), reader.GetString(2), reader.GetString(3),
                reader.GetInt32(4), reader.GetInt32(5), reader.GetFieldValue<string[]>(6), reader.GetString(7),
                reader.GetString(8), reader.GetString(9), reader.GetString(10), reader.GetFieldValue<DateTimeOffset>(11)));
        return entries;
    }

    /// <summary>Combines only authoritative, existing job audit sources. Missing sources are omitted.</summary>
    public static async Task<IReadOnlyList<JobAuditEntry>> LoadJobAuditAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, int limit = 200,
        CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        limit = Math.Clamp(limit, 1, 500);
        var entries = new List<JobAuditEntry>();

        await AddJobChangeAuditAsync(connection, tenantId, jobId, entries, cancellationToken);
        await AddAddressAuditAsync(connection, tenantId, jobId, entries, cancellationToken);
        await AddPropertyAuditAsync(connection, tenantId, jobId, entries, cancellationToken);
        await AddAutomationAuditAsync(connection, tenantId, jobId, entries, cancellationToken);
        return entries.OrderByDescending(x => x.CreatedAt).Take(limit).ToArray();
    }

    private static async Task<BasicTemplateSaveResult?> ClaimOrReplayAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, Guid tenantId, string key, string requestHash,
        CancellationToken cancellationToken)
    {
        const string insert = """
            INSERT INTO public.automation_command_idempotency
                (tenant_id,command_type,idempotency_key,request_hash)
            VALUES(@tenant,@type,@key,@hash)
            ON CONFLICT DO NOTHING;
            """;
        await using (var command = new NpgsqlCommand(insert, connection, transaction))
        {
            command.Parameters.AddWithValue("tenant", tenantId);
            command.Parameters.AddWithValue("type", CommandType);
            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("hash", requestHash);
            if (await command.ExecuteNonQueryAsync(cancellationToken) == 1) return null;
        }

        const string select = """
            SELECT request_hash,result_status,COALESCE(result_json::text,'')
            FROM public.automation_command_idempotency
            WHERE tenant_id=@tenant AND command_type=@type AND idempotency_key=@key FOR UPDATE;
            """;
        await using var query = new NpgsqlCommand(select, connection, transaction);
        query.Parameters.AddWithValue("tenant", tenantId);
        query.Parameters.AddWithValue("type", CommandType);
        query.Parameters.AddWithValue("key", key);
        await using var reader = await query.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Idempotency claim was not found.");
        var existingHash = reader.GetString(0);
        var status = reader.GetString(1);
        var json = reader.GetString(2);
        if (!string.Equals(existingHash, requestHash, StringComparison.Ordinal))
            return BasicTemplateSaveResult.IdempotencyConflict("The idempotency key was already used for different content.");
        if (status == "completed" && !string.IsNullOrWhiteSpace(json))
        {
            var saved = JsonSerializer.Deserialize<BasicTemplateSaveResult>(json);
            return saved is null ? throw new InvalidOperationException("Saved idempotency result is invalid.") : saved with { Replayed = true };
        }
        throw new InvalidOperationException("The same save command is already being processed.");
    }

    private static async Task<CurrentTemplate?> LoadCurrentForUpdateAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, Guid tenantId, string eventKey, string recipientKey,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT t.template_id,t.template_version,COALESCE(t.name,''),COALESCE(t.subject,''),COALESCE(t.html_body,'')
            FROM public.basic_automation_settings s
            LEFT JOIN public.email_templates t
              ON t.template_id=s.template_id AND t.tenant_id=s.tenant_id AND t.archived_at IS NULL
            WHERE s.tenant_id=@tenant AND s.event_key=@event AND s.recipient_key=@recipient
            FOR UPDATE OF s;
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0)) return null;
        return new(reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetString(4));
    }

    private static async Task UpsertTemplateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        BasicTemplateSaveCommand request, Guid templateId, int version, string eventKey, string recipientKey,
        string name, CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO public.email_templates
                (template_id,tenant_id,inspector_id,template_type,service_type_key,email_type,name,subject,
                 html_body,is_active,basic_event_key,basic_recipient_key,template_version,created_at,updated_at)
            VALUES(@id,@tenant,@inspector,@type,@service,'transactional',@name,@subject,@html,true,
                   @event,@recipient,@version,NOW(),NOW())
            ON CONFLICT(template_id) DO UPDATE SET
                inspector_id=EXCLUDED.inspector_id,name=EXCLUDED.name,subject=EXCLUDED.subject,
                html_body=EXCLUDED.html_body,is_active=true,archived_at=NULL,
                template_version=EXCLUDED.template_version,updated_at=NOW()
            WHERE public.email_templates.tenant_id=EXCLUDED.tenant_id;
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("id", templateId);
        command.Parameters.AddWithValue("tenant", request.TenantId);
        command.Parameters.AddWithValue("inspector", request.InspectorId);
        command.Parameters.AddWithValue("type", BasicAutomationSupport.TemplateType);
        command.Parameters.AddWithValue("service", BasicAutomationSupport.BuildServiceTypeKey(eventKey, recipientKey));
        command.Parameters.AddWithValue("name", name);
        command.Parameters.AddWithValue("subject", request.Subject);
        command.Parameters.AddWithValue("html", request.HtmlBody);
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        command.Parameters.AddWithValue("version", version);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new InvalidOperationException("The tenant-owned template could not be saved.");
    }

    private static async Task AssignTemplateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid tenantId, Guid templateId, string eventKey, string recipientKey, CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE public.basic_automation_settings SET template_id=@template,updated_at=NOW()
            WHERE tenant_id=@tenant AND event_key=@event AND recipient_key=@recipient;
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("template", templateId);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new InvalidOperationException("The Basic automation slot could not be assigned.");
    }

    private static async Task InsertAuditAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid auditId, BasicTemplateSaveCommand request, Guid templateId, string eventKey, string recipientKey,
        int previousVersion, int newVersion, CurrentTemplate? previous, string subjectHash, string bodyHash,
        IReadOnlyList<string> changedFields, CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO public.automation_template_audit
                (audit_id,tenant_id,template_id,event_key,recipient_key,previous_version,new_version,
                 previous_subject_hash,new_subject_hash,previous_body_hash,new_body_hash,changed_fields,
                 actor,request_id,idempotency_key,outcome)
            VALUES(@audit,@tenant,@template,@event,@recipient,@previous_version,@new_version,
                   @previous_subject,@new_subject,@previous_body,@new_body,@changed,
                   @actor,@request,@key,'saved');
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("audit", auditId);
        command.Parameters.AddWithValue("tenant", request.TenantId);
        command.Parameters.AddWithValue("template", templateId);
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        command.Parameters.AddWithValue("previous_version", previousVersion);
        command.Parameters.AddWithValue("new_version", newVersion);
        command.Parameters.AddWithValue("previous_subject", Hash(previous?.Subject ?? string.Empty));
        command.Parameters.AddWithValue("new_subject", subjectHash);
        command.Parameters.AddWithValue("previous_body", Hash(previous?.HtmlBody ?? string.Empty));
        command.Parameters.AddWithValue("new_body", bodyHash);
        command.Parameters.AddWithValue("changed", changedFields.ToArray());
        command.Parameters.AddWithValue("actor", request.Actor.Trim());
        command.Parameters.AddWithValue("request", request.RequestId.Trim());
        command.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertConflictAuditAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid auditId, BasicTemplateSaveCommand request, Guid? templateId, string eventKey, string recipientKey,
        int currentVersion, CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO public.automation_template_audit
                (audit_id,tenant_id,template_id,event_key,recipient_key,previous_version,new_version,
                 actor,request_id,idempotency_key,outcome)
            VALUES(@audit,@tenant,@template,@event,@recipient,@version,@version,
                   @actor,@request,@key,'conflict');
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("audit", auditId);
        command.Parameters.AddWithValue("tenant", request.TenantId);
        command.Parameters.Add("template", NpgsqlDbType.Uuid).Value = templateId.HasValue ? templateId.Value : DBNull.Value;
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        command.Parameters.AddWithValue("version", currentVersion);
        command.Parameters.AddWithValue("actor", request.Actor.Trim());
        command.Parameters.AddWithValue("request", request.RequestId.Trim());
        command.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task CompleteClaimAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid tenantId, string key, string requestHash, BasicTemplateSaveResult result,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE public.automation_command_idempotency
            SET result_status='completed',result_json=CAST(@result AS jsonb),template_id=@template,
                template_version=@version,audit_id=@audit,completed_at=NOW()
            WHERE tenant_id=@tenant AND command_type=@type AND idempotency_key=@key AND request_hash=@hash;
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("result", JsonSerializer.Serialize(result));
        command.Parameters.Add("template", NpgsqlDbType.Uuid).Value = result.TemplateId.HasValue ? result.TemplateId.Value : DBNull.Value;
        command.Parameters.Add("audit", NpgsqlDbType.Uuid).Value = result.AuditId.HasValue ? result.AuditId.Value : DBNull.Value;
        command.Parameters.AddWithValue("version", result.TemplateVersion);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("type", CommandType);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("hash", requestHash);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task AddJobChangeAuditAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        List<JobAuditEntry> target, CancellationToken cancellationToken)
    {
        if (!await TableExistsAsync(connection, "job_change_audit", cancellationToken)) return;
        const string sql = """
            SELECT event_type,COALESCE(actor,''),COALESCE(reasons,''),created_at
            FROM public.job_change_audit WHERE job_id=@job AND tenant_id=@tenant;
            """;
        await ReadJobEntriesAsync(connection, sql, tenantId, jobId, "Job change", target, cancellationToken);
    }

    private static async Task AddAddressAuditAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        List<JobAuditEntry> target, CancellationToken cancellationToken)
    {
        if (!await TableExistsAsync(connection, "address_change_audit", cancellationToken)) return;
        const string sql = """
            SELECT 'Address changed',COALESCE(confirmed_by,''),
                   CONCAT(COALESCE(previous_address,''),' -> ',COALESCE(new_address,'')),created_at
            FROM public.address_change_audit WHERE job_id=@job AND tenant_id=@tenant;
            """;
        await ReadJobEntriesAsync(connection, sql, tenantId, jobId, "Address", target, cancellationToken);
    }

    private static async Task AddPropertyAuditAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        List<JobAuditEntry> target, CancellationToken cancellationToken)
    {
        if (!await TableExistsAsync(connection, "online_property_lookup_audit", cancellationToken)) return;
        const string sql = """
            SELECT CONCAT(source,' lookup'),'',CONCAT(outcome,CASE WHEN COALESCE(error,'')='' THEN '' ELSE ': '||error END),created_at
            FROM public.online_property_lookup_audit WHERE job_id=@job AND tenant_id=@tenant;
            """;
        await ReadJobEntriesAsync(connection, sql, tenantId, jobId, "Online property", target, cancellationToken);
    }

    private static async Task AddAutomationAuditAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        List<JobAuditEntry> target, CancellationToken cancellationToken)
    {
        if (!await TableExistsAsync(connection, "automation_foundation_audit", cancellationToken)) return;
        const string sql = """
            SELECT action_key,COALESCE(changed_by,''),CONCAT(COALESCE(previous_value,''),' -> ',COALESCE(new_value,'')),created_at
            FROM public.automation_foundation_audit WHERE job_id=@job AND tenant_id=@tenant;
            """;
        await ReadJobEntriesAsync(connection, sql, tenantId, jobId, "Automation", target, cancellationToken);
    }

    private static async Task ReadJobEntriesAsync(NpgsqlConnection connection, string sql, Guid tenantId,
        Guid jobId, string source, List<JobAuditEntry> target, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            target.Add(new(source, reader.GetString(0), reader.GetString(1), reader.GetString(2),
                reader.GetFieldValue<DateTimeOffset>(3)));
    }

    private static async Task<bool> TableExistsAsync(NpgsqlConnection connection, string table,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT to_regclass(@table) IS NOT NULL;";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("table", $"public.{table}");
        return Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken));
    }

    private static void Validate(BasicTemplateSaveCommand request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.TenantId == Guid.Empty || request.InspectorId == Guid.Empty)
            throw new ArgumentException("Tenant and inspector IDs are required.");
        if (!BasicAutomationSupport.IsValidEvent(request.EventKey)) throw new ArgumentException("Unsupported Basic automation event.");
        if (!BasicAutomationSupport.IsValidRecipient(request.RecipientKey)) throw new ArgumentException("Unsupported Basic automation recipient.");
        if (request.ExpectedVersion < 0) throw new ArgumentOutOfRangeException(nameof(request.ExpectedVersion));
        if (string.IsNullOrWhiteSpace(request.Subject) || string.IsNullOrWhiteSpace(request.HtmlBody))
            throw new ArgumentException("Subject and HTML body are required.");
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey) || request.IdempotencyKey.Length > 200)
            throw new ArgumentException("A valid idempotency key is required.");
        if (string.IsNullOrWhiteSpace(request.Actor) || string.IsNullOrWhiteSpace(request.RequestId))
            throw new ArgumentException("Actor and request ID are required.");
    }

    private static string Hash(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value ?? string.Empty))).ToLowerInvariant();

    private sealed record CurrentTemplate(Guid TemplateId, int Version, string Name, string Subject, string HtmlBody);
}

public sealed record BasicTemplateSaveCommand(
    Guid TenantId, Guid InspectorId, string EventKey, string RecipientKey, string RecipientLabel,
    string Subject, string HtmlBody, int ExpectedVersion, string IdempotencyKey, string Actor, string RequestId);

public sealed record BasicTemplateSaveResult(
    string Status, Guid? TemplateId, int TemplateVersion, DateTimeOffset? UpdatedAt,
    string SubjectHash, string BodyHash, Guid? AuditId, bool Replayed, string Message)
{
    public static BasicTemplateSaveResult Conflict(Guid? id, int version, Guid? auditId, string message) =>
        new("conflict", id, version, null, string.Empty, string.Empty, auditId, false, message);
    public static BasicTemplateSaveResult IdempotencyConflict(string message) =>
        new("idempotency_conflict", null, 0, null, string.Empty, string.Empty, null, false, message);
}

public sealed record TemplateAuditEntry(
    Guid AuditId, Guid? TemplateId, string EventKey, string RecipientKey, int PreviousVersion,
    int NewVersion, string[] ChangedFields, string Actor, string RequestId, string IdempotencyKey,
    string Outcome, DateTimeOffset CreatedAt);

public sealed record JobAuditEntry(string Source, string Action, string Actor, string Outcome, DateTimeOffset CreatedAt);
