using Npgsql;
using NpgsqlTypes;

namespace AutoMateApi;

/// <summary>
/// Persistence and validation for the tenant-owned Basic Automations feature.
/// Endpoint authorization remains the responsibility of the caller.
/// </summary>
public static class BasicAutomationSupport
{
    public const string TemplateType = "basic_automation";

    public static readonly IReadOnlyList<string> EventKeys =
        new[] { "scheduling", "rescheduling", "cancellation", "service_change", "publishing" };

    public static readonly IReadOnlyList<string> RecipientKeys =
        new[] { "contact_1", "contact_2" };

    public static bool IsValidEvent(string? value) =>
        EventKeys.Contains(Normalize(value), StringComparer.Ordinal);

    public static bool IsValidRecipient(string? value) =>
        RecipientKeys.Contains(Normalize(value), StringComparer.Ordinal);

    public static string BuildServiceTypeKey(string eventKey, string recipientKey)
    {
        eventKey = RequireEvent(eventKey);
        recipientKey = RequireRecipient(recipientKey);
        return $"{eventKey}:{recipientKey}";
    }

    public static string BuildDisplayName(string eventKey, string recipientLabel)
    {
        var eventLabel = RequireEvent(eventKey) switch
        {
            "scheduling" => "Scheduling",
            "rescheduling" => "Rescheduling",
            "cancellation" => "Cancellation",
            "service_change" => "Service Change",
            "publishing" => "Publishing",
            _ => throw new InvalidOperationException()
        };
        return $"{eventLabel} - {CleanLabel(recipientLabel)}";
    }

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        const string sql = """
            CREATE TABLE IF NOT EXISTS public.basic_automation_settings
            (
                tenant_id uuid NOT NULL,
                event_key text NOT NULL,
                recipient_key text NOT NULL,
                enabled boolean NOT NULL DEFAULT false,
                template_id uuid NULL,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                setting_version integer NOT NULL DEFAULT 1,
                PRIMARY KEY (tenant_id, event_key, recipient_key),
                CONSTRAINT ck_basic_automation_event
                    CHECK (event_key IN ('scheduling','rescheduling','cancellation','service_change','publishing')),
                CONSTRAINT ck_basic_automation_recipient
                    CHECK (recipient_key IN ('contact_1','contact_2'))
            );

            CREATE INDEX IF NOT EXISTS idx_basic_automation_settings_template
                ON public.basic_automation_settings(template_id);

            CREATE TABLE IF NOT EXISTS public.basic_automation_executions
            (
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                revision_key text NOT NULL,
                event_key text NOT NULL,
                recipient_key text NOT NULL,
                state text NOT NULL DEFAULT 'claimed',
                message_id text NOT NULL DEFAULT '',
                error text NOT NULL DEFAULT '',
                claimed_at timestamptz NOT NULL DEFAULT NOW(),
                completed_at timestamptz NULL,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY (tenant_id, job_id, revision_key, event_key, recipient_key),
                CONSTRAINT ck_basic_execution_event
                    CHECK (event_key IN ('scheduling','rescheduling','cancellation','service_change','publishing')),
                CONSTRAINT ck_basic_execution_recipient
                    CHECK (recipient_key IN ('contact_1','contact_2')),
                CONSTRAINT ck_basic_execution_state
                    CHECK (state IN ('claimed','sent','skipped','failed'))
            );

            ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS tenant_id uuid NULL;
            ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS archived_at timestamptz NULL;
            ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS basic_event_key text NULL;
            ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS basic_recipient_key text NULL;

            ALTER TABLE public.basic_automation_settings DROP CONSTRAINT IF EXISTS ck_basic_automation_event;
            ALTER TABLE public.basic_automation_settings ADD CONSTRAINT ck_basic_automation_event
                CHECK (event_key IN ('scheduling','rescheduling','cancellation','service_change','publishing'));
            ALTER TABLE public.basic_automation_executions DROP CONSTRAINT IF EXISTS ck_basic_execution_event;
            ALTER TABLE public.basic_automation_executions ADD CONSTRAINT ck_basic_execution_event
                CHECK (event_key IN ('scheduling','rescheduling','cancellation','service_change','publishing'));

            CREATE UNIQUE INDEX IF NOT EXISTS uq_email_templates_basic_tenant_slot
                ON public.email_templates(tenant_id, basic_event_key, basic_recipient_key)
                WHERE tenant_id IS NOT NULL
                  AND basic_event_key IS NOT NULL
                  AND basic_recipient_key IS NOT NULL
                  AND archived_at IS NULL;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Creates the eight legacy recipient slots plus Client-only Publishing. Only Scheduling/Contact 1 is enabled initially.</summary>
    public static async Task EnsureTenantDefaultsAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty) throw new ArgumentException("Tenant ID is required.", nameof(tenantId));
        const string sql = """
            INSERT INTO public.basic_automation_settings(tenant_id,event_key,recipient_key,enabled)
            SELECT @tenant, event_key, recipient_key,
                   (event_key='scheduling' AND recipient_key='contact_1')
            FROM unnest(ARRAY['scheduling','rescheduling','cancellation','service_change']) event_key
            CROSS JOIN unnest(ARRAY['contact_1','contact_2']) recipient_key
            UNION ALL
            SELECT @tenant,'publishing','contact_1',false
            ON CONFLICT (tenant_id,event_key,recipient_key) DO NOTHING;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<IReadOnlyList<BasicAutomationSlot>> LoadAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        CancellationToken cancellationToken = default)
    {
        await EnsureTenantDefaultsAsync(connection, tenantId, cancellationToken);
        const string sql = """
            SELECT s.event_key,s.recipient_key,s.enabled,s.template_id,
                   COALESCE(t.name,''),COALESCE(t.subject,''),COALESCE(t.html_body,''),
                   t.updated_at,s.setting_version
            FROM public.basic_automation_settings s
            LEFT JOIN public.email_templates t
              ON t.template_id=s.template_id AND t.tenant_id=s.tenant_id AND t.archived_at IS NULL
            WHERE s.tenant_id=@tenant
            ORDER BY array_position(ARRAY['scheduling','rescheduling','cancellation','service_change','publishing'],s.event_key),
                     array_position(ARRAY['contact_1','contact_2'],s.recipient_key);
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var results = new List<BasicAutomationSlot>();
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new BasicAutomationSlot(
                reader.GetString(0), reader.GetString(1), reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetGuid(3), reader.GetString(4),
                reader.GetString(5), reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetDateTime(7), reader.GetInt32(8)));
        }
        return results;
    }

    public static async Task SetEnabledAsync(
        NpgsqlConnection connection, Guid tenantId, string eventKey, string recipientKey, bool enabled,
        CancellationToken cancellationToken = default)
    {
        eventKey = RequireEvent(eventKey);
        recipientKey = RequireRecipient(recipientKey);
        const string sql = """
            INSERT INTO public.basic_automation_settings(tenant_id,event_key,recipient_key,enabled,updated_at)
            VALUES(@tenant,@event,@recipient,@enabled,NOW())
            ON CONFLICT(tenant_id,event_key,recipient_key)
            DO UPDATE SET enabled=EXCLUDED.enabled,updated_at=NOW();
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        command.Parameters.AddWithValue("enabled", enabled);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Saves the single template assigned to a tenant/event/recipient slot. HTML must be sanitized by the caller.
    /// </summary>
    public static async Task<Guid> SaveTemplateAsync(
        NpgsqlConnection connection, Guid tenantId, Guid inspectorId, string eventKey, string recipientKey,
        string recipientLabel, string subject, string htmlBody, CancellationToken cancellationToken = default)
    {
        eventKey = RequireEvent(eventKey);
        recipientKey = RequireRecipient(recipientKey);
        if (tenantId == Guid.Empty || inspectorId == Guid.Empty)
            throw new ArgumentException("Tenant and inspector IDs are required.");

        await EnsureTenantDefaultsAsync(connection, tenantId, cancellationToken);
        var templateId = Guid.NewGuid();
        const string sql = """
            WITH existing AS (
                SELECT template_id FROM public.basic_automation_settings
                WHERE tenant_id=@tenant AND event_key=@event AND recipient_key=@recipient
            ), saved AS (
                INSERT INTO public.email_templates
                    (template_id,tenant_id,inspector_id,template_type,service_type_key,email_type,name,
                     subject,html_body,is_active,basic_event_key,basic_recipient_key,created_at,updated_at)
                VALUES
                    (COALESCE((SELECT template_id FROM existing),@new_id),@tenant,@inspector,@type,@service,
                     'transactional',@name,@subject,@html,true,@event,@recipient,NOW(),NOW())
                ON CONFLICT(template_id) DO UPDATE SET
                    name=EXCLUDED.name,subject=EXCLUDED.subject,html_body=EXCLUDED.html_body,
                    is_active=true,archived_at=NULL,basic_event_key=EXCLUDED.basic_event_key,
                    basic_recipient_key=EXCLUDED.basic_recipient_key,updated_at=NOW()
                WHERE public.email_templates.tenant_id=EXCLUDED.tenant_id
                RETURNING template_id
            )
            UPDATE public.basic_automation_settings s
            SET template_id=saved.template_id,updated_at=NOW()
            FROM saved
            WHERE s.tenant_id=@tenant AND s.event_key=@event AND s.recipient_key=@recipient
            RETURNING saved.template_id;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("new_id", templateId);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("inspector", inspectorId);
        command.Parameters.AddWithValue("type", TemplateType);
        command.Parameters.AddWithValue("service", BuildServiceTypeKey(eventKey, recipientKey));
        command.Parameters.AddWithValue("name", BuildDisplayName(eventKey, recipientLabel));
        command.Parameters.AddWithValue("subject", subject ?? string.Empty);
        command.Parameters.AddWithValue("html", htmlBody ?? string.Empty);
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is Guid id ? id : throw new InvalidOperationException("The Basic template could not be saved.");
    }

    /// <summary>
    /// Copies the preferred legacy booking template into Scheduling/Contact 1. Legacy data is never updated.
    /// </summary>
    public static async Task<Guid?> SeedSchedulingContactOneAsync(
        NpgsqlConnection connection, Guid tenantId, Guid inspectorId, string recipientLabel,
        CancellationToken cancellationToken = default)
    {
        await EnsureTenantDefaultsAsync(connection, tenantId, cancellationToken);
        const string sql = """
            SELECT t.subject,t.html_body
            FROM public.email_templates t
            WHERE (t.tenant_id=@tenant OR (t.tenant_id IS NULL AND t.inspector_id=@inspector))
              AND t.is_active=true AND t.archived_at IS NULL
              AND t.template_type='booking-email'
              AND NOT EXISTS (
                  SELECT 1 FROM public.basic_automation_settings s
                  WHERE s.tenant_id=@tenant AND s.event_key='scheduling'
                    AND s.recipient_key='contact_1' AND s.template_id IS NOT NULL)
            ORDER BY CASE WHEN t.service_type_key='general_booking' THEN 0 ELSE 1 END,
                     t.updated_at DESC
            LIMIT 1;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("inspector", inspectorId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var subject = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
        var html = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
        await reader.DisposeAsync();
        return await SaveTemplateAsync(connection, tenantId, inspectorId, "scheduling", "contact_1",
            recipientLabel, subject, html, cancellationToken);
    }

    /// <summary>Atomically reserves an event/recipient execution. False means it was already reserved.</summary>
    public static async Task<bool> TryClaimExecutionAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, string revisionKey,
        string eventKey, string recipientKey, CancellationToken cancellationToken = default)
    {
        eventKey = RequireEvent(eventKey);
        recipientKey = RequireRecipient(recipientKey);
        if (string.IsNullOrWhiteSpace(revisionKey)) throw new ArgumentException("Revision key is required.", nameof(revisionKey));
        const string sql = """
            INSERT INTO public.basic_automation_executions
                (tenant_id,job_id,revision_key,event_key,recipient_key)
            VALUES(@tenant,@job,@revision,@event,@recipient)
            ON CONFLICT (tenant_id,job_id,revision_key,event_key,recipient_key)
            DO UPDATE SET state='claimed',error='',claimed_at=NOW(),updated_at=NOW()
            WHERE public.basic_automation_executions.state='failed';
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.AddWithValue("revision", revisionKey.Trim());
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public static async Task CompleteExecutionAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, string revisionKey,
        string eventKey, string recipientKey, BasicExecutionState state, string? messageId = null,
        string? error = null, CancellationToken cancellationToken = default)
    {
        eventKey = RequireEvent(eventKey);
        recipientKey = RequireRecipient(recipientKey);
        var stateValue = state.ToString().ToLowerInvariant();
        const string sql = """
            UPDATE public.basic_automation_executions
            SET state=@state,message_id=@message,error=@error,
                completed_at=CASE WHEN @state IN ('sent','skipped') THEN NOW() ELSE completed_at END,
                updated_at=NOW()
            WHERE tenant_id=@tenant AND job_id=@job AND revision_key=@revision
              AND event_key=@event AND recipient_key=@recipient;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("state", stateValue);
        command.Parameters.AddWithValue("message", messageId ?? string.Empty);
        command.Parameters.AddWithValue("error", error ?? string.Empty);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.AddWithValue("revision", revisionKey.Trim());
        command.Parameters.AddWithValue("event", eventKey);
        command.Parameters.AddWithValue("recipient", recipientKey);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string RequireEvent(string? value)
    {
        value = Normalize(value);
        return IsValidEvent(value) ? value : throw new ArgumentException("Unsupported Basic automation event.");
    }

    private static string RequireRecipient(string? value)
    {
        value = Normalize(value);
        return IsValidRecipient(value) ? value : throw new ArgumentException("Unsupported Basic automation recipient.");
    }

    private static string Normalize(string? value) => (value ?? string.Empty).Trim().ToLowerInvariant();

    private static string CleanLabel(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "Recipient" : value.Trim();
}

public sealed record BasicAutomationSlot(
    string EventKey, string RecipientKey, bool Enabled, Guid? TemplateId,
    string TemplateName, string Subject, string HtmlBody, DateTime? UpdatedAt, int SettingVersion);

public enum BasicExecutionState
{
    Sent,
    Skipped,
    Failed
}
