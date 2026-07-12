using System.Security.Cryptography;
using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace AutoMateApi;

/// <summary>
/// Stores an explicitly selected disposable job's Basic Automation test actions.
/// This component deliberately has no dispatch, completion, SMTP, workflow, or
/// integration methods. An approved action remains inert until a later release
/// introduces a separately reviewed sender.
/// </summary>
public static class BasicTestExecutionSupport
{
    public const string SupportedEvent = "scheduling";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        const string sql = """
            CREATE TABLE IF NOT EXISTS public.basic_test_job_opt_ins
            (
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                enabled boolean NOT NULL DEFAULT false,
                disposable_confirmed boolean NOT NULL DEFAULT false,
                selected_by text NOT NULL DEFAULT '',
                version integer NOT NULL DEFAULT 1,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY (tenant_id, job_id)
            );

            CREATE TABLE IF NOT EXISTS public.basic_test_actions
            (
                action_id uuid PRIMARY KEY,
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                revision_key text NOT NULL,
                event_key text NOT NULL,
                recipient_key text NOT NULL,
                recipient_email text NOT NULL,
                recipient_name text NOT NULL DEFAULT '',
                template_id uuid NOT NULL,
                template_version integer NOT NULL,
                rendered_subject text NOT NULL DEFAULT '',
                rendered_html text NOT NULL DEFAULT '',
                test_recipient_email text NOT NULL DEFAULT '',
                provider_message_id text NOT NULL DEFAULT '',
                completion_error text NOT NULL DEFAULT '',
                state text NOT NULL DEFAULT 'evaluated',
                evaluation_fingerprint text NOT NULL,
                created_by text NOT NULL DEFAULT '',
                queued_by text NOT NULL DEFAULT '',
                approved_by text NOT NULL DEFAULT '',
                created_at timestamptz NOT NULL DEFAULT NOW(),
                queued_at timestamptz NULL,
                approved_at timestamptz NULL,
                completed_at timestamptz NULL,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_basic_test_action_event CHECK (event_key='scheduling'),
                CONSTRAINT ck_basic_test_action_recipient CHECK (recipient_key IN ('contact_1','contact_2')),
                CONSTRAINT ck_basic_test_action_state CHECK (state IN ('evaluated','queued','sending','test_sent','failed','cancelled')),
                UNIQUE (tenant_id, job_id, revision_key, event_key, recipient_key)
            );

            CREATE INDEX IF NOT EXISTS idx_basic_test_actions_job
                ON public.basic_test_actions(tenant_id, job_id, created_at DESC);

            ALTER TABLE public.basic_test_actions ADD COLUMN IF NOT EXISTS rendered_subject text NOT NULL DEFAULT '';
            ALTER TABLE public.basic_test_actions ADD COLUMN IF NOT EXISTS rendered_html text NOT NULL DEFAULT '';
            ALTER TABLE public.basic_test_actions ADD COLUMN IF NOT EXISTS test_recipient_email text NOT NULL DEFAULT '';
            ALTER TABLE public.basic_test_actions ADD COLUMN IF NOT EXISTS provider_message_id text NOT NULL DEFAULT '';
            ALTER TABLE public.basic_test_actions ADD COLUMN IF NOT EXISTS completion_error text NOT NULL DEFAULT '';
            ALTER TABLE public.basic_test_actions ADD COLUMN IF NOT EXISTS completed_at timestamptz NULL;
            ALTER TABLE public.basic_test_actions DROP CONSTRAINT IF EXISTS ck_basic_test_action_state;
            ALTER TABLE public.basic_test_actions ADD CONSTRAINT ck_basic_test_action_state
                CHECK (state IN ('evaluated','queued','sending','test_sent','failed','cancelled'));
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<BasicTestOptInResult> SetOptInAsync(
        NpgsqlConnection connection,
        BasicTestOptInCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (request.Enabled && (!request.Confirmed || !request.DisposableConfirmed))
            return new("confirmation_required", false, 0, "Confirm that this is a disposable test job.");

        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireOwnedBasicJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);

        const string selectSql = """
            SELECT enabled,disposable_confirmed,version
            FROM public.basic_test_job_opt_ins
            WHERE tenant_id=@tenant AND job_id=@job
            FOR UPDATE;
            """;
        bool previousEnabled = false;
        bool previousDisposable = false;
        int version = 0;
        await using (var select = new NpgsqlCommand(selectSql, connection, transaction))
        {
            select.Parameters.AddWithValue("tenant", request.TenantId);
            select.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                previousEnabled = reader.GetBoolean(0);
                previousDisposable = reader.GetBoolean(1);
                version = reader.GetInt32(2);
            }
        }

        var requestedDisposable = request.Enabled && request.DisposableConfirmed;
        if (version > 0 && previousEnabled == request.Enabled && previousDisposable == requestedDisposable)
        {
            await transaction.CommitAsync(cancellationToken);
            return new("replayed", previousEnabled, version, "The disposable test-job selection is already current.");
        }
        if (version != request.ExpectedVersion)
        {
            await transaction.CommitAsync(cancellationToken);
            return new("conflict", previousEnabled, version, "The test-job selection changed. Reload and try again.");
        }

        var newVersion = version + 1;
        const string upsertSql = """
            INSERT INTO public.basic_test_job_opt_ins
                (tenant_id,job_id,enabled,disposable_confirmed,selected_by,version,updated_at)
            VALUES(@tenant,@job,@enabled,@disposable,@actor,@version,NOW())
            ON CONFLICT(tenant_id,job_id) DO UPDATE SET
                enabled=EXCLUDED.enabled,
                disposable_confirmed=EXCLUDED.disposable_confirmed,
                selected_by=EXCLUDED.selected_by,
                version=EXCLUDED.version,
                updated_at=NOW();
            """;
        await using (var upsert = new NpgsqlCommand(upsertSql, connection, transaction))
        {
            upsert.Parameters.AddWithValue("tenant", request.TenantId);
            upsert.Parameters.AddWithValue("job", request.JobId);
            upsert.Parameters.AddWithValue("enabled", request.Enabled);
            upsert.Parameters.AddWithValue("disposable", request.Enabled && request.DisposableConfirmed);
            upsert.Parameters.AddWithValue("actor", request.Actor.Trim());
            upsert.Parameters.AddWithValue("version", newVersion);
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }

        if (!request.Enabled)
        {
            await using var cancel = new NpgsqlCommand("""
                UPDATE public.basic_test_actions SET state='cancelled',updated_at=NOW()
                WHERE tenant_id=@tenant AND job_id=@job AND state IN ('evaluated','queued');
                """, connection, transaction);
            cancel.Parameters.AddWithValue("tenant", request.TenantId);
            cancel.Parameters.AddWithValue("job", request.JobId);
            await cancel.ExecuteNonQueryAsync(cancellationToken);
        }

        await AuditAsync(connection, transaction, request.TenantId, request.JobId,
            "basic_test_job_selection", previousEnabled.ToString().ToLowerInvariant(),
            request.Enabled.ToString().ToLowerInvariant(), request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("saved", request.Enabled, newVersion,
            request.Enabled ? "Disposable job selected for inert Basic email testing." : "Basic test selection disabled.");
    }

    public static async Task<BasicTestActionResult> EvaluateSchedulingAsync(
        NpgsqlConnection connection,
        BasicTestEvaluateCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (string.IsNullOrWhiteSpace(request.RevisionKey))
            throw new ArgumentException("A scheduling revision key is required.", nameof(request));
        if (string.IsNullOrWhiteSpace(request.RenderedSubject) || string.IsNullOrWhiteSpace(request.RenderedHtml))
            throw new ArgumentException("A rendered template snapshot is required.", nameof(request));
        var recipientKey = RequireRecipient(request.RecipientKey);

        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireOwnedBasicJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);

        const string sql = """
            SELECT s.enabled,s.template_id,COALESCE(t.template_version,1),
                   CASE WHEN @recipient='contact_2' THEN COALESCE(j.contact2_email,'') ELSE COALESCE(j.contact1_email,'') END,
                   CASE WHEN @recipient='contact_2' THEN COALESCE(NULLIF(j.contact2_display_name,''),TRIM(CONCAT_WS(' ',j.contact2_first_name,j.contact2_last_name)),'')
                        ELSE COALESCE(NULLIF(j.contact1_display_name,''),TRIM(CONCAT_WS(' ',j.contact1_first_name,j.contact1_last_name)),'') END
            FROM public.jobs_staging j
            JOIN public.basic_test_job_opt_ins o ON o.tenant_id=@tenant AND o.job_id=j.job_id
                AND o.enabled=true AND o.disposable_confirmed=true
            JOIN public.basic_automation_settings s ON s.tenant_id=@tenant
                AND s.event_key='scheduling' AND s.recipient_key=@recipient
            LEFT JOIN public.email_templates t ON t.template_id=s.template_id AND t.tenant_id=@tenant
                AND t.archived_at IS NULL
            WHERE j.job_id=@job AND j.tenant_id::text=@tenant_text;
            """;
        bool enabled;
        Guid? templateId;
        int templateVersion;
        string recipientEmail;
        string recipientName;
        var found = false;
        await using (var select = new NpgsqlCommand(sql, connection, transaction))
        {
            select.Parameters.AddWithValue("tenant", request.TenantId);
            select.Parameters.AddWithValue("job", request.JobId);
            select.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
            select.Parameters.AddWithValue("recipient", recipientKey);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            found = await reader.ReadAsync(cancellationToken);
            if (found)
            {
                enabled = reader.GetBoolean(0);
                templateId = reader.IsDBNull(1) ? null : reader.GetGuid(1);
                templateVersion = reader.GetInt32(2);
                recipientEmail = reader.GetString(3).Trim();
                recipientName = reader.GetString(4).Trim();
            }
            else
            {
                enabled = false; templateId = null; templateVersion = 0; recipientEmail = ""; recipientName = "";
            }
        }
        if (!found) return await RollbackResultAsync(transaction, "not_selected", "Select and confirm this disposable job first.");

        if (!enabled) return await RollbackResultAsync(transaction, "slot_disabled", "This Basic email slot is disabled.");
        if (!templateId.HasValue) return await RollbackResultAsync(transaction, "template_required", "Save a template before evaluating this action.");
        if (!LooksLikeEmail(recipientEmail)) return await RollbackResultAsync(transaction, "recipient_required", "The selected THREED contact has no valid email address.");

        var revision = request.RevisionKey.Trim();
        var fingerprint = Hash($"{request.TenantId:N}|{request.JobId:N}|{revision}|scheduling|{recipientKey}|{templateId:N}|{templateVersion}|{recipientEmail.ToLowerInvariant()}|{Hash(request.RenderedSubject)}|{Hash(request.RenderedHtml)}");
        var actionId = Guid.NewGuid();
        const string insertSql = """
            INSERT INTO public.basic_test_actions
                (action_id,tenant_id,job_id,revision_key,event_key,recipient_key,recipient_email,
                 recipient_name,template_id,template_version,rendered_subject,rendered_html,evaluation_fingerprint,created_by)
            VALUES(@id,@tenant,@job,@revision,'scheduling',@recipient,@email,@name,@template,@version,@subject,@html,@fingerprint,@actor)
            ON CONFLICT(tenant_id,job_id,revision_key,event_key,recipient_key) DO NOTHING
            RETURNING action_id;
            """;
        await using (var insert = new NpgsqlCommand(insertSql, connection, transaction))
        {
            insert.Parameters.AddWithValue("id", actionId);
            insert.Parameters.AddWithValue("tenant", request.TenantId);
            insert.Parameters.AddWithValue("job", request.JobId);
            insert.Parameters.AddWithValue("revision", revision);
            insert.Parameters.AddWithValue("recipient", recipientKey);
            insert.Parameters.AddWithValue("email", recipientEmail);
            insert.Parameters.AddWithValue("name", recipientName);
            insert.Parameters.AddWithValue("template", templateId.Value);
            insert.Parameters.AddWithValue("version", templateVersion);
            insert.Parameters.AddWithValue("subject", request.RenderedSubject.Trim());
            insert.Parameters.AddWithValue("html", request.RenderedHtml);
            insert.Parameters.AddWithValue("fingerprint", fingerprint);
            insert.Parameters.AddWithValue("actor", request.Actor.Trim());
            var inserted = await insert.ExecuteScalarAsync(cancellationToken);
            if (inserted is not Guid)
            {
                await using var existing = new NpgsqlCommand("""
                    SELECT action_id,state,evaluation_fingerprint FROM public.basic_test_actions
                    WHERE tenant_id=@tenant AND job_id=@job AND revision_key=@revision
                      AND event_key='scheduling' AND recipient_key=@recipient;
                    """, connection, transaction);
                existing.Parameters.AddWithValue("tenant", request.TenantId);
                existing.Parameters.AddWithValue("job", request.JobId);
                existing.Parameters.AddWithValue("revision", revision);
                existing.Parameters.AddWithValue("recipient", recipientKey);
                await using var reader = await existing.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                var existingId = reader.GetGuid(0);
                var existingState = reader.GetString(1);
                var existingFingerprint = reader.GetString(2);
                await reader.DisposeAsync();
                await transaction.CommitAsync(cancellationToken);
                return existingFingerprint == fingerprint
                    ? new("replayed", existingId, existingState, true, "The same inert test action already exists.")
                    : new("revision_conflict", existingId, existingState, false, "The revision key already represents different content.");
            }
        }

        await AuditAsync(connection, transaction, request.TenantId, request.JobId,
            $"basic_test_action:evaluated:{recipientKey}", "", actionId.ToString(), request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("evaluated", actionId, "evaluated", false, "Action evaluated. Nothing has been queued or sent.");
    }

    public static Task<BasicTestActionResult> QueueAsync(NpgsqlConnection connection, BasicTestTransitionCommand request, CancellationToken cancellationToken = default) =>
        TransitionAsync(connection, request, "evaluated", "queued", cancellationToken);

    /// <summary>Convenience operation for the queue/prepare endpoint; it never sends.</summary>
    public static async Task<BasicTestActionResult> PrepareAsync(NpgsqlConnection connection,
        BasicTestEvaluateCommand request, CancellationToken cancellationToken = default)
    {
        var evaluated = await EvaluateSchedulingAsync(connection, request, cancellationToken);
        if (evaluated.ActionId is null || evaluated.Status is not ("evaluated" or "replayed")) return evaluated;
        if (evaluated.State != "evaluated") return evaluated;
        return await QueueAsync(connection,
            new BasicTestTransitionCommand(request.TenantId, request.JobId, evaluated.ActionId.Value, true, request.Actor),
            cancellationToken);
    }

    public static Task<BasicTestActionResult> ApproveAsync(NpgsqlConnection connection, BasicTestTransitionCommand request, CancellationToken cancellationToken = default)
    {
        if (!request.Confirmed)
            return Task.FromResult(new BasicTestActionResult("confirmation_required", request.ActionId, "queued", false, "Explicit approval is required."));
        return TransitionAsync(connection, request, "queued", "sending", cancellationToken);
    }

    /// <summary>Returns inert queue items for a single tenant-owned selected test job.</summary>
    public static async Task<IReadOnlyList<BasicTestQueueItem>> LoadQueueAsync(NpgsqlConnection connection,
        Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        await EnsureAsync(connection, cancellationToken);
        if (!await AutomationFoundationSupport.JobBelongsToTenantAsync(connection, tenantId, jobId))
            throw new UnauthorizedAccessException("The job does not belong to this tenant.");
        const string sql = """
            SELECT action_id,revision_key,event_key,recipient_key,recipient_email,recipient_name,
                   template_id,template_version,state,rendered_subject,rendered_html,
                   test_recipient_email,provider_message_id,completion_error,
                   created_at,queued_at,approved_at,completed_at
            FROM public.basic_test_actions
            WHERE tenant_id=@tenant AND job_id=@job
            ORDER BY created_at DESC
            LIMIT 50;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var items = new List<BasicTestQueueItem>();
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                reader.GetString(4), reader.GetString(5), reader.GetGuid(6), reader.GetInt32(7), reader.GetString(8),
                reader.GetString(9), reader.GetString(10), reader.GetString(11), reader.GetString(12), reader.GetString(13),
                reader.GetDateTime(14), ReadDate(reader, 15), ReadDate(reader, 16), ReadDate(reader, 17)));
        }
        return items;
    }

    public static async Task<BasicTestOptInResult> LoadOptInAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        await EnsureAsync(connection, cancellationToken);
        if (!await AutomationFoundationSupport.JobBelongsToTenantAsync(connection, tenantId, jobId)) throw new UnauthorizedAccessException("The job does not belong to this tenant.");
        await using var command = new NpgsqlCommand("SELECT enabled,version FROM public.basic_test_job_opt_ins WHERE tenant_id=@tenant AND job_id=@job", connection);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return new("current", false, 0, "This job is not selected for Basic test mode.");
        return new("current", reader.GetBoolean(0), reader.GetInt32(1), "Current Basic test-job selection.");
    }

    /// <summary>
    /// Records the result of the connector's explicit tester-override delivery.
    /// It cannot dispatch email and rejects completion before server-side approval.
    /// </summary>
    public static async Task<BasicTestActionResult> CompleteAsync(NpgsqlConnection connection,
        BasicTestCompleteCommand request, CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (request.ActionId == Guid.Empty) throw new ArgumentException("Action ID is required.");
        if (!request.Confirmed || !LooksLikeEmail(request.TestRecipientEmail))
            return new("confirmation_required", request.ActionId, "approved", false,
                "Confirm a valid tester-owned override address. The real contact address is never used for test delivery.");
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireOwnedBasicJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        const string readSql = """
            SELECT state,test_recipient_email,provider_message_id,completion_error,recipient_email
            FROM public.basic_test_actions
            WHERE action_id=@id AND tenant_id=@tenant AND job_id=@job FOR UPDATE;
            """;
        string? state;
        string existingRecipient = "";
        string sourceRecipient = "";
        var found = false;
        await using (var read = new NpgsqlCommand(readSql, connection, transaction))
        {
            read.Parameters.AddWithValue("id", request.ActionId);
            read.Parameters.AddWithValue("tenant", request.TenantId);
            read.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            found = await reader.ReadAsync(cancellationToken);
            state = found ? reader.GetString(0) : null;
            if (found) { existingRecipient = reader.GetString(1); sourceRecipient = reader.GetString(4); }
        }
        if (!found) return await RollbackResultAsync(transaction, "not_found", "The tenant-owned test action was not found.", request.ActionId);
        if (string.Equals(sourceRecipient, request.TestRecipientEmail.Trim(), StringComparison.OrdinalIgnoreCase))
            return await RollbackResultAsync(transaction, "customer_recipient_forbidden", "The tester override must not be the THREED contact email.", request.ActionId);
        if (state is "test_sent" or "failed")
        {
            await transaction.CommitAsync(cancellationToken);
            var same = string.Equals(existingRecipient, request.TestRecipientEmail.Trim(), StringComparison.OrdinalIgnoreCase);
            return same
                ? new("replayed", request.ActionId, state, true, "The test outcome was already recorded.")
                : new("completion_conflict", request.ActionId, state, false, "This action was completed for a different tester address.");
        }
        if (state != "sending")
        {
            await transaction.CommitAsync(cancellationToken);
            return new("invalid_state", request.ActionId, state!, false, "Claim the queued test action before recording delivery.");
        }
        var target = request.Succeeded ? "test_sent" : "failed";
        await using (var update = new NpgsqlCommand("""
            UPDATE public.basic_test_actions SET state=@state,test_recipient_email=@recipient,
                provider_message_id=@message,completion_error=@error,completed_at=NOW(),updated_at=NOW()
            WHERE action_id=@id;
            """, connection, transaction))
        {
            update.Parameters.AddWithValue("state", target);
            update.Parameters.AddWithValue("recipient", request.TestRecipientEmail.Trim());
            update.Parameters.AddWithValue("message", request.ProviderMessageId ?? "");
            update.Parameters.AddWithValue("error", request.Succeeded ? "" : request.Error ?? "Test delivery failed.");
            update.Parameters.AddWithValue("id", request.ActionId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId,
            $"basic_test_action:{target}", "sending", target, request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(target, request.ActionId, target, false,
            request.Succeeded ? "Tester-override delivery recorded." : "Test delivery failure recorded for review.");
    }

    private static async Task<BasicTestActionResult> TransitionAsync(
        NpgsqlConnection connection, BasicTestTransitionCommand request, string expected, string target,
        CancellationToken cancellationToken)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (request.ActionId == Guid.Empty) throw new ArgumentException("Action ID is required.");
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireOwnedBasicJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);

        const string sql = """
            SELECT state FROM public.basic_test_actions
            WHERE action_id=@id AND tenant_id=@tenant AND job_id=@job FOR UPDATE;
            """;
        string? current;
        await using (var select = new NpgsqlCommand(sql, connection, transaction))
        {
            select.Parameters.AddWithValue("id", request.ActionId);
            select.Parameters.AddWithValue("tenant", request.TenantId);
            select.Parameters.AddWithValue("job", request.JobId);
            current = Convert.ToString(await select.ExecuteScalarAsync(cancellationToken));
        }
        if (string.IsNullOrWhiteSpace(current)) return await RollbackResultAsync(transaction, "not_found", "The tenant-owned test action was not found.", request.ActionId);
        if (current == target)
        {
            await transaction.CommitAsync(cancellationToken);
            return new("replayed", request.ActionId, current, true, $"Action is already {target}. No email was sent.");
        }
        if (current != expected)
        {
            await transaction.CommitAsync(cancellationToken);
            return new("invalid_state", request.ActionId, current, false, $"Action must be {expected} before it can become {target}.");
        }

        var actorColumn = target == "queued" ? "queued_by" : "approved_by";
        var timeColumn = target == "queued" ? "queued_at" : "approved_at";
        var updateSql = $"UPDATE public.basic_test_actions SET state=@state,{actorColumn}=@actor,{timeColumn}=NOW(),updated_at=NOW() WHERE action_id=@id";
        await using (var update = new NpgsqlCommand(updateSql, connection, transaction))
        {
            update.Parameters.AddWithValue("state", target);
            update.Parameters.AddWithValue("actor", request.Actor.Trim());
            update.Parameters.AddWithValue("id", request.ActionId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId,
            $"basic_test_action:{target}", expected, target, request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(target, request.ActionId, target, false,
            target == "sending" ? "Test action claimed once for explicit tester-override delivery. Automatic sending is not active." : "Test action queued for review. Nothing was sent.");
    }

    private static async Task RequireOwnedBasicJobAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid tenantId, Guid jobId, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT COALESCE((SELECT activation_mode FROM public.automation_tenant_settings WHERE tenant_id=@tenant),'selected_jobs'),
                   COALESCE((SELECT use_advanced_workflows FROM public.automation_job_selections WHERE tenant_id=@tenant AND job_id=@job),false)
            WHERE EXISTS(SELECT 1 FROM public.jobs_staging WHERE job_id=@job AND tenant_id::text=@tenant_text);
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = tenantId.ToString();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new UnauthorizedAccessException("The job does not belong to this tenant.");
        if (reader.GetString(0) == "all_jobs" || reader.GetBoolean(1))
            throw new InvalidOperationException("This job uses Advanced Workflows and cannot also use Basic Automation.");
    }

    private static async Task AuditAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId,
        Guid jobId, string action, string previous, string current, string actor, CancellationToken cancellationToken)
    {
        await using var audit = new NpgsqlCommand("""
            INSERT INTO public.automation_foundation_audit
                (tenant_id,job_id,action_key,previous_value,new_value,changed_by)
            VALUES(@tenant,@job,@action,@previous,@current,@actor);
            """, connection, transaction);
        audit.Parameters.AddWithValue("tenant", tenantId);
        audit.Parameters.AddWithValue("job", jobId);
        audit.Parameters.AddWithValue("action", action);
        audit.Parameters.AddWithValue("previous", previous);
        audit.Parameters.AddWithValue("current", current);
        audit.Parameters.AddWithValue("actor", actor.Trim());
        await audit.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<BasicTestActionResult> RollbackResultAsync(NpgsqlTransaction transaction, string status,
        string message, Guid? actionId = null)
    {
        await transaction.RollbackAsync();
        return new(status, actionId, "unavailable", false, message);
    }

    private static string RequireRecipient(string value) =>
        BasicAutomationSupport.IsValidRecipient(value) ? value.Trim().ToLowerInvariant() :
            throw new ArgumentException("Unsupported Basic automation recipient.", nameof(value));

    private static void ValidateIdentity(Guid tenantId, Guid jobId, string actor)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        if (string.IsNullOrWhiteSpace(actor)) throw new ArgumentException("An authenticated actor is required.");
    }

    private static bool LooksLikeEmail(string value) =>
        !string.IsNullOrWhiteSpace(value) && value.Contains('@', StringComparison.Ordinal) && !value.Any(char.IsWhiteSpace);

    private static string Hash(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();

    private static DateTime? ReadDate(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);
}

public sealed record BasicTestOptInCommand(Guid TenantId, Guid JobId, bool Enabled, bool DisposableConfirmed,
    bool Confirmed, int ExpectedVersion, string Actor);
public sealed record BasicTestOptInResult(string Status, bool Enabled, int Version, string Message);
public sealed record BasicTestEvaluateCommand(Guid TenantId, Guid JobId, string RevisionKey, string RecipientKey,
    string RenderedSubject, string RenderedHtml, string Actor);
public sealed record BasicTestTransitionCommand(Guid TenantId, Guid JobId, Guid ActionId, bool Confirmed, string Actor);
public sealed record BasicTestCompleteCommand(Guid TenantId, Guid JobId, Guid ActionId, string TestRecipientEmail,
    bool Confirmed, bool Succeeded, string? ProviderMessageId, string? Error, string Actor);
public sealed record BasicTestActionResult(string Status, Guid? ActionId, string State, bool Replayed, string Message);
public sealed record BasicTestQueueItem(Guid ActionId, string RevisionKey, string EventKey, string RecipientKey,
    string SourceRecipientEmail, string SourceRecipientName, Guid TemplateId, int TemplateVersion, string State,
    string RenderedSubject, string RenderedHtml, string TestRecipientEmail, string ProviderMessageId,
    string CompletionError, DateTime CreatedAt, DateTime? QueuedAt, DateTime? ApprovedAt, DateTime? CompletedAt);
