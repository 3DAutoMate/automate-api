using System.Security.Cryptography;
using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace AutoMateApi;

/// <summary>
/// Guarded first production Basic Automation slice. This class only prepares,
/// explicitly arms, atomically claims, and records the SMTP outcome of a
/// Scheduling/Contact 1 action. It has no polling or dispatch facility.
/// </summary>
public static class BasicProductionSchedulingSupport
{
    public const string EventKey = "scheduling";
    public const string RecipientKey = "contact_1";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS public.basic_production_job_arms
            (
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                armed boolean NOT NULL DEFAULT false,
                disposable_confirmed boolean NOT NULL DEFAULT false,
                armed_by text NOT NULL DEFAULT '',
                version integer NOT NULL DEFAULT 1,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY(tenant_id,job_id)
            );

            CREATE TABLE IF NOT EXISTS public.basic_production_scheduling_actions
            (
                action_id uuid PRIMARY KEY,
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                approved_version integer NOT NULL,
                approved_fingerprint text NOT NULL,
                event_key text NOT NULL DEFAULT 'scheduling',
                recipient_key text NOT NULL DEFAULT 'contact_1',
                recipient_email text NOT NULL,
                recipient_name text NOT NULL DEFAULT '',
                template_id uuid NOT NULL,
                template_version integer NOT NULL,
                rendered_subject text NOT NULL,
                rendered_html text NOT NULL,
                state text NOT NULL DEFAULT 'awaiting_approval',
                content_fingerprint text NOT NULL,
                prepared_by text NOT NULL,
                approved_by text NOT NULL DEFAULT '',
                claimed_by text NOT NULL DEFAULT '',
                completed_by text NOT NULL DEFAULT '',
                provider_message_id text NOT NULL DEFAULT '',
                completion_error text NOT NULL DEFAULT '',
                prepared_at timestamptz NOT NULL DEFAULT NOW(),
                approved_at timestamptz NULL,
                claimed_at timestamptz NULL,
                completed_at timestamptz NULL,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_basic_prod_schedule_event CHECK(event_key='scheduling'),
                CONSTRAINT ck_basic_prod_schedule_recipient CHECK(recipient_key='contact_1'),
                CONSTRAINT ck_basic_prod_schedule_state CHECK(state IN
                    ('awaiting_approval','approved','sending','smtp_accepted','failed','reconciliation_required','cancelled')),
                UNIQUE(tenant_id,job_id,approved_version,event_key,recipient_key)
            );

            CREATE INDEX IF NOT EXISTS idx_basic_prod_schedule_job
                ON public.basic_production_scheduling_actions(tenant_id,job_id,prepared_at DESC);

            ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS basic_scheduling_started_at timestamptz NULL;
            UPDATE public.jobs_staging SET basic_scheduling_started_at=COALESCE(
                booking_email_last_attempt_at,terms_last_attempt_at,invoice_last_attempt_at,calendar_last_attempt_at,
                booking_email_sent_at,terms_sent_at,invoice_sent_at,calendar_created_at)
            WHERE basic_scheduling_started_at IS NULL AND (
                booking_email_sent OR terms_sent OR invoice_sent OR calendar_created OR
                booking_email_last_attempt_at IS NOT NULL OR terms_last_attempt_at IS NOT NULL OR
                invoice_last_attempt_at IS NOT NULL OR calendar_last_attempt_at IS NOT NULL);

            ALTER TABLE public.basic_production_scheduling_actions DROP CONSTRAINT IF EXISTS ck_basic_prod_schedule_state;
            ALTER TABLE public.basic_production_scheduling_actions ADD CONSTRAINT ck_basic_prod_schedule_state CHECK(state IN
                ('awaiting_approval','approved','sending','smtp_accepted','failed','reconciliation_required','cancelled'));
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<BasicProductionArmResult> SetArmAsync(NpgsqlConnection connection,
        BasicProductionArmCommand request, CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (request.Armed && (!request.Confirmed || !request.DisposableConfirmed))
            return new("confirmation_required", false, 0, "Explicit disposable-job confirmation is required.");

        await EnsureAsync(connection, cancellationToken);
        await RequireBasicEntitlementAsync(connection, request.TenantId);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireEligibleJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);

        bool currentArmed = false;
        bool currentDisposable = false;
        var version = 0;
        await using (var select = new NpgsqlCommand("""
            SELECT armed,disposable_confirmed,version
            FROM public.basic_production_job_arms
            WHERE tenant_id=@tenant AND job_id=@job FOR UPDATE;
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("tenant", request.TenantId);
            select.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                currentArmed = reader.GetBoolean(0);
                currentDisposable = reader.GetBoolean(1);
                version = reader.GetInt32(2);
            }
        }

        var desiredDisposable = request.Armed && request.DisposableConfirmed;
        if (currentArmed == request.Armed && currentDisposable == desiredDisposable && version > 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return new("replayed", currentArmed, version, "The production test arm is already current.");
        }
        if (version != request.ExpectedVersion)
        {
            await transaction.CommitAsync(cancellationToken);
            return new("conflict", currentArmed, version, "The production test arm changed. Reload and try again.");
        }

        var newVersion = version + 1;
        await using (var upsert = new NpgsqlCommand("""
            INSERT INTO public.basic_production_job_arms
                (tenant_id,job_id,armed,disposable_confirmed,armed_by,version,updated_at)
            VALUES(@tenant,@job,@armed,@disposable,@actor,@version,NOW())
            ON CONFLICT(tenant_id,job_id) DO UPDATE SET
                armed=EXCLUDED.armed,disposable_confirmed=EXCLUDED.disposable_confirmed,
                armed_by=EXCLUDED.armed_by,version=EXCLUDED.version,updated_at=NOW();
            """, connection, transaction))
        {
            upsert.Parameters.AddWithValue("tenant", request.TenantId);
            upsert.Parameters.AddWithValue("job", request.JobId);
            upsert.Parameters.AddWithValue("armed", request.Armed);
            upsert.Parameters.AddWithValue("disposable", desiredDisposable);
            upsert.Parameters.AddWithValue("actor", request.Actor.Trim());
            upsert.Parameters.AddWithValue("version", newVersion);
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }
        if (!request.Armed)
        {
            await using var cancel = new NpgsqlCommand("""
                UPDATE public.basic_production_scheduling_actions
                SET state='cancelled',updated_at=NOW()
                WHERE tenant_id=@tenant AND job_id=@job
                  AND state IN ('awaiting_approval','approved');
                """, connection, transaction);
            cancel.Parameters.AddWithValue("tenant", request.TenantId);
            cancel.Parameters.AddWithValue("job", request.JobId);
            await cancel.ExecuteNonQueryAsync(cancellationToken);
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, "basic_production_arm",
            currentArmed.ToString().ToLowerInvariant(), request.Armed.ToString().ToLowerInvariant(), request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("saved", request.Armed, newVersion, request.Armed
            ? "Disposable job armed for one explicitly approved Scheduling/Client send."
            : "Production test sending disarmed.");
    }

    public static async Task<BasicProductionActionResult> PrepareAsync(NpgsqlConnection connection,
        BasicProductionPrepareCommand request, CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (!request.Confirmed) return Unavailable("confirmation_required", "Explicit preparation confirmation is required.");
        if (string.IsNullOrWhiteSpace(request.RenderedSubject) || string.IsNullOrWhiteSpace(request.RenderedHtml))
            return Unavailable("render_required", "A rendered approved template is required.");

        await EnsureAsync(connection, cancellationToken);
        await RequireBasicEntitlementAsync(connection, request.TenantId);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireEligibleJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);

        const string gateSql = """
            SELECT j.approved_snapshot_version,COALESCE(j.approved_snapshot_fingerprint,''),
                   j.change_review_pending,j.unscheduled,j.booking_email_required,j.booking_email_sent,
                   COALESCE(j.contact1_email,''),
                   TRIM(CONCAT_WS(' ',j.contact1_first_name,j.contact1_last_name)),
                   s.enabled,s.template_id,COALESCE(t.template_version,1),
                   COALESCE(a.armed,false),COALESCE(a.disposable_confirmed,false)
            FROM public.jobs_staging j
            JOIN public.basic_automation_settings s ON s.tenant_id=@tenant
                AND s.event_key='scheduling' AND s.recipient_key='contact_1'
            LEFT JOIN public.email_templates t ON t.tenant_id=@tenant AND t.template_id=s.template_id
                AND t.archived_at IS NULL
            LEFT JOIN public.basic_production_job_arms a ON a.tenant_id=@tenant AND a.job_id=j.job_id
            WHERE j.job_id=@job AND j.tenant_id::text=@tenant_text
            FOR UPDATE OF j;
            """;
        int approvedVersion;
        string approvedFingerprint, email, name;
        bool changePending, unscheduled, bookingRequired, bookingSent, enabled, armed, disposable;
        Guid? templateId;
        int templateVersion;
        await using (var gate = new NpgsqlCommand(gateSql, connection, transaction))
        {
            gate.Parameters.AddWithValue("tenant", request.TenantId);
            gate.Parameters.AddWithValue("job", request.JobId);
            gate.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
            await using var reader = await gate.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) return await RollbackAsync(transaction, "not_available", "The owned Basic slot or job is unavailable.");
            approvedVersion = reader.GetInt32(0);
            approvedFingerprint = reader.GetString(1);
            changePending = reader.GetBoolean(2);
            unscheduled = reader.GetBoolean(3);
            bookingRequired = reader.GetBoolean(4);
            bookingSent = reader.GetBoolean(5);
            email = reader.GetString(6).Trim();
            name = reader.GetString(7).Trim();
            enabled = reader.GetBoolean(8);
            templateId = reader.IsDBNull(9) ? null : reader.GetGuid(9);
            templateVersion = reader.GetInt32(10);
            armed = reader.GetBoolean(11);
            disposable = reader.GetBoolean(12);
        }
        if (changePending) return await RollbackAsync(transaction, "change_review_pending", "Approve or reject the current THREED changes first.");
        if (unscheduled) return await RollbackAsync(transaction, "job_unscheduled", "The job is unscheduled.");
        if (!bookingRequired) return await RollbackAsync(transaction, "not_required", "A Booking Email is not required for this job.");
        if (bookingSent) return await RollbackAsync(transaction, "already_sent", "The Booking Email is already complete.");
        if (approvedVersion < 1 || string.IsNullOrWhiteSpace(approvedFingerprint)) return await RollbackAsync(transaction, "approved_snapshot_required", "No approved automation snapshot exists.");
        if (!enabled) return await RollbackAsync(transaction, "slot_disabled", "Scheduling / Client is disabled.");
        if (!templateId.HasValue) return await RollbackAsync(transaction, "template_required", "Save the Scheduling / Client template first.");
        if (!LooksLikeEmail(email)) return await RollbackAsync(transaction, "recipient_required", "Contact 1 has no valid email address.");

        var contentFingerprint = Hash($"{request.TenantId:N}|{request.JobId:N}|{approvedVersion}|{approvedFingerprint}|{templateId:N}|{templateVersion}|{email.ToLowerInvariant()}|{Hash(request.RenderedSubject)}|{Hash(request.RenderedHtml)}");
        var actionId = Guid.NewGuid();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO public.basic_production_scheduling_actions
                (action_id,tenant_id,job_id,approved_version,approved_fingerprint,recipient_email,recipient_name,
                 template_id,template_version,rendered_subject,rendered_html,content_fingerprint,prepared_by)
            VALUES(@id,@tenant,@job,@version,@approved,@email,@name,@template,@template_version,@subject,@html,@fingerprint,@actor)
            ON CONFLICT(tenant_id,job_id,approved_version,event_key,recipient_key) DO NOTHING
            RETURNING action_id;
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue("id", actionId);
            insert.Parameters.AddWithValue("tenant", request.TenantId);
            insert.Parameters.AddWithValue("job", request.JobId);
            insert.Parameters.AddWithValue("version", approvedVersion);
            insert.Parameters.AddWithValue("approved", approvedFingerprint);
            insert.Parameters.AddWithValue("email", email);
            insert.Parameters.AddWithValue("name", name);
            insert.Parameters.AddWithValue("template", templateId.Value);
            insert.Parameters.AddWithValue("template_version", templateVersion);
            insert.Parameters.AddWithValue("subject", request.RenderedSubject.Trim());
            insert.Parameters.AddWithValue("html", request.RenderedHtml);
            insert.Parameters.AddWithValue("fingerprint", contentFingerprint);
            insert.Parameters.AddWithValue("actor", request.Actor.Trim());
            var inserted = await insert.ExecuteScalarAsync(cancellationToken);
            if (inserted is not Guid)
            {
                await using var existing = new NpgsqlCommand("""
                    SELECT action_id,state,content_fingerprint
                    FROM public.basic_production_scheduling_actions
                    WHERE tenant_id=@tenant AND job_id=@job AND approved_version=@version
                      AND event_key='scheduling' AND recipient_key='contact_1';
                    """, connection, transaction);
                existing.Parameters.AddWithValue("tenant", request.TenantId);
                existing.Parameters.AddWithValue("job", request.JobId);
                existing.Parameters.AddWithValue("version", approvedVersion);
                await using var reader = await existing.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                var existingId = reader.GetGuid(0);
                var state = reader.GetString(1);
                var fingerprint = reader.GetString(2);
                await transaction.CommitAsync(cancellationToken);
                return fingerprint == contentFingerprint
                    ? new("replayed", existingId, state, true, "The same production action already exists.")
                    : new("revision_conflict", existingId, state, false, "This approved revision already has different frozen content.");
            }
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, "basic_production_prepared", "", actionId.ToString(), request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("prepared", actionId, "awaiting_approval", false, "Prepared for explicit review. Nothing has been sent.");
    }

    public static Task<BasicProductionActionResult> ApproveAsync(NpgsqlConnection connection,
        BasicProductionTransitionCommand request, CancellationToken cancellationToken = default) =>
        TransitionAsync(connection, request, "awaiting_approval", "approved", "basic_production_approved", cancellationToken);

    public static Task<BasicProductionActionResult> ClaimAsync(NpgsqlConnection connection,
        BasicProductionTransitionCommand request, CancellationToken cancellationToken = default) =>
        TransitionAsync(connection, request, "approved", "sending", "basic_production_claimed", cancellationToken);

    /// <summary>
    /// Acquires the one-time claim and returns only the frozen Basic slot content.
    /// The endpoint integrating this method may subsequently add a system-managed
    /// Client Engagement footer, but must never re-render a legacy booking template.
    /// </summary>
    public static async Task<BasicProductionDeliveryClaimResult> ClaimForDeliveryAsync(NpgsqlConnection connection,
        BasicProductionTransitionCommand request, CancellationToken cancellationToken = default)
    {
        var claimed = await ClaimAsync(connection, request, cancellationToken);
        if (claimed.State != "sending")
            return new(claimed.Status, claimed.ActionId, claimed.State, "", "", "", null, claimed.Message);
        await using var command = new NpgsqlCommand("""
            SELECT recipient_email,rendered_subject,rendered_html
            FROM public.basic_production_scheduling_actions
            WHERE action_id=@id AND tenant_id=@tenant AND job_id=@job AND state='sending';
            """, connection);
        command.Parameters.AddWithValue("id", request.ActionId);
        command.Parameters.AddWithValue("tenant", request.TenantId);
        command.Parameters.AddWithValue("job", request.JobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return new("claim_missing", request.ActionId, "unavailable", "", "", "", null,
                "The claimed action could not be reloaded. Do not retry automatically.");
        return new("claimed", request.ActionId, "sending", reader.GetString(0), reader.GetString(1),
            reader.GetString(2), null, "One-time claim acquired with frozen Basic Scheduling / Client content.");
    }

    public static async Task<BasicProductionActionResult> CompleteAsync(NpgsqlConnection connection,
        BasicProductionCompleteCommand request, CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (!request.Confirmed) return Unavailable("confirmation_required", "Explicit SMTP outcome confirmation is required.", request.ActionId, "sending");
        await EnsureAsync(connection, cancellationToken);
        await RequireBasicEntitlementAsync(connection, request.TenantId);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireEligibleJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        var outcome = (request.Outcome ?? "").Trim().ToLowerInvariant();
        if (outcome is not ("accepted" or "failed" or "unknown"))
            return Unavailable("invalid_outcome", "Outcome must be accepted, failed, or unknown.", request.ActionId, "sending");
        var accepted = outcome == "accepted";
        var target = accepted ? "smtp_accepted" : outcome == "unknown" ? "reconciliation_required" : "failed";
        await using (var update = new NpgsqlCommand("""
            UPDATE public.basic_production_scheduling_actions
            SET state=@target,provider_message_id=@message,completion_error=@error,
                completed_by=@actor,completed_at=NOW(),updated_at=NOW()
            WHERE action_id=@id AND tenant_id=@tenant AND job_id=@job AND state='sending';
            """, connection, transaction))
        {
            update.Parameters.AddWithValue("target", target);
            update.Parameters.AddWithValue("message", request.ProviderMessageId?.Trim() ?? "");
            update.Parameters.AddWithValue("error", accepted ? "" : (request.Error?.Trim() ??
                (outcome == "unknown" ? "SMTP outcome unknown; manual reconciliation required." : "SMTP delivery failed.")));
            update.Parameters.AddWithValue("actor", request.Actor.Trim());
            update.Parameters.AddWithValue("id", request.ActionId);
            update.Parameters.AddWithValue("tenant", request.TenantId);
            update.Parameters.AddWithValue("job", request.JobId);
            if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
                return await RollbackAsync(transaction, "not_claimed", "The action is not in its one-time sending claim.", request.ActionId);
        }
        if (accepted)
        {
            await using var completeJob = new NpgsqlCommand("""
                UPDATE public.jobs_staging
                SET booking_email_sent=true,booking_email_sent_at=NOW(),booking_email_retry_requested=false,
                    booking_email_retry_requested_at=NULL,booking_email_last_attempt_at=NOW(),booking_email_last_error=NULL
                WHERE job_id=@job AND tenant_id::text=@tenant_text AND change_review_pending=false AND unscheduled=false
                  AND EXISTS(SELECT 1 FROM public.basic_production_scheduling_actions a
                      WHERE a.action_id=@action AND a.tenant_id=@tenant AND a.job_id=jobs_staging.job_id
                        AND a.approved_version=jobs_staging.approved_snapshot_version
                        AND a.approved_fingerprint=COALESCE(jobs_staging.approved_snapshot_fingerprint,''));
                """, connection, transaction);
            completeJob.Parameters.AddWithValue("job", request.JobId);
            completeJob.Parameters.AddWithValue("action", request.ActionId);
            completeJob.Parameters.AddWithValue("tenant", request.TenantId);
            completeJob.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
            if (await completeJob.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                await using var reconcile = new NpgsqlCommand("""
                    UPDATE public.basic_production_scheduling_actions
                    SET state='reconciliation_required',completion_error='SMTP accepted but job state changed before workflow completion.',updated_at=NOW()
                    WHERE action_id=@id AND tenant_id=@tenant AND job_id=@job AND state='smtp_accepted';
                    """, connection, transaction);
                reconcile.Parameters.AddWithValue("id", request.ActionId);
                reconcile.Parameters.AddWithValue("tenant", request.TenantId);
                reconcile.Parameters.AddWithValue("job", request.JobId);
                await reconcile.ExecuteNonQueryAsync(cancellationToken);
                await AuditAsync(connection, transaction, request.TenantId, request.JobId,
                    "basic_production_reconciliation_required", "sending", "reconciliation_required", request.Actor, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return new("reconciliation_required", request.ActionId, "reconciliation_required", false,
                    "SMTP was accepted but the job changed before completion. Do not resend; reconcile manually.");
            }
        }
        else
        {
            await using var failJob = new NpgsqlCommand("""
                UPDATE public.jobs_staging SET booking_email_last_attempt_at=NOW(),booking_email_last_error=@error
                WHERE job_id=@job AND tenant_id::text=@tenant_text;
                """, connection, transaction);
            failJob.Parameters.AddWithValue("error", request.Error?.Trim() ??
                (outcome == "unknown" ? "SMTP outcome unknown; manual reconciliation required." : "SMTP delivery failed."));
            failJob.Parameters.AddWithValue("job", request.JobId);
            failJob.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
            await failJob.ExecuteNonQueryAsync(cancellationToken);
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, $"basic_production_{target}", "sending", target, request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(target, request.ActionId, target, false, accepted ? "SMTP accepted; Booking Email completed."
            : outcome == "unknown" ? "SMTP outcome unknown; action is fail-closed and requires manual reconciliation."
            : "SMTP failed; Booking Email remains incomplete.");
    }

    public static async Task<IReadOnlyList<BasicProductionQueueItem>> LoadAsync(NpgsqlConnection connection,
        Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        if (!await AutomationFoundationSupport.JobBelongsToTenantAsync(connection, tenantId, jobId))
            throw new UnauthorizedAccessException("The job does not belong to this tenant.");
        await EnsureAsync(connection, cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT action_id,approved_version,approved_fingerprint,recipient_email,recipient_name,template_id,
                   template_version,rendered_subject,rendered_html,state,provider_message_id,completion_error,
                   prepared_at,approved_at,claimed_at,completed_at
            FROM public.basic_production_scheduling_actions
            WHERE tenant_id=@tenant AND job_id=@job ORDER BY prepared_at DESC LIMIT 20;
            """, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<BasicProductionQueueItem>();
        while (await reader.ReadAsync(cancellationToken))
            rows.Add(new(reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
                reader.GetGuid(5), reader.GetInt32(6), reader.GetString(7), reader.GetString(8), reader.GetString(9),
                reader.GetString(10), reader.GetString(11), reader.GetDateTime(12), ReadDate(reader, 13), ReadDate(reader, 14), ReadDate(reader, 15)));
        return rows;
    }

    public static async Task<BasicProductionStatus> LoadStatusAsync(NpgsqlConnection connection,
        Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        await EnsureAsync(connection, cancellationToken);
        await RequireBasicEntitlementAsync(connection, tenantId);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireEligibleJobAsync(connection, transaction, tenantId, jobId, cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT COALESCE(a.armed,false),COALESCE(a.disposable_confirmed,false),COALESCE(a.version,0),
                   j.approved_snapshot_version,COALESCE(j.approved_snapshot_fingerprint,''),
                   COALESCE(j.contact1_email,''),
                   TRIM(CONCAT_WS(' ',j.contact1_first_name,j.contact1_last_name)),
                   s.enabled,s.template_id IS NOT NULL AND t.template_id IS NOT NULL,
                   j.booking_email_required,j.booking_email_sent,j.change_review_pending,j.unscheduled
            FROM public.jobs_staging j
            JOIN public.basic_automation_settings s ON s.tenant_id=@tenant
                AND s.event_key='scheduling' AND s.recipient_key='contact_1'
            LEFT JOIN public.email_templates t ON t.tenant_id=@tenant AND t.template_id=s.template_id AND t.archived_at IS NULL
            LEFT JOIN public.basic_production_job_arms a ON a.tenant_id=@tenant AND a.job_id=j.job_id
            WHERE j.job_id=@job AND j.tenant_id::text=@tenant_text;
            """, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = tenantId.ToString();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Scheduling / Client configuration is unavailable.");
        // Basic scheduling is a normal product workflow. The legacy disposable-job
        // arm remains readable for compatibility but is no longer a customer gate.
        var status = new BasicProductionStatus(true, true, Math.Max(1, reader.GetInt32(2)),
            reader.GetInt32(3), reader.GetString(4), LooksLikeEmail(reader.GetString(5).Trim()), reader.GetString(6),
            reader.GetString(5).Trim(), reader.GetBoolean(7), reader.GetBoolean(8), reader.GetBoolean(9),
            reader.GetBoolean(10), reader.GetBoolean(11), reader.GetBoolean(12), Array.Empty<BasicProductionQueueItem>());
        await reader.DisposeAsync();
        await transaction.CommitAsync(cancellationToken);
        return status with { Actions = await LoadAsync(connection, tenantId, jobId, cancellationToken) };
    }

    private static async Task<BasicProductionActionResult> TransitionAsync(NpgsqlConnection connection,
        BasicProductionTransitionCommand request, string expected, string target, string auditKey, CancellationToken cancellationToken)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (!request.Confirmed) return Unavailable("confirmation_required", "Explicit confirmation is required.", request.ActionId, expected);
        await EnsureAsync(connection, cancellationToken);
        await RequireBasicEntitlementAsync(connection, request.TenantId);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await RequireEligibleJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        var actorColumn = target == "approved" ? "approved_by" : "claimed_by";
        var timeColumn = target == "approved" ? "approved_at" : "claimed_at";
        var sql = $"""
            UPDATE public.basic_production_scheduling_actions
            SET state=@target,{actorColumn}=@actor,{timeColumn}=NOW(),updated_at=NOW()
            WHERE action_id=@id AND tenant_id=@tenant AND job_id=@job AND state=@expected
              AND EXISTS(SELECT 1 FROM public.jobs_staging j WHERE j.job_id=@job
                  AND j.tenant_id::text=@tenant_text AND NOT j.change_review_pending AND NOT j.unscheduled
                  AND j.approved_snapshot_version=basic_production_scheduling_actions.approved_version
                  AND COALESCE(j.approved_snapshot_fingerprint,'')=basic_production_scheduling_actions.approved_fingerprint)
              ;
            """;
        await using var update = new NpgsqlCommand(sql, connection, transaction);
        update.Parameters.AddWithValue("target", target);
        update.Parameters.AddWithValue("actor", request.Actor.Trim());
        update.Parameters.AddWithValue("id", request.ActionId);
        update.Parameters.AddWithValue("tenant", request.TenantId);
        update.Parameters.AddWithValue("job", request.JobId);
        update.Parameters.AddWithValue("expected", expected);
        update.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
        if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
            return await RollbackAsync(transaction, "transition_rejected", "The action was already used or the safety gate changed.", request.ActionId);
        if (target == "sending")
        {
            await using var attention = new NpgsqlCommand("""
                UPDATE public.jobs_staging
                SET booking_email_last_attempt_at=NOW(),
                    booking_email_last_error='Basic Scheduling delivery claimed; awaiting local SMTP outcome.'
                WHERE job_id=@job AND tenant_id::text=@tenant_text;
                """, connection, transaction);
            attention.Parameters.AddWithValue("job", request.JobId);
            attention.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
            await attention.ExecuteNonQueryAsync(cancellationToken);
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, auditKey, expected, target, request.Actor, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(target, request.ActionId, target, false, target == "sending" ? "One-time SMTP claim acquired." : "Action explicitly approved; nothing sent yet.");
    }

    private static async Task RequireEligibleJobAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid tenantId, Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            SELECT COALESCE((SELECT activation_mode FROM public.automation_tenant_settings WHERE tenant_id=@tenant),'selected_jobs'),
                   COALESCE((SELECT use_advanced_workflows FROM public.automation_job_selections WHERE tenant_id=@tenant AND job_id=@job),false)
            WHERE EXISTS(SELECT 1 FROM public.jobs_staging WHERE job_id=@job AND tenant_id::text=@tenant_text);
            """, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = tenantId.ToString();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new UnauthorizedAccessException("The job does not belong to this tenant.");
        if (reader.GetString(0) == "all_jobs" || reader.GetBoolean(1)) throw new InvalidOperationException("This job uses Advanced Workflows.");
    }

    private static async Task RequireBasicEntitlementAsync(NpgsqlConnection connection, Guid tenantId)
    {
        var entitlement = await AutomationFoundationSupport.LoadEntitlementAsync(connection, tenantId);
        if (!entitlement.Allowed || !entitlement.BasicAutomation)
            throw new UnauthorizedAccessException("An active Basic Automation entitlement is required.");
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

    private static BasicProductionActionResult Unavailable(string status, string message, Guid? actionId = null, string state = "unavailable") =>
        new(status, actionId, state, false, message);

    private static async Task<BasicProductionActionResult> RollbackAsync(NpgsqlTransaction transaction, string status,
        string message, Guid? actionId = null)
    {
        await transaction.RollbackAsync();
        return Unavailable(status, message, actionId);
    }

    private static void ValidateIdentity(Guid tenantId, Guid jobId, string actor)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        if (string.IsNullOrWhiteSpace(actor)) throw new ArgumentException("An authenticated actor is required.");
    }

    private static bool LooksLikeEmail(string value) => value.Contains('@', StringComparison.Ordinal) && !value.Any(char.IsWhiteSpace);
    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static DateTime? ReadDate(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);
}

public sealed record BasicProductionArmCommand(Guid TenantId, Guid JobId, bool Armed, bool DisposableConfirmed,
    bool Confirmed, int ExpectedVersion, string Actor);
public sealed record BasicProductionArmResult(string Status, bool Armed, int Version, string Message);
public sealed record BasicProductionPrepareCommand(Guid TenantId, Guid JobId, string RenderedSubject,
    string RenderedHtml, bool Confirmed, string Actor);
public sealed record BasicProductionTransitionCommand(Guid TenantId, Guid JobId, Guid ActionId, bool Confirmed, string Actor);
public sealed record BasicProductionCompleteCommand(Guid TenantId, Guid JobId, Guid ActionId, bool Confirmed,
    string Outcome, string? ProviderMessageId, string? Error, string Actor);
public sealed record BasicProductionActionResult(string Status, Guid? ActionId, string State, bool Replayed, string Message);
public sealed record BasicProductionQueueItem(Guid ActionId, int ApprovedVersion, string ApprovedFingerprint,
    string RecipientEmail, string RecipientName, Guid TemplateId, int TemplateVersion, string RenderedSubject,
    string RenderedHtml, string State, string ProviderMessageId, string CompletionError, DateTime PreparedAt,
    DateTime? ApprovedAt, DateTime? ClaimedAt, DateTime? CompletedAt);
public sealed record BasicProductionDeliveryClaimResult(string Status, Guid? ActionId, string State,
    string ToEmail, string Subject, string HtmlBody, Guid? CommunicationId, string Message);
public sealed record BasicProductionStatus(bool Armed, bool DisposableConfirmed, int ArmVersion,
    int ApprovedVersion, string ApprovedFingerprint, bool RecipientAvailable, string RecipientName,
    string RecipientEmail, bool SlotEnabled, bool TemplateSaved, bool BookingEmailRequired,
    bool BookingEmailSent, bool ChangeReviewPending, bool Unscheduled,
    IReadOnlyList<BasicProductionQueueItem> Actions);
