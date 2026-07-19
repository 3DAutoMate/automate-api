using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace AutoMateApi;

/// <summary>
/// Evidence-preserving lifecycle for explicitly designated disposable jobs.
/// This component never calls SMTP, SignNow, Google, Xero, or any other external service.
/// External effects must be reconciled before a new internal test cycle is activated.
/// </summary>
public static class ControlledTestCycleSupport
{
    public static readonly Guid FullRetestJobId = Guid.Parse("e06cb16f-8355-4ecd-9d0d-aaf046d3c7b2");
    private static readonly HashSet<string> Resolutions = new(StringComparer.Ordinal)
    {
        "acknowledge_retain", "confirmed_absent", "cancelled_unsigned", "removed", "failed_confirmed"
    };

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        const string sql = """
        CREATE TABLE IF NOT EXISTS public.controlled_test_jobs
        (
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            enabled boolean NOT NULL DEFAULT false,
            designation_version integer NOT NULL DEFAULT 1,
            reason text NOT NULL DEFAULT '',
            designated_by text NOT NULL DEFAULT '',
            designated_at timestamptz NOT NULL DEFAULT NOW(),
            disabled_by text NOT NULL DEFAULT '',
            disabled_at timestamptz NULL,
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            PRIMARY KEY(tenant_id,job_id)
        );

        CREATE TABLE IF NOT EXISTS public.controlled_test_cycle_commands
        (
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            command_type text NOT NULL,
            idempotency_key text NOT NULL,
            request_hash text NOT NULL,
            result_json jsonb NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            completed_at timestamptz NULL,
            PRIMARY KEY(tenant_id,job_id,command_type,idempotency_key)
        );

        CREATE TABLE IF NOT EXISTS public.job_test_cycles
        (
            cycle_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            cycle_number integer NOT NULL,
            status text NOT NULL,
            designation_version integer NOT NULL,
            prior_approved_version integer NOT NULL,
            approved_version integer NULL,
            approved_fingerprint text NOT NULL DEFAULT '',
            reconciliation_fingerprint text NOT NULL,
            reconciliation_json jsonb NOT NULL,
            reason text NOT NULL,
            xero_policy text NOT NULL DEFAULT 'retain_existing',
            prepared_by text NOT NULL,
            prepared_at timestamptz NOT NULL DEFAULT NOW(),
            started_by text NOT NULL DEFAULT '',
            started_at timestamptz NULL,
            superseded_at timestamptz NULL,
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            CONSTRAINT ck_job_test_cycle_status CHECK(status IN ('blocked','ready','active','superseded')),
            UNIQUE(tenant_id,job_id,cycle_number)
        );

        ALTER TABLE public.job_test_cycles ADD COLUMN IF NOT EXISTS xero_policy text NOT NULL DEFAULT 'retain_existing';
        ALTER TABLE public.job_test_cycles ADD COLUMN IF NOT EXISTS full_retest boolean NOT NULL DEFAULT false;

        CREATE INDEX IF NOT EXISTS idx_job_test_cycles_job
            ON public.job_test_cycles(tenant_id,job_id,cycle_number DESC);

        CREATE TABLE IF NOT EXISTS public.job_test_cycle_reconciliation_items
        (
            item_id uuid PRIMARY KEY,
            cycle_id uuid NOT NULL REFERENCES public.job_test_cycles(cycle_id),
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            effect_type text NOT NULL,
            internal_record_id text NOT NULL DEFAULT '',
            external_id text NOT NULL DEFAULT '',
            observed_status text NOT NULL DEFAULT '',
            evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            blocking boolean NOT NULL DEFAULT false,
            uncertain boolean NOT NULL DEFAULT false,
            reset_allowed boolean NOT NULL DEFAULT false,
            resolution text NOT NULL DEFAULT '',
            resolution_note text NOT NULL DEFAULT '',
            resolved_by text NOT NULL DEFAULT '',
            resolved_at timestamptz NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            UNIQUE(cycle_id,effect_type,internal_record_id,external_id)
        );

        CREATE TABLE IF NOT EXISTS public.controlled_test_external_evidence
        (
            evidence_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            cycle_id uuid NOT NULL REFERENCES public.job_test_cycles(cycle_id),
            effect_type text NOT NULL,
            internal_record_id text NOT NULL DEFAULT '',
            external_id text NOT NULL DEFAULT '',
            status text NOT NULL DEFAULT '',
            evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            captured_at timestamptz NOT NULL DEFAULT NOW(),
            UNIQUE(cycle_id,effect_type,internal_record_id,external_id)
        );

        CREATE TABLE IF NOT EXISTS public.controlled_test_cycle_audit
        (
            audit_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            cycle_id uuid NULL,
            event_type text NOT NULL,
            actor text NOT NULL,
            reason text NOT NULL DEFAULT '',
            details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT NOW()
        );

        -- Calendar identity was previously transient. This table preserves discovered/created IDs
        -- without requiring the current job row to forget prior-cycle evidence.
        CREATE TABLE IF NOT EXISTS public.job_calendar_evidence
        (
            evidence_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            provider text NOT NULL DEFAULT 'google',
            calendar_id text NOT NULL DEFAULT '',
            event_id text NOT NULL,
            event_status text NOT NULL DEFAULT 'active',
            first_observed_at timestamptz NOT NULL DEFAULT NOW(),
            last_observed_at timestamptz NOT NULL DEFAULT NOW(),
            removed_at timestamptz NULL,
            metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(tenant_id,job_id,provider,calendar_id,event_id)
        );

        CREATE TABLE IF NOT EXISTS public.job_calendar_discoveries
        (
            discovery_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            status text NOT NULL,
            match_count integer NOT NULL,
            event_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
            discovered_by text NOT NULL,
            discovered_at timestamptz NOT NULL DEFAULT NOW(),
            CONSTRAINT ck_calendar_discovery_status CHECK(status IN ('absent','retained','duplicate'))
        );
        CREATE INDEX IF NOT EXISTS idx_calendar_discoveries_job
            ON public.job_calendar_discoveries(tenant_id,job_id,discovered_at DESC);
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await RecoverAccidentalCycleThreeAsync(connection, cancellationToken);
        await BasicProductionSchedulingSupport.EnsureAsync(connection, cancellationToken);
        await ClientEngagementSupport.EnsureAsync(connection, cancellationToken);
    }

    public static async Task<ControlledTestDesignationResult> SetDesignationAsync(
        NpgsqlConnection connection, ControlledTestDesignationCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        if (!request.Confirmed || string.IsNullOrWhiteSpace(request.Reason))
            return new("confirmation_required", false, request.ExpectedVersion, false, "A reason and typed confirmation are required.");
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey) || request.IdempotencyKey.Length > 200)
            throw new ArgumentException("A bounded idempotency key is required.");
        await EnsureAsync(connection, cancellationToken);
        var requestHash = Hash(JsonSerializer.Serialize(new { request.Enabled, request.ExpectedVersion, reason = request.Reason.Trim() }));
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var replay = await ClaimCommandAsync(connection, transaction, request.TenantId, request.JobId,
            "designation", request.IdempotencyKey, requestHash, cancellationToken);
        if (replay is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            var stored = JsonSerializer.Deserialize<ControlledTestDesignationResult>(replay)
                         ?? throw new InvalidOperationException("Stored designation result is invalid.");
            return stored with { Status = "replayed", Replayed = true, Message = "The identical designation command was already completed." };
        }
        await RequireOwnedJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);

        var current = await LoadDesignationForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        if (current.Version != request.ExpectedVersion)
            throw new ControlledTestCycleException("designation_conflict", "The test designation changed. Reload and try again.");
        if (current.Version > 0 && current.Enabled == request.Enabled)
        {
            var unchanged = new ControlledTestDesignationResult("replayed", current.Enabled, current.Version, true, "The designation is already current.");
            await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "designation",
                request.IdempotencyKey, requestHash, unchanged, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return unchanged;
        }

        var version = current.Version + 1;
        await using (var command = new NpgsqlCommand("""
            INSERT INTO public.controlled_test_jobs
                (tenant_id,job_id,enabled,designation_version,reason,designated_by,designated_at,disabled_by,disabled_at,updated_at)
            VALUES(@tenant,@job,@enabled,@version,@reason,@actor,NOW(),CASE WHEN @enabled THEN '' ELSE @actor END,
                   CASE WHEN @enabled THEN NULL ELSE NOW() END,NOW())
            ON CONFLICT(tenant_id,job_id) DO UPDATE SET enabled=EXCLUDED.enabled,
                designation_version=EXCLUDED.designation_version,reason=EXCLUDED.reason,
                designated_by=CASE WHEN EXCLUDED.enabled THEN EXCLUDED.designated_by ELSE controlled_test_jobs.designated_by END,
                designated_at=CASE WHEN EXCLUDED.enabled THEN NOW() ELSE controlled_test_jobs.designated_at END,
                disabled_by=CASE WHEN EXCLUDED.enabled THEN '' ELSE EXCLUDED.designated_by END,
                disabled_at=CASE WHEN EXCLUDED.enabled THEN NULL ELSE NOW() END,updated_at=NOW();
            """, connection, transaction))
        {
            command.Parameters.AddWithValue("tenant", request.TenantId);
            command.Parameters.AddWithValue("job", request.JobId);
            command.Parameters.AddWithValue("enabled", request.Enabled);
            command.Parameters.AddWithValue("version", version);
            command.Parameters.AddWithValue("reason", request.Reason.Trim());
            command.Parameters.AddWithValue("actor", request.Actor.Trim());
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, null,
            request.Enabled ? "test_job_designated" : "test_job_designation_removed", request.Actor, request.Reason,
            JsonSerializer.Serialize(new { version }), cancellationToken);
        var result = new ControlledTestDesignationResult("saved", request.Enabled, version, false, "Controlled test designation saved.");
        await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "designation",
            request.IdempotencyKey, requestHash, result, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public static async Task<ControlledTestBaselineAcceptResult> AcceptPendingBaselineAsync(
        NpgsqlConnection connection, ControlledTestBaselineAcceptCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        ValidateCommand(request.IdempotencyKey, request.Reason, request.Confirmed);
        await EnsureAsync(connection, cancellationToken);
        var requestHash = Hash(JsonSerializer.Serialize(new
        {
            request.ExpectedApprovedVersion, request.ExpectedPendingFingerprint, reason = request.Reason.Trim()
        }));
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var replay = await ClaimCommandAsync(connection, transaction, request.TenantId, request.JobId,
            "accept_baseline", request.IdempotencyKey, requestHash, cancellationToken);
        if (replay is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            var stored = JsonSerializer.Deserialize<ControlledTestBaselineAcceptResult>(replay)
                         ?? throw new InvalidOperationException("Stored baseline result is invalid.");
            return stored with { Replayed = true, Message = "The identical baseline command was already completed. External and workflow evidence remains preserved." };
        }
        var designation = await LoadDesignationForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        if (!designation.Enabled) throw new ControlledTestCycleException("test_job_required", "The controlled test designation is not active.");

        int approvedVersion; string pendingFingerprint; string currentSnapshot; string currentFingerprint;
        await using (var select = new NpgsqlCommand("""
            SELECT approved_snapshot_version,change_review_pending,COALESCE(pending_change_fingerprint,''),
                   COALESCE(current_snapshot_json::text,''),COALESCE(current_snapshot_fingerprint,'')
            FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job FOR UPDATE;
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("tenant", request.TenantId.ToString()); select.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new UnauthorizedAccessException("Job not found for this company.");
            approvedVersion = reader.GetInt32(0);
            if (!reader.GetBoolean(1)) throw new ControlledTestCycleException("no_pending_change", "There is no pending change to accept.");
            pendingFingerprint = reader.GetString(2); currentSnapshot = reader.GetString(3); currentFingerprint = reader.GetString(4);
        }
        if (approvedVersion != request.ExpectedApprovedVersion)
            throw new ControlledTestCycleException("approved_version_conflict", "The approved version changed. Reload and try again.");
        if (currentSnapshot.Length == 0 || currentFingerprint.Length == 0 ||
            !string.Equals(pendingFingerprint, request.ExpectedPendingFingerprint, StringComparison.Ordinal))
            throw new ControlledTestCycleException("pending_change_conflict", "The pending THREED change changed. Reload and review it again.");

        var newVersion = approvedVersion + 1;
        // Deliberately do not reference any workflow, payment, SignNow, Xero, report or Calendar field.
        await using (var update = new NpgsqlCommand("""
            UPDATE public.jobs_staging SET
                approved_snapshot_json=CAST(@snapshot AS jsonb),
                approved_snapshot_fingerprint=@fingerprint,
                approved_snapshot_version=@version,
                change_review_pending=false,pending_change_json=NULL,pending_change_fingerprint=NULL,
                pending_change_reasons=NULL,change_detected_at=NULL,address_change_pending=false,
                change_confirmed_at=NOW(),change_confirmed_by=@actor,workflow_updated_at=NOW()
            WHERE tenant_id::text=@tenant AND job_id=@job;
            """, connection, transaction))
        {
            update.Parameters.AddWithValue("snapshot", currentSnapshot); update.Parameters.AddWithValue("fingerprint", currentFingerprint);
            update.Parameters.AddWithValue("version", newVersion); update.Parameters.AddWithValue("actor", request.Actor.Trim());
            update.Parameters.AddWithValue("tenant", request.TenantId.ToString()); update.Parameters.AddWithValue("job", request.JobId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        var result = new ControlledTestBaselineAcceptResult("accepted", approvedVersion, newVersion, currentFingerprint, false,
            "Current THREED values became the controlled-test baseline. External and workflow evidence was preserved.");
        await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "accept_baseline",
            request.IdempotencyKey, requestHash, result, cancellationToken);
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, null, "controlled_test_baseline_accepted",
            request.Actor, request.Reason, JsonSerializer.Serialize(new { priorApprovedVersion = approvedVersion, approvedVersion = newVersion,
                pendingFingerprint, approvedFingerprint = currentFingerprint, preservedExternalEvidence = true }), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public static CalendarDiscoveryDecision EvaluateCalendarDiscovery(IReadOnlyList<CalendarDiscoveryEvent> events)
    {
        if (events.Count == 0) return new("absent", 0, null, false, "No existing AutoMate Calendar event was found.");
        if (events.Count > 1) return new("duplicate", events.Count, null, true, "Multiple Calendar events were found. Resolve duplicates manually before testing.");
        var item = events[0];
        if (string.IsNullOrWhiteSpace(item.EventId)) throw new ArgumentException("Calendar discovery returned an empty event ID.");
        return new("retained", 1, item, false, "The existing Calendar event was retained and recorded.");
    }

    public static async Task<CalendarDiscoveryResult> RetainCalendarDiscoveryAsync(
        NpgsqlConnection connection, ControlledCalendarDiscoveryCommand request,
        IReadOnlyList<CalendarDiscoveryEvent> events, CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        ValidateCommand(request.IdempotencyKey, request.Reason, request.Confirmed);
        await EnsureAsync(connection, cancellationToken);
        var decision = EvaluateCalendarDiscovery(events);
        var requestHash = Hash(JsonSerializer.Serialize(new { request.ExpectedApprovedVersion, request.Reason,
            events = events.Select(e => new { e.CalendarId, e.EventId, e.Status, e.HtmlLink }) }));
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var replay = await ClaimCommandAsync(connection, transaction, request.TenantId, request.JobId,
            "calendar_discovery", request.IdempotencyKey, requestHash, cancellationToken);
        if (replay is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            var stored = JsonSerializer.Deserialize<CalendarDiscoveryResult>(replay)
                         ?? throw new InvalidOperationException("Stored Calendar discovery is invalid.");
            return stored with { Replayed = true, Message = "The identical read-only Calendar discovery was already completed." };
        }
        var job = await LoadJobForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        var designation = await LoadDesignationForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        if (!designation.Enabled) throw new ControlledTestCycleException("test_job_required", "The controlled test designation is not active.");
        if (job.ApprovedVersion != request.ExpectedApprovedVersion)
            throw new ControlledTestCycleException("approved_version_conflict", "The approved version changed. Reload and discover again.");

        var discoveryId = Guid.NewGuid();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO public.job_calendar_discoveries(discovery_id,tenant_id,job_id,status,match_count,event_ids_json,discovered_by)
            VALUES(@id,@tenant,@job,@status,@count,CAST(@events AS jsonb),@actor);
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue("id", discoveryId); insert.Parameters.AddWithValue("tenant", request.TenantId);
            insert.Parameters.AddWithValue("job", request.JobId); insert.Parameters.AddWithValue("status", decision.Status);
            insert.Parameters.AddWithValue("count", decision.MatchCount); insert.Parameters.AddWithValue("events", JsonSerializer.Serialize(events));
            insert.Parameters.AddWithValue("actor", request.Actor.Trim()); await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        if (decision.Event is not null)
        {
            await using var retain = new NpgsqlCommand("""
                INSERT INTO public.job_calendar_evidence
                    (evidence_id,tenant_id,job_id,provider,calendar_id,event_id,event_status,metadata_json)
                VALUES(@id,@tenant,@job,'google',@calendar,@event,@status,CAST(@metadata AS jsonb))
                ON CONFLICT(tenant_id,job_id,provider,calendar_id,event_id) DO UPDATE SET
                    event_status=EXCLUDED.event_status,last_observed_at=NOW(),metadata_json=EXCLUDED.metadata_json;
                """, connection, transaction);
            retain.Parameters.AddWithValue("id", Guid.NewGuid()); retain.Parameters.AddWithValue("tenant", request.TenantId);
            retain.Parameters.AddWithValue("job", request.JobId); retain.Parameters.AddWithValue("calendar", decision.Event.CalendarId);
            retain.Parameters.AddWithValue("event", decision.Event.EventId); retain.Parameters.AddWithValue("status", decision.Event.Status);
            retain.Parameters.AddWithValue("metadata", JsonSerializer.Serialize(new { decision.Event.HtmlLink, retained = true }));
            await retain.ExecuteNonQueryAsync(cancellationToken);
        }
        var result = new CalendarDiscoveryResult(decision.Status, discoveryId, decision.MatchCount, decision.Event?.EventId ?? "", decision.Blocking, false, decision.Message);
        await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "calendar_discovery",
            request.IdempotencyKey, requestHash, result, cancellationToken);
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, null, "calendar_discovery_" + decision.Status,
            request.Actor, request.Reason, JsonSerializer.Serialize(new { discoveryId, decision.MatchCount, eventId = decision.Event?.EventId ?? "", externalMutations = 0 }), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public static async Task<ControlledTestReadiness> LoadReadinessAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        var job = await LoadJobForUpdateAsync(connection, transaction, tenantId, jobId, cancellationToken);
        var designation = await LoadDesignationForUpdateAsync(connection, transaction, tenantId, jobId, cancellationToken);
        var evidence = await CollectEvidenceAsync(connection, transaction, tenantId, jobId, cancellationToken);
        if (!job.MappingWorkflowReady)
            evidence = evidence.Append(Input("mapping", "jobs_staging", "", "invalid_or_stale", true, false, false, new { workflowReady = false })).ToArray();
        if (job.ChangeReviewPending || job.AddressChangePending)
            evidence = evidence.Append(Input("change_review", "jobs_staging", "", "pending", true, false, false,
                new { job.ChangeReviewPending, job.AddressChangePending })).ToArray();
        await transaction.CommitAsync(cancellationToken);
        return BuildReadiness(tenantId, jobId, designation, evidence);
    }

    public static async Task<ControlledTestState> LoadStateAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, int historyLimit = 20,
        CancellationToken cancellationToken = default)
    {
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        var job = await LoadJobForUpdateAsync(connection, transaction, tenantId, jobId, cancellationToken);
        var designation = await LoadDesignationForUpdateAsync(connection, transaction, tenantId, jobId, cancellationToken);
        var cycles = new List<ControlledTestCycleSummary>();
        await using (var command = new NpgsqlCommand("""
            SELECT cycle_id,cycle_number,status,prior_approved_version,approved_version,
                   reconciliation_fingerprint,reason,xero_policy,full_retest,prepared_by,prepared_at,
                   NULLIF(started_by,''),started_at
            FROM public.job_test_cycles WHERE tenant_id=@tenant AND job_id=@job
            ORDER BY cycle_number DESC LIMIT @limit;
            """, connection, transaction))
        {
            command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
            command.Parameters.AddWithValue("limit", Math.Clamp(historyLimit, 1, 50));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                cycles.Add(new(reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetInt32(3),
                    reader.IsDBNull(4) ? null : reader.GetInt32(4), reader.GetString(5), reader.GetString(6), reader.GetString(7), reader.GetBoolean(8),
                    reader.GetString(9), reader.GetFieldValue<DateTimeOffset>(10), reader.IsDBNull(11) ? "" : reader.GetString(11),
                    reader.IsDBNull(12) ? null : reader.GetFieldValue<DateTimeOffset>(12)));
        }
        ControlledTestCycleDetail? current = null;
        var currentCycle = cycles.FirstOrDefault(c => c.Status is "ready" or "blocked" or "active");
        if (currentCycle is not null)
        {
            var items = new List<ControlledTestEvidence>();
            await using var command = new NpgsqlCommand("""
                SELECT item_id,effect_type,internal_record_id,external_id,observed_status,blocking,uncertain,
                       reset_allowed,resolution,evidence_json::text
                FROM public.job_test_cycle_reconciliation_items WHERE cycle_id=@cycle
                ORDER BY blocking DESC,effect_type,created_at;
                """, connection, transaction);
            command.Parameters.AddWithValue("cycle", currentCycle.CycleId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) items.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2),
                reader.GetString(3), reader.GetString(4), reader.GetBoolean(5), reader.GetBoolean(6), reader.GetBoolean(7),
                reader.GetString(8), reader.GetString(9)));
            var blockers = items.Where(i => i.Blocking && string.IsNullOrWhiteSpace(i.Resolution))
                .Select(i => $"{i.EffectType}:{i.ObservedStatus}").Distinct().Order().ToArray();
            current = new(currentCycle, items, blockers, currentCycle.Status == "ready" && blockers.Length == 0);
        }
        await transaction.CommitAsync(cancellationToken);
        return new(tenantId, jobId, designation.Enabled, designation.Version, job.ApprovedVersion,
            job.ApprovedFingerprint, job.MappingWorkflowReady, job.ChangeReviewPending || job.AddressChangePending, job.PendingChangeFingerprint,
            current, cycles);
    }

    public static async Task<ControlledTestPrepareResult> PrepareAsync(
        NpgsqlConnection connection, ControlledTestPrepareCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        ValidateCommand(request.IdempotencyKey, request.Reason, request.Confirmed);
        await EnsureAsync(connection, cancellationToken);
        var requestHash = Hash(JsonSerializer.Serialize(new
        {
            request.ExpectedDesignationVersion, request.ExpectedApprovedVersion,
            reason = request.Reason.Trim(), request.XeroPolicy, request.FullRetest
        }));
        ValidateXeroPolicy(request.XeroPolicy);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var replay = await ClaimCommandAsync(connection, transaction, request.TenantId, request.JobId,
            "prepare", request.IdempotencyKey, requestHash, cancellationToken);
        if (replay is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            var stored = JsonSerializer.Deserialize<ControlledTestPrepareResult>(replay)
                         ?? throw new InvalidOperationException("Stored prepare result is invalid.");
            return stored with { Replayed = true };
        }
        var job = await LoadJobForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        var designation = await LoadDesignationForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        if (!designation.Enabled) throw new ControlledTestCycleException("test_job_required", "This job is not designated for controlled testing.");
        if (designation.Version != request.ExpectedDesignationVersion)
            throw new ControlledTestCycleException("designation_conflict", "The test designation changed. Reload and try again.");
        if (job.ApprovedVersion != request.ExpectedApprovedVersion)
            throw new ControlledTestCycleException("approved_version_conflict", "The approved job version changed. Reload and try again.");

        if (request.FullRetest)
        {
            if (request.JobId != FullRetestJobId)
                throw new ControlledTestCycleException("full_retest_job_not_allowed", "Full retest is limited to the allowlisted 6 Senecio job.");
            Guid existingCycleId; int existingCycleNumber;
            await using (var existing = new NpgsqlCommand("SELECT cycle_id,cycle_number FROM public.job_test_cycles WHERE tenant_id=@tenant AND job_id=@job AND status IN ('ready','blocked') ORDER BY cycle_number DESC LIMIT 1 FOR UPDATE", connection, transaction))
            {
                existing.Parameters.AddWithValue("tenant", request.TenantId); existing.Parameters.AddWithValue("job", request.JobId);
                await using var reader = await existing.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken)) throw new ControlledTestCycleException("full_retest_cycle_required", "Prepare cycle 2 before authorizing the full retest.");
                existingCycleId = reader.GetGuid(0); existingCycleNumber = reader.GetInt32(1);
            }
            if (existingCycleNumber is not (2 or 3)) throw new ControlledTestCycleException("full_retest_cycle_required", "Full retest is allowed only for the prepared 6 Senecio retest cycle.");
            await using (var retain = new NpgsqlCommand("UPDATE public.job_test_cycle_reconciliation_items SET blocking=false,reset_allowed=true,resolution='acknowledge_retain',resolution_note=@note,resolved_by=@actor,resolved_at=COALESCE(resolved_at,NOW()) WHERE cycle_id=@cycle AND effect_type='signnow' AND observed_status='signed'", connection, transaction))
            {
                retain.Parameters.AddWithValue("note", request.Reason.Trim()); retain.Parameters.AddWithValue("actor", request.Actor.Trim()); retain.Parameters.AddWithValue("cycle", existingCycleId);
                await retain.ExecuteNonQueryAsync(cancellationToken);
            }
            var blockers = await LoadUnresolvedBlockersAsync(connection, transaction, existingCycleId, cancellationToken);
            var upgradedFingerprint = await ComputeCycleFingerprintAsync(connection, transaction, existingCycleId, cancellationToken);
            await using (var upgrade = new NpgsqlCommand("UPDATE public.job_test_cycles SET full_retest=true,status=CASE WHEN @count=0 THEN 'ready' ELSE 'blocked' END,reconciliation_fingerprint=@fingerprint,reason=@reason,updated_at=NOW() WHERE cycle_id=@cycle", connection, transaction))
            {
                upgrade.Parameters.AddWithValue("count", blockers.Count); upgrade.Parameters.AddWithValue("fingerprint", upgradedFingerprint); upgrade.Parameters.AddWithValue("reason", request.Reason.Trim()); upgrade.Parameters.AddWithValue("cycle", existingCycleId);
                await upgrade.ExecuteNonQueryAsync(cancellationToken);
            }
            var items = await LoadCycleItemsAsync(connection, transaction, existingCycleId, cancellationToken);
            var upgraded = new ControlledTestPrepareResult(blockers.Count == 0 ? "ready" : "blocked", existingCycleId, existingCycleNumber, upgradedFingerprint, blockers, items, false, true);
            await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "prepare", request.IdempotencyKey, requestHash, upgraded, cancellationToken);
            await AuditAsync(connection, transaction, request.TenantId, request.JobId, existingCycleId, "full_retest_authorized", request.Actor, request.Reason, JsonSerializer.Serialize(new { cycleNumber = existingCycleNumber, duplicateOutputs = new[] { "email", "signnow", "xero", "calendar" }, blockers }), cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return upgraded;
        }

        Guid preparedCycleId; int preparedCycleNumber;
        await using (var prepared = new NpgsqlCommand("SELECT cycle_id,cycle_number FROM public.job_test_cycles WHERE tenant_id=@tenant AND job_id=@job AND status IN ('ready','blocked') ORDER BY cycle_number DESC LIMIT 1 FOR UPDATE", connection, transaction))
        {
            prepared.Parameters.AddWithValue("tenant", request.TenantId); prepared.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await prepared.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) { preparedCycleId = reader.GetGuid(0); preparedCycleNumber = reader.GetInt32(1); }
            else { preparedCycleId = Guid.Empty; preparedCycleNumber = 0; }
        }
        if (preparedCycleId != Guid.Empty)
        {
            var items = await LoadCycleItemsAsync(connection, transaction, preparedCycleId, cancellationToken);
            var blockers = await LoadUnresolvedBlockersAsync(connection, transaction, preparedCycleId, cancellationToken);
            var refreshedFingerprint = await ComputeCycleFingerprintAsync(connection, transaction, preparedCycleId, cancellationToken);
            await using (var refresh = new NpgsqlCommand("UPDATE public.job_test_cycles SET status=CASE WHEN @count=0 THEN 'ready' ELSE 'blocked' END,reconciliation_fingerprint=@fingerprint,updated_at=NOW() WHERE cycle_id=@cycle", connection, transaction))
            { refresh.Parameters.AddWithValue("count", blockers.Count); refresh.Parameters.AddWithValue("fingerprint", refreshedFingerprint); refresh.Parameters.AddWithValue("cycle", preparedCycleId); await refresh.ExecuteNonQueryAsync(cancellationToken); }
            var refreshed = new ControlledTestPrepareResult(blockers.Count == 0 ? "ready" : "blocked", preparedCycleId, preparedCycleNumber, refreshedFingerprint, blockers, items, false, false);
            await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "prepare", request.IdempotencyKey, requestHash, refreshed, cancellationToken);
            await AuditAsync(connection, transaction, request.TenantId, request.JobId, preparedCycleId, "test_cycle_refreshed_in_place", request.Actor, request.Reason, JsonSerializer.Serialize(new { cycleNumber = preparedCycleNumber, createdNewCycle = false, blockers }), cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return refreshed;
        }

        var evidence = await CollectEvidenceAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        if (!job.MappingWorkflowReady)
            evidence = evidence.Append(Input("mapping", "jobs_staging", "", "invalid_or_stale", true, false, false,
                new { workflowReady = false })).ToArray();
        if (job.ChangeReviewPending || job.AddressChangePending)
            evidence = evidence.Append(Input("change_review", "jobs_staging", "", "pending", true, false, false,
                new { changeReviewPending = job.ChangeReviewPending, addressChangePending = job.AddressChangePending })).ToArray();
        var readiness = BuildReadiness(request.TenantId, request.JobId, designation, evidence);
        var cycleNumber = await NextCycleNumberAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        var cycleId = Guid.NewGuid();
        var status = readiness.Blockers.Count == 0 ? "ready" : "blocked";
        var json = JsonSerializer.Serialize(readiness);
        var fingerprint = Hash(json);
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO public.job_test_cycles
                (cycle_id,tenant_id,job_id,cycle_number,status,designation_version,prior_approved_version,
                 approved_fingerprint,reconciliation_fingerprint,reconciliation_json,reason,xero_policy,full_retest,prepared_by)
            VALUES(@cycle,@tenant,@job,@number,@status,@designation,@approved,@approved_fingerprint,
                   @fingerprint,CAST(@json AS jsonb),@reason,@xero_policy,false,@actor);
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue("cycle", cycleId); insert.Parameters.AddWithValue("tenant", request.TenantId);
            insert.Parameters.AddWithValue("job", request.JobId); insert.Parameters.AddWithValue("number", cycleNumber);
            insert.Parameters.AddWithValue("status", status); insert.Parameters.AddWithValue("designation", designation.Version);
            insert.Parameters.AddWithValue("approved", job.ApprovedVersion);
            insert.Parameters.AddWithValue("approved_fingerprint", job.ApprovedFingerprint);
            insert.Parameters.AddWithValue("fingerprint", fingerprint); insert.Parameters.AddWithValue("json", json);
            insert.Parameters.AddWithValue("reason", request.Reason.Trim()); insert.Parameters.AddWithValue("actor", request.Actor.Trim());
            insert.Parameters.AddWithValue("xero_policy", request.XeroPolicy);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var supersedePrepared = new NpgsqlCommand("""
            UPDATE public.job_test_cycles SET status='superseded',superseded_at=NOW(),updated_at=NOW()
            WHERE tenant_id=@tenant AND job_id=@job AND cycle_id<>@cycle AND status IN ('ready','blocked');
            """, connection, transaction))
        {
            supersedePrepared.Parameters.AddWithValue("tenant", request.TenantId);
            supersedePrepared.Parameters.AddWithValue("job", request.JobId);
            supersedePrepared.Parameters.AddWithValue("cycle", cycleId);
            await supersedePrepared.ExecuteNonQueryAsync(cancellationToken);
        }
        await InsertItemsAndEvidenceAsync(connection, transaction, cycleId, request.TenantId, request.JobId, readiness.Items, cancellationToken);
        var result = new ControlledTestPrepareResult(status, cycleId, cycleNumber, fingerprint, readiness.Blockers, readiness.Items, false, false);
        await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "prepare", request.IdempotencyKey, requestHash, result, cancellationToken);
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, cycleId, "test_cycle_prepared",
            request.Actor, request.Reason, JsonSerializer.Serialize(new { cycleNumber, status, fingerprint, blockers = readiness.Blockers }), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public static async Task<ControlledTestReconcileResult> ReconcileAsync(
        NpgsqlConnection connection, ControlledTestReconcileCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        ValidateCommand(request.IdempotencyKey, request.Note, request.Confirmed);
        if (!Resolutions.Contains(request.Resolution))
            throw new ArgumentException("Unsupported reconciliation resolution.");
        await EnsureAsync(connection, cancellationToken);
        var requestHash = Hash(JsonSerializer.Serialize(new { request.CycleId, request.ItemId, request.Resolution, note = request.Note.Trim() }));
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var replay = await ClaimCommandAsync(connection, transaction, request.TenantId, request.JobId,
            "reconcile", request.IdempotencyKey, requestHash, cancellationToken);
        if (replay is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            var stored = JsonSerializer.Deserialize<ControlledTestReconcileResult>(replay)
                         ?? throw new InvalidOperationException("Stored reconciliation result is invalid.");
            return stored with { Replayed = true };
        }
        await RequireOwnedJobAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        await using var update = new NpgsqlCommand("""
            UPDATE public.job_test_cycle_reconciliation_items
            SET resolution=@resolution,resolution_note=@note,resolved_by=@actor,resolved_at=NOW()
            WHERE item_id=@item AND cycle_id=@cycle AND tenant_id=@tenant AND job_id=@job
              AND resolved_at IS NULL
            RETURNING effect_type;
            """, connection, transaction);
        update.Parameters.AddWithValue("resolution", request.Resolution); update.Parameters.AddWithValue("note", request.Note.Trim());
        update.Parameters.AddWithValue("actor", request.Actor.Trim()); update.Parameters.AddWithValue("item", request.ItemId);
        update.Parameters.AddWithValue("cycle", request.CycleId); update.Parameters.AddWithValue("tenant", request.TenantId);
        update.Parameters.AddWithValue("job", request.JobId);
        var effect = Convert.ToString(await update.ExecuteScalarAsync(cancellationToken));
        if (string.IsNullOrWhiteSpace(effect)) throw new ControlledTestCycleException("reconciliation_conflict", "The item was already resolved or does not belong to this cycle.");
        var blockers = await LoadUnresolvedBlockersAsync(connection, transaction, request.CycleId, cancellationToken);
        var reconciliationFingerprint = await ComputeCycleFingerprintAsync(connection, transaction, request.CycleId, cancellationToken);
        await using (var cycle = new NpgsqlCommand("UPDATE public.job_test_cycles SET status=CASE WHEN @count=0 THEN 'ready' ELSE 'blocked' END,reconciliation_fingerprint=@fingerprint,updated_at=NOW() WHERE cycle_id=@cycle AND status IN ('ready','blocked')", connection, transaction))
        { cycle.Parameters.AddWithValue("count", blockers.Count); cycle.Parameters.AddWithValue("fingerprint", reconciliationFingerprint); cycle.Parameters.AddWithValue("cycle", request.CycleId); await cycle.ExecuteNonQueryAsync(cancellationToken); }
        var result = new ControlledTestReconcileResult(blockers.Count == 0 ? "ready" : "blocked", request.CycleId, request.ItemId, reconciliationFingerprint, blockers, false);
        await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "reconcile", request.IdempotencyKey, requestHash, result, cancellationToken);
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, request.CycleId, "test_cycle_item_reconciled", request.Actor,
            request.Note, JsonSerializer.Serialize(new { request.ItemId, effect, request.Resolution, remainingBlockers = blockers }), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public static async Task<ControlledTestStartResult> StartAsync(
        NpgsqlConnection connection, ControlledTestStartCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentity(request.TenantId, request.JobId, request.Actor);
        ValidateCommand(request.IdempotencyKey, request.Reason, request.Confirmed);
        ValidateXeroPolicy(request.XeroPolicy);
        var requestHash = Hash(JsonSerializer.Serialize(new { request.CycleId, request.ExpectedReconciliationFingerprint, request.ExpectedApprovedVersion, request.XeroPolicy, request.FullRetest, reason = request.Reason.Trim() }));
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var replay = await ClaimCommandAsync(connection, transaction, request.TenantId, request.JobId, "start", request.IdempotencyKey, requestHash, cancellationToken);
        if (replay is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            var stored = JsonSerializer.Deserialize<ControlledTestStartResult>(replay)
                         ?? throw new InvalidOperationException("Stored start result is invalid.");
            return stored with { Replayed = true, Message = "The identical start command was already completed. No external action was performed." };
        }
        var job = await LoadJobForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        var designation = await LoadDesignationForUpdateAsync(connection, transaction, request.TenantId, request.JobId, cancellationToken);
        if (!designation.Enabled) throw new ControlledTestCycleException("test_job_required", "The controlled test designation is not active.");
        if (job.ApprovedVersion != request.ExpectedApprovedVersion)
            throw new ControlledTestCycleException("approved_version_conflict", "The approved job version changed. Prepare again.");
        if (!job.MappingWorkflowReady)
            throw new ControlledTestCycleException("mapping_review_required", "Validate and re-sync the tenant mapping before starting a test cycle.");
        if (job.ChangeReviewPending || job.AddressChangePending)
            throw new ControlledTestCycleException("change_review_pending", "Approve or revert the pending THREED change before starting a test cycle.");

        string status; string fingerprint; int priorVersion; int cycleNumber; string xeroPolicy; bool fullRetest;
        await using (var select = new NpgsqlCommand("""
            SELECT status,reconciliation_fingerprint,prior_approved_version,cycle_number,xero_policy,full_retest
            FROM public.job_test_cycles
            WHERE cycle_id=@cycle AND tenant_id=@tenant AND job_id=@job FOR UPDATE;
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("cycle", request.CycleId); select.Parameters.AddWithValue("tenant", request.TenantId); select.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new ControlledTestCycleException("cycle_not_found", "The prepared cycle was not found.");
            status = reader.GetString(0); fingerprint = reader.GetString(1); priorVersion = reader.GetInt32(2); cycleNumber = reader.GetInt32(3); xeroPolicy = reader.GetString(4); fullRetest = reader.GetBoolean(5);
        }
        if (fullRetest != request.FullRetest) throw new ControlledTestCycleException("full_retest_intent_conflict", "The full-retest authorization changed. Reload reconciliation before starting.");
        if (fullRetest && (request.JobId != FullRetestJobId || cycleNumber is not (2 or 3))) throw new ControlledTestCycleException("full_retest_not_allowed", "Full retest is limited to the allowlisted prepared 6 Senecio retest cycle.");
        if (!string.Equals(xeroPolicy, request.XeroPolicy, StringComparison.Ordinal))
            throw new ControlledTestCycleException("xero_policy_conflict", "The Xero policy changed. Prepare a new reconciliation.");
        if (status == "active")
        {
            var existing = new ControlledTestStartResult("active", request.CycleId, cycleNumber, job.ApprovedVersion, true, "The test cycle is already active.");
            await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "start", request.IdempotencyKey, requestHash, existing, cancellationToken);
            await transaction.CommitAsync(cancellationToken); return existing;
        }
        if (status != "ready" || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(fingerprint), Encoding.UTF8.GetBytes(request.ExpectedReconciliationFingerprint)))
            throw new ControlledTestCycleException("reconciliation_conflict", "Reconciliation changed or remains blocked. Prepare/reload before starting.");
        var blockers = await LoadUnresolvedBlockersAsync(connection, transaction, request.CycleId, cancellationToken);
        if (blockers.Count > 0) throw new ControlledTestCycleException("reconciliation_blocked", "Blocking external evidence remains unresolved.");
        if (priorVersion != job.ApprovedVersion) throw new ControlledTestCycleException("approved_version_conflict", "The approved version changed after preparation.");

        // Preserve all external identifiers and payment/signature state. Only create a new
        // approved internal revision. Prior email evidence is immutable in its source tables,
        // so Booking Email may reopen. Terms reopen only after an unsigned external invite was
        // explicitly cancelled/confirmed absent; signed Terms, Xero and Calendar remain current.
        var newVersion = job.ApprovedVersion + 1;
        var resetTerms = fullRetest || await CanResetTermsAsync(connection, transaction, request.CycleId, cancellationToken);
        await using (var updateJob = new NpgsqlCommand("""
            UPDATE public.jobs_staging SET
                approved_snapshot_version=@version,
                booking_email_sent=false,booking_email_sent_at=NULL,
                booking_email_retry_requested=false,booking_email_last_error=NULL,
                terms_sent=CASE WHEN @reset_terms THEN false ELSE terms_sent END,
                terms_sent_at=CASE WHEN @reset_terms THEN NULL ELSE terms_sent_at END,
                terms_signed=CASE WHEN @reset_terms THEN false ELSE terms_signed END,
                terms_signed_at=CASE WHEN @reset_terms THEN NULL ELSE terms_signed_at END,
                signnow_document_id=CASE WHEN @reset_terms THEN NULL ELSE signnow_document_id END,
                signnow_invite_id=CASE WHEN @reset_terms THEN NULL ELSE signnow_invite_id END,
                signnow_document_status=CASE WHEN @reset_terms THEN NULL ELSE signnow_document_status END,
                signnow_signing_link=CASE WHEN @reset_terms THEN NULL ELSE signnow_signing_link END,
                signnow_webhook_subscription_id=CASE WHEN @reset_terms THEN NULL ELSE signnow_webhook_subscription_id END,
                signnow_webhook_status=CASE WHEN @reset_terms THEN NULL ELSE signnow_webhook_status END,
                terms_retry_requested=false,terms_last_error=NULL,
                invoice_retry_requested=false,invoice_last_error=NULL,
                calendar_retry_requested=false,calendar_last_error=NULL,
                report_retry_requested=false,report_last_error=NULL,
                workflow_updated_at=NOW()
            WHERE tenant_id::text=@tenant AND job_id=@job;
            """, connection, transaction))
        {
            updateJob.Parameters.AddWithValue("version", newVersion); updateJob.Parameters.AddWithValue("reset_terms", resetTerms);
            updateJob.Parameters.AddWithValue("tenant", request.TenantId.ToString()); updateJob.Parameters.AddWithValue("job", request.JobId);
            await updateJob.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var supersede = new NpgsqlCommand("UPDATE public.job_test_cycles SET status='superseded',superseded_at=NOW(),updated_at=NOW() WHERE tenant_id=@tenant AND job_id=@job AND status='active' AND cycle_id<>@cycle", connection, transaction))
        { supersede.Parameters.AddWithValue("tenant", request.TenantId); supersede.Parameters.AddWithValue("job", request.JobId); supersede.Parameters.AddWithValue("cycle", request.CycleId); await supersede.ExecuteNonQueryAsync(cancellationToken); }
        await using (var activate = new NpgsqlCommand("UPDATE public.job_test_cycles SET status='active',approved_version=@version,started_by=@actor,started_at=NOW(),updated_at=NOW() WHERE cycle_id=@cycle", connection, transaction))
        { activate.Parameters.AddWithValue("version", newVersion); activate.Parameters.AddWithValue("actor", request.Actor.Trim()); activate.Parameters.AddWithValue("cycle", request.CycleId); await activate.ExecuteNonQueryAsync(cancellationToken); }
        var result = new ControlledTestStartResult("active", request.CycleId, cycleNumber, newVersion, false, "Controlled test cycle started. No external action was performed.");
        await CompleteCommandAsync(connection, transaction, request.TenantId, request.JobId, "start", request.IdempotencyKey, requestHash, result, cancellationToken);
        await AuditAsync(connection, transaction, request.TenantId, request.JobId, request.CycleId, "test_cycle_started", request.Actor, request.Reason,
            JsonSerializer.Serialize(new { cycleNumber, fullRetest, priorApprovedVersion = priorVersion, approvedVersion = newVersion, externalCalls = 0 }), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public static IReadOnlyList<string> EvaluateBlockingReasons(IEnumerable<ControlledTestEvidenceInput> evidence) =>
        evidence.Where(e => e.Blocking && string.IsNullOrWhiteSpace(e.Resolution))
            .Select(e => $"{e.EffectType}:{e.ObservedStatus}").Distinct(StringComparer.Ordinal).Order().ToArray();

    private static ControlledTestReadiness BuildReadiness(Guid tenantId, Guid jobId, Designation designation, IReadOnlyList<ControlledTestEvidenceInput> evidence)
    {
        var items = evidence.Select(e => new ControlledTestEvidence(e.ItemId, e.EffectType, e.InternalRecordId, e.ExternalId,
            e.ObservedStatus, e.Blocking, e.Uncertain, e.ResetAllowed, e.Resolution, e.EvidenceJson)).ToArray();
        return new(tenantId, jobId, designation.Enabled, designation.Version, EvaluateBlockingReasons(evidence), items);
    }

    private static async Task<IReadOnlyList<ControlledTestEvidenceInput>> CollectEvidenceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, CancellationToken cancellationToken)
    {
        var items = new List<ControlledTestEvidenceInput>();
        var calendarCreated = false;
        await using var command = new NpgsqlCommand("""
            SELECT approved_snapshot_version,COALESCE(approved_snapshot_fingerprint,''),
                   terms_signed,COALESCE(signnow_document_id,''),COALESCE(signnow_invite_id,''),COALESCE(signnow_webhook_subscription_id,''),COALESCE(signnow_document_status,''),
                   paid,COALESCE(amount_paid,0),COALESCE(payment_status,''),COALESCE(xero_contact_id,''),COALESCE(xero_invoice_id,''),COALESCE(xero_invoice_number,''),COALESCE(xero_invoice_status,''),
                   calendar_created,report_workflow_sent,COALESCE(report_sent,''),xero_review_required,report_review_required
            FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job;
            """, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId.ToString()); command.Parameters.AddWithValue("job", jobId);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken)) throw new UnauthorizedAccessException("Job not found for this company.");
            var termsSigned = reader.GetBoolean(2); var signNowDoc = reader.GetString(3); var signNowInvite = reader.GetString(4);
            if (termsSigned || signNowDoc.Length > 0 || signNowInvite.Length > 0)
                items.Add(Input("signnow", signNowDoc, signNowInvite, termsSigned ? "signed" : reader.GetString(6), true, false, false,
                    new { documentId = signNowDoc, inviteId = signNowInvite, webhookId = reader.GetString(5), signed = termsSigned }));
            var paid = reader.GetBoolean(7); var amountPaid = reader.GetDecimal(8); var xeroInvoice = reader.GetString(11);
            if (paid || amountPaid > 0 || xeroInvoice.Length > 0)
                items.Add(Input("xero", xeroInvoice, reader.GetString(12), paid || amountPaid > 0 ? "paid_or_part_paid" : reader.GetString(13), paid || amountPaid > 0, false, false,
                    new { contactId = reader.GetString(10), invoiceId = xeroInvoice, invoiceNumber = reader.GetString(12), invoiceStatus = reader.GetString(13), paymentStatus = reader.GetString(9), paid, amountPaid }));
            calendarCreated = reader.GetBoolean(14);
            if (reader.GetBoolean(15) || reader.GetString(16).Length > 0 || reader.GetBoolean(17) || reader.GetBoolean(18))
                items.Add(Input("report_manual_review", "jobs_staging", "", "retained", false, false, false,
                    new { reportWorkflowSent = reader.GetBoolean(15), reportSent = reader.GetString(16), xeroReview = reader.GetBoolean(17), reportReview = reader.GetBoolean(18) }));
        }

        await AddQueryEvidenceAsync(connection, transaction, items, """
            SELECT action_id::text,state,COALESCE(provider_message_id,''),COALESCE(completion_error,'')
            FROM public.basic_production_scheduling_actions WHERE tenant_id=@tenant AND job_id=@job
            ORDER BY prepared_at DESC LIMIT 50;
            """, tenantId, jobId, "email", cancellationToken);
        await AddClientEvidenceAsync(connection, transaction, items, tenantId, jobId, cancellationToken);
        await AddCalendarEvidenceAsync(connection, transaction, items, tenantId, jobId, cancellationToken);
        await AddCalendarDiscoveryStateAsync(connection, transaction, items, tenantId, jobId, calendarCreated, cancellationToken);
        foreach (var effect in new[] { "email", "client_page", "signnow", "calendar", "xero", "report_manual_review" })
            if (!items.Any(item => item.EffectType == effect))
                items.Add(Input(effect, "", "", "not_present", false, false, true, new { authoritativeAbsence = true }));
        return items;
    }

    private static async Task AddQueryEvidenceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, List<ControlledTestEvidenceInput> items, string sql, Guid tenantId, Guid jobId, string effect, CancellationToken token)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction); command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var state = reader.GetString(1); var uncertain = state is "sending" or "reconciliation_required";
            items.Add(Input(effect, reader.GetString(0), reader.GetString(2), state, uncertain, uncertain, !uncertain,
                new { providerMessageId = reader.GetString(2), error = reader.GetString(3) }));
        }
    }

    private static async Task AddClientEvidenceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, List<ControlledTestEvidenceInput> items, Guid tenantId, Guid jobId, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("""
            SELECT c.communication_id::text,c.delivery_state,COALESCE(c.provider,''),c.revoked_at,
                   p.publication_id::text,p.revoked_at
            FROM public.email_communications c JOIN public.client_inspection_pages p ON p.publication_id=c.publication_id
            WHERE c.tenant_id=@tenant AND c.job_id=@job ORDER BY c.issued_at DESC LIMIT 100;
            """, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var state = reader.GetString(1); var uncertain = state == "queued";
            items.Add(Input("client_page", reader.GetString(0), reader.GetString(4), state, uncertain, uncertain, !uncertain,
                new { provider = reader.GetString(2), communicationRevoked = !reader.IsDBNull(3), publicationRevoked = !reader.IsDBNull(5) }));
        }
    }

    private static async Task AddCalendarEvidenceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, List<ControlledTestEvidenceInput> items, Guid tenantId, Guid jobId, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("SELECT evidence_id::text,event_id,event_status,calendar_id FROM public.job_calendar_evidence WHERE tenant_id=@tenant AND job_id=@job ORDER BY last_observed_at DESC LIMIT 20", connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token)) items.Add(Input("calendar", reader.GetString(0), reader.GetString(1), reader.GetString(2), false, false, false, new { calendarId = reader.GetString(3) }));
    }

    private static async Task AddCalendarDiscoveryStateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        List<ControlledTestEvidenceInput> items, Guid tenantId, Guid jobId, bool calendarCreated, CancellationToken token)
    {
        string? status = null; var count = 0;
        await using (var command = new NpgsqlCommand("""
            SELECT status,match_count FROM public.job_calendar_discoveries
            WHERE tenant_id=@tenant AND job_id=@job ORDER BY discovered_at DESC LIMIT 1;
            """, connection, transaction))
        {
            command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
            await using var reader = await command.ExecuteReaderAsync(token);
            if (await reader.ReadAsync(token)) { status = reader.GetString(0); count = reader.GetInt32(1); }
        }
        if (status == "duplicate")
            items.Add(Input("calendar", "discovery", "", "duplicate", true, true, false, new { matchCount = count }));
        else if (calendarCreated && status == "absent")
            items.Add(Input("calendar", "jobs_staging", "", "recorded_but_absent", true, true, false, new { calendarCreated, discoveredAbsent = true }));
        else if (calendarCreated && status != "retained" && !items.Any(i => i.EffectType == "calendar" && i.ExternalId.Length > 0))
            items.Add(Input("calendar", "jobs_staging", "", "unknown_event_id", true, true, false, new { calendarCreated, discoveryRequired = true }));
    }

    private static ControlledTestEvidenceInput Input(string type, string internalId, string externalId, string status, bool blocking, bool uncertain, bool resetAllowed, object evidence) =>
        new(Guid.NewGuid(), type, internalId, externalId, status, blocking, uncertain, resetAllowed, "", JsonSerializer.Serialize(evidence));

    private static async Task InsertItemsAndEvidenceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid cycleId, Guid tenantId, Guid jobId, IEnumerable<ControlledTestEvidence> items, CancellationToken token)
    {
        foreach (var item in items)
        {
            await using var command = new NpgsqlCommand("""
                INSERT INTO public.job_test_cycle_reconciliation_items
                    (item_id,cycle_id,tenant_id,job_id,effect_type,internal_record_id,external_id,observed_status,evidence_json,blocking,uncertain,reset_allowed)
                VALUES(@id,@cycle,@tenant,@job,@type,@internal,@external,@status,CAST(@evidence AS jsonb),@blocking,@uncertain,@reset);
                INSERT INTO public.controlled_test_external_evidence
                    (evidence_id,tenant_id,job_id,cycle_id,effect_type,internal_record_id,external_id,status,evidence_json)
                VALUES(@evidence_id,@tenant,@job,@cycle,@type,@internal,@external,@status,CAST(@evidence AS jsonb));
                """, connection, transaction);
            command.Parameters.AddWithValue("id", item.ItemId); command.Parameters.AddWithValue("evidence_id", Guid.NewGuid()); command.Parameters.AddWithValue("cycle", cycleId);
            command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId); command.Parameters.AddWithValue("type", item.EffectType);
            command.Parameters.AddWithValue("internal", item.InternalRecordId); command.Parameters.AddWithValue("external", item.ExternalId);
            command.Parameters.AddWithValue("status", item.ObservedStatus); command.Parameters.AddWithValue("evidence", item.EvidenceJson);
            command.Parameters.AddWithValue("blocking", item.Blocking); command.Parameters.AddWithValue("uncertain", item.Uncertain); command.Parameters.AddWithValue("reset", item.ResetAllowed);
            await command.ExecuteNonQueryAsync(token);
        }
    }

    private static async Task<List<string>> LoadUnresolvedBlockersAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid cycleId, CancellationToken token)
    {
        var result = new List<string>();
        await using var command = new NpgsqlCommand("SELECT effect_type,observed_status FROM public.job_test_cycle_reconciliation_items WHERE cycle_id=@cycle AND blocking=true AND resolved_at IS NULL ORDER BY effect_type,observed_status", connection, transaction);
        command.Parameters.AddWithValue("cycle", cycleId); await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token)) result.Add($"{reader.GetString(0)}:{reader.GetString(1)}"); return result;
    }

    private static async Task<IReadOnlyList<ControlledTestEvidence>> LoadCycleItemsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid cycleId, CancellationToken token)
    {
        var items = new List<ControlledTestEvidence>();
        await using var command = new NpgsqlCommand("SELECT item_id,effect_type,internal_record_id,external_id,observed_status,blocking,uncertain,reset_allowed,resolution,evidence_json::text FROM public.job_test_cycle_reconciliation_items WHERE cycle_id=@cycle ORDER BY blocking DESC,effect_type,created_at", connection, transaction);
        command.Parameters.AddWithValue("cycle", cycleId);
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token)) items.Add(new(reader.GetGuid(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetBoolean(5),reader.GetBoolean(6),reader.GetBoolean(7),reader.GetString(8),reader.GetString(9)));
        return items;
    }

    private static async Task RecoverAccidentalCycleThreeAsync(NpgsqlConnection connection, CancellationToken token)
    {
        await using var transaction = await connection.BeginTransactionAsync(token);
        Guid cycle2 = Guid.Empty, cycle3 = Guid.Empty; Guid tenant = Guid.Empty;
        await using (var select = new NpgsqlCommand("SELECT tenant_id,cycle_id,cycle_number FROM public.job_test_cycles WHERE job_id=@job AND cycle_number IN (2,3) ORDER BY cycle_number FOR UPDATE", connection, transaction))
        {
            select.Parameters.AddWithValue("job", FullRetestJobId);
            await using var reader = await select.ExecuteReaderAsync(token);
            while (await reader.ReadAsync(token)) { tenant = reader.GetGuid(0); if (reader.GetInt32(2) == 2) cycle2 = reader.GetGuid(1); else cycle3 = reader.GetGuid(1); }
        }
        if (cycle2 != Guid.Empty && cycle3 != Guid.Empty)
        {
            await using var recover = new NpgsqlCommand("""
                WITH eligible AS (
                    SELECT EXISTS(SELECT 1 FROM public.job_test_cycles WHERE cycle_id=@cycle2 AND status='superseded' AND started_at IS NULL)
                       AND EXISTS(SELECT 1 FROM public.job_test_cycles WHERE cycle_id=@cycle3 AND status IN ('ready','blocked') AND started_at IS NULL) AS ok
                ), demote AS (
                    UPDATE public.job_test_cycles SET status='superseded',superseded_at=NOW(),updated_at=NOW()
                    WHERE cycle_id=@cycle3 AND (SELECT ok FROM eligible) RETURNING cycle_id
                )
                UPDATE public.job_test_cycles SET status=CASE WHEN EXISTS(
                    SELECT 1 FROM public.job_test_cycle_reconciliation_items WHERE cycle_id=@cycle2 AND blocking=true AND resolved_at IS NULL
                ) THEN 'blocked' ELSE 'ready' END,superseded_at=NULL,updated_at=NOW()
                WHERE cycle_id=@cycle2 AND EXISTS(SELECT 1 FROM demote)
                RETURNING cycle_id;
                """, connection, transaction);
            recover.Parameters.AddWithValue("cycle2", cycle2); recover.Parameters.AddWithValue("cycle3", cycle3);
            var restored = await recover.ExecuteScalarAsync(token) is Guid;
            if (restored)
            {
                await using var audit = new NpgsqlCommand("INSERT INTO public.controlled_test_cycle_audit(audit_id,tenant_id,job_id,cycle_id,event_type,actor,reason,details_json) VALUES(@id,@tenant,@job,@cycle,'accidental_cycle_three_recovered','AutoMate recovery','Restore unexecuted cycle 2 and supersede accidental unexecuted cycle 3',CAST(@details AS jsonb))", connection, transaction);
                audit.Parameters.AddWithValue("id", Guid.NewGuid()); audit.Parameters.AddWithValue("tenant", tenant); audit.Parameters.AddWithValue("job", FullRetestJobId); audit.Parameters.AddWithValue("cycle", cycle2);
                audit.Parameters.AddWithValue("details", JsonSerializer.Serialize(new { restoredCycle = 2, supersededCycle = 3, externalActions = 0 })); await audit.ExecuteNonQueryAsync(token);
            }
        }
        await transaction.CommitAsync(token);
    }

    private static async Task<string> ComputeCycleFingerprintAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid cycleId, CancellationToken token)
    {
        var lines = new List<string>();
        await using var command = new NpgsqlCommand("""
            SELECT item_id,effect_type,internal_record_id,external_id,observed_status,blocking,uncertain,
                   reset_allowed,resolution,resolution_note,COALESCE(resolved_by,''),resolved_at
            FROM public.job_test_cycle_reconciliation_items WHERE cycle_id=@cycle
            ORDER BY effect_type,internal_record_id,external_id,item_id;
            """, connection, transaction);
        command.Parameters.AddWithValue("cycle", cycleId);
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
            lines.Add(string.Join("|", Enumerable.Range(0, 12).Select(i => reader.IsDBNull(i) ? "" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture) ?? "")));
        return Hash(string.Join("\n", lines));
    }

    private static async Task<bool> CanResetTermsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid cycleId, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("""
            SELECT
              COUNT(*) FILTER (WHERE effect_type='signnow') AS total,
              COUNT(*) FILTER (WHERE effect_type='signnow' AND observed_status='signed') AS signed,
              COUNT(*) FILTER (WHERE effect_type='signnow' AND resolution IN ('cancelled_unsigned','confirmed_absent','failed_confirmed')) AS safely_resolved
            FROM public.job_test_cycle_reconciliation_items WHERE cycle_id=@cycle;
            """, connection, transaction);
        command.Parameters.AddWithValue("cycle", cycleId);
        await using var reader = await command.ExecuteReaderAsync(token); await reader.ReadAsync(token);
        var total = reader.GetInt64(0); var signed = reader.GetInt64(1); var safelyResolved = reader.GetInt64(2);
        return signed == 0 && (total == 0 || safelyResolved == total);
    }

    private static async Task<JobState> LoadJobForUpdateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("SELECT approved_snapshot_version,COALESCE(approved_snapshot_fingerprint,''),mapping_workflow_ready,change_review_pending,address_change_pending,COALESCE(pending_change_fingerprint,'') FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job FOR UPDATE", connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId.ToString()); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(token); if (!await reader.ReadAsync(token)) throw new UnauthorizedAccessException("Job not found for this company.");
        return new(reader.GetInt32(0), reader.GetString(1), reader.GetBoolean(2), reader.GetBoolean(3), reader.GetBoolean(4), reader.GetString(5));
    }

    private static async Task RequireOwnedJobAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, CancellationToken token) =>
        _ = await LoadJobForUpdateAsync(connection, transaction, tenantId, jobId, token);

    private static async Task<Designation> LoadDesignationForUpdateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("SELECT enabled,designation_version FROM public.controlled_test_jobs WHERE tenant_id=@tenant AND job_id=@job FOR UPDATE", connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(token); return await reader.ReadAsync(token) ? new(reader.GetBoolean(0), reader.GetInt32(1)) : new(false, 0);
    }

    private static async Task<int> NextCycleNumberAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("SELECT COALESCE(MAX(cycle_number),0)+1 FROM public.job_test_cycles WHERE tenant_id=@tenant AND job_id=@job", connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId); return Convert.ToInt32(await command.ExecuteScalarAsync(token));
    }

    private static async Task<string?> ClaimCommandAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, string type, string key, string hash, CancellationToken token)
    {
        await using (var insert = new NpgsqlCommand("INSERT INTO public.controlled_test_cycle_commands(tenant_id,job_id,command_type,idempotency_key,request_hash) VALUES(@tenant,@job,@type,@key,@hash) ON CONFLICT DO NOTHING", connection, transaction))
        { insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("job", jobId); insert.Parameters.AddWithValue("type", type); insert.Parameters.AddWithValue("key", key.Trim()); insert.Parameters.AddWithValue("hash", hash); await insert.ExecuteNonQueryAsync(token); }
        await using var select = new NpgsqlCommand("SELECT request_hash,COALESCE(result_json::text,'') FROM public.controlled_test_cycle_commands WHERE tenant_id=@tenant AND job_id=@job AND command_type=@type AND idempotency_key=@key FOR UPDATE", connection, transaction);
        select.Parameters.AddWithValue("tenant", tenantId); select.Parameters.AddWithValue("job", jobId); select.Parameters.AddWithValue("type", type); select.Parameters.AddWithValue("key", key.Trim());
        await using var reader = await select.ExecuteReaderAsync(token); await reader.ReadAsync(token);
        if (reader.GetString(0) != hash) throw new ControlledTestCycleException("idempotency_conflict", "The idempotency key was used for different content.");
        var result = reader.GetString(1); return result.Length == 0 ? null : result;
    }

    private static async Task CompleteCommandAsync<T>(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, string type, string key, string hash, T result, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("UPDATE public.controlled_test_cycle_commands SET result_json=CAST(@json AS jsonb),completed_at=NOW() WHERE tenant_id=@tenant AND job_id=@job AND command_type=@type AND idempotency_key=@key AND request_hash=@hash", connection, transaction);
        command.Parameters.AddWithValue("json", JsonSerializer.Serialize(result)); command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId); command.Parameters.AddWithValue("type", type); command.Parameters.AddWithValue("key", key.Trim()); command.Parameters.AddWithValue("hash", hash); await command.ExecuteNonQueryAsync(token);
    }

    private static async Task AuditAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, Guid jobId, Guid? cycleId, string eventType, string actor, string reason, string details, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("INSERT INTO public.controlled_test_cycle_audit(audit_id,tenant_id,job_id,cycle_id,event_type,actor,reason,details_json) VALUES(@id,@tenant,@job,@cycle,@event,@actor,@reason,CAST(@details AS jsonb))", connection, transaction);
        command.Parameters.AddWithValue("id", Guid.NewGuid()); command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        command.Parameters.Add("cycle", NpgsqlDbType.Uuid).Value = cycleId ?? (object)DBNull.Value; command.Parameters.AddWithValue("event", eventType);
        command.Parameters.AddWithValue("actor", actor.Trim()); command.Parameters.AddWithValue("reason", reason.Trim()); command.Parameters.AddWithValue("details", details); await command.ExecuteNonQueryAsync(token);
    }

    private static void ValidateIdentity(Guid tenantId, Guid jobId, string actor)
    { if (tenantId == Guid.Empty || jobId == Guid.Empty || string.IsNullOrWhiteSpace(actor)) throw new ArgumentException("Authenticated tenant, job, and actor are required."); }
    private static void ValidateCommand(string key, string reason, bool confirmed)
    { if (!confirmed) throw new ControlledTestCycleException("confirmation_required", "Typed confirmation is required."); if (string.IsNullOrWhiteSpace(key) || key.Length > 200) throw new ArgumentException("A bounded idempotency key is required."); if (string.IsNullOrWhiteSpace(reason)) throw new ArgumentException("A reason is required."); }
    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static void ValidateXeroPolicy(string policy)
    {
        if (!string.Equals(policy, "retain_existing", StringComparison.Ordinal))
            throw new ControlledTestCycleException("unsupported_xero_policy", "Only retain_existing is supported. AutoMate will not clear, recreate, or replace a Xero invoice during a test-cycle reset.");
    }

    private sealed record Designation(bool Enabled, int Version);
    private sealed record JobState(int ApprovedVersion, string ApprovedFingerprint, bool MappingWorkflowReady, bool ChangeReviewPending, bool AddressChangePending, string PendingChangeFingerprint);
}

public sealed record ControlledTestDesignationCommand(Guid TenantId, Guid JobId, bool Enabled, int ExpectedVersion, bool Confirmed, string Reason, string Actor, string IdempotencyKey);
public sealed record ControlledTestDesignationResult(string Status, bool Enabled, int Version, bool Replayed, string Message);
public sealed record ControlledTestBaselineAcceptCommand(Guid TenantId, Guid JobId, int ExpectedApprovedVersion, string ExpectedPendingFingerprint, bool Confirmed, string Reason, string IdempotencyKey, string Actor);
public sealed record ControlledTestBaselineAcceptResult(string Status, int PriorApprovedVersion, int ApprovedVersion, string ApprovedFingerprint, bool Replayed, string Message);
public sealed record ControlledCalendarDiscoveryCommand(Guid TenantId, Guid JobId, int ExpectedApprovedVersion, bool Confirmed, string Reason, string IdempotencyKey, string Actor);
public sealed record CalendarDiscoveryEvent(string CalendarId, string EventId, string Status, string HtmlLink);
public sealed record CalendarDiscoveryDecision(string Status, int MatchCount, CalendarDiscoveryEvent? Event, bool Blocking, string Message);
public sealed record CalendarDiscoveryResult(string Status, Guid DiscoveryId, int MatchCount, string EventId, bool Blocking, bool Replayed, string Message);
public sealed record ControlledTestPrepareCommand(Guid TenantId, Guid JobId, int ExpectedDesignationVersion, int ExpectedApprovedVersion, bool Confirmed, string Reason, string IdempotencyKey, string Actor, string XeroPolicy = "retain_existing", bool FullRetest = false);
public sealed record ControlledTestPrepareResult(string Status, Guid CycleId, int CycleNumber, string ReconciliationFingerprint, IReadOnlyList<string> Blockers, IReadOnlyList<ControlledTestEvidence> Items, bool Replayed, bool FullRetest);
public sealed record ControlledTestReconcileCommand(Guid TenantId, Guid JobId, Guid CycleId, Guid ItemId, string Resolution, bool Confirmed, string Note, string IdempotencyKey, string Actor);
public sealed record ControlledTestReconcileResult(string Status, Guid CycleId, Guid ItemId, string ReconciliationFingerprint, IReadOnlyList<string> RemainingBlockers, bool Replayed);
public sealed record ControlledTestStartCommand(Guid TenantId, Guid JobId, Guid CycleId, int ExpectedApprovedVersion, string ExpectedReconciliationFingerprint, bool Confirmed, string Reason, string IdempotencyKey, string Actor, string XeroPolicy = "retain_existing", bool FullRetest = false);
public sealed record ControlledTestStartResult(string Status, Guid CycleId, int CycleNumber, int ApprovedVersion, bool Replayed, string Message);
public sealed record ControlledTestReadiness(Guid TenantId, Guid JobId, bool Designated, int DesignationVersion, IReadOnlyList<string> Blockers, IReadOnlyList<ControlledTestEvidence> Items);
public sealed record ControlledTestEvidence(Guid ItemId, string EffectType, string InternalRecordId, string ExternalId, string ObservedStatus, bool Blocking, bool Uncertain, bool ResetAllowed, string Resolution, string EvidenceJson);
public sealed record ControlledTestEvidenceInput(Guid ItemId, string EffectType, string InternalRecordId, string ExternalId, string ObservedStatus, bool Blocking, bool Uncertain, bool ResetAllowed, string Resolution, string EvidenceJson);
public sealed record ControlledTestState(Guid TenantId, Guid JobId, bool Designated, int DesignationVersion,
    int ApprovedVersion, string ApprovedFingerprint, bool MappingWorkflowReady, bool ChangeReviewPending,
    string PendingChangeFingerprint, ControlledTestCycleDetail? Current, IReadOnlyList<ControlledTestCycleSummary> History);
public sealed record ControlledTestCycleDetail(ControlledTestCycleSummary Cycle, IReadOnlyList<ControlledTestEvidence> Items, IReadOnlyList<string> Blockers, bool CanStart);
public sealed record ControlledTestCycleSummary(Guid CycleId, int CycleNumber, string Status, int PriorApprovedVersion, int? ApprovedVersion, string ReconciliationFingerprint, string Reason, string XeroPolicy, bool FullRetest, string PreparedBy, DateTimeOffset PreparedAt, string StartedBy, DateTimeOffset? StartedAt);

public sealed class ControlledTestCycleException(string code, string message) : InvalidOperationException(message)
{
    public string Code { get; } = code;
}
