using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class BasicChangeRunSupport
{
    public static readonly string[] ActionKeys = ["booking_email", "terms", "invoice", "calendar", "cancellation_email", "calendar_cancel", "client_page_revoke", "terms_cancel"];

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS public.basic_job_change_runs
            (
                run_id uuid PRIMARY KEY,
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                source_snapshot_fingerprint text NOT NULL,
                source_snapshot_version integer NOT NULL DEFAULT 0,
                status text NOT NULL DEFAULT 'settled'
                    CHECK (status IN ('settled','prepared','running','completed','attention','superseded')),
                source_changes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
                classification_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                config_references_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                detected_at timestamptz NOT NULL DEFAULT NOW(),
                settled_at timestamptz NULL,
                prepared_at timestamptz NULL,
                started_at timestamptz NULL,
                completed_at timestamptz NULL,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                UNIQUE (tenant_id, job_id, source_snapshot_fingerprint)
            );

            CREATE INDEX IF NOT EXISTS ix_basic_job_change_runs_ready
            ON public.basic_job_change_runs(tenant_id, status, detected_at);

            ALTER TABLE public.basic_job_change_runs
            ADD COLUMN IF NOT EXISTS source_changes_json jsonb NOT NULL DEFAULT '[]'::jsonb;

            CREATE TABLE IF NOT EXISTS public.basic_job_change_run_actions
            (
                action_id uuid PRIMARY KEY,
                run_id uuid NOT NULL REFERENCES public.basic_job_change_runs(run_id) ON DELETE CASCADE,
                action_key text NOT NULL
                    CHECK (action_key IN ('booking_email','terms','invoice','calendar','cancellation_email','calendar_cancel','client_page_revoke','terms_cancel')),
                status text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','completed','failed','review_required','superseded')),
                idempotency_key text NOT NULL,
                external_id text NULL,
                error_code text NULL,
                error_message text NULL,
                review_reason text NULL,
                attempt_count integer NOT NULL DEFAULT 0,
                started_at timestamptz NULL,
                completed_at timestamptz NULL,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                UNIQUE (run_id, action_key),
                UNIQUE (idempotency_key)
            );

            CREATE INDEX IF NOT EXISTS ix_basic_job_change_run_actions_work
            ON public.basic_job_change_run_actions(status, updated_at);

            ALTER TABLE public.basic_job_change_runs DROP CONSTRAINT IF EXISTS basic_job_change_runs_status_check;
            ALTER TABLE public.basic_job_change_runs ADD CONSTRAINT basic_job_change_runs_status_check
              CHECK (status IN ('settled','prepared','running','completed','attention','superseded'));
            ALTER TABLE public.basic_job_change_run_actions DROP CONSTRAINT IF EXISTS basic_job_change_run_actions_status_check;
            ALTER TABLE public.basic_job_change_run_actions ADD CONSTRAINT basic_job_change_run_actions_status_check
              CHECK (status IN ('pending','running','completed','failed','review_required','superseded'));

            CREATE SEQUENCE IF NOT EXISTS public.job_required_action_version_seq;
            ALTER TABLE public.basic_job_change_run_actions ADD COLUMN IF NOT EXISTS tenant_id uuid;
            ALTER TABLE public.basic_job_change_run_actions ADD COLUMN IF NOT EXISTS job_id uuid;
            ALTER TABLE public.basic_job_change_run_actions ADD COLUMN IF NOT EXISTS source_fingerprint text NOT NULL DEFAULT '';
            ALTER TABLE public.basic_job_change_run_actions ADD COLUMN IF NOT EXISTS action_version bigint;
            ALTER TABLE public.basic_job_change_run_actions ALTER COLUMN action_version SET DEFAULT nextval('public.job_required_action_version_seq');
            UPDATE public.basic_job_change_run_actions a SET tenant_id=r.tenant_id,job_id=r.job_id,
              source_fingerprint=r.source_snapshot_fingerprint,
              action_version=COALESCE(a.action_version,nextval('public.job_required_action_version_seq'))
            FROM public.basic_job_change_runs r WHERE r.run_id=a.run_id AND
              (a.tenant_id IS NULL OR a.job_id IS NULL OR a.source_fingerprint='' OR a.action_version IS NULL);
            ALTER TABLE public.basic_job_change_run_actions ALTER COLUMN tenant_id SET NOT NULL;
            ALTER TABLE public.basic_job_change_run_actions ALTER COLUMN job_id SET NOT NULL;
            ALTER TABLE public.basic_job_change_run_actions ALTER COLUMN action_version SET NOT NULL;
            WITH ranked AS (
              SELECT a.action_id,ROW_NUMBER() OVER(PARTITION BY a.tenant_id,a.job_id,a.action_key ORDER BY r.detected_at DESC,a.updated_at DESC,a.action_id) AS rn
              FROM public.basic_job_change_run_actions a JOIN public.basic_job_change_runs r ON r.run_id=a.run_id
              WHERE a.status IN ('pending','running','failed','review_required')
            )
            UPDATE public.basic_job_change_run_actions a SET status='superseded',updated_at=NOW(),
              action_version=nextval('public.job_required_action_version_seq')
            FROM ranked d WHERE a.action_id=d.action_id AND d.rn>1;
            CREATE UNIQUE INDEX IF NOT EXISTS ux_basic_job_change_action_current
              ON public.basic_job_change_run_actions(tenant_id,job_id,action_key)
              WHERE status IN ('pending','running','failed','review_required');
            CREATE OR REPLACE FUNCTION public.bump_basic_required_action_version() RETURNS trigger AS $$
            BEGIN
              NEW.action_version=nextval('public.job_required_action_version_seq');
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            DO $$ BEGIN
              IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='trg_basic_required_action_version') THEN
                CREATE TRIGGER trg_basic_required_action_version BEFORE UPDATE ON public.basic_job_change_run_actions
                  FOR EACH ROW EXECUTE FUNCTION public.bump_basic_required_action_version();
              END IF;
            END $$;

            CREATE TABLE IF NOT EXISTS public.basic_source_missing_observations
            (
                tenant_id uuid NOT NULL,
                job_id uuid NOT NULL,
                consecutive_count integer NOT NULL DEFAULT 0,
                first_observed_at timestamptz NOT NULL DEFAULT NOW(),
                last_observed_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY(tenant_id,job_id)
            );

            DO $$ BEGIN
                IF EXISTS(SELECT 1 FROM pg_constraint WHERE conname='basic_job_change_run_actions_action_key_check'
                    AND pg_get_constraintdef(oid) NOT LIKE '%cancellation_email%') THEN
                    ALTER TABLE public.basic_job_change_run_actions DROP CONSTRAINT basic_job_change_run_actions_action_key_check;
                    ALTER TABLE public.basic_job_change_run_actions ADD CONSTRAINT basic_job_change_run_actions_action_key_check
                        CHECK (action_key IN ('booking_email','terms','invoice','calendar','cancellation_email','calendar_cancel','client_page_revoke','terms_cancel'));
                END IF;
            END $$;

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
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<Guid> PrepareAsync(
        NpgsqlConnection connection,
        BasicChangeRunPreparation preparation,
        CancellationToken cancellationToken = default)
    {
        ValidatePreparation(preparation);
        await EnsureAsync(connection, cancellationToken);
        var classification = BasicChangeClassifier.Classify(preparation.Changes, preparation.ProviderState);
        var configJson = JsonSerializer.Serialize(preparation.ConfigReferences);
        var changesJson = JsonSerializer.Serialize(preparation.Changes);
        RejectCredentialMaterial(configJson);
        var classificationJson = JsonSerializer.Serialize(classification);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var runId = Guid.NewGuid();
        var insertedRun = false;
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO public.basic_job_change_runs
                (run_id,tenant_id,job_id,source_snapshot_fingerprint,source_snapshot_version,status,
                 source_changes_json,classification_json,config_references_json,settled_at,prepared_at)
            VALUES(@run,@tenant,@job,@fingerprint,@version,'prepared',CAST(@changes AS jsonb),
                   CAST(@classification AS jsonb),CAST(@config AS jsonb),NOW(),NOW())
            ON CONFLICT(tenant_id,job_id,source_snapshot_fingerprint) DO UPDATE SET
                   run_id=public.basic_job_change_runs.run_id
            RETURNING run_id,(xmax=0) AS inserted;
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue("run", runId);
            insert.Parameters.AddWithValue("tenant", preparation.TenantId);
            insert.Parameters.AddWithValue("job", preparation.JobId);
            insert.Parameters.AddWithValue("fingerprint", preparation.SourceSnapshotFingerprint.Trim());
            insert.Parameters.AddWithValue("version", preparation.SourceSnapshotVersion);
            insert.Parameters.AddWithValue("classification", classificationJson);
            insert.Parameters.AddWithValue("changes", changesJson);
            insert.Parameters.AddWithValue("config", configJson);
            await using var reader=await insert.ExecuteReaderAsync(cancellationToken);
            if(!await reader.ReadAsync(cancellationToken))throw new InvalidOperationException("The change run was not prepared.");
            runId=reader.GetGuid(0);insertedRun=reader.GetBoolean(1);
        }

        if(!insertedRun)
        {
            await transaction.CommitAsync(cancellationToken);
            return runId;
        }

        await using (var supersede = new NpgsqlCommand("""
            UPDATE public.basic_job_change_run_actions a SET status='superseded',updated_at=NOW(),
              action_version=nextval('public.job_required_action_version_seq')
            FROM public.basic_job_change_runs r
            WHERE a.run_id=r.run_id AND r.tenant_id=@tenant AND r.job_id=@job AND r.run_id<>@run
              AND a.status IN ('pending','running','failed','review_required');
            UPDATE public.basic_job_change_runs SET status='superseded',settled_at=COALESCE(settled_at,NOW()),updated_at=NOW()
            WHERE tenant_id=@tenant AND job_id=@job AND run_id<>@run
              AND status IN ('prepared','running','attention');
            """, connection, transaction))
        {
            supersede.Parameters.AddWithValue("tenant", preparation.TenantId);
            supersede.Parameters.AddWithValue("job", preparation.JobId);
            supersede.Parameters.AddWithValue("run", runId);
            await supersede.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var action in classification.Actions.Where(item => item.Required))
        {
            var actionStatus = action.Automatic ? "pending" : "review_required";
            var idempotencyKey = $"basic-change|{preparation.TenantId:N}|{preparation.JobId:N}|{preparation.SourceSnapshotFingerprint.Trim().ToLowerInvariant()}|{action.ActionKey}";
            await using var insertAction = new NpgsqlCommand("""
                INSERT INTO public.basic_job_change_run_actions
                    (action_id,run_id,tenant_id,job_id,source_fingerprint,action_key,status,idempotency_key,review_reason)
                VALUES(@id,@run,@tenant,@job,@fingerprint,@key,@status,@idempotency,@review)
                ON CONFLICT(run_id,action_key) DO NOTHING;
                """, connection, transaction);
            insertAction.Parameters.AddWithValue("id", Guid.NewGuid());
            insertAction.Parameters.AddWithValue("run", runId);
            insertAction.Parameters.AddWithValue("tenant", preparation.TenantId);
            insertAction.Parameters.AddWithValue("job", preparation.JobId);
            insertAction.Parameters.AddWithValue("fingerprint", preparation.SourceSnapshotFingerprint.Trim());
            insertAction.Parameters.AddWithValue("key", action.ActionKey);
            insertAction.Parameters.AddWithValue("status", actionStatus);
            insertAction.Parameters.AddWithValue("idempotency", idempotencyKey);
            insertAction.Parameters.AddWithValue("review", (object?)action.ReviewReason ?? DBNull.Value);
            await insertAction.ExecuteNonQueryAsync(cancellationToken);
        }

        if (classification.Actions.Any(item => item.Required && !item.Automatic))
        {
            await using var attention = new NpgsqlCommand(
                "UPDATE public.basic_job_change_runs SET status='attention',updated_at=NOW() WHERE run_id=@run AND status='prepared'",
                connection, transaction);
            attention.Parameters.AddWithValue("run", runId);
            await attention.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return runId;
    }

    public static async Task<BasicChangeRunView?> LoadCurrentAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        Guid runId; string fingerprint, status, changes, classification, config; int version;
        await using (var command = new NpgsqlCommand("""
            SELECT run_id,source_snapshot_fingerprint,source_snapshot_version,status,
                   source_changes_json::text,classification_json::text,config_references_json::text
            FROM public.basic_job_change_runs
            WHERE tenant_id=@tenant AND job_id=@job
            ORDER BY detected_at DESC,run_id DESC LIMIT 1;
            """, connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId);
            command.Parameters.AddWithValue("job", jobId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) return null;
            runId = reader.GetGuid(0); fingerprint = reader.GetString(1); version = reader.GetInt32(2);
            status = reader.GetString(3); changes=reader.GetString(4); classification = reader.GetString(5); config = reader.GetString(6);
        }
        return new(runId, tenantId, jobId, fingerprint, version, status, changes, classification, config,
            await LoadActionsAsync(connection, tenantId, jobId, runId, cancellationToken));
    }

    public static async Task<BasicChangeActionTransition> ClaimAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, Guid runId, string actionKey,
        CancellationToken cancellationToken = default)
    {
        ValidateActionKey(actionKey);
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var current = await LockActionAsync(connection, transaction, tenantId, jobId, runId, actionKey, cancellationToken);
        if (current is null) { await transaction.RollbackAsync(cancellationToken); return new("not_found", null, false, "The owned change action was not found."); }
        if (current.Status != "pending")
        {
            await transaction.CommitAsync(cancellationToken);
            return new(current.Status, current, true, current.Status switch
            {
                "running" => "This action is already claimed.",
                "completed" => "This action is already complete.",
                "review_required" => "This action requires review and cannot be claimed.",
                "failed" => "Retry the failed action before claiming it again.",
                _ => "This action cannot be claimed from its current state."
            });
        }
        await using (var update = new NpgsqlCommand("""
            UPDATE public.basic_job_change_run_actions SET status='running',attempt_count=attempt_count+1,
                   started_at=NOW(),completed_at=NULL,error_code=NULL,error_message=NULL,updated_at=NOW()
            WHERE action_id=@action;
            UPDATE public.basic_job_change_runs SET status='running',started_at=COALESCE(started_at,NOW()),updated_at=NOW()
            WHERE run_id=@run;
            """, connection, transaction))
        {
            update.Parameters.AddWithValue("action", current.ActionId); update.Parameters.AddWithValue("run", runId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        current = current with { Status = "running", AttemptCount = current.AttemptCount + 1, ErrorCode = null, ErrorMessage = null };
        await transaction.CommitAsync(cancellationToken);
        return new("running", current, false, "The change action was claimed.");
    }

    public static async Task<BasicChangeActionTransition> CompleteAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, Guid runId, string actionKey,
        bool succeeded, string? externalId, string? errorCode, string? errorMessage,
        CancellationToken cancellationToken = default)
    {
        ValidateActionKey(actionKey);
        if (succeeded && string.IsNullOrWhiteSpace(externalId) && actionKey is not ("booking_email" or "cancellation_email"))
            throw new ArgumentException("Successful provider actions require an external identifier.");
        if (!succeeded && (string.IsNullOrWhiteSpace(errorCode) || string.IsNullOrWhiteSpace(errorMessage)))
            throw new ArgumentException("Failed actions require a structured error code and message.");
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var current = await LockActionAsync(connection, transaction, tenantId, jobId, runId, actionKey, cancellationToken);
        if (current is null) { await transaction.RollbackAsync(cancellationToken); return new("not_found", null, false, "The owned change action was not found."); }
        var target = succeeded ? "completed" : "failed";
        if (current.Status != "running")
        {
            await transaction.CommitAsync(cancellationToken);
            var replay = current.Status == target && (!succeeded || string.Equals(current.ExternalId ?? "", externalId ?? "", StringComparison.Ordinal));
            return new(current.Status, current, replay, replay ? "The same terminal outcome is already recorded." : "Only a running action can be completed.");
        }
        await using (var update = new NpgsqlCommand("""
            UPDATE public.basic_job_change_run_actions SET status=@status,external_id=@external,
                   error_code=@code,error_message=@message,completed_at=NOW(),updated_at=NOW()
            WHERE action_id=@action;
            """, connection, transaction))
        {
            update.Parameters.AddWithValue("status", target); update.Parameters.AddWithValue("external", (object?)NullIfWhiteSpace(externalId) ?? DBNull.Value);
            update.Parameters.AddWithValue("code", succeeded ? DBNull.Value : errorCode!.Trim());
            update.Parameters.AddWithValue("message", succeeded ? DBNull.Value : errorMessage!.Trim());
            update.Parameters.AddWithValue("action", current.ActionId); await update.ExecuteNonQueryAsync(cancellationToken);
        }
        current = current with { Status = target, ExternalId = NullIfWhiteSpace(externalId), ErrorCode = succeeded ? null : errorCode!.Trim(), ErrorMessage = succeeded ? null : errorMessage!.Trim() };
        await RecalculateRunAsync(connection, transaction, tenantId, jobId, runId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(target, current, false, succeeded ? "The provider result was recorded." : "The provider failure was recorded.");
    }

    public static async Task<BasicChangeActionTransition> RetryFailedAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, Guid runId, string actionKey,
        CancellationToken cancellationToken = default)
    {
        ValidateActionKey(actionKey);
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var current = await LockActionAsync(connection, transaction, tenantId, jobId, runId, actionKey, cancellationToken);
        if (current is null) { await transaction.RollbackAsync(cancellationToken); return new("not_found", null, false, "The owned change action was not found."); }
        if (current.Status != "failed")
        {
            await transaction.CommitAsync(cancellationToken);
            return new(current.Status, current, current.Status == "pending", current.Status == "pending" ? "The retry is already pending." : "Only a failed action can be retried.");
        }
        await using (var update = new NpgsqlCommand("""
            UPDATE public.basic_job_change_run_actions SET status='pending',external_id=NULL,error_code=NULL,
                   error_message=NULL,completed_at=NULL,updated_at=NOW() WHERE action_id=@action;
            """, connection, transaction))
        { update.Parameters.AddWithValue("action", current.ActionId); await update.ExecuteNonQueryAsync(cancellationToken); }
        current = current with { Status = "pending", ExternalId = null, ErrorCode = null, ErrorMessage = null };
        await RecalculateRunAsync(connection, transaction, tenantId, jobId, runId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("pending", current, false, "The failed action is ready to retry.");
    }

    public static async Task<BasicChangeActionTransition> ApproveLiveActionAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, Guid runId, string actionKey,
        string expectedFingerprint, CancellationToken cancellationToken = default)
    {
        ValidateActionKey(actionKey);
        if (actionKey is not ("booking_email" or "calendar"))
            throw new ArgumentException("This action uses its dedicated review workflow.", nameof(actionKey));
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        string currentFingerprint="";string runFingerprint="";
        await using(var gate=new NpgsqlCommand("""
            SELECT COALESCE(j.current_snapshot_fingerprint,''),r.source_snapshot_fingerprint
            FROM public.basic_job_change_runs r
            JOIN public.jobs_staging j ON j.job_id=r.job_id AND j.tenant_id::text=r.tenant_id::text
            WHERE r.tenant_id=@tenant AND r.job_id=@job AND r.run_id=@run
            FOR UPDATE OF r,j
            """,connection,transaction))
        {
            gate.Parameters.AddWithValue("tenant",tenantId);gate.Parameters.AddWithValue("job",jobId);gate.Parameters.AddWithValue("run",runId);
            await using var reader=await gate.ExecuteReaderAsync(cancellationToken);
            if(!await reader.ReadAsync(cancellationToken)){await transaction.RollbackAsync(cancellationToken);return new("not_found",null,false,"The current changed-job action was not found.");}
            currentFingerprint=reader.GetString(0);runFingerprint=reader.GetString(1);
        }
        if(string.IsNullOrWhiteSpace(expectedFingerprint)||!string.Equals(expectedFingerprint,currentFingerprint,StringComparison.Ordinal)||!string.Equals(expectedFingerprint,runFingerprint,StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken);
            return new("stale",null,false,"THREED changed after this action loaded. Refresh before continuing.");
        }
        var current=await LockActionAsync(connection,transaction,tenantId,jobId,runId,actionKey,cancellationToken);
        if(current is null){await transaction.RollbackAsync(cancellationToken);return new("not_found",null,false,"The current changed-job action was not found.");}
        if(current.Status!="review_required")
        {
            await transaction.CommitAsync(cancellationToken);
            return new(current.Status,current,current.Status=="pending",current.Status=="pending"?"This action is already ready to run.":"Only a current review action can be approved here.");
        }
        await using(var update=new NpgsqlCommand("UPDATE public.basic_job_change_run_actions SET status='pending',review_reason=COALESCE(review_reason,'Reviewed on the job.'),updated_at=NOW() WHERE action_id=@action",connection,transaction))
        {update.Parameters.AddWithValue("action",current.ActionId);await update.ExecuteNonQueryAsync(cancellationToken);}
        current=current with{Status="pending"};
        await RecalculateRunAsync(connection,transaction,tenantId,jobId,runId,cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("pending",current,false,"The current THREED action is approved and ready to run.");
    }

    public static async Task<BasicChangeActionTransition> ApproveSignedAgreementReplacementAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, Guid runId,
        CancellationToken cancellationToken = default)
    {
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var current = await LockActionAsync(connection, transaction, tenantId, jobId, runId, "terms", cancellationToken);
        if (current is null) { await transaction.RollbackAsync(cancellationToken); return new("not_found", null, false, "The owned Terms change action was not found."); }
        if (current.Status != "review_required")
        {
            await transaction.CommitAsync(cancellationToken);
            return new(current.Status, current, current.Status == "pending", current.Status == "pending" ? "The replacement agreement is already pending." : "Only a reviewed signed-agreement action can be approved.");
        }
        await using (var update = new NpgsqlCommand("UPDATE public.basic_job_change_run_actions SET status='pending',error_code='signed_replacement_approved',error_message=NULL,review_reason='Signed agreement preserved; explicit replacement approved.',completed_at=NULL,updated_at=NOW() WHERE action_id=@action", connection, transaction))
        { update.Parameters.AddWithValue("action", current.ActionId); await update.ExecuteNonQueryAsync(cancellationToken); }
        current = current with { Status = "pending", ErrorCode = "signed_replacement_approved", ErrorMessage = null, ReviewReason = "Signed agreement preserved; explicit replacement approved." };
        await RecalculateRunAsync(connection, transaction, tenantId, jobId, runId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("pending", current, false, "The signed agreement was preserved and its replacement is ready to send.");
    }

    public static async Task<BasicChangeActionTransition> RetryFailedUsingCurrentSettingsAsync(
        NpgsqlConnection connection, Guid tenantId, Guid jobId, Guid runId, string actionKey,
        BasicChangeConfigReferences currentReferences, CancellationToken cancellationToken = default)
    {
        ValidateActionKey(actionKey);
        var configJson = JsonSerializer.Serialize(currentReferences);
        RejectCredentialMaterial(configJson);
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var current = await LockActionAsync(connection, transaction, tenantId, jobId, runId, actionKey, cancellationToken);
        if (current is null) { await transaction.RollbackAsync(cancellationToken); return new("not_found", null, false, "The owned change action was not found."); }
        if (current.Status != "failed")
        {
            await transaction.CommitAsync(cancellationToken);
            return new(current.Status, current, current.Status == "pending", "Only a failed action can be retried using current settings.");
        }
        await using (var update = new NpgsqlCommand("UPDATE public.basic_job_change_runs SET config_references_json=CAST(@config AS jsonb),updated_at=NOW() WHERE run_id=@run; UPDATE public.basic_job_change_run_actions SET status='pending',external_id=NULL,error_code=NULL,error_message=NULL,completed_at=NULL,updated_at=NOW() WHERE action_id=@action", connection, transaction))
        { update.Parameters.AddWithValue("config", configJson); update.Parameters.AddWithValue("run", runId); update.Parameters.AddWithValue("action", current.ActionId); await update.ExecuteNonQueryAsync(cancellationToken); }
        current = current with { Status = "pending", ExternalId = null, ErrorCode = null, ErrorMessage = null };
        await RecalculateRunAsync(connection, transaction, tenantId, jobId, runId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("pending", current, false, "Current configuration references were captured and the failed action is ready to retry.");
    }

    public static string AggregateStatus(IEnumerable<string> actionStatuses)
    {
        var values = actionStatuses.Select(value => (value ?? "").Trim().ToLowerInvariant()).ToArray();
        if (values.Length == 0 || values.All(value => value == "completed")) return "completed";
        if (values.Any(value => value is "failed" or "review_required")) return "attention";
        if (values.Any(value => value == "running")) return "running";
        return "prepared";
    }

    public static async Task ResolveCurrentReviewActionAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,string actionKey,string externalId,CancellationToken cancellationToken=default)
    {
        ValidateActionKey(actionKey);await EnsureAsync(connection,cancellationToken);
        await using var transaction=await connection.BeginTransactionAsync(cancellationToken);Guid? runId=null;
        await using(var select=new NpgsqlCommand("SELECT run_id FROM public.basic_job_change_runs WHERE tenant_id=@tenant AND job_id=@job ORDER BY detected_at DESC,run_id DESC LIMIT 1 FOR UPDATE",connection,transaction))
        {
            select.Parameters.AddWithValue("tenant",tenantId);select.Parameters.AddWithValue("job",jobId);var value=await select.ExecuteScalarAsync(cancellationToken);if(value is Guid id)runId=id;
        }
        if(runId.HasValue)
        {
            await using(var update=new NpgsqlCommand("UPDATE public.basic_job_change_run_actions SET status='completed',external_id=COALESCE(NULLIF(@external,''),external_id),error_code=NULL,error_message=NULL,completed_at=NOW(),updated_at=NOW() WHERE run_id=@run AND action_key=@key AND status IN ('review_required','failed','pending')",connection,transaction))
            {
                update.Parameters.AddWithValue("run",runId.Value);update.Parameters.AddWithValue("key",actionKey);update.Parameters.AddWithValue("external",externalId??"");await update.ExecuteNonQueryAsync(cancellationToken);
            }
            await RecalculateRunAsync(connection,transaction,tenantId,jobId,runId.Value,cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    public static async Task<BasicChangeActionTransition> ResolveReviewActionForFingerprintAsync(
        NpgsqlConnection connection,Guid tenantId,Guid jobId,string sourceFingerprint,string actionKey,
        string externalId,string reviewReason,CancellationToken cancellationToken=default)
    {
        ValidateActionKey(actionKey);
        if(string.IsNullOrWhiteSpace(sourceFingerprint))throw new ArgumentException("The source snapshot identity is required.");
        if(string.IsNullOrWhiteSpace(reviewReason))throw new ArgumentException("The review outcome is required.");
        await EnsureAsync(connection,cancellationToken);
        await using var transaction=await connection.BeginTransactionAsync(cancellationToken);
        Guid runId;
        BasicChangeActionView? current;
        await using(var select=new NpgsqlCommand("""
            SELECT a.run_id,a.action_id,a.action_key,a.status,a.idempotency_key,a.external_id,a.error_code,
                   a.error_message,a.review_reason,a.attempt_count
            FROM public.basic_job_change_run_actions a
            WHERE a.tenant_id=@tenant AND a.job_id=@job AND a.source_fingerprint=@fingerprint AND a.action_key=@key
            ORDER BY a.action_version DESC,a.updated_at DESC,a.action_id DESC
            LIMIT 1 FOR UPDATE OF a;
            """,connection,transaction))
        {
            select.Parameters.AddWithValue("tenant",tenantId);select.Parameters.AddWithValue("job",jobId);
            select.Parameters.AddWithValue("fingerprint",sourceFingerprint.Trim());select.Parameters.AddWithValue("key",actionKey);
            await using var reader=await select.ExecuteReaderAsync(cancellationToken);
            if(!await reader.ReadAsync(cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return new("not_found",null,false,"The exact current review action was not found. Refresh before recording the outcome.");
            }
            runId=reader.GetGuid(0);
            current=new(reader.GetGuid(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),
                reader.IsDBNull(5)?null:reader.GetString(5),reader.IsDBNull(6)?null:reader.GetString(6),
                reader.IsDBNull(7)?null:reader.GetString(7),reader.IsDBNull(8)?null:reader.GetString(8),reader.GetInt32(9));
        }
        if(current.Status=="completed")
        {
            await transaction.CommitAsync(cancellationToken);
            return new("completed",current,true,"This exact review outcome was already recorded.");
        }
        if(current.Status is not ("review_required" or "failed" or "pending"))
        {
            await transaction.CommitAsync(cancellationToken);
            return new(current.Status,current,false,"This exact review action is no longer current. Refresh before recording the outcome.");
        }
        await using(var update=new NpgsqlCommand("""
            UPDATE public.basic_job_change_run_actions
            SET status='completed',external_id=COALESCE(NULLIF(@external,''),external_id),error_code=NULL,error_message=NULL,
                review_reason=@reason,completed_at=NOW(),updated_at=NOW(),
                action_version=nextval('public.job_required_action_version_seq')
            WHERE action_id=@action;
            """,connection,transaction))
        {
            update.Parameters.AddWithValue("action",current.ActionId);update.Parameters.AddWithValue("external",externalId??"");
            update.Parameters.AddWithValue("reason",reviewReason.Trim());await update.ExecuteNonQueryAsync(cancellationToken);
        }
        await RecalculateRunAsync(connection,transaction,tenantId,jobId,runId,cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("completed",current with{Status="completed",ExternalId=string.IsNullOrWhiteSpace(externalId)?current.ExternalId:externalId.Trim(),ErrorCode=null,ErrorMessage=null,ReviewReason=reviewReason.Trim()},false,"The exact current review outcome was recorded.");
    }

    public static async Task<bool> IsCompletedReviewForFingerprintAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,string sourceFingerprint,string actionKey,string reviewReasonPrefix,CancellationToken cancellationToken=default)
    {
        ValidateActionKey(actionKey);
        if(string.IsNullOrWhiteSpace(sourceFingerprint)||string.IsNullOrWhiteSpace(reviewReasonPrefix))return false;
        await EnsureAsync(connection,cancellationToken);
        await using var command=new NpgsqlCommand("""
            SELECT EXISTS(
              SELECT 1 FROM public.basic_job_change_run_actions
              WHERE tenant_id=@tenant AND job_id=@job AND source_fingerprint=@fingerprint
                AND action_key=@key AND status='completed'
                AND LEFT(COALESCE(review_reason,''),LENGTH(@reason_prefix))=@reason_prefix);
            """,connection);
        command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("job",jobId);
        command.Parameters.AddWithValue("fingerprint",sourceFingerprint.Trim());command.Parameters.AddWithValue("key",actionKey);
        command.Parameters.AddWithValue("reason_prefix",reviewReasonPrefix.Trim());
        return (bool)(await command.ExecuteScalarAsync(cancellationToken)??false);
    }

    private static async Task<IReadOnlyList<BasicChangeActionView>> LoadActionsAsync(NpgsqlConnection connection,
        Guid tenantId, Guid jobId, Guid runId, CancellationToken cancellationToken)
    {
        var result = new List<BasicChangeActionView>();
        await using var command = new NpgsqlCommand("""
            SELECT a.action_id,a.action_key,a.status,a.idempotency_key,a.external_id,a.error_code,
                   a.error_message,a.review_reason,a.attempt_count
            FROM public.basic_job_change_run_actions a
            JOIN public.basic_job_change_runs r ON r.run_id=a.run_id
            WHERE r.tenant_id=@tenant AND r.job_id=@job AND r.run_id=@run
            ORDER BY CASE a.action_key WHEN 'booking_email' THEN 1 WHEN 'cancellation_email' THEN 1 WHEN 'terms' THEN 2 WHEN 'terms_cancel' THEN 2 WHEN 'invoice' THEN 3 WHEN 'calendar' THEN 4 WHEN 'calendar_cancel' THEN 4 ELSE 5 END;
            """, connection);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId); command.Parameters.AddWithValue("run", runId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) result.Add(ReadAction(reader));
        return result;
    }

    private static async Task<BasicChangeActionView?> LockActionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid tenantId, Guid jobId, Guid runId, string actionKey, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            SELECT a.action_id,a.action_key,a.status,a.idempotency_key,a.external_id,a.error_code,
                   a.error_message,a.review_reason,a.attempt_count
            FROM public.basic_job_change_run_actions a
            JOIN public.basic_job_change_runs r ON r.run_id=a.run_id
            WHERE r.tenant_id=@tenant AND r.job_id=@job AND r.run_id=@run AND a.action_key=@key
            FOR UPDATE OF a,r;
            """, connection, transaction);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId);
        command.Parameters.AddWithValue("run", runId); command.Parameters.AddWithValue("key", actionKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadAction(reader) : null;
    }

    private static BasicChangeActionView ReadAction(NpgsqlDataReader reader) => new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2),
        reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
        reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7), reader.GetInt32(8));

    private static async Task RecalculateRunAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid tenantId, Guid jobId, Guid runId, CancellationToken cancellationToken)
    {
        var statuses = new List<string>();
        await using (var select = new NpgsqlCommand("""
            SELECT a.status FROM public.basic_job_change_run_actions a
            JOIN public.basic_job_change_runs r ON r.run_id=a.run_id
            WHERE r.tenant_id=@tenant AND r.job_id=@job AND r.run_id=@run FOR UPDATE OF a,r;
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("tenant", tenantId); select.Parameters.AddWithValue("job", jobId); select.Parameters.AddWithValue("run", runId);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) statuses.Add(reader.GetString(0));
        }
        var status = AggregateStatus(statuses);
        await using var update = new NpgsqlCommand("""
            UPDATE public.basic_job_change_runs SET status=@status,
                   completed_at=CASE WHEN @status='completed' THEN COALESCE(completed_at,NOW()) ELSE NULL END,
                   updated_at=NOW() WHERE tenant_id=@tenant AND job_id=@job AND run_id=@run;
            """, connection, transaction);
        update.Parameters.AddWithValue("status", status); update.Parameters.AddWithValue("tenant", tenantId);
        update.Parameters.AddWithValue("job", jobId); update.Parameters.AddWithValue("run", runId);
        await update.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void ValidateActionKey(string actionKey)
    {
        if (!ActionKeys.Contains(actionKey, StringComparer.Ordinal)) throw new ArgumentException("Unknown Basic change action.");
    }

    private static string? NullIfWhiteSpace(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static void ValidatePreparation(BasicChangeRunPreparation value)
    {
        if (value.TenantId == Guid.Empty) throw new ArgumentException("Tenant identity is required.");
        if (value.JobId == Guid.Empty) throw new ArgumentException("Job identity is required.");
        if (string.IsNullOrWhiteSpace(value.SourceSnapshotFingerprint)) throw new ArgumentException("Source snapshot identity is required.");
        if (value.SourceSnapshotFingerprint.Length > 256) throw new ArgumentException("Source snapshot identity is too long.");
        var classification=BasicChangeClassifier.Classify(value.Changes,value.ProviderState);
        if(classification.Actions.Any(action=>action.ActionKey=="terms"&&action.Required)&&
           (!value.ConfigReferences.AgreementPlanId.HasValue||value.ConfigReferences.AgreementPlanVersion.GetValueOrDefault()<=0))
            throw new ArgumentException("A concrete agreement plan identity is required for a Terms change action.");
        if(classification.Actions.Any(action=>action.ActionKey=="calendar"&&action.Required)&&
           (string.IsNullOrWhiteSpace(value.ConfigReferences.GoogleMappingReference)||
            string.Equals(value.ConfigReferences.GoogleMappingReference,"inspector-calendar-mapping",StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException("A concrete inspector Calendar mapping reference is required for a Calendar change action.");
    }

    internal static void RejectCredentialMaterial(string json)
    {
        using var document = JsonDocument.Parse(json);
        Walk(document.RootElement);
        static void Walk(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
                foreach (var property in element.EnumerateObject())
                {
                    var key = property.Name.Replace("_", "", StringComparison.Ordinal).Replace("-", "", StringComparison.Ordinal).ToLowerInvariant();
                    if (key.Contains("password", StringComparison.Ordinal) || key.Contains("secret", StringComparison.Ordinal) ||
                        key.Contains("accesstoken", StringComparison.Ordinal) || key.Contains("refreshtoken", StringComparison.Ordinal) ||
                        key.Contains("credential", StringComparison.Ordinal))
                        throw new ArgumentException("Configuration references must not contain credentials or tokens.");
                    Walk(property.Value);
                }
            else if (element.ValueKind == JsonValueKind.Array)
                foreach (var item in element.EnumerateArray()) Walk(item);
        }
    }
}

public static class BasicChangeClassifier
{
    public static BasicChangeClassification Classify(
        IEnumerable<BasicSourceFieldChange> changes,
        BasicChangeProviderState providerState)
    {
        var sourceChanges=changes.ToArray();
        var cancellation=sourceChanges.Any(change=>IsAppointmentField(change.Field)&&!string.IsNullOrWhiteSpace(change.OldValue)&&string.IsNullOrWhiteSpace(change.NewValue));
        var categories = sourceChanges
            .Select(change => NormalizeCategory(change.Category, change.Field))
            .Where(category => category != BasicChangeCategory.FormattingOnly)
            .Distinct()
            .OrderBy(category => category)
            .ToArray();

        if(cancellation)
        {
            var cancellationActions=new[]
            {
                Action("cancellation_email",true,false,"Appointment removed — review and send cancellation email."),
                Action("terms_cancel",providerState.TermsRequired&&!providerState.TermsSigned,false,"Appointment removed — review the unsigned Terms cancellation."),
                Action("calendar_cancel",providerState.CalendarRequired,false,"Appointment removed — review and cancel the calendar event."),
                Action("client_page_revoke",true,false,"Appointment removed — review Client View access."),
                Action("invoice",providerState.InvoiceRequired,false,"The existing Xero invoice is preserved. Review void, credit or refund requirements after cancellation.")
            };
            return new([BasicChangeCategory.Cancellation],false,cancellationActions,false);
        }

        var bookingChange = categories.Any(category => category is
            BasicChangeCategory.Appointment or BasicChangeCategory.Address or BasicChangeCategory.Inspector or
            BasicChangeCategory.PrimaryService or BasicChangeCategory.Contact);
        var agreementChange = categories.Any(category => category is
            BasicChangeCategory.Address or BasicChangeCategory.PrimaryService or BasicChangeCategory.Contact);
        var invoiceChange = categories.Any(category => category is
            BasicChangeCategory.Invoice or BasicChangeCategory.PrimaryService or BasicChangeCategory.Contact);
        var calendarChange = categories.Any(category => category is
            BasicChangeCategory.Appointment or BasicChangeCategory.Address or BasicChangeCategory.Inspector or BasicChangeCategory.PrimaryService);

        var invoiceReviewReason="Job total changed — review the additional invoice or credit action.";
        var totalChange=sourceChanges.FirstOrDefault(change=>string.Equals(change.Field,"invoiceTotal",StringComparison.OrdinalIgnoreCase));
        if(totalChange is not null&&decimal.TryParse(totalChange.OldValue,System.Globalization.NumberStyles.Number,System.Globalization.CultureInfo.InvariantCulture,out var oldTotal)&&decimal.TryParse(totalChange.NewValue,System.Globalization.NumberStyles.Number,System.Globalization.CultureInfo.InvariantCulture,out var newTotal))
        {
            var difference=newTotal-oldTotal;
            invoiceReviewReason=difference>0m?$"Job total increased by ${difference:0.00} — review and create an additional invoice":difference<0m?$"Job total decreased by ${Math.Abs(difference):0.00} — accounting credit review required":"Job total is unchanged — no accounting action is required.";
        }
        var actions = new[]
        {
            Action("booking_email", bookingChange, false, "Job details changed — review and resend booking email"),
            Action("terms", agreementChange && providerState.TermsRequired, false, "Agreement details changed — review and send replacement Terms"),
            Action("invoice", invoiceChange && providerState.InvoiceRequired, false, invoiceReviewReason),
            Action("calendar", calendarChange && providerState.CalendarRequired, false, "Appointment details changed — review and update calendar event")
        };
        return new(categories, categories.Length == 0, actions,
            actions.All(action => !action.Required || action.Automatic));
    }

    private static BasicChangeActionDecision Action(string key, bool required, bool automatic, string? review) =>
        new(key, required, required && automatic, required ? review : null);

    public static BasicChangeCategory NormalizeCategory(string? category, string? field)
    {
        var value = (category ?? "").Trim().ToLowerInvariant().Replace('-', '_').Replace(' ', '_');
        var name = (field ?? "").Trim().ToLowerInvariant().Replace('-', '_').Replace(' ', '_');
        if (value is "formatting" or "formatting_only" or "representation_only") return BasicChangeCategory.FormattingOnly;
        if (value is "schedule" or "appointment" || name is "jobdate" or "job_date" or "inspectiondate" or "inspection_date" or "inspectiontime" or "inspection_time" or "durationminutes" or "duration_minutes") return BasicChangeCategory.Appointment;
        if (value == "address" || name.Contains("address", StringComparison.Ordinal)) return BasicChangeCategory.Address;
        if (value == "inspector" || name is "inspector" or "inspectorid" or "inspector_id") return BasicChangeCategory.Inspector;
        if (value is "services" or "primary_service" || name is "primaryservice" or "primary_service") return BasicChangeCategory.PrimaryService;
        if (value is "price" or "invoice" or "invoice_only" || name.StartsWith("invoice", StringComparison.Ordinal)) return BasicChangeCategory.Invoice;
        if (value is "customer" or "contact" || name is "clientfirstname" or "client_first_name" or "clientlastname" or "client_last_name" or "clientemail" or "client_email" or "contact1firstname" or "contact_1_first_name" or "contact1lastname" or "contact_1_last_name" or "contact1email" or "contact_1_email") return BasicChangeCategory.Contact;
        return BasicChangeCategory.FormattingOnly;
    }

    private static bool IsAppointmentField(string? field)
    {
        var name=(field??"").Trim().ToLowerInvariant().Replace("_","").Replace("-","");
        return name is "jobdate" or "inspectiondate" or "inspectiontime" or "appointmentat";
    }
}

public enum BasicChangeCategory { FormattingOnly, Appointment, Address, Inspector, PrimaryService, Invoice, Contact, Cancellation }

public sealed record BasicSourceFieldChange(string Field, string Category, string OldValue = "", string NewValue = "");
public sealed record BasicChangeProviderState(bool TermsRequired, bool TermsSigned, bool InvoiceRequired, bool XeroInvoiceIsDraft, bool CalendarRequired);
public sealed record BasicChangeActionDecision(string ActionKey, bool Required, bool Automatic, string? ReviewReason);
public sealed record BasicChangeClassification(IReadOnlyList<BasicChangeCategory> Categories, bool FormattingOnly, IReadOnlyList<BasicChangeActionDecision> Actions, bool FullyAutomatic);

public sealed record BasicChangeConfigReferences(
    int ConfigurationRevision,
    Guid? BookingTemplateId,
    int? BookingTemplateVersion,
    Guid? AgreementPlanId,
    int? AgreementPlanVersion,
    string? XeroSettingsReference,
    string? GoogleCalendarId,
    string? GoogleMappingReference,
    string? SmtpConfigurationIdentity);

public sealed record BasicChangeRunPreparation(
    Guid TenantId,
    Guid JobId,
    string SourceSnapshotFingerprint,
    int SourceSnapshotVersion,
    IReadOnlyList<BasicSourceFieldChange> Changes,
    BasicChangeProviderState ProviderState,
    BasicChangeConfigReferences ConfigReferences);

public sealed record BasicChangeActionView(Guid ActionId, string ActionKey, string Status, string IdempotencyKey,
    string? ExternalId, string? ErrorCode, string? ErrorMessage, string? ReviewReason, int AttemptCount);
public sealed record BasicChangeActionTransition(string Status, BasicChangeActionView? Action, bool Replayed, string Message);
public sealed record BasicChangeRunView(Guid RunId, Guid TenantId, Guid JobId, string SourceSnapshotFingerprint,
    int SourceSnapshotVersion, string Status, string SourceChangesJson, string ClassificationJson, string ConfigReferencesJson,
    IReadOnlyList<BasicChangeActionView> Actions);
