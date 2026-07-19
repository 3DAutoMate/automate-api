using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public sealed record AuthoritativeAttentionItem(
    Guid AttentionId, Guid? JobId, string JobName, string Owner, string ActionKey,
    string Kind, string Title, string Detail, string RecommendedAction,
    DateTimeOffset DetectedAt, string Severity, string Route, string Tab,
    string Section, string Status, string? TemplateSlot, string? TechnicalReferenceId,
    JsonElement Changes, string? ProviderStatus, string? ExternalId,
    DateTimeOffset? ResolvedAt, DateTimeOffset? SupersededAt,
    long ActionVersion, string EvidenceFingerprint);

public static class AuthoritativeAttentionSupport
{
    public const int ContractVersion = 2;

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
        CREATE TABLE IF NOT EXISTS public.job_attention_reviews
        (
            attention_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NULL,
            reason_key text NOT NULL,
            action_key text NOT NULL DEFAULT '',
            classification text NOT NULL,
            title text NOT NULL,
            detail text NOT NULL,
            recommended_action text NOT NULL DEFAULT '',
            severity text NOT NULL DEFAULT 'medium',
            status text NOT NULL DEFAULT 'open',
            target_route text NOT NULL DEFAULT '',
            target_tab text NOT NULL DEFAULT '',
            target_section text NOT NULL DEFAULT '',
            template_slot text NULL,
            changes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
            provider_status text NULL,
            external_id text NULL,
            technical_reference_id text NULL,
            detected_at timestamptz NOT NULL DEFAULT NOW(),
            resolved_at timestamptz NULL,
            superseded_at timestamptz NULL,
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            UNIQUE(tenant_id,job_id,reason_key,action_key)
        );
        CREATE INDEX IF NOT EXISTS ix_job_attention_reviews_current
            ON public.job_attention_reviews(tenant_id,status,detected_at DESC);
        ALTER TABLE public.job_attention_reviews ADD COLUMN IF NOT EXISTS incident_key text NOT NULL DEFAULT '';
        CREATE SEQUENCE IF NOT EXISTS public.job_required_action_version_seq;
        ALTER TABLE public.job_attention_reviews ADD COLUMN IF NOT EXISTS action_version bigint;
        ALTER TABLE public.job_attention_reviews ALTER COLUMN action_version SET DEFAULT nextval('public.job_required_action_version_seq');
        UPDATE public.job_attention_reviews SET action_version=nextval('public.job_required_action_version_seq') WHERE action_version IS NULL;
        ALTER TABLE public.job_attention_reviews ALTER COLUMN action_version SET NOT NULL;
        ALTER TABLE public.job_attention_reviews ADD COLUMN IF NOT EXISTS evidence_fingerprint text NOT NULL DEFAULT '';
        ALTER TABLE public.job_attention_reviews DROP CONSTRAINT IF EXISTS job_attention_reviews_tenant_id_job_id_reason_key_action_ke_key;
        ALTER TABLE public.job_attention_reviews DROP CONSTRAINT IF EXISTS job_attention_reviews_tenant_id_job_id_reason_key_action_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS ux_job_attention_review_incident_v2
            ON public.job_attention_reviews(tenant_id,COALESCE(job_id,'00000000-0000-0000-0000-000000000000'::uuid),reason_key,action_key,incident_key);
        WITH ranked AS (
          SELECT attention_id,ROW_NUMBER() OVER(PARTITION BY tenant_id,job_id,action_key ORDER BY detected_at DESC,updated_at DESC,attention_id) AS rn
          FROM public.job_attention_reviews WHERE status='open'
        )
        UPDATE public.job_attention_reviews r
        SET status='superseded',superseded_at=COALESCE(r.superseded_at,NOW()),updated_at=NOW(),action_version=nextval('public.job_required_action_version_seq')
        FROM ranked d WHERE r.attention_id=d.attention_id AND d.rn>1;
        CREATE UNIQUE INDEX IF NOT EXISTS ux_job_required_action_current_v2
            ON public.job_attention_reviews(tenant_id,COALESCE(job_id,'00000000-0000-0000-0000-000000000000'::uuid),action_key) WHERE status='open';
        CREATE TABLE IF NOT EXISTS public.required_action_ledger_migrations
        (
          tenant_id uuid PRIMARY KEY,
          migration_version integer NOT NULL,
          migrated_at timestamptz NOT NULL DEFAULT NOW(),
          migrated_by text NOT NULL,
          result_json jsonb NOT NULL DEFAULT '{}'::jsonb
        );
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task ReconcileAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default,
        bool migrateLegacyFlags = false)
    {
        await EnsureAsync(connection, ct);
        await JobLifecycleSupport.EnsureAsync(connection, ct);
        await BasicAutomationSupport.EnsureAsync(connection, ct);
        await BasicChangeRunSupport.EnsureAsync(connection, ct);

        // Compatibility projection is permitted exactly once by the explicit migration.
        // Normal sync/save commands pass the default false value, so a legacy boolean can
        // never create or reopen a user-facing action after cutover.
        if (migrateLegacyFlags)
        {
        const string templateSql = """
        SELECT j.job_id,
               COALESCE(NULLIF(j.site_address,''),NULLIF(j.job_name,''),j.job_id::text),
               COALESCE(j.inspector_name,''),
               COALESCE(j.change_detected_at,j.address_change_detected_at,j.workflow_updated_at,j.updated_at,NOW()),
               COALESCE(j.pending_change_reasons,''),
               COALESCE(s.enabled,false),
               (s.template_id IS NOT NULL AND t.template_id IS NOT NULL AND t.archived_at IS NULL AND COALESCE(t.is_active,true))
        FROM public.jobs_staging j
        LEFT JOIN LATERAL (
          SELECT CASE WHEN COALESCE(j.pending_change_reasons,'') ~* '(service|scope|price)'
                      THEN 'service_change' ELSE 'rescheduling' END AS event_key
        ) e ON TRUE
        LEFT JOIN public.basic_automation_settings s
          ON s.tenant_id::text=j.tenant_id::text AND s.event_key=e.event_key AND s.recipient_key='contact_1'
        LEFT JOIN public.email_templates t
          ON t.tenant_id::text=j.tenant_id::text AND t.template_id=s.template_id
        WHERE j.tenant_id::text=@tenant AND j.change_template_setup_required=true
        """;
        var templateRows = new List<(Guid JobId,string Reasons,bool Enabled,bool Saved,DateTimeOffset Detected)>();
        await using (var command = new NpgsqlCommand(templateSql, connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                templateRows.Add((reader.GetGuid(0),reader.GetString(4),reader.GetBoolean(5),reader.GetBoolean(6),DatabaseTimeSupport.ReadRequired(reader,3)));
            }
        }
        foreach(var row in templateRows)
        {
            var eventKey = ContainsChange(row.Reasons, "service", "scope", "price") ? "service_change" : "rescheduling";
            if (!row.Enabled || row.Saved)
            {
                await SupersedeAsync(connection, tenantId, row.JobId, "missing_template", eventKey, ct);
                await ClearLegacyFlagAsync(connection, tenantId, row.JobId, "change_template_setup_required", ct);
                continue;
            }
            var label = eventKey == "service_change" ? "Service change" : "Rescheduling";
            await UpsertAsync(connection, tenantId, row.JobId, "missing_template", eventKey, "missing_template",
                $"{label} email for Contact 1 needs a template",
                $"{label} for Contact 1 is enabled, but this exact slot has no saved active template.",
                $"Save the {label} template for Contact 1 or disable that automation.", "medium",
                $"/automations/templates/{eventKey}/contact_1", label, "template-editor",
                $"{eventKey}/contact_1", row.Detected, "[]", null, null, null, ct);
        }

        // Mapping is content based. Revision-only differences are not review items.
        const string mappingSql = """
        SELECT job_id,COALESCE(NULLIF(site_address,''),NULLIF(job_name,''),job_id::text),
               COALESCE(inspector_name,''),COALESCE(mapping_synced_at,workflow_updated_at,updated_at,NOW()),
               COALESCE(mapping_attention_reason,''),mapping_review_required,mapping_workflow_ready,
               COALESCE(mapping_profile_fingerprint,''),COALESCE(mapping_discovery_fingerprint,'')
        FROM public.jobs_staging WHERE tenant_id::text=@tenant AND mapping_review_required=true
        """;
        var mappingRows = new List<(Guid JobId,string Reason,bool Ready,string Current,string Discovery,DateTimeOffset Detected)>();
        await using (var command = new NpgsqlCommand(mappingSql, connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                mappingRows.Add((reader.GetGuid(0),reader.GetString(4),reader.GetBoolean(6),reader.GetString(7),reader.GetString(8),DatabaseTimeSupport.ReadRequired(reader,3)));
            }
        }
        foreach(var row in mappingRows)
        {
            var genuinelyChanged = !row.Ready;
            if (!genuinelyChanged)
            {
                await SupersedeAsync(connection, tenantId, row.JobId, "mapping_review", "mapping_review", ct);
                await ClearLegacyFlagAsync(connection, tenantId, row.JobId, "mapping_review_required", ct);
                continue;
            }
            await UpsertAsync(connection, tenantId, row.JobId, "mapping_review", "mapping_review", "mapping_review",
                "THREED mapping requires review", string.IsNullOrWhiteSpace(row.Reason) ? "The Current mapping does not match this job's discovered THREED fields." : row.Reason,
                "Review the exact changed assignments, approve Current mapping, then apply it to affected jobs.",
                "high", "/mapping", "Canonical fields", "mapping-review", null,row.Detected, "[]", null, null, null, ct);
        }

        // Xero review is never inferred from unpaid state. It exists only because the
        // job changed/was unscheduled after an invoice was already recorded.
        const string xeroSql = """
        SELECT job_id,COALESCE(NULLIF(site_address,''),NULLIF(job_name,''),job_id::text),
               COALESCE(inspector_name,''),CASE WHEN automate_status IN ('Cancelled','Unscheduled') THEN COALESCE(lifecycle_updated_at,workflow_updated_at,updated_at,NOW()) ELSE COALESCE(change_detected_at,address_change_detected_at,workflow_updated_at,updated_at,NOW()) END,
               automate_status,COALESCE(pending_change_json,'[]'::jsonb)::text,
               COALESCE(xero_invoice_status,''),COALESCE(xero_invoice_id,''),COALESCE(current_snapshot_fingerprint,'')
        FROM public.jobs_staging
        WHERE tenant_id::text=@tenant AND xero_review_required=true AND invoice_sent=true
        """;
        var xeroRows = new List<(Guid JobId,string AutomateStatus,string Changes,string Provider,string External,string Fingerprint,DateTimeOffset Detected)>();
        await using (var command = new NpgsqlCommand(xeroSql, connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                xeroRows.Add((reader.GetGuid(0),reader.GetString(4),NormalizeChanges(reader.GetString(5)),reader.GetString(6),reader.GetString(7),reader.GetString(8),DatabaseTimeSupport.ReadRequired(reader,3)));
            }
        }
        foreach(var row in xeroRows)await UpsertAsync(connection, tenantId, row.JobId, "xero_review", "invoice", "xero_review",
            row.AutomateStatus=="Cancelled" ? "Cancellation accounting review required" : row.AutomateStatus=="Unscheduled"?"Unscheduled job accounting review":"Invoice adjustment needs review",
            row.AutomateStatus=="Cancelled" ? "This AutoMate job was cancelled after an invoice was issued. Xero remains unchanged." : row.AutomateStatus=="Unscheduled"?"This job returned to Unscheduled after an invoice was issued. Xero remains unchanged.":"AutoMate and current THREED invoice evidence differ. Review the exact remaining adjustment.",
            row.AutomateStatus is "Cancelled" or "Unscheduled" ? "Open Payments and record the explicit accounting decision." : "Open Payments, update AutoMate from the displayed THREED changes, then complete the exact invoice action shown.",
            "high", $"/jobs/{row.JobId:D}", "Payments", "invoice-reconciliation", null,row.Detected,row.Changes,row.Provider,row.External,null,
            string.IsNullOrWhiteSpace(row.Fingerprint)?$"invoice:{row.External}:{row.AutomateStatus}":row.Fingerprint,ct);
        }

        // Current failed/review actions are persisted independently. Pending untouched
        // siblings are not Attention items.
        const string actionSql = """
        SELECT r.job_id,a.action_id,a.action_key,a.status,
               COALESCE(NULLIF(a.error_message,''),NULLIF(a.review_reason,''),'Review this changed-job action.'),
               r.detected_at
        FROM public.basic_job_change_runs r
        JOIN public.basic_job_change_run_actions a ON a.run_id=r.run_id
        WHERE r.tenant_id=@tenant AND a.status IN ('failed','review_required') AND FALSE
        """;
        var actionRows = new List<(Guid JobId,Guid ActionId,string ActionKey,string Status,string Detail,DateTimeOffset Detected)>();
        await using (var command = new NpgsqlCommand(actionSql, connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                actionRows.Add((reader.GetGuid(0),reader.GetGuid(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),DatabaseTimeSupport.ReadRequired(reader,5)));
            }
        }
        foreach(var row in actionRows)
        {
                var technical = row.Status == "failed" && IsTechnical(row.Detail);
                var reference = technical ? Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(row.Detail)))[..12] : null;
                var tab = row.ActionKey.Contains("invoice", StringComparison.OrdinalIgnoreCase) ? "Payments" : "Overview";
                var section = tab == "Payments" ? "invoice-reconciliation" : $"schedule-action-{NormalizeAction(row.ActionKey)}";
                await UpsertAsync(connection, tenantId, row.JobId, technical ? "technical_failure" : "workflow_action",
                    row.ActionKey, technical ? "technical_failure" : "failed_action",
                    technical ? "AutoMate service error — no customer action was completed" : ActionTitle(row.ActionKey),
                    technical ? $"AutoMate could not complete this action. Reference {reference}." : row.Detail,
                    technical ? "Retry only this failed action after AutoMate is updated." : "Review and retry only this failed action.",
                    "high", $"/jobs/{row.JobId:D}", tab, section, null,row.Detected, "[]", null, row.ActionId.ToString("D"), reference, ct);
        }

        const string lifecycleSql = """
        SELECT job_id,automate_status,threed_record_state,threed_complete,
               COALESCE(source_missing_at,threed_complete_observed_at,lifecycle_updated_at,updated_at,NOW())
        FROM public.jobs_staging
        WHERE tenant_id::text=@tenant AND (threed_record_state IN ('missing','reappeared') OR threed_complete=true)
        """;
        var lifecycleRows=new List<(Guid JobId,string Status,string RecordState,bool Complete,DateTimeOffset Detected)>();
        await using(var command=new NpgsqlCommand(lifecycleSql,connection))
        {command.Parameters.AddWithValue("tenant",tenantId.ToString("D"));await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))lifecycleRows.Add((reader.GetGuid(0),reader.GetString(1),reader.GetString(2),reader.GetBoolean(3),DatabaseTimeSupport.ReadRequired(reader,4)));}
        foreach(var row in lifecycleRows)
        {
            if(row.RecordState=="missing")await UpsertAsync(connection,tenantId,row.JobId,"threed_deleted","remove_job","source_missing","THREED job deleted — remove from AutoMate?","Repeated successful tenant-wide scans no longer find this THREED JobID. AutoMate has not changed its lifecycle state.","Review retained provider/accounting evidence, then explicitly remove this job from active AutoMate data or keep it.","high",$"/jobs/{row.JobId:D}","Overview","source-removal-review",null,row.Detected,"[]",null,null,null,ct);
            if(row.RecordState=="reappeared")await UpsertAsync(connection,tenantId,row.JobId,"threed_reappeared","restore_job","source_reappeared","Removed THREED JobID has reappeared","This JobID was previously removed from active AutoMate data and has now reappeared in THREED. It was not silently restored.","Review the tombstone and current THREED record before explicitly restoring or leaving it removed.","high",$"/jobs/{row.JobId:D}","Overview","source-removal-review",null,row.Detected,"[]",null,null,null,ct);
            if(row.Complete&&row.Status=="Scheduled")await UpsertAsync(connection,tenantId,row.JobId,"threed_complete","report","report_review","THREED marks the source job complete — review report release","THREED Complete is source evidence only. AutoMate remains Scheduled until Company SMTP accepts the report email.","Open Documents, verify Terms/payment/release evidence, then explicitly publish and send the report.","medium",$"/jobs/{row.JobId:D}","Documents","report-status",null,row.Detected,"[]",null,null,null,ct);
            if(row.Complete&&(row.Status is "Unscheduled" or "Cancelled"))await UpsertAsync(connection,tenantId,row.JobId,"threed_complete_invalid_state","report","report_review","THREED Complete needs lifecycle review",$"THREED marks this source record complete while AutoMate is {row.Status}. No report action was started.","Review the AutoMate lifecycle and report evidence. Do not send automatically.","high",$"/jobs/{row.JobId:D}","Overview","job-progress",null,row.Detected,"[]",null,null,null,ct);
        }

        // Open records whose underlying legacy/evidence condition disappeared are retained
        // as superseded history and leave the unresolved count.
        const string sweepSql = """
        UPDATE public.job_attention_reviews r SET status='superseded',superseded_at=NOW(),updated_at=NOW(),
          action_version=nextval('public.job_required_action_version_seq')
        WHERE r.tenant_id=@tenant AND r.status='open' AND r.job_id IS NOT NULL AND (
          (@legacy AND r.reason_key='xero_review' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.xero_review_required AND j.invoice_sent)) OR
          (@legacy AND r.reason_key='mapping_review' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.mapping_review_required)) OR
          (@legacy AND r.reason_key='missing_template' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.change_template_setup_required)) OR
          (r.reason_key='threed_deleted' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.threed_record_state='missing')) OR
          (r.reason_key='threed_reappeared' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.threed_record_state='reappeared')) OR
          (r.reason_key='threed_complete' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.threed_complete AND j.automate_status='Scheduled')) OR
          (r.reason_key='threed_complete_invalid_state' AND NOT EXISTS (SELECT 1 FROM public.jobs_staging j WHERE j.job_id=r.job_id AND j.tenant_id::text=@tenant_text AND j.threed_complete AND j.automate_status IN ('Unscheduled','Cancelled'))) OR
          (r.reason_key IN ('workflow_action','technical_failure') AND NOT EXISTS (
             SELECT 1 FROM public.basic_job_change_runs cr JOIN public.basic_job_change_run_actions ca ON ca.run_id=cr.run_id
             WHERE cr.tenant_id=@tenant AND cr.job_id=r.job_id AND ca.action_key=r.action_key AND ca.status IN ('failed','review_required')))
        );
        """;
        await using (var sweep = new NpgsqlCommand(sweepSql, connection))
        {
            sweep.Parameters.AddWithValue("tenant", tenantId);
            sweep.Parameters.AddWithValue("tenant_text", tenantId.ToString("D"));
            sweep.Parameters.AddWithValue("legacy", migrateLegacyFlags);
            await sweep.ExecuteNonQueryAsync(ct);
        }
    }

    public static async Task<IReadOnlyList<AuthoritativeAttentionItem>> LoadCurrentAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        var result = new List<AuthoritativeAttentionItem>();
        const string sql = """
        SELECT r.attention_id,r.job_id,COALESCE(NULLIF(j.site_address,''),NULLIF(j.job_name,''),r.job_id::text,'Company'),
               COALESCE(j.inspector_name,''),r.action_key,r.classification,r.title,r.detail,r.recommended_action,
               r.detected_at,r.severity,r.target_route,r.target_tab,r.target_section,r.status,r.template_slot,
               r.technical_reference_id,r.changes_json::text,r.provider_status,r.external_id,r.resolved_at,r.superseded_at,
               r.action_version,r.evidence_fingerprint
        FROM public.job_attention_reviews r
        LEFT JOIN public.jobs_staging j ON j.job_id=r.job_id AND j.tenant_id::text=r.tenant_id::text
        WHERE r.tenant_id=@tenant AND r.status='open'
        ORDER BY CASE r.severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,r.detected_at DESC
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            using var changes = JsonDocument.Parse(reader.GetString(17));
            result.Add(new(reader.GetGuid(0), reader.IsDBNull(1) ? null : reader.GetGuid(1), reader.GetString(2),
                reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetString(6), reader.GetString(7),
                reader.GetString(8), DatabaseTimeSupport.ReadRequired(reader,9), reader.GetString(10), reader.GetString(11),
                reader.GetString(12), reader.GetString(13), reader.GetString(14), reader.IsDBNull(15) ? null : reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16), changes.RootElement.Clone(),
                reader.IsDBNull(18) ? null : reader.GetString(18), reader.IsDBNull(19) ? null : reader.GetString(19),
                DatabaseTimeSupport.ReadNullable(reader,20),
                DatabaseTimeSupport.ReadNullable(reader,21),reader.GetInt64(22),reader.GetString(23)));
        }
        return result;
    }

    public static async Task ResolveAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string reasonKey, string actionKey, string status = "resolved", CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand("UPDATE public.job_attention_reviews SET status=@status,resolved_at=CASE WHEN @status='resolved' THEN NOW() ELSE resolved_at END,superseded_at=CASE WHEN @status='superseded' THEN NOW() ELSE superseded_at END,updated_at=NOW(),action_version=nextval('public.job_required_action_version_seq') WHERE tenant_id=@tenant AND job_id=@job AND reason_key=@reason AND (@action='' OR action_key=@action) AND status='open'", connection);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.AddWithValue("reason", reasonKey);
        command.Parameters.AddWithValue("action", actionKey ?? "");
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task ResolveTemplateSlotAsync(NpgsqlConnection connection, Guid tenantId, string templateSlot,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand("""
        UPDATE public.job_attention_reviews SET status='resolved',resolved_at=NOW(),updated_at=NOW(),
          action_version=nextval('public.job_required_action_version_seq')
        WHERE tenant_id=@tenant AND reason_key='missing_template' AND template_slot=@slot AND status='open'
        """, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("slot", templateSlot);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task ResolveTenantReasonAsync(NpgsqlConnection connection, Guid tenantId, string reasonKey,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand("""
        UPDATE public.job_attention_reviews SET status='resolved',resolved_at=NOW(),updated_at=NOW(),
          action_version=nextval('public.job_required_action_version_seq')
        WHERE tenant_id=@tenant AND reason_key=@reason AND status='open'
        """, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("reason", reasonKey);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<long> LoadVersionAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        await using var command = new NpgsqlCommand("""
        SELECT COALESCE(MAX(action_version),0)
        FROM (
          SELECT action_version FROM public.job_attention_reviews WHERE tenant_id=@tenant
          UNION ALL
          SELECT a.action_version FROM public.basic_job_change_run_actions a WHERE a.tenant_id=@tenant
        ) versions
        """, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    public static async Task MarkMigrationAsync(NpgsqlConnection connection, Guid tenantId, string actor, int openCount, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand("""
        INSERT INTO public.required_action_ledger_migrations(tenant_id,migration_version,migrated_by,result_json)
        VALUES(@tenant,@version,@actor,jsonb_build_object('openActions',@count,'legacyFlagsAreDisplayDisabled',true))
        ON CONFLICT(tenant_id) DO UPDATE SET migration_version=EXCLUDED.migration_version,migrated_at=NOW(),
          migrated_by=EXCLUDED.migrated_by,result_json=EXCLUDED.result_json
        """, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("version", ContractVersion);
        command.Parameters.AddWithValue("actor", actor);
        command.Parameters.AddWithValue("count", openCount);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<bool> HasMigrationAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS(SELECT 1 FROM public.required_action_ledger_migrations WHERE tenant_id=@tenant AND migration_version>=@version)", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("version", ContractVersion);
        return Convert.ToBoolean(await command.ExecuteScalarAsync(ct));
    }

    public static async Task RecordLifecycleInvoiceReviewAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        string lifecycleStatus, int lifecycleVersion, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        string invoiceId="",invoiceStatus="";bool invoiceSent=false;
        await using(var command=new NpgsqlCommand("SELECT invoice_sent,COALESCE(xero_invoice_id,''),COALESCE(xero_invoice_status,'') FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job",connection))
        {
            command.Parameters.AddWithValue("tenant",tenantId.ToString("D"));command.Parameters.AddWithValue("job",jobId);
            await using var reader=await command.ExecuteReaderAsync(ct);if(!await reader.ReadAsync(ct))return;
            invoiceSent=reader.GetBoolean(0);invoiceId=reader.GetString(1);invoiceStatus=reader.GetString(2);
        }
        if(!invoiceSent)return;
        var cancelled=lifecycleStatus==JobLifecycleSupport.Cancelled;
        var title=cancelled?"Cancellation accounting review required":"Unscheduled job accounting review";
        var detail=cancelled?"This AutoMate job was cancelled after an invoice was issued. Xero remains unchanged.":"This job is Unscheduled after an invoice was issued. Xero remains unchanged.";
        await UpsertAsync(connection,tenantId,jobId,"lifecycle_invoice_review","invoice","accounting_decision",title,detail,
            "Open Payments and record the explicit accounting decision.","high",$"/jobs/{jobId:D}","Payments","invoice-reconciliation",null,
            DateTimeOffset.UtcNow,"[]",invoiceStatus,invoiceId,null,$"lifecycle:{lifecycleVersion}:{lifecycleStatus}:{invoiceId}",ct);
    }

    private static async Task UpsertAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string reasonKey,
        string actionKey, string classification, string title, string detail, string recommendation, string severity,
        string route, string tab, string section, string? templateSlot, DateTimeOffset detectedAt, string changesJson,
        string? providerStatus, string? externalId, string? technicalReference, CancellationToken ct)
        => await UpsertAsync(connection,tenantId,jobId,reasonKey,actionKey,classification,title,detail,recommendation,severity,
            route,tab,section,templateSlot,detectedAt,changesJson,providerStatus,externalId,technicalReference,null,ct);

    private static async Task UpsertAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string reasonKey,
        string actionKey, string classification, string title, string detail, string recommendation, string severity,
        string route, string tab, string section, string? templateSlot, DateTimeOffset detectedAt, string changesJson,
        string? providerStatus, string? externalId, string? technicalReference, string? evidenceFingerprint, CancellationToken ct)
    {
        var evidence=string.IsNullOrWhiteSpace(evidenceFingerprint)?detectedAt.ToString("O"):evidenceFingerprint.Trim();
        var incidentKey=RequiredActionPolicySupport.IncidentKey(reasonKey,actionKey,evidence);
        if (incidentKey.Length > 0)
        {
            await using var supersedeOlder = new NpgsqlCommand("""
            UPDATE public.job_attention_reviews
            SET status='superseded',superseded_at=COALESCE(superseded_at,NOW()),updated_at=NOW(),
                action_version=nextval('public.job_required_action_version_seq')
            WHERE tenant_id=@tenant AND job_id=@job AND reason_key=@reason AND action_key=@action
              AND status='open' AND incident_key<>@incident
            """, connection);
            supersedeOlder.Parameters.AddWithValue("tenant", tenantId);
            supersedeOlder.Parameters.AddWithValue("job", jobId);
            supersedeOlder.Parameters.AddWithValue("reason", reasonKey);
            supersedeOlder.Parameters.AddWithValue("action", actionKey);
            supersedeOlder.Parameters.AddWithValue("incident", incidentKey);
            await supersedeOlder.ExecuteNonQueryAsync(ct);
        }
        const string sql = """
        INSERT INTO public.job_attention_reviews(attention_id,tenant_id,job_id,reason_key,action_key,incident_key,classification,title,detail,
          recommended_action,severity,status,target_route,target_tab,target_section,template_slot,changes_json,provider_status,
          external_id,technical_reference_id,detected_at,evidence_fingerprint)
        VALUES(@id,@tenant,@job,@reason,@action,@incident,@class,@title,@detail,@recommendation,@severity,'open',@route,@tab,@section,
          @slot,CAST(@changes AS jsonb),@provider,@external,@technical,@detected,@evidence)
        ON CONFLICT(tenant_id,job_id,reason_key,action_key,incident_key) DO UPDATE SET
          classification=EXCLUDED.classification,title=EXCLUDED.title,detail=EXCLUDED.detail,
          recommended_action=EXCLUDED.recommended_action,severity=EXCLUDED.severity,target_route=EXCLUDED.target_route,
          target_tab=EXCLUDED.target_tab,target_section=EXCLUDED.target_section,template_slot=EXCLUDED.template_slot,
          changes_json=EXCLUDED.changes_json,provider_status=EXCLUDED.provider_status,external_id=EXCLUDED.external_id,
          technical_reference_id=EXCLUDED.technical_reference_id,evidence_fingerprint=EXCLUDED.evidence_fingerprint,
          action_version=nextval('public.job_required_action_version_seq'),updated_at=NOW()
        WHERE public.job_attention_reviews.status='open'
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("id", DeterministicId(tenantId, jobId, reasonKey, actionKey+"|"+incidentKey));
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.AddWithValue("reason", reasonKey);
        command.Parameters.AddWithValue("action", actionKey);
        command.Parameters.AddWithValue("incident",incidentKey);
        command.Parameters.AddWithValue("class", classification);
        command.Parameters.AddWithValue("title", title);
        command.Parameters.AddWithValue("detail", detail);
        command.Parameters.AddWithValue("recommendation", recommendation);
        command.Parameters.AddWithValue("severity", severity);
        command.Parameters.AddWithValue("route", route);
        command.Parameters.AddWithValue("tab", tab);
        command.Parameters.AddWithValue("section", section);
        command.Parameters.AddWithValue("slot", (object?)templateSlot ?? DBNull.Value);
        command.Parameters.AddWithValue("changes", changesJson);
        command.Parameters.AddWithValue("provider", (object?)providerStatus ?? DBNull.Value);
        command.Parameters.AddWithValue("external", (object?)externalId ?? DBNull.Value);
        command.Parameters.AddWithValue("technical", (object?)technicalReference ?? DBNull.Value);
        command.Parameters.AddWithValue("detected", detectedAt);
        command.Parameters.AddWithValue("evidence", evidence);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SupersedeAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string reasonKey, string actionKey, CancellationToken ct)
        => await ResolveAsync(connection, tenantId, jobId, reasonKey, actionKey, "superseded", ct);

    private static async Task ClearLegacyFlagAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string field, CancellationToken ct)
    {
        if (field is not ("change_template_setup_required" or "mapping_review_required")) return;
        await using var command = new NpgsqlCommand($"UPDATE public.jobs_staging SET {field}=false,workflow_updated_at=NOW() WHERE tenant_id::text=@tenant AND job_id=@job", connection);
        command.Parameters.AddWithValue("tenant", tenantId.ToString("D"));
        command.Parameters.AddWithValue("job", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string NormalizeChanges(string value)
    {
        try { using var document = JsonDocument.Parse(value); return document.RootElement.ValueKind == JsonValueKind.Array ? value : "[]"; }
        catch { return "[]"; }
    }
    private static bool ContainsChange(string value, params string[] keys) => keys.Any(key => value.Contains(key, StringComparison.OrdinalIgnoreCase));
    private static bool IsTechnical(string value) => value.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) || value.Contains("column ", StringComparison.OrdinalIgnoreCase) || value.Contains("InternalServerError", StringComparison.OrdinalIgnoreCase) || value.Contains("operator does not exist", StringComparison.OrdinalIgnoreCase);
    private static string NormalizeAction(string value) => value.Contains("booking", StringComparison.OrdinalIgnoreCase) ? "email" : value.Contains("term", StringComparison.OrdinalIgnoreCase) ? "terms" : value.Contains("invoice", StringComparison.OrdinalIgnoreCase) ? "invoice" : value.Contains("calendar", StringComparison.OrdinalIgnoreCase) ? "calendar" : value;
    private static string ActionTitle(string value) => $"Review failed {NormalizeAction(value).Replace('_', ' ')}";
    private static Guid DeterministicId(Guid tenantId, Guid jobId, string reason, string action)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes($"{tenantId:D}|{jobId:D}|{reason}|{action}"));
        return new Guid(bytes[..16]);
    }
}
