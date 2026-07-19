using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class TenantAgreementPolicySupport
{
    public const int ContractVersion = 1;
    public const string ReviewRequired = "review_required";
    public const string Required = "required";
    public const string NotRequired = "not_required";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.tenant_agreement_policy_state
(
 tenant_id uuid PRIMARY KEY,draft_version integer NOT NULL DEFAULT 0,active_version integer NOT NULL DEFAULT 0,
 draft_json jsonb NOT NULL DEFAULT '{"contractVersion":1,"catalogueVersion":0,"services":[]}'::jsonb,
 updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.tenant_agreement_policy_versions
(
 tenant_id uuid NOT NULL,policy_version integer NOT NULL,policy_json jsonb NOT NULL,policy_fingerprint text NOT NULL,
 catalogue_version integer NOT NULL,created_by text NOT NULL,created_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,policy_version)
);
CREATE TABLE IF NOT EXISTS public.tenant_agreement_policy_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,action_key text NOT NULL,
 draft_version integer NOT NULL,policy_version integer NOT NULL,actor text NOT NULL,detail_json jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.job_agreement_plans
(
 plan_id uuid PRIMARY KEY,tenant_id uuid NOT NULL,job_id uuid NOT NULL,plan_version integer NOT NULL DEFAULT 1,
 policy_version integer NOT NULL,catalogue_version integer NOT NULL,handler_key text NOT NULL,provider_key text NOT NULL DEFAULT '',
 service_fingerprint text NOT NULL,plan_fingerprint text NOT NULL,review_required boolean NOT NULL DEFAULT false,
 captured_by text NOT NULL,captured_at timestamptz NOT NULL DEFAULT NOW(),updated_at timestamptz NOT NULL DEFAULT NOW(),
 UNIQUE(tenant_id,job_id,plan_version)
);
CREATE TABLE IF NOT EXISTS public.job_agreement_items
(
 plan_item_id uuid PRIMARY KEY,plan_id uuid NOT NULL REFERENCES public.job_agreement_plans(plan_id) ON DELETE RESTRICT,
 tenant_id uuid NOT NULL,job_id uuid NOT NULL,service_id uuid NOT NULL,service_name text NOT NULL,
 handler_key text NOT NULL,provider_key text NOT NULL DEFAULT '',template_id text NOT NULL DEFAULT '',template_name text NOT NULL DEFAULT '',
 status text NOT NULL DEFAULT 'not_prepared',external_document_id text NOT NULL DEFAULT '',external_invite_id text NOT NULL DEFAULT '',
 signed_at timestamptz NULL,created_at timestamptz NOT NULL DEFAULT NOW(),updated_at timestamptz NOT NULL DEFAULT NOW(),
 UNIQUE(plan_id,service_id),CONSTRAINT ck_job_agreement_item_status CHECK(status IN ('not_prepared','prepared','invited','signed','failed','superseded'))
);
CREATE TABLE IF NOT EXISTS public.job_agreement_report_overrides
(
 override_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,job_id uuid NOT NULL,plan_id uuid NOT NULL,
 plan_fingerprint text NOT NULL,reason text NOT NULL,actor text NOT NULL,active boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_job_agreement_plan_job ON public.job_agreement_plans(tenant_id,job_id,plan_version DESC);
CREATE INDEX IF NOT EXISTS idx_job_agreement_item_job ON public.job_agreement_items(tenant_id,job_id,status);
CREATE INDEX IF NOT EXISTS idx_job_agreement_override_job ON public.job_agreement_report_overrides(tenant_id,job_id,created_at DESC);
DO $$ BEGIN IF to_regclass('public.jobs_staging') IS NOT NULL THEN
 ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS agreement_plan_id uuid NULL;
 ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS agreement_plan_fingerprint text NOT NULL DEFAULT '';
 ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS agreement_policy_version integer NOT NULL DEFAULT 0;
 ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS agreement_handler_key text NOT NULL DEFAULT '';
 ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS agreement_plan_review_required boolean NOT NULL DEFAULT false;
END IF; END $$;
""";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<AgreementPolicyView> LoadAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        await EnsureSeedAsync(connection, tenantId, ct);
        int draftVersion, activeVersion;
        AgreementPolicyDraft stored;
        await using (var command = new NpgsqlCommand("SELECT draft_version,active_version,draft_json::text FROM public.tenant_agreement_policy_state WHERE tenant_id=@tenant", connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId);
            await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
            draftVersion = reader.GetInt32(0); activeVersion = reader.GetInt32(1);
            stored = JsonSerializer.Deserialize<AgreementPolicyDraft>(reader.GetString(2), JsonOptions) ?? EmptyDraft();
        }
        var merged = await MergeWithCatalogueAsync(connection, tenantId, stored, false, ct);
        var validation = await ValidateAsync(connection, tenantId, merged, false, ct);
        var handler = await ResolveHandlerAsync(connection, tenantId, ct);
        var readiness = await TemplateReadinessAsync(connection, tenantId, merged, handler, ct);
        return new(draftVersion, activeVersion, merged, validation, handler, readiness);
    }

    public static async Task<AgreementPolicySaveResult> SaveDraftAsync(NpgsqlConnection connection, Guid tenantId,
        AgreementPolicySaveRequest request, string actor, CancellationToken ct = default)
    {
        if (!request.Confirmed) return new(false, "confirmation_required", request.ExpectedDraftVersion, 0, EmptyValidation("Confirm the agreement policy draft save."), "Confirm the agreement policy draft save.");
        var current = await LoadAsync(connection, tenantId, ct);
        if (current.DraftVersion != request.ExpectedDraftVersion)
            return new(false, "version_conflict", current.DraftVersion, current.ActiveVersion, current.Validation, "Agreement policy changed; reload and review it again.");
        var merged = await MergeWithCatalogueAsync(connection, tenantId, request.Draft ?? EmptyDraft(), false, ct);
        var validation = await ValidateAsync(connection, tenantId, merged, false, ct);
        var next = current.DraftVersion + 1;
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var update = new NpgsqlCommand("UPDATE public.tenant_agreement_policy_state SET draft_version=@version,draft_json=@json::jsonb,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant AND draft_version=@expected", connection, tx))
        {
            update.Parameters.AddWithValue("tenant", tenantId); update.Parameters.AddWithValue("version", next);
            update.Parameters.AddWithValue("expected", current.DraftVersion); update.Parameters.AddWithValue("json", JsonSerializer.Serialize(merged, JsonOptions)); update.Parameters.AddWithValue("actor", actor);
            if (await update.ExecuteNonQueryAsync(ct) != 1) { await tx.RollbackAsync(ct); return new(false, "version_conflict", current.DraftVersion, current.ActiveVersion, validation, "Agreement policy changed; reload and review it again."); }
        }
        await AuditAsync(connection, tx, tenantId, "agreement_policy_draft_saved", next, current.ActiveVersion, actor, new { validation.Valid }, ct);
        await tx.CommitAsync(ct);
        return new(true, "saved", next, current.ActiveVersion, validation, "Agreement policy draft saved. No job or provider action was changed.");
    }

    public static async Task<AgreementPolicySaveResult> ActivateAsync(NpgsqlConnection connection, Guid tenantId,
        AgreementPolicyActivateRequest request, string actor, CancellationToken ct = default)
    {
        if (!request.Confirmed) return new(false, "confirmation_required", request.ExpectedDraftVersion, 0, EmptyValidation("Confirm agreement policy activation."), "Confirm agreement policy activation.");
        var current = await LoadAsync(connection, tenantId, ct);
        if (current.DraftVersion != request.ExpectedDraftVersion)
            return new(false, "version_conflict", current.DraftVersion, current.ActiveVersion, current.Validation, "Agreement policy changed; reload before activation.");
        var validation = await ValidateAsync(connection, tenantId, current.Draft, true, ct);
        if (!validation.Valid) return new(false, "policy_invalid", current.DraftVersion, current.ActiveVersion, validation, "Classify every active Service before activation.");
        var next = current.ActiveVersion + 1; var json = JsonSerializer.Serialize(current.Draft, JsonOptions); var fingerprint = Hash(json);
        var requiredIds = current.Draft.Services.Where(x => x.Requirement == Required).Select(x => x.ServiceId).ToArray();
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var insert = new NpgsqlCommand("INSERT INTO public.tenant_agreement_policy_versions(tenant_id,policy_version,policy_json,policy_fingerprint,catalogue_version,created_by) VALUES(@tenant,@version,@json::jsonb,@fingerprint,@catalogue,@actor)", connection, tx))
        {
            insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("version", next); insert.Parameters.AddWithValue("json", json);
            insert.Parameters.AddWithValue("fingerprint", fingerprint); insert.Parameters.AddWithValue("catalogue", current.Draft.CatalogueVersion); insert.Parameters.AddWithValue("actor", actor); await insert.ExecuteNonQueryAsync(ct);
        }
        await using (var update = new NpgsqlCommand("UPDATE public.tenant_agreement_policy_state SET active_version=@version,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant AND draft_version=@draft", connection, tx))
        {
            update.Parameters.AddWithValue("tenant", tenantId); update.Parameters.AddWithValue("version", next); update.Parameters.AddWithValue("draft", current.DraftVersion); update.Parameters.AddWithValue("actor", actor);
            if (await update.ExecuteNonQueryAsync(ct) != 1) { await tx.RollbackAsync(ct); return new(false, "version_conflict", current.DraftVersion, current.ActiveVersion, validation, "Agreement policy changed during activation."); }
        }
        if (await TableExistsAsync(connection, tx, "tenant_signnow_catalogue_mappings", ct))
        {
            await using var archive = new NpgsqlCommand("UPDATE public.tenant_signnow_catalogue_mappings SET active=false,updated_at=NOW() WHERE tenant_id=@tenant AND active=true AND NOT(target_id=ANY(@required))", connection, tx);
            archive.Parameters.AddWithValue("tenant", tenantId); archive.Parameters.AddWithValue("required", requiredIds); await archive.ExecuteNonQueryAsync(ct);
        }
        await AuditAsync(connection, tx, tenantId, "agreement_policy_activated", current.DraftVersion, next, actor, new { fingerprint, requiredServiceIds = requiredIds }, ct);
        await tx.CommitAsync(ct);
        return new(true, "activated", current.DraftVersion, next, validation, "Agreement policy activated for newly scheduled jobs. Existing plans were not changed.");
    }

    public static async Task<AgreementPlanPreview> PreviewJobAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        var existing = await LoadJobPlanAsync(connection, tenantId, jobId, ct);
        if (existing is not null) return ExistingPreview(existing);
        int catalogueVersion; string snapshot, email;
        await using (var command = new NpgsqlCommand("SELECT COALESCE(service_catalogue_version,0),COALESCE(service_catalogue_snapshot_json::text,''),COALESCE(contact1_email,'') FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job", connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId.ToString()); command.Parameters.AddWithValue("job", jobId);
            await using var reader = await command.ExecuteReaderAsync(ct); if (!await reader.ReadAsync(ct)) return AgreementPlanPreview.Failed("job_not_found", "The job was not found for this company.");
            catalogueVersion = reader.GetInt32(0); snapshot = reader.GetString(1); email = reader.GetString(2);
        }
        return await PreviewSnapshotAsync(connection, tenantId, catalogueVersion, snapshot, string.IsNullOrWhiteSpace(email), ct);
    }

    public static async Task<AgreementSchedulingGate> CheckSchedulingGateAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        var preview = await PreviewJobAsync(connection, tenantId, jobId, ct);
        var allowed = preview.Status is "selected" or "not_required";
        return new(allowed, preview.Status, preview.Message, preview.PolicyVersion, preview.CatalogueVersion, preview.Agreements.Count, preview.Handler);
    }

    public static async Task<AgreementSchedulingGate> CaptureForSchedulingAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string actor, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        var existing = await LoadJobPlanAsync(connection, tenantId, jobId, ct);
        if (existing is not null)
        {
            var existingPreview = ExistingPreview(existing); var ok = !existing.ReviewRequired && (existingPreview.Status is "selected" or "not_required");
            return new(ok, ok ? existingPreview.Status : "plan_review_required", ok ? "The frozen agreement plan is current." : "Review the frozen agreement plan before scheduling.", existing.PolicyVersion, existing.CatalogueVersion, existing.Items.Count, existing.Handler);
        }
        var preview = await PreviewJobAsync(connection, tenantId, jobId, ct);
        if (preview.Status is not ("selected" or "not_required")) return new(false, preview.Status, preview.Message, preview.PolicyVersion, preview.CatalogueVersion, preview.Agreements.Count, preview.Handler);
        var serviceFingerprint = Hash(string.Join("|", preview.SelectedServiceIds.OrderBy(x => x)));
        var planFingerprint = Hash(JsonSerializer.Serialize(new { preview.PolicyVersion, preview.CatalogueVersion, preview.Handler.HandlerKey, preview.Handler.ProviderKey, preview.Agreements }, JsonOptions));
        var planId = Guid.NewGuid();
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var rowLock = new NpgsqlCommand("SELECT job_id FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job FOR UPDATE", connection, tx))
        { rowLock.Parameters.AddWithValue("tenant", tenantId.ToString()); rowLock.Parameters.AddWithValue("job", jobId); if (await rowLock.ExecuteScalarAsync(ct) is null) { await tx.RollbackAsync(ct); return new(false, "job_not_found", "The job was not found for this company.", 0, 0, 0, preview.Handler); } }
        await using (var insert = new NpgsqlCommand("INSERT INTO public.job_agreement_plans(plan_id,tenant_id,job_id,policy_version,catalogue_version,handler_key,provider_key,service_fingerprint,plan_fingerprint,captured_by) VALUES(@id,@tenant,@job,@policy,@catalogue,@handler,@provider,@services,@fingerprint,@actor)", connection, tx))
        {
            insert.Parameters.AddWithValue("id", planId); insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("job", jobId);
            insert.Parameters.AddWithValue("policy", preview.PolicyVersion); insert.Parameters.AddWithValue("catalogue", preview.CatalogueVersion); insert.Parameters.AddWithValue("handler", preview.Handler.HandlerKey); insert.Parameters.AddWithValue("provider", preview.Handler.ProviderKey);
            insert.Parameters.AddWithValue("services", serviceFingerprint); insert.Parameters.AddWithValue("fingerprint", planFingerprint); insert.Parameters.AddWithValue("actor", actor); await insert.ExecuteNonQueryAsync(ct);
        }
        foreach (var item in preview.Agreements)
        {
            await using var insert = new NpgsqlCommand("INSERT INTO public.job_agreement_items(plan_item_id,plan_id,tenant_id,job_id,service_id,service_name,handler_key,provider_key,template_id,template_name) VALUES(@id,@plan,@tenant,@job,@service,@name,@handler,@provider,@template,@templateName)", connection, tx);
            insert.Parameters.AddWithValue("id", Guid.NewGuid()); insert.Parameters.AddWithValue("plan", planId); insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("job", jobId);
            insert.Parameters.AddWithValue("service", item.ServiceId); insert.Parameters.AddWithValue("name", item.ServiceName); insert.Parameters.AddWithValue("handler", preview.Handler.HandlerKey); insert.Parameters.AddWithValue("provider", preview.Handler.ProviderKey);
            insert.Parameters.AddWithValue("template", item.TemplateId); insert.Parameters.AddWithValue("templateName", item.TemplateName); await insert.ExecuteNonQueryAsync(ct);
        }
        await using (var update = new NpgsqlCommand("UPDATE public.jobs_staging SET agreement_plan_id=@plan,agreement_plan_fingerprint=@fingerprint,agreement_policy_version=@policy,agreement_handler_key=@handler,agreement_plan_review_required=false,terms_required=@required WHERE tenant_id::text=@tenant AND job_id=@job", connection, tx))
        {
            update.Parameters.AddWithValue("plan", planId); update.Parameters.AddWithValue("fingerprint", planFingerprint); update.Parameters.AddWithValue("policy", preview.PolicyVersion); update.Parameters.AddWithValue("handler", preview.Handler.HandlerKey); update.Parameters.AddWithValue("required", preview.Agreements.Count > 0); update.Parameters.AddWithValue("tenant", tenantId.ToString()); update.Parameters.AddWithValue("job", jobId); await update.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        return new(true, preview.Status, preview.Agreements.Count == 0 ? "Frozen agreement plan captured: no agreements required." : $"Frozen agreement plan captured with {preview.Agreements.Count} required agreement(s). No provider action ran.", preview.PolicyVersion, preview.CatalogueVersion, preview.Agreements.Count, preview.Handler);
    }

    public static async Task<AgreementSchedulingGate> ReplaceUnsignedPlanForServiceChangeAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string actor, CancellationToken ct = default, bool allowSignedReplacement = false)
    {
        await EnsureAsync(connection, ct);
        var existing = await LoadJobPlanAsync(connection, tenantId, jobId, ct);
        if (existing is null) return await CaptureForSchedulingAsync(connection, tenantId, jobId, actor, ct);
        if (existing.Items.Any(item => item.Status == "signed") && !allowSignedReplacement)
            return new(false, "signed_agreement_review_required", "The existing agreement is signed. Review the changed booking details before sending a replacement agreement.", existing.PolicyVersion, existing.CatalogueVersion, existing.Items.Count, existing.Handler);

        int catalogueVersion; string snapshot, email;
        await using (var command = new NpgsqlCommand("SELECT COALESCE(service_catalogue_version,0),COALESCE(service_catalogue_snapshot_json::text,''),COALESCE(contact1_email,'') FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job", connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId.ToString()); command.Parameters.AddWithValue("job", jobId);
            await using var reader = await command.ExecuteReaderAsync(ct); if (!await reader.ReadAsync(ct)) return new(false, "job_not_found", "The job was not found for this company.", 0, 0, 0, existing.Handler);
            catalogueVersion = reader.GetInt32(0); snapshot = reader.GetString(1); email = reader.GetString(2);
        }
        var preview = await PreviewSnapshotAsync(connection, tenantId, catalogueVersion, snapshot, string.IsNullOrWhiteSpace(email), ct);
        if (preview.Status is not ("selected" or "not_required")) return new(false, preview.Status, preview.Message, preview.PolicyVersion, preview.CatalogueVersion, preview.Agreements.Count, preview.Handler);
        var serviceFingerprint = Hash(string.Join("|", preview.SelectedServiceIds.OrderBy(x => x)));
        var planFingerprint = Hash(JsonSerializer.Serialize(new { preview.PolicyVersion, preview.CatalogueVersion, preview.Handler.HandlerKey, preview.Handler.ProviderKey, preview.Agreements }, JsonOptions));
        var planId = Guid.NewGuid(); var planVersion = existing.PlanVersion + 1;
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var insert = new NpgsqlCommand("INSERT INTO public.job_agreement_plans(plan_id,tenant_id,job_id,plan_version,policy_version,catalogue_version,handler_key,provider_key,service_fingerprint,plan_fingerprint,captured_by) VALUES(@id,@tenant,@job,@planVersion,@policy,@catalogue,@handler,@provider,@services,@fingerprint,@actor)", connection, tx))
        {
            insert.Parameters.AddWithValue("id", planId); insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("job", jobId); insert.Parameters.AddWithValue("planVersion", planVersion);
            insert.Parameters.AddWithValue("policy", preview.PolicyVersion); insert.Parameters.AddWithValue("catalogue", preview.CatalogueVersion); insert.Parameters.AddWithValue("handler", preview.Handler.HandlerKey); insert.Parameters.AddWithValue("provider", preview.Handler.ProviderKey);
            insert.Parameters.AddWithValue("services", serviceFingerprint); insert.Parameters.AddWithValue("fingerprint", planFingerprint); insert.Parameters.AddWithValue("actor", actor); await insert.ExecuteNonQueryAsync(ct);
        }
        foreach (var item in preview.Agreements)
        {
            await using var insert = new NpgsqlCommand("INSERT INTO public.job_agreement_items(plan_item_id,plan_id,tenant_id,job_id,service_id,service_name,handler_key,provider_key,template_id,template_name) VALUES(@id,@plan,@tenant,@job,@service,@name,@handler,@provider,@template,@templateName)", connection, tx);
            insert.Parameters.AddWithValue("id", Guid.NewGuid()); insert.Parameters.AddWithValue("plan", planId); insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("job", jobId);
            insert.Parameters.AddWithValue("service", item.ServiceId); insert.Parameters.AddWithValue("name", item.ServiceName); insert.Parameters.AddWithValue("handler", preview.Handler.HandlerKey); insert.Parameters.AddWithValue("provider", preview.Handler.ProviderKey);
            insert.Parameters.AddWithValue("template", item.TemplateId); insert.Parameters.AddWithValue("templateName", item.TemplateName); await insert.ExecuteNonQueryAsync(ct);
        }
        await using (var supersede = new NpgsqlCommand("UPDATE public.job_agreement_items SET status='superseded',updated_at=NOW() WHERE plan_id=@oldPlan AND status<>'signed'", connection, tx))
        { supersede.Parameters.AddWithValue("oldPlan", existing.PlanId); await supersede.ExecuteNonQueryAsync(ct); }
        await using (var update = new NpgsqlCommand("UPDATE public.jobs_staging SET agreement_plan_id=@plan,agreement_plan_fingerprint=@fingerprint,agreement_policy_version=@policy,agreement_handler_key=@handler,agreement_plan_review_required=false,terms_required=@required WHERE tenant_id::text=@tenant AND job_id=@job", connection, tx))
        {
            update.Parameters.AddWithValue("plan", planId); update.Parameters.AddWithValue("fingerprint", planFingerprint); update.Parameters.AddWithValue("policy", preview.PolicyVersion); update.Parameters.AddWithValue("handler", preview.Handler.HandlerKey);
            update.Parameters.AddWithValue("required", preview.Agreements.Count > 0); update.Parameters.AddWithValue("tenant", tenantId.ToString()); update.Parameters.AddWithValue("job", jobId); await update.ExecuteNonQueryAsync(ct);
        }
        await AuditAsync(connection, tx, tenantId, allowSignedReplacement ? "signed_agreement_replacement_explicitly_approved" : "unsigned_agreement_plan_replaced_for_service_change", existing.PolicyVersion, preview.PolicyVersion, actor, new { jobId, previousPlanId = existing.PlanId, replacementPlanId = planId, planVersion }, ct);
        await tx.CommitAsync(ct);
        return new(true, preview.Status, preview.Agreements.Count == 0 ? "The changed Service no longer requires an agreement." : "The changed Service agreement mapping was captured for automatic unsigned replacement.", preview.PolicyVersion, preview.CatalogueVersion, preview.Agreements.Count, preview.Handler);
    }

    public static async Task<JobAgreementPlanView?> LoadJobPlanAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct); Guid planId; int planVersion, policyVersion, catalogueVersion; string handler, provider, serviceFingerprint, planFingerprint; bool review; DateTime capturedAt;
        await using (var command = new NpgsqlCommand("SELECT plan_id,plan_version,policy_version,catalogue_version,handler_key,provider_key,service_fingerprint,plan_fingerprint,review_required,captured_at FROM public.job_agreement_plans WHERE tenant_id=@tenant AND job_id=@job ORDER BY plan_version DESC LIMIT 1", connection))
        {
            command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId); await using var reader = await command.ExecuteReaderAsync(ct); if (!await reader.ReadAsync(ct)) return null;
            planId = reader.GetGuid(0); planVersion = reader.GetInt32(1); policyVersion = reader.GetInt32(2); catalogueVersion = reader.GetInt32(3); handler = reader.GetString(4); provider = reader.GetString(5); serviceFingerprint = reader.GetString(6); planFingerprint = reader.GetString(7); review = reader.GetBoolean(8); capturedAt = reader.GetDateTime(9);
        }
        var items = new List<JobAgreementItemView>();
        await using (var command = new NpgsqlCommand("SELECT plan_item_id,service_id,service_name,handler_key,provider_key,template_id,template_name,status,external_document_id,external_invite_id,signed_at FROM public.job_agreement_items WHERE plan_id=@plan ORDER BY service_name", connection))
        {
            command.Parameters.AddWithValue("plan", planId); await using var reader = await command.ExecuteReaderAsync(ct); while (await reader.ReadAsync(ct)) items.Add(new(reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetString(6), reader.GetString(7), reader.GetString(8), reader.GetString(9), reader.IsDBNull(10) ? null : reader.GetDateTime(10)));
        }
        bool overridden; string overrideReason = "";
        await using (var command = new NpgsqlCommand("SELECT reason FROM public.job_agreement_report_overrides WHERE tenant_id=@tenant AND job_id=@job AND plan_id=@plan AND plan_fingerprint=@fingerprint AND active=true ORDER BY created_at DESC LIMIT 1", connection))
        { command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("job", jobId); command.Parameters.AddWithValue("plan", planId); command.Parameters.AddWithValue("fingerprint", planFingerprint); var value = await command.ExecuteScalarAsync(ct); overridden = value is string; overrideReason = value as string ?? ""; }
        var reportGate = ReportGate(items, review, overridden, overrideReason);
        var currentHandler = await ResolveHandlerAsync(connection, tenantId, ct);
        var available = handler == "integrated" && currentHandler.Available && currentHandler.ProviderKey == provider;
        var handlerMessage = handler == "native" ? "Native agreement authoring is not available yet." : available ? "The captured signing integration remains connected." : "The signing integration captured by this plan is not connected. AutoMate will not switch this job to native handling.";
        return new(planId, planVersion, policyVersion, catalogueVersion, new(handler, provider, available, handlerMessage), serviceFingerprint, planFingerprint, review, capturedAt, items, reportGate);
    }

    public static async Task<AgreementReportOverrideResult> CreateReportOverrideAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, AgreementReportOverrideRequest request, string actor, CancellationToken ct = default)
    {
        if (!request.Confirmed || request.ConfirmationText != "OVERRIDE UNSIGNED AGREEMENTS") return new(false, "confirmation_required", "Type OVERRIDE UNSIGNED AGREEMENTS exactly.", null);
        if (string.IsNullOrWhiteSpace(request.Reason) || request.Reason.Trim().Length < 10) return new(false, "reason_required", "Enter a meaningful override reason of at least 10 characters.", null);
        var plan = await LoadJobPlanAsync(connection, tenantId, jobId, ct); if (plan is null) return new(false, "plan_required", "No frozen agreement plan exists for this job.", null);
        if (plan.PlanFingerprint != request.ExpectedPlanFingerprint) return new(false, "plan_conflict", "The agreement plan changed; reload before overriding.", plan.ReportGate);
        if (plan.ReviewRequired) return new(false, "plan_review_required", "Review the changed agreement plan before recording any unsigned-agreement override.", plan.ReportGate);
        if (plan.Items.Count == 0 || plan.Items.All(x => x.Status == "signed")) return new(false, "override_not_required", "There are no unsigned required agreements to override.", plan.ReportGate);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var expire = new NpgsqlCommand("UPDATE public.job_agreement_report_overrides SET active=false WHERE tenant_id=@tenant AND job_id=@job AND active=true", connection, tx))
        { expire.Parameters.AddWithValue("tenant", tenantId); expire.Parameters.AddWithValue("job", jobId); await expire.ExecuteNonQueryAsync(ct); }
        await using (var insert = new NpgsqlCommand("INSERT INTO public.job_agreement_report_overrides(tenant_id,job_id,plan_id,plan_fingerprint,reason,actor) VALUES(@tenant,@job,@plan,@fingerprint,@reason,@actor)", connection, tx))
        { insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("job", jobId); insert.Parameters.AddWithValue("plan", plan.PlanId); insert.Parameters.AddWithValue("fingerprint", plan.PlanFingerprint); insert.Parameters.AddWithValue("reason", request.Reason.Trim()); insert.Parameters.AddWithValue("actor", actor); await insert.ExecuteNonQueryAsync(ct); }
        await AuditAsync(connection, tx, tenantId, "agreement_report_override_recorded", 0, plan.PolicyVersion, actor, new { jobId, plan.PlanId, plan.PlanFingerprint, unsignedServiceIds = plan.Items.Where(x => x.Status != "signed").Select(x => x.ServiceId), reason = request.Reason.Trim() }, ct);
        await tx.CommitAsync(ct);
        return new(true, "overridden", "Unsigned agreements remain visible. This override only clears the agreement report gate and does not publish anything.", ReportGate(plan.Items, plan.ReviewRequired, true, request.Reason.Trim()));
    }

    public static async Task<AgreementReportGate> CheckReportGateAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        var plan = await LoadJobPlanAsync(connection, tenantId, jobId, ct);
        if (plan is not null) return plan.ReportGate;
        await using var command = new NpgsqlCommand("SELECT terms_required,terms_signed FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job", connection);
        command.Parameters.AddWithValue("tenant", tenantId.ToString()); command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return new(false, "job_not_found", false, "The job was not found for this company.");
        var required = reader.GetBoolean(0); var signed = reader.GetBoolean(1);
        if (!required) return new(true, "legacy_not_required", false, "The legacy job does not require an agreement.");
        return signed ? new(true, "legacy_signed", false, "Legacy signed-agreement evidence is preserved.") : new(false, "unsigned_agreements", true, "The legacy job requires signed-agreement evidence before report release.");
    }

    public static async Task MarkPlanReviewIfServicesChangedAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string snapshotJson, CancellationToken ct = default)
    {
        var plan = await LoadJobPlanAsync(connection, tenantId, jobId, ct); if (plan is null) return;
        var current = Hash(string.Join("|", ParseServices(snapshotJson).Select(x => x.Id).OrderBy(x => x)));
        if (current == plan.ServiceFingerprint) return;
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var update = new NpgsqlCommand("UPDATE public.job_agreement_plans SET review_required=true,updated_at=NOW() WHERE plan_id=@plan", connection, tx)) { update.Parameters.AddWithValue("plan", plan.PlanId); await update.ExecuteNonQueryAsync(ct); }
        await using (var update = new NpgsqlCommand("UPDATE public.jobs_staging SET agreement_plan_review_required=true WHERE tenant_id::text=@tenant AND job_id=@job", connection, tx)) { update.Parameters.AddWithValue("tenant", tenantId.ToString()); update.Parameters.AddWithValue("job", jobId); await update.ExecuteNonQueryAsync(ct); }
        await using (var expire = new NpgsqlCommand("UPDATE public.job_agreement_report_overrides SET active=false WHERE tenant_id=@tenant AND job_id=@job AND active=true", connection, tx)) { expire.Parameters.AddWithValue("tenant", tenantId); expire.Parameters.AddWithValue("job", jobId); await expire.ExecuteNonQueryAsync(ct); }
        await tx.CommitAsync(ct);
    }

    public static async Task<AgreementRequirementResolution> ResolveTermsRequiredAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, int catalogueVersion, string snapshotJson, CancellationToken ct = default)
    {
        var plan = await LoadJobPlanAsync(connection, tenantId, jobId, ct); if (plan is not null) return new(true, plan.Items.Count > 0, plan.ReviewRequired ? "plan_review_required" : "captured");
        var active = await LoadActiveAsync(connection, tenantId, ct); if (active.Version <= 0 || active.Draft.CatalogueVersion != catalogueVersion) return new(false, false, "policy_review_required");
        var selected = ParseServices(snapshotJson).Select(x => x.Id).ToHashSet(); var map = active.Draft.Services.ToDictionary(x => x.ServiceId);
        if (selected.Any(x => !map.TryGetValue(x, out var row) || row.Requirement == ReviewRequired)) return new(false, false, "policy_review_required");
        return new(true, selected.Any(x => map[x].Requirement == Required), "resolved");
    }

    public static async Task<HashSet<Guid>> LoadRequiredServiceIdsAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        var active = await LoadActiveAsync(connection, tenantId, ct); return active.Version <= 0 ? [] : active.Draft.Services.Where(x => x.Requirement == Required).Select(x => x.ServiceId).ToHashSet();
    }

    public static AgreementPlanPreview EvaluatePreview(int policyVersion, AgreementPolicyDraft policy, int jobCatalogueVersion, string snapshotJson, bool missingEmail, AgreementHandlerView handler, IReadOnlyDictionary<Guid, AgreementTemplateMapping> mappings)
    {
        if (jobCatalogueVersion < 1 || string.IsNullOrWhiteSpace(snapshotJson)) return AgreementPlanPreview.Failed("approved_catalogue_snapshot_required", "The job has no approved Service Catalogue snapshot.");
        if (policy.CatalogueVersion != jobCatalogueVersion) return AgreementPlanPreview.Failed("policy_stale", "The job must be re-synced against the Service Catalogue version reviewed by Agreements & Terms.", policyVersion, jobCatalogueVersion);
        var services = ParseServices(snapshotJson); var policies = policy.Services.ToDictionary(x => x.ServiceId);
        if (policyVersion <= 0)
        {
            var reviewedRequired = services.Where(x => policies.TryGetValue(x.Id, out var row) && row.Requirement == Required).DistinctBy(x => x.Id).ToList();
            var draftItems = reviewedRequired.Select(service => !handler.Available
                ? new AgreementSelection(service.Id, service.Name, "", "", "handler_unavailable")
                : missingEmail
                    ? new AgreementSelection(service.Id, service.Name, "", "", "client_email_required")
                    : mappings.TryGetValue(service.Id, out var mapping)
                        ? new AgreementSelection(service.Id, service.Name, mapping.TemplateId, mapping.TemplateName, "selected")
                        : new AgreementSelection(service.Id, service.Name, "", "", "mapping_required")).ToList();
            return new("policy_review_required", 0, jobCatalogueVersion, handler, draftItems, services.Select(x => x.Id).ToList(), "Draft selection shown for review only. Classify every active Service and activate Agreements & Terms before scheduling.");
        }
        if (services.Any(x => !policies.TryGetValue(x.Id, out var policyRow) || policyRow.Requirement == ReviewRequired)) return AgreementPlanPreview.Failed("policy_review_required", "Every selected Service must be classified in Agreements & Terms.", policyVersion, jobCatalogueVersion, services.Select(x => x.Id).ToList());
        var required = services.Where(x => policies[x.Id].Requirement == Required).DistinctBy(x => x.Id).ToList();
        if (required.Count == 0) return new("not_required", policyVersion, jobCatalogueVersion, handler, [], services.Select(x => x.Id).ToList(), "No selected Service requires an agreement.");
        if (!handler.Available) return new("handler_unavailable", policyVersion, jobCatalogueVersion, handler, required.Select(x => new AgreementSelection(x.Id, x.Name, "", "", "handler_unavailable")).ToList(), services.Select(x => x.Id).ToList(), handler.Message);
        if (missingEmail) return new("client_email_required", policyVersion, jobCatalogueVersion, handler, required.Select(x => new AgreementSelection(x.Id, x.Name, "", "", "client_email_required")).ToList(), services.Select(x => x.Id).ToList(), "Client email is required before agreements can be prepared.");
        var items = required.Select(service => mappings.TryGetValue(service.Id, out var mapping) ? new AgreementSelection(service.Id, service.Name, mapping.TemplateId, mapping.TemplateName, "selected") : new AgreementSelection(service.Id, service.Name, "", "", "mapping_required")).ToList();
        if (items.Any(x => x.Status != "selected")) return new("mapping_required", policyVersion, jobCatalogueVersion, handler, items, services.Select(x => x.Id).ToList(), "Map every agreement-required Service to a signing template before scheduling.");
        return new("selected", policyVersion, jobCatalogueVersion, handler, items, services.Select(x => x.Id).ToList(), $"Selected {items.Count} Service agreement(s). No document or invitation was created.");
    }

    private static async Task<AgreementPlanPreview> PreviewSnapshotAsync(NpgsqlConnection connection, Guid tenantId, int catalogueVersion, string snapshotJson, bool missingEmail, CancellationToken ct)
    {
        var active = await LoadActiveAsync(connection, tenantId, ct); var handler = await ResolveHandlerAsync(connection, tenantId, ct);
        var evaluatedPolicy = active.Version > 0 ? active.Draft : (await LoadAsync(connection, tenantId, ct)).Draft;
        var storedMappings = await LoadActiveProviderMappingsAsync(connection, tenantId, handler.ProviderKey, ct);
        var mappings = storedMappings.ToDictionary(x => x.Key, x => new AgreementTemplateMapping(x.Value.TemplateId, x.Value.TemplateName));
        return EvaluatePreview(active.Version, evaluatedPolicy, catalogueVersion, snapshotJson, missingEmail, handler, mappings);
    }

    private static AgreementPlanPreview ExistingPreview(JobAgreementPlanView plan)
    {
        var agreements = plan.Items.Select(x => new AgreementSelection(x.ServiceId, x.ServiceName, x.TemplateId, x.TemplateName, x.Status)).ToList();
        var status = plan.ReviewRequired ? "plan_review_required" : agreements.Count > 0 && !plan.Handler.Available ? "handler_unavailable" : agreements.Count == 0 ? "not_required" : "selected";
        return new(status, plan.PolicyVersion, plan.CatalogueVersion, plan.Handler, agreements, [], plan.ReviewRequired ? "The frozen agreement plan requires review after a Service change." : status == "handler_unavailable" ? plan.Handler.Message : "Using the immutable agreement plan captured when this job was first scheduled.");
    }

    public static AgreementReportGate ReportGate(IReadOnlyList<JobAgreementItemView> items, bool review, bool overridden, string reason)
    {
        if (review) return new(false, "plan_review_required", true, "The agreement plan changed and requires review before report release.");
        if (items.Count == 0) return new(true, "not_required", false, "No agreements are required.");
        if (items.All(x => x.Status == "signed")) return new(true, "signed", false, "Every required Service agreement is signed.");
        if (overridden) return new(true, "overridden", true, $"Unsigned agreement override recorded: {reason}");
        return new(false, "unsigned_agreements", true, "Every required Service agreement must be signed, or a tenant administrator must record an override.");
    }

    private static async Task EnsureSeedAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct)
    {
        await EnsureAsync(connection, ct); await TenantServiceCatalogueSupport.EnsureAsync(connection, ct);
        var exists = Convert.ToBoolean(await ScalarAsync(connection, "SELECT EXISTS(SELECT 1 FROM public.tenant_agreement_policy_state WHERE tenant_id=@tenant)", tenantId, ct)); if (exists) return;
        var active = await TenantServiceCatalogueSupport.LoadActiveAsync(connection, tenantId, ct); var mapped = await ExistingMappedServiceIdsAsync(connection, tenantId, ct); var categories = active.Draft.Categories.ToDictionary(x => x.Id, x => x.Name);
        var services = active.Draft.Services.Where(x => !x.Archived).Select(x => new AgreementServicePolicy(x.Id, x.Name, categories.GetValueOrDefault(x.CategoryId, ""), mapped.Contains(x.Id) ? Required : ReviewRequired)).ToList();
        var draft = new AgreementPolicyDraft(ContractVersion, active.Version, services); var json = JsonSerializer.Serialize(draft, JsonOptions);
        await using var command = new NpgsqlCommand("INSERT INTO public.tenant_agreement_policy_state(tenant_id,draft_version,draft_json,updated_by) VALUES(@tenant,1,@json::jsonb,'migration') ON CONFLICT DO NOTHING", connection);
        command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("json", json); await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<AgreementPolicyDraft> MergeWithCatalogueAsync(NpgsqlConnection connection, Guid tenantId, AgreementPolicyDraft draft, bool seedFromMappings, CancellationToken ct)
    {
        var active = await TenantServiceCatalogueSupport.LoadActiveAsync(connection, tenantId, ct); var categories = active.Draft.Categories.ToDictionary(x => x.Id, x => x.Name); var existing = (draft.Services ?? []).GroupBy(x => x.ServiceId).ToDictionary(x => x.Key, x => x.First()); var mapped = seedFromMappings ? await ExistingMappedServiceIdsAsync(connection, tenantId, ct) : [];
        var services = active.Draft.Services.Where(x => !x.Archived).Select(x => existing.TryGetValue(x.Id, out var row) ? row with { ServiceName = x.Name, CategoryName = categories.GetValueOrDefault(x.CategoryId, "") } : new AgreementServicePolicy(x.Id, x.Name, categories.GetValueOrDefault(x.CategoryId, ""), mapped.Contains(x.Id) ? Required : ReviewRequired)).OrderBy(x => x.CategoryName).ThenBy(x => x.ServiceName).ToList();
        return new(ContractVersion, active.Version, services);
    }

    private static async Task<AgreementPolicyValidation> ValidateAsync(NpgsqlConnection connection, Guid tenantId, AgreementPolicyDraft draft, bool activation, CancellationToken ct)
    {
        var errors = new List<AgreementPolicyIssue>(); var warnings = new List<AgreementPolicyIssue>();
        if (draft.ContractVersion != ContractVersion) errors.Add(new("policy", "unsupported_contract", "The Agreements & Terms contract version is unsupported."));
        var active = await TenantServiceCatalogueSupport.LoadActiveAsync(connection, tenantId, ct); if (active.Version != draft.CatalogueVersion) errors.Add(new("policy", "catalogue_stale", "Reload Agreements & Terms after activating a Service Catalogue change."));
        var serviceIds = active.Draft.Services.Where(x => !x.Archived).Select(x => x.Id).ToHashSet();
        foreach (var duplicate in (draft.Services ?? []).GroupBy(x => x.ServiceId).Where(x => x.Key == Guid.Empty || x.Count() > 1)) errors.Add(new("service", "duplicate_service", "Each active Service requires one agreement decision."));
        foreach (var row in draft.Services ?? [])
        {
            if (!serviceIds.Contains(row.ServiceId)) errors.Add(new(row.ServiceName, "service_stale", "This Service is no longer active in the Service Catalogue."));
            if (row.Requirement is not (ReviewRequired or Required or NotRequired)) errors.Add(new(row.ServiceName, "invalid_requirement", "Choose Review required, Agreement required, or Agreement not required."));
            if (row.Requirement == ReviewRequired) (activation ? errors : warnings).Add(new(row.ServiceName, "agreement_review_required", "Classify this Service before activating the agreement policy."));
        }
        foreach (var missing in serviceIds.Except((draft.Services ?? []).Select(x => x.ServiceId))) errors.Add(new(missing.ToString(), "service_policy_missing", "A newly active Service requires agreement review."));
        return new(errors.Count == 0, errors, warnings);
    }

    private static async Task<(int Version, AgreementPolicyDraft Draft)> LoadActiveAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct)
    {
        await EnsureSeedAsync(connection, tenantId, ct); int version; string? json;
        await using var command = new NpgsqlCommand("SELECT s.active_version,v.policy_json::text FROM public.tenant_agreement_policy_state s LEFT JOIN public.tenant_agreement_policy_versions v ON v.tenant_id=s.tenant_id AND v.policy_version=s.active_version WHERE s.tenant_id=@tenant", connection);
        command.Parameters.AddWithValue("tenant", tenantId); await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct); version = reader.GetInt32(0); json = reader.IsDBNull(1) ? null : reader.GetString(1);
        return (version, version > 0 && json is not null ? JsonSerializer.Deserialize<AgreementPolicyDraft>(json, JsonOptions) ?? EmptyDraft() : EmptyDraft());
    }

    private static async Task<AgreementHandlerView> ResolveHandlerAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct)
    {
        await IntegrationHubSupport.EnsureAsync(connection, ct); string provider = "signnow";
        await using (var command = new NpgsqlCommand("SELECT provider_key FROM public.tenant_integration_action_defaults WHERE tenant_id=@tenant AND action_type='agreement_management'", connection)) { command.Parameters.AddWithValue("tenant", tenantId); provider = Convert.ToString(await command.ExecuteScalarAsync(ct)) ?? "signnow"; }
        provider=AgreementProviderSupport.NormalizeProvider(provider);var connected = false;
        await using (var command = new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.inspector_integrations ii WHERE lower(ii.provider)=@provider AND lower(COALESCE(ii.status,''))='connected' AND (ii.inspector_id='00000000-0000-0000-0000-000000000000'::uuid OR EXISTS(SELECT 1 FROM public.inspectors i WHERE i.inspector_id=ii.inspector_id AND i.tenant_id=@tenant)))", connection)) { command.Parameters.AddWithValue("provider", provider); command.Parameters.AddWithValue("tenant", tenantId); connected = Convert.ToBoolean(await command.ExecuteScalarAsync(ct)); }
        if(!connected){await ProviderIntegrationSupport.EnsureAsync(connection,ct);await using var modern=new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.tenant_provider_accounts WHERE tenant_id=@tenant AND provider_key=@provider AND status='connected')",connection);modern.Parameters.AddWithValue("tenant",tenantId);modern.Parameters.AddWithValue("provider",provider);connected=Convert.ToBoolean(await modern.ExecuteScalarAsync(ct));}
        if (connected && AgreementProviderSupport.Providers.Contains(provider)) return new("integrated", provider, true, $"Connected {ProviderDisplayName(provider)} is the agreement handler. Native terms are inactive.");
        if (connected) return new("integrated", provider, false, "The selected signing integration adapter is unavailable.");
        return new("native", "native", false, "No signing integration is connected. Native agreements are selected automatically but the native builder is not available yet.");
    }

    private static async Task<List<AgreementTemplateReadiness>> TemplateReadinessAsync(NpgsqlConnection connection, Guid tenantId, AgreementPolicyDraft draft, AgreementHandlerView handler, CancellationToken ct)
    {
        var mappings = await LoadActiveProviderMappingsAsync(connection, tenantId, handler.ProviderKey, ct);
        return draft.Services.Select(x => x.Requirement != Required
            ? new AgreementTemplateReadiness(x.ServiceId, "not_required", "")
            : mappings.TryGetValue(x.ServiceId, out var mapping)
                ? new AgreementTemplateReadiness(x.ServiceId, "ready", mapping.TemplateName)
                : new AgreementTemplateReadiness(x.ServiceId, handler.Available ? "mapping_required" : "handler_unavailable", "")).ToList();
    }

    private static async Task<Dictionary<Guid, (string TemplateId, string TemplateName)>> LoadActiveSignNowMappingsAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct)
    {
        var result = new Dictionary<Guid, (string, string)>(); if (!await TableExistsAsync(connection, null, "tenant_signnow_catalogue_mappings", ct)) return result;
        await using var command = new NpgsqlCommand("SELECT target_id,signnow_template_id,signnow_template_name FROM public.tenant_signnow_catalogue_mappings WHERE tenant_id=@tenant AND target_type='service' AND active=true", connection); command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct); while (await reader.ReadAsync(ct)) result[reader.GetGuid(0)] = (reader.GetString(1), reader.GetString(2)); return result;
    }

    private static async Task<Dictionary<Guid,(string TemplateId,string TemplateName)>> LoadActiveProviderMappingsAsync(NpgsqlConnection connection,Guid tenantId,string provider,CancellationToken ct)
    {
        provider=AgreementProviderSupport.NormalizeProvider(provider);if(provider=="signnow")return await LoadActiveSignNowMappingsAsync(connection,tenantId,ct);var result=new Dictionary<Guid,(string,string)>();
        foreach(var row in await AgreementProviderSupport.LoadMappingsAsync(connection,tenantId,provider,ct))if(row.Active)result[row.ServiceId]=(row.TemplateId,row.TemplateName);return result;
    }

    private static async Task<HashSet<Guid>> ExistingMappedServiceIdsAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct)
    {
        var result=(await LoadActiveSignNowMappingsAsync(connection,tenantId,ct)).Keys.ToHashSet();foreach(var provider in new[]{"adobe_sign","docusign"})foreach(var row in await AgreementProviderSupport.LoadMappingsAsync(connection,tenantId,provider,ct))if(row.Active)result.Add(row.ServiceId);return result;
    }
    private static string ProviderDisplayName(string provider)=>provider switch{"adobe_sign"=>"Adobe Acrobat Sign","docusign"=>"DocuSign","signnow"=>"SignNow",_=>provider};
    private static List<ResolvedAgreementService> ParseServices(string json)
    {
        var result = new List<ResolvedAgreementService>(); if (string.IsNullOrWhiteSpace(json)) return result;
        try { using var document = JsonDocument.Parse(json); if (document.RootElement.TryGetProperty("services", out var services)) foreach (var item in services.EnumerateArray()) if (item.TryGetProperty("serviceId", out var id) && Guid.TryParse(id.ToString(), out var parsed)) result.Add(new(parsed, item.TryGetProperty("serviceName", out var name) ? name.GetString() ?? "" : "")); } catch (JsonException) { }
        return result.DistinctBy(x => x.Id).ToList();
    }
    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, Guid tenantId, CancellationToken ct) { await using var command = new NpgsqlCommand(sql, connection); command.Parameters.AddWithValue("tenant", tenantId); return await command.ExecuteScalarAsync(ct); }
    private static async Task<bool> TableExistsAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, string table, CancellationToken ct) { await using var command = new NpgsqlCommand("SELECT to_regclass('public.'||@table) IS NOT NULL", connection, transaction); command.Parameters.AddWithValue("table", table); return Convert.ToBoolean(await command.ExecuteScalarAsync(ct)); }
    private static async Task AuditAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, string action, int draftVersion, int policyVersion, string actor, object detail, CancellationToken ct) { await using var command = new NpgsqlCommand("INSERT INTO public.tenant_agreement_policy_audit(tenant_id,action_key,draft_version,policy_version,actor,detail_json) VALUES(@tenant,@action,@draft,@policy,@actor,@detail::jsonb)", connection, transaction); command.Parameters.AddWithValue("tenant", tenantId); command.Parameters.AddWithValue("action", action); command.Parameters.AddWithValue("draft", draftVersion); command.Parameters.AddWithValue("policy", policyVersion); command.Parameters.AddWithValue("actor", actor); command.Parameters.AddWithValue("detail", JsonSerializer.Serialize(detail, JsonOptions)); await command.ExecuteNonQueryAsync(ct); }
    private static string Hash(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text ?? ""))).ToLowerInvariant();
    private static AgreementPolicyDraft EmptyDraft() => new(ContractVersion, 0, []);
    private static AgreementPolicyValidation EmptyValidation(string message) => new(false, [new("policy", "confirmation_required", message)], []);
    private sealed record ResolvedAgreementService(Guid Id, string Name);
}

public sealed record AgreementServicePolicy(Guid ServiceId, string ServiceName, string CategoryName, string Requirement);
public sealed record AgreementPolicyDraft(int ContractVersion, int CatalogueVersion, List<AgreementServicePolicy> Services);
public sealed record AgreementPolicyIssue(string Subject, string Code, string Message);
public sealed record AgreementPolicyValidation(bool Valid, List<AgreementPolicyIssue> Errors, List<AgreementPolicyIssue> Warnings);
public sealed record AgreementHandlerView(string HandlerKey, string ProviderKey, bool Available, string Message);
public sealed record AgreementTemplateReadiness(Guid ServiceId, string Status, string TemplateName);
public sealed record AgreementTemplateMapping(string TemplateId, string TemplateName);
public sealed record AgreementPolicyView(int DraftVersion, int ActiveVersion, AgreementPolicyDraft Draft, AgreementPolicyValidation Validation, AgreementHandlerView Handler, List<AgreementTemplateReadiness> TemplateReadiness);
public sealed record AgreementPolicySaveRequest(int ExpectedDraftVersion, AgreementPolicyDraft? Draft, bool Confirmed);
public sealed record AgreementPolicyActivateRequest(int ExpectedDraftVersion, bool Confirmed);
public sealed record AgreementPolicySaveResult(bool Success, string Status, int DraftVersion, int ActiveVersion, AgreementPolicyValidation Validation, string Message);
public sealed record AgreementSelection(Guid ServiceId, string ServiceName, string TemplateId, string TemplateName, string Status);
public sealed record AgreementPlanPreview(string Status, int PolicyVersion, int CatalogueVersion, AgreementHandlerView Handler, List<AgreementSelection> Agreements, List<Guid> SelectedServiceIds, string Message)
{
    public static AgreementPlanPreview Failed(string status, string message, int policyVersion = 0, int catalogueVersion = 0, List<Guid>? selected = null) => new(status, policyVersion, catalogueVersion, new("unavailable", "", false, message), [], selected ?? [], message);
}
public sealed record AgreementSchedulingGate(bool Allowed, string Status, string Message, int PolicyVersion, int CatalogueVersion, int AgreementCount, AgreementHandlerView Handler);
public sealed record JobAgreementItemView(Guid PlanItemId, Guid ServiceId, string ServiceName, string HandlerKey, string ProviderKey, string TemplateId, string TemplateName, string Status, string ExternalDocumentId, string ExternalInviteId, DateTime? SignedAt);
public sealed record AgreementReportGate(bool Allowed, string Status, bool Warning, string Message);
public sealed record JobAgreementPlanView(Guid PlanId, int PlanVersion, int PolicyVersion, int CatalogueVersion, AgreementHandlerView Handler, string ServiceFingerprint, string PlanFingerprint, bool ReviewRequired, DateTime CapturedAt, List<JobAgreementItemView> Items, AgreementReportGate ReportGate);
public sealed record AgreementReportOverrideRequest(string ExpectedPlanFingerprint, string Reason, string ConfirmationText, bool Confirmed);
public sealed record AgreementReportOverrideResult(bool Success, string Status, string Message, AgreementReportGate? ReportGate);
public sealed record AgreementRequirementResolution(bool Authoritative, bool TermsRequired, string Status);
