using Npgsql;
using NpgsqlTypes;

public static class AutomationFoundationSupport
{
    public static async Task EnsureAsync(NpgsqlConnection conn)
    {
        const string sql = @"
CREATE TABLE IF NOT EXISTS public.automation_tenant_settings
(
    tenant_id uuid PRIMARY KEY,
    activation_mode text NOT NULL DEFAULT 'selected_jobs',
    updated_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_automation_activation_mode CHECK (activation_mode IN ('selected_jobs', 'all_jobs'))
);

CREATE TABLE IF NOT EXISTS public.automation_job_selections
(
    tenant_id uuid NOT NULL,
    job_id uuid NOT NULL,
    use_advanced_workflows boolean NOT NULL DEFAULT false,
    updated_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, job_id)
);

CREATE TABLE IF NOT EXISTS public.automation_foundation_audit
(
    audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    job_id uuid NULL,
    action_key text NOT NULL,
    previous_value text NOT NULL DEFAULT '',
    new_value text NOT NULL DEFAULT '',
    changed_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS tenant_id uuid NULL;
ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS archived_at timestamptz NULL;
ALTER TABLE public.email_templates ADD COLUMN IF NOT EXISTS channel text NOT NULL DEFAULT 'email';
CREATE INDEX IF NOT EXISTS idx_email_templates_tenant ON public.email_templates(tenant_id, archived_at, updated_at DESC);

ALTER TABLE public.automation_rules ADD COLUMN IF NOT EXISTS template_id uuid NULL;
CREATE INDEX IF NOT EXISTS idx_automation_job_selections_tenant ON public.automation_job_selections(tenant_id, use_advanced_workflows);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();

        const string backfill = @"
UPDATE public.email_templates t
SET tenant_id = i.tenant_id
FROM public.inspectors i
WHERE t.tenant_id IS NULL AND i.inspector_id = t.inspector_id;";
        await using var backfillCmd = new NpgsqlCommand(backfill, conn);
        await backfillCmd.ExecuteNonQueryAsync();
    }

    public static async Task<AutomationEntitlement> LoadEntitlementAsync(NpgsqlConnection conn, Guid tenantId)
    {
        const string sql = @"
SELECT COALESCE(s.plan_name, ''), COALESCE(s.status, 'not_registered'), s.trial_ends_at
FROM public.inspectors i
LEFT JOIN public.subscriptions s ON s.inspector_id = i.inspector_id
WHERE i.tenant_id = @tenant
ORDER BY CASE WHEN s.status='active' THEN 0 WHEN s.status='trialing' AND s.trial_ends_at>NOW() THEN 1 ELSE 2 END,
         s.updated_at DESC NULLS LAST
LIMIT 1;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return new(false, "not_registered", "", false, false, false);
        var plan = reader.GetString(0).Trim().ToLowerInvariant();
        var status = reader.GetString(1).Trim().ToLowerInvariant();
        DateTime? trialEnds = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
        var allowed = status == "active" || (status == "trialing" && trialEnds.HasValue && trialEnds.Value > DateTime.UtcNow);
        var advanced = allowed && plan != "basic";
        return new(allowed, status, plan, allowed, advanced, advanced);
    }

    public static async Task<bool> InspectorBelongsToTenantAsync(NpgsqlConnection conn, Guid tenantId, Guid inspectorId)
    {
        if (tenantId == Guid.Empty || inspectorId == Guid.Empty) return false;
        const string sql = @"SELECT EXISTS(SELECT 1 FROM public.inspectors WHERE tenant_id=@tenant AND inspector_id=@inspector);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenant", tenantId);
        cmd.Parameters.AddWithValue("inspector", inspectorId);
        return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
    }

    public static async Task<string> GetActivationModeAsync(NpgsqlConnection conn, Guid tenantId)
    {
        const string sql = @"SELECT activation_mode FROM public.automation_tenant_settings WHERE tenant_id=@tenant;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenant", tenantId);
        return Convert.ToString(await cmd.ExecuteScalarAsync()) ?? "selected_jobs";
    }

    public static async Task<bool> JobUsesAdvancedAsync(NpgsqlConnection conn, Guid tenantId, Guid jobId, string mode)
    {
        if (mode == "all_jobs") return true;
        const string sql = @"SELECT COALESCE((SELECT use_advanced_workflows FROM public.automation_job_selections WHERE tenant_id=@tenant AND job_id=@job), false);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenant", tenantId);
        cmd.Parameters.AddWithValue("job", jobId);
        return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
    }

    public static async Task<bool> JobBelongsToTenantAsync(NpgsqlConnection conn, Guid tenantId, Guid jobId)
    {
        const string sql = @"SELECT EXISTS(SELECT 1 FROM public.jobs_staging WHERE job_id=@job AND tenant_id::text=@tenant);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("job", jobId);
        cmd.Parameters.Add("tenant", NpgsqlDbType.Text).Value = tenantId.ToString();
        return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
    }

    public static async Task AuditAsync(NpgsqlConnection conn, Guid tenantId, Guid? jobId, string action, string previous, string current, string changedBy)
    {
        const string sql = @"INSERT INTO public.automation_foundation_audit(tenant_id,job_id,action_key,previous_value,new_value,changed_by) VALUES(@tenant,@job,@action,@previous,@current,@by);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("tenant", tenantId);
        cmd.Parameters.AddWithValue("job", jobId.HasValue ? jobId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("action", action ?? "");
        cmd.Parameters.AddWithValue("previous", previous ?? "");
        cmd.Parameters.AddWithValue("current", current ?? "");
        cmd.Parameters.AddWithValue("by", changedBy ?? "");
        await cmd.ExecuteNonQueryAsync();
    }
}

public record AutomationEntitlement(bool Allowed, string Status, string PlanName, bool BasicAutomation, bool AdvancedWorkflows, bool OutgoingWebhooks);
