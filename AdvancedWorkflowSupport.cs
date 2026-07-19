using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class AdvancedWorkflowSupport
{
    public const string Basic = "basic";
    public const string Advanced = "advanced";
    public static readonly string[] RequiredEvents = ["inspection_scheduled", "inspection_rescheduled", "inspection_unscheduled", "inspection_cancelled"];

    public static async Task EnsureAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.advanced_workflow_tenant_settings
(
    tenant_id uuid PRIMARY KEY,
    engine_mode text NOT NULL DEFAULT 'basic',
    settings_version integer NOT NULL DEFAULT 1,
    changed_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_advanced_workflow_engine_mode CHECK (engine_mode IN ('basic','advanced'))
);

CREATE TABLE IF NOT EXISTS public.advanced_workflow_versions
(
    workflow_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    version integer NOT NULL,
    name text NOT NULL,
    event_key text NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    workflow_conditions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    actions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    validation_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    validated_at timestamptz NULL,
    created_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY(workflow_id,version)
);
CREATE INDEX IF NOT EXISTS idx_advanced_workflow_versions_tenant_event ON public.advanced_workflow_versions(tenant_id,event_key,enabled,created_at DESC);

CREATE TABLE IF NOT EXISTS public.advanced_workflow_current
(
    tenant_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    current_version integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY(tenant_id,workflow_id),
    FOREIGN KEY(workflow_id,current_version) REFERENCES public.advanced_workflow_versions(workflow_id,version)
);

CREATE TABLE IF NOT EXISTS public.job_workflow_engine_assignments
(
    tenant_id uuid NOT NULL,
    job_id uuid NOT NULL,
    engine_key text NOT NULL,
    assignment_reason text NOT NULL DEFAULT '',
    first_scheduled_at timestamptz NULL,
    assigned_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY(tenant_id,job_id),
    CONSTRAINT ck_job_workflow_engine_key CHECK (engine_key IN ('basic','advanced'))
);

CREATE TABLE IF NOT EXISTS public.advanced_workflow_executions
(
    execution_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    job_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    workflow_version integer NOT NULL,
    event_key text NOT NULL,
    event_idempotency_key text NOT NULL,
    field_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'pending',
    current_action_index integer NOT NULL DEFAULT 0,
    last_error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT NOW(),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id,workflow_id,event_idempotency_key),
    FOREIGN KEY(workflow_id,workflow_version) REFERENCES public.advanced_workflow_versions(workflow_id,version)
);

CREATE TABLE IF NOT EXISTS public.advanced_workflow_action_executions
(
    action_execution_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id uuid NOT NULL REFERENCES public.advanced_workflow_executions(execution_id) ON DELETE CASCADE,
    action_index integer NOT NULL,
    action_key text NOT NULL,
    action_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    external_id text NOT NULL DEFAULT '',
    provider_status text NOT NULL DEFAULT '',
    last_error text NOT NULL DEFAULT '',
    attempt_count integer NOT NULL DEFAULT 0,
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE(execution_id,action_index),
    UNIQUE(idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_advanced_action_resume ON public.advanced_workflow_action_executions(execution_id,status,action_index);
""";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync(ct);

        // Jobs that were already scheduled before this feature existed remain Basic forever.
        const string backfill = """
INSERT INTO public.job_workflow_engine_assignments(tenant_id,job_id,engine_key,assignment_reason,first_scheduled_at)
SELECT tenant_id::uuid,job_id,'basic','existing_scheduled_job',COALESCE(job_date,workflow_updated_at,updated_at,NOW())
FROM public.jobs_staging
WHERE tenant_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  AND (COALESCE(automate_status,'')='Scheduled' OR job_date IS NOT NULL)
ON CONFLICT(tenant_id,job_id) DO NOTHING;
""";
        await using var backfillCommand = new NpgsqlCommand(backfill, conn);
        await backfillCommand.ExecuteNonQueryAsync(ct);
    }

    public static async Task<(string Mode, int Version)> LoadModeAsync(NpgsqlConnection conn, Guid tenantId, CancellationToken ct = default)
    {
        await using var command = new NpgsqlCommand("SELECT engine_mode,settings_version FROM public.advanced_workflow_tenant_settings WHERE tenant_id=@tenant", conn);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetString(0), reader.GetInt32(1)) : (Basic, 0);
    }

    public static async Task<IReadOnlyList<string>> MissingRequiredEventsAsync(NpgsqlConnection conn, Guid tenantId, CancellationToken ct = default)
    {
        const string sql = """
SELECT DISTINCT v.event_key
FROM public.advanced_workflow_current c
JOIN public.advanced_workflow_versions v ON v.workflow_id=c.workflow_id AND v.version=c.current_version
WHERE c.tenant_id=@tenant AND v.enabled=true AND v.validated_at IS NOT NULL;
""";
        var present = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) present.Add(reader.GetString(0));
        return RequiredEvents.Where(value => !present.Contains(value)).ToArray();
    }

    public static async Task<(string Mode, int Version)> SaveModeAsync(NpgsqlConnection conn, Guid tenantId, string mode, int expectedVersion, string actor, CancellationToken ct = default)
    {
        mode = string.Equals(mode, Advanced, StringComparison.OrdinalIgnoreCase) ? Advanced : Basic;
        var current = await LoadModeAsync(conn, tenantId, ct);
        if (current.Version != expectedVersion) throw new InvalidOperationException("Advanced Workflow settings changed. Reload before switching modes.");
        if (mode == Advanced)
        {
            var missing = await MissingRequiredEventsAsync(conn, tenantId, ct);
            if (missing.Count > 0) throw new AdvancedWorkflowReadinessException(missing);
        }
        const string sql = """
INSERT INTO public.advanced_workflow_tenant_settings(tenant_id,engine_mode,settings_version,changed_by)
VALUES(@tenant,@mode,1,@actor)
ON CONFLICT(tenant_id) DO UPDATE SET engine_mode=EXCLUDED.engine_mode,settings_version=advanced_workflow_tenant_settings.settings_version+1,changed_by=EXCLUDED.changed_by,updated_at=NOW()
WHERE advanced_workflow_tenant_settings.settings_version=@expected
RETURNING engine_mode,settings_version;
""";
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("mode", mode);
        command.Parameters.AddWithValue("actor", actor ?? "");
        command.Parameters.AddWithValue("expected", expectedVersion);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Advanced Workflow settings changed. Reload before switching modes.");
        return (reader.GetString(0), reader.GetInt32(1));
    }

    public static async Task<string> AssignAtFirstSchedulingAsync(NpgsqlConnection conn, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        var mode = (await LoadModeAsync(conn, tenantId, ct)).Mode;
        const string sql = """
INSERT INTO public.job_workflow_engine_assignments(tenant_id,job_id,engine_key,assignment_reason,first_scheduled_at)
VALUES(@tenant,@job,@engine,'first_scheduling',NOW())
ON CONFLICT(tenant_id,job_id) DO NOTHING;
SELECT engine_key FROM public.job_workflow_engine_assignments WHERE tenant_id=@tenant AND job_id=@job;
""";
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        command.Parameters.AddWithValue("engine", mode);
        return Convert.ToString(await command.ExecuteScalarAsync(ct)) ?? Basic;
    }

    public static async Task<string> ResolveEngineAsync(NpgsqlConnection conn,Guid tenantId,Guid jobId,CancellationToken ct=default)
    {
        await using var command=new NpgsqlCommand("SELECT engine_key FROM public.job_workflow_engine_assignments WHERE tenant_id=@tenant AND job_id=@job",conn);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("job",jobId);var assigned=Convert.ToString(await command.ExecuteScalarAsync(ct));return string.IsNullOrWhiteSpace(assigned)?(await LoadModeAsync(conn,tenantId,ct)).Mode:assigned;
    }

    public static async Task<Guid> SaveVersionAsync(NpgsqlConnection conn, Guid tenantId, Guid workflowId, string name, string eventKey, bool enabled, string conditionGroupsJson, string actionsJson, string actor, CancellationToken ct = default)
    {
        workflowId = workflowId == Guid.Empty ? Guid.NewGuid() : workflowId;
        await using var transaction = await conn.BeginTransactionAsync(ct);
        await using (var lockCommand=new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtext(@key))",conn,transaction))
        {
            lockCommand.Parameters.AddWithValue("key",workflowId.ToString("D"));
            await lockCommand.ExecuteNonQueryAsync(ct);
        }
        int version;
        await using (var versionCommand = new NpgsqlCommand("SELECT COALESCE(MAX(version),0)+1 FROM public.advanced_workflow_versions WHERE workflow_id=@id", conn, transaction))
        {
            versionCommand.Parameters.AddWithValue("id", workflowId);
            version = Convert.ToInt32(await versionCommand.ExecuteScalarAsync(ct));
        }
        const string insert = """
INSERT INTO public.advanced_workflow_versions(workflow_id,tenant_id,version,name,event_key,enabled,workflow_conditions_json,actions_json,validation_json,validated_at,created_by)
VALUES(@id,@tenant,@version,@name,@event,@enabled,CAST(@conditions AS jsonb),CAST(@actions AS jsonb),'[]'::jsonb,NOW(),@actor);
INSERT INTO public.advanced_workflow_current(tenant_id,workflow_id,current_version)
VALUES(@tenant,@id,@version)
ON CONFLICT(tenant_id,workflow_id) DO UPDATE SET current_version=EXCLUDED.current_version,updated_at=NOW();
""";
        await using (var command = new NpgsqlCommand(insert, conn, transaction))
        {
            command.Parameters.AddWithValue("id", workflowId);
            command.Parameters.AddWithValue("tenant", tenantId);
            command.Parameters.AddWithValue("version", version);
            command.Parameters.AddWithValue("name", name);
            command.Parameters.AddWithValue("event", eventKey);
            command.Parameters.AddWithValue("enabled", enabled);
            command.Parameters.AddWithValue("conditions", conditionGroupsJson);
            command.Parameters.AddWithValue("actions", actionsJson);
            command.Parameters.AddWithValue("actor", actor ?? "");
            await command.ExecuteNonQueryAsync(ct);
        }
        await transaction.CommitAsync(ct);
        return workflowId;
    }
}

public sealed class AdvancedWorkflowReadinessException(IReadOnlyList<string> missingEvents) : InvalidOperationException("Required Advanced Workflows are missing or disabled.")
{
    public IReadOnlyList<string> MissingEvents { get; } = missingEvents;
}
