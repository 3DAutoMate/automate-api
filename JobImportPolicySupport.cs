using System.Security.Cryptography;
using System.Text;
using Npgsql;

namespace AutoMateApi;

public sealed record HistoricJobCleanupCommand(
    Guid TenantId,
    Guid[] KeepJobIds,
    string ExpectedFingerprint,
    string IdempotencyKey,
    bool Confirmed,
    string Reason);

public sealed record JobImportDecision(bool Allowed, bool ExistingJob, DateTimeOffset RegistrationCutoff, string Code);

public sealed record HistoricJobCleanupPreview(
    Guid TenantId,
    DateTimeOffset RegistrationCutoff,
    Guid[] KeepJobIds,
    int RetainedJobCount,
    int RemovalJobCount,
    string Fingerprint,
    IReadOnlyDictionary<string,long> RowsByTable);

public sealed record HistoricJobCleanupResult(
    Guid TenantId,
    DateTimeOffset RegistrationCutoff,
    int RetainedJobCount,
    int RemovedJobCount,
    string Fingerprint,
    bool IdempotentReplay,
    DateTimeOffset CompletedAt);

/// <summary>
/// Freezes the tenant's first AutoMate registration time and enforces it as the
/// earliest source DateAdded accepted for a new THREED job. Existing AutoMate jobs
/// remain readable/updateable so deliberately retained records are not orphaned.
/// This component never writes to THREED or calls a provider.
/// </summary>
public static class JobImportPolicySupport
{
    public const int ContractVersion = 1;
    public const string RuleText = "THREED jobs are eligible for AutoMate only when created on or after the tenant's frozen AutoMate registration/activation boundary. Existing explicitly retained AutoMate jobs are grandfathered.";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
        CREATE TABLE IF NOT EXISTS public.tenant_job_import_policy
        (
          tenant_id uuid PRIMARY KEY,
          registration_cutoff timestamptz NOT NULL,
          source text NOT NULL DEFAULT 'first_inspector_registration',
          contract_version integer NOT NULL DEFAULT 1,
          created_at timestamptz NOT NULL DEFAULT NOW(),
          updated_at timestamptz NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS public.tenant_job_cleanup_audit
        (
          cleanup_id uuid PRIMARY KEY,
          tenant_id uuid NOT NULL,
          action_key text NOT NULL,
          registration_cutoff timestamptz NOT NULL,
          removed_job_count integer NOT NULL,
          retained_job_count integer NOT NULL,
          rule_text text NOT NULL,
          actor text NOT NULL,
          reason text NOT NULL,
          idempotency_key text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT NOW(),
          UNIQUE(tenant_id,idempotency_key)
        );
        """;
        await using(var command=new NpgsqlCommand(sql,connection))await command.ExecuteNonQueryAsync(ct);

        // created_at is immutable registration evidence. ON CONFLICT deliberately
        // does not move the boundary when another inspector is later added.
        const string seed = """
        INSERT INTO public.tenant_job_import_policy(tenant_id,registration_cutoff,source,contract_version)
        SELECT tenant_id,MIN(created_at),'first_inspector_registration',@version
        FROM public.inspectors
        WHERE tenant_id IS NOT NULL
        GROUP BY tenant_id
        ON CONFLICT(tenant_id) DO NOTHING;
        """;
        await using var seedCommand=new NpgsqlCommand(seed,connection);
        seedCommand.Parameters.AddWithValue("version",ContractVersion);
        await seedCommand.ExecuteNonQueryAsync(ct);
    }

    public static async Task<DateTimeOffset> LoadCutoffAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        await using var command=new NpgsqlCommand("SELECT registration_cutoff FROM public.tenant_job_import_policy WHERE tenant_id=@tenant",connection);
        command.Parameters.AddWithValue("tenant",tenantId);
        await using var reader=await command.ExecuteReaderAsync(ct);
        if(!await reader.ReadAsync(ct))throw new InvalidOperationException("AutoMate registration time is unavailable for this tenant. Job import is blocked.");
        return DatabaseTimeSupport.ReadRequired(reader,0);
    }

    public static async Task<JobImportDecision> EvaluateAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, DateTimeOffset? sourceDateAdded, CancellationToken ct = default)
    {
        var cutoff=await LoadCutoffAsync(connection,tenantId,ct);
        await using(var existing=new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job)",connection))
        {
            existing.Parameters.AddWithValue("tenant",tenantId.ToString("D"));existing.Parameters.AddWithValue("job",jobId);
            if(Convert.ToBoolean(await existing.ExecuteScalarAsync(ct)))return new(true,true,cutoff,JobImportPolicy.DecisionCode(true,cutoff,sourceDateAdded));
        }
        var code=JobImportPolicy.DecisionCode(false,cutoff,sourceDateAdded);
        return new(JobImportPolicy.IsEligible(false,cutoff,sourceDateAdded),false,cutoff,code);
    }

    public static async Task<HistoricJobCleanupPreview> PreviewCleanupAsync(NpgsqlConnection connection, Guid tenantId, IReadOnlyCollection<Guid> keepJobIds, CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);
        var cutoff=await LoadCutoffAsync(connection,tenantId,ct);
        var keep=NormalizeKeepIds(keepJobIds);
        var candidates=new List<Guid>();var retained=0;
        await using(var command=new NpgsqlCommand("SELECT job_id FROM public.jobs_staging WHERE tenant_id::text=@tenant ORDER BY job_id",connection))
        {
            command.Parameters.AddWithValue("tenant",tenantId.ToString("D"));
            await using var reader=await command.ExecuteReaderAsync(ct);
            while(await reader.ReadAsync(ct)){var id=reader.GetGuid(0);if(keep.Contains(id))retained++;else candidates.Add(id);}
        }
        var counts=await CountDirectJobRowsAsync(connection,candidates,ct);
        return new(tenantId,cutoff,keep.ToArray(),retained,candidates.Count,Fingerprint(tenantId,cutoff,keep,candidates),counts);
    }

    public static async Task<HistoricJobCleanupResult> ExecuteCleanupAsync(NpgsqlConnection connection, HistoricJobCleanupCommand request, string actor, CancellationToken ct=default)
    {
        if(!request.Confirmed||string.IsNullOrWhiteSpace(request.Reason)||string.IsNullOrWhiteSpace(request.IdempotencyKey))throw new InvalidOperationException("Explicit confirmation, reason and idempotency key are required.");
        var keep=NormalizeKeepIds(request.KeepJobIds);
        await EnsureAsync(connection,ct);
        var cutoff=await LoadCutoffAsync(connection,request.TenantId,ct);
        await using(var replay=new NpgsqlCommand("SELECT registration_cutoff,removed_job_count,retained_job_count,created_at FROM public.tenant_job_cleanup_audit WHERE tenant_id=@tenant AND idempotency_key=@key",connection))
        {
            replay.Parameters.AddWithValue("tenant",request.TenantId);replay.Parameters.AddWithValue("key",request.IdempotencyKey.Trim());
            await using var row=await replay.ExecuteReaderAsync(ct);
            if(await row.ReadAsync(ct))return new(request.TenantId,DatabaseTimeSupport.ReadRequired(row,0),row.GetInt32(2),row.GetInt32(1),request.ExpectedFingerprint,true,DatabaseTimeSupport.ReadRequired(row,3));
        }

        await using var tx=await connection.BeginTransactionAsync(ct);
        try
        {
            await using(var temp=new NpgsqlCommand("CREATE TEMP TABLE cleanup_job_ids(job_id uuid PRIMARY KEY) ON COMMIT DROP; INSERT INTO cleanup_job_ids(job_id) SELECT job_id FROM public.jobs_staging WHERE tenant_id::text=@tenant AND NOT(job_id=ANY(@keep));",connection,tx))
            {
                temp.Parameters.AddWithValue("tenant",request.TenantId.ToString("D"));temp.Parameters.AddWithValue("keep",keep.ToArray());await temp.ExecuteNonQueryAsync(ct);
            }
            var candidates=new List<Guid>();await using(var rows=new NpgsqlCommand("SELECT job_id FROM cleanup_job_ids ORDER BY job_id",connection,tx)){await using var reader=await rows.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))candidates.Add(reader.GetGuid(0));}
            var retained=0;await using(var count=new NpgsqlCommand("SELECT COUNT(*)::int FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=ANY(@keep)",connection,tx)){count.Parameters.AddWithValue("tenant",request.TenantId.ToString("D"));count.Parameters.AddWithValue("keep",keep.ToArray());retained=Convert.ToInt32(await count.ExecuteScalarAsync(ct));}
            if(retained!=keep.Count)throw new InvalidOperationException("All explicitly retained jobs must exist for this tenant before cleanup.");
            var actualFingerprint=Fingerprint(request.TenantId,cutoff,keep,candidates);
            if(!string.Equals(actualFingerprint,request.ExpectedFingerprint,StringComparison.Ordinal))throw new InvalidOperationException("The cleanup candidate set changed. Preview again before deleting any AutoMate data.");

            // Child rows with restrictive or non-cascading foreign keys must go first.
            const string orderedChildren="""
            DO $$ BEGIN
              IF to_regclass('public.basic_job_change_run_actions') IS NOT NULL AND to_regclass('public.basic_job_change_runs') IS NOT NULL THEN
                DELETE FROM public.basic_job_change_run_actions a USING public.basic_job_change_runs r,cleanup_job_ids x WHERE a.run_id=r.run_id AND r.job_id=x.job_id;
              END IF;
              IF to_regclass('public.email_engagement_events') IS NOT NULL THEN DELETE FROM public.email_engagement_events e USING cleanup_job_ids x WHERE e.job_id=x.job_id; END IF;
              IF to_regclass('public.email_communications') IS NOT NULL THEN DELETE FROM public.email_communications e USING cleanup_job_ids x WHERE e.job_id=x.job_id; END IF;
              IF to_regclass('public.client_inspection_pages') IS NOT NULL THEN DELETE FROM public.client_inspection_pages p USING cleanup_job_ids x WHERE p.job_id=x.job_id; END IF;
              IF to_regclass('public.job_agreement_items') IS NOT NULL THEN DELETE FROM public.job_agreement_items i USING cleanup_job_ids x WHERE i.job_id=x.job_id; END IF;
            END $$;
            """;
            await using(var childCommand=new NpgsqlCommand(orderedChildren,connection,tx))await childCommand.ExecuteNonQueryAsync(ct);

            // Remove every current/future direct job-owned table rather than leaving
            // old communications, audits, tokens or provider identifiers behind.
            const string deleteDirect="""
            DO $$ DECLARE r record; BEGIN
              FOR r IN
                SELECT c.table_name FROM information_schema.columns c
                JOIN information_schema.tables t ON t.table_schema=c.table_schema AND t.table_name=c.table_name
                WHERE c.table_schema='public' AND c.column_name='job_id' AND t.table_type='BASE TABLE'
                  AND c.table_name NOT IN ('jobs_staging','tenant_job_cleanup_audit')
                ORDER BY CASE c.table_name
                  WHEN 'email_engagement_events' THEN 1 WHEN 'email_communications' THEN 2 WHEN 'client_inspection_pages' THEN 3
                  WHEN 'job_agreement_items' THEN 4 WHEN 'job_agreement_report_overrides' THEN 5 WHEN 'job_agreement_plans' THEN 6
                  WHEN 'job_test_cycle_reconciliation_items' THEN 7 WHEN 'controlled_test_external_evidence' THEN 8 WHEN 'job_test_cycles' THEN 9
                  ELSE 20 END,c.table_name
              LOOP
                EXECUTE format('DELETE FROM public.%I d USING cleanup_job_ids x WHERE d.job_id=x.job_id',r.table_name);
              END LOOP;
            END $$;
            DELETE FROM public.jobs_staging j USING cleanup_job_ids x WHERE j.job_id=x.job_id;
            """;
            await using(var deleteCommand=new NpgsqlCommand(deleteDirect,connection,tx))await deleteCommand.ExecuteNonQueryAsync(ct);

            var remaining=0;await using(var verify=new NpgsqlCommand("SELECT COUNT(*)::int FROM public.jobs_staging WHERE tenant_id::text=@tenant AND NOT(job_id=ANY(@keep))",connection,tx)){verify.Parameters.AddWithValue("tenant",request.TenantId.ToString("D"));verify.Parameters.AddWithValue("keep",keep.ToArray());remaining=Convert.ToInt32(await verify.ExecuteScalarAsync(ct));}
            if(remaining!=0)throw new InvalidOperationException("Cleanup verification failed; the transaction was rolled back.");

            // This one-time legacy cleanup is also the activation boundary. Without
            // advancing it, post-registration historic rows deleted here could be
            // re-imported by the next local scan. The two retained rows remain
            // eligible through their durable existing-job identity.
            var completed=DateTimeOffset.UtcNow;
            await using(var boundary=new NpgsqlCommand("UPDATE public.tenant_job_import_policy SET registration_cutoff=@cutoff,source='historic_cleanup_activation',contract_version=@version,updated_at=NOW() WHERE tenant_id=@tenant",connection,tx))
            {
                boundary.Parameters.AddWithValue("cutoff",completed);boundary.Parameters.AddWithValue("version",ContractVersion);boundary.Parameters.AddWithValue("tenant",request.TenantId);
                if(await boundary.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("The tenant import boundary could not be advanced; cleanup was rolled back.");
            }
            await using(var audit=new NpgsqlCommand("INSERT INTO public.tenant_job_cleanup_audit(cleanup_id,tenant_id,action_key,registration_cutoff,removed_job_count,retained_job_count,rule_text,actor,reason,idempotency_key,created_at) VALUES(@id,@tenant,'historic_jobs_removed',@cutoff,@removed,@retained,@rule,@actor,@reason,@key,@created)",connection,tx))
            {
                audit.Parameters.AddWithValue("id",Guid.NewGuid());audit.Parameters.AddWithValue("tenant",request.TenantId);audit.Parameters.AddWithValue("cutoff",completed);audit.Parameters.AddWithValue("removed",candidates.Count);audit.Parameters.AddWithValue("retained",retained);audit.Parameters.AddWithValue("rule",RuleText);audit.Parameters.AddWithValue("actor",actor);audit.Parameters.AddWithValue("reason",request.Reason.Trim());audit.Parameters.AddWithValue("key",request.IdempotencyKey.Trim());audit.Parameters.AddWithValue("created",completed);await audit.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
            return new(request.TenantId,completed,retained,candidates.Count,actualFingerprint,false,completed);
        }
        catch{await tx.RollbackAsync(CancellationToken.None);throw;}
    }

    private static HashSet<Guid> NormalizeKeepIds(IEnumerable<Guid>? values)
    {
        var result=(values??Array.Empty<Guid>()).Where(x=>x!=Guid.Empty).ToHashSet();
        if(result.Count==0)throw new InvalidOperationException("At least one retained job is required.");
        return result;
    }

    private static string Fingerprint(Guid tenantId,DateTimeOffset cutoff,IEnumerable<Guid> keep,IEnumerable<Guid> remove)
    {
        var material=tenantId.ToString("D")+"|"+cutoff.ToUniversalTime().ToString("O")+"|keep:"+string.Join(",",keep.OrderBy(x=>x).Select(x=>x.ToString("D")))+"|remove:"+string.Join(",",remove.OrderBy(x=>x).Select(x=>x.ToString("D")));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(material))).ToLowerInvariant();
    }

    private static async Task<IReadOnlyDictionary<string,long>> CountDirectJobRowsAsync(NpgsqlConnection connection,IReadOnlyCollection<Guid> jobIds,CancellationToken ct)
    {
        var result=new SortedDictionary<string,long>(StringComparer.Ordinal);if(jobIds.Count==0)return result;
        var tables=new List<string>();await using(var list=new NpgsqlCommand("SELECT c.table_name FROM information_schema.columns c JOIN information_schema.tables t ON t.table_schema=c.table_schema AND t.table_name=c.table_name WHERE c.table_schema='public' AND c.column_name='job_id' AND t.table_type='BASE TABLE' ORDER BY c.table_name",connection)){await using var reader=await list.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))tables.Add(reader.GetString(0));}
        foreach(var table in tables)
        {
            var quoted=new NpgsqlCommandBuilder().QuoteIdentifier(table);
            await using var count=new NpgsqlCommand($"SELECT COUNT(*) FROM public.{quoted} WHERE job_id=ANY(@jobs)",connection);count.Parameters.AddWithValue("jobs",jobIds.ToArray());var value=Convert.ToInt64(await count.ExecuteScalarAsync(ct));if(value>0)result[table]=value;
        }
        return result;
    }
}
