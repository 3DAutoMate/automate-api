using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public sealed record JobReconciliationState(
    Guid JobId,
    Guid TenantId,
    string ApprovedSnapshot,
    string CurrentSnapshot,
    string ApprovedFingerprint,
    string CurrentFingerprint,
    int ApprovedVersion,
    bool ChangeReviewPending,
    bool XeroReviewRequired,
    IReadOnlyList<JobFieldChange> Changes);

public sealed record AdditionalInvoiceEvidence(
    Guid EvidenceId,
    string InvoiceId,
    string InvoiceNumber,
    decimal PreviousTotal,
    decimal CurrentTotal,
    decimal Difference,
    string Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset? SentAt,
    string LastError);

public static class JobReconciliationSupport
{
    public static async Task EnsureAsync(NpgsqlConnection conn,CancellationToken ct=default)
    {
        const string sql="""
        CREATE TABLE IF NOT EXISTS public.job_additional_invoice_evidence
        (
            evidence_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            original_invoice_id text NOT NULL,
            additional_invoice_id text NOT NULL,
            additional_invoice_number text NOT NULL DEFAULT '',
            previous_total numeric NOT NULL,
            current_total numeric NOT NULL,
            difference numeric NOT NULL,
            status text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            sent_at timestamptz NULL,
            last_error text NOT NULL DEFAULT '',
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            UNIQUE(tenant_id,job_id,additional_invoice_id)
        );
        ALTER TABLE public.job_additional_invoice_evidence ADD COLUMN IF NOT EXISTS sent_at timestamptz NULL;
        ALTER TABLE public.job_additional_invoice_evidence ADD COLUMN IF NOT EXISTS last_error text NOT NULL DEFAULT '';
        ALTER TABLE public.job_additional_invoice_evidence ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW();
        CREATE INDEX IF NOT EXISTS ix_job_additional_invoice_job ON public.job_additional_invoice_evidence(tenant_id,job_id,created_at);
        CREATE TABLE IF NOT EXISTS public.job_reconciliation_commands
        (
            command_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            command_key text NOT NULL,
            target_fingerprint text NOT NULL,
            idempotency_key text NOT NULL,
            status text NOT NULL,
            actor text NOT NULL DEFAULT '',
            detail text NOT NULL DEFAULT '',
            created_at timestamptz NOT NULL DEFAULT NOW(),
            completed_at timestamptz NULL,
            UNIQUE(tenant_id,idempotency_key),
            UNIQUE(tenant_id,job_id,command_key,target_fingerprint)
        );
        """;
        await using var cmd=new NpgsqlCommand(sql,conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<JobReconciliationState?> LoadAsync(NpgsqlConnection conn,Guid tenantId,Guid jobId,CancellationToken ct=default)
    {
        await EnsureAsync(conn,ct);
        await BasicChangeRunSupport.EnsureAsync(conn,ct);
        const string sql="""
        SELECT COALESCE(approved_snapshot_json::text,'{}'),COALESCE(current_snapshot_json::text,'{}'),
               COALESCE(approved_snapshot_fingerprint,''),COALESCE(current_snapshot_fingerprint,''),
               approved_snapshot_version,change_review_pending,xero_review_required
        FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job
        """;
        await using var cmd=new NpgsqlCommand(sql,conn);
        cmd.Parameters.AddWithValue("tenant",tenantId.ToString());cmd.Parameters.AddWithValue("job",jobId);
        string approved,current,approvedFingerprint,currentFingerprint;int approvedVersion;bool changeReviewPending,xeroReviewRequired;
        await using(var reader=await cmd.ExecuteReaderAsync(ct))
        {
            if(!await reader.ReadAsync(ct))return null;
            approved=reader.GetString(0);current=reader.GetString(1);approvedFingerprint=reader.GetString(2);currentFingerprint=reader.GetString(3);
            approvedVersion=reader.GetInt32(4);changeReviewPending=reader.GetBoolean(5);xeroReviewRequired=reader.GetBoolean(6);
        }
        IReadOnlyList<JobFieldChange> changes=JobChangeSupport.Diff(approved,current);
        if(changes.Count==0&&xeroReviewRequired)
        {
            const string reviewSql="""
            SELECT r.source_changes_json::text
            FROM public.basic_job_change_runs r
            JOIN public.basic_job_change_run_actions a ON a.run_id=r.run_id
            WHERE r.tenant_id=@tenant AND r.job_id=@job AND a.action_key='invoice' AND a.status='review_required'
            ORDER BY r.detected_at DESC,r.run_id DESC LIMIT 1
            """;
            await using var review=new NpgsqlCommand(reviewSql,conn);review.Parameters.AddWithValue("tenant",tenantId);review.Parameters.AddWithValue("job",jobId);
            var reviewJson=await review.ExecuteScalarAsync(ct) as string;
            if(!string.IsNullOrWhiteSpace(reviewJson))try{changes=JsonSerializer.Deserialize<List<JobFieldChange>>(reviewJson,new JsonSerializerOptions{PropertyNameCaseInsensitive=true})??[];}catch{/* Unreadable legacy evidence is omitted. */}
        }
        return new(jobId,tenantId,approved,current,approvedFingerprint,currentFingerprint,approvedVersion,changeReviewPending,xeroReviewRequired,changes);
    }

    public static async Task<IReadOnlyList<AdditionalInvoiceEvidence>> LoadAdditionalAsync(NpgsqlConnection conn,Guid tenantId,Guid jobId,CancellationToken ct=default)
    {
        await EnsureAsync(conn,ct);
        var result=new List<AdditionalInvoiceEvidence>();
        const string sql="""
        SELECT evidence_id,additional_invoice_id,additional_invoice_number,previous_total,current_total,difference,status,created_at,sent_at,last_error
        FROM public.job_additional_invoice_evidence WHERE tenant_id=@tenant AND job_id=@job ORDER BY created_at
        """;
        await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("tenant",tenantId);cmd.Parameters.AddWithValue("job",jobId);
        await using var reader=await cmd.ExecuteReaderAsync(ct);
        while(await reader.ReadAsync(ct))result.Add(new(reader.GetGuid(0),reader.GetString(1),reader.GetString(2),reader.GetDecimal(3),reader.GetDecimal(4),reader.GetDecimal(5),reader.GetString(6),reader.GetFieldValue<DateTimeOffset>(7),reader.IsDBNull(8)?null:reader.GetFieldValue<DateTimeOffset>(8),reader.GetString(9)));
        return result;
    }

    public static async Task<int> AcceptCurrentAsync(NpgsqlConnection conn,Guid tenantId,Guid jobId,string expectedFingerprint,string actor,CancellationToken ct=default)
    {
        await EnsureAsync(conn,ct);
        await using var tx=await conn.BeginTransactionAsync(ct);
        string current="{}",fingerprint="";int version=0;bool pending=false;
        await using(var select=new NpgsqlCommand("SELECT COALESCE(current_snapshot_json::text,'{}'),COALESCE(current_snapshot_fingerprint,''),approved_snapshot_version,change_review_pending FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job FOR UPDATE",conn,tx))
        {
            select.Parameters.AddWithValue("tenant",tenantId.ToString());select.Parameters.AddWithValue("job",jobId);
            await using var reader=await select.ExecuteReaderAsync(ct);if(!await reader.ReadAsync(ct))throw new KeyNotFoundException("The job does not belong to this company.");
            current=reader.GetString(0);fingerprint=reader.GetString(1);version=reader.GetInt32(2);pending=reader.GetBoolean(3);
        }
        if(!string.Equals(fingerprint,expectedFingerprint,StringComparison.Ordinal))throw new JobReconciliationException("stale_job","THREED changed after this comparison loaded. Refresh before updating AutoMate.");
        if(!pending)
        {
            await tx.CommitAsync(ct);return version;
        }
        var next=Math.Max(1,version+1);
        const string update="""
        UPDATE public.jobs_staging SET approved_snapshot_json=CAST(@snapshot AS jsonb),approved_snapshot_fingerprint=@fingerprint,
        approved_snapshot_version=@version,change_review_pending=false,pending_change_json=NULL,pending_change_fingerprint=NULL,
        pending_change_reasons=NULL,change_detected_at=NULL,change_confirmed_at=NOW(),change_confirmed_by=@actor,
        address_change_pending=false,current_snapshot_captured_at=COALESCE(current_snapshot_captured_at,NOW()),
        current_snapshot_source_modified_at=COALESCE(current_snapshot_source_modified_at,source_updated_at),
        live_baseline_updated_at=NOW(),workflow_updated_at=NOW() WHERE tenant_id::text=@tenant AND job_id=@job
        """;
        await using(var cmd=new NpgsqlCommand(update,conn,tx))
        {
            cmd.Parameters.AddWithValue("tenant",tenantId.ToString());cmd.Parameters.AddWithValue("job",jobId);cmd.Parameters.AddWithValue("snapshot",current);
            cmd.Parameters.AddWithValue("fingerprint",fingerprint);cmd.Parameters.AddWithValue("version",next);cmd.Parameters.AddWithValue("actor",actor??"");await cmd.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        await JobChangeSupport.AuditAsync(conn,jobId,tenantId,next,"automate_job_updated",fingerprint,"[]","accepted_current_threed",actor??"",null);
        return next;
    }

    public static async Task<bool> ReserveAsync(NpgsqlConnection conn,Guid tenantId,Guid jobId,string command,string targetFingerprint,string idempotencyKey,string actor,CancellationToken ct=default)
    {
        await EnsureAsync(conn,ct);
        const string sql="""
        INSERT INTO public.job_reconciliation_commands(tenant_id,job_id,command_key,target_fingerprint,idempotency_key,status,actor)
        VALUES(@tenant,@job,@command,@target,@key,'running',@actor)
        ON CONFLICT(tenant_id,job_id,command_key,target_fingerprint) DO UPDATE SET
          idempotency_key=EXCLUDED.idempotency_key,status='running',actor=EXCLUDED.actor,detail='',created_at=NOW(),completed_at=NULL
        WHERE public.job_reconciliation_commands.status='failed'
        """;
        await using var cmd=new NpgsqlCommand(sql,conn);cmd.Parameters.AddWithValue("tenant",tenantId);cmd.Parameters.AddWithValue("job",jobId);cmd.Parameters.AddWithValue("command",command);cmd.Parameters.AddWithValue("target",targetFingerprint);cmd.Parameters.AddWithValue("key",idempotencyKey);cmd.Parameters.AddWithValue("actor",actor??"");return await cmd.ExecuteNonQueryAsync(ct)==1;
    }

    public static async Task CompleteCommandAsync(NpgsqlConnection conn,Guid tenantId,string idempotencyKey,bool success,string detail,CancellationToken ct=default)
    {
        await using var cmd=new NpgsqlCommand("UPDATE public.job_reconciliation_commands SET status=@status,detail=@detail,completed_at=NOW() WHERE tenant_id=@tenant AND idempotency_key=@key",conn);
        cmd.Parameters.AddWithValue("tenant",tenantId);cmd.Parameters.AddWithValue("key",idempotencyKey);cmd.Parameters.AddWithValue("status",success?"completed":"failed");cmd.Parameters.AddWithValue("detail",detail??"");await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task MarkAdditionalSentAsync(NpgsqlConnection conn,Guid tenantId,Guid jobId,string invoiceId,bool success,string error,CancellationToken ct=default)
    {
        await using var cmd=new NpgsqlCommand("UPDATE public.job_additional_invoice_evidence SET sent_at=CASE WHEN @success THEN NOW() ELSE sent_at END,last_error=@error,updated_at=NOW() WHERE tenant_id=@tenant AND job_id=@job AND additional_invoice_id=@invoice",conn);
        cmd.Parameters.AddWithValue("tenant",tenantId);cmd.Parameters.AddWithValue("job",jobId);cmd.Parameters.AddWithValue("invoice",invoiceId);cmd.Parameters.AddWithValue("success",success);cmd.Parameters.AddWithValue("error",error??"");await cmd.ExecuteNonQueryAsync(ct);
    }
}

public sealed class JobReconciliationException(string code,string message):Exception(message)
{
    public string Code { get; }=code;
}
