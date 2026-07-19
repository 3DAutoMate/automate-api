using Npgsql;

namespace AutoMateApi;

public sealed record ManualPaymentOverride(Guid OverrideId, Guid TenantId, Guid JobId, decimal Amount,
    string Reason, DateTimeOffset PaymentDate, string Reference, string Actor, DateTimeOffset CreatedAt,
    DateTimeOffset? RemovedAt, string? RemovedBy, string? RemovalReason);

public sealed record ManualPaymentCommand(Guid TenantId, string IdempotencyKey, bool Confirmed,
    string Reason, DateTimeOffset? PaymentDate, string? Reference, decimal ExpectedOutstanding);

public sealed record RemoveManualPaymentCommand(Guid TenantId, string IdempotencyKey, bool Confirmed,
    string Reason, Guid ExpectedOverrideId);

public sealed record JobLifecycleCommand(Guid TenantId, string IdempotencyKey, bool Confirmed,
    string Reason, int ExpectedLifecycleVersion);
public sealed record KeepCurrentJobCommand(Guid TenantId,string IdempotencyKey,bool Confirmed,string Reason,string ExpectedThreedFingerprint);
public sealed record CancellationAccountingDecisionCommand(Guid TenantId,string IdempotencyKey,bool Confirmed,string Decision,string Reason,int ExpectedLifecycleVersion);
public sealed record SourceRemovalCommand(Guid TenantId,string IdempotencyKey,bool Confirmed,string Reason,int ExpectedLifecycleVersion);

public sealed record JobLifecycleState(Guid JobId, Guid TenantId, string AutomateStatus,
    int LifecycleVersion, string ThreedRecordState, bool ThreedComplete, DateTimeOffset? AppointmentAt,
    bool InvoiceExists, bool CalendarExists, DateTimeOffset UpdatedAt);

public sealed record JobWorkflowEvent(Guid EventId,string Step,string Action,string Status,string Detail,
    string Actor,DateTimeOffset OccurredAt,string Provider,string ExternalId,string TechnicalReferenceId);

public static class DatabaseTimeSupport
{
    private static readonly TimeZoneInfo BusinessTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Pacific/Auckland");

    public static DateTimeOffset? ReadNullable(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : ReadValue(reader.GetValue(ordinal));

    public static DateTimeOffset ReadRequired(NpgsqlDataReader reader, int ordinal)
        => ReadNullable(reader, ordinal) ?? throw new InvalidOperationException($"Database timestamp at ordinal {ordinal} is required.");

    private static DateTimeOffset ReadValue(object value) => value switch
    {
        DateTimeOffset offset => offset,
        DateTime dateTime when dateTime.Kind == DateTimeKind.Utc => new DateTimeOffset(dateTime),
        DateTime dateTime when dateTime.Kind == DateTimeKind.Local => new DateTimeOffset(dateTime),
        DateTime dateTime => new DateTimeOffset(dateTime, BusinessTimeZone.GetUtcOffset(dateTime)),
        _ when DateTimeOffset.TryParse(Convert.ToString(value), out var parsed) => parsed,
        _ => throw new InvalidOperationException($"Unsupported database timestamp value '{value}'.")
    };
}

public static class JobTrackingSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
        CREATE TABLE IF NOT EXISTS public.job_manual_payment_overrides
        (
          override_id uuid PRIMARY KEY,
          tenant_id uuid NOT NULL,
          job_id uuid NOT NULL,
          amount numeric(12,2) NOT NULL CHECK(amount>0),
          reason text NOT NULL,
          payment_date timestamptz NOT NULL,
          reference text NOT NULL DEFAULT '',
          actor text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT NOW(),
          removed_at timestamptz NULL,
          removed_by text NULL,
          removal_reason text NULL,
          idempotency_key text NOT NULL,
          UNIQUE(tenant_id,idempotency_key)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_job_manual_payment_current
          ON public.job_manual_payment_overrides(tenant_id,job_id) WHERE removed_at IS NULL;
        CREATE TABLE IF NOT EXISTS public.job_lifecycle_audit
        (
          audit_id uuid PRIMARY KEY,
          tenant_id uuid NOT NULL,
          job_id uuid NOT NULL,
          action_key text NOT NULL,
          reason text NOT NULL,
          actor text NOT NULL,
          outcomes_json jsonb NOT NULL DEFAULT '{}'::jsonb,
          idempotency_key text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT NOW(),
          UNIQUE(tenant_id,idempotency_key)
        );
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
        await JobLifecycleSupport.EnsureAsync(connection, ct);
    }

    public static async Task<ManualPaymentOverride?> LoadManualPaymentAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        const string sql = "SELECT override_id,tenant_id,job_id,amount,reason,payment_date,reference,actor,created_at,removed_at,removed_by,removal_reason FROM public.job_manual_payment_overrides WHERE tenant_id=@tenant AND job_id=@job AND removed_at IS NULL ORDER BY created_at DESC LIMIT 1";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(ct);if(!await reader.ReadAsync(ct))return null;
        return new(reader.GetGuid(0),reader.GetGuid(1),reader.GetGuid(2),reader.GetDecimal(3),reader.GetString(4),DatabaseTimeSupport.ReadRequired(reader,5),reader.GetString(6),reader.GetString(7),DatabaseTimeSupport.ReadRequired(reader,8),DatabaseTimeSupport.ReadNullable(reader,9),reader.IsDBNull(10)?null:reader.GetString(10),reader.IsDBNull(11)?null:reader.GetString(11));
    }

    public static async Task<ManualPaymentOverride> MarkPaidAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        decimal amount, ManualPaymentCommand request, string actor, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        if(!request.Confirmed||string.IsNullOrWhiteSpace(request.Reason))throw new InvalidOperationException("Confirm the manual paid mark and enter a reason.");
        if(amount<=0.005m||Math.Abs(amount-request.ExpectedOutstanding)>0.01m)throw new InvalidOperationException("The outstanding amount changed. Reload Payments before marking it paid.");
        await using var transaction=await connection.BeginTransactionAsync(ct);
        try
        {
            await using(var existing=new NpgsqlCommand("SELECT override_id FROM public.job_manual_payment_overrides WHERE tenant_id=@tenant AND idempotency_key=@key",connection,transaction)){existing.Parameters.AddWithValue("tenant",tenantId);existing.Parameters.AddWithValue("key",request.IdempotencyKey);var replay=await existing.ExecuteScalarAsync(ct);if(replay is Guid){await transaction.CommitAsync(ct);return (await LoadManualPaymentAsync(connection,tenantId,jobId,ct))!;}}
            await using(var guard=new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM public.job_manual_payment_overrides WHERE tenant_id=@tenant AND job_id=@job AND removed_at IS NULL)",connection,transaction)){guard.Parameters.AddWithValue("tenant",tenantId);guard.Parameters.AddWithValue("job",jobId);if(Convert.ToBoolean(await guard.ExecuteScalarAsync(ct)))throw new InvalidOperationException("This job is already manually marked paid.");}
            var id=Guid.NewGuid();await using(var insert=new NpgsqlCommand("INSERT INTO public.job_manual_payment_overrides(override_id,tenant_id,job_id,amount,reason,payment_date,reference,actor,idempotency_key) VALUES(@id,@tenant,@job,@amount,@reason,@date,@reference,@actor,@key)",connection,transaction)){insert.Parameters.AddWithValue("id",id);insert.Parameters.AddWithValue("tenant",tenantId);insert.Parameters.AddWithValue("job",jobId);insert.Parameters.AddWithValue("amount",amount);insert.Parameters.AddWithValue("reason",request.Reason.Trim());insert.Parameters.AddWithValue("date",request.PaymentDate??DateTimeOffset.UtcNow);insert.Parameters.AddWithValue("reference",(request.Reference??"").Trim());insert.Parameters.AddWithValue("actor",actor);insert.Parameters.AddWithValue("key",request.IdempotencyKey);await insert.ExecuteNonQueryAsync(ct);}
            // Provider-paid evidence stays authoritative. This separate AutoMate override
            // covers only the outstanding amount captured when the user confirms it.
            await using(var update=new NpgsqlCommand("UPDATE public.jobs_staging SET marked_as_paid_override=true,payment_status='manual_paid',workflow_updated_at=NOW() WHERE tenant_id::text=@tenant AND job_id=@job",connection,transaction)){update.Parameters.AddWithValue("tenant",tenantId.ToString("D"));update.Parameters.AddWithValue("job",jobId);await update.ExecuteNonQueryAsync(ct);}
            await transaction.CommitAsync(ct);
            return (await LoadManualPaymentAsync(connection,tenantId,jobId,ct))!;
        }
        catch{await transaction.RollbackAsync(CancellationToken.None);throw;}
    }

    public static async Task RemovePaidAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, RemoveManualPaymentCommand request, string actor, CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);if(!request.Confirmed||string.IsNullOrWhiteSpace(request.Reason))throw new InvalidOperationException("Confirm removal and enter a correction reason.");
        await using var transaction=await connection.BeginTransactionAsync(ct);
        try
        {
            await using var update=new NpgsqlCommand("UPDATE public.job_manual_payment_overrides SET removed_at=NOW(),removed_by=@actor,removal_reason=@reason WHERE tenant_id=@tenant AND job_id=@job AND override_id=@id AND removed_at IS NULL",connection,transaction);update.Parameters.AddWithValue("actor",actor);update.Parameters.AddWithValue("reason",request.Reason.Trim());update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("job",jobId);update.Parameters.AddWithValue("id",request.ExpectedOverrideId);if(await update.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("The current manual paid mark changed. Reload Payments.");
            await using var job=new NpgsqlCommand("UPDATE public.jobs_staging SET marked_as_paid_override=false,paid=CASE WHEN COALESCE(amount_outstanding,job_total-amount_paid)<=0.005 THEN true ELSE false END,payment_status=CASE WHEN COALESCE(amount_outstanding,job_total-amount_paid)<=0.005 THEN 'paid' WHEN COALESCE(amount_paid,0)>0 THEN 'part_paid' ELSE 'unpaid' END,workflow_updated_at=NOW() WHERE tenant_id::text=@tenant AND job_id=@job",connection,transaction);job.Parameters.AddWithValue("tenant",tenantId.ToString("D"));job.Parameters.AddWithValue("job",jobId);await job.ExecuteNonQueryAsync(ct);await transaction.CommitAsync(ct);
        }
        catch{await transaction.RollbackAsync(CancellationToken.None);throw;}
    }
}

public static class JobLifecycleSupport
{
    public const int ContractVersion = 2;
    public const string Unscheduled = "Unscheduled";
    public const string Scheduled = "Scheduled";
    public const string Cancelled = "Cancelled";
    public const string Complete = "Complete";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS automate_status text NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS lifecycle_version integer NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS lifecycle_updated_at timestamptz NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS threed_record_state text NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS threed_complete boolean NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS threed_complete_observed_at timestamptz NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS source_missing_successful_scans integer NULL;
        ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS source_removed_at timestamptz NULL;
        UPDATE public.jobs_staging SET
          automate_status=CASE
            WHEN report_workflow_sent OR LOWER(COALESCE(report_sent,'')) IN ('true','sent','complete','completed') THEN 'Complete'
            WHEN COALESCE(unscheduled,false) THEN 'Unscheduled'
            WHEN basic_scheduling_started_at IS NOT NULL THEN 'Scheduled'
            ELSE 'Unscheduled' END,
          lifecycle_version=COALESCE(lifecycle_version,1),
          lifecycle_updated_at=COALESCE(lifecycle_updated_at,workflow_updated_at,updated_at,NOW()),
          threed_record_state=COALESCE(NULLIF(threed_record_state,''),'present'),
          threed_complete=COALESCE(threed_complete,LOWER(COALESCE(status,'')) IN ('complete','completed')),
          threed_complete_observed_at=CASE WHEN COALESCE(threed_complete,LOWER(COALESCE(status,'')) IN ('complete','completed')) THEN COALESCE(threed_complete_observed_at,updated_at,NOW()) ELSE NULL END,
          source_missing_successful_scans=COALESCE(source_missing_successful_scans,0)
        WHERE automate_status IS NULL OR lifecycle_version IS NULL OR lifecycle_updated_at IS NULL
           OR threed_record_state IS NULL OR threed_complete IS NULL OR source_missing_successful_scans IS NULL
           OR (threed_complete AND threed_complete_observed_at IS NULL);
        ALTER TABLE public.jobs_staging ALTER COLUMN automate_status SET DEFAULT 'Unscheduled';
        ALTER TABLE public.jobs_staging ALTER COLUMN automate_status SET NOT NULL;
        ALTER TABLE public.jobs_staging ALTER COLUMN lifecycle_version SET DEFAULT 1;
        ALTER TABLE public.jobs_staging ALTER COLUMN lifecycle_version SET NOT NULL;
        ALTER TABLE public.jobs_staging ALTER COLUMN lifecycle_updated_at SET DEFAULT NOW();
        ALTER TABLE public.jobs_staging ALTER COLUMN lifecycle_updated_at SET NOT NULL;
        ALTER TABLE public.jobs_staging ALTER COLUMN threed_record_state SET DEFAULT 'present';
        ALTER TABLE public.jobs_staging ALTER COLUMN threed_record_state SET NOT NULL;
        ALTER TABLE public.jobs_staging ALTER COLUMN threed_complete SET DEFAULT false;
        ALTER TABLE public.jobs_staging ALTER COLUMN threed_complete SET NOT NULL;
        ALTER TABLE public.jobs_staging ALTER COLUMN source_missing_successful_scans SET DEFAULT 0;
        ALTER TABLE public.jobs_staging ALTER COLUMN source_missing_successful_scans SET NOT NULL;
        CREATE TABLE IF NOT EXISTS public.job_lifecycle_events
        (
          event_id uuid PRIMARY KEY,
          tenant_id uuid NOT NULL,
          job_id uuid NOT NULL,
          from_status text NOT NULL,
          to_status text NOT NULL,
          lifecycle_version integer NOT NULL,
          reason text NOT NULL,
          actor text NOT NULL,
          idempotency_key text NOT NULL,
          evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
          created_at timestamptz NOT NULL DEFAULT NOW(),
          UNIQUE(tenant_id,idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS ix_job_lifecycle_events_job
          ON public.job_lifecycle_events(tenant_id,job_id,created_at DESC);
        CREATE TABLE IF NOT EXISTS public.job_threed_snapshot_decisions
        (
          decision_id uuid PRIMARY KEY,
          tenant_id uuid NOT NULL,
          job_id uuid NOT NULL,
          threed_fingerprint text NOT NULL,
          decision text NOT NULL,
          reason text NOT NULL,
          actor text NOT NULL,
          idempotency_key text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT NOW(),
          UNIQUE(tenant_id,job_id,threed_fingerprint),
          UNIQUE(tenant_id,idempotency_key)
        );
        CREATE TABLE IF NOT EXISTS public.job_source_tombstones
        (
          tombstone_id uuid PRIMARY KEY,
          tenant_id uuid NOT NULL,
          job_id uuid NOT NULL,
          lifecycle_status text NOT NULL,
          lifecycle_version integer NOT NULL,
          job_label text NOT NULL,
          reason text NOT NULL,
          actor text NOT NULL,
          provider_ids_json jsonb NOT NULL DEFAULT '{}'::jsonb,
          removed_at timestamptz NOT NULL DEFAULT NOW(),
          idempotency_key text NOT NULL,
          UNIQUE(tenant_id,job_id),
          UNIQUE(tenant_id,idempotency_key)
        );
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<JobLifecycleState?> LoadAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        const string sql = """
        SELECT job_id,tenant_id,automate_status,lifecycle_version,threed_record_state,threed_complete,job_date,
               invoice_sent,calendar_created,lifecycle_updated_at
        FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job LIMIT 1
        """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId.ToString("D"));command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(ct);if(!await reader.ReadAsync(ct))return null;
        return new(reader.GetGuid(0),Guid.Parse(reader.GetValue(1).ToString()!),reader.GetString(2),reader.GetInt32(3),reader.GetString(4),reader.GetBoolean(5),
            DatabaseTimeSupport.ReadNullable(reader,6),reader.GetBoolean(7),reader.GetBoolean(8),DatabaseTimeSupport.ReadRequired(reader,9));
    }

    public static async Task<JobLifecycleState> TransitionAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId,
        string targetStatus, JobLifecycleCommand request, string actor, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        if(!request.Confirmed||string.IsNullOrWhiteSpace(request.Reason))throw new InvalidOperationException("Confirm the lifecycle change and enter a reason.");
        if(targetStatus is not (Unscheduled or Scheduled or Cancelled or Complete))throw new InvalidOperationException("Unsupported AutoMate lifecycle state.");
        await using var transaction=await connection.BeginTransactionAsync(ct);
        try
        {
            await using(var replay=new NpgsqlCommand("SELECT job_id FROM public.job_lifecycle_events WHERE tenant_id=@tenant AND idempotency_key=@key",connection,transaction))
            {replay.Parameters.AddWithValue("tenant",tenantId);replay.Parameters.AddWithValue("key",request.IdempotencyKey);if(await replay.ExecuteScalarAsync(ct) is Guid){await transaction.CommitAsync(ct);return (await LoadAsync(connection,tenantId,jobId,ct))!;}}
            string current;int version;bool invoiceExists;
            await using(var read=new NpgsqlCommand("SELECT automate_status,lifecycle_version,invoice_sent FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job FOR UPDATE",connection,transaction))
            {read.Parameters.AddWithValue("tenant",tenantId.ToString("D"));read.Parameters.AddWithValue("job",jobId);await using var row=await read.ExecuteReaderAsync(ct);if(!await row.ReadAsync(ct))throw new InvalidOperationException("The AutoMate job was not found.");current=row.GetString(0);version=row.GetInt32(1);invoiceExists=row.GetBoolean(2);}
            if(version!=request.ExpectedLifecycleVersion)throw new InvalidOperationException("The job lifecycle changed. Reload before trying again.");
            Validate(current,targetStatus);
            var nextVersion=version+1;
            await using(var update=new NpgsqlCommand("""
            UPDATE public.jobs_staging SET automate_status=@target,lifecycle_version=@version,lifecycle_updated_at=NOW(),
              unscheduled=(@target IN ('Unscheduled','Cancelled')),
              basic_scheduling_started_at=CASE WHEN @target='Scheduled' THEN COALESCE(basic_scheduling_started_at,NOW()) ELSE basic_scheduling_started_at END,
              xero_review_required=CASE WHEN @target='Cancelled' AND invoice_sent THEN true ELSE xero_review_required END,
              workflow_updated_at=NOW()
            WHERE tenant_id::text=@tenant AND job_id=@job
            """,connection,transaction))
            {update.Parameters.AddWithValue("target",targetStatus);update.Parameters.AddWithValue("version",nextVersion);update.Parameters.AddWithValue("tenant",tenantId.ToString("D"));update.Parameters.AddWithValue("job",jobId);await update.ExecuteNonQueryAsync(ct);}
            if(targetStatus is Unscheduled or Cancelled)
            {
                await using var actions=new NpgsqlCommand("UPDATE public.job_workflow_actions SET status='superseded',retry_requested=false,updated_at=NOW() WHERE job_id=@job AND status NOT IN ('sent','completed')",connection,transaction);
                actions.Parameters.AddWithValue("job",jobId);await actions.ExecuteNonQueryAsync(ct);
            }
            var evidence=$"{{\"threedChanged\":false,\"providerActions\":false,\"invoiceReview\":{invoiceExists.ToString().ToLowerInvariant()}}}";
            await using(var audit=new NpgsqlCommand("INSERT INTO public.job_lifecycle_events(event_id,tenant_id,job_id,from_status,to_status,lifecycle_version,reason,actor,idempotency_key,evidence_json) VALUES(@id,@tenant,@job,@from,@to,@version,@reason,@actor,@key,CAST(@evidence AS jsonb))",connection,transaction))
            {audit.Parameters.AddWithValue("id",Guid.NewGuid());audit.Parameters.AddWithValue("tenant",tenantId);audit.Parameters.AddWithValue("job",jobId);audit.Parameters.AddWithValue("from",current);audit.Parameters.AddWithValue("to",targetStatus);audit.Parameters.AddWithValue("version",nextVersion);audit.Parameters.AddWithValue("reason",request.Reason.Trim());audit.Parameters.AddWithValue("actor",actor);audit.Parameters.AddWithValue("key",request.IdempotencyKey);audit.Parameters.AddWithValue("evidence",evidence);await audit.ExecuteNonQueryAsync(ct);}
            await transaction.CommitAsync(ct);
            return (await LoadAsync(connection,tenantId,jobId,ct))!;
        }
        catch{await transaction.RollbackAsync(CancellationToken.None);throw;}
    }

    public static async Task MarkSchedulingStartedAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, string actor, CancellationToken ct = default)
    {
        await EnsureAsync(connection,ct);
        await using var transaction=await connection.BeginTransactionAsync(ct);
        await using var command=new NpgsqlCommand("""
        UPDATE public.jobs_staging SET automate_status='Scheduled',lifecycle_version=lifecycle_version+1,
          lifecycle_updated_at=NOW(),unscheduled=false,basic_scheduling_started_at=COALESCE(basic_scheduling_started_at,NOW()),workflow_updated_at=NOW()
        WHERE tenant_id::text=@tenant AND job_id=@job AND automate_status='Unscheduled'
        RETURNING lifecycle_version
        """,connection,transaction);
        command.Parameters.AddWithValue("tenant",tenantId.ToString("D"));command.Parameters.AddWithValue("job",jobId);var result=await command.ExecuteScalarAsync(ct);
        if(result is int version)
        {
            var key=$"schedule-start:{jobId:D}:v{version}";await using var audit=new NpgsqlCommand("INSERT INTO public.job_lifecycle_events(event_id,tenant_id,job_id,from_status,to_status,lifecycle_version,reason,actor,idempotency_key,evidence_json) VALUES(@id,@tenant,@job,'Unscheduled','Scheduled',@version,'AutoMate scheduling workflow started',@actor,@key,'{\"providerActions\":false}'::jsonb) ON CONFLICT(tenant_id,idempotency_key) DO NOTHING",connection,transaction);audit.Parameters.AddWithValue("id",Guid.NewGuid());audit.Parameters.AddWithValue("tenant",tenantId);audit.Parameters.AddWithValue("job",jobId);audit.Parameters.AddWithValue("version",version);audit.Parameters.AddWithValue("actor",actor);audit.Parameters.AddWithValue("key",key);await audit.ExecuteNonQueryAsync(ct);
        }
        await transaction.CommitAsync(ct);
    }

    public static async Task MarkReportAcceptedAsync(NpgsqlConnection connection, Guid tenantId, Guid jobId, CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);
        await using var transaction=await connection.BeginTransactionAsync(ct);await using var command=new NpgsqlCommand("""
        UPDATE public.jobs_staging SET automate_status='Complete',lifecycle_version=lifecycle_version+1,
          lifecycle_updated_at=NOW(),workflow_updated_at=NOW()
        WHERE tenant_id::text=CAST(@tenant AS text) AND job_id=@job AND automate_status='Scheduled'
        RETURNING lifecycle_version
        """,connection,transaction);
        command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("job",jobId);var result=await command.ExecuteScalarAsync(ct);if(result is int version){var key=$"report-smtp-accepted:{jobId:D}:v{version}";await using var audit=new NpgsqlCommand("INSERT INTO public.job_lifecycle_events(event_id,tenant_id,job_id,from_status,to_status,lifecycle_version,reason,actor,idempotency_key,evidence_json) VALUES(@id,@tenant,@job,'Scheduled','Complete',@version,'Company SMTP accepted the report email','AutoMate',@key,'{\"smtpAccepted\":true}'::jsonb) ON CONFLICT(tenant_id,idempotency_key) DO NOTHING",connection,transaction);audit.Parameters.AddWithValue("id",Guid.NewGuid());audit.Parameters.AddWithValue("tenant",tenantId);audit.Parameters.AddWithValue("job",jobId);audit.Parameters.AddWithValue("version",version);audit.Parameters.AddWithValue("key",key);await audit.ExecuteNonQueryAsync(ct);}await transaction.CommitAsync(ct);
    }

    private static void Validate(string current,string target)
    {
        if(!JobLifecyclePolicy.CanTransition(current,target))throw new InvalidOperationException($"A job cannot move from {current} to {target} using this action.");
    }
}
