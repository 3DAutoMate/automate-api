using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

public sealed record JobChangePreparation(
    string SnapshotJson,
    string Fingerprint,
    bool Pending,
    string ChangesJson,
    string Reasons,
    bool XeroReview,
    bool ReportReview,
    int ApprovedVersion);

public sealed class JobFieldChange
{
    public string field { get; set; } = "";
    public string oldValue { get; set; } = "";
    public string newValue { get; set; } = "";
    public string category { get; set; } = "";
}

public static class JobChangeSupport
{
    public static async Task EnsureAsync(NpgsqlConnection conn)
    {
        const string sql = @"
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS approved_snapshot_json jsonb NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS approved_snapshot_fingerprint text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS approved_snapshot_version integer NOT NULL DEFAULT 0;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS current_snapshot_json jsonb NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS current_snapshot_fingerprint text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS current_snapshot_captured_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS current_snapshot_source_modified_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS live_baseline_updated_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_review_pending boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS pending_change_json jsonb NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS pending_change_fingerprint text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS pending_change_reasons text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_detected_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_confirmed_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_confirmed_by text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS xero_review_required boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS report_review_required boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS xero_review_change_owned boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS report_review_change_owned boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_template_setup_required boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS source_missing boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS source_missing_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS unscheduled boolean NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS public.job_change_audit
(
 audit_id bigserial PRIMARY KEY, job_id uuid NOT NULL, tenant_id uuid NULL, approved_version integer NOT NULL,
 event_type text NOT NULL, snapshot_fingerprint text NULL, changes_json jsonb NULL, reasons text NULL,
 actor text NULL, created_at timestamptz NOT NULL DEFAULT NOW()
);
UPDATE public.jobs_staging SET
 change_review_pending=true,
 pending_change_json=jsonb_build_array(jsonb_build_object('field','address','oldValue',COALESCE(previous_site_address,''),'newValue',COALESCE(site_address,''),'category','address')),
 pending_change_fingerprint=md5(COALESCE(previous_site_address,'') || '>' || COALESCE(site_address,'')),
 pending_change_reasons='address',change_detected_at=COALESCE(address_change_detected_at,NOW()),
 xero_review_required=(xero_review_required OR invoice_sent),report_review_required=(report_review_required OR report_workflow_sent OR COALESCE(report_sent,'') <> '')
WHERE address_change_pending=true AND change_review_pending=false;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task BackfillApprovedSnapshotsAsync(NpgsqlConnection conn)
    {
        var rows = new List<(Guid JobId, string Tenant, string Payload)>();
        await using (var select = new NpgsqlCommand(@"SELECT job_id,COALESCE(tenant_id::text,''),COALESCE(raw_payload_json,'') FROM public.jobs_staging
WHERE approved_snapshot_json IS NULL AND COALESCE(raw_payload_json,'') <> ''", conn))
        await using (var reader = await select.ExecuteReaderAsync())
            while (await reader.ReadAsync()) rows.Add((reader.GetGuid(0), reader.GetString(1), reader.GetString(2)));
        foreach (var row in rows)
        {
            JobUploadRequest? payload;
            try { payload = JsonSerializer.Deserialize<JobUploadRequest>(row.Payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }); }
            catch { continue; }
            if (payload == null) continue;
            string snapshot = BuildSnapshot(payload), fingerprint = Fingerprint(snapshot);
            await using var update = new NpgsqlCommand(@"UPDATE public.jobs_staging SET approved_snapshot_json=CAST(@snapshot AS jsonb),approved_snapshot_fingerprint=@fingerprint,
approved_snapshot_version=1,current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint,
current_snapshot_captured_at=NOW(),current_snapshot_source_modified_at=source_updated_at,live_baseline_updated_at=NOW()
WHERE job_id=@job AND approved_snapshot_json IS NULL", conn);
            update.Parameters.AddWithValue("job", row.JobId); update.Parameters.AddWithValue("snapshot", snapshot); update.Parameters.AddWithValue("fingerprint", fingerprint); await update.ExecuteNonQueryAsync();
        }
    }

    public static async Task<int> AdoptInactiveLiveBaselinesAsync(NpgsqlConnection conn, string actor, CancellationToken cancellationToken = default)
    {
        await EnsureAsync(conn);
        var rows = new List<(Guid JobId, Guid TenantId, string Fingerprint)>();
        await using (var select = new NpgsqlCommand(@"SELECT job_id,tenant_id::text,COALESCE(current_snapshot_fingerprint,'')
FROM public.jobs_staging
WHERE change_review_pending=true AND automate_status IN ('Unscheduled','Cancelled','Complete')
  AND current_snapshot_json IS NOT NULL AND COALESCE(current_snapshot_fingerprint,'')<>''", conn))
        await using (var reader = await select.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                if (Guid.TryParse(reader.GetString(1), out var tenant)) rows.Add((reader.GetGuid(0), tenant, reader.GetString(2)));
        var adopted = 0;
        foreach (var row in rows)
        {
            await AutoMateApi.JobReconciliationSupport.AcceptCurrentAsync(conn,row.TenantId,row.JobId,row.Fingerprint,actor,cancellationToken);
            adopted++;
        }
        return adopted;
    }

    public static async Task RepairPendingChangesAsync(NpgsqlConnection conn)
    {
        var rows = new List<(Guid JobId, string Approved, string Current)>();
        await using (var select = new NpgsqlCommand(@"SELECT job_id,approved_snapshot_json::text,current_snapshot_json::text FROM public.jobs_staging
WHERE change_review_pending=true AND approved_snapshot_json IS NOT NULL AND current_snapshot_json IS NOT NULL", conn))
        await using (var reader = await select.ExecuteReaderAsync())
            while (await reader.ReadAsync()) rows.Add((reader.GetGuid(0), reader.GetString(1), reader.GetString(2)));
        foreach (var row in rows)
        {
            var changes = Diff(row.Approved, row.Current);
            if (changes.Count == 0)
            {
                await using var clear = new NpgsqlCommand(@"UPDATE public.jobs_staging SET change_review_pending=false,pending_change_json=NULL,
pending_change_reasons=NULL,pending_change_fingerprint=NULL,change_detected_at=NULL,
xero_review_required=CASE WHEN xero_review_change_owned THEN false ELSE xero_review_required END,
report_review_required=CASE WHEN report_review_change_owned THEN false ELSE report_review_required END,
xero_review_change_owned=false,report_review_change_owned=false
WHERE job_id=@job AND change_review_pending=true", conn);
                clear.Parameters.AddWithValue("job", row.JobId); await clear.ExecuteNonQueryAsync();
                continue;
            }
            string json = JsonSerializer.Serialize(changes); string reasons = string.Join(",", changes.Select(change => change.category).Distinct()); string fingerprint = Fingerprint(row.Current);
            await using var update = new NpgsqlCommand(@"UPDATE public.jobs_staging SET pending_change_json=CAST(@changes AS jsonb),pending_change_reasons=@reasons,
pending_change_fingerprint=@fingerprint WHERE job_id=@job AND change_review_pending=true", conn);
            update.Parameters.AddWithValue("job", row.JobId); update.Parameters.AddWithValue("changes", json); update.Parameters.AddWithValue("reasons", reasons); update.Parameters.AddWithValue("fingerprint", fingerprint); await update.ExecuteNonQueryAsync();
        }
    }

    public static string BuildSnapshot(JobUploadRequest payload)
    {
        static string T(string? value) => string.Join(" ", (value ?? "").Trim().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        static string Money(string? value) => decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var amount) ? amount.ToString("0.00", CultureInfo.InvariantCulture) : T(value);
        var fields = new SortedDictionary<string, object?>(StringComparer.Ordinal)
        {
            ["snapshotSchemaVersion"] = 2,
            ["address"] = T(payload.Job?.SiteAddress), ["jobDate"] = T(payload.Job?.JobDate),
            ["inspectorId"] = T(payload.Job?.InspectorId), ["inspectorName"] = T(payload.Job?.InspectorName),
            ["durationMinutes"] = payload.Job?.InspectionDurationMinutes ?? 0, ["invoiceTotal"] = Money(payload.Job?.InvoiceTotal),
            ["notes"] = T(payload.Job?.Notes), ["directions"] = T(payload.Job?.Directions), ["instructions"] = T(payload.Job?.Instructions),
            ["primaryService"] = T(payload.Services?.Primary), ["additionalService1"] = T(payload.Services?.Additional1), ["additionalService2"] = T(payload.Services?.Additional2),
            ["buildingType"] = T(payload.JobDetails?.BuildingType), ["stories"] = T(payload.JobDetails?.Stories), ["floorArea"] = T(payload.JobDetails?.FloorArea),
            ["outbuilding"] = T(payload.JobDetails?.Outbuilding), ["occupied"] = T(payload.JobDetails?.Occupied),
            ["attachedFlat"] = T(payload.JobDetails?.AttachedFlat), ["travelFee"] = T(payload.JobDetails?.TravelFee),
            ["hhsBedrooms"] = T(payload.JobDetails?.HhsBedrooms), ["methSamples"] = T(payload.JobDetails?.MethSamples),
            ["hhsReinspect"] = T(payload.JobDetails?.HhsReinspect), ["councilFiles"] = T(payload.JobDetails?.CouncilFiles),
            ["foundationSpace"] = T(payload.JobDetails?.FoundationSpace), ["weathertightness"] = T(payload.JobDetails?.Weathertightness),
            ["hhsReinspectDate"] = T(payload.JobDetails?.HhsReinspectDate), ["accessBy"] = T(payload.JobDetails?.AccessBy),
            ["hhsCompliance"] = T(payload.JobDetails?.HhsCompliance),
            ["clientFirstName"] = T(payload.Contact1?.FirstName), ["clientLastName"] = T(payload.Contact1?.LastName),
            ["clientEmail"] = T(payload.Contact1?.Email).ToLowerInvariant(), ["clientPhone"] = T(payload.Contact1?.Cellular),
            ["agentName"] = T(string.Join(" ", new[] { payload.Contact2?.FirstName, payload.Contact2?.LastName }.Where(v => !string.IsNullOrWhiteSpace(v)))),
            ["agentDisplayName"] = T(payload.Contact2?.DisplayName), ["agentSalutation"] = T(payload.Contact2?.Salutation),
            ["agentEmail"] = T(payload.Contact2?.Email).ToLowerInvariant(), ["agentPhone"] = T(payload.Contact2?.Cellular),
            ["invoiceLines"] = (payload.InvoiceLines ?? new()).OrderBy(line => line.LineIndex).Select(line => new
            {
                index = line.LineIndex, description = T(line.Description), quantity = line.Quantity.ToString("0.####", CultureInfo.InvariantCulture),
                unitPrice = line.UnitPrice.ToString("0.00", CultureInfo.InvariantCulture)
            }).ToArray()
        };
        return JsonSerializer.Serialize(fields);
    }

    public static string Fingerprint(string json)
    {
        string canonical = CanonicalizeSnapshot(json);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical))).ToLowerInvariant();
    }

    public static async Task<JobChangePreparation> PrepareAsync(NpgsqlConnection conn, Guid jobId, JobUploadRequest payload)
    {
        await EnsureAsync(conn);
        string currentJson = BuildSnapshot(payload), currentFingerprint = Fingerprint(currentJson);
        string approvedJson = ""; int version = 0; bool workflowStarted = false; bool reportDone = false;
        await using (var cmd = new NpgsqlCommand(@"SELECT COALESCE(approved_snapshot_json::text,''),approved_snapshot_version,
(booking_email_sent OR terms_sent OR calendar_created OR invoice_sent), (report_workflow_sent OR COALESCE(report_sent,'') <> '')
FROM public.jobs_staging WHERE job_id=@job_id", conn))
        {
            cmd.Parameters.AddWithValue("job_id", jobId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync()) { approvedJson = reader.GetString(0); version = reader.GetInt32(1); workflowStarted = reader.GetBoolean(2); reportDone = reader.GetBoolean(3); }
        }
        if (string.IsNullOrWhiteSpace(approvedJson)) return new(currentJson, currentFingerprint, false, "[]", "", false, false, Math.Max(1, version));
        var changes = Diff(approvedJson, currentJson);
        if (changes.Count == 0) return new(currentJson, currentFingerprint, false, "[]", "", false, false, Math.Max(1, version));
        if (!workflowStarted) return new(currentJson, currentFingerprint, false, "[]", "", false, false, Math.Max(1, version + 1));
        var categories = changes.Select(change => change.category).Distinct().ToArray();
        bool xero = categories.Any(category => category is "address" or "services" or "price" or "customer");
        return new(currentJson, currentFingerprint, true, JsonSerializer.Serialize(changes), string.Join(",", categories), xero, reportDone, Math.Max(1, version));
    }

    public static async Task ApplyAsync(NpgsqlConnection conn, Guid jobId, Guid tenantId, JobChangePreparation change)
    {
        await using var transaction = await conn.BeginTransactionAsync();
        try
        {
            string approvedJson = "";
            string storedCurrentJson = "";
            string storedPendingChanges = "[]";
            string currentFingerprint = "";
            string pendingFingerprint = "";
            int approvedVersion = 0;
            bool pending = false;
            bool workflowStarted = false;
            bool reportDone = false;
            bool xeroRequired = false;
            bool reportRequired = false;
            bool xeroOwned = false;
            bool reportOwned = false;

            await using (var select = new NpgsqlCommand(@"SELECT COALESCE(approved_snapshot_json::text,''),approved_snapshot_version,
COALESCE(current_snapshot_fingerprint,''),change_review_pending,COALESCE(pending_change_fingerprint,''),
(booking_email_sent OR terms_sent OR calendar_created OR invoice_sent),
(report_workflow_sent OR COALESCE(report_sent,'') <> ''),xero_review_required,report_review_required,
xero_review_change_owned,report_review_change_owned,COALESCE(current_snapshot_json::text,''),COALESCE(pending_change_json::text,'[]')
FROM public.jobs_staging WHERE job_id=@job FOR UPDATE", conn, transaction))
            {
                select.Parameters.AddWithValue("job", jobId);
                await using var reader = await select.ExecuteReaderAsync();
                if (!await reader.ReadAsync())
                {
                    await transaction.RollbackAsync();
                    return;
                }

                approvedJson = reader.GetString(0);
                approvedVersion = reader.GetInt32(1);
                currentFingerprint = reader.GetString(2);
                pending = reader.GetBoolean(3);
                pendingFingerprint = reader.GetString(4);
                workflowStarted = reader.GetBoolean(5);
                reportDone = reader.GetBoolean(6);
                xeroRequired = reader.GetBoolean(7);
                reportRequired = reader.GetBoolean(8);
                xeroOwned = reader.GetBoolean(9);
                reportOwned = reader.GetBoolean(10);
                storedCurrentJson = reader.GetString(11);
                storedPendingChanges = reader.GetString(12);
            }

            string fingerprint = Fingerprint(change.SnapshotJson);
            if (string.IsNullOrWhiteSpace(approvedJson))
            {
                await UpdateBaselineAsync(conn, transaction, jobId, change.SnapshotJson, fingerprint, Math.Max(1, approvedVersion));
                await transaction.CommitAsync();
                return;
            }

            using (var approvedDocument = JsonDocument.Parse(approvedJson))
            {
                if (!approvedDocument.RootElement.TryGetProperty("snapshotSchemaVersion", out var schema) ||
                    !schema.TryGetInt32(out var schemaVersion) || schemaVersion < 2)
                {
                    // Schema migration only: capture the same live THREED state using
                    // First/Last Name and inspector identity without firing providers.
                    await UpdateBaselineAsync(conn, transaction, jobId, change.SnapshotJson, fingerprint, Math.Max(1, approvedVersion + 1));
                    await AuditAsync(conn, jobId, tenantId, Math.Max(1, approvedVersion + 1), "snapshot_schema_upgraded", fingerprint, "[]", "representation_only", "AutoMate migration", transaction);
                    await transaction.CommitAsync();
                    return;
                }
            }

            var differences = Diff(approvedJson, change.SnapshotJson);
            if (differences.Count == 0)
            {
                if (pending)
                {
                    await using var clear = new NpgsqlCommand(@"UPDATE public.jobs_staging SET
current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint,
change_review_pending=false,pending_change_json=NULL,pending_change_fingerprint=NULL,pending_change_reasons=NULL,
change_detected_at=NULL,xero_review_required=CASE WHEN xero_review_change_owned THEN false ELSE xero_review_required END,
report_review_required=CASE WHEN report_review_change_owned THEN false ELSE report_review_required END,
xero_review_change_owned=false,report_review_change_owned=false,source_missing=false,source_missing_at=NULL
WHERE job_id=@job", conn, transaction);
                    clear.Parameters.AddWithValue("job", jobId);
                    clear.Parameters.AddWithValue("snapshot", change.SnapshotJson);
                    clear.Parameters.AddWithValue("fingerprint", fingerprint);
                    await clear.ExecuteNonQueryAsync();
                    await AuditAsync(conn, jobId, tenantId, approvedVersion, "reverted", fingerprint, storedPendingChanges, "", "3D sync", transaction);
                }
                else if (!string.Equals(currentFingerprint, fingerprint, StringComparison.Ordinal))
                {
                    await using var current = new NpgsqlCommand(@"UPDATE public.jobs_staging SET current_snapshot_json=CAST(@snapshot AS jsonb),
current_snapshot_fingerprint=@fingerprint,source_missing=false,source_missing_at=NULL WHERE job_id=@job", conn, transaction);
                    current.Parameters.AddWithValue("job", jobId);
                    current.Parameters.AddWithValue("snapshot", change.SnapshotJson);
                    current.Parameters.AddWithValue("fingerprint", fingerprint);
                    await current.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
                return;
            }

            if (!workflowStarted)
            {
                await UpdateBaselineAsync(conn, transaction, jobId, change.SnapshotJson, fingerprint, Math.Max(1, approvedVersion + 1));
                await transaction.CommitAsync();
                return;
            }

            string changesJson = JsonSerializer.Serialize(differences);
            string reasons = string.Join(",", differences.Select(item => item.category).Distinct(StringComparer.Ordinal));
            var categories = differences.Select(item => item.category).ToHashSet(StringComparer.Ordinal);
            bool needsXero = categories.Overlaps(new[] { "address", "services", "price", "customer" });
            bool needsReport = reportDone;

            // The row lock makes this an atomic no-op even if several connector scans arrive together.
            if (pending && (string.Equals(pendingFingerprint, fingerprint, StringComparison.Ordinal) ||
                (!string.IsNullOrWhiteSpace(storedCurrentJson) && Diff(storedCurrentJson, change.SnapshotJson).Count == 0)))
            {
                await transaction.CommitAsync();
                return;
            }

            bool nextXeroRequired = xeroRequired;
            bool nextXeroOwned = xeroOwned;
            if (needsXero && !xeroRequired) { nextXeroRequired = true; nextXeroOwned = true; }
            else if (!needsXero && xeroOwned) { nextXeroRequired = false; nextXeroOwned = false; }

            bool nextReportRequired = reportRequired;
            bool nextReportOwned = reportOwned;
            if (needsReport && !reportRequired) { nextReportRequired = true; nextReportOwned = true; }
            else if (!needsReport && reportOwned) { nextReportRequired = false; nextReportOwned = false; }

            await using (var update = new NpgsqlCommand(@"UPDATE public.jobs_staging SET
current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint,
current_snapshot_captured_at=NOW(),current_snapshot_source_modified_at=source_updated_at,
change_review_pending=true,pending_change_json=CAST(@changes AS jsonb),pending_change_fingerprint=@fingerprint,
pending_change_reasons=@reasons,change_detected_at=CASE WHEN change_review_pending THEN COALESCE(change_detected_at,NOW()) ELSE NOW() END,
xero_review_required=@xero,xero_review_change_owned=@xero_owned,
report_review_required=@report,report_review_change_owned=@report_owned,source_missing=false,source_missing_at=NULL
WHERE job_id=@job", conn, transaction))
            {
                update.Parameters.AddWithValue("job", jobId);
                update.Parameters.AddWithValue("snapshot", change.SnapshotJson);
                update.Parameters.AddWithValue("fingerprint", fingerprint);
                update.Parameters.AddWithValue("changes", changesJson);
                update.Parameters.AddWithValue("reasons", reasons);
                update.Parameters.AddWithValue("xero", nextXeroRequired);
                update.Parameters.AddWithValue("xero_owned", nextXeroOwned);
                update.Parameters.AddWithValue("report", nextReportRequired);
                update.Parameters.AddWithValue("report_owned", nextReportOwned);
                await update.ExecuteNonQueryAsync();
            }

            await AuditAsync(conn, jobId, tenantId, approvedVersion, pending ? "revised" : "detected", fingerprint, changesJson, reasons, "3D sync", transaction);
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    private static async Task UpdateBaselineAsync(NpgsqlConnection conn, NpgsqlTransaction transaction, Guid jobId, string snapshot, string fingerprint, int version)
    {
        await using var cmd = new NpgsqlCommand(@"UPDATE public.jobs_staging SET
approved_snapshot_json=CAST(@snapshot AS jsonb),approved_snapshot_fingerprint=@fingerprint,approved_snapshot_version=@version,
current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint,
current_snapshot_captured_at=NOW(),current_snapshot_source_modified_at=source_updated_at,live_baseline_updated_at=NOW(),
change_review_pending=false,pending_change_json=NULL,pending_change_fingerprint=NULL,pending_change_reasons=NULL,change_detected_at=NULL,
xero_review_required=CASE WHEN xero_review_change_owned THEN false ELSE xero_review_required END,
report_review_required=CASE WHEN report_review_change_owned THEN false ELSE report_review_required END,
xero_review_change_owned=false,report_review_change_owned=false,source_missing=false,source_missing_at=NULL WHERE job_id=@job", conn, transaction);
        cmd.Parameters.AddWithValue("job", jobId);
        cmd.Parameters.AddWithValue("snapshot", snapshot);
        cmd.Parameters.AddWithValue("fingerprint", fingerprint);
        cmd.Parameters.AddWithValue("version", version);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<int> AcceptForAutomaticRunAsync(NpgsqlConnection conn,Guid jobId,Guid tenantId,string snapshot,string fingerprint,string changes,string reasons)
    {
        await using var transaction=await conn.BeginTransactionAsync();
        var version=1;
        await using(var select=new NpgsqlCommand("SELECT approved_snapshot_version FROM public.jobs_staging WHERE job_id=@job AND tenant_id::text=@tenant FOR UPDATE",conn,transaction))
        {
            select.Parameters.AddWithValue("job",jobId);select.Parameters.AddWithValue("tenant",tenantId.ToString());
            var value=await select.ExecuteScalarAsync();if(value==null){await transaction.RollbackAsync();throw new KeyNotFoundException("The synchronized job was not found.");}
            version=Math.Max(1,Convert.ToInt32(value)+1);
        }
        // Advancing the THREED baseline must not reset or rewrite any provider
        // evidence. The durable change-run ledger owns every follow-up action.
        await UpdateBaselineAsync(conn,transaction,jobId,snapshot,fingerprint,version);
        await AuditAsync(conn,jobId,tenantId,version,"automatic_change_run_prepared",fingerprint,changes,reasons,"THREED sync",transaction);
        await transaction.CommitAsync();
        return version;
    }

    public static async Task AuditAsync(NpgsqlConnection conn, Guid jobId, Guid tenantId, int version, string eventType, string fingerprint, string changes, string reasons, string actor, NpgsqlTransaction? transaction = null)
    {
        await using var cmd = new NpgsqlCommand(@"INSERT INTO public.job_change_audit(job_id,tenant_id,approved_version,event_type,snapshot_fingerprint,changes_json,reasons,actor)
VALUES(@job,@tenant,@version,@event,@fingerprint,CAST(@changes AS jsonb),@reasons,@actor)", conn, transaction);
        cmd.Parameters.AddWithValue("job", jobId); cmd.Parameters.AddWithValue("tenant", tenantId == Guid.Empty ? DBNull.Value : tenantId); cmd.Parameters.AddWithValue("version", version);
        cmd.Parameters.AddWithValue("event", eventType); cmd.Parameters.AddWithValue("fingerprint", fingerprint ?? ""); cmd.Parameters.AddWithValue("changes", string.IsNullOrWhiteSpace(changes) ? "[]" : changes);
        cmd.Parameters.AddWithValue("reasons", reasons ?? ""); cmd.Parameters.AddWithValue("actor", actor ?? ""); await cmd.ExecuteNonQueryAsync();
    }

    public static List<JobFieldChange> Diff(string approvedJson, string currentJson)
    {
        using var oldDoc = JsonDocument.Parse(approvedJson); using var newDoc = JsonDocument.Parse(currentJson);
        var result = new List<JobFieldChange>();
        foreach (var property in newDoc.RootElement.EnumerateObject())
        {
            oldDoc.RootElement.TryGetProperty(property.Name, out var oldValue);
            string before = oldValue.ValueKind == JsonValueKind.Undefined ? "" : oldValue.GetRawText(); string after = property.Value.GetRawText();
            if (SemanticallyEqual(property.Name, before, after)) continue;
            result.Add(new JobFieldChange { field = property.Name, oldValue = Display(before), newValue = Display(after), category = Category(property.Name) });
        }
        return result;
    }

    private static string CanonicalizeSnapshot(string json)
    {
        using var document = JsonDocument.Parse(json);
        var canonical = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var property in document.RootElement.EnumerateObject())
            canonical[property.Name] = CanonicalValue(property.Name, property.Value.GetRawText());
        return JsonSerializer.Serialize(canonical);
    }

    private static bool SemanticallyEqual(string field, string before, string after) =>
        string.Equals(CanonicalValue(field, before), CanonicalValue(field, after), StringComparison.Ordinal);

    private static string CanonicalValue(string field, string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "";
        try
        {
            using var document = JsonDocument.Parse(raw);
            var element = document.RootElement;
            if (field == "invoiceLines" && element.ValueKind == JsonValueKind.Array)
            {
                var lines = element.EnumerateArray().Select(line => new
                {
                    index = ReadInt(line, "index"),
                    description = NormalizeText(ReadString(line, "description"), ignoreCase: true),
                    quantity = NormalizeDecimal(ReadString(line, "quantity"), 4),
                    unitPrice = NormalizeDecimal(ReadString(line, "unitPrice"), 2)
                }).OrderBy(line => line.index).ToArray();
                return JsonSerializer.Serialize(lines);
            }

            string value = element.ValueKind == JsonValueKind.String ? element.GetString() ?? "" : element.ToString();
            return field switch
            {
                "address" => NormalizeAddress(value),
                "invoiceTotal" or "travelFee" => NormalizeDecimal(value, 2), "floorArea" => NormalizeDecimal(value, 4),
                "durationMinutes" => NormalizeDecimal(value, 0),
                "jobDate" or "hhsReinspectDate" => NormalizeDate(value),
                "clientEmail" or "agentEmail" => NormalizeText(value, ignoreCase: true),
                "clientPhone" or "agentPhone" => new string(value.Where(char.IsDigit).ToArray()),
                _ => NormalizeText(value, ignoreCase: true)
            };
        }
        catch
        {
            return NormalizeText(raw, ignoreCase: true);
        }
    }

    private static string NormalizeText(string? value, bool ignoreCase)
    {
        string normalized = string.Join(" ", (value ?? "").Trim().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        return ignoreCase ? normalized.ToLowerInvariant() : normalized;
    }

    private static string NormalizeAddress(string value)
    {
        // Punctuation is insignificant, but it remains a token boundary so unit 1/2 cannot collide with street number 12.
        var characters = value.Select(character => char.IsLetterOrDigit(character) ? char.ToLowerInvariant(character) : ' ').ToArray();
        string normalized = NormalizeText(new string(characters), ignoreCase: true);
        var result = new StringBuilder(normalized.Length);
        for (int index = 0; index < normalized.Length; index++)
        {
            char character = normalized[index];
            if (character == ' ' && index > 0 && index + 1 < normalized.Length &&
                ((char.IsDigit(normalized[index - 1]) && char.IsLetter(normalized[index + 1])) ||
                 (char.IsLetter(normalized[index - 1]) && char.IsDigit(normalized[index + 1]))))
                continue;
            result.Append(character);
        }
        return result.ToString();
    }

    private static string NormalizeDecimal(string value, int scale)
    {
        string cleaned = (value ?? "").Trim().Replace("$", "", StringComparison.Ordinal).Replace(",", "", StringComparison.Ordinal);
        if (!decimal.TryParse(cleaned, NumberStyles.Any, CultureInfo.InvariantCulture, out var number) &&
            !decimal.TryParse(cleaned, NumberStyles.Any, CultureInfo.CurrentCulture, out number))
            return NormalizeText(value, ignoreCase: true);
        return number.ToString(scale == 0 ? "0" : $"0.{new string('#', scale)}", CultureInfo.InvariantCulture);
    }

    private static string NormalizeDate(string value)
    {
        // THREED appointment fields are New Zealand wall-clock values. An offset was
        // added to the wire format in 2026; that representation change must not look
        // like the appointment itself moved by twelve hours.
        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeLocal, out var date))
            return date.DateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        return NormalizeText(value, ignoreCase: true);
    }

    private static string ReadString(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) ? (value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : value.ToString()) : "";

    private static int ReadInt(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.TryGetInt32(out var number) ? number : 0;
    private static string Display(string raw) { if (string.IsNullOrWhiteSpace(raw)) return ""; try { using var doc = JsonDocument.Parse(raw); return doc.RootElement.ValueKind == JsonValueKind.String ? doc.RootElement.GetString() ?? "" : doc.RootElement.ToString(); } catch { return raw; } }
    private static string Category(string field) => field switch
    {
        "address" => "address", "primaryService" or "additionalService1" or "additionalService2" => "services",
        "invoiceTotal" or "invoiceLines" => "price", "jobDate" or "durationMinutes" => "schedule",
        "inspectorId" or "inspectorName" => "inspector",
        "clientFirstName" or "clientLastName" or "clientEmail" or "clientPhone" => "customer", "agentName" or "agentDisplayName" or "agentSalutation" or "agentEmail" or "agentPhone" or "notes" or "directions" or "instructions" or "accessBy" => "operational",
        _ => "scope"
    };
}
