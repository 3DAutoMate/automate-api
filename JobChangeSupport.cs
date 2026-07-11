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
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_review_pending boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS pending_change_json jsonb NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS pending_change_fingerprint text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS pending_change_reasons text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_detected_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_confirmed_at timestamptz NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS change_confirmed_by text NULL;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS xero_review_required boolean NOT NULL DEFAULT false;
ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS report_review_required boolean NOT NULL DEFAULT false;
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
approved_snapshot_version=1,current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint WHERE job_id=@job AND approved_snapshot_json IS NULL", conn);
            update.Parameters.AddWithValue("job", row.JobId); update.Parameters.AddWithValue("snapshot", snapshot); update.Parameters.AddWithValue("fingerprint", fingerprint); await update.ExecuteNonQueryAsync();
        }
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
pending_change_reasons=NULL,pending_change_fingerprint=NULL WHERE job_id=@job AND change_review_pending=true", conn);
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
            ["address"] = T(payload.Job?.SiteAddress), ["jobDate"] = T(payload.Job?.JobDate),
            ["durationMinutes"] = payload.Job?.InspectionDurationMinutes ?? 0, ["invoiceTotal"] = Money(payload.Job?.InvoiceTotal),
            ["notes"] = T(payload.Job?.Notes), ["directions"] = T(payload.Job?.Directions), ["instructions"] = T(payload.Job?.Instructions),
            ["primaryService"] = T(payload.Services?.Primary), ["additionalService1"] = T(payload.Services?.Additional1), ["additionalService2"] = T(payload.Services?.Additional2),
            ["buildingType"] = T(payload.JobDetails?.BuildingType), ["stories"] = T(payload.JobDetails?.Stories),
            ["outbuilding"] = T(payload.JobDetails?.Outbuilding), ["occupied"] = T(payload.JobDetails?.Occupied),
            ["attachedFlat"] = T(payload.JobDetails?.AttachedFlat), ["travelFee"] = T(payload.JobDetails?.TravelFee),
            ["hhsBedrooms"] = T(payload.JobDetails?.HhsBedrooms), ["methSamples"] = T(payload.JobDetails?.MethSamples),
            ["hhsReinspect"] = T(payload.JobDetails?.HhsReinspect), ["councilFiles"] = T(payload.JobDetails?.CouncilFiles),
            ["foundationSpace"] = T(payload.JobDetails?.FoundationSpace), ["weathertightness"] = T(payload.JobDetails?.Weathertightness),
            ["hhsReinspectDate"] = T(payload.JobDetails?.HhsReinspectDate), ["accessBy"] = T(payload.JobDetails?.AccessBy),
            ["hhsCompliance"] = T(payload.JobDetails?.HhsCompliance),
            ["clientName"] = T(string.Join(" ", new[] { payload.Contact1?.FirstName, payload.Contact1?.LastName }.Where(v => !string.IsNullOrWhiteSpace(v)))),
            ["clientEmail"] = T(payload.Contact1?.Email).ToLowerInvariant(), ["clientPhone"] = T(payload.Contact1?.Cellular),
            ["agentName"] = T(string.Join(" ", new[] { payload.Contact2?.FirstName, payload.Contact2?.LastName }.Where(v => !string.IsNullOrWhiteSpace(v)))),
            ["agentEmail"] = T(payload.Contact2?.Email).ToLowerInvariant(), ["agentPhone"] = T(payload.Contact2?.Cellular),
            ["invoiceLines"] = (payload.InvoiceLines ?? new()).OrderBy(line => line.LineIndex).Select(line => new
            {
                index = line.LineIndex, description = T(line.Description), quantity = line.Quantity.ToString("0.####", CultureInfo.InvariantCulture),
                unitPrice = line.UnitPrice.ToString("0.00", CultureInfo.InvariantCulture)
            }).ToArray()
        };
        return JsonSerializer.Serialize(fields);
    }

    public static string Fingerprint(string json) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json))).ToLowerInvariant();

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
        string sql = change.Pending
            ? @"UPDATE public.jobs_staging SET current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint,
change_review_pending=true,pending_change_json=CAST(@changes AS jsonb),pending_change_fingerprint=@fingerprint,pending_change_reasons=@reasons,
change_detected_at=NOW(),xero_review_required=(xero_review_required OR @xero),report_review_required=(report_review_required OR @report) WHERE job_id=@job_id"
            : @"UPDATE public.jobs_staging SET approved_snapshot_json=CAST(@snapshot AS jsonb),approved_snapshot_fingerprint=@fingerprint,
approved_snapshot_version=@version,current_snapshot_json=CAST(@snapshot AS jsonb),current_snapshot_fingerprint=@fingerprint,
change_review_pending=false,pending_change_json=NULL,pending_change_fingerprint=NULL,pending_change_reasons=NULL,source_missing=false,source_missing_at=NULL WHERE job_id=@job_id";
        await using var cmd = new NpgsqlCommand(sql, conn); cmd.Parameters.AddWithValue("job_id", jobId); cmd.Parameters.AddWithValue("snapshot", change.SnapshotJson);
        cmd.Parameters.AddWithValue("fingerprint", change.Fingerprint); cmd.Parameters.AddWithValue("changes", change.ChangesJson); cmd.Parameters.AddWithValue("reasons", change.Reasons);
        cmd.Parameters.AddWithValue("xero", change.XeroReview); cmd.Parameters.AddWithValue("report", change.ReportReview); cmd.Parameters.AddWithValue("version", change.ApprovedVersion);
        await cmd.ExecuteNonQueryAsync();
        if (change.Pending) await AuditAsync(conn, jobId, tenantId, change.ApprovedVersion, "detected", change.Fingerprint, change.ChangesJson, change.Reasons, "3D sync");
    }

    public static async Task AuditAsync(NpgsqlConnection conn, Guid jobId, Guid tenantId, int version, string eventType, string fingerprint, string changes, string reasons, string actor)
    {
        await using var cmd = new NpgsqlCommand(@"INSERT INTO public.job_change_audit(job_id,tenant_id,approved_version,event_type,snapshot_fingerprint,changes_json,reasons,actor)
VALUES(@job,@tenant,@version,@event,@fingerprint,CAST(@changes AS jsonb),@reasons,@actor)", conn);
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
            if (string.Equals(before, after, StringComparison.Ordinal)) continue;
            result.Add(new JobFieldChange { field = property.Name, oldValue = Display(before), newValue = Display(after), category = Category(property.Name) });
        }
        return result;
    }
    private static string Display(string raw) { if (string.IsNullOrWhiteSpace(raw)) return ""; try { using var doc = JsonDocument.Parse(raw); return doc.RootElement.ValueKind == JsonValueKind.String ? doc.RootElement.GetString() ?? "" : doc.RootElement.ToString(); } catch { return raw; } }
    private static string Category(string field) => field switch
    {
        "address" => "address", "primaryService" or "additionalService1" or "additionalService2" => "services",
        "invoiceTotal" or "invoiceLines" => "price", "jobDate" or "durationMinutes" => "schedule",
        "clientName" or "clientEmail" or "clientPhone" => "customer", "agentName" or "agentEmail" or "agentPhone" or "notes" or "directions" or "instructions" or "accessBy" => "operational",
        _ => "scope"
    };
}
