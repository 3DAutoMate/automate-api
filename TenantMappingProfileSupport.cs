using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

/// <summary>
/// Tenant-owned current mapping. Internal revision numbers provide optimistic concurrency,
/// while only the Current complete mapping payload is retained.
/// This remains independent from the legacy inspector_field_mappings tables while consumers
/// migrate to canonical fields.
/// </summary>
public static class TenantMappingProfileSupport
{
    public const int ContractVersion = 1;

    public static readonly IReadOnlyDictionary<string, CanonicalMappingField> CanonicalFields =
        new Dictionary<string, CanonicalMappingField>(StringComparer.OrdinalIgnoreCase)
        {
            ["service.primary"] = new("service.primary", "service", "text", true, true),
            ["service.additional_1"] = new("service.additional_1", "service", "text", false, true),
            ["service.additional_2"] = new("service.additional_2", "service", "text", false, true),
            ["scope.weathertightness"] = new("scope.weathertightness", "scope", "boolean", false, true),
            ["scope.garage_outbuilding"] = new("scope.garage_outbuilding", "scope", "boolean", false, true),
            ["scope.attached_flat"] = new("scope.attached_flat", "scope", "boolean", false, true),
            ["scope.property_file_review"] = new("scope.property_file_review", "scope", "boolean", false, true),
            ["scope.monolithic"] = new("scope.monolithic", "scope", "boolean", false, true),
            ["scope.occupied"] = new("scope.occupied", "scope", "boolean", false, true),
            ["scope.travel_fee"] = new("scope.travel_fee", "scope", "boolean", false, true),
            ["scope.healthy_homes_bedrooms"] = new("scope.healthy_homes_bedrooms", "scope", "integer", false, true),
            ["scope.meth_samples"] = new("scope.meth_samples", "scope", "integer", false, true),
            ["scope.healthy_homes_reinspection"] = new("scope.healthy_homes_reinspection", "scope", "boolean", false, true),
            ["scope.foundation_space"] = new("scope.foundation_space", "scope", "boolean", false, true),
            ["scope.healthy_homes_reinspection_date"] = new("scope.healthy_homes_reinspection_date", "scope", "date", false, true),
            ["workflow.report_status"] = new("workflow.report_status", "workflow", "text", false, false),
            ["property.year_built"] = new("property.year_built", "property", "text", false, false),
            ["property.floor_area"] = new("property.floor_area", "property", "decimal", false, false),
            ["property.bedrooms"] = new("property.bedrooms", "property", "integer", false, false),
            ["property.bathrooms"] = new("property.bathrooms", "property", "integer", false, false),
            ["property.levels"] = new("property.levels", "property", "integer", false, false),
            ["property.wall_roof"] = new("property.wall_roof", "property", "text", false, false),
            ["contact.1"] = new("contact.1", "contact", "contact", true, false),
            ["contact.2"] = new("contact.2", "contact", "contact", false, false),
            ["invoice.lines"] = new("invoice.lines", "invoice", "invoice_lines", true, true),
            ["invoice.total"] = new("invoice.total", "invoice", "decimal", true, true),
            ["invoice.amount_paid"] = new("invoice.amount_paid", "invoice", "decimal", false, true),
            ["invoice.balance_due"] = new("invoice.balance_due", "invoice", "decimal", false, true),
            ["branz.wind_zone_destination"] = new("branz.wind_zone_destination", "write_back", "text", false, false, true),
            ["branz.exposure_zone_destination"] = new("branz.exposure_zone_destination", "write_back", "text", false, false, true)
        };

    private static readonly HashSet<string> AllowedSourceKinds = new(StringComparer.OrdinalIgnoreCase)
    {
        "column", "contact_index", "invoice_lines", "computed", "custom_text"
    };

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.tenant_mapping_profiles
(
    tenant_id uuid PRIMARY KEY,
    current_version integer NOT NULL DEFAULT 0,
    validated_version integer NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.tenant_mapping_profile_versions
(
    tenant_id uuid NOT NULL,
    profile_version integer NOT NULL,
    contract_version integer NOT NULL,
    profile_json jsonb NOT NULL,
    profile_fingerprint text NOT NULL,
    validation_status text NOT NULL,
    validation_json jsonb NOT NULL,
    discovery_fingerprint text NOT NULL DEFAULT '',
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, profile_version),
    CONSTRAINT ck_mapping_profile_status CHECK
        (validation_status IN ('valid','invalid','stale','unvalidated'))
);

CREATE TABLE IF NOT EXISTS public.tenant_mapping_profile_audit
(
    audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    profile_version integer NOT NULL,
    action_key text NOT NULL,
    actor text NOT NULL,
    profile_fingerprint text NOT NULL,
    detail_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mapping_profile_audit_tenant_created
ON public.tenant_mapping_profile_audit(tenant_id, created_at DESC);

DELETE FROM public.tenant_mapping_profile_versions v
USING public.tenant_mapping_profiles p
WHERE v.tenant_id=p.tenant_id
  AND v.profile_version<>p.current_version;

DO $$ BEGIN
 IF to_regclass('public.jobs_staging') IS NOT NULL THEN
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_contract_version integer NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_profile_version integer NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_profile_fingerprint text NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_discovery_fingerprint text NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_validation_status text NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_compatibility_mode boolean NOT NULL DEFAULT true;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_values_fingerprint text NOT NULL DEFAULT '';
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_source text NOT NULL DEFAULT 'legacy.payload-v0';
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_workflow_ready boolean NOT NULL DEFAULT true;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS canonical_values_json jsonb NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_review_required boolean NOT NULL DEFAULT false;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_attention_reason text NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS mapping_synced_at timestamptz NULL;

  UPDATE public.jobs_staging j
  SET mapping_contract_version=v.contract_version,
      mapping_profile_version=p.current_version,
      mapping_validation_status='valid',
      mapping_compatibility_mode=false,
      mapping_workflow_ready=true,
      mapping_review_required=false,
      mapping_attention_reason=NULL
  FROM public.tenant_mapping_profiles p
  JOIN public.tenant_mapping_profile_versions v
    ON v.tenant_id=p.tenant_id AND v.profile_version=p.current_version
  WHERE j.tenant_id::text=p.tenant_id::text
    AND v.validation_status='valid'
    AND COALESCE(j.mapping_profile_fingerprint,'')=v.profile_fingerprint
    AND COALESCE(j.mapping_discovery_fingerprint,'')=COALESCE(v.discovery_fingerprint,'')
    AND (COALESCE(j.mapping_profile_version,0)<>p.current_version
      OR NOT COALESCE(j.mapping_workflow_ready,false)
      OR COALESCE(j.mapping_review_required,false));
 END IF;
END $$;
""";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static MappingValidationResult Validate(TenantMappingProfileDraft draft)
    {
        var issues = new List<MappingValidationIssue>();
        if (draft.ContractVersion != ContractVersion)
            issues.Add(new("contract_version", "unsupported_contract", $"Contract version {draft.ContractVersion} is not supported.", "error"));

        var mappings = draft.Mappings ?? [];
        var canonicalDuplicates = mappings.GroupBy(x => Normalize(x.CanonicalField), StringComparer.OrdinalIgnoreCase)
            .Where(x => !string.IsNullOrEmpty(x.Key) && x.Count() > 1);
        foreach (var duplicate in canonicalDuplicates)
            issues.Add(new(duplicate.Key, "duplicate_canonical_field", "The canonical field is mapped more than once.", "error"));

        foreach (var mapping in mappings)
        {
            var canonical = Normalize(mapping.CanonicalField);
            if (!CanonicalFields.TryGetValue(canonical, out var definition))
            {
                issues.Add(new(canonical, "unknown_canonical_field", "The canonical field is not in the versioned allowlist.", "error"));
                continue;
            }
            if (!AllowedSourceKinds.Contains(mapping.SourceKind ?? ""))
                issues.Add(new(canonical, "unsupported_source_kind", $"Source kind '{mapping.SourceKind}' is not supported.", "error"));
            if (string.IsNullOrWhiteSpace(mapping.SourcePath))
                issues.Add(new(canonical, "missing_source", "A source path is required.", "error"));
            if (!string.Equals(definition.ValueType, mapping.ValueType, StringComparison.OrdinalIgnoreCase))
                issues.Add(new(canonical, "incompatible_type", $"Expected {definition.ValueType}, received {mapping.ValueType}.", "error"));
            if (string.Equals(mapping.SourceKind, "custom_text", StringComparison.OrdinalIgnoreCase) && !IsCustomText(mapping.SourcePath))
                issues.Add(new(canonical, "invalid_custom_text", "CustomText sources must be CustomText1 through CustomText28.", "error"));
            if (definition.WriteBack && !mapping.WriteBack)
                issues.Add(new(canonical, "write_back_required", "This destination mapping must be marked write-back.", "error"));
            if (!definition.WriteBack && mapping.WriteBack)
                issues.Add(new(canonical, "unexpected_write_back", "This canonical field cannot write to THREED.", "error"));
        }

        foreach (var required in CanonicalFields.Values.Where(x => x.Required))
            if (!mappings.Any(x => string.Equals(x.CanonicalField, required.Key, StringComparison.OrdinalIgnoreCase)))
                issues.Add(new(required.Key, "missing_required", "A required canonical field is not mapped.", "error"));

        var destinationCollisions = mappings.Where(x => x.WriteBack && !string.IsNullOrWhiteSpace(x.SourcePath))
            .GroupBy(x => NormalizeDestination(x.SourceTable, x.SourcePath), StringComparer.OrdinalIgnoreCase)
            .Where(x => x.Count() > 1);
        foreach (var collision in destinationCollisions)
            issues.Add(new(collision.Key, "destination_collision", "Multiple write-back fields use the same THREED destination.", "error"));

        if (!string.IsNullOrWhiteSpace(draft.ValidatedDiscoveryFingerprint) &&
            !string.IsNullOrWhiteSpace(draft.CurrentDiscoveryFingerprint) &&
            !CryptographicOperations.FixedTimeEquals(
                Encoding.UTF8.GetBytes(draft.ValidatedDiscoveryFingerprint),
                Encoding.UTF8.GetBytes(draft.CurrentDiscoveryFingerprint)))
            issues.Add(new("profile", "stale_discovery", "THREED discovery changed after this mapping was validated.", "error"));

        var status = issues.Any(x => x.Severity == "error")
            ? (issues.Any(x => x.Code == "stale_discovery") ? "stale" : "invalid")
            : "valid";
        return new(status, issues, status == "valid");
    }

    public static async Task<TenantMappingProfileVersion?> LoadCurrentAsync(
        NpgsqlConnection connection, Guid tenantId, CancellationToken cancellationToken = default)
    {
        const string sql = """
SELECT v.profile_version,v.contract_version,v.profile_json::text,v.profile_fingerprint,
       v.validation_status,v.validation_json::text,v.discovery_fingerprint,v.created_by,v.created_at
FROM public.tenant_mapping_profiles p
JOIN public.tenant_mapping_profile_versions v
  ON v.tenant_id=p.tenant_id AND v.profile_version=p.current_version
WHERE p.tenant_id=@tenant;
""";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadVersion(tenantId, reader) : null;
    }

    public static async Task<MappingSaveResult> SaveCurrentAsync(
        NpgsqlConnection connection, Guid tenantId, int expectedVersion, TenantMappingProfileDraft draft,
        string actor, CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty) throw new ArgumentException("Tenant is required.", nameof(tenantId));
        if (string.IsNullOrWhiteSpace(actor)) throw new ArgumentException("Authenticated actor is required.", nameof(actor));
        var validation = Validate(draft);
        var normalized = draft with { Mappings = draft.Mappings.OrderBy(x => x.CanonicalField, StringComparer.OrdinalIgnoreCase).ToList() };
        var profileJson = JsonSerializer.Serialize(normalized, JsonOptions);
        var validationJson = JsonSerializer.Serialize(validation, JsonOptions);
        var fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(profileJson))).ToLowerInvariant();

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var seed = new NpgsqlCommand("INSERT INTO public.tenant_mapping_profiles(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING", connection, transaction))
        { seed.Parameters.AddWithValue("tenant", tenantId); await seed.ExecuteNonQueryAsync(cancellationToken); }

        int current; string currentFingerprint; string currentStatus;
        await using (var load = new NpgsqlCommand("""
SELECT p.current_version,COALESCE(v.profile_fingerprint,''),COALESCE(v.validation_status,'')
FROM public.tenant_mapping_profiles p
LEFT JOIN public.tenant_mapping_profile_versions v
  ON v.tenant_id=p.tenant_id AND v.profile_version=p.current_version
WHERE p.tenant_id=@tenant
FOR UPDATE OF p;
""", connection, transaction))
        {
            load.Parameters.AddWithValue("tenant", tenantId);
            await using var reader = await load.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            current = reader.GetInt32(0); currentFingerprint = reader.GetString(1); currentStatus = reader.GetString(2);
        }
        if (current != expectedVersion)
        { await transaction.RollbackAsync(cancellationToken); return new("conflict", current, null, validation, "Mapping profile changed; reload before saving."); }

        if (current > 0 && !RequiresSnapshot(currentFingerprint, fingerprint, currentStatus, validation.Status))
        {
            await PruneSnapshotsAsync(connection, transaction, tenantId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new("unchanged", current, currentFingerprint, validation, "Current mapping is already saved. No new snapshot was created.");
        }

        var next = current + 1;
        const string insertSql = """
INSERT INTO public.tenant_mapping_profile_versions
(tenant_id,profile_version,contract_version,profile_json,profile_fingerprint,validation_status,validation_json,discovery_fingerprint,created_by)
VALUES(@tenant,@version,@contract,CAST(@profile AS jsonb),@fingerprint,@status,CAST(@validation AS jsonb),@discovery,@actor);
UPDATE public.tenant_mapping_profiles SET current_version=@version,
validated_version=CASE WHEN @status='valid' THEN @version ELSE validated_version END,updated_at=NOW() WHERE tenant_id=@tenant;
INSERT INTO public.tenant_mapping_profile_audit
(tenant_id,profile_version,action_key,actor,profile_fingerprint,detail_json)
VALUES(@tenant,@version,'mapping_profile.current_replaced',@actor,@fingerprint,CAST(@validation AS jsonb));
""";
        await using var insert = new NpgsqlCommand(insertSql, connection, transaction);
        insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("version", next);
        insert.Parameters.AddWithValue("contract", draft.ContractVersion); insert.Parameters.AddWithValue("profile", profileJson);
        insert.Parameters.AddWithValue("fingerprint", fingerprint); insert.Parameters.AddWithValue("status", validation.Status);
        insert.Parameters.AddWithValue("validation", validationJson); insert.Parameters.AddWithValue("discovery", draft.CurrentDiscoveryFingerprint ?? "");
        insert.Parameters.AddWithValue("actor", actor.Trim());
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await using (var exists = new NpgsqlCommand("SELECT to_regclass('public.jobs_staging') IS NOT NULL", connection, transaction))
        if (Convert.ToBoolean(await exists.ExecuteScalarAsync(cancellationToken)))
        {
            const string invalidateSql = """
UPDATE public.jobs_staging
SET mapping_workflow_ready=false,
    mapping_review_required=true,
    mapping_validation_status=CASE WHEN @status='valid' THEN 'mapping_profile_mismatch' ELSE 'mapping_profile_invalid' END,
    mapping_attention_reason=CASE WHEN @status='valid'
      THEN 'MAPPING CHANGED - re-sync and review required'
      ELSE 'THREED MAPPING INVALID - workflow blocked' END
WHERE tenant_id::text=@tenant_text
  AND (@status<>'valid' OR COALESCE(mapping_profile_fingerprint,'')<>@fingerprint);
""";
            await using var invalidate = new NpgsqlCommand(invalidateSql, connection, transaction);
            invalidate.Parameters.AddWithValue("status", validation.Status);
            invalidate.Parameters.AddWithValue("tenant_text", tenantId.ToString());
            invalidate.Parameters.AddWithValue("version", next);
            invalidate.Parameters.AddWithValue("fingerprint", fingerprint);
            await invalidate.ExecuteNonQueryAsync(cancellationToken);
        }
        await PruneSnapshotsAsync(connection, transaction, tenantId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("saved", next, fingerprint, validation, validation.IsValid ? "Current mapping validated and saved." : "Current mapping saved but cannot be used by workflows.");
    }

    public static bool RequiresSnapshot(string? currentFingerprint, string proposedFingerprint, string? currentStatus, string proposedStatus) =>
        !string.Equals(currentFingerprint, proposedFingerprint, StringComparison.OrdinalIgnoreCase) ||
        !string.Equals(currentStatus, proposedStatus, StringComparison.OrdinalIgnoreCase);

    public static IReadOnlyList<int> RetainedSnapshotVersions(IEnumerable<int> versions) =>
        versions.Where(x => x > 0).Distinct().OrderByDescending(x => x).Take(1).ToArray();

    private static async Task PruneSnapshotsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid tenantId, CancellationToken cancellationToken)
    {
        await using var prune = new NpgsqlCommand("""
DELETE FROM public.tenant_mapping_profile_versions
WHERE tenant_id=@tenant AND profile_version<>(
  SELECT current_version FROM public.tenant_mapping_profiles WHERE tenant_id=@tenant
);
""", connection, transaction);
        prune.Parameters.AddWithValue("tenant", tenantId);
        await prune.ExecuteNonQueryAsync(cancellationToken);
    }

    public static TenantMappingProfileDraft CreateProSpectCompatibilityDraft(string discoveryFingerprint = "") => new(
        ContractVersion,
        discoveryFingerprint,
        discoveryFingerprint,
        [
            new("service.primary", "custom_text", "CustomText1", "text", false, "dbo.tblJob", "Primary service"),
            new("service.additional_1", "custom_text", "CustomText2", "text", false, "dbo.tblJob", "Additional service 1"),
            new("service.additional_2", "custom_text", "CustomText3", "text", false, "dbo.tblJob", "Additional service 2"),
            new("scope.monolithic", "custom_text", "CustomText9", "boolean", false, "dbo.tblJob", "Monolithic or plaster cladding"),
            new("scope.garage_outbuilding", "custom_text", "CustomText10", "boolean", false, "dbo.tblJob", "Separate outbuildings"),
            new("scope.occupied", "custom_text", "CustomText11", "boolean", false, "dbo.tblJob", "Occupied"),
            new("scope.attached_flat", "custom_text", "CustomText12", "boolean", false, "dbo.tblJob", "Attached flat"),
            new("scope.travel_fee", "custom_text", "CustomText13", "boolean", false, "dbo.tblJob", "Travel fee"),
            new("scope.healthy_homes_bedrooms", "custom_text", "CustomText14", "integer", false, "dbo.tblJob", "Healthy Homes bedrooms"),
            new("scope.meth_samples", "custom_text", "CustomText15", "integer", false, "dbo.tblJob", "Meth samples"),
            new("scope.healthy_homes_reinspection", "custom_text", "CustomText16", "boolean", false, "dbo.tblJob", "Healthy Homes reinspection"),
            new("scope.property_file_review", "custom_text", "CustomText17", "boolean", false, "dbo.tblJob", "Property file review"),
            new("scope.foundation_space", "custom_text", "CustomText18", "boolean", false, "dbo.tblJob", "Foundation space"),
            new("scope.healthy_homes_reinspection_date", "custom_text", "CustomText19", "date", false, "dbo.tblJob", "Healthy Homes reinspection date"),
            new("workflow.report_status", "custom_text", "CustomText28", "text", false, "dbo.tblJob", "Report status"),
            new("property.year_built", "custom_text", "CustomText4", "text", false, "dbo.tblJob", "Year built"),
            new("property.bedrooms", "custom_text", "CustomText7", "integer", false, "dbo.tblJob", "Bedrooms"),
            new("property.bathrooms", "custom_text", "CustomText8", "integer", false, "dbo.tblJob", "Bathrooms"),
            new("property.levels", "custom_text", "CustomText6", "integer", false, "dbo.tblJob", "Levels"),
            new("property.wall_roof", "custom_text", "CustomText5", "text", false, "dbo.tblJob", "Building type"),
            new("contact.1", "contact_index", "dbo.tblJobContacts.ContactIndex:0", "contact", false, "dbo.tblJobContacts", "Client"),
            new("contact.2", "contact_index", "dbo.tblJobContacts.ContactIndex:1", "contact", false, "dbo.tblJobContacts", "Buyers Agent"),
            new("invoice.lines", "invoice_lines", "dbo.tblJobItem", "invoice_lines", false, "dbo.tblJobItem", "Invoice lines"),
            new("invoice.total", "computed", "invoice.lines.sum", "decimal", false, "", "Invoice total"),
            new("invoice.amount_paid", "column", "dbo.tblJob.AmountPaid", "decimal", false, "dbo.tblJob", "Amount paid"),
            new("invoice.balance_due", "computed", "invoice.total-minus-paid", "decimal", false, "", "Balance due")
        ]);

    private static TenantMappingProfileVersion ReadVersion(Guid tenantId, NpgsqlDataReader reader) => new(
        tenantId, reader.GetInt32(0), reader.GetInt32(1),
        JsonSerializer.Deserialize<TenantMappingProfileDraft>(reader.GetString(2), JsonOptions)!, reader.GetString(3),
        reader.GetString(4), JsonSerializer.Deserialize<MappingValidationResult>(reader.GetString(5), JsonOptions)!,
        reader.GetString(6), reader.GetString(7), reader.GetFieldValue<DateTimeOffset>(8));

    private static bool IsCustomText(string? value)
    {
        var name = (value ?? "").Split('.').Last();
        return name.StartsWith("CustomText", StringComparison.OrdinalIgnoreCase) &&
               int.TryParse(name[10..], out var index) && index is >= 1 and <= 28;
    }

    private static string Normalize(string? value) => (value ?? "").Trim().ToLowerInvariant();
    private static string NormalizeDestination(string? table, string? path) => $"{Normalize(table)}:{Normalize(path)}";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
}

public sealed record CanonicalMappingField(string Key, string Category, string ValueType, bool Required, bool WorkflowCritical, bool WriteBack = false);
public sealed record TenantFieldMapping(string CanonicalField, string SourceKind, string SourcePath, string ValueType, bool WriteBack, string SourceTable = "", string Label = "");
public sealed record TenantMappingProfileDraft(int ContractVersion, string ValidatedDiscoveryFingerprint, string CurrentDiscoveryFingerprint, List<TenantFieldMapping> Mappings);
public sealed record MappingValidationIssue(string Field, string Code, string Message, string Severity);
public sealed record MappingValidationResult(string Status, List<MappingValidationIssue> Issues, bool IsValid);
public sealed record TenantMappingProfileVersion(Guid TenantId, int ProfileVersion, int ContractVersion, TenantMappingProfileDraft Profile, string ProfileFingerprint, string ValidationStatus, MappingValidationResult Validation, string DiscoveryFingerprint, string CreatedBy, DateTimeOffset CreatedAt);
public sealed record MappingSaveResult(string Status, int CurrentVersion, string? ProfileFingerprint, MappingValidationResult Validation, string Message);
