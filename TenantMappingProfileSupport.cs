using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

/// <summary>
/// Tenant-owned, immutable mapping profile versions. This is deliberately independent from the
/// legacy inspector_field_mappings tables so existing connector releases remain compatible while
/// consumers migrate to canonical fields.
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

    public static async Task<MappingSaveResult> SaveVersionAsync(
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

        int current;
        await using (var load = new NpgsqlCommand("SELECT current_version FROM public.tenant_mapping_profiles WHERE tenant_id=@tenant FOR UPDATE", connection, transaction))
        { load.Parameters.AddWithValue("tenant", tenantId); current = Convert.ToInt32(await load.ExecuteScalarAsync(cancellationToken)); }
        if (current != expectedVersion)
        { await transaction.RollbackAsync(cancellationToken); return new("conflict", current, null, validation, "Mapping profile changed; reload before saving."); }

        var next = current + 1;
        const string insertSql = """
INSERT INTO public.tenant_mapping_profile_versions
(tenant_id,profile_version,contract_version,profile_json,profile_fingerprint,validation_status,validation_json,discovery_fingerprint,created_by)
VALUES(@tenant,@version,@contract,CAST(@profile AS jsonb),@fingerprint,@status,CAST(@validation AS jsonb),@discovery,@actor);
UPDATE public.tenant_mapping_profiles SET current_version=@version,
validated_version=CASE WHEN @status='valid' THEN @version ELSE validated_version END,updated_at=NOW() WHERE tenant_id=@tenant;
INSERT INTO public.tenant_mapping_profile_audit
(tenant_id,profile_version,action_key,actor,profile_fingerprint,detail_json)
VALUES(@tenant,@version,'mapping_profile.version_created',@actor,@fingerprint,CAST(@validation AS jsonb));
""";
        await using var insert = new NpgsqlCommand(insertSql, connection, transaction);
        insert.Parameters.AddWithValue("tenant", tenantId); insert.Parameters.AddWithValue("version", next);
        insert.Parameters.AddWithValue("contract", draft.ContractVersion); insert.Parameters.AddWithValue("profile", profileJson);
        insert.Parameters.AddWithValue("fingerprint", fingerprint); insert.Parameters.AddWithValue("status", validation.Status);
        insert.Parameters.AddWithValue("validation", validationJson); insert.Parameters.AddWithValue("discovery", draft.CurrentDiscoveryFingerprint ?? "");
        insert.Parameters.AddWithValue("actor", actor.Trim());
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new("saved", next, fingerprint, validation, validation.IsValid ? "Mapping profile validated and saved." : "Mapping profile saved but cannot be used by workflows.");
    }

    public static TenantMappingProfileDraft CreateProSpectCompatibilityDraft(string discoveryFingerprint = "") => new(
        ContractVersion,
        discoveryFingerprint,
        discoveryFingerprint,
        [
            new("service.primary", "custom_text", "CustomText1", "text", false, "dbo.tblJob", "Primary service"),
            new("service.additional_1", "custom_text", "CustomText2", "text", false, "dbo.tblJob", "Additional service 1"),
            new("service.additional_2", "custom_text", "CustomText3", "text", false, "dbo.tblJob", "Additional service 2"),
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
