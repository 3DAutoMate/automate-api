using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

/// <summary>
/// Canonical boundary for THREED sync consumers.  New consumers should read only through
/// <see cref="CanonicalSyncAccessors"/> after <see cref="CanonicalSyncGate.Evaluate"/> succeeds.
/// The legacy adapter is intentionally explicit so migration use remains visible and removable.
/// </summary>
public static class CanonicalSyncEnvelopeSupport
{
    public const int ContractVersion = TenantMappingProfileSupport.ContractVersion;
    public const string EnvelopeProperty = "canonicalMapping";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static CanonicalSyncParseResult Parse(string rawPayloadJson, JobUploadRequest legacyPayload)
    {
        if (legacyPayload == null) throw new ArgumentNullException(nameof(legacyPayload));
        if (string.IsNullOrWhiteSpace(rawPayloadJson))
            return CanonicalSyncParseResult.Legacy(CanonicalSyncLegacyAdapter.From(legacyPayload), "canonical_envelope_missing");

        try
        {
            using var document = JsonDocument.Parse(rawPayloadJson);
            if (!TryProperty(document.RootElement, EnvelopeProperty, out var envelope) &&
                !TryProperty(document.RootElement, "canonical_mapping", out envelope))
                return CanonicalSyncParseResult.Legacy(CanonicalSyncLegacyAdapter.From(legacyPayload), "canonical_envelope_missing");

            if (envelope.ValueKind != JsonValueKind.Object)
                return CanonicalSyncParseResult.Invalid("canonical_envelope_invalid", "The canonical mapping envelope must be an object.");

            var contractVersion = Int(envelope, "contractVersion", "contract_version");
            var profileVersion = Int(envelope, "mappingVersion", "mapping_version", "profileVersion", "profile_version");
            var profileFingerprint = Text(envelope, "mappingFingerprint", "mapping_fingerprint", "profileFingerprint", "profile_fingerprint");
            var mappingSource = Text(envelope, "mappingSource", "mapping_source");
            var discoveryFingerprint = Text(envelope, "discoveryFingerprint", "discovery_fingerprint");
            var declaredStatus = Text(envelope, "validationStatus", "validation_status");
            var declaredValuesFingerprint = Text(envelope, "valuesFingerprint", "values_fingerprint");
            var declaredValid = Boolean(envelope, "isValid", "is_valid") ??
                                string.Equals(declaredStatus, "valid", StringComparison.OrdinalIgnoreCase);
            var missingRequired = Strings(envelope, "missingRequiredFields", "missing_required_fields");
            if (!TryProperty(envelope, "values", out var values) && !TryProperty(envelope, "canonicalValues", out values) &&
                !TryProperty(envelope, "canonical_values", out values))
                return CanonicalSyncParseResult.Invalid("canonical_values_missing", "The canonical envelope has no values object.");
            if (values.ValueKind != JsonValueKind.Object)
                return CanonicalSyncParseResult.Invalid("canonical_values_invalid", "Canonical values must be an object.");

            var normalizedValues = Canonicalize(values);
            var computedValuesFingerprint = Sha256(normalizedValues);
            var compatibility = string.Equals(mappingSource, "connector.field-mapping-v1", StringComparison.OrdinalIgnoreCase);
            var parsed = new CanonicalSyncEnvelope(contractVersion, profileVersion, profileFingerprint, mappingSource,
                discoveryFingerprint, declaredStatus, declaredValid, missingRequired, declaredValuesFingerprint,
                computedValuesFingerprint, values.Clone(), compatibility);
            return CanonicalSyncParseResult.Parsed(parsed);
        }
        catch (JsonException error)
        {
            return CanonicalSyncParseResult.Invalid("canonical_envelope_json_invalid", error.Message);
        }
    }

    public static CanonicalSyncEnvelope CreateForTransport(
        int profileVersion, string profileFingerprint, string discoveryFingerprint,
        IReadOnlyDictionary<string, object?> values)
    {
        if (profileVersion < 1) throw new ArgumentOutOfRangeException(nameof(profileVersion));
        if (!CanonicalSyncGate.IsSha256(profileFingerprint)) throw new ArgumentException("A SHA-256 profile fingerprint is required.", nameof(profileFingerprint));
        var element = JsonSerializer.SerializeToElement(values, JsonOptions);
        var fingerprint = Sha256(Canonicalize(element));
        return new(ContractVersion, profileVersion, profileFingerprint.ToLowerInvariant(), "railway.tenant-profile",
            discoveryFingerprint ?? "", "valid", true, [], fingerprint, fingerprint, element, false);
    }

    internal static string Canonicalize(JsonElement value)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream)) WriteCanonical(writer, value);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in value.EnumerateObject().OrderBy(x => x.Name, StringComparer.Ordinal))
                { writer.WritePropertyName(property.Name); WriteCanonical(writer, property.Value); }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray(); foreach (var item in value.EnumerateArray()) WriteCanonical(writer, item); writer.WriteEndArray();
                break;
            default: value.WriteTo(writer); break;
        }
    }

    private static string Sha256(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static bool TryProperty(JsonElement element, string name, out JsonElement value)
    {
        if (element.TryGetProperty(name, out value)) return true;
        foreach (var property in element.EnumerateObject())
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase)) { value = property.Value; return true; }
        value = default; return false;
    }
    private static string Text(JsonElement element, params string[] names)
    {
        foreach (var name in names) if (TryProperty(element, name, out var value))
            return value.ValueKind == JsonValueKind.String ? value.GetString()?.Trim() ?? "" : value.ToString().Trim();
        return "";
    }
    private static int Int(JsonElement element, params string[] names)
    {
        foreach (var name in names) if (TryProperty(element, name, out var value))
        { if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var parsed)) return parsed; if (int.TryParse(value.ToString(), out parsed)) return parsed; }
        return 0;
    }
    private static bool? Boolean(JsonElement element, params string[] names)
    {
        foreach (var name in names) if (TryProperty(element, name, out var value))
        {
            if (value.ValueKind == JsonValueKind.True) return true; if (value.ValueKind == JsonValueKind.False) return false;
            if (bool.TryParse(value.ToString(), out var parsed)) return parsed;
        }
        return null;
    }
    private static IReadOnlyList<string> Strings(JsonElement element, params string[] names)
    {
        foreach (var name in names) if (TryProperty(element, name, out var value) && value.ValueKind == JsonValueKind.Array)
            return value.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()?.Trim() ?? "").Where(x => x.Length > 0).ToArray();
        return [];
    }
}

public static class CanonicalSyncGate
{
    public static CanonicalSyncGateResult Evaluate(
        CanonicalSyncParseResult parse, TenantMappingProfileVersion? activeProfile,
        CanonicalSyncGateOptions? options = null)
    {
        options ??= new();
        if (!parse.Success || parse.Envelope == null)
            return CanonicalSyncGateResult.Block(parse.Code, parse.Message);

        var envelope = parse.Envelope;
        if (envelope.LegacyCompatibility && string.Equals(envelope.MappingSource, "legacy.payload-v0", StringComparison.OrdinalIgnoreCase))
        {
            if (activeProfile != null)
                return CanonicalSyncGateResult.Block("mapping_profile_mismatch", "A tenant mapping profile is active; re-sync with that profile before workflow approval.");
            if (!options.AllowLegacyCompatibility)
                return CanonicalSyncGateResult.Block("canonical_envelope_required", "This operation requires a versioned canonical sync envelope.");
            if (options.RequireActiveProfile)
                return CanonicalSyncGateResult.Block("mapping_profile_required", "A validated tenant mapping profile is required.");
            return CanonicalSyncGateResult.Allow("legacy_compatibility", true,
                "Legacy payload accepted through the explicit Pro-Spect compatibility adapter.");
        }

        if (envelope.ContractVersion != CanonicalSyncEnvelopeSupport.ContractVersion)
            return CanonicalSyncGateResult.Block("mapping_contract_unsupported", $"Canonical contract {envelope.ContractVersion} is not supported.");
        if (envelope.ProfileVersion < 1 || !IsSha256(envelope.ProfileFingerprint))
            return CanonicalSyncGateResult.Block("mapping_identity_invalid", "A valid mapping profile version and fingerprint are required.");
        if (!envelope.DeclaredValid || envelope.MissingRequiredFields.Count > 0 ||
            (!string.IsNullOrWhiteSpace(envelope.DeclaredValidationStatus) && !string.Equals(envelope.DeclaredValidationStatus, "valid", StringComparison.OrdinalIgnoreCase)))
            return CanonicalSyncGateResult.Block("mapping_not_validated", "The connector did not use a validated mapping profile.");
        if (!string.IsNullOrWhiteSpace(envelope.DeclaredValuesFingerprint) &&
            (!IsSha256(envelope.DeclaredValuesFingerprint) || !FixedEquals(envelope.DeclaredValuesFingerprint, envelope.ComputedValuesFingerprint)))
            return CanonicalSyncGateResult.Block("canonical_values_tampered", "The canonical value fingerprint does not match the payload.");
        var shapeFailure = ValidateCanonicalShape(envelope.Values);
        if (shapeFailure != null) return shapeFailure;

        if (envelope.LegacyCompatibility)
        {
            if (activeProfile != null)
                return CanonicalSyncGateResult.Block("mapping_profile_mismatch", "A tenant mapping profile is active; the connector compatibility mapping can no longer authorize workflows.");
            if (!options.AllowLegacyCompatibility)
                return CanonicalSyncGateResult.Block("canonical_envelope_required", "This operation requires the authoritative tenant mapping profile.");
            if (options.RequireActiveProfile)
                return CanonicalSyncGateResult.Block("mapping_profile_required", "The compatibility mapping cannot satisfy an authoritative-profile operation.");
            return CanonicalSyncGateResult.Allow("connector_mapping_compatibility", true,
                "Connector field mapping accepted in explicit compatibility mode.");
        }

        if (activeProfile == null)
            return options.RequireActiveProfile
                ? CanonicalSyncGateResult.Block("mapping_profile_required", "No active tenant mapping profile exists.")
                : CanonicalSyncGateResult.Allow("embedded_profile", false, "Canonical envelope is internally valid; no active profile comparison was required.");

        if (!string.Equals(activeProfile.ValidationStatus, "valid", StringComparison.OrdinalIgnoreCase) ||
            activeProfile.Validation?.IsValid != true)
            return CanonicalSyncGateResult.Block("mapping_profile_invalid", "The active tenant mapping profile is not valid.");
        if (activeProfile.ContractVersion != envelope.ContractVersion)
            return CanonicalSyncGateResult.Block("mapping_contract_mismatch", "The sync and active profile use different mapping contracts.");
        if (activeProfile.ProfileVersion != envelope.ProfileVersion || !FixedEquals(activeProfile.ProfileFingerprint, envelope.ProfileFingerprint))
            return CanonicalSyncGateResult.Block("mapping_profile_mismatch", "The job was mapped with a different profile version. Re-sync it before workflow approval.");
        if (!string.IsNullOrWhiteSpace(activeProfile.DiscoveryFingerprint) &&
            !string.IsNullOrWhiteSpace(envelope.DiscoveryFingerprint) &&
            !FixedEquals(activeProfile.DiscoveryFingerprint, envelope.DiscoveryFingerprint))
            return CanonicalSyncGateResult.Block("mapping_discovery_stale", "THREED discovery changed after the active mapping was validated.");

        return CanonicalSyncGateResult.Allow("current_profile", false, "Canonical mapping identity is current and valid.");
    }

    public static bool IsSha256(string? value) => value?.Length == 64 && value.All(Uri.IsHexDigit);
    private static CanonicalSyncGateResult? ValidateCanonicalShape(JsonElement values)
    {
        foreach (var required in TenantMappingProfileSupport.CanonicalFields.Values.Where(x => x.Required))
            if (!TryValue(values, required.Key, out var requiredValue) || requiredValue.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                return CanonicalSyncGateResult.Block("canonical_required_value_missing", $"Canonical field '{required.Key}' is required.");

        foreach (var property in values.EnumerateObject())
        {
            if (!TenantMappingProfileSupport.CanonicalFields.TryGetValue(property.Name, out var definition))
                continue; // additive transport metadata remains forward compatible within contract v1
            var valid = definition.ValueType switch
            {
                "contact" => property.Value.ValueKind == JsonValueKind.Object,
                "invoice_lines" => property.Value.ValueKind == JsonValueKind.Array,
                "boolean" => property.Value.ValueKind is JsonValueKind.True or JsonValueKind.False or JsonValueKind.String,
                "integer" or "decimal" => property.Value.ValueKind is JsonValueKind.Number or JsonValueKind.String,
                _ => property.Value.ValueKind is JsonValueKind.String or JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False
            };
            if (!valid) return CanonicalSyncGateResult.Block("canonical_value_type_invalid", $"Canonical field '{property.Name}' is not a valid {definition.ValueType} value.");
        }
        return null;
    }
    private static bool TryValue(JsonElement values, string key, out JsonElement value)
    {
        if (values.TryGetProperty(key, out value)) return true;
        foreach (var property in values.EnumerateObject())
            if (string.Equals(property.Name, key, StringComparison.OrdinalIgnoreCase)) { value = property.Value; return true; }
        value = default; return false;
    }
    private static bool FixedEquals(string? left, string? right)
    {
        var a = Encoding.UTF8.GetBytes((left ?? "").Trim().ToLowerInvariant());
        var b = Encoding.UTF8.GetBytes((right ?? "").Trim().ToLowerInvariant());
        return a.Length == b.Length && CryptographicOperations.FixedTimeEquals(a, b);
    }
}

public sealed class CanonicalSyncAccessors
{
    private readonly JsonElement _values;
    public CanonicalSyncEnvelope Envelope { get; }

    public CanonicalSyncAccessors(CanonicalSyncEnvelope envelope, CanonicalSyncGateResult gate)
    {
        if (!gate.Allowed) throw new InvalidOperationException($"Canonical values are unavailable: {gate.Code}.");
        Envelope = envelope ?? throw new ArgumentNullException(nameof(envelope));
        _values = envelope.Values;
    }

    public bool Has(string key) => Try(key, out var value) && value.ValueKind is not JsonValueKind.Null and not JsonValueKind.Undefined;
    public string Text(string key, string fallback = "") => Try(key, out var value) ? ScalarText(value, fallback) : fallback;
    public bool? Boolean(string key) => Try(key, out var value) ? ParseBoolean(value) : null;
    public decimal? Decimal(string key) => Try(key, out var value) && decimal.TryParse(ScalarText(value, ""), NumberStyles.Any, CultureInfo.InvariantCulture, out var result) ? result : null;
    public int? Integer(string key) => Try(key, out var value) && int.TryParse(ScalarText(value, ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out var result) ? result : null;
    public CanonicalContact? Contact(int index)
    {
        if (index is not (1 or 2) || !Try($"contact.{index}", out var value) || value.ValueKind != JsonValueKind.Object) return null;
        return new(Field(value, "firstName"), Field(value, "lastName"), Field(value, "displayName"), Field(value, "salutation"), Field(value, "email"), Field(value, "phone"));
    }
    public IReadOnlyList<CanonicalInvoiceLine> InvoiceLines()
    {
        if (!Try("invoice.lines", out var value) || value.ValueKind != JsonValueKind.Array) return [];
        var lines = new List<CanonicalInvoiceLine>(); var ordinal = 0;
        foreach (var item in value.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object) continue;
            ordinal++;
            var index = int.TryParse(Field(item, "lineIndex", "index"), out var parsedIndex) ? parsedIndex : ordinal;
            decimal.TryParse(Field(item, "quantity"), NumberStyles.Any, CultureInfo.InvariantCulture, out var quantity);
            decimal.TryParse(Field(item, "unitPrice"), NumberStyles.Any, CultureInfo.InvariantCulture, out var unitPrice);
            decimal.TryParse(Field(item, "amount"), NumberStyles.Any, CultureInfo.InvariantCulture, out var amount);
            if (amount == 0m) amount = quantity * unitPrice;
            lines.Add(new(index, Field(item, "description"), quantity, unitPrice, amount));
        }
        return lines.OrderBy(x => x.LineIndex).ToArray();
    }

    public void ProjectOnto(JobUploadRequest payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        payload.Services ??= new ServicesSection(); payload.JobDetails ??= new JobDetailsSection();
        payload.Contact1 ??= new ContactFlat(); payload.Contact2 ??= new ContactFlat(); payload.Job ??= new JobSection();
        payload.Services.Primary = Text("service.primary");
        payload.Services.Additional1 = Text("service.additional_1"); payload.Services.Additional2 = Text("service.additional_2");
        payload.JobDetails.Weathertightness = YesNo("scope.weathertightness");
        payload.JobDetails.Outbuilding = YesNo("scope.garage_outbuilding"); payload.JobDetails.AttachedFlat = YesNo("scope.attached_flat");
        payload.JobDetails.CouncilFiles = YesNo("scope.property_file_review"); payload.JobDetails.Monolithic = YesNo("scope.monolithic");
        payload.JobDetails.Occupied = YesNo("scope.occupied"); payload.JobDetails.TravelFee = YesNo("scope.travel_fee");
        payload.JobDetails.HhsBedrooms = Integer("scope.healthy_homes_bedrooms")?.ToString(CultureInfo.InvariantCulture) ?? "";
        payload.JobDetails.MethSamples = Integer("scope.meth_samples")?.ToString(CultureInfo.InvariantCulture) ?? "";
        payload.JobDetails.HhsReinspect = YesNo("scope.healthy_homes_reinspection"); payload.JobDetails.FoundationSpace = YesNo("scope.foundation_space");
        payload.JobDetails.HhsReinspectDate = Text("scope.healthy_homes_reinspection_date");
        payload.Job.AgeOfBuilding = Text("property.year_built", payload.Job.GetAgeOfBuilding());
        payload.JobDetails.Bedrooms = Integer("property.bedrooms")?.ToString(CultureInfo.InvariantCulture) ?? payload.JobDetails.Bedrooms;
        payload.JobDetails.Bathrooms = Integer("property.bathrooms")?.ToString(CultureInfo.InvariantCulture) ?? payload.JobDetails.Bathrooms;
        payload.JobDetails.Stories = Integer("property.levels")?.ToString(CultureInfo.InvariantCulture) ?? payload.JobDetails.Stories;
        payload.JobDetails.BuildingType = Text("property.wall_roof", payload.JobDetails.BuildingType);
        ApplyContact(payload.Contact1, Contact(1), 0); ApplyContact(payload.Contact2, Contact(2), 1);
        payload.InvoiceLines = InvoiceLines().Select(x => new InvoiceLineSection
        { LineIndex = x.LineIndex, Description = x.Description, Quantity = x.Quantity, UnitPrice = x.UnitPrice }).ToList();
        if (Decimal("invoice.total") is { } total) payload.Job.InvoiceTotal = total.ToString("0.00", CultureInfo.InvariantCulture);
    }

    private bool Try(string key, out JsonElement value)
    {
        if (_values.TryGetProperty(key, out value)) return true;
        foreach (var property in _values.EnumerateObject())
            if (string.Equals(property.Name, key, StringComparison.OrdinalIgnoreCase)) { value = property.Value; return true; }
        value = default; return false;
    }
    private static string ScalarText(JsonElement value, string fallback) => value.ValueKind switch
    { JsonValueKind.String => value.GetString()?.Trim() ?? fallback, JsonValueKind.True => "true", JsonValueKind.False => "false", JsonValueKind.Number => value.GetRawText(), _ => fallback };
    private static bool? ParseBoolean(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.True) return true; if (value.ValueKind == JsonValueKind.False) return false;
        return ScalarText(value, "").Trim().ToLowerInvariant() switch { "yes" or "y" or "true" or "1" => true, "no" or "n" or "false" or "0" => false, _ => null };
    }
    private static string Field(JsonElement item, params string[] names)
    {
        foreach (var name in names) if (item.TryGetProperty(name, out var value)) return ScalarText(value, "");
        return "";
    }
    private string YesNo(string key) => Boolean(key) switch { true => "Yes", false => "No", _ => Text(key) };
    private static void ApplyContact(ContactFlat target, CanonicalContact? source, int index)
    {
        if (source == null) return;
        target.ContactIndex = index; target.FirstName = source.FirstName; target.LastName = source.LastName;
        target.DisplayName = source.DisplayName; target.Salutation = source.Salutation; target.Email = source.Email; target.Cellular = source.Phone;
    }
}

public static class CanonicalSyncLegacyAdapter
{
    public static CanonicalSyncEnvelope From(JobUploadRequest payload)
    {
        var contact1 = Contact(payload.Contact1); var contact2 = Contact(payload.Contact2);
        var lines = (payload.InvoiceLines ?? []).OrderBy(x => x.LineIndex).Select(x => new Dictionary<string, object?>
        { ["lineIndex"] = x.LineIndex, ["description"] = x.Description, ["quantity"] = x.Quantity, ["unitPrice"] = x.UnitPrice, ["amount"] = x.Quantity * x.UnitPrice }).ToArray();
        decimal.TryParse(payload.Job?.InvoiceTotal, NumberStyles.Any, CultureInfo.InvariantCulture, out var invoiceTotal);
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["service.primary"] = payload.Services?.Primary ?? "", ["service.additional_1"] = payload.Services?.Additional1 ?? "",
            ["service.additional_2"] = payload.Services?.Additional2 ?? "", ["scope.weathertightness"] = payload.JobDetails?.Weathertightness ?? "",
            ["scope.garage_outbuilding"] = payload.JobDetails?.Outbuilding ?? "", ["scope.attached_flat"] = payload.JobDetails?.AttachedFlat ?? "",
            ["scope.property_file_review"] = payload.JobDetails?.CouncilFiles ?? "", ["property.year_built"] = payload.Job?.AgeOfBuilding ?? "",
            ["contact.1"] = contact1, ["contact.2"] = contact2, ["invoice.lines"] = lines, ["invoice.total"] = invoiceTotal
        };
        var element = JsonSerializer.SerializeToElement(values, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(CanonicalSyncEnvelopeSupport.Canonicalize(element)))).ToLowerInvariant();
        return new(CanonicalSyncEnvelopeSupport.ContractVersion, 0, "", "legacy.payload-v0", "", "legacy_compatibility",
            true, [], fingerprint, fingerprint, element, true);
    }

    private static object Contact(ContactFlat? value) => new
    {
        firstName = value?.FirstName ?? "", lastName = value?.LastName ?? "", displayName = value?.DisplayName ?? "",
        salutation = value?.Salutation ?? "", email = value?.Email ?? "", phone = value?.Cellular ?? ""
    };
}

public sealed record CanonicalSyncEnvelope(int ContractVersion, int ProfileVersion, string ProfileFingerprint,
    string MappingSource, string DiscoveryFingerprint, string DeclaredValidationStatus, bool DeclaredValid,
    IReadOnlyList<string> MissingRequiredFields, string DeclaredValuesFingerprint,
    string ComputedValuesFingerprint, JsonElement Values, bool LegacyCompatibility);
public sealed record CanonicalSyncParseResult(bool Success, CanonicalSyncEnvelope? Envelope, string Code, string Message)
{
    public static CanonicalSyncParseResult Parsed(CanonicalSyncEnvelope envelope) => new(true, envelope, "parsed", "Canonical envelope parsed.");
    public static CanonicalSyncParseResult Legacy(CanonicalSyncEnvelope envelope, string code) => new(true, envelope, code, "Legacy compatibility payload parsed.");
    public static CanonicalSyncParseResult Invalid(string code, string message) => new(false, null, code, message);
}
public sealed record CanonicalSyncGateOptions(bool AllowLegacyCompatibility = true, bool RequireActiveProfile = false);
public sealed record CanonicalSyncGateResult(bool Allowed, string Code, bool CompatibilityMode, string Message)
{
    public static CanonicalSyncGateResult Allow(string code, bool compatibility, string message) => new(true, code, compatibility, message);
    public static CanonicalSyncGateResult Block(string code, string message) => new(false, code, false, message);
}
public sealed record CanonicalContact(string FirstName, string LastName, string DisplayName, string Salutation, string Email, string Phone);
public sealed record CanonicalInvoiceLine(int LineIndex, string Description, decimal Quantity, decimal UnitPrice, decimal Amount);

public static class CanonicalSyncResolver
{
    /// <summary>
    /// One-call integration hook for sync ingestion and guarded workflow preparation.
    /// Ingestion should pass AllowLegacyCompatibility=true/RequireActiveProfile=false;
    /// customer-facing workflow preparation should pass false/true.
    /// </summary>
    public static async Task<CanonicalSyncResolution> ResolveAsync(
        NpgsqlConnection connection, Guid tenantId, string rawPayloadJson, JobUploadRequest legacyPayload,
        CanonicalSyncGateOptions options, CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty) throw new ArgumentException("Tenant is required.", nameof(tenantId));
        var parse = CanonicalSyncEnvelopeSupport.Parse(rawPayloadJson, legacyPayload);
        var profile = await TenantMappingProfileSupport.LoadCurrentAsync(connection, tenantId, cancellationToken);
        var gate = CanonicalSyncGate.Evaluate(parse, profile, options);
        return new(parse, profile, gate, gate.Allowed && parse.Envelope != null ? new CanonicalSyncAccessors(parse.Envelope, gate) : null);
    }

    public static string AttentionReason(CanonicalSyncGateResult gate) => gate.Code switch
    {
        "mapping_profile_mismatch" => "MAPPING CHANGED - re-sync and review required",
        "mapping_discovery_stale" => "THREED MAPPING STALE - validation required",
        "mapping_profile_invalid" or "mapping_not_validated" => "THREED MAPPING INVALID - workflow blocked",
        "mapping_profile_required" or "canonical_envelope_required" => "THREED MAPPING REQUIRED - workflow blocked",
        "canonical_values_tampered" or "canonical_value_type_invalid" or "canonical_required_value_missing" => "CANONICAL JOB DATA INVALID - workflow blocked",
        _ => gate.Allowed ? "" : "THREED MAPPING REVIEW REQUIRED"
    };
}

public sealed record CanonicalSyncResolution(CanonicalSyncParseResult Parse, TenantMappingProfileVersion? ActiveProfile,
    CanonicalSyncGateResult Gate, CanonicalSyncAccessors? Values);

public static class CanonicalSyncProjection
{
    public static void Apply(JobUploadRequest payload,CanonicalSyncAccessors values)
    {
        if(payload==null||values==null)return;
        payload.Services??=new();payload.JobDetails??=new();payload.Job??=new();
        payload.Services.Primary=values.Text("service.primary",payload.Services.Primary);payload.Services.Additional1=values.Text("service.additional_1",payload.Services.Additional1);payload.Services.Additional2=values.Text("service.additional_2",payload.Services.Additional2);
        SetBool(values,"scope.weathertightness",v=>payload.JobDetails.Weathertightness=v);SetBool(values,"scope.monolithic",v=>payload.JobDetails.Monolithic=v);SetBool(values,"scope.garage_outbuilding",v=>payload.JobDetails.Outbuilding=v);
        SetBool(values,"scope.occupied",v=>payload.JobDetails.Occupied=v);SetBool(values,"scope.attached_flat",v=>payload.JobDetails.AttachedFlat=v);SetBool(values,"scope.travel_fee",v=>payload.JobDetails.TravelFee=v);
        SetBool(values,"scope.healthy_homes_reinspection",v=>payload.JobDetails.HhsReinspect=v);SetBool(values,"scope.property_file_review",v=>payload.JobDetails.CouncilFiles=v);SetBool(values,"scope.foundation_space",v=>payload.JobDetails.FoundationSpace=v);
        SetText(values,"scope.healthy_homes_bedrooms",v=>payload.JobDetails.HhsBedrooms=v);SetText(values,"scope.meth_samples",v=>payload.JobDetails.MethSamples=v);SetText(values,"scope.healthy_homes_reinspection_date",v=>payload.JobDetails.HhsReinspectDate=v);
        SetText(values,"property.year_built",v=>payload.JobDetails.AgeOfBuilding=v);SetText(values,"property.bedrooms",v=>payload.JobDetails.Bedrooms=v);SetText(values,"property.bathrooms",v=>payload.JobDetails.Bathrooms=v);SetText(values,"property.levels",v=>payload.JobDetails.Stories=v);SetText(values,"property.wall_roof",v=>payload.JobDetails.BuildingType=v);
        var contact1=values.Contact(1);if(contact1!=null)payload.Contact1=Contact(contact1,0,payload.Contact1?.RoleLabel??"Client");var contact2=values.Contact(2);if(contact2!=null)payload.Contact2=Contact(contact2,1,payload.Contact2?.RoleLabel??"Buyers Agent");
        var lines=values.InvoiceLines();if(lines.Count>0)payload.InvoiceLines=lines.Select(x=>new InvoiceLineSection{LineIndex=x.LineIndex,Description=x.Description,Quantity=x.Quantity,UnitPrice=x.UnitPrice}).ToList();
        var total=values.Decimal("invoice.total");if(total.HasValue)payload.Job.InvoiceTotal=total.Value.ToString("0.00",CultureInfo.InvariantCulture);
    }
    private static void SetBool(CanonicalSyncAccessors values,string key,Action<string> set){var value=values.Boolean(key);if(value.HasValue)set(value.Value?"Yes":"No");}
    private static void SetText(CanonicalSyncAccessors values,string key,Action<string> set){if(values.Has(key))set(values.Text(key));}
    private static ContactFlat Contact(CanonicalContact value,int index,string role)=>new(){ContactIndex=index,RoleLabel=role,FirstName=value.FirstName,LastName=value.LastName,DisplayName=value.DisplayName,Salutation=value.Salutation,Email=value.Email,Cellular=value.Phone};
}
