using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

public static class TenantServiceCatalogueSupport
{
    public const int ContractVersion = 1;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.tenant_service_catalogue_state
(
 tenant_id uuid PRIMARY KEY,
 draft_version integer NOT NULL DEFAULT 0,
 active_version integer NOT NULL DEFAULT 0,
 draft_json jsonb NULL,
 discovery_fingerprint text NOT NULL DEFAULT '',
 updated_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.tenant_service_catalogue_versions
(
 tenant_id uuid NOT NULL,
 catalogue_version integer NOT NULL,
 catalogue_json jsonb NOT NULL,
 catalogue_fingerprint text NOT NULL,
 discovery_fingerprint text NOT NULL DEFAULT '',
 created_by text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,catalogue_version)
);
CREATE TABLE IF NOT EXISTS public.tenant_service_catalogue_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id uuid NOT NULL,
 action_key text NOT NULL,
 draft_version integer NOT NULL,
 catalogue_version integer NOT NULL,
 actor text NOT NULL,
 detail_json jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
DO $$ BEGIN
 IF to_regclass('public.jobs_staging') IS NOT NULL THEN
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS service_catalogue_version integer NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS service_catalogue_snapshot_json jsonb NULL;
  ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS service_catalogue_review_required boolean NOT NULL DEFAULT false;
 END IF;
END $$;
""";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static ServiceCatalogueValidation Validate(ServiceCatalogueDraft draft)
    {
        var errors = new List<ServiceCatalogueIssue>();
        var warnings = new List<ServiceCatalogueIssue>();
        if (draft.ContractVersion != ContractVersion) errors.Add(new("catalogue", "unsupported_contract", "The service catalogue contract version is not supported."));
        var categories = draft.Categories ?? [];
        var services = draft.Services ?? [];
        var modifiers = draft.ModifierGroups ?? [];
        var quantityRules=draft.QuantityPricingRules??[];
        foreach (var duplicate in categories.GroupBy(x => x.Id).Where(x => x.Key == Guid.Empty || x.Count() > 1))
            errors.Add(new("category", "duplicate_category", "Every category requires a unique stable ID."));
        foreach (var duplicate in categories.Where(x => !x.Archived).GroupBy(x => Normalize(x.Name)).Where(x => string.IsNullOrEmpty(x.Key) || x.Count() > 1))
            errors.Add(new("category", "duplicate_category_name", "Active category names must be unique and non-empty."));
        var categoryIds = categories.Select(x => x.Id).ToHashSet();
        var archivedCategoryIds=categories.Where(x=>x.Archived).Select(x=>x.Id).ToHashSet();
        foreach(var field in draft.Fields??[])
            if(string.IsNullOrWhiteSpace(field.SourceField)||!new[]{"service","modifier","property","operational","ignored"}.Contains(field.Role))
                errors.Add(new("field","invalid_field_role","Every classified THREED field requires a supported role."));
        var bindings = new Dictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);
        foreach (var service in services)
        {
            if (service.Id == Guid.Empty || string.IsNullOrWhiteSpace(service.Name)) errors.Add(new("service", "invalid_service", "Every service requires a stable ID and name."));
            if (!categoryIds.Contains(service.CategoryId)) errors.Add(new(service.Name, "missing_category", "The service category does not exist."));
            if(!service.Archived&&archivedCategoryIds.Contains(service.CategoryId))errors.Add(new(service.Name,"archived_category","An active service cannot use an archived category."));
            foreach (var binding in service.Bindings ?? [])
            {
                var key = BindingKey(binding.SourceField, binding.ListItemId);
                if (string.IsNullOrEmpty(key)) errors.Add(new(service.Name, "invalid_binding", "A service binding requires a THREED field and list item ID."));
                else if (bindings.TryGetValue(key, out var owner) && owner != service.Id) errors.Add(new(service.Name, "duplicate_binding", "A THREED selection is mapped to more than one service."));
                else bindings[key] = service.Id;
                if (string.IsNullOrWhiteSpace(binding.InvoiceItemId)) warnings.Add(new(service.Name, "missing_invoice_link", $"{binding.ListItemName} has no linked THREED invoice item."));
                if(!binding.Active)warnings.Add(new(service.Name,"inactive_list_item",$"{binding.ListItemName} is inactive in THREED."));
            }
        }
        foreach (var modifier in modifiers)
        {
            if (modifier.Id == Guid.Empty || string.IsNullOrWhiteSpace(modifier.Name) || string.IsNullOrWhiteSpace(modifier.SourceField))
                errors.Add(new("modifier", "invalid_modifier", "Every modifier group requires a stable ID, name and THREED source field."));
            if (!modifier.AllCategories && (modifier.CategoryIds == null || modifier.CategoryIds.Count == 0))
                errors.Add(new(modifier.Name, "modifier_category_required", "Choose at least one applicable service category or All categories."));
            foreach (var categoryId in modifier.CategoryIds ?? []) if (!categoryIds.Contains(categoryId))
                errors.Add(new(modifier.Name, "modifier_category_missing", "A selected modifier category no longer exists."));
            foreach (var option in modifier.Options ?? []) if (string.IsNullOrWhiteSpace(option.InvoiceItemId))
                warnings.Add(new(modifier.Name, "missing_invoice_link", $"{option.ListItemName} has no linked THREED invoice item."));
        }
        foreach(var rule in quantityRules.Where(x=>x.Active))
        {
            if(rule.RuleVersion!=1||!string.Equals(rule.SourceCanonicalField,"property.floor_area",StringComparison.OrdinalIgnoreCase))errors.Add(new("Floor-area pricing","invalid_quantity_source","Floor-area pricing must use property.floor_area contract version 1."));
            if(string.IsNullOrWhiteSpace(rule.InvoiceItemId)||string.IsNullOrWhiteSpace(rule.InvoiceItemName)||rule.UnitRateInclGst<=0)errors.Add(new("Floor-area pricing","invoice_item_required","Choose an active THREED invoice item with a positive GST-inclusive per-m² price."));
            if(!rule.AllCategories&&(rule.CategoryIds==null||rule.CategoryIds.Count==0))errors.Add(new("Floor-area pricing","category_required","Choose applicable service categories or All categories."));
            foreach(var id in rule.CategoryIds??[])if(!categoryIds.Contains(id))errors.Add(new("Floor-area pricing","category_missing","An applicable service category no longer exists."));
        }
        if(quantityRules.Count(x=>x.Active)>1)errors.Add(new("Floor-area pricing","duplicate_active_rule","Only one floor-area pricing rule may be active."));
        if(draft.TravelPricingRule is { Active:true } travel)
        {
            if(travel.RuleVersion!=1||string.IsNullOrWhiteSpace(travel.InvoiceItemId)||string.IsNullOrWhiteSpace(travel.InvoiceItemName)||travel.PriceInclGst<=0)
                errors.Add(new("Travel charge","invoice_item_required","Choose one active THREED invoice item with a positive GST-inclusive travel price."));
        }
        return new(errors.Count == 0, errors, warnings);
    }

    public static async Task<ServiceCatalogueState> LoadAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct = default)
    {
        await EnsureSeedAsync(connection, tenantId, ct);
        const string sql = "SELECT draft_version,active_version,draft_json::text,discovery_fingerprint FROM public.tenant_service_catalogue_state WHERE tenant_id=@tenant";
        await using var command = new NpgsqlCommand(sql, connection); command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
        var draft = JsonSerializer.Deserialize<ServiceCatalogueDraft>(reader.GetString(2), JsonOptions)!;
        return new(reader.GetInt32(0), reader.GetInt32(1), draft, reader.GetString(3), Validate(draft));
    }

    public static async Task<(int Version,ServiceCatalogueDraft Draft)> LoadActiveAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureSeedAsync(connection,tenantId,ct);
        const string sql="""
SELECT s.active_version,v.catalogue_json::text FROM public.tenant_service_catalogue_state s
LEFT JOIN public.tenant_service_catalogue_versions v ON v.tenant_id=s.tenant_id AND v.catalogue_version=s.active_version
WHERE s.tenant_id=@tenant
""";
        await using var command=new NpgsqlCommand(sql,connection);command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);
        var version=reader.GetInt32(0);var draft=version>0&&!reader.IsDBNull(1)?JsonSerializer.Deserialize<ServiceCatalogueDraft>(reader.GetString(1),JsonOptions)!:new ServiceCatalogueDraft(ContractVersion,"",[],[],[],[]);
        return(version,draft);
    }

    public static async Task<ServiceCatalogueSchedulingGate> CheckSchedulingGateAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);
        var active=await LoadActiveAsync(connection,tenantId,ct);
        if(active.Version<=0)return new(false,"catalogue_not_activated","Activate a valid service catalogue before scheduling new jobs.",active.Version,0);
        const string sql="SELECT COALESCE(service_catalogue_version,0),service_catalogue_review_required FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job LIMIT 1";
        await using var command=new NpgsqlCommand(sql,connection);command.Parameters.AddWithValue("tenant",tenantId.ToString());command.Parameters.AddWithValue("job",jobId);
        await using var reader=await command.ExecuteReaderAsync(ct);if(!await reader.ReadAsync(ct))return new(false,"job_not_synced","Sync this THREED job before scheduling it.",active.Version,0);
        var jobVersion=reader.GetInt32(0);var review=reader.GetBoolean(1);
        if(jobVersion!=active.Version)return new(false,"catalogue_resync_required","The service catalogue changed. Re-sync this job before scheduling it.",active.Version,jobVersion);
        if(review)return new(false,"catalogue_mapping_required","This job contains a THREED service or modifier that is not mapped. Update Data Mapping and activate a new catalogue version before scheduling.",active.Version,jobVersion);
        return new(true,"ready","Service catalogue mapping is current.",active.Version,jobVersion);
    }

    public static async Task<ServiceCatalogueJobResolution> ResolveJobAsync(NpgsqlConnection connection, Guid tenantId, JobUploadRequest job, CancellationToken ct = default)
    {
        var (version, draft) = await LoadActiveAsync(connection, tenantId, ct);
        if (version <= 0)
            return new(0, "not_activated", false, 0, 0, "{}");

        var raw = job.RawCustomFields ?? new(StringComparer.OrdinalIgnoreCase);
        var categories = (draft.Categories ?? []).ToDictionary(x => x.Id);
        var selectedServices = new List<object>();
        var selectedCategoryIds = new HashSet<Guid>();
        var selectedServiceIds = new HashSet<Guid>();
        var modifiers = new List<object>();
        var invoiceReview = new List<object>();
        var warnings = new List<string>();
        var pricingReview=false;
        var serviceFields = (draft.Fields ?? []).Where(x => x.Role == "service").Select(x => x.SourceField).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var service in (draft.Services ?? []).Where(x => !x.Archived))
        {
            foreach (var binding in service.Bindings ?? [])
            {
                if (!raw.TryGetValue(binding.SourceField, out var selected) || !SameSelection(selected, binding.ListItemName)) continue;
                if (!selectedServiceIds.Add(service.Id)) continue;
                selectedCategoryIds.Add(service.CategoryId);
                categories.TryGetValue(service.CategoryId, out var category);
                var invoice = FindInvoice(job.InvoiceLines, binding.InvoiceItemName);
                var expectedAmount = binding.InvoiceItemPrice;
                var priceDiffers = invoice != null && expectedAmount.HasValue && invoice.UnitPrice != expectedAmount.Value;
                selectedServices.Add(new { serviceId=service.Id, serviceName=service.Name, categoryId=service.CategoryId, categoryName=category?.Name ?? "", sourceField=binding.SourceField, selectedValue=selected, binding.ListItemId });
                invoiceReview.Add(new { serviceId=service.Id, expectedInvoiceItemId=binding.InvoiceItemId, expectedInvoiceItem=binding.InvoiceItemName, expectedPrice=expectedAmount, present=invoice != null, actualDescription=invoice?.Description ?? "", actualUnitPrice=invoice?.UnitPrice, priceDiffers });
                if (invoice == null && !string.IsNullOrWhiteSpace(binding.InvoiceItemName)) warnings.Add($"Expected invoice item missing for {service.Name}: {binding.InvoiceItemName}");
            }
        }

        foreach (var field in serviceFields)
            if (raw.TryGetValue(field, out var selected) && !string.IsNullOrWhiteSpace(selected) && !(draft.Services ?? []).SelectMany(x => x.Bindings ?? []).Any(x => x.SourceField.Equals(field,StringComparison.OrdinalIgnoreCase) && SameSelection(selected,x.ListItemName)))
                warnings.Add($"Unmapped service selection in {field}: {selected}");

        foreach (var group in (draft.ModifierGroups ?? []).Where(x => !x.Archived))
        {
            if (!raw.TryGetValue(group.SourceField, out var selected) || string.IsNullOrWhiteSpace(selected)) continue;
            if (!group.AllCategories && !(group.CategoryIds ?? []).Any(selectedCategoryIds.Contains)) continue;
            var option=(group.Options??[]).FirstOrDefault(x=>SameSelection(selected,x.ListItemName));
            modifiers.Add(new { modifierGroupId=group.Id, modifierGroupName=group.Name, sourceField=group.SourceField, value=selected, normalizedValue=option?.NormalizedValue ?? Normalize(selected), optionListItemId=option?.ListItemId ?? "" });
            if(option==null)warnings.Add($"Unmapped modifier value in {group.SourceField}: {selected}");
        }

        var floorArea=decimal.TryParse(job.JobDetails?.FloorArea,System.Globalization.NumberStyles.Number,System.Globalization.CultureInfo.InvariantCulture,out var parsedArea)&&parsedArea>0?parsedArea:(decimal?)null;
        foreach(var rule in (draft.QuantityPricingRules??[]).Where(x=>x.Active&&(x.AllCategories||(x.CategoryIds??[]).Any(selectedCategoryIds.Contains))))
        {
            var expectedAmount=floorArea.HasValue?Math.Round(floorArea.Value*rule.UnitRateInclGst,2,MidpointRounding.AwayFromZero):(decimal?)null;
            var invoice=(job.InvoiceLines??[]).FirstOrDefault(x=>!string.IsNullOrWhiteSpace(x.ItemId)&&string.Equals(x.ItemId,rule.InvoiceItemId,StringComparison.OrdinalIgnoreCase))??FindInvoice(job.InvoiceLines,rule.InvoiceItemName);
            var quantityMatches=invoice!=null&&floorArea.HasValue&&Math.Abs(invoice.Quantity-floorArea.Value)<=0.0001m;var rateMatches=invoice!=null&&Math.Abs(invoice.UnitPrice-rule.UnitRateInclGst)<=0.01m;var amountMatches=invoice!=null&&expectedAmount.HasValue&&Math.Abs(invoice.Amount-expectedAmount.Value)<=0.01m;
            invoiceReview.Add(new{pricingRuleId=rule.Id,source=rule.SourceCanonicalField,floorArea,expectedInvoiceItemId=rule.InvoiceItemId,expectedInvoiceItem=rule.InvoiceItemName,expectedQuantity=floorArea,expectedUnitPrice=rule.UnitRateInclGst,expectedAmount,present=invoice!=null,actualItemId=invoice?.ItemId??"",actualDescription=invoice?.Description??"",actualQuantity=invoice?.Quantity,actualUnitPrice=invoice?.UnitPrice,actualAmount=invoice?.Amount,quantityMatches,rateMatches,amountMatches});
            if(!floorArea.HasValue){warnings.Add("Floor-area pricing applies, but mapped THREED Property floor area is missing or invalid.");pricingReview=true;}
            else if(invoice==null){warnings.Add($"Calculated floor-area line requires approval: {floorArea.Value:0.####} m² × {rule.UnitRateInclGst:C} incl GST = {expectedAmount:C}. AutoMate did not write to THREED.");pricingReview=true;}
            else if(!quantityMatches||!rateMatches||!amountMatches){warnings.Add($"Floor-area invoice line differs from the active catalogue rule. Expected {floorArea.Value:0.####} m² × {rule.UnitRateInclGst:C} incl GST = {expectedAmount:C}.");pricingReview=true;}
        }

        var snapshot = new
        {
            contractVersion = ContractVersion,
            catalogueVersion = version,
            resolvedAtUtc = DateTimeOffset.UtcNow,
            categories = selectedCategoryIds.Select(id => new { categoryId=id, categoryName=categories.TryGetValue(id,out var category)?category.Name:"" }).ToList(),
            services = selectedServices,
            modifiers,
            invoiceReconciliation = invoiceReview,
            actualInvoiceLines = (job.InvoiceLines ?? []).Select(x => new { x.ItemId,x.LineIndex, x.Description, x.Quantity, x.UnitPrice, x.Amount }).ToList(),
            warnings
        };
        var reviewRequired = pricingReview||warnings.Any(x => x.StartsWith("Unmapped service selection", StringComparison.Ordinal));
        return new(version, reviewRequired ? "review_required" : "resolved", reviewRequired, selectedServices.Count, modifiers.Count, JsonSerializer.Serialize(snapshot, JsonOptions));
    }

    public static async Task<ServiceCatalogueSaveResult> SaveDraftAsync(NpgsqlConnection connection, Guid tenantId, int expectedVersion, ServiceCatalogueDraft draft, string actor, CancellationToken ct = default)
    {
        var validation = Validate(draft);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await EnsureSeedAsync(connection, tenantId, ct, transaction);
        int current;
        await using (var load = new NpgsqlCommand("SELECT draft_version FROM public.tenant_service_catalogue_state WHERE tenant_id=@tenant FOR UPDATE", connection, transaction))
        { load.Parameters.AddWithValue("tenant", tenantId); current = Convert.ToInt32(await load.ExecuteScalarAsync(ct)); }
        if (current != expectedVersion) { await transaction.RollbackAsync(ct); return new(false, "conflict", current, 0, validation, "The catalogue changed; reload before saving."); }
        var next = current + 1; var json = JsonSerializer.Serialize(draft, JsonOptions);
        const string sql = """
UPDATE public.tenant_service_catalogue_state SET draft_version=@version,draft_json=CAST(@json AS jsonb),discovery_fingerprint=@discovery,updated_at=NOW() WHERE tenant_id=@tenant;
INSERT INTO public.tenant_service_catalogue_audit(tenant_id,action_key,draft_version,catalogue_version,actor,detail_json)
VALUES(@tenant,'catalogue_draft_saved',@version,0,@actor,jsonb_build_object('valid',@valid,'errorCount',@errors,'warningCount',@warnings));
""";
        await using var save = new NpgsqlCommand(sql, connection, transaction);
        save.Parameters.AddWithValue("tenant", tenantId); save.Parameters.AddWithValue("version", next); save.Parameters.AddWithValue("json", json);
        save.Parameters.AddWithValue("discovery", draft.DiscoveryFingerprint ?? ""); save.Parameters.AddWithValue("actor", actor);
        save.Parameters.AddWithValue("valid", validation.Valid); save.Parameters.AddWithValue("errors", validation.Errors.Count); save.Parameters.AddWithValue("warnings", validation.Warnings.Count);
        await save.ExecuteNonQueryAsync(ct); await transaction.CommitAsync(ct);
        return new(true, "saved", next, 0, validation, "Service catalogue draft saved.");
    }

    public static async Task<ServiceCatalogueSaveResult> ActivateAsync(NpgsqlConnection connection, Guid tenantId, int expectedDraftVersion, string actor, CancellationToken ct = default)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct); await EnsureSeedAsync(connection, tenantId, ct, transaction);
        int draftVersion, activeVersion; string json, discovery;
        await using (var load = new NpgsqlCommand("SELECT draft_version,active_version,draft_json::text,discovery_fingerprint FROM public.tenant_service_catalogue_state WHERE tenant_id=@tenant FOR UPDATE", connection, transaction))
        {
            load.Parameters.AddWithValue("tenant", tenantId); await using var reader = await load.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
            draftVersion=reader.GetInt32(0);activeVersion=reader.GetInt32(1);json=reader.GetString(2);discovery=reader.GetString(3);
        }
        var draft=JsonSerializer.Deserialize<ServiceCatalogueDraft>(json,JsonOptions)!;var validation=Validate(draft);
        if(draftVersion!=expectedDraftVersion){await transaction.RollbackAsync(ct);return new(false,"conflict",draftVersion,activeVersion,validation,"The catalogue draft changed; reload before activating.");}
        if(!validation.Valid){await transaction.RollbackAsync(ct);return new(false,"invalid",draftVersion,activeVersion,validation,"Resolve catalogue errors before activation.");}
        var next=activeVersion+1;var fingerprint=Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json))).ToLowerInvariant();
        const string sql="""
INSERT INTO public.tenant_service_catalogue_versions(tenant_id,catalogue_version,catalogue_json,catalogue_fingerprint,discovery_fingerprint,created_by)
VALUES(@tenant,@version,CAST(@json AS jsonb),@fingerprint,@discovery,@actor);
UPDATE public.tenant_service_catalogue_state SET active_version=@version,updated_at=NOW() WHERE tenant_id=@tenant;
INSERT INTO public.tenant_service_catalogue_audit(tenant_id,action_key,draft_version,catalogue_version,actor,detail_json)
VALUES(@tenant,'catalogue_version_activated',@draft,@version,@actor,jsonb_build_object('fingerprint',@fingerprint,'warningCount',@warnings));
UPDATE public.jobs_staging SET service_catalogue_review_required=true WHERE tenant_id::text=@tenant_text AND COALESCE(service_catalogue_version,0)<>@version;
""";
        await using var activate=new NpgsqlCommand(sql,connection,transaction);activate.Parameters.AddWithValue("tenant",tenantId);activate.Parameters.AddWithValue("version",next);activate.Parameters.AddWithValue("json",json);activate.Parameters.AddWithValue("fingerprint",fingerprint);activate.Parameters.AddWithValue("discovery",discovery);activate.Parameters.AddWithValue("actor",actor);activate.Parameters.AddWithValue("draft",draftVersion);activate.Parameters.AddWithValue("warnings",validation.Warnings.Count);activate.Parameters.AddWithValue("tenant_text",tenantId.ToString());await activate.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);return new(true,"activated",draftVersion,next,validation,"Service catalogue activated. Existing jobs require controlled re-sync; no workflow action was triggered.");
    }

    private static async Task EnsureSeedAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        var categoryId=StableId(tenantId,"building-inspection");var draft=new ServiceCatalogueDraft(ContractVersion,"",[],[new(categoryId,"Building Inspection",false,0)],[],[]);
        const string sql="INSERT INTO public.tenant_service_catalogue_state(tenant_id,draft_json) VALUES(@tenant,CAST(@json AS jsonb)) ON CONFLICT DO NOTHING";
        await using var command=new NpgsqlCommand(sql,connection,transaction);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("json",JsonSerializer.Serialize(draft,JsonOptions));await command.ExecuteNonQueryAsync(ct);
    }
    private static Guid StableId(Guid tenantId,string key){var bytes=SHA256.HashData(Encoding.UTF8.GetBytes(tenantId.ToString("D")+":"+key));var guid=new byte[16];Array.Copy(bytes,guid,16);return new Guid(guid);}
    private static bool SameSelection(string? left,string? right)=>Normalize(left)==Normalize(right);
    private static InvoiceLineSection? FindInvoice(List<InvoiceLineSection>? lines,string? description)
    {
        var expected=Normalize(description);if(string.IsNullOrEmpty(expected))return null;
        return (lines??[]).FirstOrDefault(x=>Normalize(x.Description)==expected)
            ?? (lines??[]).FirstOrDefault(x=>Normalize(x.Description).Contains(expected,StringComparison.Ordinal)||expected.Contains(Normalize(x.Description),StringComparison.Ordinal));
    }
    private static string BindingKey(string? field,string? item)=>string.IsNullOrWhiteSpace(field)||string.IsNullOrWhiteSpace(item)?"":Normalize(field)+":"+Normalize(item);
    private static string Normalize(string? value)=>(value??"").Trim().ToLowerInvariant();
}

public sealed record ServiceCatalogueDraft(int ContractVersion,string DiscoveryFingerprint,List<FieldClassificationDraft> Fields,List<ServiceCategoryDraft> Categories,List<ServiceDefinitionDraft> Services,List<ModifierGroupDraft> ModifierGroups,List<QuantityPricingRuleDraft>? QuantityPricingRules=null,TravelPricingRuleDraft? TravelPricingRule=null);
public sealed record FieldClassificationDraft(string SourceField,string SourceLabel,string Role);
public sealed record ServiceCategoryDraft(Guid Id,string Name,bool Archived,int SortOrder);
public sealed record ServiceDefinitionDraft(Guid Id,string Name,Guid CategoryId,bool Archived,List<ServiceSourceBinding> Bindings);
public sealed record ServiceSourceBinding(string SourceField,string SourceLabel,string ListItemId,string ListItemName,bool Active,string InvoiceItemId,string InvoiceItemName,decimal? InvoiceItemPrice);
public sealed record ModifierGroupDraft(Guid Id,string Name,string SourceField,string SourceLabel,bool Archived,bool AllCategories,List<Guid> CategoryIds,List<ModifierOptionDraft> Options);
public sealed record ModifierOptionDraft(string ListItemId,string ListItemName,string NormalizedValue,bool Active,string InvoiceItemId,string InvoiceItemName,decimal? InvoiceItemPrice);
public sealed record QuantityPricingRuleDraft(Guid Id,int RuleVersion,string SourceCanonicalField,string InvoiceItemId,string InvoiceItemName,decimal UnitRateInclGst,bool AllCategories,List<Guid> CategoryIds,bool Active);
public sealed record TravelPricingRuleDraft(Guid Id,int RuleVersion,string InvoiceItemId,string InvoiceItemName,decimal PriceInclGst,bool Active);
public sealed record ServiceCatalogueIssue(string Subject,string Code,string Message);
public sealed record ServiceCatalogueValidation(bool Valid,List<ServiceCatalogueIssue> Errors,List<ServiceCatalogueIssue> Warnings);
public sealed record ServiceCatalogueState(int DraftVersion,int ActiveVersion,ServiceCatalogueDraft Draft,string DiscoveryFingerprint,ServiceCatalogueValidation Validation);
public sealed record ServiceCatalogueSaveResult(bool Success,string Status,int DraftVersion,int ActiveVersion,ServiceCatalogueValidation Validation,string Message);
public sealed record ServiceCatalogueJobResolution(int Version,string Status,bool ReviewRequired,int ServiceCount,int ModifierCount,string SnapshotJson);
public sealed record ServiceCatalogueSchedulingGate(bool Allowed,string Status,string Message,int ActiveVersion,int JobVersion);
