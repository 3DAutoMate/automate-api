using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class SignNowCatalogueMappingSupport
{
    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        const string sql="""
CREATE TABLE IF NOT EXISTS public.tenant_signnow_mapping_state(
 tenant_id uuid PRIMARY KEY,version integer NOT NULL DEFAULT 1,updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS public.tenant_signnow_catalogue_mappings(
 mapping_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,target_type text NOT NULL,target_id uuid NOT NULL,
 signnow_template_id text NOT NULL,signnow_template_name text NOT NULL DEFAULT '',active boolean NOT NULL DEFAULT true,
 mapping_version integer NOT NULL DEFAULT 1,created_at timestamptz NOT NULL DEFAULT NOW(),updated_at timestamptz NOT NULL DEFAULT NOW(),
 CONSTRAINT ck_signnow_mapping_target CHECK(target_type = 'service'),UNIQUE(tenant_id,target_type,target_id));
CREATE INDEX IF NOT EXISTS idx_signnow_catalogue_mapping_tenant ON public.tenant_signnow_catalogue_mappings(tenant_id,active,target_type,target_id);
""";
        await using var command=new NpgsqlCommand(sql,connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<SignNowMappingState> LoadAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using(var seed=new NpgsqlCommand("INSERT INTO public.tenant_signnow_mapping_state(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection)){seed.Parameters.AddWithValue("tenant",tenantId);await seed.ExecuteNonQueryAsync(ct);}
        int version;await using(var state=new NpgsqlCommand("SELECT version FROM public.tenant_signnow_mapping_state WHERE tenant_id=@tenant",connection)){state.Parameters.AddWithValue("tenant",tenantId);version=Convert.ToInt32(await state.ExecuteScalarAsync(ct));}
        var rows=new List<SignNowCatalogueMapping>();await using var command=new NpgsqlCommand("SELECT mapping_id,target_type,target_id,signnow_template_id,signnow_template_name,active,mapping_version FROM public.tenant_signnow_catalogue_mappings WHERE tenant_id=@tenant ORDER BY target_type,target_id",connection);command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))rows.Add(new(reader.GetGuid(0),reader.GetString(1),reader.GetGuid(2),reader.GetString(3),reader.GetString(4),reader.GetBoolean(5),reader.GetInt32(6)));
        return new(version,rows,Validate(rows));
    }

    public static async Task<SignNowMappingState> SaveAsync(NpgsqlConnection connection,SignNowMappingSaveRequest request,string actor,CancellationToken ct=default)
    {
        var validation=Validate(request.Mappings);if(!validation.Valid)throw new SignNowMappingException("mapping_invalid","Resolve mapping errors before saving.");
        await EnsureAsync(connection,ct);var catalogue=await TenantServiceCatalogueSupport.LoadActiveAsync(connection,request.TenantId,ct);var activeServiceIds=catalogue.Draft.Services.Where(x=>!x.Archived).Select(x=>x.Id).ToHashSet();
        if(catalogue.Version<1||request.Mappings.Any(x=>x.Active&&!activeServiceIds.Contains(x.TargetId)))throw new SignNowMappingException("catalogue_stale","Reload the active Service Catalogue before saving SignNow mappings.");
        var agreementPolicy=await TenantAgreementPolicySupport.LoadAsync(connection,request.TenantId,ct);var requiredServiceIds=agreementPolicy.ActiveVersion>0?await TenantAgreementPolicySupport.LoadRequiredServiceIdsAsync(connection,request.TenantId,ct):agreementPolicy.Draft.Services.Where(x=>x.Requirement==TenantAgreementPolicySupport.Required).Select(x=>x.ServiceId).ToHashSet();
        if(request.Mappings.Any(x=>x.Active&&!requiredServiceIds.Contains(x.TargetId)))throw new SignNowMappingException("agreement_not_required","Only Services classified as Agreement required may have an active SignNow mapping.");
        await using var transaction=await connection.BeginTransactionAsync(ct);
        await using(var seed=new NpgsqlCommand("INSERT INTO public.tenant_signnow_mapping_state(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection,transaction)){seed.Parameters.AddWithValue("tenant",request.TenantId);await seed.ExecuteNonQueryAsync(ct);}
        int current;await using(var state=new NpgsqlCommand("SELECT version FROM public.tenant_signnow_mapping_state WHERE tenant_id=@tenant FOR UPDATE",connection,transaction)){state.Parameters.AddWithValue("tenant",request.TenantId);current=Convert.ToInt32(await state.ExecuteScalarAsync(ct));}
        if(current!=request.ExpectedVersion)throw new SignNowMappingException("mapping_version_conflict","Mappings changed; reload before saving.");
        await using(var archive=new NpgsqlCommand("UPDATE public.tenant_signnow_catalogue_mappings SET active=false,mapping_version=@version,updated_at=NOW() WHERE tenant_id=@tenant",connection,transaction)){archive.Parameters.AddWithValue("tenant",request.TenantId);archive.Parameters.AddWithValue("version",current+1);await archive.ExecuteNonQueryAsync(ct);}
        foreach(var row in request.Mappings){await using var insert=new NpgsqlCommand("INSERT INTO public.tenant_signnow_catalogue_mappings(mapping_id,tenant_id,target_type,target_id,signnow_template_id,signnow_template_name,active,mapping_version) VALUES(@id,@tenant,@type,@target,@template,@name,@active,@version) ON CONFLICT(tenant_id,target_type,target_id) DO UPDATE SET signnow_template_id=EXCLUDED.signnow_template_id,signnow_template_name=EXCLUDED.signnow_template_name,active=EXCLUDED.active,mapping_version=EXCLUDED.mapping_version,updated_at=NOW()",connection,transaction);insert.Parameters.AddWithValue("id",row.MappingId==Guid.Empty?Guid.NewGuid():row.MappingId);insert.Parameters.AddWithValue("tenant",request.TenantId);insert.Parameters.AddWithValue("type",row.TargetType);insert.Parameters.AddWithValue("target",row.TargetId);insert.Parameters.AddWithValue("template",row.SignNowTemplateId);insert.Parameters.AddWithValue("name",row.SignNowTemplateName??"");insert.Parameters.AddWithValue("active",row.Active);insert.Parameters.AddWithValue("version",current+1);await insert.ExecuteNonQueryAsync(ct);}
        await using(var update=new NpgsqlCommand("UPDATE public.tenant_signnow_mapping_state SET version=@version,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant",connection,transaction)){update.Parameters.AddWithValue("version",current+1);update.Parameters.AddWithValue("actor",actor);update.Parameters.AddWithValue("tenant",request.TenantId);await update.ExecuteNonQueryAsync(ct);}await transaction.CommitAsync(ct);return await LoadAsync(connection,request.TenantId,ct);
    }

    public static SignNowMappingValidation Validate(IEnumerable<SignNowCatalogueMapping> mappings)
    {
        var active=mappings.Where(x=>x.Active).ToList();var errors=new List<string>();
        foreach(var row in active){if(row.TargetId==Guid.Empty)errors.Add("Every mapping requires a Service ID.");if(row.TargetType!="service")errors.Add("SignNow mappings must target a Service.");if(string.IsNullOrWhiteSpace(row.SignNowTemplateId))errors.Add("Every active mapping requires a SignNow template.");}
        foreach(var duplicate in active.GroupBy(x=>new{x.TargetType,x.TargetId}).Where(x=>x.Count()>1))errors.Add($"Multiple templates target the same {duplicate.Key.TargetType}.");
        return new(errors.Count==0,errors.Distinct().ToList());
    }

    public static SignNowMappingPreview Preview(SignNowMappingState state,int jobCatalogueVersion,string snapshotJson,bool missingClientEmail)
    {
        if(missingClientEmail)return new("client_email_required","",Guid.Empty,"","Client email is required before an agreement can be prepared.");
        if(jobCatalogueVersion<1||string.IsNullOrWhiteSpace(snapshotJson))return new("approved_catalogue_snapshot_required","",Guid.Empty,"","The job has no approved Service Catalogue snapshot.");
        using var document=JsonDocument.Parse(snapshotJson);var root=document.RootElement;if(!root.TryGetProperty("catalogueVersion",out var cv)||cv.GetInt32()!=jobCatalogueVersion)return new("catalogue_stale","",Guid.Empty,"","The job catalogue snapshot is stale.");
        var services=Ids(root,"services","serviceId");var active=state.Mappings.Where(x=>x.Active&&x.TargetType=="service").ToList();var serviceMatches=active.Where(x=>services.Contains(x.TargetId)).ToList();if(serviceMatches.Count==1)return Selected(serviceMatches[0]);if(serviceMatches.Count>1)return new("mapping_ambiguous","",Guid.Empty,"","More than one Service mapping matched.");return new("mapping_required","",Guid.Empty,"","No active SignNow Service mapping matched this job.");
    }
    private static HashSet<Guid> Ids(JsonElement root,string array,string property){var result=new HashSet<Guid>();if(root.TryGetProperty(array,out var values))foreach(var value in values.EnumerateArray())if(value.TryGetProperty(property,out var id)&&Guid.TryParse(id.ToString(),out var parsed))result.Add(parsed);return result;}
    private static SignNowMappingPreview Selected(SignNowCatalogueMapping row)=>new("selected",row.TargetType,row.TargetId,row.SignNowTemplateId,$"Selected {row.SignNowTemplateName}. No SignNow document or invitation was created.");
}

public sealed record SignNowCatalogueMapping(Guid MappingId,string TargetType,Guid TargetId,string SignNowTemplateId,string SignNowTemplateName,bool Active,int MappingVersion);
public sealed record SignNowMappingValidation(bool Valid,List<string> Errors);
public sealed record SignNowMappingState(int Version,List<SignNowCatalogueMapping> Mappings,SignNowMappingValidation Validation);
public sealed record SignNowMappingSaveRequest(Guid TenantId,int ExpectedVersion,List<SignNowCatalogueMapping> Mappings,bool Confirmed);
public sealed record SignNowMappingPreviewRequest(Guid TenantId,Guid JobId);
public sealed record SignNowMappingPreview(string Status,string TargetType,Guid TargetId,string SignNowTemplateId,string Message);
public sealed class SignNowMappingException(string code,string message):Exception(message){public string Code{get;}=code;}
