using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class AgreementProviderSupport
{
    public static readonly string[] Providers = ["signnow", "adobe_sign", "docusign"];

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.tenant_agreement_provider_mappings
(
 mapping_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,provider_key text NOT NULL,
 service_id uuid NOT NULL,service_name text NOT NULL DEFAULT '',template_id text NOT NULL,template_name text NOT NULL DEFAULT '',
 signer_role text NOT NULL DEFAULT '',active boolean NOT NULL DEFAULT true,mapping_version integer NOT NULL DEFAULT 1,
 updated_by text NOT NULL DEFAULT '',updated_at timestamptz NOT NULL DEFAULT NOW(),
 UNIQUE(tenant_id,provider_key,service_id)
);
CREATE TABLE IF NOT EXISTS public.agreement_provider_webhook_events
(
 provider_key text NOT NULL,event_key text NOT NULL,tenant_id uuid NULL,external_document_id text NOT NULL DEFAULT '',
 signature_valid boolean NOT NULL DEFAULT false,payload_hash text NOT NULL DEFAULT '',payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
 processed_at timestamptz NULL,last_error text NOT NULL DEFAULT '',received_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(provider_key,event_key)
);
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS provider_status text NOT NULL DEFAULT '';
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS provider_updated_at timestamptz NULL;
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS signer_role text NOT NULL DEFAULT '';
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS last_error text NOT NULL DEFAULT '';
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS sent_at timestamptz NULL;
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS webhook_status text NOT NULL DEFAULT '';
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS webhook_subscription_id text NOT NULL DEFAULT '';
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS webhook_last_error text NOT NULL DEFAULT '';
ALTER TABLE public.job_agreement_items ADD COLUMN IF NOT EXISTS webhook_updated_at timestamptz NULL;
CREATE INDEX IF NOT EXISTS ix_job_agreement_items_external
 ON public.job_agreement_items(provider_key,external_document_id) WHERE external_document_id<>'';
""";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static string NormalizeProvider(string provider) => (provider ?? "").Trim().ToLowerInvariant() switch
    {
        "adobe" or "adobe_acrobat_sign" or "acrobat_sign" => "adobe_sign",
        "docu_sign" => "docusign",
        var value => value
    };

    public static async Task<IReadOnlyList<AgreementProviderMappingView>> LoadMappingsAsync(NpgsqlConnection connection, Guid tenantId, string provider, CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);var result=new List<AgreementProviderMappingView>();
        await using var command=new NpgsqlCommand("SELECT mapping_id,service_id,service_name,template_id,template_name,signer_role,active,mapping_version,updated_at FROM public.tenant_agreement_provider_mappings WHERE tenant_id=@tenant AND provider_key=@provider ORDER BY service_name",connection);
        command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("provider",NormalizeProvider(provider));await using var reader=await command.ExecuteReaderAsync(ct);
        while(await reader.ReadAsync(ct))result.Add(new(reader.GetGuid(0),reader.GetGuid(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5),reader.GetBoolean(6),reader.GetInt32(7),reader.GetDateTime(8)));
        return result;
    }

    public static async Task<IReadOnlyList<AgreementProviderMappingView>> SaveMappingsAsync(NpgsqlConnection connection, Guid tenantId, string provider, AgreementProviderMappingsSaveRequest request, string actor, CancellationToken ct = default)
    {
        if(!request.Confirmed)throw new AgreementProviderException("confirmation_required","Confirm the agreement template mappings.");provider=NormalizeProvider(provider);if(!Providers.Contains(provider))throw new AgreementProviderException("invalid_provider","Choose a supported agreement provider.");
        var rows=request.Mappings??[];if(rows.GroupBy(x=>x.ServiceId).Any(x=>x.Key==Guid.Empty||x.Count()>1))throw new AgreementProviderException("invalid_mapping","Each Service can be mapped once and requires a valid Service ID.");
        if(rows.Any(x=>string.IsNullOrWhiteSpace(x.TemplateId)||string.IsNullOrWhiteSpace(x.SignerRole)))throw new AgreementProviderException("mapping_incomplete","Each agreement mapping requires a template and signer role.");
        await EnsureAsync(connection,ct);await using var tx=await connection.BeginTransactionAsync(ct);
        await using(var disable=new NpgsqlCommand("UPDATE public.tenant_agreement_provider_mappings SET active=false,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant AND provider_key=@provider",connection,tx)){disable.Parameters.AddWithValue("tenant",tenantId);disable.Parameters.AddWithValue("provider",provider);disable.Parameters.AddWithValue("actor",actor??"");await disable.ExecuteNonQueryAsync(ct);}
        foreach(var row in rows)
        {
            await using var command=new NpgsqlCommand("""
INSERT INTO public.tenant_agreement_provider_mappings(tenant_id,provider_key,service_id,service_name,template_id,template_name,signer_role,active,updated_by)
VALUES(@tenant,@provider,@service,@serviceName,@template,@templateName,@role,true,@actor)
ON CONFLICT(tenant_id,provider_key,service_id) DO UPDATE SET service_name=EXCLUDED.service_name,template_id=EXCLUDED.template_id,
 template_name=EXCLUDED.template_name,signer_role=EXCLUDED.signer_role,active=true,mapping_version=tenant_agreement_provider_mappings.mapping_version+1,
 updated_by=EXCLUDED.updated_by,updated_at=NOW()
""",connection,tx);
            command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("provider",provider);command.Parameters.AddWithValue("service",row.ServiceId);command.Parameters.AddWithValue("serviceName",row.ServiceName??"");command.Parameters.AddWithValue("template",row.TemplateId.Trim());command.Parameters.AddWithValue("templateName",row.TemplateName??"");command.Parameters.AddWithValue("role",row.SignerRole.Trim());command.Parameters.AddWithValue("actor",actor??"");await command.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);return await LoadMappingsAsync(connection,tenantId,provider,ct);
    }

    public static async Task<AgreementProviderMappingView?> LoadMappingAsync(NpgsqlConnection connection,Guid tenantId,string provider,Guid serviceId,CancellationToken ct=default)
    {
        var rows=await LoadMappingsAsync(connection,tenantId,provider,ct);return rows.FirstOrDefault(x=>x.Active&&x.ServiceId==serviceId);
    }

    public static async Task UpdateItemAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,Guid itemId,string status,string externalDocumentId,string externalInviteId,string providerStatus,string error,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);if(status is not ("not_prepared" or "prepared" or "invited" or "signed" or "failed" or "superseded"))throw new AgreementProviderException("invalid_item_status","Unsupported agreement item status.");
        await using var command=new NpgsqlCommand("""
UPDATE public.job_agreement_items SET status=@status,external_document_id=CASE WHEN @document='' THEN external_document_id ELSE @document END,
 external_invite_id=CASE WHEN @invite='' THEN external_invite_id ELSE @invite END,provider_status=@providerStatus,
 provider_updated_at=NOW(),last_error=@error,sent_at=CASE WHEN @status='invited' THEN COALESCE(sent_at,NOW()) ELSE sent_at END,
 signed_at=CASE WHEN @status='signed' THEN COALESCE(signed_at,NOW()) ELSE signed_at END,updated_at=NOW()
WHERE tenant_id=@tenant AND job_id=@job AND plan_item_id=@item
""",connection);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("job",jobId);command.Parameters.AddWithValue("item",itemId);command.Parameters.AddWithValue("status",status);command.Parameters.AddWithValue("document",externalDocumentId??"");command.Parameters.AddWithValue("invite",externalInviteId??"");command.Parameters.AddWithValue("providerStatus",providerStatus??"");command.Parameters.AddWithValue("error",error??"");await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task RefreshJobAggregateAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using var command=new NpgsqlCommand("""
WITH current_plan AS (SELECT agreement_plan_id FROM public.jobs_staging WHERE tenant_id::text=@tenantText AND job_id=@job),
aggregate AS (SELECT COUNT(*) total,COUNT(*) FILTER(WHERE i.status IN ('invited','signed')) sent,COUNT(*) FILTER(WHERE i.status='signed') signed
 FROM public.job_agreement_items i JOIN current_plan p ON p.agreement_plan_id=i.plan_id)
UPDATE public.jobs_staging j SET terms_sent=(a.total>0 AND a.sent=a.total),terms_sent_at=CASE WHEN a.total>0 AND a.sent=a.total THEN COALESCE(j.terms_sent_at,NOW()) ELSE NULL END,
 terms_signed=(a.total>0 AND a.signed=a.total),terms_signed_at=CASE WHEN a.total>0 AND a.signed=a.total THEN COALESCE(j.terms_signed_at,NOW()) ELSE NULL END,updated_at=NOW()
FROM aggregate a WHERE j.tenant_id::text=@tenantText AND j.job_id=@job
""",connection);command.Parameters.AddWithValue("tenantText",tenantId.ToString("D"));command.Parameters.AddWithValue("job",jobId);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task RecordWebhookRegistrationAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,Guid itemId,bool success,string subscriptionId,string error,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using var command=new NpgsqlCommand("""
UPDATE public.job_agreement_items
SET webhook_status=@status,webhook_subscription_id=@subscription,webhook_last_error=@error,webhook_updated_at=NOW(),updated_at=NOW()
WHERE tenant_id=@tenant AND job_id=@job AND plan_item_id=@item
""",connection);
        command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("job",jobId);command.Parameters.AddWithValue("item",itemId);
        command.Parameters.AddWithValue("status",success?"active":"failed");command.Parameters.AddWithValue("subscription",subscriptionId??"");command.Parameters.AddWithValue("error",error??"");
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task RecordWebhookAsync(NpgsqlConnection connection,string provider,string eventKey,Guid? tenantId,string documentId,bool signatureValid,string payloadHash,string payloadJson,string error,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await using var command=new NpgsqlCommand("""
INSERT INTO public.agreement_provider_webhook_events(provider_key,event_key,tenant_id,external_document_id,signature_valid,payload_hash,payload_json,last_error)
VALUES(@provider,@event,@tenant,@document,@valid,@hash,CAST(@payload AS jsonb),@error)
ON CONFLICT(provider_key,event_key) DO NOTHING
""",connection);command.Parameters.AddWithValue("provider",NormalizeProvider(provider));command.Parameters.AddWithValue("event",eventKey);command.Parameters.AddWithValue("tenant",(object?)tenantId??DBNull.Value);command.Parameters.AddWithValue("document",documentId??"");command.Parameters.AddWithValue("valid",signatureValid);command.Parameters.AddWithValue("hash",payloadHash??"");command.Parameters.AddWithValue("payload",string.IsNullOrWhiteSpace(payloadJson)?"{}":payloadJson);command.Parameters.AddWithValue("error",error??"");await command.ExecuteNonQueryAsync(ct);
    }
}

public sealed class AgreementProviderMappingsSaveRequest{public Guid TenantId{get;set;}public bool Confirmed{get;set;}public List<AgreementProviderMappingInput> Mappings{get;set;}=[];}
public sealed class AgreementProviderMappingInput{public Guid ServiceId{get;set;}public string ServiceName{get;set;}="";public string TemplateId{get;set;}="";public string TemplateName{get;set;}="";public string SignerRole{get;set;}="";}
public sealed record AgreementProviderMappingView(Guid MappingId,Guid ServiceId,string ServiceName,string TemplateId,string TemplateName,string SignerRole,bool Active,int MappingVersion,DateTime UpdatedAt);
public sealed record AgreementProviderTemplate(string Id,string Name,IReadOnlyList<string> SignerRoles);
public sealed class AgreementProviderException(string code,string message):Exception(message){public string Code{get;}=code;}
