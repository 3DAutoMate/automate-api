using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace AutoMateApi;

public static class IntegrationHubSupport
{
    private static readonly HashSet<string> AvailableProviders=new(StringComparer.OrdinalIgnoreCase){"xero","signnow","adobe_sign","docusign","google_calendar","microsoft_calendar","microsoft_documents","google_drive","smtp"};
    public static readonly IReadOnlyDictionary<string,string[]> ProvidersByAction =
        new Dictionary<string,string[]>(StringComparer.OrdinalIgnoreCase)
        {
            ["accounting"]=["xero"],
            ["agreement_management"]=["signnow","adobe_sign","docusign"],
            ["calendar"]=["google_calendar","microsoft_calendar"],
            ["document_storage"]=["microsoft_documents","google_drive"],
            ["email_delivery"]=["smtp"]
        };

    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        const string sql="""
CREATE TABLE IF NOT EXISTS public.tenant_integration_settings
(
 tenant_id uuid PRIMARY KEY, settings_version integer NOT NULL DEFAULT 1,
 xero_invoice_mode text NOT NULL DEFAULT 'draft',
 xero_delivery_mode text NOT NULL DEFAULT 'review',
 xero_line_mode text NOT NULL DEFAULT 'itemised',
 xero_sales_account_code text NOT NULL DEFAULT '',
 xero_branding_theme_id text NOT NULL DEFAULT '',
 xero_branding_theme_name text NOT NULL DEFAULT '',
 updated_by text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT NOW(),
 CONSTRAINT ck_xero_invoice_mode CHECK(xero_invoice_mode IN ('draft','authorised')),
 CONSTRAINT ck_xero_delivery_mode CHECK(xero_delivery_mode IN ('review','send')),
 CONSTRAINT ck_xero_line_mode CHECK(xero_line_mode IN ('itemised','summary'))
);
ALTER TABLE public.tenant_integration_settings ADD COLUMN IF NOT EXISTS xero_branding_theme_id text NOT NULL DEFAULT '';
ALTER TABLE public.tenant_integration_settings ADD COLUMN IF NOT EXISTS xero_branding_theme_name text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS public.tenant_integration_action_defaults
(
 tenant_id uuid NOT NULL, action_type text NOT NULL, provider_key text NOT NULL,
 updated_by text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,action_type)
);
CREATE TABLE IF NOT EXISTS public.tenant_integration_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid NOT NULL,
 settings_version integer NOT NULL,actor text NOT NULL,detail_json jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.inspector_integrations
(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),inspector_id uuid NOT NULL,provider text NOT NULL,
 status text DEFAULT 'disconnected',access_token_encrypted text NULL,refresh_token_encrypted text NULL,
 expires_at timestamptz NULL,external_account_email text NULL,external_tenant_id text NULL,
 created_at timestamptz DEFAULT NOW(),updated_at timestamptz DEFAULT NOW(),
 CONSTRAINT uq_inspector_integrations_inspector_provider UNIQUE(inspector_id,provider)
);
""";
        await using var command=new NpgsqlCommand(sql,connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<IntegrationHubView> LoadAsync(NpgsqlConnection connection,Guid tenantId,Guid inspectorId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await SeedAsync(connection,tenantId,ct);
        IntegrationHubSettings settings;
        await using(var command=new NpgsqlCommand("SELECT settings_version,xero_invoice_mode,xero_delivery_mode,xero_line_mode,xero_sales_account_code,xero_branding_theme_id,xero_branding_theme_name,updated_at FROM public.tenant_integration_settings WHERE tenant_id=@tenant",connection))
        {
            command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);
            settings=new(reader.GetInt32(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5),reader.GetString(6),reader.GetDateTime(7));
        }
        var defaults=new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase);
        await using(var command=new NpgsqlCommand("SELECT action_type,provider_key FROM public.tenant_integration_action_defaults WHERE tenant_id=@tenant",connection))
        {
            command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);
            while(await reader.ReadAsync(ct))defaults[reader.GetString(0)]=reader.GetString(1);
        }
        var accounts=new Dictionary<string,IntegrationAccountView>(StringComparer.OrdinalIgnoreCase);
        await using(var command=new NpgsqlCommand("SELECT provider,status,external_account_email,external_tenant_id,expires_at,updated_at FROM public.inspector_integrations WHERE inspector_id IN (@inspector,@legacy) ORDER BY CASE WHEN inspector_id=@inspector THEN 0 ELSE 1 END",connection))
        {
            command.Parameters.AddWithValue("inspector",inspectorId);command.Parameters.AddWithValue("legacy",Guid.Empty);await using var reader=await command.ExecuteReaderAsync(ct);
            while(await reader.ReadAsync(ct))
            {
                var provider=ProviderKey(reader.GetString(0));if(accounts.ContainsKey(provider))continue;
                accounts[provider]=new(provider,reader.GetString(1),reader.IsDBNull(2)?"":reader.GetString(2),reader.IsDBNull(3)?"":reader.GetString(3),reader.IsDBNull(4)?null:reader.GetDateTime(4),reader.GetDateTime(5));
            }
        }
        var tenantAccounts=await ProviderIntegrationSupport.LoadSummariesAsync(connection,tenantId,ct);
        foreach(var pair in tenantAccounts)
        {
            var account=pair.Value;
            accounts[pair.Key]=new(pair.Key,account.Status,account.AccountEmail,account.ExternalAccountId,account.ExpiresAt,account.UpdatedAt);
        }
        var providers=new[]
        {
            Provider("xero","Xero","accounting",true,accounts),
            Provider("signnow","SignNow","agreement_management",true,accounts),
            Provider("adobe_sign","Adobe Acrobat Sign","agreement_management",true,accounts),
            Provider("docusign","DocuSign","agreement_management",true,accounts),
            Provider("google_calendar","Google Calendar","calendar",true,accounts),
            Provider("microsoft_calendar","Microsoft Calendar","calendar",true,accounts),
            Provider("microsoft_documents","Microsoft OneDrive / SharePoint","document_storage",true,accounts),
            Provider("google_drive","Google Drive","document_storage",true,accounts),
            new IntegrationProviderView("smtp","Company SMTP","email_delivery",true,"local","Configured in the Windows app; credentials never leave the workstation.","","",null,null)
        };
        return new(settings,defaults,providers);
    }

    public static async Task<IntegrationHubView> SaveAsync(NpgsqlConnection connection,Guid tenantId,Guid inspectorId,IntegrationHubSaveRequest request,string actor,CancellationToken ct=default)
    {
        if(!request.Confirmed)throw new IntegrationHubException("confirmation_required","Confirm the company integration settings.");
        await EnsureAsync(connection,ct);await SeedAsync(connection,tenantId,ct);
        var current=await LoadAsync(connection,tenantId,inspectorId,ct);
        if(request.ExpectedVersion!=current.Settings.Version)throw new IntegrationHubException("version_conflict","Integration settings changed elsewhere. Reload and review them.");
        var invoice=Normalize(request.XeroInvoiceMode,new[]{"draft","authorised"},"xero_invoice_mode");
        var delivery=Normalize(request.XeroDeliveryMode,new[]{"review","send"},"xero_delivery_mode");
        var lines=Normalize(request.XeroLineMode,new[]{"itemised","summary"},"xero_line_mode");
        if(invoice=="draft"&&delivery=="send")throw new IntegrationHubException("invalid_xero_delivery","A Draft invoice cannot be sent. Select Authorised invoice or Review in Xero.");
        var account=(request.XeroSalesAccountCode??"").Trim();
        if(account.Length>20||account.Any(ch=>!(char.IsLetterOrDigit(ch)||ch is '-' or '_')))throw new IntegrationHubException("invalid_sales_account","Enter a valid Xero Sales account code using letters, numbers, dash or underscore.");
        var brandingThemeId=(request.XeroBrandingThemeId??"").Trim();var brandingThemeName=(request.XeroBrandingThemeName??"").Trim();
        if(brandingThemeId.Length>0&&!Guid.TryParse(brandingThemeId,out _))throw new IntegrationHubException("invalid_branding_theme","Select a valid Xero branding theme.");
        if(brandingThemeName.Length>120)throw new IntegrationHubException("invalid_branding_theme","The Xero branding theme name is too long.");
        if(brandingThemeId.Length==0)brandingThemeName="";
        var requested=(request.Defaults??[]).ToDictionary(x=>(x.ActionType??"").Trim().ToLowerInvariant(),x=>(x.ProviderKey??"").Trim().ToLowerInvariant());
        foreach(var action in ProvidersByAction.Keys)
        {
            if(!requested.TryGetValue(action,out var provider))throw new IntegrationHubException("default_required",$"Select one default provider for {action.Replace('_',' ')}.");
            if(!ProvidersByAction[action].Contains(provider,StringComparer.OrdinalIgnoreCase))throw new IntegrationHubException("invalid_default",$"{provider} cannot be the default for {action.Replace('_',' ')}.");
            if(!AvailableProviders.Contains(provider))throw new IntegrationHubException("provider_unavailable",$"{provider} cannot be selected until its integration adapter is available.");
        }
        var next=current.Settings.Version+1;await using var tx=await connection.BeginTransactionAsync(ct);
        await using(var update=new NpgsqlCommand("UPDATE public.tenant_integration_settings SET settings_version=@version,xero_invoice_mode=@invoice,xero_delivery_mode=@delivery,xero_line_mode=@lines,xero_sales_account_code=@account,xero_branding_theme_id=@theme_id,xero_branding_theme_name=@theme_name,updated_by=@actor,updated_at=NOW() WHERE tenant_id=@tenant",connection,tx))
        {update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("version",next);update.Parameters.AddWithValue("invoice",invoice);update.Parameters.AddWithValue("delivery",delivery);update.Parameters.AddWithValue("lines",lines);update.Parameters.AddWithValue("account",account);update.Parameters.AddWithValue("theme_id",brandingThemeId);update.Parameters.AddWithValue("theme_name",brandingThemeName);update.Parameters.AddWithValue("actor",actor);await update.ExecuteNonQueryAsync(ct);}
        foreach(var pair in requested)
        {
            await using var command=new NpgsqlCommand("INSERT INTO public.tenant_integration_action_defaults(tenant_id,action_type,provider_key,updated_by) VALUES(@tenant,@action,@provider,@actor) ON CONFLICT(tenant_id,action_type) DO UPDATE SET provider_key=EXCLUDED.provider_key,updated_by=EXCLUDED.updated_by,updated_at=NOW()",connection,tx);
            command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("action",pair.Key);command.Parameters.AddWithValue("provider",pair.Value);command.Parameters.AddWithValue("actor",actor);await command.ExecuteNonQueryAsync(ct);
        }
        await using(var audit=new NpgsqlCommand("INSERT INTO public.tenant_integration_audit(tenant_id,settings_version,actor,detail_json) VALUES(@tenant,@version,@actor,@detail::jsonb)",connection,tx))
        {audit.Parameters.AddWithValue("tenant",tenantId);audit.Parameters.AddWithValue("version",next);audit.Parameters.AddWithValue("actor",actor);audit.Parameters.AddWithValue("detail",JsonSerializer.Serialize(new{defaults=requested,xero=new{invoice,delivery,lines,account,brandingThemeId,brandingThemeName}}));await audit.ExecuteNonQueryAsync(ct);}
        await tx.CommitAsync(ct);return await LoadAsync(connection,tenantId,inspectorId,ct);
    }

    public static async Task<XeroIntegrationSettings> LoadXeroAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await SeedAsync(connection,tenantId,ct);
        await using var command=new NpgsqlCommand("SELECT xero_invoice_mode,xero_delivery_mode,xero_line_mode,xero_sales_account_code,xero_branding_theme_id,xero_branding_theme_name FROM public.tenant_integration_settings WHERE tenant_id=@tenant",connection);
        command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);
        return new(reader.GetString(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5));
    }

    private static async Task SeedAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct)
    {
        await using(var command=new NpgsqlCommand("INSERT INTO public.tenant_integration_settings(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection)){command.Parameters.AddWithValue("tenant",tenantId);await command.ExecuteNonQueryAsync(ct);}
        var defaults=new Dictionary<string,string>{{"accounting","xero"},{"agreement_management","signnow"},{"calendar","google_calendar"},{"document_storage","microsoft_documents"},{"email_delivery","smtp"}};
        foreach(var pair in defaults){await using var command=new NpgsqlCommand("INSERT INTO public.tenant_integration_action_defaults(tenant_id,action_type,provider_key) VALUES(@tenant,@action,@provider) ON CONFLICT DO NOTHING",connection);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("action",pair.Key);command.Parameters.AddWithValue("provider",pair.Value);await command.ExecuteNonQueryAsync(ct);}
    }
    private static IntegrationProviderView Provider(string key,string name,string action,bool available,IDictionary<string,IntegrationAccountView> accounts)
    {
        accounts.TryGetValue(key,out var account);return new(key,name,action,available,account?.Status??"disconnected",available?"Connect and manage this provider.":"Planned provider — unavailable until its adapter is released.",account?.AccountEmail??"",account?.ExternalAccountId??"",account?.ExpiresAt,account?.UpdatedAt);
    }
    private static string ProviderKey(string provider)=>provider.ToLowerInvariant() switch{"google"=>"google_calendar","microsoft"=>"microsoft_documents",_=>provider.ToLowerInvariant()};
    private static string Normalize(string value,IEnumerable<string> allowed,string field){var result=(value??"").Trim().ToLowerInvariant();if(!allowed.Contains(result,StringComparer.OrdinalIgnoreCase))throw new IntegrationHubException("invalid_setting",$"Unsupported {field} value.");return result;}
}

public sealed class IntegrationHubSaveRequest
{
    public Guid TenantId{get;set;} public int ExpectedVersion{get;set;} public bool Confirmed{get;set;}
    public string XeroInvoiceMode{get;set;}="draft";public string XeroDeliveryMode{get;set;}="review";
    public string XeroLineMode{get;set;}="itemised";public string XeroSalesAccountCode{get;set;}="";
    public string XeroBrandingThemeId{get;set;}="";public string XeroBrandingThemeName{get;set;}="";
    public List<IntegrationDefaultInput> Defaults{get;set;}=[];
}
public sealed class IntegrationDefaultInput{public string ActionType{get;set;}="";public string ProviderKey{get;set;}="";}
public sealed record IntegrationHubSettings(int Version,string XeroInvoiceMode,string XeroDeliveryMode,string XeroLineMode,string XeroSalesAccountCode,string XeroBrandingThemeId,string XeroBrandingThemeName,DateTime UpdatedAt);
public sealed record IntegrationAccountView(string ProviderKey,string Status,string AccountEmail,string ExternalAccountId,DateTime? ExpiresAt,DateTime UpdatedAt);
public sealed record IntegrationProviderView(string ProviderKey,string Name,string ActionType,bool Available,string Status,string Description,string AccountEmail,string ExternalAccountId,DateTime? ExpiresAt,DateTime? UpdatedAt);
public sealed record IntegrationHubView(IntegrationHubSettings Settings,IReadOnlyDictionary<string,string> Defaults,IReadOnlyList<IntegrationProviderView> Providers);
public sealed record XeroIntegrationSettings(string InvoiceMode,string DeliveryMode,string LineMode,string SalesAccountCode,string BrandingThemeId,string BrandingThemeName);
public sealed class IntegrationHubException(string code,string message):Exception(message){public string Code{get;}=code;}
