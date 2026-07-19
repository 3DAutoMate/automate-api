using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

public static class TenantContactConfigurationSupport
{
    private static readonly JsonSerializerOptions JsonOptions=new(JsonSerializerDefaults.Web);
    public static async Task EnsureAsync(NpgsqlConnection connection,CancellationToken ct=default)
    {
        const string sql="""
CREATE TABLE IF NOT EXISTS public.tenant_contact_configuration_state
(
 tenant_id uuid PRIMARY KEY, configuration_version integer NOT NULL DEFAULT 0,
 confirmed_fingerprint text NOT NULL DEFAULT '', confirmed_slots_json jsonb NOT NULL DEFAULT '[]'::jsonb,
 discovered_roles_json jsonb NOT NULL DEFAULT '[]'::jsonb, pending_fingerprint text NOT NULL DEFAULT '',
 pending_slots_json jsonb NULL, review_required boolean NOT NULL DEFAULT false,
 billing_warning boolean NOT NULL DEFAULT false, ambiguous boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.tenant_recipient_bindings
(
 binding_id uuid PRIMARY KEY, tenant_id uuid NOT NULL, threed_role_id text NOT NULL DEFAULT '', normalized_label text NOT NULL,
 contact_label text NOT NULL, role_name text NOT NULL DEFAULT '', slot_index integer NOT NULL,
 active boolean NOT NULL DEFAULT true, archived boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT NOW(), updated_at timestamptz NOT NULL DEFAULT NOW(),
 UNIQUE(tenant_id,binding_id)
);
CREATE TABLE IF NOT EXISTS public.job_contacts
(
 tenant_id uuid NOT NULL, job_id uuid NOT NULL, contact_index integer NOT NULL, recipient_binding_id uuid NULL,
 threed_contact_id text NOT NULL DEFAULT '', threed_role_id text NOT NULL DEFAULT '', contact_label text NOT NULL DEFAULT '',
 display_name text NOT NULL DEFAULT '', salutation text NOT NULL DEFAULT '', first_name text NOT NULL DEFAULT '', last_name text NOT NULL DEFAULT '',
 person_display_name text NOT NULL DEFAULT '', company_id text NOT NULL DEFAULT '', company_name text NOT NULL DEFAULT '',
 address text NOT NULL DEFAULT '', city text NOT NULL DEFAULT '', state text NOT NULL DEFAULT '', postal_code text NOT NULL DEFAULT '',
 email text NOT NULL DEFAULT '', phone text NOT NULL DEFAULT '', configuration_version integer NOT NULL DEFAULT 0, synced_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,job_id,contact_index)
);
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS tenant_id uuid NULL;
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS contact_index integer NOT NULL DEFAULT 0;
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS recipient_binding_id uuid NULL;
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS threed_contact_id text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS threed_role_id text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS contact_label text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS role_name text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ALTER COLUMN role_name SET DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS display_name text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS salutation text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS first_name text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS last_name text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS person_display_name text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS company_id text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS company_name text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS address text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS city text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS state text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS postal_code text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS email text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS phone text NOT NULL DEFAULT '';
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS configuration_version integer NOT NULL DEFAULT 0;
ALTER TABLE public.job_contacts ADD COLUMN IF NOT EXISTS synced_at timestamptz NOT NULL DEFAULT NOW();
DO $$ BEGIN IF to_regclass('public.jobs_staging') IS NOT NULL THEN
 UPDATE public.job_contacts jc SET tenant_id=j.tenant_id::uuid FROM public.jobs_staging j
 WHERE jc.tenant_id IS NULL AND jc.job_id=j.job_id AND j.tenant_id::text ~* '^[0-9a-f-]{36}$';
END IF; END $$;
ALTER TABLE public.job_contacts DROP CONSTRAINT IF EXISTS job_contacts_job_id_fkey;
DO $$ BEGIN IF to_regclass('public.jobs_staging') IS NOT NULL AND NOT EXISTS
 (
  SELECT 1 FROM pg_constraint
  WHERE conname='job_contacts_jobs_staging_job_id_fkey'
    AND conrelid='public.job_contacts'::regclass
    AND confrelid='public.jobs_staging'::regclass
    AND contype='f'
 ) THEN
  ALTER TABLE public.job_contacts
   ADD CONSTRAINT job_contacts_jobs_staging_job_id_fkey
   FOREIGN KEY(job_id) REFERENCES public.jobs_staging(job_id) ON DELETE CASCADE NOT VALID;
 END IF; END $$;
DO $$ DECLARE missing_columns text; BEGIN
 SELECT string_agg(required.column_name,', ' ORDER BY required.column_name) INTO missing_columns
 FROM unnest(ARRAY['tenant_id','job_id','contact_index','recipient_binding_id','threed_contact_id','threed_role_id','contact_label','role_name','display_name','person_display_name','salutation','first_name','last_name','company_id','company_name','address','city','state','postal_code','email','phone','configuration_version','synced_at']) required(column_name)
 LEFT JOIN information_schema.columns actual ON actual.table_schema='public' AND actual.table_name='job_contacts' AND actual.column_name=required.column_name
 WHERE actual.column_name IS NULL;
 IF missing_columns IS NOT NULL THEN RAISE EXCEPTION 'job_contacts migration incomplete: %',missing_columns; END IF;
END $$;
CREATE TABLE IF NOT EXISTS public.tenant_contact_configuration_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, action_key text NOT NULL,
 configuration_version integer NOT NULL, actor text NOT NULL, detail_json jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.recipient_binding_basic_settings
(
 tenant_id uuid NOT NULL, recipient_binding_id uuid NOT NULL, event_key text NOT NULL,
 enabled boolean NOT NULL DEFAULT false, template_id uuid NULL, setting_version integer NOT NULL DEFAULT 1,
 updated_at timestamptz NOT NULL DEFAULT NOW(), PRIMARY KEY(tenant_id,recipient_binding_id,event_key),
 CONSTRAINT ck_recipient_binding_basic_event CHECK(event_key IN ('scheduling','rescheduling','cancellation','service_change'))
);
DO $$ BEGIN IF to_regclass('public.jobs_staging') IS NOT NULL THEN
 ALTER TABLE public.jobs_staging ADD COLUMN IF NOT EXISTS contact_configuration_version integer NOT NULL DEFAULT 0;
END IF; END $$;
""";
        await using var command=new NpgsqlCommand(sql,connection);await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<ContactConfigurationView> LoadAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct=default)
    {
        await EnsureAsync(connection,ct);await EnsureStateAsync(connection,tenantId,ct);
        const string sql="SELECT configuration_version,confirmed_fingerprint,confirmed_slots_json::text,discovered_roles_json::text,pending_fingerprint,pending_slots_json::text,review_required,billing_warning,ambiguous FROM public.tenant_contact_configuration_state WHERE tenant_id=@tenant";
        await using var command=new NpgsqlCommand(sql,connection);command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);await reader.ReadAsync(ct);
        return new(reader.GetInt32(0),reader.GetString(1),Slots(reader.GetString(2)),Roles(reader.GetString(3)),reader.GetString(4),reader.IsDBNull(5)?[]:Slots(reader.GetString(5)),reader.GetBoolean(6),reader.GetBoolean(7),reader.GetBoolean(8));
    }

    public static async Task<ContactConfigurationView> DiscoverAsync(NpgsqlConnection connection,Guid tenantId,ContactConfigurationDiscovery discovery,string actor,CancellationToken ct=default)
    {
        var slots=(discovery.Slots??[]).Where(x=>x.ContactIndex is >=0 and <=6).OrderBy(x=>x.ContactIndex).Select(x=>new ContactConfigurationSlot(x.ContactIndex,x.Enabled,(x.Label??"").Trim(),(x.RoleId??"").Trim(),(x.RoleName??"").Trim(),x.BindingId)).ToList();
        if(slots.Select(x=>x.ContactIndex).Distinct().Count()!=slots.Count)throw new InvalidOperationException("Duplicate THREED contact slot indices were supplied.");
        await EnsureAsync(connection,ct);await EnsureStateAsync(connection,tenantId,ct);var current=await LoadAsync(connection,tenantId,ct);
        var fingerprint=Fingerprint(slots.Where(x=>x.Enabled));var rolesJson=JsonSerializer.Serialize(discovery.Roles??[],JsonOptions);
        if(fingerprint==current.ConfirmedFingerprint||fingerprint==current.PendingFingerprint)
        {
            var source=fingerprint==current.ConfirmedFingerprint?current.ConfirmedSlots:current.PendingSlots;
            foreach(var slot in slots){var match=source.FirstOrDefault(x=>x.ContactIndex==slot.ContactIndex&&Key(x.RoleId,x.Label)==Key(slot.RoleId,slot.Label));if(match!=null)slot.BindingId=match.BindingId;}
            var column=fingerprint==current.ConfirmedFingerprint?"confirmed_slots_json":"pending_slots_json";
            await using var roles=new NpgsqlCommand("UPDATE public.tenant_contact_configuration_state SET discovered_roles_json=@roles::jsonb,"+column+"=@slots::jsonb WHERE tenant_id=@tenant",connection);roles.Parameters.AddWithValue("tenant",tenantId);roles.Parameters.AddWithValue("roles",rolesJson);roles.Parameters.AddWithValue("slots",JsonSerializer.Serialize(slots,JsonOptions));await roles.ExecuteNonQueryAsync(ct);return await LoadAsync(connection,tenantId,ct);
        }
        var pending=await BindAsync(connection,tenantId,slots,ct);var ambiguous=pending.GroupBy(x=>Key(x.RoleId,x.Label)).Any(x=>x.Key!="|"&&x.Count()>1);
        var billing=BillingChanged(current.ConfirmedSlots,pending);
        await using(var update=new NpgsqlCommand("UPDATE public.tenant_contact_configuration_state SET discovered_roles_json=@roles::jsonb,pending_fingerprint=@fingerprint,pending_slots_json=@slots::jsonb,review_required=true,billing_warning=@billing,ambiguous=@ambiguous,updated_at=NOW() WHERE tenant_id=@tenant",connection))
        {update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("roles",rolesJson);update.Parameters.AddWithValue("fingerprint",fingerprint);update.Parameters.AddWithValue("slots",JsonSerializer.Serialize(pending,JsonOptions));update.Parameters.AddWithValue("billing",billing);update.Parameters.AddWithValue("ambiguous",ambiguous);await update.ExecuteNonQueryAsync(ct);}
        await Audit(connection,tenantId,"contact_configuration_detected",current.Version,actor,new{fingerprint,billing,ambiguous},ct);return await LoadAsync(connection,tenantId,ct);
    }

    public static async Task<ContactConfigurationView> ConfirmAsync(NpgsqlConnection connection,Guid tenantId,bool billingConfirmed,string actor,CancellationToken ct=default)
    {
        var current=await LoadAsync(connection,tenantId,ct);if(!current.ReviewRequired)return current;if(current.Ambiguous)throw new InvalidOperationException("Recipient matching is ambiguous and must be resolved before confirmation.");if(current.BillingWarning&&!billingConfirmed)throw new InvalidOperationException("Confirm the THREED Slot 1 billing-recipient change.");var version=current.Version+1;
        await using var tx=await connection.BeginTransactionAsync(ct);
        await using(var archive=new NpgsqlCommand("UPDATE public.tenant_recipient_bindings SET active=false,archived=true,updated_at=NOW() WHERE tenant_id=@tenant",connection,tx)){archive.Parameters.AddWithValue("tenant",tenantId);await archive.ExecuteNonQueryAsync(ct);}
        foreach(var slot in current.PendingSlots.Where(x=>x.Enabled))await UpsertBinding(connection,tx,tenantId,slot,ct);
        foreach(var slot in current.PendingSlots.Where(x=>x.Enabled))await EnsureBasicSettings(connection,tx,tenantId,slot,ct);
        await SeedLegacyBasicSettings(connection,tx,tenantId,current.PendingSlots,ct);
        await using(var update=new NpgsqlCommand("UPDATE public.tenant_contact_configuration_state SET configuration_version=@version,confirmed_fingerprint=pending_fingerprint,confirmed_slots_json=pending_slots_json,pending_fingerprint='',pending_slots_json=NULL,review_required=false,billing_warning=false,ambiguous=false,updated_at=NOW() WHERE tenant_id=@tenant",connection,tx)){update.Parameters.AddWithValue("tenant",tenantId);update.Parameters.AddWithValue("version",version);await update.ExecuteNonQueryAsync(ct);}
        await tx.CommitAsync(ct);await Audit(connection,tenantId,"contact_configuration_confirmed",version,actor,new{version},ct);return await LoadAsync(connection,tenantId,ct);
    }

    public static async Task<ContactSchedulingGate> CheckSchedulingGateAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,CancellationToken ct=default)
    {
        var state=await LoadAsync(connection,tenantId,ct);if(state.ReviewRequired)return new(false,"contact_review_required",state.BillingWarning?"Confirm the changed THREED billing/contact configuration before scheduling.":"Review the changed THREED contact slots before scheduling.",state.Version,0);
        if(state.Version<=0)return new(false,"contact_configuration_required","Confirm Contacts & Recipients in Data Mapping before scheduling.",0,0);
        await using var command=new NpgsqlCommand("SELECT contact_configuration_version FROM public.jobs_staging WHERE tenant_id::text=@tenant AND job_id=@job",connection);command.Parameters.AddWithValue("tenant",tenantId.ToString());command.Parameters.AddWithValue("job",jobId);var value=await command.ExecuteScalarAsync(ct);var jobVersion=value==null||value==DBNull.Value?0:Convert.ToInt32(value);return jobVersion==state.Version?new(true,"ready","Contact configuration is current.",state.Version,jobVersion):new(false,"contact_resync_required","Re-sync this job after confirming the contact configuration.",state.Version,jobVersion);
    }

    public static async Task SyncJobContactsAsync(NpgsqlConnection connection,Guid tenantId,Guid jobId,IEnumerable<ContactFlat> contacts,CancellationToken ct=default)
    {
        var state=await LoadAsync(connection,tenantId,ct);var bindings=state.ConfirmedSlots.Where(x=>x.Enabled).ToDictionary(x=>x.ContactIndex);
        await using(var delete=new NpgsqlCommand("DELETE FROM public.job_contacts WHERE job_id=@job AND (tenant_id=@tenant OR tenant_id IS NULL)",connection)){delete.Parameters.AddWithValue("tenant",tenantId);delete.Parameters.AddWithValue("job",jobId);await delete.ExecuteNonQueryAsync(ct);}
        foreach(var contact in (contacts??[]).Where(x=>x.ContactIndex is >=0 and <=6))
        {ContactConfigurationSlot? slot;bindings.TryGetValue(contact.ContactIndex,out slot);await using var insert=new NpgsqlCommand("INSERT INTO public.job_contacts(tenant_id,job_id,contact_index,recipient_binding_id,threed_contact_id,threed_role_id,contact_label,role_name,display_name,person_display_name,salutation,first_name,last_name,company_id,company_name,address,city,state,postal_code,email,phone,configuration_version) VALUES(@tenant,@job,@index,@binding,@contact,@role,@label,@roleName,@display,@person,@salutation,@first,@last,@companyId,@company,@address,@city,@state,@postal,@email,@phone,@version)",connection);insert.Parameters.AddWithValue("tenant",tenantId);insert.Parameters.AddWithValue("job",jobId);insert.Parameters.AddWithValue("index",contact.ContactIndex);insert.Parameters.AddWithValue("binding",slot==null?(object)DBNull.Value:slot.BindingId);insert.Parameters.AddWithValue("contact",contact.ContactId??"");insert.Parameters.AddWithValue("role",contact.RoleId??slot?.RoleId??"");insert.Parameters.AddWithValue("label",contact.RoleLabel??slot?.Label??"");insert.Parameters.AddWithValue("roleName",slot?.RoleName??contact.RoleLabel??slot?.Label??"");insert.Parameters.AddWithValue("display",contact.DisplayName??"");insert.Parameters.AddWithValue("person",contact.PersonDisplayName??"");insert.Parameters.AddWithValue("salutation",contact.Salutation??"");insert.Parameters.AddWithValue("first",contact.FirstName??"");insert.Parameters.AddWithValue("last",contact.LastName??"");insert.Parameters.AddWithValue("companyId",contact.CompanyId??"");insert.Parameters.AddWithValue("company",contact.CompanyName??"");insert.Parameters.AddWithValue("address",contact.Address??"");insert.Parameters.AddWithValue("city",contact.City??"");insert.Parameters.AddWithValue("state",contact.State??"");insert.Parameters.AddWithValue("postal",contact.PostalCode??"");insert.Parameters.AddWithValue("email",contact.Email??"");insert.Parameters.AddWithValue("phone",contact.Cellular??"");insert.Parameters.AddWithValue("version",state.Version);await insert.ExecuteNonQueryAsync(ct);}
        await using var update=new NpgsqlCommand("UPDATE public.jobs_staging SET contact_configuration_version=@version WHERE tenant_id::text=@tenant AND job_id=@job",connection);update.Parameters.AddWithValue("version",state.Version);update.Parameters.AddWithValue("tenant",tenantId.ToString());update.Parameters.AddWithValue("job",jobId);await update.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<ContactConfigurationSlot>> BindAsync(NpgsqlConnection connection,Guid tenantId,List<ContactConfigurationSlot> slots,CancellationToken ct)
    {var existing=new List<ContactConfigurationSlot>();await using(var command=new NpgsqlCommand("SELECT binding_id,threed_role_id,contact_label,role_name,slot_index,active FROM public.tenant_recipient_bindings WHERE tenant_id=@tenant",connection)){command.Parameters.AddWithValue("tenant",tenantId);await using var reader=await command.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))existing.Add(new(reader.GetInt32(4),reader.GetBoolean(5),reader.GetString(2),reader.GetString(1),reader.GetString(3),reader.GetGuid(0)));}foreach(var slot in slots){var matches=existing.Where(x=>Key(x.RoleId,x.Label)==Key(slot.RoleId,slot.Label)).ToList();slot.BindingId=matches.Count==1?matches[0].BindingId:Guid.NewGuid();}return slots;}
    private static async Task UpsertBinding(NpgsqlConnection connection,NpgsqlTransaction tx,Guid tenantId,ContactConfigurationSlot slot,CancellationToken ct){await using var command=new NpgsqlCommand("INSERT INTO public.tenant_recipient_bindings(binding_id,tenant_id,threed_role_id,normalized_label,contact_label,role_name,slot_index,active,archived) VALUES(@id,@tenant,@role,@normalized,@label,@roleName,@slot,true,false) ON CONFLICT(binding_id) DO UPDATE SET threed_role_id=EXCLUDED.threed_role_id,normalized_label=EXCLUDED.normalized_label,contact_label=EXCLUDED.contact_label,role_name=EXCLUDED.role_name,slot_index=EXCLUDED.slot_index,active=true,archived=false,updated_at=NOW()",connection,tx);command.Parameters.AddWithValue("id",slot.BindingId);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("role",slot.RoleId??"");command.Parameters.AddWithValue("normalized",Normalize(slot.Label));command.Parameters.AddWithValue("label",slot.Label??"");command.Parameters.AddWithValue("roleName",slot.RoleName??"");command.Parameters.AddWithValue("slot",slot.ContactIndex);await command.ExecuteNonQueryAsync(ct);}
    private static async Task EnsureBasicSettings(NpgsqlConnection connection,NpgsqlTransaction tx,Guid tenantId,ContactConfigurationSlot slot,CancellationToken ct)
    {await using var command=new NpgsqlCommand("INSERT INTO public.recipient_binding_basic_settings(tenant_id,recipient_binding_id,event_key,enabled) SELECT @tenant,@binding,event_key,false FROM unnest(ARRAY['scheduling','rescheduling','cancellation','service_change']) event_key ON CONFLICT DO NOTHING",connection,tx);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("binding",slot.BindingId);await command.ExecuteNonQueryAsync(ct);}
    private static async Task SeedLegacyBasicSettings(NpgsqlConnection connection,NpgsqlTransaction tx,Guid tenantId,IEnumerable<ContactConfigurationSlot> slots,CancellationToken ct)
    {if(!await TableExists(connection,tx,"basic_automation_settings",ct))return;foreach(var slot in slots.Where(x=>x.Enabled&&x.ContactIndex<2)){await using var command=new NpgsqlCommand("UPDATE public.recipient_binding_basic_settings n SET enabled=l.enabled,template_id=l.template_id,setting_version=GREATEST(n.setting_version,l.setting_version),updated_at=NOW() FROM public.basic_automation_settings l WHERE n.tenant_id=@tenant AND n.recipient_binding_id=@binding AND l.tenant_id=n.tenant_id AND l.event_key=n.event_key AND l.recipient_key=@recipient",connection,tx);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("binding",slot.BindingId);command.Parameters.AddWithValue("recipient",slot.ContactIndex==0?"contact_1":"contact_2");await command.ExecuteNonQueryAsync(ct);}}
    private static async Task<bool> TableExists(NpgsqlConnection connection,NpgsqlTransaction tx,string table,CancellationToken ct){await using var command=new NpgsqlCommand("SELECT to_regclass('public.'||@table) IS NOT NULL",connection,tx);command.Parameters.AddWithValue("table",table);return Convert.ToBoolean(await command.ExecuteScalarAsync(ct));}
    private static async Task EnsureStateAsync(NpgsqlConnection connection,Guid tenantId,CancellationToken ct){await using var command=new NpgsqlCommand("INSERT INTO public.tenant_contact_configuration_state(tenant_id) VALUES(@tenant) ON CONFLICT DO NOTHING",connection);command.Parameters.AddWithValue("tenant",tenantId);await command.ExecuteNonQueryAsync(ct);}
    private static async Task Audit(NpgsqlConnection connection,Guid tenantId,string action,int version,string actor,object detail,CancellationToken ct){await using var command=new NpgsqlCommand("INSERT INTO public.tenant_contact_configuration_audit(tenant_id,action_key,configuration_version,actor,detail_json) VALUES(@tenant,@action,@version,@actor,@detail::jsonb)",connection);command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("action",action);command.Parameters.AddWithValue("version",version);command.Parameters.AddWithValue("actor",actor);command.Parameters.AddWithValue("detail",JsonSerializer.Serialize(detail,JsonOptions));await command.ExecuteNonQueryAsync(ct);}
    private static bool BillingChanged(IReadOnlyList<ContactConfigurationSlot> current,IReadOnlyList<ContactConfigurationSlot> pending){var a=current.FirstOrDefault(x=>x.ContactIndex==0);var b=pending.FirstOrDefault(x=>x.ContactIndex==0);return a==null||b==null||a.Enabled!=b.Enabled||Key(a.RoleId,a.Label)!=Key(b.RoleId,b.Label);}
    private static string Fingerprint(IEnumerable<ContactConfigurationSlot> slots){var text=string.Join("\n",slots.OrderBy(x=>x.ContactIndex).Select(x=>$"{x.ContactIndex}|{x.Enabled}|{Normalize(x.RoleId)}|{Normalize(x.Label)}|{RolePresent(x)}"));return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text))).ToLowerInvariant();}
    private static bool RolePresent(ContactConfigurationSlot slot){Guid id;return string.IsNullOrWhiteSpace(slot.RoleId)||(Guid.TryParse(slot.RoleId,out id)&&id==Guid.Empty)||!string.IsNullOrWhiteSpace(slot.RoleName);}
    private static string Key(string role,string label)=>Normalize(role)+"|"+Normalize(label);private static string Normalize(string value)=>new((value??"").Where(char.IsLetterOrDigit).Select(char.ToLowerInvariant).ToArray());
    private static List<ContactConfigurationSlot> Slots(string json)=>JsonSerializer.Deserialize<List<ContactConfigurationSlot>>(json,JsonOptions)??[];private static List<ContactConfigurationRole> Roles(string json)=>JsonSerializer.Deserialize<List<ContactConfigurationRole>>(json,JsonOptions)??[];
}

public sealed class ContactConfigurationSlot
{
    public int ContactIndex{get;set;} public bool Enabled{get;set;} public string Label{get;set;}=""; public string RoleId{get;set;}=""; public string RoleName{get;set;}=""; public Guid BindingId{get;set;}
    public ContactConfigurationSlot(){}
    public ContactConfigurationSlot(int contactIndex,bool enabled,string label,string roleId,string roleName,Guid bindingId=default){ContactIndex=contactIndex;Enabled=enabled;Label=label;RoleId=roleId;RoleName=roleName;BindingId=bindingId;}
}
public sealed class ContactConfigurationRole
{
    public string RoleId{get;set;}="";public string Name{get;set;}="";public bool AssignedToEnabledSlot{get;set;}
    public ContactConfigurationRole(){}
    public ContactConfigurationRole(string roleId,string name,bool assignedToEnabledSlot){RoleId=roleId;Name=name;AssignedToEnabledSlot=assignedToEnabledSlot;}
}
public sealed class ContactConfigurationDiscovery
{
    public List<ContactConfigurationSlot> Slots{get;set;}=[];public List<ContactConfigurationRole> Roles{get;set;}=[];
}
public sealed record ContactConfigurationView(int Version,string ConfirmedFingerprint,List<ContactConfigurationSlot> ConfirmedSlots,List<ContactConfigurationRole> Roles,string PendingFingerprint,List<ContactConfigurationSlot> PendingSlots,bool ReviewRequired,bool BillingWarning,bool Ambiguous);
public sealed record ContactSchedulingGate(bool Allowed,string Status,string Message,int ConfigurationVersion,int JobVersion);
