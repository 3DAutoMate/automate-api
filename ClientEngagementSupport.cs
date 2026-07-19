using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace AutoMateApi;

/// <summary>
/// Persistence primitives for immutable client inspection pages and privacy-reduced
/// engagement evidence. This type does not send email, expose HTTP endpoints, update
/// workflow completion, or call Google Calendar.
/// </summary>
public static class ClientEngagementSupport
{
    public const int DefaultTokenBytes = 32;

    public static string SchemaSql => """
        CREATE TABLE IF NOT EXISTS public.client_engagement_settings
        (
            tenant_id uuid PRIMARY KEY,
            page_enabled boolean NOT NULL DEFAULT false,
            pixel_enabled boolean NOT NULL DEFAULT false,
            version integer NOT NULL DEFAULT 1,
            updated_by text NOT NULL DEFAULT '',
            updated_at timestamptz NOT NULL DEFAULT NOW()
        );
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS introduction_text text NOT NULL DEFAULT 'Hello {{CLIENT_SALUTATION}}. Here are the approved details for your inspection.';
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS payment_instruction text NOT NULL DEFAULT 'Your invoice will be sent to {{CLIENT_EMAIL}}. Payment is required to secure your booking time.';
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS bank_account_name text NOT NULL DEFAULT '';
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS bank_account_number text NOT NULL DEFAULT '';
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS payment_reference_instruction text NOT NULL DEFAULT '';
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS show_bank_with_accounting boolean NOT NULL DEFAULT false;
        ALTER TABLE public.client_engagement_settings ADD COLUMN IF NOT EXISTS brand_colour text NOT NULL DEFAULT '#0b5f86';

        CREATE TABLE IF NOT EXISTS public.client_engagement_setting_commands
        (
            tenant_id uuid NOT NULL,
            idempotency_key text NOT NULL,
            request_hash text NOT NULL,
            result_json jsonb NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            completed_at timestamptz NULL,
            PRIMARY KEY(tenant_id,idempotency_key)
        );

        CREATE TABLE IF NOT EXISTS public.client_engagement_setting_audit
        (
            audit_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            previous_page_enabled boolean NOT NULL,
            new_page_enabled boolean NOT NULL,
            previous_pixel_enabled boolean NOT NULL,
            new_pixel_enabled boolean NOT NULL,
            setting_version integer NOT NULL,
            changed_by text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS public.client_inspection_pages
        (
            publication_id uuid PRIMARY KEY,
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            approved_version integer NOT NULL,
            snapshot_fingerprint text NOT NULL,
            snapshot_json jsonb NOT NULL,
            published_by text NOT NULL,
            published_at timestamptz NOT NULL DEFAULT NOW(),
            revoked_at timestamptz NULL,
            revoked_by text NOT NULL DEFAULT '',
            revoke_reason text NOT NULL DEFAULT '',
            UNIQUE(tenant_id,job_id,approved_version,snapshot_fingerprint)
        );

        CREATE INDEX IF NOT EXISTS idx_client_publications_job
            ON public.client_inspection_pages(tenant_id,job_id,published_at DESC);

        CREATE TABLE IF NOT EXISTS public.email_communications
        (
            communication_id uuid PRIMARY KEY,
            publication_id uuid NOT NULL REFERENCES public.client_inspection_pages(publication_id),
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            recipient_key text NOT NULL,
            recipient_address_hash text NOT NULL,
            purpose text NOT NULL,
            idempotency_key text NOT NULL,
            token_hash text NOT NULL UNIQUE,
            issued_by text NOT NULL,
            issued_at timestamptz NOT NULL DEFAULT NOW(),
            expires_at timestamptz NOT NULL,
            revoked_at timestamptz NULL,
            revoked_by text NOT NULL DEFAULT '',
            revoke_reason text NOT NULL DEFAULT '',
            confirmed_at timestamptz NULL,
            calendar_requested_at timestamptz NULL,
            delivery_state text NOT NULL DEFAULT 'queued',
            accepted_at timestamptz NULL,
            failed_at timestamptz NULL,
            provider text NOT NULL DEFAULT '',
            connector_version text NOT NULL DEFAULT '',
            redacted_error text NOT NULL DEFAULT '',
            UNIQUE(tenant_id,job_id,purpose,recipient_key,idempotency_key)
        );

        ALTER TABLE public.email_communications ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'queued';
        ALTER TABLE public.email_communications ADD COLUMN IF NOT EXISTS accepted_at timestamptz NULL;
        ALTER TABLE public.email_communications ADD COLUMN IF NOT EXISTS failed_at timestamptz NULL;
        ALTER TABLE public.email_communications ADD COLUMN IF NOT EXISTS provider text NOT NULL DEFAULT '';
        ALTER TABLE public.email_communications ADD COLUMN IF NOT EXISTS connector_version text NOT NULL DEFAULT '';
        ALTER TABLE public.email_communications ADD COLUMN IF NOT EXISTS redacted_error text NOT NULL DEFAULT '';
        ALTER TABLE public.email_communications DROP CONSTRAINT IF EXISTS ck_email_communication_delivery_state;
        ALTER TABLE public.email_communications ADD CONSTRAINT ck_email_communication_delivery_state
            CHECK(delivery_state IN ('queued','smtp_accepted','failed'));

        CREATE INDEX IF NOT EXISTS idx_client_communications_job
            ON public.email_communications(tenant_id,job_id,issued_at DESC);

        CREATE TABLE IF NOT EXISTS public.email_engagement_events
        (
            event_id uuid PRIMARY KEY,
            communication_id uuid NOT NULL REFERENCES public.email_communications(communication_id),
            tenant_id uuid NOT NULL,
            job_id uuid NOT NULL,
            event_type text NOT NULL,
            event_key text NOT NULL,
            occurred_at timestamptz NOT NULL DEFAULT NOW(),
            ip_network_hash text NOT NULL DEFAULT '',
            user_agent_family text NOT NULL DEFAULT '',
            referrer_origin text NOT NULL DEFAULT '',
            metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(communication_id,event_type,event_key),
            CONSTRAINT ck_client_engagement_event CHECK
                (event_type IN ('pixel','view','confirm','calendar'))
        );

        CREATE INDEX IF NOT EXISTS idx_client_engagement_job
            ON public.email_engagement_events(tenant_id,job_id,occurred_at DESC);
        """;

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        await using var command = new NpgsqlCommand(SchemaSql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Generates a URL-safe bearer secret. Only its hash should be persisted.</summary>
    public static ClientPageToken CreateToken(string purpose, string pepper, int byteCount = DefaultTokenBytes)
    {
        if (byteCount < 32) throw new ArgumentOutOfRangeException(nameof(byteCount), "Tokens must contain at least 256 bits.");
        if (string.IsNullOrWhiteSpace(purpose)) throw new ArgumentException("Token purpose is required.", nameof(purpose));
        if (string.IsNullOrWhiteSpace(pepper)) throw new ArgumentException("A server-side token pepper is required.", nameof(pepper));
        var secret = Base64Url(RandomNumberGenerator.GetBytes(byteCount));
        return new(secret, HashToken(secret, purpose, pepper));
    }

    public static string HashToken(string secret, string purpose, string pepper)
    {
        if (string.IsNullOrWhiteSpace(secret) || string.IsNullOrWhiteSpace(purpose) || string.IsNullOrWhiteSpace(pepper))
            throw new ArgumentException("Token, purpose and pepper are required.");
        return Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(pepper),
            Encoding.UTF8.GetBytes($"automate-client-page:v1:{purpose.Trim().ToLowerInvariant()}:{secret}"))).ToLowerInvariant();
    }

    public static async Task<ClientEngagementSettings> LoadSettingsAsync(NpgsqlConnection connection, Guid tenantId,
        CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty) throw new ArgumentException("Tenant ID is required.");
        await EnsureAsync(connection, cancellationToken);
        await using (var ensure = new NpgsqlCommand("""
            INSERT INTO public.client_engagement_settings(tenant_id) VALUES(@tenant)
            ON CONFLICT(tenant_id) DO NOTHING;
            """, connection))
        {
            ensure.Parameters.AddWithValue("tenant", tenantId);
            await ensure.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var command = new NpgsqlCommand("""
            SELECT page_enabled,pixel_enabled,version,updated_at,introduction_text,payment_instruction,
                   bank_account_name,bank_account_number,payment_reference_instruction,show_bank_with_accounting,brand_colour
            FROM public.client_engagement_settings WHERE tenant_id=@tenant;
            """, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new(tenantId,reader.GetBoolean(0),reader.GetBoolean(1),reader.GetInt32(2),reader.GetDateTime(3),reader.GetString(4),reader.GetString(5),reader.GetString(6),reader.GetString(7),reader.GetString(8),reader.GetBoolean(9),reader.GetString(10));
    }

    public static async Task<ClientEngagementSettingsSaveResult> SaveSettingsAsync(NpgsqlConnection connection,
        SaveClientEngagementSettingsCommand request, CancellationToken cancellationToken = default)
    {
        if (request.TenantId == Guid.Empty || string.IsNullOrWhiteSpace(request.Actor))
            throw new ArgumentException("Tenant and authenticated actor are required.");
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey) || request.IdempotencyKey.Length > 200)
            throw new ArgumentException("A bounded idempotency key is required.");
        if (!request.Confirmed) return new("confirmation_required", null, "Explicit confirmation is required.");
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var requestHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            $"{request.TenantId:N}|{request.PageEnabled}|{request.PixelEnabled}|{request.ExpectedVersion}|{request.IntroductionText}|{request.PaymentInstruction}|{request.BankAccountName}|{request.BankAccountNumber}|{request.PaymentReferenceInstruction}|{request.ShowBankWithAccounting}|{request.BrandColour}"))).ToLowerInvariant();
        await using (var claim = new NpgsqlCommand("""
            INSERT INTO public.client_engagement_setting_commands(tenant_id,idempotency_key,request_hash)
            VALUES(@tenant,@key,@hash) ON CONFLICT DO NOTHING;
            """, connection, transaction))
        {
            claim.Parameters.AddWithValue("tenant", request.TenantId);
            claim.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
            claim.Parameters.AddWithValue("hash", requestHash);
            if (await claim.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                await using var existing = new NpgsqlCommand("""
                    SELECT request_hash,COALESCE(result_json::text,'')
                    FROM public.client_engagement_setting_commands
                    WHERE tenant_id=@tenant AND idempotency_key=@key FOR UPDATE;
                    """, connection, transaction);
                existing.Parameters.AddWithValue("tenant", request.TenantId);
                existing.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
                await using var existingReader = await existing.ExecuteReaderAsync(cancellationToken);
                await existingReader.ReadAsync(cancellationToken);
                var existingHash = existingReader.GetString(0);
                var existingJson = existingReader.GetString(1);
                if (existingHash != requestHash) throw new InvalidOperationException("The idempotency key was used for different settings.");
                if (string.IsNullOrWhiteSpace(existingJson)) throw new InvalidOperationException("The same settings command is already processing.");
                var replay = JsonSerializer.Deserialize<ClientEngagementSettingsSaveResult>(existingJson)!;
                await existingReader.DisposeAsync();
                await transaction.CommitAsync(cancellationToken);
                return replay with { Status = "replayed" };
            }
        }
        await using (var ensure = new NpgsqlCommand("""
            INSERT INTO public.client_engagement_settings(tenant_id) VALUES(@tenant)
            ON CONFLICT(tenant_id) DO NOTHING;
            """, connection, transaction))
        {
            ensure.Parameters.AddWithValue("tenant", request.TenantId);
            await ensure.ExecuteNonQueryAsync(cancellationToken);
        }
        bool previousPage;
        bool previousPixel;
        int currentVersion;
        await using (var current = new NpgsqlCommand("""
            SELECT page_enabled,pixel_enabled,version FROM public.client_engagement_settings
            WHERE tenant_id=@tenant FOR UPDATE;
            """, connection, transaction))
        {
            current.Parameters.AddWithValue("tenant", request.TenantId);
            await using var currentReader = await current.ExecuteReaderAsync(cancellationToken);
            await currentReader.ReadAsync(cancellationToken);
            previousPage = currentReader.GetBoolean(0);
            previousPixel = currentReader.GetBoolean(1);
            currentVersion = currentReader.GetInt32(2);
        }
        ClientEngagementSettingsSaveResult result;
        if (currentVersion != request.ExpectedVersion)
        {
            result = new("conflict", await LoadSettingsAsync(connection,request.TenantId,cancellationToken),
                "Engagement settings changed. Reload and try again.");
        }
        else
        {
        const string sql = """
            UPDATE public.client_engagement_settings
            SET page_enabled=@page,pixel_enabled=@pixel,introduction_text=@intro,payment_instruction=@payment,
                bank_account_name=@bank_name,bank_account_number=@bank_number,payment_reference_instruction=@reference,
                show_bank_with_accounting=@show_bank,brand_colour=@brand,version=version+1,updated_by=@actor,updated_at=NOW()
            WHERE tenant_id=@tenant AND version=@expected
            RETURNING page_enabled,pixel_enabled,version,updated_at;
            """;
        await using (var command = new NpgsqlCommand(sql, connection, transaction))
        {
            command.Parameters.AddWithValue("page", request.PageEnabled);
            command.Parameters.AddWithValue("pixel", request.PixelEnabled);
            command.Parameters.AddWithValue("intro", CleanSetting(request.IntroductionText,500));
            command.Parameters.AddWithValue("payment", CleanSetting(request.PaymentInstruction,500));
            command.Parameters.AddWithValue("bank_name", CleanSetting(request.BankAccountName,120));
            command.Parameters.AddWithValue("bank_number", CleanSetting(request.BankAccountNumber,80));
            command.Parameters.AddWithValue("reference", CleanSetting(request.PaymentReferenceInstruction,200));
            command.Parameters.AddWithValue("show_bank",request.ShowBankWithAccounting);
            command.Parameters.AddWithValue("brand",ValidBrand(request.BrandColour));
            command.Parameters.AddWithValue("actor", request.Actor.Trim());
            command.Parameters.AddWithValue("tenant", request.TenantId);
            command.Parameters.AddWithValue("expected", request.ExpectedVersion);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            var settings=new ClientEngagementSettings(request.TenantId,reader.GetBoolean(0),reader.GetBoolean(1),reader.GetInt32(2),reader.GetDateTime(3),CleanSetting(request.IntroductionText,500),CleanSetting(request.PaymentInstruction,500),CleanSetting(request.BankAccountName,120),CleanSetting(request.BankAccountNumber,80),CleanSetting(request.PaymentReferenceInstruction,200),request.ShowBankWithAccounting,ValidBrand(request.BrandColour));
            result = new("saved", settings, "Client engagement settings saved.");
        }
        await using var audit = new NpgsqlCommand("""
            INSERT INTO public.client_engagement_setting_audit
                (audit_id,tenant_id,previous_page_enabled,new_page_enabled,previous_pixel_enabled,
                 new_pixel_enabled,setting_version,changed_by)
            VALUES(@id,@tenant,@previous_page,@new_page,@previous_pixel,@new_pixel,@version,@actor);
            """, connection, transaction);
        audit.Parameters.AddWithValue("id", Guid.NewGuid());
        audit.Parameters.AddWithValue("tenant", request.TenantId);
        audit.Parameters.AddWithValue("previous_page", previousPage);
        audit.Parameters.AddWithValue("new_page", request.PageEnabled);
        audit.Parameters.AddWithValue("previous_pixel", previousPixel);
        audit.Parameters.AddWithValue("new_pixel", request.PixelEnabled);
        audit.Parameters.AddWithValue("version", result.Settings!.Version);
        audit.Parameters.AddWithValue("actor", request.Actor.Trim());
        await audit.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var complete = new NpgsqlCommand("""
            UPDATE public.client_engagement_setting_commands
            SET result_json=CAST(@result AS jsonb),completed_at=NOW()
            WHERE tenant_id=@tenant AND idempotency_key=@key AND request_hash=@hash;
            """, connection, transaction))
        {
            complete.Parameters.AddWithValue("result", JsonSerializer.Serialize(result));
            complete.Parameters.AddWithValue("tenant", request.TenantId);
            complete.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
            complete.Parameters.AddWithValue("hash", requestHash);
            await complete.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    /// <summary>Copies the current approved snapshot into an immutable publication.</summary>
    public static async Task<ClientPagePublicationResult> PublishApprovedSnapshotAsync(NpgsqlConnection connection,
        PublishClientPageCommand request, CancellationToken cancellationToken = default)
    {
        ValidateOwnedInput(request.TenantId, request.JobId, request.Actor);
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string readSql = """
            SELECT approved_snapshot_version,COALESCE(approved_snapshot_fingerprint,''),
                   COALESCE(approved_snapshot_json::text,'')
            FROM public.jobs_staging
            WHERE job_id=@job AND tenant_id::text=@tenant_text
            FOR UPDATE;
            """;
        int version;
        string fingerprint;
        string snapshot;
        await using (var read = new NpgsqlCommand(readSql, connection, transaction))
        {
            read.Parameters.AddWithValue("job", request.JobId);
            read.Parameters.Add("tenant_text", NpgsqlDbType.Text).Value = request.TenantId.ToString();
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new UnauthorizedAccessException("The job does not belong to this tenant.");
            version = reader.GetInt32(0);
            fingerprint = reader.GetString(1);
            snapshot = ProjectClientSnapshot(reader.GetString(2));
        }
        if (version < 1 || string.IsNullOrWhiteSpace(fingerprint) || string.IsNullOrWhiteSpace(snapshot))
            throw new InvalidOperationException("The job has no approved snapshot to publish.");

        var publicationId = Guid.NewGuid();
        const string insertSql = """
            INSERT INTO public.client_inspection_pages
                (publication_id,tenant_id,job_id,approved_version,snapshot_fingerprint,snapshot_json,published_by)
            VALUES(@id,@tenant,@job,@version,@fingerprint,CAST(@snapshot AS jsonb),@actor)
            ON CONFLICT(tenant_id,job_id,approved_version,snapshot_fingerprint) DO NOTHING
            RETURNING publication_id,published_at;
            """;
        DateTime publishedAt;
        var replayed = false;
        await using (var insert = new NpgsqlCommand(insertSql, connection, transaction))
        {
            insert.Parameters.AddWithValue("id", publicationId);
            insert.Parameters.AddWithValue("tenant", request.TenantId);
            insert.Parameters.AddWithValue("job", request.JobId);
            insert.Parameters.AddWithValue("version", version);
            insert.Parameters.AddWithValue("fingerprint", fingerprint);
            insert.Parameters.AddWithValue("snapshot", snapshot);
            insert.Parameters.AddWithValue("actor", request.Actor.Trim());
            await using var reader = await insert.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) publishedAt = reader.GetDateTime(1);
            else
            {
                await reader.DisposeAsync();
                await using var existing = new NpgsqlCommand("""
                    SELECT publication_id,published_at FROM public.client_inspection_pages
                    WHERE tenant_id=@tenant AND job_id=@job AND approved_version=@version
                      AND snapshot_fingerprint=@fingerprint;
                    """, connection, transaction);
                existing.Parameters.AddWithValue("tenant", request.TenantId);
                existing.Parameters.AddWithValue("job", request.JobId);
                existing.Parameters.AddWithValue("version", version);
                existing.Parameters.AddWithValue("fingerprint", fingerprint);
                await using var existingReader = await existing.ExecuteReaderAsync(cancellationToken);
                await existingReader.ReadAsync(cancellationToken);
                publicationId = existingReader.GetGuid(0);
                publishedAt = existingReader.GetDateTime(1);
                replayed = true;
            }
        }
        // Revocation disables the old bearer links, not the immutable approved snapshot.
        // A later explicit publish for the same approved revision must reactivate that
        // publication so a newly issued, independently hashed token can use it.
        await using (var reactivate = new NpgsqlCommand("""
            UPDATE public.client_inspection_pages
            SET revoked_at=NULL,revoked_by='',revoke_reason='',published_by=@actor
            WHERE publication_id=@current AND tenant_id=@tenant AND job_id=@job;
            """, connection, transaction))
        {
            reactivate.Parameters.AddWithValue("actor", request.Actor.Trim());
            reactivate.Parameters.AddWithValue("current", publicationId);
            reactivate.Parameters.AddWithValue("tenant", request.TenantId);
            reactivate.Parameters.AddWithValue("job", request.JobId);
            await reactivate.ExecuteNonQueryAsync(cancellationToken);
        }
        if (request.RevokePrior)
        {
            await using var revokePublications = new NpgsqlCommand("""
                UPDATE public.client_inspection_pages
                SET revoked_at=NOW(),revoked_by=@actor,revoke_reason='superseded approved snapshot'
                WHERE tenant_id=@tenant AND job_id=@job AND publication_id<>@current AND revoked_at IS NULL;
                """, connection, transaction);
            revokePublications.Parameters.AddWithValue("actor", request.Actor.Trim());
            revokePublications.Parameters.AddWithValue("tenant", request.TenantId);
            revokePublications.Parameters.AddWithValue("job", request.JobId);
            revokePublications.Parameters.AddWithValue("current", publicationId);
            await revokePublications.ExecuteNonQueryAsync(cancellationToken);

            await using var revokeCommunications = new NpgsqlCommand("""
                UPDATE public.email_communications
                SET revoked_at=NOW(),revoked_by=@actor,revoke_reason='superseded approved snapshot'
                WHERE tenant_id=@tenant AND job_id=@job AND publication_id<>@current AND revoked_at IS NULL;
                """, connection, transaction);
            revokeCommunications.Parameters.AddWithValue("actor", request.Actor.Trim());
            revokeCommunications.Parameters.AddWithValue("tenant", request.TenantId);
            revokeCommunications.Parameters.AddWithValue("job", request.JobId);
            revokeCommunications.Parameters.AddWithValue("current", publicationId);
            await revokeCommunications.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return new(publicationId, request.TenantId, request.JobId, version, fingerprint, publishedAt, replayed);
    }

    /// <summary>
    /// Issues an access token record idempotently. The raw token is supplied by the caller and never stored.
    /// A retry must reuse the same raw token or it receives an idempotency conflict.
    /// </summary>
    public static async Task<ClientCommunicationIssueResult> IssueCommunicationAsync(NpgsqlConnection connection,
        IssueClientCommunicationCommand request, string tokenPepper, CancellationToken cancellationToken = default)
    {
        ValidateOwnedInput(request.TenantId, request.JobId, request.Actor);
        if (request.PublicationId == Guid.Empty) throw new ArgumentException("Publication ID is required.");
        if (string.IsNullOrWhiteSpace(request.RecipientKey) || string.IsNullOrWhiteSpace(request.RecipientAddress))
            throw new ArgumentException("Recipient key and address are required.");
        var recipientKey = request.RecipientKey.Trim().ToLowerInvariant();
        if (recipientKey != "contact_1")
            throw new InvalidOperationException("Client inspection pages may only be issued to THREED Contact 1.");
        if (request.IsTest || request.IsPreview || request.Subject.TrimStart().StartsWith("[TEST]", StringComparison.OrdinalIgnoreCase) ||
            request.Subject.TrimStart().StartsWith("[AUTOMATION TEST]", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Test and preview communications cannot issue client inspection-page access.");
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey) || request.IdempotencyKey.Length > 200)
            throw new ArgumentException("A valid idempotency key is required.");
        if (request.ExpiresAt <= DateTime.UtcNow) throw new ArgumentException("Expiry must be in the future.");
        var purpose = NormalizePurpose(request.Purpose);
        var tokenHash = HashToken(request.RawToken, purpose, tokenPepper);
        var addressHash = PrivacyHash(request.RecipientAddress.Trim().ToLowerInvariant(), tokenPepper);

        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string publicationSql = """
            SELECT 1 FROM public.client_inspection_pages p
            JOIN public.client_engagement_settings s ON s.tenant_id=p.tenant_id
                AND (s.page_enabled=true OR s.pixel_enabled=true)
            WHERE p.publication_id=@publication AND p.tenant_id=@tenant AND p.job_id=@job AND p.revoked_at IS NULL;
            """;
        await using (var publication = new NpgsqlCommand(publicationSql, connection, transaction))
        {
            publication.Parameters.AddWithValue("publication", request.PublicationId);
            publication.Parameters.AddWithValue("tenant", request.TenantId);
            publication.Parameters.AddWithValue("job", request.JobId);
            if (await publication.ExecuteScalarAsync(cancellationToken) is null)
                throw new UnauthorizedAccessException("The active publication does not belong to this job and tenant.");
        }

        var id = Guid.NewGuid();
        const string insertSql = """
            INSERT INTO public.email_communications
                (communication_id,publication_id,tenant_id,job_id,recipient_key,recipient_address_hash,
                 purpose,idempotency_key,token_hash,issued_by,expires_at)
            VALUES(@id,@publication,@tenant,@job,@recipient,@address_hash,@purpose,@key,@token_hash,@actor,@expires)
            ON CONFLICT(tenant_id,job_id,purpose,recipient_key,idempotency_key) DO NOTHING
            RETURNING communication_id,issued_at;
            """;
        DateTime issuedAt;
        var replayed = false;
        await using (var insert = new NpgsqlCommand(insertSql, connection, transaction))
        {
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("publication", request.PublicationId);
            insert.Parameters.AddWithValue("tenant", request.TenantId);
            insert.Parameters.AddWithValue("job", request.JobId);
            insert.Parameters.AddWithValue("recipient", recipientKey);
            insert.Parameters.AddWithValue("address_hash", addressHash);
            insert.Parameters.AddWithValue("purpose", purpose);
            insert.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
            insert.Parameters.AddWithValue("token_hash", tokenHash);
            insert.Parameters.AddWithValue("actor", request.Actor.Trim());
            insert.Parameters.AddWithValue("expires", request.ExpiresAt.ToUniversalTime());
            await using var reader = await insert.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) issuedAt = reader.GetDateTime(1);
            else
            {
                await reader.DisposeAsync();
                await using var existing = new NpgsqlCommand("""
                    SELECT communication_id,issued_at,token_hash,publication_id,expires_at
                    FROM public.email_communications
                    WHERE tenant_id=@tenant AND job_id=@job AND purpose=@purpose
                      AND recipient_key=@recipient AND idempotency_key=@key FOR UPDATE;
                    """, connection, transaction);
                existing.Parameters.AddWithValue("tenant", request.TenantId);
                existing.Parameters.AddWithValue("job", request.JobId);
                existing.Parameters.AddWithValue("purpose", purpose);
                existing.Parameters.AddWithValue("recipient", recipientKey);
                existing.Parameters.AddWithValue("key", request.IdempotencyKey.Trim());
                await using var existingReader = await existing.ExecuteReaderAsync(cancellationToken);
                await existingReader.ReadAsync(cancellationToken);
                if (!CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(existingReader.GetString(2)), Encoding.ASCII.GetBytes(tokenHash)) ||
                    existingReader.GetGuid(3) != request.PublicationId || existingReader.GetDateTime(4) != request.ExpiresAt.ToUniversalTime())
                    throw new InvalidOperationException("The idempotency key was already used with different communication content.");
                id = existingReader.GetGuid(0);
                issuedAt = existingReader.GetDateTime(1);
                replayed = true;
            }
        }
        await transaction.CommitAsync(cancellationToken);
        return new(id, request.PublicationId, issuedAt, request.ExpiresAt.ToUniversalTime(), replayed,
            replayed ? null : request.RawToken);
    }

    public static async Task<ClientDeliveryResult> MarkDeliveryAsync(NpgsqlConnection connection,
        MarkClientDeliveryCommand request, CancellationToken cancellationToken = default)
    {
        ValidateOwnedInput(request.TenantId, request.JobId, request.Actor);
        if (request.CommunicationId == Guid.Empty) throw new ArgumentException("Communication ID is required.");
        var target = request.Accepted ? "smtp_accepted" : "failed";
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string readSql = """
            SELECT delivery_state,provider,connector_version,redacted_error
            FROM public.email_communications
            WHERE communication_id=@id AND tenant_id=@tenant AND job_id=@job FOR UPDATE;
            """;
        string? current;
        string provider;
        string connector;
        string error;
        await using (var read = new NpgsqlCommand(readSql, connection, transaction))
        {
            read.Parameters.AddWithValue("id", request.CommunicationId);
            read.Parameters.AddWithValue("tenant", request.TenantId);
            read.Parameters.AddWithValue("job", request.JobId);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new UnauthorizedAccessException("The communication does not belong to this job and tenant.");
            current = reader.GetString(0); provider = reader.GetString(1); connector = reader.GetString(2); error = reader.GetString(3);
        }
        var safeProvider = ReduceLabel(request.Provider, 60);
        var safeConnector = ReduceLabel(request.ConnectorVersion, 40);
        var safeError = request.Accepted ? "" : RedactError(request.RedactedError);
        if (current is "smtp_accepted" or "failed")
        {
            await transaction.CommitAsync(cancellationToken);
            var same = current == target && provider == safeProvider && connector == safeConnector && error == safeError;
            return new(same ? "replayed" : "conflict", request.CommunicationId, current, true,
                same ? "Delivery outcome was already recorded." : "A different final delivery outcome is already authoritative.");
        }
        await using (var update = new NpgsqlCommand("""
            UPDATE public.email_communications SET delivery_state=@state,
                accepted_at=CASE WHEN @accepted THEN NOW() ELSE NULL END,
                failed_at=CASE WHEN @accepted THEN NULL ELSE NOW() END,
                provider=@provider,connector_version=@connector,redacted_error=@error
            WHERE communication_id=@id;
            """, connection, transaction))
        {
            update.Parameters.AddWithValue("state", target);
            update.Parameters.AddWithValue("accepted", request.Accepted);
            update.Parameters.AddWithValue("provider", safeProvider);
            update.Parameters.AddWithValue("connector", safeConnector);
            update.Parameters.AddWithValue("error", safeError);
            update.Parameters.AddWithValue("id", request.CommunicationId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return new("saved", request.CommunicationId, target, false, "Delivery evidence recorded.");
    }

    public static async Task<IReadOnlyList<ClientCommunicationSummary>> LoadCommunicationsAsync(NpgsqlConnection connection,
        Guid tenantId, Guid jobId, CancellationToken cancellationToken = default)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        const string sql = """
            SELECT c.communication_id,c.publication_id,c.recipient_key,c.purpose,c.delivery_state,
                   c.issued_at,c.expires_at,c.accepted_at,c.failed_at,c.provider,c.connector_version,
                   c.redacted_error,c.revoked_at,c.confirmed_at,c.calendar_requested_at,
                   COUNT(e.event_id) FILTER(WHERE e.event_type='pixel'),
                   MIN(e.occurred_at) FILTER(WHERE e.event_type='pixel'),
                   MAX(e.occurred_at) FILTER(WHERE e.event_type='pixel'),
                   COUNT(e.event_id) FILTER(WHERE e.event_type='view'),
                   MIN(e.occurred_at) FILTER(WHERE e.event_type='view'),
                   MAX(e.occurred_at) FILTER(WHERE e.event_type='view')
            FROM public.email_communications c
            LEFT JOIN public.email_engagement_events e ON e.communication_id=c.communication_id
            WHERE c.tenant_id=@tenant AND c.job_id=@job
            GROUP BY c.communication_id,c.publication_id,c.recipient_key,c.purpose,c.delivery_state,
                     c.issued_at,c.expires_at,c.accepted_at,c.failed_at,c.provider,c.connector_version,
                     c.redacted_error,c.revoked_at,c.confirmed_at,c.calendar_requested_at
            ORDER BY c.issued_at DESC LIMIT 100;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("job", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var results = new List<ClientCommunicationSummary>();
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new(reader.GetGuid(0),reader.GetGuid(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),
                reader.GetDateTime(5),reader.GetDateTime(6),ReadDate(reader,7),ReadDate(reader,8),reader.GetString(9),
                reader.GetString(10),reader.GetString(11),ReadDate(reader,12),ReadDate(reader,13),ReadDate(reader,14),
                reader.GetInt64(15),ReadDate(reader,16),ReadDate(reader,17),reader.GetInt64(18),ReadDate(reader,19),ReadDate(reader,20)));
        }
        return results;
    }

    /// <summary>Resolves an active bearer secret without returning its stored hash.</summary>
    public static async Task<ClientPageAccess?> ResolveAsync(NpgsqlConnection connection, string rawToken,
        string purpose, string tokenPepper, CancellationToken cancellationToken = default)
    {
        var normalizedPurpose = NormalizePurpose(purpose);
        var tokenHash = HashToken(rawToken, normalizedPurpose, tokenPepper);
        const string sql = """
            SELECT c.communication_id,c.publication_id,c.tenant_id,c.job_id,c.recipient_key,c.expires_at,
                   p.approved_version,p.snapshot_fingerprint,p.snapshot_json::text
            FROM public.email_communications c
            JOIN public.client_inspection_pages p ON p.publication_id=c.publication_id
            JOIN public.client_engagement_settings s ON s.tenant_id=c.tenant_id AND s.page_enabled=true
            WHERE c.token_hash=@hash AND c.purpose=@purpose AND c.revoked_at IS NULL
              AND c.expires_at>NOW() AND p.revoked_at IS NULL;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("hash", tokenHash);
        command.Parameters.AddWithValue("purpose", normalizedPurpose);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new(reader.GetGuid(0), reader.GetGuid(1), reader.GetGuid(2), reader.GetGuid(3), reader.GetString(4),
            reader.GetDateTime(5), reader.GetInt32(6), reader.GetString(7), reader.GetString(8));
    }

    /// <summary>
    /// Resolves only for tracking-pixel evidence. Pixel opt-in is independent of page opt-in;
    /// callers must never use the returned snapshot to render the client page.
    /// </summary>
    public static async Task<ClientPageAccess?> ResolveForPixelAsync(NpgsqlConnection connection, string rawToken,
        string purpose, string tokenPepper, CancellationToken cancellationToken = default)
    {
        var normalizedPurpose = NormalizePurpose(purpose);
        var tokenHash = HashToken(rawToken, normalizedPurpose, tokenPepper);
        const string sql = """
            SELECT c.communication_id,c.publication_id,c.tenant_id,c.job_id,c.recipient_key,c.expires_at,
                   p.approved_version,p.snapshot_fingerprint,p.snapshot_json::text
            FROM public.email_communications c
            JOIN public.client_inspection_pages p ON p.publication_id=c.publication_id
            JOIN public.client_engagement_settings s ON s.tenant_id=c.tenant_id AND s.pixel_enabled=true
            WHERE c.token_hash=@hash AND c.purpose=@purpose AND c.revoked_at IS NULL
              AND c.expires_at>NOW() AND p.revoked_at IS NULL;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("hash", tokenHash);
        command.Parameters.AddWithValue("purpose", normalizedPurpose);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new(reader.GetGuid(0), reader.GetGuid(1), reader.GetGuid(2), reader.GetGuid(3), reader.GetString(4),
            reader.GetDateTime(5), reader.GetInt32(6), reader.GetString(7), reader.GetString(8));
    }

    public static Task<ClientEngagementResult> RecordPixelAsync(NpgsqlConnection connection, ClientEngagementCommand request,
        string privacyPepper, CancellationToken cancellationToken = default) =>
        RecordEventAsync(connection, request with { EventType = "pixel" }, privacyPepper, cancellationToken);

    public static Task<ClientEngagementResult> RecordViewAsync(NpgsqlConnection connection, ClientEngagementCommand request,
        string privacyPepper, CancellationToken cancellationToken = default) =>
        RecordEventAsync(connection, request with { EventType = "view" }, privacyPepper, cancellationToken);

    public static Task<ClientEngagementResult> RecordConfirmAsync(NpgsqlConnection connection, ClientEngagementCommand request,
        string privacyPepper, CancellationToken cancellationToken = default) =>
        RecordEventAsync(connection, request with { EventType = "confirm" }, privacyPepper, cancellationToken);

    public static Task<ClientEngagementResult> RecordCalendarAsync(NpgsqlConnection connection, ClientEngagementCommand request,
        string privacyPepper, CancellationToken cancellationToken = default) =>
        RecordEventAsync(connection, request with { EventType = "calendar" }, privacyPepper, cancellationToken);

    public static async Task<ClientEngagementResult> RecordEventAsync(NpgsqlConnection connection,
        ClientEngagementCommand request, string privacyPepper, CancellationToken cancellationToken = default)
    {
        if (request.CommunicationId == Guid.Empty || request.TenantId == Guid.Empty || request.JobId == Guid.Empty)
            throw new ArgumentException("Communication, tenant and job IDs are required.");
        var eventType = NormalizeEvent(request.EventType);
        if (string.IsNullOrWhiteSpace(request.EventKey) || request.EventKey.Length > 200)
            throw new ArgumentException("A bounded event key is required.");
        var ipHash = PrivacyHash(ReduceIp(request.IpAddress), privacyPepper);
        var agentFamily = ReduceUserAgent(request.UserAgent);
        var origin = ReduceReferrer(request.Referrer);
        var metadata = ReduceMetadata(request.MetadataJson);
        await EnsureAsync(connection, cancellationToken);
        var id = Guid.NewGuid();
        const string sql = """
            INSERT INTO public.email_engagement_events
                (event_id,communication_id,tenant_id,job_id,event_type,event_key,ip_network_hash,
                 user_agent_family,referrer_origin,metadata_json)
            SELECT @id,c.communication_id,c.tenant_id,c.job_id,@type,@key,@ip,@agent,@origin,CAST(@metadata AS jsonb)
            FROM public.email_communications c
            JOIN public.client_inspection_pages p ON p.publication_id=c.publication_id
            JOIN public.client_engagement_settings s ON s.tenant_id=c.tenant_id
            WHERE c.communication_id=@communication AND c.tenant_id=@tenant AND c.job_id=@job
              AND c.revoked_at IS NULL AND c.expires_at>NOW() AND p.revoked_at IS NULL
              AND ((@type='pixel' AND s.pixel_enabled=true) OR (@type<>'pixel' AND s.page_enabled=true))
            ON CONFLICT(communication_id,event_type,event_key) DO NOTHING
            RETURNING event_id,occurred_at;
            """;
        DateTime occurred;
        var replayed = false;
        await using (var command = new NpgsqlCommand(sql, connection))
        {
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("communication", request.CommunicationId);
            command.Parameters.AddWithValue("tenant", request.TenantId);
            command.Parameters.AddWithValue("job", request.JobId);
            command.Parameters.AddWithValue("type", eventType);
            command.Parameters.AddWithValue("key", request.EventKey.Trim());
            command.Parameters.AddWithValue("ip", ipHash);
            command.Parameters.AddWithValue("agent", agentFamily);
            command.Parameters.AddWithValue("origin", origin);
            command.Parameters.AddWithValue("metadata", metadata);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) occurred = reader.GetDateTime(1);
            else
            {
                await reader.DisposeAsync();
                await using var existing = new NpgsqlCommand("""
                    SELECT e.event_id,e.occurred_at FROM public.email_engagement_events e
                    JOIN public.client_engagement_settings s ON s.tenant_id=e.tenant_id
                    WHERE e.communication_id=@communication AND e.tenant_id=@tenant AND e.job_id=@job
                      AND e.event_type=@type AND e.event_key=@key
                      AND ((@type='pixel' AND s.pixel_enabled=true) OR (@type<>'pixel' AND s.page_enabled=true));
                    """, connection);
                existing.Parameters.AddWithValue("communication", request.CommunicationId);
                existing.Parameters.AddWithValue("tenant", request.TenantId);
                existing.Parameters.AddWithValue("job", request.JobId);
                existing.Parameters.AddWithValue("type", eventType);
                existing.Parameters.AddWithValue("key", request.EventKey.Trim());
                await using var existingReader = await existing.ExecuteReaderAsync(cancellationToken);
                if (!await existingReader.ReadAsync(cancellationToken))
                    throw new InvalidOperationException("The communication is expired, revoked, or unavailable.");
                id = existingReader.GetGuid(0);
                occurred = existingReader.GetDateTime(1);
                replayed = true;
            }
        }
        if (!replayed && eventType is "confirm" or "calendar")
        {
            var column = eventType == "confirm" ? "confirmed_at" : "calendar_requested_at";
            await using var update = new NpgsqlCommand($"UPDATE public.email_communications SET {column}=COALESCE({column},@at) WHERE communication_id=@id", connection);
            update.Parameters.AddWithValue("at", occurred);
            update.Parameters.AddWithValue("id", request.CommunicationId);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        return new(id, eventType, occurred, replayed);
    }

    public static async Task<int> RevokeJobAsync(NpgsqlConnection connection, RevokeClientPageCommand request,
        CancellationToken cancellationToken = default)
    {
        ValidateOwnedInput(request.TenantId, request.JobId, request.Actor);
        if (string.IsNullOrWhiteSpace(request.Reason)) throw new ArgumentException("A revocation reason is required.");
        await EnsureAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string publicationSql = """
            UPDATE public.client_inspection_pages SET revoked_at=COALESCE(revoked_at,NOW()),
                revoked_by=CASE WHEN revoked_at IS NULL THEN @actor ELSE revoked_by END,
                revoke_reason=CASE WHEN revoked_at IS NULL THEN @reason ELSE revoke_reason END
            WHERE tenant_id=@tenant AND job_id=@job AND revoked_at IS NULL;
            """;
        int count;
        await using (var publications = new NpgsqlCommand(publicationSql, connection, transaction))
        {
            publications.Parameters.AddWithValue("actor", request.Actor.Trim());
            publications.Parameters.AddWithValue("reason", request.Reason.Trim());
            publications.Parameters.AddWithValue("tenant", request.TenantId);
            publications.Parameters.AddWithValue("job", request.JobId);
            count = await publications.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var communications = new NpgsqlCommand("""
            UPDATE public.email_communications SET revoked_at=COALESCE(revoked_at,NOW()),
                revoked_by=CASE WHEN revoked_at IS NULL THEN @actor ELSE revoked_by END,
                revoke_reason=CASE WHEN revoked_at IS NULL THEN @reason ELSE revoke_reason END
            WHERE tenant_id=@tenant AND job_id=@job AND revoked_at IS NULL;
            """, connection, transaction))
        {
            communications.Parameters.AddWithValue("actor", request.Actor.Trim());
            communications.Parameters.AddWithValue("reason", request.Reason.Trim());
            communications.Parameters.AddWithValue("tenant", request.TenantId);
            communications.Parameters.AddWithValue("job", request.JobId);
            count += await communications.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return count;
    }

    public static async Task<int> ExpireIssuedTokensAsync(NpgsqlConnection connection, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        await EnsureAsync(connection, cancellationToken);
        const string sql = """
            UPDATE public.email_communications
            SET revoked_at=COALESCE(revoked_at,@now),revoked_by='system',revoke_reason='expired'
            WHERE expires_at<=@now AND revoked_at IS NULL;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("now", nowUtc.ToUniversalTime());
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string NormalizePurpose(string value)
    {
        var normalized = (value ?? "").Trim().ToLowerInvariant();
        return normalized is "inspection_page" ? normalized : throw new ArgumentException("Unsupported token purpose.");
    }

    private static string NormalizeEvent(string value)
    {
        var normalized = (value ?? "").Trim().ToLowerInvariant();
        return normalized is "pixel" or "view" or "confirm" or "calendar" or "terms" or "terms_status" or "payment_status" or "booking_complete" ? normalized :
            throw new ArgumentException("Unsupported engagement event.");
    }

    private static void ValidateOwnedInput(Guid tenantId, Guid jobId, string actor)
    {
        if (tenantId == Guid.Empty || jobId == Guid.Empty) throw new ArgumentException("Tenant and job IDs are required.");
        if (string.IsNullOrWhiteSpace(actor)) throw new ArgumentException("An authenticated actor is required.");
    }

    private static string ReduceIp(string? value)
    {
        if (!IPAddress.TryParse(value, out var ip)) return "";
        var bytes = ip.GetAddressBytes();
        if (bytes.Length == 4) bytes[3] = 0;
        else for (var i = 7; i < bytes.Length; i++) bytes[i] = 0;
        return new IPAddress(bytes).ToString();
    }

    private static string ReduceUserAgent(string? value)
    {
        var agent = value ?? "";
        if (agent.Contains("Edg/", StringComparison.OrdinalIgnoreCase)) return "Edge";
        if (agent.Contains("Chrome/", StringComparison.OrdinalIgnoreCase)) return "Chrome";
        if (agent.Contains("Firefox/", StringComparison.OrdinalIgnoreCase)) return "Firefox";
        if (agent.Contains("Safari/", StringComparison.OrdinalIgnoreCase)) return "Safari";
        return string.IsNullOrWhiteSpace(agent) ? "" : "Other";
    }

    private static string ReduceReferrer(string? value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri) ? uri.GetLeftPart(UriPartial.Authority)[..Math.Min(200, uri.GetLeftPart(UriPartial.Authority).Length)] : "";

    private static string ReduceMetadata(string? value)
    {
        var trimmed = (value ?? "{}").Trim();
        if (trimmed.Length > 1000) return "{}";
        try
        {
            using var document = JsonDocument.Parse(trimmed);
            if (document.RootElement.ValueKind != JsonValueKind.Object) return "{}";
            var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                { "viewportClass", "locale", "timezone", "clientAction", "calendarFormat" };
            var reduced = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (!allowed.Contains(property.Name) || property.Value.ValueKind != JsonValueKind.String) continue;
                var safeValue = property.Value.GetString() ?? "";
                reduced[property.Name] = safeValue[..Math.Min(100, safeValue.Length)];
            }
            return JsonSerializer.Serialize(reduced);
        }
        catch (JsonException)
        {
            return "{}";
        }
    }

    private static string ProjectClientSnapshot(string sourceJson)
    {
        using var document = JsonDocument.Parse(sourceJson);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("The approved snapshot is invalid.");
        // Deliberately excludes contact details, office notes, directions, access instructions and internal flags.
        var allowed = new HashSet<string>(StringComparer.Ordinal)
        {
            "address", "jobDate", "durationMinutes", "invoiceTotal",
            "primaryService", "additionalService1", "additionalService2",
            "clientDisplayName", "clientSalutation"
        };
        var projected = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in document.RootElement.EnumerateObject())
            if (allowed.Contains(property.Name)) projected[property.Name] = property.Value.Clone();
        return JsonSerializer.Serialize(projected);
    }

    private static string PrivacyHash(string value, string pepper) =>
        string.IsNullOrEmpty(value) ? "" : Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(pepper), Encoding.UTF8.GetBytes(value))).ToLowerInvariant();

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static DateTime? ReadDate(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);

    private static string ReduceLabel(string? value, int maximum)
    {
        var clean = new string((value ?? "").Where(c => !char.IsControl(c)).ToArray()).Trim();
        return clean[..Math.Min(maximum, clean.Length)];
    }

    private static string RedactError(string? value)
    {
        var text = ReduceLabel(value, 500);
        foreach (var marker in new[] { "authorization", "bearer", "password", "token", "api_key", "apikey", "connection string" })
            if (text.Contains(marker, StringComparison.OrdinalIgnoreCase)) return "[REDACTED DELIVERY ERROR]";
        return text;
    }

    private static string CleanSetting(string? value,int maximum)
    {
        var text=WebUtility.HtmlDecode(value??"").Replace("<","").Replace(">","");
        text=new string(text.Where(c=>!char.IsControl(c)||c is '\r' or '\n' or '\t').ToArray()).Trim();
        return text[..Math.Min(maximum,text.Length)];
    }
    private static string ValidBrand(string? value)=>System.Text.RegularExpressions.Regex.IsMatch(value??"", "^#[0-9a-fA-F]{6}$")?value!.ToLowerInvariant():"#0b5f86";
}

public sealed record ClientPageToken(string Secret, string Hash);
public sealed record PublishClientPageCommand(Guid TenantId, Guid JobId, string Actor, bool RevokePrior = true);
public sealed record ClientPagePublicationResult(Guid PublicationId, Guid TenantId, Guid JobId, int ApprovedVersion,
    string SnapshotFingerprint, DateTime PublishedAt, bool Replayed);
public sealed record IssueClientCommunicationCommand(Guid TenantId, Guid JobId, Guid PublicationId, string RecipientKey,
    string RecipientAddress, string Purpose, string IdempotencyKey, string RawToken, DateTime ExpiresAt, string Subject,
    bool IsTest, bool IsPreview, string Actor);
public sealed record ClientCommunicationIssueResult(Guid CommunicationId, Guid PublicationId, DateTime IssuedAt,
    DateTime ExpiresAt, bool Replayed, string? RawToken);
public sealed record ClientPageAccess(Guid CommunicationId, Guid PublicationId, Guid TenantId, Guid JobId,
    string RecipientKey, DateTime ExpiresAt, int ApprovedVersion, string SnapshotFingerprint, string SnapshotJson);
public sealed record ClientEngagementCommand(Guid CommunicationId, Guid TenantId, Guid JobId, string EventType,
    string EventKey, string? IpAddress, string? UserAgent, string? Referrer, string? MetadataJson);
public sealed record ClientEngagementResult(Guid EventId, string EventType, DateTime OccurredAt, bool Replayed);
public sealed record RevokeClientPageCommand(Guid TenantId, Guid JobId, string Reason, string Actor);
public sealed record ClientEngagementSettings(Guid TenantId,bool PageEnabled,bool PixelEnabled,int Version,DateTime UpdatedAt,string IntroductionText,string PaymentInstruction,string BankAccountName,string BankAccountNumber,string PaymentReferenceInstruction,bool ShowBankWithAccounting,string BrandColour);
public sealed record SaveClientEngagementSettingsCommand(Guid TenantId, bool PageEnabled, bool PixelEnabled,
    int ExpectedVersion,string IdempotencyKey,bool Confirmed,string Actor,string IntroductionText="Hello {{CLIENT_SALUTATION}}. Here are the approved details for your inspection.",string PaymentInstruction="Your invoice will be sent to {{CLIENT_EMAIL}}. Payment is required to secure your booking time.",string BankAccountName="",string BankAccountNumber="",string PaymentReferenceInstruction="",bool ShowBankWithAccounting=false,string BrandColour="#0b5f86");
public sealed record ClientEngagementSettingsSaveResult(string Status, ClientEngagementSettings? Settings, string Message);
public sealed record MarkClientDeliveryCommand(Guid TenantId, Guid JobId, Guid CommunicationId, bool Accepted,
    string? Provider, string? ConnectorVersion, string? RedactedError, string Actor);
public sealed record ClientDeliveryResult(string Status, Guid CommunicationId, string DeliveryState, bool Replayed, string Message);
public sealed record ClientCommunicationSummary(Guid CommunicationId, Guid PublicationId, string RecipientKey,
    string Purpose, string DeliveryState, DateTime IssuedAt, DateTime ExpiresAt, DateTime? AcceptedAt,
    DateTime? FailedAt, string Provider, string ConnectorVersion, string RedactedError, DateTime? RevokedAt,
    DateTime? ConfirmedAt, DateTime? CalendarRequestedAt, long PixelCount, DateTime? FirstPixelAt,
    DateTime? LastPixelAt, long ViewCount, DateTime? FirstViewAt, DateTime? LastViewAt);
