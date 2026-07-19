using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace AutoMateApi;

public static class ProviderIntegrationSupport
{
    public const string MissingValue = "__ADD_PROVIDER_VALUE__";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        const string sql = """
CREATE TABLE IF NOT EXISTS public.tenant_provider_accounts
(
 tenant_id uuid NOT NULL,
 provider_key text NOT NULL,
 status text NOT NULL DEFAULT 'disconnected',
 access_token_ciphertext text NOT NULL DEFAULT '',
 refresh_token_ciphertext text NOT NULL DEFAULT '',
 token_type text NOT NULL DEFAULT 'Bearer',
 scopes text NOT NULL DEFAULT '',
 expires_at timestamptz NULL,
 account_email text NOT NULL DEFAULT '',
 external_account_id text NOT NULL DEFAULT '',
 api_base_uri text NOT NULL DEFAULT '',
 metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
 granted_by_inspector_id uuid NULL,
 created_at timestamptz NOT NULL DEFAULT NOW(),
 updated_at timestamptz NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,provider_key)
);
CREATE TABLE IF NOT EXISTS public.provider_oauth_states
(
 state_hash text PRIMARY KEY,
 tenant_id uuid NOT NULL,
 inspector_id uuid NOT NULL,
 provider_key text NOT NULL,
 redirect_after text NOT NULL DEFAULT '',
 code_verifier_ciphertext text NOT NULL DEFAULT '',
 expires_at timestamptz NOT NULL,
 consumed_at timestamptz NULL,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_provider_oauth_states_expiry
 ON public.provider_oauth_states(expires_at) WHERE consumed_at IS NULL;
CREATE TABLE IF NOT EXISTS public.tenant_provider_audit
(
 audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id uuid NOT NULL,
 provider_key text NOT NULL,
 action text NOT NULL,
 actor text NOT NULL DEFAULT '',
 detail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
 created_at timestamptz NOT NULL DEFAULT NOW()
);
""";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static bool HasConfiguration(IConfiguration configuration, params string[] keys) =>
        keys.All(key => IsRealValue(configuration[key]));

    public static bool IsRealValue(string? value) =>
        !string.IsNullOrWhiteSpace(value) &&
        !string.Equals(value.Trim(), MissingValue, StringComparison.OrdinalIgnoreCase);

    public static async Task<ProviderOAuthStart> CreateOAuthStateAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        Guid inspectorId,
        string providerKey,
        string? redirectAfter,
        string? codeVerifier,
        string encryptionKey,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        var rawState = Base64Url(RandomNumberGenerator.GetBytes(32));
        var stateHash = HashState(rawState);
        var expiresAt = DateTime.UtcNow.AddMinutes(10);
        var safeRedirect = NormalizeRedirect(redirectAfter);
        var protectedVerifier = string.IsNullOrWhiteSpace(codeVerifier)
            ? ""
            : AutomationSecretProtector.Protect(codeVerifier, encryptionKey);
        await using var command = new NpgsqlCommand("""
INSERT INTO public.provider_oauth_states
(state_hash,tenant_id,inspector_id,provider_key,redirect_after,code_verifier_ciphertext,expires_at)
VALUES(@hash,@tenant,@inspector,@provider,@redirect,@verifier,@expires)
""", connection);
        command.Parameters.AddWithValue("hash", stateHash);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("inspector", inspectorId);
        command.Parameters.AddWithValue("provider", NormalizeProvider(providerKey));
        command.Parameters.AddWithValue("redirect", safeRedirect);
        command.Parameters.AddWithValue("verifier", protectedVerifier);
        command.Parameters.AddWithValue("expires", expiresAt);
        await command.ExecuteNonQueryAsync(ct);
        return new(rawState, expiresAt);
    }

    public static async Task<ProviderOAuthContext?> ConsumeOAuthStateAsync(
        NpgsqlConnection connection,
        string rawState,
        string providerKey,
        string encryptionKey,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(rawState)) return null;
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand("""
UPDATE public.provider_oauth_states
SET consumed_at=NOW()
WHERE state_hash=@hash AND provider_key=@provider AND consumed_at IS NULL AND expires_at>NOW()
RETURNING tenant_id,inspector_id,redirect_after,code_verifier_ciphertext
""", connection);
        command.Parameters.AddWithValue("hash", HashState(rawState));
        command.Parameters.AddWithValue("provider", NormalizeProvider(providerKey));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        var verifier = reader.GetString(3);
        if (!string.IsNullOrWhiteSpace(verifier)) verifier = AutomationSecretProtector.Unprotect(verifier, encryptionKey);
        return new(reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2), verifier);
    }

    public static async Task UpsertAccountAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        Guid inspectorId,
        ProviderAccountWrite account,
        string encryptionKey,
        string actor,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        if (!IsRealValue(encryptionKey)) throw new InvalidOperationException("AUTOMATE_INTEGRATION_SECRET_KEY is required before provider credentials can be stored.");
        var provider = NormalizeProvider(account.ProviderKey);
        var access = AutomationSecretProtector.Protect(account.AccessToken ?? "", encryptionKey);
        var refresh = string.IsNullOrWhiteSpace(account.RefreshToken) ? "" : AutomationSecretProtector.Protect(account.RefreshToken, encryptionKey);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var command = new NpgsqlCommand("""
INSERT INTO public.tenant_provider_accounts
(tenant_id,provider_key,status,access_token_ciphertext,refresh_token_ciphertext,token_type,scopes,expires_at,account_email,external_account_id,api_base_uri,metadata_json,granted_by_inspector_id)
VALUES(@tenant,@provider,'connected',@access,@refresh,@type,@scopes,@expires,@email,@external,@base,CAST(@metadata AS jsonb),@inspector)
ON CONFLICT(tenant_id,provider_key) DO UPDATE SET
 status='connected',access_token_ciphertext=EXCLUDED.access_token_ciphertext,
 refresh_token_ciphertext=CASE WHEN EXCLUDED.refresh_token_ciphertext='' THEN tenant_provider_accounts.refresh_token_ciphertext ELSE EXCLUDED.refresh_token_ciphertext END,
 token_type=EXCLUDED.token_type,scopes=EXCLUDED.scopes,expires_at=EXCLUDED.expires_at,
 account_email=EXCLUDED.account_email,external_account_id=EXCLUDED.external_account_id,
 api_base_uri=EXCLUDED.api_base_uri,metadata_json=EXCLUDED.metadata_json,
 granted_by_inspector_id=EXCLUDED.granted_by_inspector_id,updated_at=NOW()
""", connection, tx))
        {
            command.Parameters.AddWithValue("tenant", tenantId);
            command.Parameters.AddWithValue("provider", provider);
            command.Parameters.AddWithValue("access", access);
            command.Parameters.AddWithValue("refresh", refresh);
            command.Parameters.AddWithValue("type", string.IsNullOrWhiteSpace(account.TokenType) ? "Bearer" : account.TokenType.Trim());
            command.Parameters.AddWithValue("scopes", account.Scopes ?? "");
            command.Parameters.AddWithValue("expires", (object?)account.ExpiresAt ?? DBNull.Value);
            command.Parameters.AddWithValue("email", account.AccountEmail ?? "");
            command.Parameters.AddWithValue("external", account.ExternalAccountId ?? "");
            command.Parameters.AddWithValue("base", account.ApiBaseUri ?? "");
            command.Parameters.AddWithValue("metadata", JsonSerializer.Serialize(account.Metadata ?? new Dictionary<string, string>()));
            command.Parameters.AddWithValue("inspector", inspectorId);
            await command.ExecuteNonQueryAsync(ct);
        }
        await AuditAsync(connection, tx, tenantId, provider, "connected", actor, new { account.AccountEmail, account.ExternalAccountId, account.ApiBaseUri, account.Scopes }, ct);
        await tx.CommitAsync(ct);
    }

    public static async Task<ProviderAccount?> LoadAccountAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        string providerKey,
        string encryptionKey,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        await using var command = new NpgsqlCommand("""
SELECT status,access_token_ciphertext,refresh_token_ciphertext,token_type,scopes,expires_at,
 account_email,external_account_id,api_base_uri,metadata_json::text,updated_at
FROM public.tenant_provider_accounts WHERE tenant_id=@tenant AND provider_key=@provider
""", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("provider", NormalizeProvider(providerKey));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        var access = AutomationSecretProtector.Unprotect(reader.GetString(1), encryptionKey);
        var refresh = AutomationSecretProtector.Unprotect(reader.GetString(2), encryptionKey);
        if (access.StartsWith("[encrypted", StringComparison.Ordinal) || refresh.StartsWith("[encrypted", StringComparison.Ordinal))
            throw new InvalidOperationException("Provider credentials cannot be decrypted. Check AUTOMATE_INTEGRATION_SECRET_KEY.");
        return new(
            NormalizeProvider(providerKey), reader.GetString(0), access, refresh, reader.GetString(3), reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetDateTime(5), reader.GetString(6), reader.GetString(7), reader.GetString(8),
            reader.GetString(9), reader.GetDateTime(10));
    }

    public static async Task<IReadOnlyDictionary<string, ProviderAccountSummary>> LoadSummariesAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        var result = new Dictionary<string, ProviderAccountSummary>(StringComparer.OrdinalIgnoreCase);
        await using var command = new NpgsqlCommand("SELECT provider_key,status,account_email,external_account_id,expires_at,updated_at FROM public.tenant_provider_accounts WHERE tenant_id=@tenant", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var key = reader.GetString(0);
            result[key] = new(key, reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetDateTime(4), reader.GetDateTime(5));
        }
        return result;
    }

    public static async Task UpdateTokensAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        string providerKey,
        string accessToken,
        string? refreshToken,
        DateTime? expiresAt,
        string encryptionKey,
        CancellationToken ct = default)
    {
        var access = AutomationSecretProtector.Protect(accessToken, encryptionKey);
        var refresh = string.IsNullOrWhiteSpace(refreshToken) ? "" : AutomationSecretProtector.Protect(refreshToken, encryptionKey);
        await using var command = new NpgsqlCommand("""
UPDATE public.tenant_provider_accounts SET access_token_ciphertext=@access,
 refresh_token_ciphertext=CASE WHEN @refresh='' THEN refresh_token_ciphertext ELSE @refresh END,
 expires_at=@expires,status='connected',updated_at=NOW()
WHERE tenant_id=@tenant AND provider_key=@provider
""", connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("provider", NormalizeProvider(providerKey));
        command.Parameters.AddWithValue("access", access);
        command.Parameters.AddWithValue("refresh", refresh);
        command.Parameters.AddWithValue("expires", (object?)expiresAt ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<bool> DisconnectAsync(
        NpgsqlConnection connection,
        Guid tenantId,
        string providerKey,
        string actor,
        CancellationToken ct = default)
    {
        await EnsureAsync(connection, ct);
        var provider = NormalizeProvider(providerKey);
        await using var tx = await connection.BeginTransactionAsync(ct);
        string accountEmail = "", externalAccountId = "";
        await using (var current = new NpgsqlCommand("SELECT account_email,external_account_id FROM public.tenant_provider_accounts WHERE tenant_id=@tenant AND provider_key=@provider FOR UPDATE", connection, tx))
        {
            current.Parameters.AddWithValue("tenant", tenantId);
            current.Parameters.AddWithValue("provider", provider);
            await using var reader = await current.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                accountEmail = reader.GetString(0);
                externalAccountId = reader.GetString(1);
            }
            else
            {
                await tx.RollbackAsync(ct);
                return false;
            }
        }
        await using (var update = new NpgsqlCommand("""
UPDATE public.tenant_provider_accounts SET status='disconnected',access_token_ciphertext='',
 refresh_token_ciphertext='',token_type='Bearer',scopes='',expires_at=NULL,account_email='',
 external_account_id='',api_base_uri='',metadata_json='{}'::jsonb,granted_by_inspector_id=NULL,updated_at=NOW()
WHERE tenant_id=@tenant AND provider_key=@provider
""", connection, tx))
        {
            update.Parameters.AddWithValue("tenant", tenantId);
            update.Parameters.AddWithValue("provider", provider);
            await update.ExecuteNonQueryAsync(ct);
        }
        await using (var expireStates = new NpgsqlCommand("UPDATE public.provider_oauth_states SET consumed_at=NOW() WHERE tenant_id=@tenant AND provider_key=@provider AND consumed_at IS NULL", connection, tx))
        {
            expireStates.Parameters.AddWithValue("tenant", tenantId);
            expireStates.Parameters.AddWithValue("provider", provider);
            await expireStates.ExecuteNonQueryAsync(ct);
        }
        await AuditAsync(connection, tx, tenantId, provider, "disconnected", actor, new { accountEmail, externalAccountId, credentialsRemoved = true }, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public static string CreatePkceVerifier() => Base64Url(RandomNumberGenerator.GetBytes(48));
    public static string CreatePkceChallenge(string verifier) => Base64Url(SHA256.HashData(Encoding.ASCII.GetBytes(verifier)));

    private static string NormalizeProvider(string value) => (value ?? "").Trim().ToLowerInvariant().Replace('-', '_');
    private static string HashState(string raw) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(raw))).ToLowerInvariant();
    private static string Base64Url(byte[] value) => Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    private static string NormalizeRedirect(string? redirectAfter) =>
        !string.IsNullOrWhiteSpace(redirectAfter) && redirectAfter.StartsWith("/", StringComparison.Ordinal) && !redirectAfter.StartsWith("//", StringComparison.Ordinal)
            ? redirectAfter.Trim()
            : "";

    private static async Task AuditAsync(NpgsqlConnection connection, NpgsqlTransaction tx, Guid tenantId, string provider, string action, string actor, object detail, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("INSERT INTO public.tenant_provider_audit(tenant_id,provider_key,action,actor,detail_json) VALUES(@tenant,@provider,@action,@actor,CAST(@detail AS jsonb))", connection, tx);
        command.Parameters.AddWithValue("tenant", tenantId);
        command.Parameters.AddWithValue("provider", provider);
        command.Parameters.AddWithValue("action", action);
        command.Parameters.AddWithValue("actor", actor ?? "");
        command.Parameters.AddWithValue("detail", JsonSerializer.Serialize(detail));
        await command.ExecuteNonQueryAsync(ct);
    }
}

public sealed record ProviderOAuthStart(string State, DateTime ExpiresAt);
public sealed record ProviderOAuthContext(Guid TenantId, Guid InspectorId, string RedirectAfter, string CodeVerifier);
public sealed record ProviderAccountWrite(string ProviderKey, string AccessToken, string RefreshToken, string TokenType, string Scopes, DateTime? ExpiresAt, string AccountEmail, string ExternalAccountId, string ApiBaseUri, IReadOnlyDictionary<string, string>? Metadata = null);
public sealed record ProviderAccount(string ProviderKey, string Status, string AccessToken, string RefreshToken, string TokenType, string Scopes, DateTime? ExpiresAt, string AccountEmail, string ExternalAccountId, string ApiBaseUri, string MetadataJson, DateTime UpdatedAt);
public sealed record ProviderAccountSummary(string ProviderKey, string Status, string AccountEmail, string ExternalAccountId, DateTime? ExpiresAt, DateTime UpdatedAt);
public sealed class ProviderDisconnectRequest { public Guid TenantId { get; set; } public bool Confirmed { get; set; } }
