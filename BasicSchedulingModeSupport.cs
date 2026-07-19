using Npgsql;

namespace AutoMateApi;

public static class BasicSchedulingModeSupport
{
    public const string Manual = "manual";
    public const string Automatic = "automatic";

    public static async Task EnsureAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        const string sql = """
            ALTER TABLE public.automation_tenant_settings
            ADD COLUMN IF NOT EXISTS basic_scheduling_mode text NOT NULL DEFAULT 'manual';
            ALTER TABLE public.automation_tenant_settings
            DROP CONSTRAINT IF EXISTS ck_basic_scheduling_mode;
            ALTER TABLE public.automation_tenant_settings
            ADD CONSTRAINT ck_basic_scheduling_mode CHECK (basic_scheduling_mode IN ('manual','automatic'));
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<string> LoadAsync(NpgsqlConnection connection, Guid tenantId, CancellationToken cancellationToken = default)
    {
        await EnsureAsync(connection, cancellationToken);
        const string sql = "SELECT COALESCE((SELECT basic_scheduling_mode FROM public.automation_tenant_settings WHERE tenant_id=@tenant),'manual');";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("tenant", tenantId);
        return Convert.ToString(await command.ExecuteScalarAsync(cancellationToken)) ?? Manual;
    }

    public static async Task<string> SaveAsync(NpgsqlConnection connection, Guid tenantId, string mode, string actor, CancellationToken cancellationToken = default)
    {
        mode=(mode??"").Trim().ToLowerInvariant();
        if(mode is not (Manual or Automatic))throw new ArgumentException("Scheduling mode must be manual or automatic.");
        await EnsureAsync(connection,cancellationToken);
        const string sql="""
            INSERT INTO public.automation_tenant_settings(tenant_id,activation_mode,basic_scheduling_mode,updated_by)
            VALUES(@tenant,'selected_jobs',@mode,@actor)
            ON CONFLICT(tenant_id) DO UPDATE SET basic_scheduling_mode=EXCLUDED.basic_scheduling_mode,updated_by=EXCLUDED.updated_by,updated_at=NOW();
            """;
        await using var command=new NpgsqlCommand(sql,connection);
        command.Parameters.AddWithValue("tenant",tenantId);command.Parameters.AddWithValue("mode",mode);command.Parameters.AddWithValue("actor",actor??"Connector user");
        await command.ExecuteNonQueryAsync(cancellationToken);return mode;
    }
}
