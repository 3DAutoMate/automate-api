using System.Security.Cryptography;
using System.Text;

public static class AutomationSecretProtector
{
    private const string Prefix = "enc:v1:";
    public static bool IsProtected(string? value) => !string.IsNullOrWhiteSpace(value) && value.StartsWith(Prefix, StringComparison.Ordinal);

    public static string Protect(string value, string? configuredKey)
    {
        if (string.IsNullOrWhiteSpace(value) || IsProtected(value)) return value ?? "";
        if (string.IsNullOrWhiteSpace(configuredKey)) throw new InvalidOperationException("AUTOMATE_AUTOMATION_SECRET_KEY is required before webhook headers can be stored.");
        var key = SHA256.HashData(Encoding.UTF8.GetBytes(configuredKey));
        using var aes = Aes.Create(); aes.Key = key; aes.GenerateIV(); aes.Mode = CipherMode.CBC; aes.Padding = PaddingMode.PKCS7;
        using var encryptor = aes.CreateEncryptor(); var plain = Encoding.UTF8.GetBytes(value); var cipher = encryptor.TransformFinalBlock(plain, 0, plain.Length);
        var payload = new byte[aes.IV.Length + cipher.Length]; Buffer.BlockCopy(aes.IV, 0, payload, 0, aes.IV.Length); Buffer.BlockCopy(cipher, 0, payload, aes.IV.Length, cipher.Length);
        return Prefix + Convert.ToBase64String(payload);
    }

    public static string Unprotect(string value, string? configuredKey)
    {
        if (!IsProtected(value)) return value ?? "";
        if (string.IsNullOrWhiteSpace(configuredKey)) return "[encrypted headers unavailable]";
        try
        {
            var payload = Convert.FromBase64String(value.Substring(Prefix.Length)); var key = SHA256.HashData(Encoding.UTF8.GetBytes(configuredKey));
            using var aes = Aes.Create(); aes.Key = key; aes.Mode = CipherMode.CBC; aes.Padding = PaddingMode.PKCS7; var ivLength = aes.BlockSize / 8; var iv = payload.Take(ivLength).ToArray(); var cipher = payload.Skip(ivLength).ToArray(); aes.IV = iv;
            using var decryptor = aes.CreateDecryptor(); var plain = decryptor.TransformFinalBlock(cipher, 0, cipher.Length); return Encoding.UTF8.GetString(plain);
        }
        catch { return "[encrypted headers unavailable]"; }
    }
}
