using System.Security.Cryptography;
using System.Text;

public static class AutomationSecretProtector
{
    private const string V1Prefix = "enc:v1:";
    private const string V2Prefix = "enc:v2:";
    public static bool IsProtected(string? value) => !string.IsNullOrWhiteSpace(value) && (value.StartsWith(V1Prefix, StringComparison.Ordinal)||value.StartsWith(V2Prefix,StringComparison.Ordinal));

    public static string Protect(string value, string? configuredKey)
    {
        if (string.IsNullOrWhiteSpace(value) || IsProtected(value)) return value ?? "";
        if (string.IsNullOrWhiteSpace(configuredKey)) throw new InvalidOperationException("AUTOMATE_AUTOMATION_SECRET_KEY is required before webhook headers can be stored.");
        var key = SHA256.HashData(Encoding.UTF8.GetBytes(configuredKey));
        var nonce=RandomNumberGenerator.GetBytes(12);var plain=Encoding.UTF8.GetBytes(value);var cipher=new byte[plain.Length];var tag=new byte[16];
        using(var aes=new AesGcm(key,tag.Length))aes.Encrypt(nonce,plain,cipher,tag,Encoding.UTF8.GetBytes(V2Prefix));
        var payload=new byte[nonce.Length+tag.Length+cipher.Length];Buffer.BlockCopy(nonce,0,payload,0,nonce.Length);Buffer.BlockCopy(tag,0,payload,nonce.Length,tag.Length);Buffer.BlockCopy(cipher,0,payload,nonce.Length+tag.Length,cipher.Length);
        return V2Prefix+Convert.ToBase64String(payload);
    }

    public static string Unprotect(string value, string? configuredKey)
    {
        if (!IsProtected(value)) return value ?? "";
        if (string.IsNullOrWhiteSpace(configuredKey)) return "[encrypted headers unavailable]";
        try
        {
            var key = SHA256.HashData(Encoding.UTF8.GetBytes(configuredKey));
            if(value.StartsWith(V2Prefix,StringComparison.Ordinal))
            {
                var payload=Convert.FromBase64String(value.Substring(V2Prefix.Length));if(payload.Length<29)throw new CryptographicException("Encrypted payload is invalid.");var nonce=payload[..12];var tag=payload[12..28];var cipher=payload[28..];var plain=new byte[cipher.Length];using(var aes=new AesGcm(key,tag.Length))aes.Decrypt(nonce,cipher,tag,plain,Encoding.UTF8.GetBytes(V2Prefix));return Encoding.UTF8.GetString(plain);
            }
            // Read compatibility for credentials written before authenticated encryption was introduced.
            var legacy = Convert.FromBase64String(value.Substring(V1Prefix.Length));
            using var aesLegacy = Aes.Create(); aesLegacy.Key = key; aesLegacy.Mode = CipherMode.CBC; aesLegacy.Padding = PaddingMode.PKCS7; var ivLength = aesLegacy.BlockSize / 8; var iv = legacy.Take(ivLength).ToArray(); var legacyCipher = legacy.Skip(ivLength).ToArray(); aesLegacy.IV = iv;
            using var decryptor = aesLegacy.CreateDecryptor(); var legacyPlain = decryptor.TransformFinalBlock(legacyCipher, 0, legacyCipher.Length); return Encoding.UTF8.GetString(legacyPlain);
        }
        catch { return "[encrypted headers unavailable]"; }
    }
}
