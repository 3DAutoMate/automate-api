using System.Security.Cryptography;
using System.Text;

namespace AutoMateApi;

public static class RequiredActionPolicySupport
{
    public static bool IsTerminal(string status) => status is "resolved" or "completed" or "superseded";

    public static bool CanRefreshIncident(string status) => string.Equals(status,"open",StringComparison.Ordinal);

    public static string IncidentKey(string reasonKey,string actionKey,string evidenceFingerprint)
    {
        var bytes=SHA256.HashData(Encoding.UTF8.GetBytes($"{reasonKey}|{actionKey}|{evidenceFingerprint}"));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    public static bool CanPresentInvoiceAdjustment(bool providerVerified,string reconciliationAction,decimal remainingDifference)
        => providerVerified&&
           reconciliationAction is ("additional_invoice" or "credit_review" or "reference_update" or "contact_correction")&&
           (reconciliationAction is not ("additional_invoice" or "credit_review")||Math.Abs(remainingDifference)>0.005m);
}
