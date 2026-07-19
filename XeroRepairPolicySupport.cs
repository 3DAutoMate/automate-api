namespace AutoMateApi;

public static class XeroRepairPolicySupport
{
    public static object BuildReferenceUpdatePayload(string invoiceId,string reference)
    {
        if(string.IsNullOrWhiteSpace(invoiceId))throw new ArgumentException("InvoiceID is required.",nameof(invoiceId));
        if(string.IsNullOrWhiteSpace(reference))throw new ArgumentException("Reference is required.",nameof(reference));
        return new{Invoices=new[]{new XeroReferencePatch(invoiceId.Trim(),reference.Trim())}};
    }

    public static bool TotalsMatch(decimal? xeroTotal,decimal? currentTotal)=>xeroTotal.HasValue&&currentTotal.HasValue&&Math.Abs(xeroTotal.Value-currentTotal.Value)<=0.01m;

    public static bool CanResolveCoveredReview(bool providerVerified,bool needsUpdate,bool changeReviewPending,bool unscheduled,string action,decimal remainingDifference)
        => providerVerified
           && !needsUpdate
           && !changeReviewPending
           && !unscheduled
           && string.Equals(action,"none",StringComparison.Ordinal)
           && Math.Abs(remainingDifference)<=0.01m;
}

public sealed record XeroReferencePatch(string InvoiceID,string Reference);

public sealed record ReconciliationDecision(string Action,decimal RemainingDifference,string Message)
{
    public static ReconciliationDecision Classify(decimal currentTotal,decimal invoicedTotal,bool addressChanged,bool customerChanged)
    {
        var remaining=Math.Round(currentTotal-invoicedTotal,2,MidpointRounding.AwayFromZero);
        if(remaining>0.01m)return new("additional_invoice",remaining,$"Create an additional invoice for {remaining:C} including GST.");
        if(remaining< -0.01m)return new("credit_review",remaining,$"The invoiced total is {Math.Abs(remaining):C} above THREED. Credit review is required.");
        if(customerChanged)return new("contact_review",0m,"The customer changed. Confirm the existing Xero contact before correcting its name.");
        if(addressChanged)return new("reference_update",0m,"The amount is unchanged. Update only the existing Xero invoice Reference.");
        return new("none",0m,"No invoice adjustment is required.");
    }
}
