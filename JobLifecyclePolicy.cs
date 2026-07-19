namespace AutoMateApi;

public static class JobLifecyclePolicy
{
    public static string InitialImportStatus => "Unscheduled";

    public static string Migrate(bool reportEmailAccepted,bool legacyUnscheduled,bool schedulingStarted)
        => reportEmailAccepted ? "Complete" : legacyUnscheduled ? "Unscheduled" : schedulingStarted ? "Scheduled" : "Unscheduled";

    public static bool CanTransition(string current,string target) => (current,target) switch
    {
        ("Unscheduled","Scheduled")=>true,
        ("Unscheduled","Cancelled")=>true,
        ("Scheduled","Unscheduled")=>true,
        ("Scheduled","Cancelled")=>true,
        ("Cancelled","Unscheduled")=>true,
        _=>false
    };

    public static bool CanExposeScheduling(string automateStatus,bool hasAppointment)
        => hasAppointment && (automateStatus is "Unscheduled" or "Scheduled");

    public static string PaymentDisplay(string automateStatus,bool hasManualPaidOverride,bool fullyCovered)
        => automateStatus=="Cancelled"&&hasManualPaidOverride ? "Cancellation payment review required"
            : hasManualPaidOverride&&fullyCovered ? "Manually marked paid in AutoMate"
            : fullyCovered ? "Paid" : "Awaiting payment";
}
