namespace AutoMateApi;

public static class JobImportPolicy
{
    public static bool IsEligible(bool alreadyExists,DateTimeOffset registrationCutoff,DateTimeOffset? sourceDateAdded)
        => alreadyExists||(sourceDateAdded.HasValue&&sourceDateAdded.Value.ToUniversalTime()>=registrationCutoff.ToUniversalTime());

    public static string DecisionCode(bool alreadyExists,DateTimeOffset registrationCutoff,DateTimeOffset? sourceDateAdded)
        => alreadyExists?"existing_automate_job"
          :!sourceDateAdded.HasValue?"source_date_added_required"
          :IsEligible(false,registrationCutoff,sourceDateAdded)?"created_after_registration"
          :"pre_registration_job_ignored";
}
