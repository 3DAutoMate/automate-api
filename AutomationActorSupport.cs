namespace AutoMateApi;

public static class AutomationActorSupport
{
    public static string Resolve(string? storedActor, Guid authenticatedInspectorId)
    {
        if (authenticatedInspectorId == Guid.Empty)
            throw new global::AuthenticatedAutomationIdentityException("A valid authenticated AutoMate/THREED user is required.");
        return string.IsNullOrWhiteSpace(storedActor)
            ? authenticatedInspectorId.ToString("D")
            : storedActor.Trim();
    }
}
