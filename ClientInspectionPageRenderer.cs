using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;

namespace AutoMateApi;

public sealed record ClientInspectionDisplay(
    string CompanyName, string CompanyPhone, string CompanyEmail, string InspectorName,
    string InspectorPhone, string InspectorEmail, string ClientDisplayName, decimal AmountPaid,
    bool TermsRequired, bool TermsSigned, string TermsSigningLink, bool Cancelled);

public static class ClientInspectionPageRenderer
{
    public static string Render(ClientPageAccess access, ClientInspectionDisplay display, string rawToken)
    {
        using var document = JsonDocument.Parse(access.SnapshotJson);
        var root = document.RootElement;
        var address = Text(root, "address");
        var startsAt = ParseDate(Text(root, "jobDate"));
        var duration = Number(root, "durationMinutes");
        var total = Money(root, "invoiceTotal");
        var balance = Math.Max(0, total - display.AmountPaid);
        var services = new[] { Text(root, "primaryService"), Text(root, "additionalService1"), Text(root, "additionalService2") }
            .Where(value => !string.IsNullOrWhiteSpace(value)).ToArray();
        var company = string.IsNullOrWhiteSpace(display.CompanyName) ? "Your inspection company" : display.CompanyName;
        var approvedClient = Text(root, "clientDisplayName");
        var client = !string.IsNullOrWhiteSpace(approvedClient) ? approvedClient : string.IsNullOrWhiteSpace(display.ClientDisplayName) ? "there" : display.ClientDisplayName;
        var status = display.Cancelled ? "Inspection cancelled" : "Inspection scheduled";
        var date = startsAt.HasValue ? startsAt.Value.ToString("dddd, d MMMM yyyy", CultureInfo.GetCultureInfo("en-NZ")) : "To be confirmed";
        var time = startsAt.HasValue ? startsAt.Value.ToString("h:mm tt", CultureInfo.GetCultureInfo("en-NZ")) : "";
        var terms = SafeHttps(display.TermsSigningLink);
        var token = Uri.EscapeDataString(rawToken);

        return """
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow,noarchive"><title>Inspection details</title><style>
:root{--brand:#0b5f86;--strong:#073f5b;--soft:#e5f3f8;--canvas:#f5f7f9;--surface:#fff;--text:#17212b;--muted:#64717d;--border:#dfe5e9;--success:#167454}*{box-sizing:border-box}body{margin:0;background:var(--canvas);color:var(--text);font:15px/1.5 "Segoe UI",system-ui,sans-serif}.shell{max-width:960px;margin:auto;padding:24px 16px 48px}.brand{display:flex;gap:12px;align-items:center;margin-bottom:28px}.mark{display:grid;place-items:center;width:44px;height:44px;border-radius:11px;background:var(--brand);color:#fff;font-weight:800}.brand span{display:flex;flex-direction:column}.brand small,.muted,footer{color:var(--muted)}.hero{margin-bottom:22px}.kicker{text-transform:uppercase;letter-spacing:.08em;color:var(--brand);font-size:12px;font-weight:800}.hero h1{font-size:clamp(28px,6vw,44px);line-height:1.08;margin:5px 0 10px}.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;box-shadow:0 4px 16px rgb(23 33 43 / 5%);padding:20px;margin:16px 0}.primary{border-top:4px solid var(--brand)}.summary{display:grid;gap:18px}.date{display:flex;justify-content:space-between;gap:12px}.date strong{font-size:18px}.date b{color:var(--brand)}.facts,.grid,.payment{display:grid;gap:12px}.fact,.row{display:flex;justify-content:space-between;gap:16px;padding:9px 0;border-bottom:1px solid var(--border)}.fact:last-child,.row:last-child{border:0}.row span{color:var(--muted)}.row strong{text-align:right}.actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:18px}.button{min-height:44px;border:1px solid var(--border);border-radius:8px;background:#fff;padding:10px 16px;color:var(--text);font:inherit;font-weight:700;text-decoration:none;cursor:pointer}.button.primary{background:var(--brand);border-color:var(--brand);color:#fff}.button[disabled]{opacity:.65}.notice{background:var(--soft);border-color:#c8e3ee}footer{display:flex;justify-content:space-between;gap:10px;font-size:12px;padding:18px 4px}@media(min-width:700px){.shell{padding:36px 28px}.grid{grid-template-columns:1fr 1fr}.payment{grid-template-columns:repeat(3,1fr)}.payment .row{display:block;border:0;border-right:1px solid var(--border)}.payment .row:last-child{border:0}.payment .row strong{display:block;text-align:left;font-size:18px}}
</style></head><body><main class="shell"><header class="brand"><div class="mark">A</div><span><strong>__COMPANY__</strong><small>Inspection details</small></span></header>
<section class="hero"><div class="kicker">__STATUS__</div><h1>__ADDRESS__</h1><p>Hello __CLIENT__. Here are the approved details for your inspection.</p></section>
<section class="card primary"><div class="date"><strong>__DATE__</strong><b>__TIME__</b></div><div class="facts"><div class="fact"><span>Duration</span><strong>__DURATION__</strong></div><div class="fact"><span>Services</span><strong>__SERVICES__</strong></div><div class="fact"><span>Inspector</span><strong>__INSPECTOR__</strong></div></div><div class="actions">__PRIMARY_ACTIONS__</div></section>
<div class="grid"><section class="card"><h2>Appointment</h2><div class="row"><span>Address</span><strong>__ADDRESS__</strong></div><div class="row"><span>Status</span><strong>__STATUS__</strong></div><div class="row"><span>Reference</span><strong>__REFERENCE__</strong></div></section><section class="card"><h2>Your inspector</h2><strong>__INSPECTOR__</strong><div class="actions">__CONTACT_ACTIONS__</div></section></div>
<section class="card"><h2>Payment summary</h2><div class="payment"><div class="row"><span>Total</span><strong>__TOTAL__</strong></div><div class="row"><span>Paid</span><strong>__PAID__</strong></div><div class="row"><span>Balance</span><strong>__BALANCE__</strong></div></div></section>
__TERMS__<section class="card notice"><h2>Need to change something?</h2><p>Contact __COMPANY__. Changes cannot be made from this page.</p></section><footer><span>Secure AutoMate inspection page</span><span>Expires __EXPIRES__</span></footer></main>
<script>const b=document.getElementById('confirm');if(b)b.addEventListener('click',async()=>{b.disabled=true;try{const r=await fetch('/inspection/__TOKEN__/confirm',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});if(!r.ok)throw 0;b.textContent='Receipt confirmed';}catch{b.disabled=false;b.textContent='Try confirmation again';}});</script></body></html>
"""
            .Replace("__COMPANY__", H(company)).Replace("__STATUS__", H(status)).Replace("__ADDRESS__", H(address))
            .Replace("__CLIENT__", H(client)).Replace("__DATE__", H(date)).Replace("__TIME__", H(time))
            .Replace("__DURATION__", duration > 0 ? $"{duration:0} minutes" : "To be confirmed")
            .Replace("__SERVICES__", H(services.Length == 0 ? "To be confirmed" : string.Join(", ", services)))
            .Replace("__INSPECTOR__", H(display.InspectorName)).Replace("__REFERENCE__", H(PublicReference(access)))
            .Replace("__TOTAL__", Currency(total)).Replace("__PAID__", Currency(display.AmountPaid)).Replace("__BALANCE__", Currency(balance))
            .Replace("__PRIMARY_ACTIONS__", display.Cancelled ? "" : $"<button id=\"confirm\" class=\"button primary\">Confirm received</button><a class=\"button\" href=\"/inspection/{token}/calendar.ics\">Add to calendar</a>")
            .Replace("__CONTACT_ACTIONS__", ContactActions(display)).Replace("__TERMS__", Terms(display, terms))
            .Replace("__EXPIRES__", H(access.ExpiresAt.ToString("d MMM yyyy", CultureInfo.GetCultureInfo("en-NZ"))))
            .Replace("__TOKEN__", token);
    }

    public static string Calendar(ClientPageAccess access, ClientInspectionDisplay display)
    {
        using var document = JsonDocument.Parse(access.SnapshotJson); var root = document.RootElement;
        var start = ParseDate(Text(root, "jobDate")) ?? DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
        var minutes = Math.Max(1, (int)Number(root, "durationMinutes")); var end = start.AddMinutes(minutes);
        var address = Ics(Text(root, "address")); var company = Ics(display.CompanyName); var reference = Ics(PublicReference(access));
        return $"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//AutoMate//Inspection//EN\r\nBEGIN:VEVENT\r\nUID:{reference}@3dautomate.co.nz\r\nDTSTAMP:{DateTime.UtcNow:yyyyMMdd'T'HHmmss'Z'}\r\nDTSTART:{start.ToUniversalTime():yyyyMMdd'T'HHmmss'Z'}\r\nDTEND:{end.ToUniversalTime():yyyyMMdd'T'HHmmss'Z'}\r\nSUMMARY:Property inspection - {company}\r\nLOCATION:{address}\r\nDESCRIPTION:Inspection reference {reference}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
    }

    private static string Terms(ClientInspectionDisplay display, string link)
    {
        if (!display.TermsRequired) return "";
        if (display.TermsSigned) return "<section class=\"card\"><h2>Terms</h2><p>Agreement signed.</p></section>";
        return string.IsNullOrWhiteSpace(link) ? "<section class=\"card\"><h2>Terms</h2><p>Agreement pending. Contact your inspector if you need the signing link resent.</p></section>" : $"<section class=\"card\"><h2>Terms</h2><p>Your agreement is waiting for review.</p><a class=\"button primary\" href=\"{H(link)}\" rel=\"noreferrer\">Review and sign terms</a></section>";
    }
    private static string ContactActions(ClientInspectionDisplay display)
    {
        var output = new StringBuilder();
        if (!string.IsNullOrWhiteSpace(display.InspectorPhone)) output.Append($"<a class=\"button\" href=\"tel:{H(display.InspectorPhone)}\">Call inspector</a>");
        if (!string.IsNullOrWhiteSpace(display.InspectorEmail)) output.Append($"<a class=\"button\" href=\"mailto:{H(display.InspectorEmail)}\">Email inspector</a>");
        return output.ToString();
    }
    private static string H(string? value) => WebUtility.HtmlEncode(value ?? "");
    private static string Currency(decimal value) => value.ToString("C", CultureInfo.GetCultureInfo("en-NZ"));
    private static string Text(JsonElement root, string key) => root.TryGetProperty(key, out var value) ? value.ToString().Trim() : "";
    private static decimal Number(JsonElement root, string key) => root.TryGetProperty(key, out var value) && decimal.TryParse(value.ToString().Trim('"'), NumberStyles.Any, CultureInfo.InvariantCulture, out var number) ? number : 0;
    private static decimal Money(JsonElement root, string key) => Number(root, key);
    private static DateTime? ParseDate(string value) => DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var date) ? date : null;
    private static string SafeHttps(string? value) => Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme == Uri.UriSchemeHttps ? uri.ToString() : "";
    private static string PublicReference(ClientPageAccess access) => "AM-" + access.CommunicationId.ToString("N")[..10].ToUpperInvariant();
    private static string Ics(string value) => (value ?? "").Replace("\\", "\\\\").Replace("\r", "").Replace("\n", "\\n").Replace(",", "\\,").Replace(";", "\\;");
}
