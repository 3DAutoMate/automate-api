using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;

namespace AutoMateApi;

public sealed record ClientInspectionDisplay(
    string CompanyName, string CompanyPhone, string CompanyEmail, string InspectorName,
    string InspectorPhone, string InspectorEmail, string ClientDisplayName, decimal AmountPaid,
    bool TermsRequired,bool TermsSigned,string TermsSigningLink,bool Cancelled,string IntroductionText,string PaymentInstruction,string BankAccountName,string BankAccountNumber,string PaymentReferenceInstruction,bool ShowBankWithAccounting,string BrandColour,string CompanyLogoUrl,bool AccountingConnected);

public static class ClientInspectionPageRenderer
{
    public sealed record BookingProgress(string TermsState,decimal Balance,bool Complete);
    public static BookingProgress Progress(ClientPageAccess access,ClientInspectionDisplay display){using var document=JsonDocument.Parse(access.SnapshotJson);var total=Money(document.RootElement,"invoiceTotal");var balance=Math.Max(0,total-display.AmountPaid);var terms=!display.TermsRequired?"not_required":display.TermsSigned?"signed":"awaiting_signature";return new(terms,balance,(!display.TermsRequired||display.TermsSigned)&&balance<=0.005m);}
    public static string RenderPreview(ClientPageAccess access, ClientInspectionDisplay display)
    {
        var html = Render(access, display, "preview");
        html = html.Replace("<body><main class=\"shell\">", "<body><main class=\"shell\"><section class=\"card\" style=\"border:2px solid #d49328;background:#fff7e8\"><strong>Preview — not the live client link</strong><p style=\"margin-bottom:0\">This shows the approved customer-facing snapshot. Preview actions are disabled and no engagement is recorded.</p></section>");
        html = html.Replace("<button id=\"confirm\" class=\"button primary\">Confirm received</button>", "<button class=\"button primary\" disabled>Confirm received</button>");
        html = html.Replace("<a class=\"button\" href=\"/inspection/preview/calendar.ics\">Add to calendar</a>", "<button class=\"button\" disabled>Add to calendar</button>");
        html = System.Text.RegularExpressions.Regex.Replace(html, "<script>.*?</script>", "", System.Text.RegularExpressions.RegexOptions.Singleline);
        return html;
    }

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
        var clientEmail=Text(root,"clientEmail");
        var introduction=SettingText(display.IntroductionText,client,clientEmail);
        var paymentInstruction=SettingText(display.PaymentInstruction,client,clientEmail);
        var bookingComplete=(!display.TermsRequired||display.TermsSigned)&&balance<=0.005m;
        var brand=System.Text.RegularExpressions.Regex.IsMatch(display.BrandColour??"","^#[0-9a-fA-F]{6}$")?display.BrandColour:"#0b5f86";
        var logo=SafeHttps(display.CompanyLogoUrl);
        var brandMark=string.IsNullOrWhiteSpace(logo)?"<div class=\"mark\">A</div>":$"<img class=\"company-logo\" src=\"{H(logo)}\" alt=\"{H(company)}\">";

        return """
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow,noarchive"><title>Inspection details</title><style>
:root{--brand:__BRAND__;--strong:#073f5b;--soft:#e5f3f8;--canvas:#f5f7f9;--surface:#fff;--text:#17212b;--muted:#64717d;--border:#dfe5e9;--success:#167454}*{box-sizing:border-box}body{margin:0;background:var(--canvas);color:var(--text);font:15px/1.5 "Segoe UI",system-ui,sans-serif}.shell{max-width:960px;margin:auto;padding:24px 16px 48px}.brand{display:flex;gap:12px;align-items:center;margin-bottom:28px}.mark{display:grid;place-items:center;width:44px;height:44px;border-radius:11px;background:var(--brand);color:#fff;font-weight:800}.company-logo{display:block;max-width:210px;max-height:64px;width:auto;height:auto}.brand span{display:flex;flex-direction:column}.brand small,.muted,footer{color:var(--muted)}.hero{margin-bottom:22px}.kicker{text-transform:uppercase;letter-spacing:.08em;color:var(--brand);font-size:12px;font-weight:800}.hero h1{font-size:clamp(28px,6vw,44px);line-height:1.08;margin:5px 0 10px}.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;box-shadow:0 4px 16px rgb(23 33 43 / 5%);padding:20px;margin:16px 0}.primary{border-top:4px solid var(--brand)}.summary{display:grid;gap:18px}.date{display:flex;justify-content:space-between;gap:12px}.date strong{font-size:18px}.date b{color:var(--brand)}.facts,.grid,.payment{display:grid;gap:12px}.fact,.row{display:flex;justify-content:space-between;gap:16px;padding:9px 0;border-bottom:1px solid var(--border)}.fact:last-child,.row:last-child{border:0}.row span{color:var(--muted)}.row strong{text-align:right}.actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:18px}.button{min-height:44px;border:1px solid var(--border);border-radius:8px;background:#fff;padding:10px 16px;color:var(--text);font:inherit;font-weight:700;text-decoration:none;cursor:pointer}.button.primary{background:var(--brand);border-color:var(--brand);color:#fff}.button[disabled]{opacity:.65}.notice{background:var(--soft);border-color:#c8e3ee}.progress{counter-reset:step}.step{position:relative;padding:15px 15px 15px 52px;margin:10px 0;border:1px solid var(--border);border-radius:10px}.step:before{counter-increment:step;content:counter(step);position:absolute;left:15px;top:15px;width:25px;height:25px;border-radius:50%;display:grid;place-items:center;background:var(--brand);color:#fff;font-weight:800}.step.complete{border-color:#a8d7c3;background:#eef9f4}.step h3{margin:0 0 5px}.step p{margin:0}.secure{color:var(--success);font-weight:800}footer{display:flex;justify-content:space-between;gap:10px;font-size:12px;padding:18px 4px}@media(min-width:700px){.shell{padding:36px 28px}.grid{grid-template-columns:1fr 1fr}.payment{grid-template-columns:repeat(3,1fr)}.payment .row{display:block;border:0;border-right:1px solid var(--border)}.payment .row:last-child{border:0}.payment .row strong{display:block;text-align:left;font-size:18px}}
</style></head><body><main class="shell"><header class="brand">__BRAND_MARK__<span><strong>__COMPANY__</strong><small>Inspection details</small></span></header>
<section class="hero"><div class="kicker">__STATUS__</div><h1>__ADDRESS__</h1><p>__INTRODUCTION__</p></section>
<section class="card primary"><h2>Secure your booking</h2><p class="__SECURE_CLASS__">__SECURE_MESSAGE__</p><div class="progress">__TERMS_STEP____PAYMENT_STEP__</div></section>
<section class="card primary"><div class="date"><strong>__DATE__</strong><b>__TIME__</b></div><div class="facts"><div class="fact"><span>Duration</span><strong>__DURATION__</strong></div><div class="fact"><span>Services</span><strong>__SERVICES__</strong></div><div class="fact"><span>Inspector</span><strong>__INSPECTOR__</strong></div></div><div class="actions">__PRIMARY_ACTIONS__</div></section>
<div class="grid"><section class="card"><h2>Appointment</h2><div class="row"><span>Address</span><strong>__ADDRESS__</strong></div><div class="row"><span>Status</span><strong>__STATUS__</strong></div><div class="row"><span>Reference</span><strong>__REFERENCE__</strong></div></section><section class="card"><h2>Your inspector</h2><strong>__INSPECTOR__</strong><div class="actions">__CONTACT_ACTIONS__</div></section></div>
<section class="card"><h2>Payment summary</h2><div class="payment"><div class="row"><span>Total</span><strong>__TOTAL__</strong></div><div class="row"><span>Paid</span><strong>__PAID__</strong></div><div class="row"><span>Balance</span><strong>__BALANCE__</strong></div></div>__PAYMENT_INSTRUCTIONS__</section>
<section class="card notice"><h2>Need to change something?</h2><p>Contact __COMPANY__. Changes cannot be made from this page.</p></section><footer><span>Secure AutoMate inspection page</span><span>Expires __EXPIRES__</span></footer></main>
<script>const b=document.getElementById('confirm');if(b)b.addEventListener('click',async()=>{b.disabled=true;try{const r=await fetch('/inspection/__TOKEN__/confirm',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});if(!r.ok)throw 0;b.textContent='Receipt confirmed';}catch{b.disabled=false;b.textContent='Try confirmation again';}});</script></body></html>
"""
            .Replace("__BRAND__",H(brand)).Replace("__BRAND_MARK__",brandMark).Replace("__COMPANY__", H(company)).Replace("__STATUS__", H(status)).Replace("__ADDRESS__", H(address))
            .Replace("__CLIENT__", H(client)).Replace("__DATE__", H(date)).Replace("__TIME__", H(time))
            .Replace("__INTRODUCTION__",H(introduction)).Replace("__SECURE_CLASS__",bookingComplete?"secure":"muted").Replace("__SECURE_MESSAGE__",bookingComplete?"Booking requirements completed":"Complete the required steps below to secure your booking time.")
            .Replace("__TERMS_STEP__",TermsStep(display,terms,token)).Replace("__PAYMENT_STEP__",PaymentStep(balance)).Replace("__PAYMENT_INSTRUCTIONS__",PaymentInstructions(display,paymentInstruction,clientEmail))
            .Replace("__DURATION__", duration > 0 ? $"{duration:0} minutes" : "To be confirmed")
            .Replace("__SERVICES__", H(services.Length == 0 ? "To be confirmed" : string.Join(", ", services)))
            .Replace("__INSPECTOR__", H(display.InspectorName)).Replace("__REFERENCE__", H(PublicReference(access)))
            .Replace("__TOTAL__", Currency(total)).Replace("__PAID__", Currency(display.AmountPaid)).Replace("__BALANCE__", Currency(balance))
            .Replace("__PRIMARY_ACTIONS__", display.Cancelled ? "" : $"<button id=\"confirm\" class=\"button primary\">Confirm received</button><a class=\"button\" href=\"/inspection/{token}/calendar.ics\">Add to calendar</a>")
            .Replace("__CONTACT_ACTIONS__", ContactActions(display))
            .Replace("__EXPIRES__", H(access.ExpiresAt.ToString("d MMM yyyy", CultureInfo.GetCultureInfo("en-NZ"))))
            .Replace("__TOKEN__", token);
    }

    public static string Calendar(ClientPageAccess access, ClientInspectionDisplay display)
        => IcalendarEventRenderer.Render(access,display);

    private static string TermsStep(ClientInspectionDisplay display,string link,string token)
    {
        if(!display.TermsRequired)return "<div class=\"step complete\"><h3>Review and sign Terms</h3><p>Not required for this inspection.</p></div>";
        if(display.TermsSigned)return "<div class=\"step complete\"><h3>Review and sign Terms</h3><p>Completed.</p></div>";
        var action=string.IsNullOrWhiteSpace(link)?"<p>Terms are awaiting preparation. Contact the company if you need assistance.</p>":$"<p>Signature required.</p><div class=\"actions\"><a class=\"button primary\" href=\"/inspection/{H(token)}/terms\" rel=\"noreferrer\">Review and sign Terms</a></div>";
        return $"<div class=\"step\"><h3>Review and sign Terms</h3>{action}</div>";
    }
    private static string PaymentStep(decimal balance)=>balance<=0.005m?"<div class=\"step complete\"><h3>Make payment</h3><p>Payment completed.</p></div>":$"<div class=\"step\"><h3>Make payment</h3><p>{H(Currency(balance))} remains payable to secure the booking time.</p></div>";
    private static string PaymentInstructions(ClientInspectionDisplay display,string instruction,string clientEmail)
    {
        if(string.IsNullOrWhiteSpace(instruction))return "";var output=new StringBuilder($"<p>{H(instruction)}</p>");var showBank=!display.AccountingConnected||display.ShowBankWithAccounting;
        if(showBank&&!string.IsNullOrWhiteSpace(display.BankAccountNumber)){output.Append($"<div class=\"notice card\"><strong>Bank payment</strong><p>{H(display.BankAccountName)}<br>{H(display.BankAccountNumber)}</p>");if(!string.IsNullOrWhiteSpace(display.PaymentReferenceInstruction))output.Append($"<p>{H(display.PaymentReferenceInstruction)}</p>");output.Append("</div>");}
        else if(showBank)output.Append("<p class=\"muted\">Contact us for payment instructions.</p>");return output.ToString();
    }
    private static string SettingText(string value,string client,string email)=>string.IsNullOrWhiteSpace(value)?"":value.Replace("{{CLIENT_SALUTATION}}",client,StringComparison.OrdinalIgnoreCase).Replace("{{CLIENT_EMAIL}}",email,StringComparison.OrdinalIgnoreCase);
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
}
