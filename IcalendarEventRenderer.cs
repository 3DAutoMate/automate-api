using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace AutoMateApi;

public static class IcalendarEventRenderer
{
    public static string Render(ClientPageAccess access, ClientInspectionDisplay display)
    {
        using var document = JsonDocument.Parse(access.SnapshotJson);
        var root = document.RootElement;
        return Render(new(access.TenantId,access.JobId,access.ApprovedVersion,ParseInstant(Text(root,"jobDate"))??DateTimeOffset.UtcNow,
            Math.Max(1,(int)Number(root,"durationMinutes")),Text(root,"address"),display.CompanyName,display.Cancelled));
    }

    public static string Render(IcalendarEventInput input)
    {
        var method = input.Cancelled ? "CANCEL" : "PUBLISH";
        var status = input.Cancelled ? "CANCELLED" : "CONFIRMED";
        var uid = StableUid(input.TenantId,input.JobId);
        var lines = new[]
        {
            "BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//AutoMate//Inspection//EN","CALSCALE:GREGORIAN",
            "METHOD:"+method,"BEGIN:VEVENT","UID:"+uid,"SEQUENCE:"+Math.Max(0,input.Sequence),
            "DTSTAMP:"+Utc(DateTimeOffset.UtcNow),"DTSTART:"+Utc(input.StartsAt),"DTEND:"+Utc(input.StartsAt.AddMinutes(Math.Max(1,input.DurationMinutes))),
            "STATUS:"+status,"SUMMARY:"+Escape("Property inspection - "+input.CompanyName),
            "LOCATION:"+Escape(input.Address),"DESCRIPTION:"+Escape("Inspection details from "+input.CompanyName),
            "END:VEVENT","END:VCALENDAR"
        };
        return string.Join("\r\n",lines.Select(Fold))+"\r\n";
    }

    private static string StableUid(Guid tenantId,Guid jobId)
    {
        var bytes=SHA256.HashData(Encoding.UTF8.GetBytes(tenantId.ToString("N")+":"+jobId.ToString("N")));
        return Convert.ToHexString(bytes)[..32].ToLowerInvariant()+"@3dautomate.co.nz";
    }
    private static string Utc(DateTimeOffset value)=>value.UtcDateTime.ToString("yyyyMMdd'T'HHmmss'Z'",CultureInfo.InvariantCulture);
    private static DateTimeOffset? ParseInstant(string value)=>DateTimeOffset.TryParse(value,CultureInfo.InvariantCulture,DateTimeStyles.RoundtripKind,out var result)?result:null;
    private static string Text(JsonElement root,string key)=>root.TryGetProperty(key,out var value)?value.ToString().Trim():"";
    private static decimal Number(JsonElement root,string key)=>root.TryGetProperty(key,out var value)&&decimal.TryParse(value.ToString().Trim('"'),NumberStyles.Any,CultureInfo.InvariantCulture,out var number)?number:0;
    private static string Escape(string? value)=>(value??"").Replace("\\","\\\\").Replace("\r","").Replace("\n","\\n").Replace(",","\\,").Replace(";","\\;");

    internal static string Fold(string value)
    {
        const int limit=75;if(Encoding.UTF8.GetByteCount(value)<=limit)return value;
        var result=new StringBuilder();var currentBytes=0;
        foreach(var rune in value.EnumerateRunes())
        {
            var text=rune.ToString();var bytes=Encoding.UTF8.GetByteCount(text);
            if(currentBytes+bytes>limit){result.Append("\r\n ");currentBytes=1;}
            result.Append(text);currentBytes+=bytes;
        }
        return result.ToString();
    }
}

public sealed record IcalendarEventInput(Guid TenantId,Guid JobId,int Sequence,DateTimeOffset StartsAt,int DurationMinutes,string Address,string CompanyName,bool Cancelled);
