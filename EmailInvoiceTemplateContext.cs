using System.Globalization;
using System.Text;
using Npgsql;

namespace AutoMateApi;

/// <summary>
/// Loads and formats the invoice portion of an email-template context.
/// This helper is read-only and performs no schema creation or external calls.
/// </summary>
public static class EmailInvoiceTemplateContext
{
    public const int IndexedLineLimit = 20;

    private static readonly CultureInfo NewZealandCulture = CultureInfo.GetCultureInfo("en-NZ");

    public static async Task<EmailInvoiceTemplateContextResult?> LoadAsync(
        NpgsqlConnection connection,
        Guid jobId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        const string jobSql = """
            SELECT job_total, amount_paid
            FROM public.jobs_staging
            WHERE job_id = @job_id
            LIMIT 1;
            """;

        decimal invoiceTotal;
        decimal amountPaid;

        await using (var jobCommand = new NpgsqlCommand(jobSql, connection))
        {
            jobCommand.Parameters.AddWithValue("job_id", jobId);
            await using var reader = await jobCommand.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return null;

            invoiceTotal = reader.IsDBNull(0) ? 0m : reader.GetDecimal(0);
            amountPaid = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
        }

        const string linesSql = """
            SELECT line_index, description, quantity, unit_price
            FROM public.job_invoice_lines
            WHERE job_id = @job_id
            ORDER BY line_index;
            """;

        var lines = new List<EmailInvoiceTemplateLine>();
        await using (var linesCommand = new NpgsqlCommand(linesSql, connection))
        {
            linesCommand.Parameters.AddWithValue("job_id", jobId);
            await using var reader = await linesCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var quantity = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2);
                var unitPrice = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3);
                lines.Add(new EmailInvoiceTemplateLine(
                    reader.GetInt32(0),
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    quantity,
                    unitPrice,
                    quantity * unitPrice));
            }
        }

        var tokens = BuildTokens(invoiceTotal, amountPaid, lines);
        return new EmailInvoiceTemplateContextResult(
            invoiceTotal,
            amountPaid,
            invoiceTotal - amountPaid,
            lines,
            tokens);
    }

    public static IReadOnlyDictionary<string, string> BuildTokens(
        decimal invoiceTotal,
        decimal amountPaid,
        IReadOnlyList<EmailInvoiceTemplateLine> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);

        var tokens = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["INVOICE_TOTAL"] = FormatMoney(invoiceTotal),
            ["AMOUNT_PAID"] = FormatMoney(amountPaid),
            ["BALANCE_DUE"] = FormatMoney(invoiceTotal - amountPaid),
            ["INVOICE_LINE_ITEMS"] = BuildLineItemsHtml(lines)
        };

        for (var position = 1; position <= IndexedLineLimit; position++)
        {
            var line = position <= lines.Count ? lines[position - 1] : null;
            tokens[$"INVOICE_LINE_{position}_DESCRIPTION"] = line == null
                ? string.Empty
                : line.Description ?? string.Empty;
            tokens[$"INVOICE_LINE_{position}_QUANTITY"] = line == null
                ? string.Empty
                : FormatQuantity(line.Quantity);
            tokens[$"INVOICE_LINE_{position}_UNIT_PRICE"] = line == null
                ? string.Empty
                : FormatMoney(line.UnitPrice);
            tokens[$"INVOICE_LINE_{position}_TOTAL"] = line == null
                ? string.Empty
                : FormatMoney(line.LineTotal);
        }

        return tokens;
    }

    public static string BuildLineItemsHtml(IReadOnlyList<EmailInvoiceTemplateLine> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);

        if (lines.Count == 0)
            return string.Empty;

        var html = new StringBuilder();
        html.Append("<table role=\"presentation\" style=\"width:100%;border-collapse:collapse\">");
        html.Append("<thead><tr>");
        AppendHeader(html, "Description");
        AppendHeader(html, "Quantity");
        AppendHeader(html, "Unit price");
        AppendHeader(html, "Total");
        html.Append("</tr></thead><tbody>");

        foreach (var line in lines)
        {
            html.Append("<tr>");
            AppendCell(html, line.Description ?? string.Empty, false);
            AppendCell(html, FormatQuantity(line.Quantity), true);
            AppendCell(html, FormatMoney(line.UnitPrice), true);
            AppendCell(html, FormatMoney(line.LineTotal), true);
            html.Append("</tr>");
        }

        html.Append("</tbody></table>");
        return html.ToString();
    }

    private static string FormatMoney(decimal value) => value.ToString("C", NewZealandCulture);

    private static string FormatQuantity(decimal value) => value.ToString("0.##", NewZealandCulture);

    private static void AppendHeader(StringBuilder html, string value)
    {
        html.Append("<th style=\"padding:6px;border-bottom:1px solid #ccc;text-align:left\">");
        html.Append(System.Net.WebUtility.HtmlEncode(value));
        html.Append("</th>");
    }

    private static void AppendCell(StringBuilder html, string value, bool alignRight)
    {
        html.Append("<td style=\"padding:6px;border-bottom:1px solid #eee;");
        if (alignRight)
            html.Append("text-align:right;");
        html.Append("\">");
        html.Append(System.Net.WebUtility.HtmlEncode(value));
        html.Append("</td>");
    }
}

public sealed record EmailInvoiceTemplateLine(
    int LineIndex,
    string Description,
    decimal Quantity,
    decimal UnitPrice,
    decimal LineTotal);

public sealed record EmailInvoiceTemplateContextResult(
    decimal InvoiceTotal,
    decimal AmountPaid,
    decimal BalanceDue,
    IReadOnlyList<EmailInvoiceTemplateLine> Lines,
    IReadOnlyDictionary<string, string> Tokens);
