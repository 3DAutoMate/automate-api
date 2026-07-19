# AutoMate Workspace Log

Last updated: 2026-06-09

Purpose: keep one plain-English map of all AutoMate-related folders, launchers, notes, and local-only config so the project does not drift across duplicate locations.

## Canonical Folders

Use these as the main working locations going forward.

| Area | Canonical path | Purpose |
| --- | --- | --- |
| Railway/API | `C:\Users\Phil\source\repos\automate-api` | Cloud API source. Main file: `Program.cs`. Current `withemail` API build comes from here. |
| Desktop connector | `C:\Users\Phil\source\repos\AutoMateConnector` | Windows connector app source. Main UI file: `AutoMateConnector\Form1.cs`. |
| 3D companion | `C:\Users\Phil\source\repos\ThreeDInspection12Companion` | Separate 3D Inspection 12 companion project. Not part of AutoMate API/connector. |

## Legacy Or Duplicate Folders

Do not delete or move these until they have been compared and archived deliberately.

| Path | Current role |
| --- | --- |
| `C:\Users\Phil\AutoMateAPI` | Older API working folder. Contains useful notes and the existing local-only `appsettings.Local.json` that was copied into the canonical API repo on 2026-06-09. |
| `C:\Users\Phil\AutoMateAPI - Backup` | Older API backup folder. Keep until the canonical repo has been verified and backed up. |

## Desktop Launchers And Files

| Path | Purpose |
| --- | --- |
| `C:\Users\Phil\Desktop\AutoMateConnector.exe` | Current Desktop test launcher for the connector app. |
| `C:\Users\Phil\Desktop\withemail.lnk` | Shortcut for the API build with email-template work. Points to `run-withemail.ps1` in the canonical API repo. |
| `C:\Users\Phil\Desktop\withemail.log` | Startup log written by `run-withemail.ps1`. |
| `C:\Users\Phil\Desktop\3D AutoMate` | Desktop asset/backup folder, including connector backups and logo assets. |
| `C:\Users\Phil\Desktop\3DInspectionCompanion-Handover.md` | Separate companion-project handover note. |

## Local Config And Secrets

Secrets must remain local-only and must not be committed.

| File | Rule |
| --- | --- |
| `C:\Users\Phil\source\repos\automate-api\appsettings.Local.json` | Local API secrets for the canonical API repo. Contains `DATABASE_PUBLIC_URL`. Ignored by `.gitignore`. |
| `C:\Users\Phil\source\repos\automate-api\appsettings.Development.json` | Local development config. Ignored by `.gitignore`. |
| `C:\Users\Phil\AutoMateAPI\appsettings.Local.json` | Older source of the local DB config. Keep as historical fallback until consolidation is complete. |

The API now loads configuration in this order:

1. standard ASP.NET config
2. `appsettings.Local.json`
3. environment variables

Production still uses Railway environment variables. Required production variable: `DATABASE_PUBLIC_URL`.

## Current API Launcher State

`withemail.lnk` runs:

```text
C:\Users\Phil\source\repos\automate-api\run-withemail.ps1
```

The wrapper:

- writes startup output to `C:\Users\Phil\Desktop\withemail.log`
- keeps the PowerShell window open on startup errors
- accepts `DATABASE_PUBLIC_URL` from either Windows environment variables or `appsettings.Local.json`

## Consolidation Plan

1. Treat `C:\Users\Phil\source\repos\automate-api` as the canonical API repo.
2. Treat `C:\Users\Phil\source\repos\AutoMateConnector` as the canonical connector repo.
3. Keep Desktop files as launchers/assets only, not source-of-truth code.
4. Copy useful notes from `C:\Users\Phil\AutoMateAPI\PROJECT_NOTES.md` into a canonical local notes file later.
5. Compare old `C:\Users\Phil\AutoMateAPI` against canonical `automate-api` before archiving it.
6. After comparison, move legacy folders into a clearly named archive folder instead of deleting them.

## Do Not Move Yet

These need deliberate update/checks before any folder moves:

- Desktop shortcuts
- connector backup folder
- local-only config files
- Visual Studio solution/user files
- any Railway/GitHub deployment assumptions

## Latest Change Log

2026-06-09:

- Added email-template API endpoints to canonical API repo.
- Added `run-withemail.ps1` API launcher wrapper.
- Copied local-only `appsettings.Local.json` from old API folder into canonical API repo.
- Added `.gitignore` protection for local config and generated build folders.
- Verified an `AutoMateAPI.exe` process can start from the canonical API build after local config fix.
- Product direction clarified: AutoMate should feel like one product, preferably a single 3D AutoMate web app for daily use. The local connector should become a background sync/write-back helper, not a second user-facing app. Users should open one place for jobs, email templates, service mappings, workflow actions, integrations, and account settings. Technically the local helper remains necessary because the web app cannot directly read/write each inspector's local THREED SQL Server, but it should be treated as plumbing rather than a separate product.

## Current Handoff - 2026-06-09

Read this before continuing AutoMate work.

### What Was Built Today

API repo:

- Active API workspace confirmed as `C:\Users\Phil\source\repos\automate-api`.
- Legacy API folder `C:\Users\Phil\AutoMateAPI` is reference only.
- Added a first API-hosted email template maker at:

```text
http://127.0.0.1:5000/email-template-maker
```

- Added email template storage endpoint support.
- Added template preview/send endpoints.
- Added grouped placeholders:
  - Job Details
  - Service Items
  - Client Details
  - Agent Details
  - Inspector Details
  - Links
- Added service type choices for booking templates:
  - `general_booking`
  - `pre_purchase`
  - `building_investigation`
  - `healthy_homes_assessment`
  - `meth_field_composite`
  - etc.
- Changed template storage direction so booking email templates can be saved per inspector and per `service_type_key`.
- Added mapped service fields to `jobs_staging`:
  - `primary_service_key`
  - `additional1_service_key`
  - `additional2_service_key`
  - `booking_template_key`
  - workflow required flags

Connector repo:

- Updated Mapping Preview in `C:\Users\Phil\source\repos\AutoMateConnector`.
- Added a new section:

```text
Selected job THREED item mapping preview
```

- This reads selected job invoice/catalog items from `tblJobInvoice` joined to `tblItem`.
- It shows `@` / `+` THREED items mapped to:
  - Mapping Kind
  - Canonical Type
  - Booking Template
  - workflow required flags
- Connector build passed.
- Desktop `AutoMateConnector.exe` was replaced with the fresh build.
- Backup made:

```text
C:\Users\Phil\Desktop\AutoMateConnector.backup-before-item-mapping-preview.exe
```

### Important Problems Found

The local API config is stale:

- `appsettings.Local.json` exists and loads correctly.
- But its `DATABASE_PUBLIC_URL` password is no longer accepted by Railway Postgres.
- Local `/db-test` currently fails with:

```text
28P01: password authentication failed for user "postgres"
```

- Production `/db-test` still succeeds:

```text
https://automate-api-production.up.railway.app/db-test
```

Conclusion:

- Production Railway env vars are correct.
- Local secret file is outdated.
- Railway CLI is installed but not logged in:

```text
railway status
Unauthorized. Please login with railway login
```

Do not waste time debugging email preview until the local DB URL is refreshed, or use production endpoints instead.

### Lessons Learned

- Building an API-served editor page was useful for prototyping, but it is not the final product UX.
- The browser page cannot truly know the "logged-in 3D AutoMate inspector" unless it has a real AutoMate session or is opened from the connector with context.
- Inspector ID should not be a visible user field.
- The correct product model is:

```text
THREED invoice/list items
-> connector item mapping
-> canonical AutoMate service type
-> booking email template selection
```

- Email placeholders should be generated from the mapped AutoMate job payload, not a disconnected hardcoded concept.
- Mapping Preview is the right place to verify whether a job will choose the correct booking template.
- "Canonical type" is developer language. User-facing wording should be "Email Type" and start with only:

```text
Booking email
```

- Service type is what matters for automation:

```text
pre_purchase job -> pre_purchase booking email template
```

### Product Direction

Avoid creating three visible things for the user.

The target product should feel like:

```text
3D AutoMate = one web app
```

The connector should become:

```text
Local Sync Agent / background helper
```

The connector remains technically necessary because THREED is a local SQL Server database and the cloud cannot directly access it. But it should not feel like a separate app where the user manages templates/workflows/settings.

Decision recorded 2026-06-10:

- The solid long-term direction is a 3D AutoMate web app as the main product, plus a hidden/lightweight Windows connector service on each user's PC.
- Do not double down on the current Windows app as the main daily UI.
- The current Windows app/connector is a bridge and early testing surface. Over time, reduce it to setup, sync status, diagnostics, and safe local THREED write-back.
- The web app should be the place users manage jobs, workflow actions, email templates, service mappings, Microsoft integration, account/subscription, logs, and optional webhooks.
- The least-steps/best-UX model is: user opens the web app; the connector runs quietly in the background.
- Future native AutoMate automation should replace Zapier gradually.
- Keep optional outgoing webhooks for users who want advanced/custom integrations beyond native AutoMate workflows.
- Future-proofing priority: centralize automation, settings, integrations, account state, and support visibility in the cloud; keep only local THREED access and local write-back inside the connector.

Recommended architecture:

```text
THREED local SQL Server
-> Windows Local Sync Agent
-> 3D AutoMate cloud API/Postgres
-> 3D AutoMate web app
-> native AutoMate workflow workers
-> optional outgoing webhooks
```

Why this direction wins:

- Best user experience: one visible product instead of a desktop tool plus separate cloud automation pieces.
- Most reliable support model: cloud can show connector heartbeat, last sync, connector version, failed jobs, account state, and integration errors.
- Easier updates: most UI/workflow improvements ship to the web app/cloud once instead of requiring every user to update a desktop build.
- Better long-term compatibility: works for teams, admins, office staff, mobile/tablet viewing, and future integrations.
- Best path away from Zapier: native workflow workers can replace Zapier one workflow at a time while webhooks remain available for expansion.

Main trade-offs:

- A Windows component is still required while THREED data lives in local SQL Server.
- Connector registration, authentication, queued sync, and safe cloud-to-local write-back need deliberate design.
- The connector should be boring and reliable: sync, heartbeat, diagnostics, and write-back only.

Best future UX:

- User opens one 3D AutoMate app.
- They manage:
  - jobs
  - booking email templates
  - service mappings
  - workflow actions
  - Microsoft integration
  - account/subscription
- Local helper quietly syncs THREED and performs safe local write-back.

### Report Delivery Direction

Decision recorded 2026-06-10:

- Do not make SharePoint a required dependency for report delivery.
- Default future model: 3D AutoMate hosts report PDFs itself using cloud object storage.
- SharePoint, Microsoft, and other storage destinations should be optional integrations only.
- The hidden Windows connector/local sync agent remains responsible for detecting report PDFs saved by the PC-based report writer.
- The cloud app owns the report delivery workflow, client links, status tracking, release rules, and email sending.

Recommended report flow:

```text
Inspector saves/exports report PDF on PC
-> Windows Local Sync Agent detects completed PDF
-> connector matches PDF to the correct THREED/AutoMate job
-> connector uploads PDF to 3D AutoMate cloud storage
-> Postgres stores report metadata/status/audit trail
-> 3D AutoMate sends branded secure client link
-> optional webhook/Microsoft/SharePoint integrations run if configured
```

Why this direction wins:

- Works for all users, not only Microsoft/SharePoint users.
- Avoids SharePoint permission, tenant, sharing-policy, and token reliability problems as core product dependencies.
- Gives clients one consistent branded 3D AutoMate report delivery experience.
- Allows 3D AutoMate to track views/downloads, report release rules, resend status, payment/agreement gates, and support logs.
- Keeps SharePoint useful for customers who want it, without making it required.

Connector report responsibilities:

- Watch one or more configured report export folders.
- Wait until the PDF is fully written/unlocked before upload.
- Match the PDF to a job using job number/name/date/client/report metadata where possible.
- Avoid duplicate uploads using file hash/checksum and job/report status.
- Retry uploads safely if the internet/API is unavailable.
- Ask for manual match only when confident auto-matching is not possible.
- Report upload/sync status back to the web app.

### Payment Direction

Decision recorded 2026-06-10:

- Use Stripe for online/card payments.
- Each inspection business should connect its own Stripe account, likely via Stripe Connect, so payments settle to the inspector/business rather than 3D AutoMate holding client funds.
- Bank transfer should remain supported by showing the inspection business's bank details on the invoice/payment page.
- Add future integrations for Xero and MYOB so invoice/payment state can sync with common accounting systems.
- Jobs must include a manual `paid` checkbox/action for cases where payment is received outside Stripe, Xero, or MYOB.
- Payment status should be usable as a workflow gate, especially for report release rules.

Recommended payment model:

```text
3D AutoMate invoice/payment page
-> Stripe payment if connected
-> bank transfer instructions if enabled
-> optional Xero/MYOB sync if configured
-> manual paid action for offline payments
-> payment status controls report release automation
```

Important product rules:

- Do not require Stripe for every user; allow bank transfer/manual paid workflows.
- Do not require Xero or MYOB; treat them as optional integrations.
- Show payment source/status clearly on each job: unpaid, paid manually, paid by Stripe, paid in Xero/MYOB, failed, refunded/part-refunded if supported later.
- Keep a payment audit trail: who marked paid, when, source, amount, invoice/reference, and any integration event IDs.

### Next Best Step

Before continuing template preview work:

1. Refresh local `DATABASE_PUBLIC_URL`.
   - Either log in to Railway CLI and pull current variable.
   - Or manually copy the current Railway `DATABASE_PUBLIC_URL` into local `appsettings.Local.json`.
   - Do not print the secret into chat or commit it.
2. Decide whether to keep iterating on the prototype editor or start a proper 3D AutoMate web app shell.
3. If continuing the prototype:
   - Use selected job to resolve inspector and service type.
   - Load/save only Booking email templates.
   - Save by `service_type_key`.
   - Preview against real staged jobs once DB auth works.
4. If moving toward the real product:
   - Scaffold web app UI.
   - Build Email Templates page there.
   - Treat connector as sync agent.

### Current Git/Workspace Notes

API repo has local changes:

- `Program.cs`
- `.gitignore`
- `AUTOMATE_WORKSPACE_LOG.md`
- `run-withemail.ps1`
- local ignored `appsettings.Local.json`

Connector repo has local changes:

- `AutoMateConnector\Form1.cs`

Generated folders may exist and should not be treated as source:

- `.vs/`
- `bin/`
- `obj/`
