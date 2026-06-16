# AutoMate Connector Progress - 2026-06-11

## Current Direction

- Continue working in the connector first.
- Keep 3D auto-detect and inspector selection.
- Keep 3D as the short-term source of truth for job creation.
- Use the connector as the setup/sync/helper app before moving the polished experience into the web app.
- Desktop build should always be overwritten at:
  - `C:\Users\Phil\Desktop\AutoMateConnector.exe`

## Product Decisions Captured

- Web app remains the long-term main AutoMate product.
- Local connector remains for 3D sync, diagnostics, and safe write-back.
- 3D-created jobs are still primary for now.
- AutoMate owns automation state, workflow status, terms/payment/report gates, templates, retries, logs, and communications.
- Zapier remains useful as an optional bridge, but core workflow rules should live in AutoMate.

## Xero Progress

- Xero is now connected through Railway as the first native accounting integration.
- OAuth connect, token storage, and tenant selection are working.
- We found and fixed the scope issues by narrowing the initial auth request and then expanding it once Xero accepted the app.
- The connector now has a manual `Create Xero Draft` action for the selected job.
- Railway can create or reuse the client contact and create a Xero draft invoice from the selected job data.
- Invoice lines are synced from THREED job invoice data into Railway and used when creating the draft invoice.
- The working mapping is:
  - contact name from client name fields, with email fallback
  - contact email from `contact1_email`
  - contact phone from `contact1_cellular`
  - due date from `job_date`
  - reference from `site_address`
  - invoice status set to draft
  - `SentToContact = false`
- We successfully created a draft invoice in Xero and confirmed it is visible there.
- Remaining Xero phases are sending, payment sync, tax/account code mapping, and webhook-driven updates.

## Connector UI Work Completed

- Inspections now opens by default after startup setup.
- Main inspection list is simplified to:
  - Job Name
  - Client
  - Inspection Address
  - Total Price
  - Terms / Agreement
  - Paid
  - Job Complete
  - Report Sent
- Job ID is hidden from the normal list but retained internally for selection/details lookup.
- Rows were tightened so the list fits inside the window.
- Mapping tools were moved away from the main inspection list into `Setup / Settings`.
- `Build Payload` was renamed to `Sync Selected Job`.
- `Check 3D Jobs` remains as a diagnostics/testing tool.

## Selected Inspection Workspace

- Reworked the selected job area into a cleaner tabbed workspace:
  - Overview
  - People
  - Services
  - Invoice
  - Automations
- Header now shows the selected job name, address/date/inspector/price, and status chips.
- Workflow and Automations were consolidated so Automations is the workflow/comms area.

## Overview Tab

- Overview now uses a two-column layout.
- Left half contains:
  - Inspection
  - Status
  - Property Details
- Right half contains Google/Street View, filling the height beside the left cards.
- Property Details currently pulls mapped 3D fields:
  - Bedrooms
  - Bathrooms
  - Age
  - Storeys
  - Building Type
  - Floor Area / m2, if an obvious local 3D column exists
  - Outbuildings
  - Occupied
  - Foundation Space
  - Weathertightness

## People Tab

- Split into separate cards for:
  - Client
  - Agent / Secondary Contact
- Local 3D contacts are preferred over API row values.
- Client phone now falls back from cellular to phone.
- Agent detection now prefers a contact whose name contains `agent`, e.g. `Test Agent`.

## Services Tab

- Services tab is now full width/full height.
- Shows primary services and add-ons as individual rows.
- Add/remove service controls live in this section.
- Changes refresh invoice totals.

## Invoice Tab

- Replaced inline/truncated invoice text with a grid:
  - Item
  - Description
  - Quantity
  - Unit Price
  - Amount
- Shows total clearly.
- Supports:
  - Add Catalogue Item
  - Add Custom Line
  - Remove Line
- Invoice total changes trigger a prompt about the difference/new invoice adjustment.

## Automations Tab

- Automations are locked by default.
- User must unlock before resend/retry controls are enabled.
- After a resend/retry action is triggered, automations lock again.
- Communications history area shows available sent/opened/status/error fields.
- True opened/viewed tracking still depends on later API/web app tracking support.

## Build Status

- Latest connector build succeeded with:
  - `0` warnings
  - `0` errors
- Latest Desktop EXE was overwritten:
  - `C:\Users\Phil\Desktop\AutoMateConnector.exe`
- Last verified Desktop timestamp:
  - `11/06/2026 9:45:27 am`

## Rollback / Checkpoints

- API repo checkpoint commit:
  - `5a363de Checkpoint AutoMate job ownership direction`
- API repo branch:
  - `checkpoint/job-ownership-before-webapp`
- Connector rollback branch created before inspections UI work:
  - `checkpoint/inspections-cleanup-before`

## Known Next Items

- Confirm the Overview layout visually in the running connector.
- Confirm whether 3D has a reliable field for floor area / m2.
- If floor area is a custom field, add it to Field Mapping settings instead of relying on guessed local column names.
- Continue improving visual polish of the tabs/card layout.
- Later: automatic sync when a new 3D job row appears, while keeping manual `Sync Selected Job` for testing.

## Latest Progress - 2026-06-14 Email Template Maker And Catalog

### Current Direction

- Continue using the API-hosted email template maker for now.
- It can be opened from the connector later, but the editor/storage should remain web/API based because templates, preview, Microsoft sending, and logo data all live in the cloud side.
- Users should not see raw HTML/code while editing templates. The template maker should show the rendered email body/editor surface.

### Catalog Decisions Captured

- Service catalog mapping was reworked around these canonical types:
  - `building_inspection`
  - `building_investigation`
  - `healthy_homes_assessment`
  - `meth_field_composite`
  - `meth_lab_composite`
  - `weathertightness`
  - `garage_outbuilding`
  - `attached_flat`
  - `property_file_review`
- `Pre-Purchase Inspection` and `Pre-Sale Inspection` map to `building_inspection`.
- `Building Investigation` stays separate because it uses a consultant agreement flow.
- `Weathertightness Report`, `Garage/Outbuilding`, `Attached Flat`, and `Review Property Files` are add-on services.
- Keep `TermsRequired` as an editable per-catalog-row flag for now. Do not hardcode service names for terms logic.

### Connector Catalog Work Done Locally

- Updated connector catalog dropdown/default mapping in:
  - `AutoMateConnector\ServiceMappingForm.cs`
  - `AutoMateConnector\ServiceMappingSettings.cs`
- Updated local saved catalog XML:
  - `C:\Users\Phil\AppData\Roaming\3D AutoMate Connector\service-mapping-v1.xml`
- Desktop connector build was replaced after catalog work:
  - `C:\Users\Phil\Desktop\AutoMateConnector.exe`
- Backup created:
  - `C:\Users\Phil\Desktop\AutoMateConnector.backup-before-catalog-recategory.exe`
- Connector build passed after changes.

### Email Template Maker Work Done Locally

- `Program.cs` on local API `main` now contains the fuller visual email template maker work:
  - visual rendered editor instead of raw HTML textarea
  - hidden HTML storage behind the editor
  - placeholder insertion into visual body or subject
  - dynamic add-on placeholders from add-on service types
  - `{{LOGO_URL}}` and `{{COMPANY_LOGO_URL}}`
  - default booking email container / Letter of Engagement shell
  - `Send Test Email` button that does not require Job ID
  - local test sends routed directly to the production Microsoft send endpoint
  - friendlier DB/auth error messages
- Test email path now should use inspector profile fields server-side for logo/company/inspector placeholders.

### Production Deploys Done

Production `origin/main` received four minimal deploy commits from branch `deploy/html-test-send`:

- `09d1e2e Send Microsoft test emails as HTML`
- `8be7f46 Use inspector logo in Microsoft test emails`
- `7f96f6f Use Pro-Spect fallback branding in test emails`
- `a896bb5 Allow local template maker test sends`

Purpose:

- Microsoft test emails are sent as rendered HTML instead of plain text/source code.
- `{{LOGO_URL}}` / `{{COMPANY_LOGO_URL}}` are substituted from `public.inspectors.logo_url` for the test-send inspector before sending.
- If inspector branding is blank, test-send falls back to Pro-Spect Building Reports Ltd and the Pro-Spect report logo, not 3D AutoMate branding.
- Production allows the local editor origin `http://127.0.0.1:5000` to call the live test-send endpoint directly, avoiding local proxy issues such as `127.0.0.1:9`.
- The `127.0.0.1:9` failure was caused by broken local proxy environment variables (`HTTP_PROXY` / `HTTPS_PROXY` / related Git proxy variables) inherited by local outbound requests.

Production health check after push:

- `https://automate-api-production.up.railway.app/db-test`
- returned database connection successful.
- Production CORS preflight for `http://127.0.0.1:5000` to `/integrations/microsoft/send-test-email` returned `204 No Content` with `access-control-allow-origin: http://127.0.0.1:5000`.

### Sender Direction

- Current Microsoft Graph email sending uses the connected inspector Microsoft account via `/me/sendMail`.
- To send from `no-reply@get3dautomate.com`, use a Microsoft 365 shared mailbox rather than spoofing the From address.
- Grant the connected AutoMate/inspector Microsoft account **Send As** permission for `no-reply@get3dautomate.com`.
- Future code change would send through the no-reply mailbox path instead of `/me/sendMail` once the shared mailbox and permissions exist.
- Chosen direction for now: shared mailbox, not a licensed mailbox or third-party email service.

### Important Git State

API repo:

- Current local branch: `main`
- Local `main` is intentionally diverged from `origin/main`.
- Observed state after work:
  - local `main` ahead by 5 commits
  - local `main` behind production by 22 commits
- Local commits include the fuller template maker/editor work:
  - `5a363de Checkpoint AutoMate job ownership direction`
  - `cb3d5c2 Improve email template maker test send`
  - `db92bfc Use inspector logo in test emails`
  - `3c60986 Use Pro-Spect fallback branding for test emails`
  - `1383c84 Route local template test sends to production`
- Do not blindly pull/merge this branch. Use care because production has many commits not present in this local branch.
- `deploy/html-test-send` tracks production and was used only for minimal production deploy patches.

Connector repo:

- Still has uncommitted local changes, including broader UI work and catalog changes.
- Do not reset or discard these.

### Current Runtime Notes

- Local API was restarted from local `main` after the deploy work.
- Local email maker URL:
  - `http://127.0.0.1:5000/email-template-maker`
- Local DB auth is still stale:
  - local `/db-test` fails with `28P01 password authentication failed for user "postgres"`
- Because of stale local DB auth, local job preview/save/load against Postgres is still blocked.
- Test send can still work from the local page because it calls the production send endpoint directly.

### Next Best Step

1. Refresh local `DATABASE_PUBLIC_URL` so local preview/save/load can use real jobs.
2. Send another test email from the local email maker and confirm:
   - email renders as polished HTML
   - logo is the inspector logo from `inspectors.logo_url`, or Pro-Spect fallback branding if that field is blank
   - no raw `<!DOCTYPE html>` source appears
3. If logo is still wrong, inspect/update the production `public.inspectors.logo_url` value for the selected inspector.
4. Decide how to reconcile local API `main` with production `origin/main` before any larger push of the visual editor work.
5. Keep the connector catalog changes; later commit connector work deliberately once visually confirmed.

## Progress Update - 2026-06-17

### Railway / GitHub Access

- Railway CLI is now authenticated as Phil Smith and linked in this repo to:
  - Project: `observant-patience`
  - Environment: `production`
  - Service: `automate-api`
- Railway deployment/log commands now work directly from `C:\Users\Phil\source\repos\automate-api`.
- GitHub remote is available as:
  - `origin https://github.com/3DAutoMate/automate-api`
- API deploys were pushed directly to `origin/main` from local branch `xero-routes-on-remote`.

### Schedule Job / Google Calendar

- Added production API orchestration endpoint:
  - `POST /jobs/{jobId}/schedule`
- Added Google Calendar integration endpoints:
  - `GET /integrations/google/connect-url`
  - `GET /api/integrations/google/callback`
  - `GET /integrations/google/status`
- Schedule Job now attempts the first scheduling bundle:
  - booking emails via Microsoft mail integration
  - Xero draft invoice creation/reuse
  - Google Calendar event creation/reuse
  - SignNow/terms currently returns a clear not-configured result
- Connector has a new `Schedule Job` toolbar button.
- Connector integrations window now includes:
  - Google Calendar connection/status
  - Scheduling mode selector: `Manual`, `Hybrid`, `Automatic`
- Scheduling mode is stored locally in `IntegrationSettings.xml`; only Manual behavior is wired so far.

### Hard Reset For Testing

- Added testing/admin endpoint:
  - `POST /jobs/{jobId}/hard-reset`
- Added connector toolbar button:
  - `Hard Reset`
- Purpose:
  - clear Railway job workflow/payment/test state back to pending/not sent/unpaid
  - do not delete external Xero invoices, Google Calendar events, emails, or future agreement documents
- Current hard reset behavior resets only `jobs_staging`.
- It no longer touches `job_workflow_actions`, because production did not have that table and the earlier version returned 500.
- Latest deployed API commit for this fix:
  - `fdb1ca0 Simplify hard reset to staged job state`
- Railway deployment confirmed latest successful deployment uses commit `fdb1ca0`.

### Desktop Connector

- Desktop executable was rebuilt and copied to:
  - `C:\Users\Phil\Desktop\AutoMateConnector.exe`
- Running connector instances may lock the Desktop exe; close/stop `AutoMateConnector` before copying a new build.
- Correct connector source tree remains:
  - `C:\Users\Phil\source\repos\3D-AutoMate\src\Connector\AutoMateConnector`
- The connector source tree does not appear to be a git repo from this workspace, so connector source edits are local file changes.

### Production Commits Pushed

- `011cc75 Add schedule job orchestration`
- `4084060 Add hard reset job endpoint`
- `bc5e309 Harden hard reset workflow action reset`
- `d162506 Avoid missing workflow actions table on hard reset`
- `fdb1ca0 Simplify hard reset to staged job state`

### Current Known Issues / Next Steps

1. Retry `Hard Reset` from the connector against the current production deployment.
2. If Hard Reset still returns 500, use Railway logs immediately:
   - `railway logs --http --status 500..599 --lines 20 --json`
   - `railway logs --lines 80 --filter "Hard reset"`
3. Add a safer diagnostic/version endpoint later so the connector can show which API commit is live.
4. Implement actual SignNow agreement integration before treating Schedule Job as fully complete.
5. Decide whether booking emails should use the new email template storage/send endpoints once those are fully ported to `automate-api`.

## Progress Update - 2026-06-17 - SignNow Integration Build

### Google / Railway Setup

- Google Railway variables are now present in production:
  - `GOOGLE_CLIENT_ID`
  - `GOOGLE_CLIENT_SECRET`
  - `GOOGLE_REDIRECT_URI`
- `GOOGLE_REDIRECT_URI` is:
  - `https://automate-api-production.up.railway.app/api/integrations/google/callback`

### SignNow API Work Implemented Locally

- Added company-level SignNow OAuth/config support in `automate-api`.
- Railway variable set:
  - `SIGNNOW_REDIRECT_URI=https://automate-api-production.up.railway.app/api/integrations/signnow/callback`
- User still needs to add in Railway:
  - `SIGNNOW_CLIENT_ID`
  - `SIGNNOW_CLIENT_SECRET`
- Added API endpoints:
  - `GET /integrations/signnow/connect-url`
  - `GET /api/integrations/signnow/callback`
  - `GET /integrations/signnow/status`
  - `GET /integrations/signnow/templates`
  - `GET /integrations/signnow/template-mappings`
  - `POST /integrations/signnow/template-mappings`
  - `POST /integrations/signnow/jobs/{jobId}/send-terms`
  - `POST /integrations/signnow/jobs/{jobId}/refresh-status`
  - `POST /api/integrations/signnow/webhook`
- Added Railway job tracking fields:
  - `terms_signed`
  - `terms_signed_at`
  - `signnow_document_id`
  - `signnow_invite_id`
  - `signnow_template_id`
  - `signnow_document_status`
  - `signnow_last_checked_at`
  - `signnow_signing_link`
- Added `signnow_template_mappings` table for service/template-key to SignNow-template mapping.
- Replaced the Schedule Job SignNow placeholder with `SendSignNowTermsForJobAsync`.
- Send behavior:
  - skips if terms are not required
  - skips if already sent/signed unless manual resend is used
  - fails clearly if client email is missing
  - fails clearly if no SignNow template mapping exists
  - sends only to client/contact1
  - attempts to prefill SignNow fields:
    - `JobID`
    - `Address of Property to be inspected`
    - `Full name`
- Signed tracking:
  - webhook endpoint updates `terms_signed` where possible
  - manual refresh endpoint checks document status from SignNow

### Connector App Work Implemented Locally

- Added `Setup / Settings > SignNow Templates`.
- Added new WinForms setup form:
  - `SignNowTemplateMappingForm.cs`
- Setup form includes:
  - connection status
  - `Connect SignNow`
  - `Refresh Templates`
  - service/template-key mapping grid
  - `Save Mappings`
  - scrollable details/error box so long messages are not clipped
- Updated Automations tab:
  - Terms button now shows `Send / Retry`
  - Terms action calls:
    - `/integrations/signnow/jobs/{jobId}/send-terms`
    - then `/integrations/signnow/jobs/{jobId}/refresh-status`
- Added tooltips for automation status labels so long failure text is still readable.

### Verification Completed

- API build passed:
  - `dotnet build`
- Connector build passed:
  - `dotnet build AutoMateConnector\AutoMateConnector.csproj`
- Railway variable-name check shows:
  - `SIGNNOW_REDIRECT_URI`
- SignNow client ID/secret are intentionally not set yet.

### Current Local File State

- `automate-api` changed:
  - `Program.cs`
- `automate-api` still has untracked notes:
  - `AutoMateConnector-Progress-2026-06-11.md`
  - `Xero-Chat-Summary-2026-06-16.md`
- `AutoMateConnector` changed by this SignNow pass:
  - `AutoMateConnector/Form1.cs`
  - `AutoMateConnector/AutoMateConnector.csproj`
  - `AutoMateConnector/SignNowTemplateMappingForm.cs`
- `AutoMateConnector` also already had other modified/untracked connector files before/around this work; do not assume they are all SignNow changes.

### Next Steps

1. In SignNow developer/app settings, add redirect URI:
   - `https://automate-api-production.up.railway.app/api/integrations/signnow/callback`
2. Add Railway production variables:
   - `SIGNNOW_CLIENT_ID`
   - `SIGNNOW_CLIENT_SECRET`
3. Commit and deploy `automate-api`.
4. Rebuild/copy connector executable after confirming connector source location.
5. Open connector:
   - `Setup / Settings > SignNow Templates`
   - connect SignNow
   - refresh templates
   - map service/template keys
   - save mappings
6. Test a selected job:
   - Sync Selected Job
   - Schedule Job or Terms `Send / Retry`
   - confirm SignNow document is sent to the client
   - confirm Railway shows `terms_sent`
   - sign test document and verify webhook/manual refresh updates `terms_signed`
