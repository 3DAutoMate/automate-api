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
