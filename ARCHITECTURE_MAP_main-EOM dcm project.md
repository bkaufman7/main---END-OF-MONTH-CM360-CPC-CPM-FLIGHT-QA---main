TITLE:
ARCHITECTURE MAP

1. One-Page System Map
System purpose:
- Automate CM360 QA operations: ingest report attachments, detect anomalies, route actionable outputs, and close the loop via operator replies.

Main inputs:
- Gmail messages/threads with report attachments (CSV, ZIP->CSV).
- Spreadsheet tabs: Networks, Monitored Networks, Advertisers to ignore, EMAIL LIST.
- Operator replies (plain text commands/notes).
- Script/Document properties state.

Main outputs:
- Raw Data sheet (normalized ingest).
- Violations sheet (rule results + owner attribution).
- Handled Placements / Removed Networks / Monthly Overages / hidden cache sheets.
- Summary + alert emails (HTML + XLSX attachment).
- Audit/failure logs and admin notifications.

Core modules (Code.js):
- UI and control plane: onOpen, status/reset, manual launchers.
- Ingestion: importDCMReports, processCSV, network auto-add/sync.
- Rule engine: runQAOnly + low-priority classifier + stale/change tracking.
- Notification engine: sendEmailSummaryChunked_ staged renderer and sender.
- Reply/command processor: processEmailReplies + network removal commands.
- State/resume: QA/email state keys + trigger scheduling/cancellation.
- Alert sideflows: performance spike, mid-flight drop.
- Persistence helpers: sidecar violation cache workbook and hidden local sheets.

Execution flow:
- Trigger or UI action -> ingest -> QA chunk engine -> staged email assembly -> send + cleanup.
- Reply trigger -> parse commands -> update handled/removal state -> impacts next QA/email cycles.

Operator touchpoints:
- Spreadsheet custom menu commands.
- Sheet configuration edits.
- Email reply commands:
  - placement handling note + placement IDs
  - REMOVE NETWORK <id>

External dependencies:
- Google Apps Script services: SpreadsheetApp, GmailApp, MailApp, DriveApp, UrlFetchApp, ScriptApp, LockService, PropertiesService.
- Manifest-enabled advanced services: Drive v3, Gmail v1.
- CLASP project linkage (.clasp.json).

2. File Responsibility Matrix

| File | Responsibility | Depends On | Used By | Notes |
|---|---|---|---|---|
| .clasp.json | CLASP binding/config | CLASP tooling | Dev/deploy workflow | Holds scriptId and extension rules |
| .gitignore | Git hygiene | Git | Developers | Excludes local noise |
| appsscript.json | Runtime/permission manifest | Apps Script runtime | Apps Script execution | Enables Drive/Gmail advanced services |
| campaign extractor.js | Separate campaign mapping importer | GmailApp, SpreadsheetApp, Utilities | Manual/script runs | Adjacent helper flow; not core pipeline |
| Code.js | Primary application runtime | All Apps Script services + sheet schema | All triggers/UI/manual operations | Monolithic core with 100+ functions |
| filter.js | Regex taxonomy constants (CT_RULES) | JS regex engine | Potential rule dev usage | Not wired to main rule path directly |
| README.md | System/process documentation | N/A | Operators/maintainers | Rich architecture + workflow doc |
| TROUBLESHOOTING.md | Incident/runbook guidance | N/A | Operators/maintainers | Failure patterns and diagnostics |
| PROJECT_DOSSIER_main-EOM dcm project.md | Deep intelligence report artifact | Repo contents | Humans/AI handoff | Generated analysis artifact |

3. Entry Point Map

| Entry Point | Type | First Function Called | Downstream Functions | Final Outputs |
|---|---|---|---|---|
| Spreadsheet open | UI trigger | onOpen | menu action bindings | Custom menu available |
| Run It All (Immediate) | UI action/manual | runItAll | trimAllSheetsToData_ -> importDCMReports -> runQAOnly -> alerts -> sendEmailSummary | Updated sheets + emails |
| Run It All (Auto-Resume) | UI action/manual | runItAllChunked | runItAllMorning | Ingest + QA/alerts; email separate |
| Pull Data (Immediate) | UI action/manual | importDCMReports | processNetworkRemovalRequests -> processCSV -> syncMonitoredNetworks_ -> autoAddNewNetworks_ | Raw Data + network tabs updated |
| Pull Data (Auto-Resume) | UI action/manual | importDCMReportsChunked | importDCMReports | Same as above |
| Run QA Only (Auto-Resume) | UI action/trigger/manual | runQAOnly | load config/maps -> rule evaluation -> cache update -> scheduleNextQAChunk_ | Violations rows + QA state/trigger |
| Run QA Only (Immediate) | UI action/manual | runQAOnlyImmediate | initializes state -> runQAOnly | Violations rows |
| Send Email Only (Auto-Resume) | UI action/trigger/manual | sendEmailSummary | sendEmailSummaryChunked_ staged sections | Summary email + XLSX + state cleanup |
| Send Email Only (Immediate) | UI action/manual | sendEmailSummaryImmediate | sendEmailSummaryChunked_ | Same as above |
| FORCE Send Email Now | UI action/manual | sendEmailNow | clear state -> sendEmailSummaryChunked_(skipDateCheck=true) | Forced summary email |
| Daily email trigger | time trigger | runDailyEmailSummary | sendEmailSummary | Date-gated summary email |
| Reply processor trigger | time trigger | processEmailReplies | parseReplyEmail_ -> storeHandledPlacements_/removeNetworks_ | Handled/monitoring state updates |
| Process Email Replies (Manual) | UI action/manual | processEmailReplies | same as trigger path | same as above |
| Process Network Removal Requests | UI action/manual | processNetworkRemovalRequests | Gmail search -> remove from Networks -> audit Removed Networks | Network suppression + confirmation email |
| Mid-flight alert run path | manual/orchestrated | sendMidFlightDropAlert | generateMidFlightDropHtml_ -> detectMidFlightDrop_ | Alert email (pre-15th) |
| Performance spike alert run path | manual/orchestrated | sendPerformanceSpikeAlertIfPre15 | cache compare + HTML build + send | Alert email (pre-15th) |
| Trigger provisioning | UI action/manual | createDailyEmailTrigger / createReplyProcessorTrigger | ScriptApp.newTrigger | Installable triggers |
| State management | UI action/manual | showSystemStatus / resetAllState | state reads and trigger cleanup | Operator diagnostics/reset |

4. Runtime Flow Diagram
Main monthly/daily QA pipeline:
- Time/UI Trigger
  -> runItAll or runItAllMorning
  -> Config Load (sheet names, ignore list, monitored networks, owner map)
  -> Data Ingestion (Gmail search + attachments)
  -> Parsing (CSV header detection + row normalization)
  -> Storage (Raw Data)
  -> Rule Engine (runQAOnly chunk)
  -> Violation Cache Update (sidecar + stale change fields)
  -> Output Generation (Violations sheet)
  -> Branch:
    - if rows remain: save state + schedule next QA chunk
    - else: clear QA state
  -> Branch (pre-15 alerts): sendPerformanceSpikeAlertIfPre15 / sendMidFlightDropAlert
  -> Email Summary Stage Machine (if >=15th unless forced)
    - Stage 1 network summary
    - Stage 2 grouped + stale sections
    - Stage 3 immediate attention by owner chunks
    - Stage 4 XLSX creation
    - Stage 5 send + cleanup

Reply/command pipeline:
- Daily Reply Trigger
  -> processEmailReplies
  -> Gmail search by subject/date
  -> parseReplyEmail_
  -> Branch:
    - HANDLE placement notes -> storeHandledPlacements_
    - REMOVE NETWORK id -> removeNetworks_
    - Parse error -> sendReplyErrorEmail_
  -> Updated Handled Placements / Monitored Networks

5. Dependency Graph
File-level:
- Code.js -> appsscript.json (runtime permissions/services)
- Code.js -> spreadsheet tabs (runtime schema/config)
- Code.js -> Gmail/Drive/Mail/UrlFetch/Script services
- campaign extractor.js -> GmailApp + SpreadsheetApp + Utilities
- README.md/TROUBLESHOOTING.md -> operational understanding of Code.js behaviors
- .clasp.json -> deployment linkage for Code.js and other script files

Module-level inside Code.js:
- Entry layer -> orchestration layer (runItAll*, runDailyEmailSummary)
- Orchestration -> ingestion module (importDCMReports)
- Orchestration -> QA module (runQAOnly)
- QA module -> config helpers (ignore, owner map, monitored IDs)
- QA module -> cache/state helpers (vChange sidecar, properties)
- QA outputs -> email module (sendEmailSummaryChunked_)
- Email module -> HTML section builders + XLSX export
- Reply module -> handled/removal sheets -> influences QA/email outputs

Configuration dependencies:
- Constants (hardcoded rates/thresholds/chunk sizes)
- Sheet cells (Networks H1/H3/I1)
- Properties keys (QA_STATE_KEY, EMAIL_STATE_KEY, trigger keys, cache ids)
- Manifest service toggles

Google service dependencies:
- SpreadsheetApp: all tab I/O and local data model
- GmailApp: ingestion and command intake
- MailApp: outbound comms
- DriveApp + UrlFetchApp: XLSX temp file lifecycle
- ScriptApp + LockService + PropertiesService: reliability and orchestration

UI component dependencies:
- onOpen menu items map directly to public functions; no HTML frontend artifacts in repo.

6. Data Object Map

| Object / Structure | Created In | Fields | Transformations | Used In |
|---|---|---|---|---|
| Raw Data row | processCSV/importDCMReports | Network ID, Advertiser, Placement/Campaign/date fields, Ad, Impressions, Clicks, Report Date | Header alignment, type coercion, report-date append | runQAOnly, summaries |
| Violation row (25 cols) | runQAOnly | Raw dimensions + metrics + issue strings + stale days + Owner (Ops) | Rule evaluation, metric computation, owner resolution, cache lookup | Violations sheet, email builders |
| QA state object | runQAOnly/getQAState_ | session, next, totalRows | JSON serialize/deserialize in DocumentProperties | chunk continuation |
| Email state object | sendEmailSummaryChunked_ | session, stage, cachedHtml, processedOwners, allOwners, xlsxFileId/name | stage transitions and resumable chunk writes | staged email pipeline |
| Owner map | loadOwnerMapFromNetworks_ | byKey composite map (raw + normalized) -> rep | header inference + normalization | runQAOnly, immediate attention builders |
| Ignore advertiser set | loadIgnoreAdvertisers | lowercase advertiser names | sheet read + normalization | runQAOnly filters |
| Monitored network list | getMonitoredNetworkIds_ | network IDs | tab read | runQAOnly scope gate |
| Violation change map | loadViolationChangeMap_/upsertViolationChange_ | key, pe, lastReport, lastImp, lastClk, lastImpChange, lastClkChange | cache migration, compaction, stale-day derivation | runQAOnly + stale outputs |
| Perf alert snapshot row | appendTodaySnapshots_ | date, key, impressions, clicks | daily append + compaction | pre-15 alerts/drop detection |
| Reply parse result | parseReplyEmail_ | type, note, placementIds, networkIds, error | stop-marker and regex parsing | processEmailReplies |
| Handled placement row | storeHandledPlacements_ | advertiser, campaign, placement id/name, metrics, issues, note, updated date, emails | dedupe/append note history | immediate attention rendering |
| Removed network record | processNetworkRemovalRequests | network id/name, requester, date, source email link | dedupe and audit append | import filtering + governance |

7. Rule and Decision Map

| Rule Name | Location | Inputs | Logic | Resulting Action | Configurable |
|---|---|---|---|---|---|
| Monitored scope gate | runQAOnly | networkId, monitored list | If list non-empty and id not in list -> skip row | reduces QA scope | Yes (Monitored Networks tab) |
| Ignore advertiser filter | runQAOnly/loadIgnoreAdvertisers | advertiser | lowercased advertiser in ignore set or includes bidmanager -> skip | suppresses monitored rows | Yes (Advertisers to ignore tab) |
| DART Search filter | runQAOnly | campaign/advertiser text | if contains DART Search -> skip | removes non-target traffic | No (hardcoded) |
| Grand total filter | runQAOnly | advertiser | if Grand Total marker -> skip | removes aggregate rows | No (hardcoded) |
| Zero metrics filter | runQAOnly | impressions, clicks | if both zero -> skip | avoids empty activity rows | No |
| Billing risk rules | runQAOnly | dates, clicks, impressions, cpc | expired/recent/active risk conditions | add 🟥 issue/detail | Partly (rates fixed) |
| Delivery post-flight rule | runQAOnly | placement end, report date, metrics | activity after flight window | add 🟦 issue/detail | No |
| Performance high CTR rule | runQAOnly | ctr, cpm | ctr >= 90 and cpm >= 10 | add 🟨 issue/detail | No |
| Cost high spend rules | runQAOnly | cpc, cpm, clicks/impressions | CPC-only, CPM-only, CPC+CPM overage pattern | add 🟩 issue/detail + overage logging | No (threshold hardcoded) |
| Low-priority classifier | scoreAndLabelLowPriority_ | placement name, metric gating | weighted regex scoring by category and confidence bands | add low-priority cost tag | Partly (pattern arrays in code) |
| Stale-change tracking | upsertViolationChange_/buildStaleHtml_ | key + current metrics + report date | update last-change dates and compute days since | stale counts in summary + columns | Yes (threshold via Networks H1) |
| Mid-flight drop alert | generateMidFlightDropHtml_ | history cache, current metrics, flight dates, threshold | compare today increments vs 3-day average drop | separate alert section/email | Yes (Networks I1 and H3) |
| Date gate for summary | sendEmailSummaryChunked_ | today date, force flag | if day<15 and not forced -> skip | prevents early monthly sends | Yes (force action path) |

8. Configuration Surface Map
Code constants (Code.js):
- CPC_RATE, CPM_RATE: cost calculations.
- QA_CHUNK_ROWS, QA_TIME_BUDGET_MS: QA runtime chunking.
- EMAIL_TIME_BUDGET_MS, MAX_OWNERS_PER_CHUNK: email stage chunking.
- ADMIN_EMAIL and property key constants: alerting/state plumbing.
- Low-priority and negative regex pattern arrays: classifier behavior.

Spreadsheet-driven config:
- Networks tab:
  - Column A/B network registry and names.
  - Mapping columns (including OPS owner field inference).
  - H1 stale-threshold input.
  - H3 mid-flight drop threshold input.
  - I1 mid-flight toggle (ON/TRUE/YES/ENABLED).
- Monitored Networks tab: active monitored network IDs.
- Advertisers to ignore tab: advertiser suppression set.
- EMAIL LIST tab: recipients.

Manifest/config files:
- appsscript.json: runtime version, timezone, enabled services.
- .clasp.json: deployment binding to script project.

Properties state/config:
- DocumentProperties:
  - qa_progress_v2
  - email_progress_v1
  - historical cache migration keys
- ScriptProperties:
  - qa_chunk_trigger_id
  - email_chunk_trigger_id
  - vChangeBookId

UI inputs/actions:
- Menu command selection changes execution paths.
- FORCE send and reset/status tools alter control-state behavior.

9. Failure Surface Map
High-risk surfaces:
- Gmail ingestion/search:
  - Symptom: no imported rows or stale data.
  - Impact: QA and email outputs empty/inaccurate.
- CSV/header parsing:
  - Symptom: rows dropped due to missing Advertiser header alignment.
  - Impact: under-detection.
- Sheet schema drift (renamed/deleted/misaligned columns):
  - Symptom: missing owner mapping, runtime errors, wrong metric indexing.
  - Impact: bad routing or failed runs.
- Trigger/state mismatch:
  - Symptom: stuck partial runs, duplicate processing, no resume.
  - Impact: delayed reporting.
- API calls (Drive export/UrlFetch/MailApp/GmailApp):
  - Symptom: missing attachments or send failures.
  - Impact: recipients not notified; incomplete audit.
- Permissions/OAuth scope issues:
  - Symptom: authorization errors, trigger failures.
  - Impact: end-to-end automation blocked.
- Reply parser strictness:
  - Symptom: valid human replies rejected.
  - Impact: handled/removal loop breaks.

10. Shared Library Candidates
- Chunked stateful runner:
  - lock-guard + property state + one-shot trigger scheduler.
- Gmail attachment ingestion package:
  - label query, attachment extraction, CSV/ZIP parsing helpers.
- Rule engine abstraction:
  - declarative conditions -> issue payload builder.
- Owner resolution module:
  - normalization, composite-key lookup, fallback strategies.
- Email stage assembler:
  - chunk-safe section generation + size-trim + attachment pipeline.
- Failure/audit toolkit:
  - standardized audit rows, context-rich failure emails.
- Reply command parser:
  - configurable command grammar + validation and sender policy hooks.

11. FAST AI HANDOFF
What matters most:
- Code.js controls everything; runQAOnly and sendEmailSummaryChunked_ are the structural center.
- Spreadsheet schema and properties keys are the runtime contract.

What is fragile:
- Monolithic code layout, duplicate helper declarations, hardcoded strings/cells, and parser assumptions.

What is reusable:
- Chunking/resume pattern, Gmail ingest, staged email sender, owner-mapping resolver, sidecar cache strategy.

What another AI should inspect first:
1. Entry-point-to-side-effect map (sheet writes, emails, properties, triggers).
2. Duplicate function definitions and override order.
3. Sheet schema assumptions and header dependencies.
4. Rule thresholds and configuration points for externalization.

Future automation ideas suggested:
- Declarative rule packs with validation.
- Trigger/state health dashboard.
- Regression simulator for CSV snapshots.
- Auto-ticketing integration for high-severity violation groups.
