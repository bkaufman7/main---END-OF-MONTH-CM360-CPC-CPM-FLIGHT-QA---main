TITLE:
PROJECT INTELLIGENCE DOSSIER

1. Executive Overview
This system is a Google Apps Script based operational QA automation for CM360 placement reporting, implemented primarily in a single script file with supporting docs and helper scripts. Its primary purpose is to ingest daily CM360 CSV report attachments from Gmail, normalize and load them into Google Sheets, evaluate billing/delivery/performance/cost anomalies, and distribute actionable owner-scoped reports by email.

Operational problem solved:
- Manual QA of high-volume CM360 placement exports is error-prone and slow.
- Teams need repeatable detection of billing risk, post-flight delivery, abnormal performance/cost signatures, and stale activity.
- Ops teams also need a closed-loop workflow to acknowledge handled issues and remove networks from monitoring without directly editing script code.

Automated workflows:
- Gmail intake of labeled report emails and attachment extraction (CSV and ZIP-contained CSV).
- Raw data ingestion into Raw Data sheet.
- Auto-discovery and onboarding of new network IDs.
- Rule-driven violation generation into Violations sheet.
- Chunked execution and resumable trigger orchestration to stay under Apps Script runtime ceilings.
- Staged email assembly and distribution with XLSX attachment export.
- Reply parsing for handled placement notes and network removal commands.
- Optional pre-15th alerts: performance spike and mid-flight drop alerts.
- Monthly overage rollup for specific CPC+CPM overbilling conditions.

Likely users/operators:
- Ad operations analysts.
- Billing/reconciliation specialists.
- Platform automation owner/maintainer.
- Campaign/network owners receiving owner-specific immediate attention tables.

Input types processed:
- Gmail thread/message metadata and body text.
- CSV payloads and ZIP-embedded CSV files.
- Spreadsheet-driven configuration (Networks, EMAIL LIST, Advertisers to ignore, Monitored Networks).
- Email replies with structured commands (placement note blocks, REMOVE NETWORK lines).

Outputs generated:
- Populated/updated operational sheets (Raw Data, Violations, Handled Placements, Removed Networks, Monthly Overages, hidden cache sheets).
- HTML summary emails and auxiliary alert emails.
- XLSX export attachment from Violations.
- Execution log records and administrative failure notifications.

Larger workflow fit:
- Acts as downstream quality control and exception routing for a CM360 reporting pipeline.
- Integrates into monthly close and pre-close operational readiness cadence.
- Serves as a hub between reporting ingestion, QA policy enforcement, owner assignment, and human remediation acknowledgment.

2. Repository File Inventory

| File | Type | Purpose | Key Functions or Components | Importance Level |
|---|---|---|---|---|
| .clasp.json | JSON config | CLASP project binding and extension behavior | scriptId, rootDir, extension mappings | High |
| .gitignore | Config | Excludes local/IDE/log artifacts from Git | clasp/OS/IDE/log ignore patterns | Medium |
| appsscript.json | Apps Script manifest | Runtime and advanced service declarations | V8 runtime, Stackdriver logging, Drive v3 and Gmail v1 advanced services | High |
| campaign extractor.js | Apps Script module | Campaign ID mapping ingestion from Gmail CSV/ZIP | importCampaignIdMapping, parseCampaignCsv, extractNetworkId usage, start | Medium |
| Code.js | Apps Script monolith | Core system logic: UI menu, ingestion, QA rules, emailing, triggers, reply processing, monitoring, cache/state | ~100+ functions across end-to-end pipeline | Critical |
| filter.js | JS constants/helper rules | Regex tag taxonomy for click-tracker and low-priority signal classification experiments | CT_RULES array | Low-Medium |
| README.md | Documentation | System architecture, data flow, runbook, operational benchmark narrative | Detailed process docs and setup instructions | High |
| TROUBLESHOOTING.md | Documentation | Failure scenarios, diagnostics, remediation playbooks | Error references and operational workflows | High |

File interaction model:
- Code.js is the executable nucleus. It reads configuration and recipient data from spreadsheet tabs and properties stores, processes Gmail attachments/replies, and writes all operational outputs.
- appsscript.json governs execution environment and enabled APIs for Code.js behavior.
- .clasp.json defines local-to-Apps-Script sync identity used by CLASP deployment.
- campaign extractor.js appears to support an adjacent/specialized import flow (Campaign ID mapping) and reuses parsing/network extraction patterns parallel to Code.js.
- filter.js contains pattern rules likely related to classifier development; in current state, default low-priority patterns are hardcoded inside Code.js, so filter.js is not directly wired into the main runtime path.
- README.md and TROUBLESHOOTING.md mirror and explain implementation intent, known failure modes, and operator procedures.

3. Development and Deployment Workflow
Local development workflow:
- Developer edits JS files locally (primarily Code.js).
- Uses CLASP project binding from .clasp.json to sync script code.
- Reads/writes behavior contracts via README/TROUBLESHOOTING for operational continuity.

Version control usage:
- Git-managed repository with standard ignore rules.
- Monolithic file strategy means high merge-conflict surface in Code.js.
- No explicit branch strategy encoded in repo, but docs imply iterative bugfix workflow with dated production incidents.

CLASP usage:
- Presence of .clasp.json with scriptId confirms CLASP-backed Apps Script project sync.
- rootDir is project root, script and JSON extensions configured.
- No explicit .claspignore present in repository root despite mention in .gitignore.

Deployment process (likely):
- Local code changes validated via manual runs in Apps Script.
- clasp push to deploy updated script code to bound Apps Script project.
- Trigger-based production operation through installable time triggers.

Testing workflow (observed):
- No automated test harness/unit tests in repo.
- Testing relies on debug functions and manual command runs:
  - debugQAFiltering
  - debugQALogic
  - countNonZeroRows
  - showSystemStatus
- Operational verification via logs, sheet outputs, and delivered email artifacts.

Potential workflow risks:
- Manual deployment and trigger management can drift from repository state.
- No CI validation of syntax, lint, or behavior regressions.
- High-coupling monolith increases regression probability for small edits.
- Duplicate function definitions in Code.js raise override/order ambiguity risk.

4. System Architecture
Overall architecture is functionally layered but physically monolithic.

Core modules/subsystems in Code.js:
- UI and trigger provisioning: onOpen, trigger creators, status/reset utilities.
- Ingestion: Gmail search, attachment parse, Raw Data population, network auto-add.
- Rule engine: runQAOnly with category logic, scoring, stale/change tracking.
- Notification: staged email summary generation, per-owner prioritization, XLSX export.
- Alert sideflows: pre-15th performance spike and mid-flight drop alerts.
- Reply loop: parse replies, persist handled notes, remove monitored networks.
- Persistence/state: DocumentProperties and ScriptProperties orchestration; hidden sheet caches; sidecar spreadsheet for change map.
- Observability: execution audit sheet and failure email notifications.

Architecture style:
- Runtime behavior is modular by function families but code packaging is monolithic in one file.
- Significant shared global constants and implicit contracts via sheet names/column headers.

Runtime flow (primary):
- Trigger or menu action -> runItAll/runItAllMorning -> trim + importDCMReports -> runQAOnly (single chunk or resumed chunks) -> optional alerts -> sendEmailSummary (staged chunks) -> recipient distribution + cleanup.

Alternate flow (reply loop):
- Daily trigger -> processEmailReplies -> parse commands -> store handled placement notes and/or remove monitored networks -> affects future immediate-attention output and import filtering.

5. Entry Points and Triggers
Menu/UI entry points (onOpen):
- Run It All (Immediate): full sequence manual execution.
- Run It All (Auto-Resume): chunk-friendly orchestration path.
- Pull Data (Immediate/Auto-Resume): ingestion functions.
- Run QA Only (Immediate/Auto-Resume): validation engine.
- Send Email Only (Immediate/Auto-Resume): notification pipeline.
- FORCE Send Email Now: bypasses date gating.
- Debug tools and management controls.

Time-driven triggers:
- createDailyEmailTrigger: schedules runDailyEmailSummary at 9am.
- createReplyProcessorTrigger: schedules processEmailReplies at 7am.
- scheduleNextQAChunk_: one-shot continuation of runQAOnly.
- scheduleNextEmailChunk_: one-shot continuation of sendEmailSummary.

Manual script runs:
- Any public function may be executed from Script Editor.
- Debug and reset functions are designed for direct/manual operation.

Email/API-driven indirect starts:
- Gmail replies create command payloads consumed by processEmailReplies and processNetworkRemovalRequests during polling runs.

What each initiates:
- Ingestion functions initialize data substrate.
- QA functions generate structured violations.
- Email functions build/ship stakeholder-facing artifacts.
- Reply processors mutate monitoring/handled state for subsequent runs.

6. Data Flow
Input sources:
- Gmail label-based search for report delivery emails.
- Attachment payloads (CSV and ZIP->CSV).
- Spreadsheet tabs for config and references.
- Document/Script properties for resumable state.
- Reply email plaintext bodies for command parsing.

Primary file/data formats:
- CSV from CM360-like exports.
- JSON-like serialized state in PropertiesService.
- HTML bodies for outbound email.
- XLSX export blob generated via Sheets export endpoint.

Ingestion and parsing steps:
- Locate header row beginning at Advertiser inside CSV text.
- Parse rows with Utilities.parseCsv.
- Enrich each row with Network ID and Report Date.
- Batch write into Raw Data.

Transformations:
- Header-map creation for dynamic column indexing.
- Numeric conversions for impressions/clicks/CTR/CPC/CPM.
- Date coercion for report/flight windows and month boundaries.
- Owner enrichment via network+advertiser matching.
- Low-priority classification descriptor generation for single-metric rows.
- Violation change map update for stale-day computation.

Validation/filtering:
- Monitored network gating if list populated.
- Ignore advertiser list exclusion.
- DART Search and Grand Total exclusion.
- Zero-metric exclusion.
- Required sheet checks at function entry.

Outputs:
- Violations rows (25 columns including owner and stale-day fields).
- HTML sections (network summary, grouped summary, immediate attention, stale summary).
- XLSX attachment and final report email.
- Optional alert emails and administrative failure notifications.

Alternate flows:
- If QA still active, email stage defers itself via reschedule.
- If before day 15, monthly summary suppresses unless force-send path used.
- If no violations, summary send exits early.
- If no recipients, send stage exits safely.
- If parse errors in replies, sender gets format guidance email.

7. Business Logic and Rules
Major rule groups in runQAOnly:
- Billing rules:
  - Expired CPC risk: placement ended before month and clicks > impressions.
  - Recently expired CPC risk: ended before report date and clicks > impressions.
  - Active CPC billing risk: active placement with clicks > impressions and cpc > $10.
- Delivery rules:
  - Post-flight activity: activity after placement ended (month-window constrained).
- Performance rules:
  - CTR >= 90% and CPM >= $10.
- Cost rules:
  - CPC-only > $10.
  - CPM-only > $10.
  - CPC+CPM with clicks > impressions and cpc > $10.
- Low-priority classifier:
  - Only for single-metric rows (CPM-only or CPC-only), never mixed.
  - Uses weighted regex signals and confidence banding to annotate likely tracker/pixel patterns.

Rule significance and implementation characteristics:
- Rules are implemented as explicit conditional blocks, not a declarative rule table.
- Thresholds are partly hardcoded (e.g., 90%, $10) and partly sheet-configured (stale days, drop threshold, feature toggle).
- Rule effects include issue type string accumulation and detail message concatenation per row.

Configurability:
- Stale threshold from Networks H1.
- Mid-flight drop enable from Networks I1 and threshold from Networks H3.
- Monitoring scope from Monitored Networks sheet.
- Ignore list from Advertisers to ignore sheet.
- Owner mapping from Networks header-detected columns.

Edge cases:
- Date parse failures can propagate subtle misclassification when invalid dates produce NaN comparisons.
- Header mismatches in source CSV can silently suppress parse output.
- If owner mapping headers are altered unexpectedly, owners default to Unassigned.
- Duplicate function definitions may alter intended helper behavior depending on declaration order.

8. Configuration Model
Configuration channels:
- Constants in code: CPC_RATE, CPM_RATE, chunk sizes, time budgets, admin email, max owners per chunk.
- Spreadsheet tabs as dynamic config:
  - Networks (network labels, owner mapping, stale/drop controls).
  - Monitored Networks (inclusion scope).
  - Advertisers to ignore.
  - EMAIL LIST.
- PropertiesService:
  - DocumentProperties for state snapshots and progress.
  - ScriptProperties for trigger IDs and sidecar workbook id.
- Manifest-level configuration via appsscript.json.

Flexibility:
- High operational flexibility without code edits for recipients, ignore list, owner mapping, and monitoring scope.
- Moderate flexibility for thresholds through specific sheet cells.

Risks:
- Strong dependence on exact sheet names and expected columns.
- Hidden configuration coupling (cell H1/H3/I1 semantics not discoverable unless documented).
- Global constants remain hardcoded for key financial thresholds and alert criteria.

9. External Integrations
Google services:
- GmailApp: search labeled emails, parse replies, read messages/attachments.
- MailApp: send summary, alert, and failure emails.
- SpreadsheetApp: core data store, views, and cache sheets.
- DriveApp: temporary XLSX file storage and deletion.
- UrlFetchApp: download XLSX via Sheets export endpoint.
- ScriptApp: trigger scheduling/cancellation, script metadata.
- LockService: concurrency control.
- PropertiesService: persistent run-state storage.

Advanced services declared in manifest:
- Drive v3 and Gmail v1 enabled; main code mostly uses native Apps Script services.

Request flow examples:
- XLSX generation: create temp spreadsheet -> copy Violations sheet -> fetch /export?format=xlsx using OAuth bearer -> trash temp file.
- Gmail ingest: query by label/date -> iterate threads/messages -> attachment parse.
- Reply command processing: query by subject/date -> parse body -> mutate monitored/handled sheets.

10. Spreadsheet and Data Storage Usage
Primary operational sheets:
- Raw Data: imported report records.
- Violations: rule outputs and enriched attribution fields.
- Networks: network metadata and owner mapping.
- EMAIL LIST: recipients.
- Advertisers to ignore: exclusion controls.
- Monitored Networks: runtime scope control.
- Handled Placements: user-acknowledged remediation notes.
- Removed Networks: audit and suppression list.
- Monthly Overages: month-level accumulated overage by network/placement.
- Hidden sheets: _Execution Log, _Perf Alert Cache.

External/sidecar storage:
- Separate spreadsheet for _Violation Change Cache (scale/stability strategy for large map persistence).

Read/write patterns:
- Uses getDataRange/getValues heavily for full-sheet scans.
- Performs batch writes for large row blocks (good) but still has some appendRow/deleteRow loops (costly at scale).
- Uses per-row filtering and classification in JS memory.

Lookups:
- Header map indexing by column name for resilient column position handling.
- Owner resolution by composite keys network|||advertiser and normalized fallback.
- Change map keyed by pid:... or synthetic key based on network/campaign/placement.

Performance implications:
- Full-sheet reads are O(n) per phase and may degrade with long retention.
- appendRow in loops for some workflows increases API call count.
- Frequent trigger rescheduling and property serialization can add overhead but improves timeout resilience.

11. UI and Operator Experience
UI surface:
- Custom spreadsheet menu with operational commands, debug, status, and reset controls.
- Alert dialogs for status and confirmations (e.g., reset all state, monthly overage display).

Operator actions supported:
- Manual run modes (immediate vs auto-resume).
- Trigger installation for daily automation.
- Forcing date-gated email sends.
- Manual processing of email replies.
- State reset and system status checks.

Backend interaction pattern:
- UI actions call Apps Script entry functions directly.
- Most user-facing outputs are sheet artifacts and outbound email.
- No HTML sidebar/dialog frontend present in repository; operator UX is menu + sheets + email-driven commands.

12. Utilities and Shared Logic
Reusable utilities in Code.js:
- Header mapping and value parsers: getHeaderMap, _parseMoney_, _parsePct_.
- State and trigger management wrappers.
- Retry/backoff for sidecar cache operations.
- Date/time formatting and duration formatting.
- Owner normalization and resolution helpers.
- CSV conversion helper.

Duplication hotspots:
- Perf alert cache helpers are defined twice (same function names repeated later).
- Some similar ingestion/parse logic exists in campaign extractor.js and Code.js independently.
- Multiple pathways for network removal command handling with overlapping intent.

Potential shared library candidates:
- Gmail attachment ingestion/parsing framework.
- Rule evaluation engine with declarative rule config.
- Trigger-safe chunk runner abstraction.
- Owner mapping/normalization resolver.
- Email section builder library with size-aware assembly.

13. Error Handling and Resilience
Strengths:
- Extensive try/catch in high-risk paths (emailing, backfill, network-removal processing, overage logging).
- Concurrency protection via document/script locks.
- Resumable chunking with explicit state for QA and email stages.
- Failure notification emails to admin with context.
- Audit logging to hidden sheet with retention trimming.

Validation/fallback behavior:
- Required-sheet guards.
- Early exits on no data/no recipients.
- Trigger duplication checks before scheduling.
- Safe cleanup of temp XLSX and stale trigger IDs.

Weaknesses:
- Some catch blocks only log and continue, potentially masking persistent data quality errors.
- Date parsing and header assumptions may fail silently in parts of the pipeline.
- Duplicate function definitions can hide bugs because later declarations override earlier versions.

14. Performance and Scalability
Apps Script constraints addressed:
- QA chunk size and time budget to stay under execution limits.
- Email staged processing with chunked owner rendering.
- Trigger handoff when close to quota limit.

Efficient patterns used:
- Batch setValues for large inserts.
- Cached state/progress to avoid restarting long runs.
- Hidden cache compaction for performance alert snapshots.

Bottlenecks observed:
- Monolithic runQAOnly still scans full Raw Data per chunk invocation start.
- getDataRange on large sheets repeatedly across phases.
- appendRow and deleteRow in loops for certain management functions.
- Sidecar cache save operation can be heavy for very large key maps despite batching.

Quota/API considerations:
- Gmail search and message iteration can become expensive with broad date windows.
- Frequent MailApp sends with many recipients and attachments can approach limits.
- UrlFetch export and Drive file churn add external call overhead.

15. Security and Access Considerations
Potential risks:
- Hardcoded admin/ops email addresses in code and docs.
- Broad Gmail search queries may process unintended replies if subject matches are noisy.
- Command parsing from email body relies on sender and format but not robust auth/allowlist enforcement.
- Spreadsheet as control plane means editors can alter operational thresholds/recipient lists.

Permissions model implications:
- Script likely runs with broad access to Gmail, Drive, and Spreadsheet scopes.
- Trigger executions may run under installer account context, centralizing power and risk.

Governance recommendations:
- Add sender allowlist for command execution paths.
- Externalize sensitive contacts and constants into protected properties.
- Add change audit for configuration tabs.

16. Code Quality and Maintainability
Positive attributes:
- Clear function naming and broad inline comments.
- Operationally rich logging for debugging and support.
- Comprehensive docs with troubleshooting coverage.

Maintainability constraints:
- Very large monolithic script with tightly coupled domains.
- Duplicate function blocks and drift between docs and implementation details.
- Limited automated testing and no lint/type gates.
- Implicit contracts via sheet names/column labels and magic cells.

Ease of maintenance:
- Good for single-owner fast iteration.
- Hard for team-scale collaboration and safe refactoring without regression harness.

17. Technical Debt and Fragility
Fragile areas:
- Large Code.js file size and mixed concerns (UI, ingestion, rules, email, state, alerts, reply processing).
- Duplicate helper declarations (perf cache functions) can create non-obvious behavior.
- Hardcoded strings for sheet names, issue labels, and parser expectations.
- Business logic deeply embedded in imperative if-chains instead of data-driven rule configuration.
- Reply parser assumptions can break with email client variations and quoted text structures.
- Filename-based network id extraction can fail for nonconforming report naming.

Why it matters:
- Increases change blast radius.
- Raises onboarding complexity for new maintainers/agents.
- Makes behavior drift likely across parallel operational enhancements.

18. Missing Capabilities
Notable missing or partial capabilities:
- Automated test suite for parser/rule regression.
- Centralized configuration schema validation and startup health checks.
- Structured logging framework with correlation/session IDs beyond ad hoc messages.
- Alert deduplication/rate control for repeated anomalies.
- Sender authorization and stronger command integrity for reply processing.
- Dead-letter/error queue for failed parse/process inputs.
- Formal run history dashboard over audit metrics.

19. Expansion Opportunities
High-value evolution paths:
- Convert rule engine to declarative JSON/tab-driven policy definitions with per-rule enablement.
- Build owner/team SLA dashboards from Violations + Handled Placements + Execution Log.
- Add adaptive thresholds by network/advertiser based on historical baselines.
- Extend ingestion to additional ad platform schemas with a common normalization layer.
- Add webhook/Chat integrations for critical alerts (billing risk, mid-flight drops).
- Implement configurable suppression windows and duplicate-notification controls.

20. Reusable Components
Strong reusable components across automation projects:
- Chunked resumable executor pattern (state + trigger continuation + lock guard).
- Gmail attachment ingestion and CSV normalization framework.
- Staged email composition with HTML size-safety and attachment creation.
- Owner resolution utility using exact+normalized key strategies.
- Audit/failure notification scaffolding.
- Sidecar-cache pattern for large mutable maps exceeding property storage practicality.

Why reusable:
- These patterns address generic Apps Script constraints (runtime ceilings, quota sensitivity, operational observability).
- They abstract platform-specific reliability concerns common to sheet-centric automations.

21. Cross-Project Ecosystem Potential
Ecosystem integration potential:
- Serve as central QA service feeding downstream reporting pipelines via Sheets exports/API pulls.
- Integrate with broader marketing ops orchestration for ticket creation (e.g., Jira/ServiceNow) based on high-priority violations.
- Share standardized config tabs and rule packs across related account QA workbooks.
- Emit normalized event rows to a warehouse (BigQuery) for longitudinal anomaly analytics.
- Reuse reply-command framework for multi-project operational command center via email.

22. Strategic Summary
System strengths:
- Mature end-to-end automation from ingestion to owner-targeted action delivery.
- Robust runtime resilience via chunking, state persistence, and trigger orchestration.
- Strong operational ergonomics through menu tooling and rich troubleshooting docs.
- Practical exception workflows (handled notes, network removal, overage rollups).

System weaknesses:
- Monolithic structure with duplicated functions and implicit configuration contracts.
- Limited formal testing and schema validation.
- Security/governance gaps in command authorization and hardcoded operational identities.
- Heavy dependence on full-sheet scans and manual data hygiene for scale.

Highest-impact refactoring opportunities:
- Split Code.js into domain modules (ingestion, rules, notifications, state, ops UI).
- Eliminate duplicate function definitions and centralize helper ownership.
- Introduce declarative rule configuration and validation layer.
- Implement test harness for CSV parser, rule outcomes, and reply parser.
- Add configuration bootstrap checks with explicit operator warnings.

Future development potential:
- Strong base for a reusable Apps Script QA framework.
- Can evolve into multi-platform anomaly and remediation orchestration service.
- Ready for observability and governance upgrades that materially reduce operational risk.

23. Appendix: Function and Component Index
Ingestion and import:
- importDCMReports
- importDCMReportsChunked
- processCSV
- extractNetworkId
- autoAddNewNetworks_
- syncMonitoredNetworks_
- getRemovedNetworks
- ensureRemovedNetworksSheet
- processNetworkRemovalRequests
- backfillSourceEmailLinks

QA processing and rules:
- runQAOnly
- runQAOnlyImmediate
- scoreAndLabelLowPriority_
- compileLPPatternsIfNeeded_
- normalizeName_
- loadIgnoreAdvertisers
- getStaleThresholdDays_
- isMidFlightDropEnabled_
- logMonthlyOverage_
- getMonthlyOverageTotal
- getCurrentMonthOverage

Email summary and rendering:
- sendEmailSummary
- sendEmailSummaryChunked_
- sendEmailSummaryImmediate
- sendEmailNow
- buildNetworkSummaryHtml_
- buildGroupedSummaryHtml_
- buildStaleHtml_
- buildImmediateAttentionData_
- buildImmediateAttentionHtmlForOwners_
- createXLSXFromSheet

Reply/command handling:
- processEmailReplies
- parseReplyEmail_
- storeHandledPlacements_
- sendReplyErrorEmail_
- removeNetworks_
- extractEmail_
- clearHandledPlacements

Triggers, orchestration, and status:
- onOpen
- runItAll
- runItAllChunked
- runItAllMorning
- runDailyEmailSummary
- createDailyEmailTrigger
- createReplyProcessorTrigger
- scheduleNextQAChunk_
- cancelQAChunkTrigger_
- scheduleNextEmailChunk_
- cancelEmailChunkTrigger_
- showSystemStatus
- resetAllState

State/cache/persistence:
- getQAState_, saveQAState_, clearQAState_
- getEmailState_, saveEmailState_, clearEmailState_
- getScriptProps_
- getVChangeBook_, getVChangeSheet_
- loadViolationChangeMap_, saveViolationChangeMap_
- cleanupViolationCache_, upsertViolationChange_
- getPerfAlertCacheSheet_, loadLatestCacheMap_, appendTodaySnapshots_, compactPerfAlertCache_

Alerts and anomaly sideflows:
- sendPerformanceSpikeAlertIfPre15
- getDropThreshold_
- detectMidFlightDrop_
- generateMidFlightDropHtml_
- sendMidFlightDropAlert
- getHistoricalData_

Observability and diagnostics:
- sendFailureEmail_
- getAuditSheet_
- logAuditEntry_
- logStep_
- fmtMs_
- debugQAFiltering
- countNonZeroRows
- debugQALogic

Utilities:
- getHeaderMap
- normalizeAdv_
- resolveRep_
- loadOwnerMapFromNetworks_
- withBackoff_
- arrayToCsv
- trimAllSheetsToData_
- clearViolations
- showCurrentMonthOverage

AI HANDOFF NOTES
Most important repository areas:
- Code.js is the authoritative runtime. Prioritize runQAOnly and sendEmailSummaryChunked_ first.
- appsscript.json and .clasp.json define deploy/runtime constraints.
- README.md and TROUBLESHOOTING.md provide intent, incident context, and operational assumptions to cross-check with code.

Hidden complexity hotspots:
- Duplicate helper function definitions (especially perf alert cache helpers) create override-order ambiguity.
- State machine interactions across QA and email chunking can produce stuck/partial behavior if triggers/properties drift.
- Owner mapping depends on dynamic header inference and special column zones in Networks.
- Reply parsing is sensitive to body formatting and quoted text variants.

What another AI should analyze first:
1. Build a control-flow map from entry points to terminal side effects (sheet writes, emails, properties changes).
2. Diff docs versus implementation for threshold cells, rule semantics, and stage counts.
3. Locate and resolve duplicate function declarations and dead code paths.
4. Model sheet schemas and required columns as a machine-checkable contract.

Future tooling opportunities:
- Static analyzer to detect sheet-column contract drift and duplicate declarations.
- Replay simulator that runs rule logic on archived CSV snapshots for regression testing.
- Trigger/state inspector dashboard for stuck-session diagnosis.
- Config linting and policy pack management for rule thresholds and recipient governance.
