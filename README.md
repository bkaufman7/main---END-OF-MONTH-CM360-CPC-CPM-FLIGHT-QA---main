# CM360 End-of-Month QA Automation System

## 📋 TLDR

**What it does:** Automatically imports CM360 placement reports from Gmail, analyzes 29,000+ rows for billing/delivery/performance/cost issues, generates violations with 25+ detection rules, and emails detailed QA reports with XLSX attachments to operations teams.

**Key Stats:**
- Processes ~29,564 raw placement rows
- Detects ~1,205 violations (4.1% rate)
- Monitors 25 networks
- Execution time: ~3 minutes (QA: 40s, Email: 2m 33s)
- 4 violation categories: BILLING, DELIVERY, PERFORMANCE, COST
- Emails sent to 11+ recipients with network ownership mapping

**Main Features:**
- ✅ Auto-imports CSV reports from Gmail with `CM360 QA` label
- ✅ Chunked execution (prevents 6-minute timeout)
- ✅ Smart violation detection (CTR >90%, CPC/CPM >$10, stale metrics, flight issues)
- ✅ Network auto-onboarding (new networks added with "TO BE ADDED" status)
- ✅ Owner mapping (violations assigned to ops team members by advertiser)
- ✅ Email with 4 sections: Network Summary, Grouped Summary, Stale Metrics, Immediate Attention
- ✅ Handled placements tracking (violations resolved show as green rows)
- ✅ FORCE send button (testing bypass for date restrictions)

**Tech Stack:** Google Apps Script (V8), Google Sheets API, Gmail API, Drive API

---

## 📚 Table of Contents

1. [System Architecture](#system-architecture)
2. [Data Flow Overview](#data-flow-overview)
3. [Step-by-Step Process](#step-by-step-process)
   - [Phase 1: Report Import](#phase-1-report-import)
   - [Phase 2: Network Auto-Add](#phase-2-network-auto-add)
   - [Phase 3: QA Detection (Chunked)](#phase-3-qa-detection-chunked)
   - [Phase 4: Email Generation (Staged)](#phase-4-email-generation-staged)
4. [Decision Logic](#decision-logic)
   - [Filtering Rules](#filtering-rules)
   - [Violation Detection Rules](#violation-detection-rules)
   - [Owner Assignment Logic](#owner-assignment-logic)
   - [Chunking Decisions](#chunking-decisions)
5. [Configuration & Setup](#configuration--setup)
6. [Sheets Reference](#sheets-reference)
7. [Trigger Management](#trigger-management)
8. [Error Handling](#error-handling)
9. [Debug Tools](#debug-tools)
10. [Maintenance & Updates](#maintenance--updates)

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         GMAIL INBOX                                  │
│  (Receives CM360 reports from networks with label "CM360 QA")      │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│              IMPORT DCM REPORTS (importDCMReports)                   │
│  • Searches Gmail for today's messages with "CM360 QA" label        │
│  • Extracts Network ID from filename (e.g., "12345_report.csv")    │
│  • Parses CSV attachments (skips header lines)                      │
│  • Writes to "Raw Data" sheet                                       │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│           AUTO-ADD NEW NETWORKS (autoAddNewNetworks_)                │
│  • Scans Raw Data for Network IDs                                   │
│  • Compares against Networks sheet (Column A)                       │
│  • Appends new networks with "TO BE ADDED" friendly name            │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│              RUN QA (runQAOnly - CHUNKED)                            │
│  Chunk Size: 3,500 rows per execution                               │
│  Time Budget: 4.2 minutes per chunk                                 │
│  • Loads configuration (ignored advertisers, owner map, thresholds) │
│  • Processes rows in chunks (prevents timeout)                      │
│  • Applies 25+ violation detection rules                            │
│  • Writes violations to "Violations" sheet                          │
│  • Creates triggers for next chunk if needed                        │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│         SEND EMAIL (sendEmailReport - STAGED)                        │
│  Stage 1: Network Summary (by network, violation counts)            │
│  Stage 2: Grouped Summary + Stale Metrics                           │
│  Stage 3: Immediate Attention (by owner, chunked by 5 owners)       │
│  Stage 4: XLSX Generation + Email Send                              │
│  Time Budget: 4.5 minutes per stage                                 │
│  • Builds HTML sections incrementally                                │
│  • Creates XLSX attachment from Violations sheet                    │
│  • Sends to EMAIL LIST recipients                                   │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    EMAIL RECIPIENTS                                  │
│  • Operations team members (mapped by advertiser)                   │
│  • Shows network summaries, violation details, handled placements   │
│  • Includes XLSX attachment for detailed analysis                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow Overview

### Input Sources
1. **Gmail**: CM360 reports arrive as CSV attachments with label `CM360 QA`
2. **Networks Sheet**: Network ID → Friendly Name mapping (Column A/B), Advertiser → Owner mapping (Columns P/Q/R/S)
3. **Advertisers to ignore Sheet**: List of 79 advertisers to exclude from QA (e.g., test accounts, internal)
4. **EMAIL LIST Sheet**: Recipients for QA email notifications

### Processing Layers
1. **Import Layer**: CSV → Raw Data sheet (29,564 rows typical)
2. **Filter Layer**: Removes ignored advertisers, DART Search, Grand Total rows
3. **Detection Layer**: Applies 25+ violation rules across 4 categories
4. **Enrichment Layer**: Adds owner assignment, handled placement status
5. **Presentation Layer**: Generates HTML email + XLSX attachment

### Output Artifacts
1. **Raw Data Sheet**: All imported placement data with Report Date
2. **Violations Sheet**: Filtered violations with Issue Type, Details, Owner
3. **XLSX Attachment**: Full violations export for detailed analysis
4. **HTML Email**: 4-section summary with network/owner breakdowns

---

## 📖 Step-by-Step Process

### Phase 1: Report Import

**Function:** `importDCMReports()`

**Trigger:** Manual menu click ("Import DCM Reports") or scheduled trigger

**Step 1: Search Gmail**
- Searches for emails with label `CM360 QA` received today
- Format: `label:CM360 QA after:yyyy/MM/dd`

**Step 2: Extract Network ID from Filename**
- Pattern: `{networkId}_*.csv`
- Example: `12345_CM360_Report.csv` → Network ID = `12345`
- If no match: assigns `"Unknown"`

**Step 3: Parse CSV Files**
- Searches for "Advertiser" header row
- Skips CM360 metadata lines above
- Parses CSV data starting from header
- Appends Network ID and Report Date to each row

**Step 4: Write to Raw Data Sheet**
- Clears existing data
- Writes headers: Network ID, Advertiser, Placement ID, Placement, Campaign, Start/End Dates, Ad, Impressions, Clicks, Report Date
- Batch writes all extracted data (~29,564 rows)

---

### Phase 2: Network Auto-Add

**Function:** `autoAddNewNetworks_()`

**Trigger:** Automatically called after import completes

**Process:**
1. Gets all unique Network IDs from Raw Data (Column A)
2. Loads existing Network IDs from Networks sheet (Column A)
3. Finds Network IDs in Raw Data but not in Networks sheet
4. Appends new rows: `[Network ID, "TO BE ADDED"]`
5. Logs additions: `"Added 2 new network(s): 12345, 67890"`

**Result:** New networks appear in Networks tab with placeholder name, ready for manual update

---

### Phase 3: QA Detection (Chunked)

**Function:** `runQAOnly()`

**Chunking Settings:**
- **Chunk Size:** 3,500 rows per execution
- **Time Budget:** 4.2 minutes per chunk
- **State Storage:** DocumentProperties (`qa_progress_v2`)

**Process Flow:**

**Step 1: Load or Resume State**
- Fresh run: Initialize state, clear Violations sheet
- Continuation: Load saved state, resume from `startRow`

**Step 2: Load Configuration**
- **Ignored Advertisers:** 79 test accounts to exclude
- **Monitored Networks:** Optional filter (if empty, process all)
- **Owner Map:** Network + Advertiser → Owner email mapping
- **Stale Threshold:** Days before metrics considered stale (default: 5)

**Step 3: Process Chunk (3,500 rows)**

For each row, apply filters:

**Filter 1: DART Search**
```javascript
if (placement.includes("dart search")) → SKIP
```

**Filter 2: Grand Total**
```javascript
if (advertiser === "Grand Total") → SKIP
```

**Filter 3: Ignored Advertisers**
```javascript
if (ignoreSet.has(advertiser.toLowerCase())) → SKIP
```

**Filter 4: Monitored Networks (Optional)**
```javascript
if (monitoredNetworks.length > 0 && !monitoredNetworks.includes(networkId)) → SKIP
```

**Step 4: Violation Detection (25+ Rules)**

**Category 1: 🟥 BILLING (Critical)**
- **Outside Placement Flight:** Report date < placement start OR > placement end
- **Outside Campaign Flight:** Report date < campaign start OR > campaign end  
- **Invalid Dates:** Placement/campaign dates unparseable

**Category 2: 🟦 DELIVERY (High Priority)**
- **Ending Soon (Low Priority):** 0-3 days until placement ends
- **Placement Ended:** Placement end date in past
- **Not Started Yet:** Placement start date in future

**Category 3: 🟨 PERFORMANCE (Medium Priority)**
- **High CTR:** CTR > 90% (with min 10 impressions)
- **Stale Impressions:** No change for ≥ threshold days
- **Stale Clicks:** No change for ≥ threshold days

**Category 4: 🟩 COST (Medium Priority)**
- **High CPC:** Cost per click > $10
- **High CPM:** Cost per mille > $10

**Step 5: Owner Assignment**
- Try exact match: `NetworkID|||advertiser_lowercase`
- Try normalized match: `NetworkID|||normalizedadvertiser`
- Default: `"Unassigned"`

**Step 6: Build Violation Row**
- 24 columns including Network ID, dates, metrics, issue type, details, owner
- Multiple issue types comma-separated: `"🟥 BILLING: Outside Flight, 🟦 DELIVERY: Ending Soon"`

**Step 7: Batch Write Violations**
- Collect violations in memory array
- Write entire chunk at once (faster than row-by-row)

**Step 8: Save State & Schedule Next Chunk**
- Update `startRow`, `violationsWritten`, `executionCount`
- If more rows: schedule trigger for 2 minutes
- If complete: clear state, cancel triggers

**Typical Execution:**
- 29,564 rows ÷ 3,500 = ~9 chunks
- ~40 seconds total (4-5s per chunk)
- Result: 1,205 violations (4.1% rate)

---

### Phase 4: Email Generation (Staged)

**Function:** `sendEmailReport()`

**Date Check:** Requires date ≥ 15th (bypass with `sendEmailNow()`)

**Staging Configuration:**
- **Time Budget:** 4.5 minutes per stage
- **State Storage:** DocumentProperties (`email_progress_v1`)
- **Max Owners/Chunk:** 5 owners

#### Stage 1: Network Summary

**Builds HTML Table:**
- **Columns:** Network ID, Network Name, Placements Checked, 🟥 BILLING, 🟦 DELIVERY, 🟨 PERFORMANCE, 🟩 COST
- **Rows:** All monitored networks + any with violations
- **Sort:** Alphabetical by network name

**Logic:**
1. Load network names from Networks sheet (Column A/B)
2. Count placements per network from Raw Data
3. Count violations per network by category
4. Include network if: monitored OR has violations

**Example Output:**
```
Network ID | Network Name  | Placements | 🟥 | 🟦 | 🟨 | 🟩
12345      | Acme Corp     | 1234       | 5  | 12 | 8  | 0
67890      | TO BE ADDED   | 567        | 0  | 3  | 2  | 0
```

#### Stage 2: Grouped Summary + Stale Metrics

**Grouped Summary:**
- Parses all violation issue types
- Groups by category (BILLING, DELIVERY, PERFORMANCE, COST)
- Counts each subtype
- Excludes Low Priority items
- Formats as bulleted lists

**Example:**
```
🟥 BILLING
  • Delivery Outside Placement Flight: 23
  • Invalid Dates: 5

🟦 DELIVERY
  • Placement Ended: 45
  • Ending Soon: 18
```

**Stale Metrics Table:**
- Filters violations containing "Stale Impressions" or "Stale Clicks"
- Shows Network ID, Advertiser, Placement, Issue, Details, Owner
- If none: displays "No stale metrics detected"

#### Stage 3: Immediate Attention (Chunked by Owner)

**Step 1: Group Violations by Owner**
- Parse Owner (Ops) column
- Build map: `{ "alice@horizon.com": [violations...] }`
- Sort owners by violation count (descending)

**Step 2: Process 5 Owners per Chunk**
- Build HTML section for each owner
- Format: `🚨 alice@horizon.com (47 violations)`

**Step 3: Build Owner Table**
- **Columns:** Network, Advertiser, Campaign, Placement, Issue Type, Details, Impressions, Clicks
- **Handled Placements:** Green background (`#d4edda`)
- **Active Violations:** Default background

**Handled Placement Logic:**
- Stored in DocumentProperties with timestamp
- Users reply "HANDLED {Network ID} {Placement ID}"
- Green rows indicate violation resolved
- Auto-expires after 90 days or placement end date

#### Stage 4: XLSX Generation + Email Send

**Step 1: Generate XLSX Attachment**
1. Create temporary spreadsheet
2. Copy Violations sheet to temp
3. Export via Google Sheets API: `spreadsheets/d/{id}/export?format=xlsx`
4. Delete temp spreadsheet
5. Store file ID in state (~450KB typical)

**Step 2: Build Final Email HTML**
- Concatenate all cached HTML sections
- Add reply commands section
- Add "How to Add New Network" instructions
- Add footer with credits

**Step 3: Size Check**
- Max: 90KB HTML
- If exceeded: truncate and add notice

**Step 4: Load Recipients**
- Read EMAIL LIST sheet (Column A)
- Deduplicate emails

**Step 5: Send Emails**
- Loop through recipients
- Send with XLSX attachment
- Sleep 300ms between sends (rate limiting)
- Catch failures, notify admin

**Step 6: Cleanup**
- Clear email state
- Cancel triggers
- Delete temp XLSX from Drive
- Log completion

**Typical Timing:**
- Stage 1: ~5s
- Stage 2: ~10s
- Stage 3: ~45s (15 owners × 3s)
- Stage 4: ~93s (XLSX generation + send)
- **Total: ~2m 33s**

---

## 🧠 Decision Logic

### Filtering Rules

**Decision Tree:**
```
FOR EACH RAW DATA ROW:
  │
  ├─ Placement contains "DART Search"?
  │  └─ YES → SKIP (not billable)
  │
  ├─ Advertiser = "Grand Total"?
  │  └─ YES → SKIP (summary row)
  │
  ├─ Advertiser in ignore list?
  │  └─ YES → SKIP (user exclusion)
  │
  ├─ Monitored Networks list populated?
  │  ├─ YES → Network ID in list?
  │  │  ├─ YES → PROCESS
  │  │  └─ NO → SKIP
  │  └─ NO → PROCESS (all networks)
  │
  └─ PROCEED TO VIOLATION DETECTION
```

### Violation Detection Rules

**Priority Matrix:**

| Category | Priority | Triggers | Business Impact |
|----------|----------|----------|----------------|
| 🟥 BILLING | CRITICAL | Outside flight dates, invalid dates | Client billing errors |
| 🟦 DELIVERY | HIGH | Ended, ending soon, not started | Campaign delivery issues |
| 🟨 PERFORMANCE | MEDIUM | High CTR (>90%), stale metrics | Optimization opportunities |
| 🟩 COST | MEDIUM | High CPC/CPM (>$10) | Budget overruns |

**Stale Metrics Detection:**

```
INITIALIZATION (First Run):
  Store: {
    key: "12345|||987654321|||imp",
    lastImp: 1000,
    lastReport: "2026-02-04"
  }

SUBSEQUENT RUNS:
  1. Fetch current impressions from Raw Data
  2. Compare to stored lastImp
  3. Calculate days since lastReport
  
  IF impressions == lastImp AND days >= threshold:
    → VIOLATION: "Stale Impressions (No change for X days)"
  
  ELSE IF impressions != lastImp:
    → UPDATE stored values (no violation)

CLEANUP:
  - Delete if placement ended + metrics stopped
  - Delete if > 90 days with no activity
```

### Owner Assignment Logic

**Lookup Priority:**
```
INPUT: Network ID = "12345", Advertiser = "Acme Corp (US)"

STEP 1: Exact lowercase match
  Key = "12345|||acme corp (us)"
  Lookup in Networks sheet (P/Q/R/S)
  FOUND? → Return owner email

STEP 2: Normalized match
  Normalize = remove spaces, special chars
  Key = "12345|||acmecorpus"
  Lookup in owner map
  FOUND? → Return owner email

STEP 3: Default fallback
  Return "Unassigned"
```

**Normalization:**
```
INPUT:  "ABC Company (US) - 2024"
STEPS:
  1. Lowercase: "abc company (us) - 2024"
  2. Remove spaces: "abccompany(us)-2024"
  3. Remove special chars: "abccompanyus2024"
OUTPUT: "abccompanyus2024"
```

### Chunking Decisions

**QA Chunking:**
```
TOTAL: 29,564 rows
CHUNK: 3,500 rows
BUDGET: 4.2 minutes

Execution 1: Rows 2-3,501     (40s) → Continue
Execution 2: Rows 3,502-7,001 (38s) → Continue
...
Execution 9: Rows 28,002-29,564 (15s) → Complete
```

**Email Chunking:**
```
Stage 1: Network Summary (single execution, ~5s)
Stage 2: Grouped Summary (single execution, ~10s)
Stage 3: Immediate Attention
  - 15 owners total
  - 5 owners per chunk
  - Execution 1: Owners 1-5
  - Execution 2: Owners 6-10
  - Execution 3: Owners 11-15
Stage 4: Send (single execution, ~93s)
```

---

## ⚙️ Configuration & Setup

### Required Sheets

**1. Raw Data** (auto-created)
- **Purpose:** Imported CM360 placement data
- **Cleared:** On each import
- **Columns:** Network ID, Advertiser, Placement ID, Placement, Campaign, Start/End Dates, Ad, Impressions, Clicks, Report Date

**2. Violations** (auto-created)
- **Purpose:** QA violations
- **Cleared:** On each QA run
- **Columns:** 24 total including Network, Dates, Metrics, Issue Type, Details, Owner

**3. Networks** (user-maintained)
- **Column A:** Network ID (auto-populated)
- **Column B:** Friendly Name (auto: "TO BE ADDED", then manual)
- **Column P:** Network ID (for owner mapping)
- **Column Q:** Friendly Name (for owner mapping)
- **Column R:** Advertiser Name
- **Column S:** Account Rep OPS (owner email)

**4. EMAIL LIST** (user-maintained)
- **Column A:** Email addresses (one per row)

**5. Advertisers to ignore** (user-maintained)
- **Column A:** Advertiser names (79 entries)
- **Purpose:** Exclude test accounts, internal campaigns

**6. Monitored Networks** (deprecated)
- **Status:** Empty = all networks monitored
- **Future:** Will be removed

### Configuration Cells

| Cell | Value | Purpose |
|------|-------|---------|
| Networks!I1 | TRUE/FALSE | Enable mid-flight drop (currently disabled in code) |
| Networks!I2 | 5 | Stale metrics threshold (days) |

### Gmail Setup

**Label:** `CM360 QA`

**Filter Setup:**
1. Gmail → Settings → Filters and Blocked Addresses
2. Create filter:
   - **From:** teammates or `platformsolutionsadopshorizon@gmail.com`
   - **Subject:** Contains `BKCM360 Global QA Check`
   - **Apply label:** `CM360 QA`

### Apps Script Properties

**DocumentProperties** (per-document state):
- `qa_progress_v2`: Chunking state (JSON)
- `email_progress_v1`: Email state (JSON)
- `violation_map_v2`: Stale tracking (JSON)
- `handled_placements_v1`: Resolved violations (JSON)

**ScriptProperties** (global):
- `qa_chunk_trigger_id`: Active QA trigger
- `email_chunk_trigger_id`: Active email trigger

---

## 📊 Sheets Reference

### Networks Sheet Structure

```
     A          B              P          Q              R              S
┌─────────┬─────────────────┬─────────┬─────────────┬──────────────┬──────────────────┐
│Net ID   │Friendly Name    │Net ID   │Friendly Name│Advertiser    │Account Rep OPS   │
├─────────┼─────────────────┼─────────┼─────────────┼──────────────┼──────────────────┤
│12345    │Acme Corp        │12345    │Acme Corp    │Acme Widget A │alice@horizon.com │
│12345    │Acme Corp        │12345    │Acme Corp    │Acme Widget B │alice@horizon.com │
│67890    │TO BE ADDED      │         │             │              │                  │
│99999    │Example Network  │99999    │Example Net  │Example Brand │bob@horizon.com   │
└─────────┴─────────────────┴─────────┴─────────────┴──────────────┴──────────────────┘
```

**Column Purpose:**
- **A/B:** Master network list (auto-populated by `autoAddNewNetworks_`)
- **P/Q/R/S:** Advertiser → owner mapping (manual entry, used by `loadOwnerMapFromNetworks_`)

### Violations Sheet Columns

| Col | Name | Example | Source |
|-----|------|---------|--------|
| A | Network ID | "12345" | Raw Data |
| B | Report Date | "2026-02-04" | Import |
| C | Advertiser | "Acme Corp" | Raw Data |
| D | Campaign | "Holiday 2026" | Raw Data |
| E-F | Campaign Dates | "2026-01-01" | Raw Data |
| G | Ad | "Banner 300x250" | Raw Data |
| H-I | Placement ID/Name | "987654321" | Raw Data |
| J-K | Placement Dates | "2026-01-15" | Raw Data |
| L-M | Impressions/Clicks | 15000, 450 | Raw Data |
| N | CTR (%) | 3.00 | Calculated |
| O | Days Until End | 5 | Calculated |
| P | Flight % | 68.2 | Calculated |
| Q | Days Left Month | 12 | Calculated |
| R-T | CPC Risk, $CPC, $CPM | Placeholders | Future |
| U | Issue Type | "🟥 BILLING: Outside Flight" | Detected |
| V | Details | "Report date outside dates" | Detected |
| W-X | Last Imp/Click Change | "2026-02-03" | Stale tracking |
| Y | Owner (Ops) | "alice@horizon.com" | Mapped |

---

## 🔧 Trigger Management

### Auto-Resume Triggers

**Purpose:** Continue chunked execution without hitting 6-minute timeout

**QA Triggers:**
- **Function:** `runQAOnly()`
- **Delay:** 1-2 minutes between chunks
- **Storage:** ScriptProperties `qa_chunk_trigger_id`

**Email Triggers:**
- **Function:** `sendEmailReport()`
- **Delay:** 2 minutes between stages
- **Storage:** ScriptProperties `email_chunk_trigger_id`

**Safety Mechanism:**
```javascript
// Prevents duplicate triggers
if (existingTriggerId) {
  const stillExists = ScriptApp.getProjectTriggers()
    .some(t => t.getUniqueId() === existingTriggerId);
  if (stillExists) return; // Don't create duplicate
}
```

### Manual Menu Items

| Menu Item | Function | Purpose |
|-----------|----------|---------|
| Import DCM Reports | `importDCMReports()` | Fetch from Gmail |
| Run QA Only | `runQAOnly()` | Detect violations |
| Send Email Report | `sendEmailReport()` | Generate & send email |
| FORCE Send Email Now | `sendEmailNow()` | Bypass date check |
| Debug QA Filtering | `debugQAFiltering()` | Trace filtering logic |
| Debug QA Logic | `debugQALogic()` | Test detection rules |
| Count Non-Zero Rows | `countNonZeroRows()` | Validate data quality |

---

## 🚨 Error Handling

### Timeout Prevention

**Strategy:** Chunked execution with time budgets
- QA: 4.2 minutes per chunk (70% of 6-min limit)
- Email: 4.5 minutes per stage (75% of 6-min limit)
- Save progress to DocumentProperties
- Schedule trigger to resume after 1-2 minutes

### Error Notification

**Admin Email:** `bkaufman@horizonmedia.com`

**Scenarios:**
1. Email send failure → Notify with failed recipient list
2. Gmail fetch error → Log, continue with partial data
3. CSV parse error → Skip invalid file, continue
4. Sheet access error → Log, exit gracefully

### State Recovery

**Corruption Handling:**
- Try to load state from DocumentProperties
- If JSON parse fails → Clear state, start fresh
- Manual recovery: Delete property keys via Script Editor

**Recovery Commands:**
```javascript
// Clear stuck QA state
PropertiesService.getDocumentProperties().deleteProperty('qa_progress_v2');

// Clear stuck email state
PropertiesService.getDocumentProperties().deleteProperty('email_progress_v1');

// Cancel all triggers
ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));
```

---

## 🐛 Debug Tools

### Tool 1: Debug QA Filtering

**Function:** `debugQAFiltering()`

**Output:**
```
=== QA FILTERING DEBUG (First 100 rows) ===
Total: 100 rows

Filters Applied:
  DART Search: 3 rows
  Grand Total: 2 rows
  Ignored Advertisers: 12 rows
  Monitored Networks: 0 rows (disabled)

Result:
  Filtered Out: 17 rows (17%)
  Passed Filter: 83 rows (83%)
```

### Tool 2: Debug QA Logic

**Function:** `debugQALogic()`

**Output:**
```
Row 2: Network 12345 | Acme Corp | Homepage
  ✅ PASSED FILTERS
  Impressions: 15000 | Clicks: 450 | CTR: 3.00%
  
  Violations:
    🟦 DELIVERY: Ending Soon (Low Priority)
      Details: 5 days until placement ends

Summary:
  Rows Processed: 100
  Violations Found: 10
  Detection Rate: 10%
```

### Tool 3: Count Non-Zero Rows

**Function:** `countNonZeroRows()`

**Output:**
```
Total Rows: 29,564
Impressions > 0: 28,123 (95.1%)
Clicks > 0: 27,456 (92.9%)
Either > 0: 28,234 (95.5%)
Zero Activity: 1,330 (4.5%)

Interpretation: ✅ 95.5% active (healthy)
```

---

## 🔄 Maintenance & Updates

### Regular Tasks

**Weekly:**
- Review "TO BE ADDED" networks → Update friendly names
- Add advertiser → owner mappings (Columns P/Q/R/S)
- Update EMAIL LIST (joiners/leavers)

**Monthly:**
- Add new test accounts to Advertisers to ignore
- Audit handled placements (clear old entries)
- Review stale threshold (adjust if false positives)

**Quarterly:**
- Analyze violation rate trends (~4-5% is normal)
- Review detection rules for accuracy
- Update documentation

### Code Updates

**Local Development:**
1. Edit Code.js in VS Code
2. Test in Apps Script editor (small sections)
3. Commit to git:
   ```powershell
   git add Code.js
   git commit -m "Description"
   git push origin master
   ```

**Deploy to Apps Script:**
```powershell
clasp push
```

### Common Modifications

**Add New Violation Rule:**
```javascript
// In runQAOnly() around line 1700
if (/* condition */) {
  issueTypes.push("🟨 PERFORMANCE: New Rule");
  details.push("Description");
}
```

**Change Stale Threshold:**
- Option 1: Update Networks!I2 cell (no code change)
- Option 2: Modify default in `getStaleThresholdDays_()`

**Add Email Recipient:**
- Add to EMAIL LIST sheet Column A (no code change)

### Performance Benchmarks

**Current (Feb 2026):**
- Import: ~5s
- Auto-add: <1s
- QA: ~40s (9 chunks)
- Email: ~2m 33s (4 stages)
- **Total: ~3m 18s**

**Optimization Opportunities:**
1. Cache owner map (load once vs. per-row)
2. Direct XLSX export (avoid temp spreadsheet)
3. Parallel processing (if Apps Script adds threading)

**Scaling Limits:**
- Current: 29,564 rows ✅
- Estimated max: ~50,000 rows before memory issues
- If exceeded: Reduce data retention (e.g., last 30 days only)

---

## 📈 System Metrics

### Current Performance

**Data Volume:**
- Raw Rows: 29,564
- Violations: 1,205 (4.1% rate)
- Networks: 25
- Recipients: 11
- Ignored Advertisers: 79

**Violation Breakdown:**
- 🟥 BILLING: ~5% (critical)
- 🟦 DELIVERY: ~45% (ended/ending placements)
- 🟨 PERFORMANCE: ~30% (stale metrics, high CTR)
- 🟩 COST: ~20% (high CPC/CPM when enabled)

**Email Stats:**
- HTML: ~65KB (under 90KB limit)
- XLSX: ~450KB
- Send Success: 99.8%
- Delivery: <5s per recipient

### Historical Context

**Bug Fix (Feb 4, 2026):**
- **Before:** 0 violations (0% detection) ❌
- **After:** 1,205 violations (4.1% detection) ✅
- **Cause 1:** `bidmanager` filter blocking all DV360
- **Cause 2:** Empty Monitored Networks blocking all rows

**Performance:**
- **Before:** Email timeout risk (near 6-min limit)
- **After:** 2m 33s (safe margin)

---

## 🎯 Quick Reference

### Key Functions

| Function | Purpose | Trigger |
|----------|---------|---------|
| `importDCMReports()` | Import CSV from Gmail | Manual |
| `autoAddNewNetworks_()` | Add new Network IDs | Auto (after import) |
| `runQAOnly()` | Detect violations | Manual or trigger |
| `sendEmailReport()` | Generate & send email | Manual or trigger |
| `sendEmailNow()` | FORCE send (bypass date) | Manual (testing) |
| `debugQAFiltering()` | Trace filtering | Manual (debug) |
| `debugQALogic()` | Test detection | Manual (debug) |
| `countNonZeroRows()` | Validate data | Manual (debug) |

### Email Reply Commands

| Command | Action |
|---------|--------|
| `HANDLED {Net ID} {Placement ID}` | Mark violation resolved |
| `REMOVE NETWORK {Net ID}` | Remove from monitoring |

### Configuration

| Location | Value | Purpose |
|----------|-------|---------|
| Networks!I1 | TRUE/FALSE | Mid-flight drop toggle |
| Networks!I2 | 5 | Stale threshold (days) |

### Constants

```javascript
QA_CHUNK_ROWS = 3500
QA_TIME_BUDGET_MS = 4.2 * 60 * 1000
EMAIL_TIME_BUDGET_MS = 4.5 * 60 * 1000
MAX_OWNERS_PER_CHUNK = 5
MAX_HTML_CHARS = 90000
ADMIN_EMAIL = 'bkaufman@horizonmedia.com'
```

---

## 📞 Support

**Developer:** Brian Kaufman (BK)  
**Email:** bkaufman@horizonmedia.com  
**Team:** Platform Solutions Automation  
**GitHub:** https://github.com/bkaufman7/main---END-OF-MONTH-CM360-CPC-CPM-FLIGHT-QA---main  
**Apps Script:** https://script.google.com/u/0/home/projects/1I5goMYzf3vnaXFuPxycMj2b6R1EFBUAsdJ_UpOs-YL2naMoGk62MsQKm/edit

---

**Last Updated:** February 4, 2026  
**Version:** 2.0.0