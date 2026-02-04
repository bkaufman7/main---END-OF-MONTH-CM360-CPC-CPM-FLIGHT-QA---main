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

*[Continue with full detailed documentation as provided above...]*

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