# CM360 QA Automation - Troubleshooting Guide

## 🚨 Quick Diagnosis

### Issue: QA found 0 violations

**Symptom:** QA runs successfully but Violations sheet shows 0 rows despite having data in Raw Data.

**Common Causes:**

1. **Empty Monitored Networks List Blocking All Rows** ✅ FIXED (Feb 4, 2026)
   - **Diagnosis:** Check if Monitored Networks sheet is empty
   - **Why it happens:** Code was checking if network ID exists in empty list
   - **Solution:** Now fixed - empty list means "process all networks"
   - **Manual workaround (old versions):** Add at least one network to Monitored Networks sheet

2. **Bidmanager Filter Blocking DV360 Advertisers** ✅ FIXED (Feb 4, 2026)
   - **Diagnosis:** Check if Raw Data has advertisers containing "bidmanager"
   - **Why it happens:** Hardcoded filter was blocking all DV360 advertisers
   - **Solution:** Now fixed - filter removed
   - **Manual workaround (old versions):** None - requires code update

3. **All Rows Filtered Out by Ignore List**
   - **Diagnosis:** Check "Advertisers to ignore" sheet - are all your advertisers listed?
   - **Solution:** Remove advertisers from ignore list that should be monitored
   - **Verify:** Run `debugQAFiltering()` from menu - check "Ignored Advertisers" count

4. **All Placements are DART Search**
   - **Diagnosis:** Check Raw Data - do Placement names contain "DART Search"?
   - **Why it happens:** DART Search placements are non-billable, auto-excluded
   - **Solution:** This is expected behavior - DART Search placements should be filtered

5. **No Data Has Violations**
   - **Diagnosis:** Run `debugQALogic()` from menu on first 100 rows
   - **Check:** Are placements within flight dates? CTR < 90%? Metrics changing?
   - **Solution:** This might be legitimate - healthy campaigns = fewer violations

**Quick Test:**
```javascript
// Run from Script Editor
function testQAFiltering() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawData = ss.getSheetByName("Raw Data").getDataRange().getValues();
  Logger.log("Total rows: " + (rawData.length - 1));
  
  // Count by advertiser
  const advCounts = {};
  for (let i = 1; i < rawData.length; i++) {
    const adv = String(rawData[i][1] || "").trim();
    advCounts[adv] = (advCounts[adv] || 0) + 1;
  }
  Logger.log("Advertisers: " + JSON.stringify(advCounts));
}
```

---

### Issue: "Not yet the 15th of the month"

**Symptom:** Email send button shows alert: "Not yet the 15th of the month. Email will be sent after the 15th."

**Why it happens:** Built-in safety to prevent accidental mid-month sends

**Solutions:**

1. **Testing/Override:**
   - Use **"FORCE Send Email Now"** menu item (bypasses date check)
   - Function: `sendEmailNow()`

2. **Change Date Restriction:**
   ```javascript
   // In sendEmailReport() function around line 1884
   // Change this line:
   if (today.getDate() < 15) {
   // To:
   if (today.getDate() < 1) {  // Always allows sending
   ```

3. **Remove Restriction Entirely:**
   - Delete the entire date check block (lines ~1884-1888)

---

### Issue: Network shows "TO BE ADDED" forever

**Symptom:** Network appears in Networks sheet with "TO BE ADDED" but never gets updated with friendly name.

**Why it happens:** Auto-add feature populates Column A/B automatically, but friendly name requires manual update.

**Solution:**

1. **Update Friendly Name:**
   - Open Networks sheet
   - Find row with your Network ID in Column A
   - Update Column B with actual network name (e.g., "Acme Corp")

2. **Add Owner Mapping:**
   - Same row, populate Columns P, Q, R, S:
     - **P:** Network ID (duplicate from A)
     - **Q:** Network Name (duplicate from B)
     - **R:** Advertiser Name (from Raw Data)
     - **S:** Account Rep OPS email (owner)

3. **Verify:**
   - Next QA run will assign violations to specified owner
   - Check Violations sheet Column Y (Owner)

**Prevention:** Set up a weekly reminder to review Networks sheet for "TO BE ADDED" entries.

---

### Issue: Stale metrics false positives

**Symptom:** Getting violations for "Stale Impressions" or "Stale Clicks" on placements that ARE changing.

**Common Causes:**

1. **Threshold Too Low**
   - **Check:** Networks sheet, Cell I2 (default: 5 days)
   - **Solution:** Increase to 7-10 days for less sensitive detection
   - **Update:** Change cell value, no code change needed

2. **Metrics Changed But Below Detection**
   - **Example:** Impressions: 1000 → 1001 (detected as stale)
   - **Why:** System only tracks exact value, not percentage change
   - **Solution:** This is a known limitation - significant changes will be caught

3. **First Run After Long Break**
   - **Why:** System doesn't have historical data yet
   - **Solution:** Ignore first run's stale metrics, valid on subsequent runs
   - **Reset:** Clear `violation_map_v2` DocumentProperty to start fresh

**Manual Reset:**
```javascript
// Run from Script Editor
function clearStaleMetrics() {
  PropertiesService.getDocumentProperties().deleteProperty('violation_map_v2');
  SpreadsheetApp.getUi().alert('✅ Stale metrics tracking reset');
}
```

---

### Issue: Handled placements not showing green

**Symptom:** Replied with "HANDLED {Network ID} {Placement ID}" but row still appears white in next email.

**Common Causes:**

1. **Incorrect Format**
   - **Required:** `HANDLED {Network ID} {Placement ID}` on single line
   - **Example:** `HANDLED 12345 987654321`
   - **Wrong:** `HANDLED 12345` (missing placement ID)
   - **Wrong:** `Handled 12345 987654321` (lowercase)

2. **Email Not Processed**
   - **Check:** Gmail label "CM360 QA Replies" applied?
   - **Verify:** Reply processor trigger installed? (Menu: "Install Reply Processor")
   - **Test:** Check logs after 7am next day (trigger runs daily)

3. **Placement ID Mismatch**
   - **Check:** Placement ID must EXACTLY match Violations sheet Column H
   - **Common mistake:** Using Placement Name instead of Placement ID
   - **Example:** `HANDLED 12345 Homepage` ❌ vs `HANDLED 12345 987654321` ✅

4. **Handled Placements Expired**
   - **Why:** Auto-expires after 90 days or placement end date passes
   - **Check:** Run `getHandledPlacements()` to see current map
   - **Solution:** Reply "HANDLED" again to re-add

**Verify Handled Status:**
```javascript
// Run from Script Editor
function getHandledPlacements() {
  const mapStr = PropertiesService.getDocumentProperties().getProperty('handled_placements_v1');
  if (!mapStr) {
    Logger.log("No handled placements found");
    return;
  }
  const map = JSON.parse(mapStr);
  Logger.log("Handled placements: " + JSON.stringify(map, null, 2));
}
```

---

### Issue: Email attachment too large

**Symptom:** Email fails to send or gets rejected due to attachment size.

**Why it happens:** XLSX file exceeds Gmail's 25MB attachment limit (rare with ~1,200 violations).

**Solutions:**

1. **Check Violations Count:**
   ```javascript
   const violationsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Violations");
   Logger.log("Violations: " + (violationsSheet.getLastRow() - 1));
   ```

2. **Reduce Data:**
   - Add more advertisers to ignore list
   - Enable Monitored Networks filter (process fewer networks)
   - Filter out Low Priority violations more aggressively

3. **Split Email by Owner:**
   - Modify code to send separate emails per owner
   - Each email contains only that owner's violations

4. **Alternative Delivery:**
   - Upload XLSX to Google Drive
   - Send email with Drive link instead of attachment

---

### Issue: Timeout errors mid-execution

**Symptom:** Script stops with "Maximum execution time exceeded" (6 minutes).

**Why it happens:** Processing too much data in single execution without chunking.

**Diagnosis:**

1. **Check State:**
   ```javascript
   const qaState = PropertiesService.getDocumentProperties().getProperty('qa_progress_v2');
   Logger.log("QA State: " + qaState);
   
   const emailState = PropertiesService.getDocumentProperties().getProperty('email_progress_v1');
   Logger.log("Email State: " + emailState);
   ```

2. **Identify Phase:**
   - **QA Phase:** Check if violations being written
   - **Email Phase:** Check logs for "Stage X/4"

**Solutions:**

1. **QA Timeout:**
   - Reduce `QA_CHUNK_ROWS` (currently 3,500)
   - Increase `QA_TIME_BUDGET_MS` safety margin
   - Let auto-resume trigger complete (wait 2 minutes)

2. **Email Timeout:**
   - Reduce `MAX_OWNERS_PER_CHUNK` (currently 5)
   - Increase `EMAIL_TIME_BUDGET_MS` safety margin
   - Let staged execution complete (wait 2 minutes between stages)

3. **Manual Recovery:**
   ```javascript
   // Clear stuck state
   function clearAllStates() {
     const props = PropertiesService.getDocumentProperties();
     props.deleteProperty('qa_progress_v2');
     props.deleteProperty('email_progress_v1');
     
     // Cancel triggers
     ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));
     
     SpreadsheetApp.getUi().alert('✅ States cleared, triggers canceled');
   }
   ```

---

### Issue: Owner showing as "Unassigned" when mapping exists

**Symptom:** Violations sheet Column Y shows "Unassigned" despite having Network + Advertiser mapped in Networks sheet.

**Common Causes:**

1. **Exact Match Failed**
   - **Check:** Networks sheet Columns P (Network ID) and R (Advertiser) exactly match Raw Data?
   - **Case sensitivity:** "Acme Corp" ≠ "acme corp" ≠ "ACME CORP"
   - **Extra spaces:** "Acme Corp" ≠ "Acme Corp " (trailing space)
   - **Special chars:** "Acme Corp (US)" ≠ "Acme Corp"

2. **Normalized Match Failed**
   - **System tries:** `"12345|||acmecorpus"` (removes spaces, special chars)
   - **Check:** Run normalization test:
   ```javascript
   function normalizeAdv_(adv) {
     return adv.toLowerCase()
       .replace(/\s+/g, "")
       .replace(/[^a-z0-9]/g, "")
       .trim();
   }
   
   // Test
   Logger.log(normalizeAdv_("Acme Corp (US) - 2024")); // "acmecorpus2024"
   ```

3. **Wrong Columns Used**
   - **Required:** Owner mapping reads from Columns P/Q/R/S (NOT A/B)
   - **Verify:** 
     - Column P: Network ID
     - Column Q: Network Name (for reference)
     - Column R: Advertiser Name (EXACT match from Raw Data)
     - Column S: Owner email

4. **Multiple Advertisers, Missing Entry**
   - **Scenario:** Network 12345 has 5 advertisers, only 4 mapped
   - **Solution:** Add row for each advertiser in network:
     ```
     P      Q           R               S
     12345  Acme Corp   Acme Widget A   alice@example.com
     12345  Acme Corp   Acme Widget B   alice@example.com
     12345  Acme Corp   Acme Widget C   bob@example.com
     ```

**Debug Owner Mapping:**
```javascript
function debugOwnerMapping() {
  const ownerMap = loadOwnerMapFromNetworks_();
  Logger.log("Owner map keys: " + Object.keys(ownerMap.byKey).join(", "));
  
  // Test specific combination
  const testKey = "12345|||acme corp";
  Logger.log("Test key '" + testKey + "': " + JSON.stringify(ownerMap.byKey[testKey]));
}
```

---

## 📋 Error Message Reference

### Log Message Tags

| Tag | Severity | Meaning |
|-----|----------|---------|
| ✅ | Success | Operation completed successfully |
| ❌ | Error | Operation failed, requires attention |
| ⚠️ | Warning | Operation completed but with issues |
| 📧 | Info | Email-related activity |
| 📊 | Info | QA/Data processing activity |
| 🗓️ | Info | Scheduled/timed activity |
| 🗑️ | Info | Deletion/cleanup activity |
| 📋 | Info | Data import/export activity |
| 🔍 | Debug | Detailed diagnostic information |
| ⏳ | Progress | Chunked execution progress |
| 🚨 | Alert | Immediate attention required |

---

### Common Error Messages

#### Import Phase

**`"autoAddNewNetworks_: Required sheets not found"`**
- **Cause:** Raw Data or Networks sheet missing
- **Solution:** Run "Import DCM Reports" first to create Raw Data sheet
- **Prevention:** Don't delete system sheets

**`"autoAddNewNetworks_: No data in Raw Data sheet"`**
- **Cause:** Import found no valid CSV files
- **Check:** Gmail label "CM360 QA" applied to report emails?
- **Verify:** CSV files have "Advertiser" header row?

**`"autoAddNewNetworks_: No valid Network IDs in Raw Data"`**
- **Cause:** All Network IDs are "Unknown" (filename pattern didn't match)
- **Solution:** Rename CSV files to format: `{networkId}_*.csv`
- **Example:** `12345_CM360_Report.csv`

**`"autoAddNewNetworks_: No new networks to add"`**
- **Cause:** All networks already exist in Networks sheet
- **Result:** This is normal - no action needed

---

#### QA Phase

**`"❌ Violations sheet not found"`**
- **Cause:** Violations sheet was deleted or renamed
- **Solution:** Run QA again - sheet will be auto-created

**`"⏳ QA chunk complete, scheduling next chunk"`**
- **Meaning:** Processing large dataset, will continue in 2 minutes
- **Action:** Wait for auto-resume trigger to fire
- **Check:** ScriptProperties `qa_chunk_trigger_id` exists

**`"✅ QA Complete: X violations written in Ys"`**
- **Meaning:** QA finished successfully
- **X violations:** Total rows written to Violations sheet
- **Verify:** Check Violations sheet has data

**`"📊 QA Progress: X/Y rows (Z%) | N violations | Chunk C"`**
- **Meaning:** Chunked execution progress
- **X/Y:** Rows processed / Total rows
- **N:** Violations found so far
- **C:** Current chunk number

---

#### Email Phase

**`"Not yet the 15th of the month"`**
- **Cause:** Date safety check preventing accidental sends
- **Solution:** Use "FORCE Send Email Now" menu item
- **Code location:** Line ~1884 in `sendEmailReport()`

**`"📧 Email Stage 1/4: Building network summary..."`**
- **Meaning:** Stage 1 of 4 in progress
- **Time:** ~5 seconds
- **Output:** Network-level violation counts table

**`"📧 Email Stage 2/4: Building grouped summary..."`**
- **Meaning:** Stage 2 of 4 in progress
- **Time:** ~10 seconds
- **Output:** Violation type breakdowns, stale metrics table

**`"📧 Email Stage 3/4: Building immediate attention section..."`**
- **Meaning:** Stage 3 of 4 in progress (may chunk)
- **Time:** ~45 seconds
- **Output:** Per-owner violation tables

**`"📧 Email Stage 4/4: Generating XLSX and sending..."`**
- **Meaning:** Final stage - XLSX creation and email send
- **Time:** ~90 seconds
- **Output:** Email sent to recipients

**`"⏳ Email stage X partial, scheduling next chunk"`**
- **Meaning:** Stage not complete, will resume in 2 minutes
- **Action:** Wait for auto-resume trigger

**`"✅ Email sent successfully to X recipient(s) in Ym Zs"`**
- **Meaning:** Email delivery complete
- **X:** Number of recipients
- **Time:** Total email generation time

**`"❌ Failed to send failure notification: ..."`**
- **Cause:** Admin notification email failed (network issue)
- **Impact:** Non-critical - main email may have succeeded
- **Check:** Verify email arrived at recipients

---

#### Reply Processing

**`"📧 Found X reply threads"`**
- **Meaning:** Processing email replies from ops team
- **X:** Number of reply emails found
- **Trigger:** Daily at 7am

**`"❌ Parse error from {email}: {error}"`**
- **Cause:** Reply format doesn't match expected pattern
- **User notified:** Automatic error email sent to user
- **Examples:**
  - "No placement IDs found"
  - "Invalid HANDLED format"
  - "Network ID not numeric"

**`"✅ Processed X placement notes from email replies"`**
- **Meaning:** Successfully handled X "HANDLED" commands
- **Result:** Placements marked, will show green in next email

**`"🗑️ {email} removed X network(s): {ids}"`**
- **Meaning:** "REMOVE NETWORK" command processed
- **Result:** Networks removed from Monitored Networks sheet

**`"⚠️ X emails had errors"`**
- **Meaning:** X reply emails failed to process
- **Check:** Review logs for specific parse errors
- **Action:** Users will receive error emails with instructions

---

#### Data Validation

**`"⚠️ Invalid placement IDs from {email}: {ids}"`**
- **Cause:** Placement IDs in reply don't match format
- **Check:** IDs must be numeric strings
- **Example:** "abc123" is invalid, "987654321" is valid

**`"🗓️ First of month - clearing Handled Placements"`**
- **Meaning:** Monthly cleanup of handled placements map
- **When:** Automatically on 1st of each month
- **Result:** All green rows reset, violations show as active again

**`"🗑️ Removed network X (requested by {email})"`**
- **Meaning:** Network removed from monitoring via email command
- **Verify:** Check Monitored Networks sheet

---

#### Debug Tools

**`"=== QA FILTERING DEBUG (First 100 rows) ==="`**
- **Tool:** `debugQAFiltering()` menu item
- **Output:** Filter counts (DART Search, Grand Total, Ignored, etc.)
- **Use:** Diagnose why rows are being excluded

**`"=== QA LOGIC DEBUG (First 100 rows) ==="`**
- **Tool:** `debugQALogic()` menu item
- **Output:** Violation detection on sample rows
- **Use:** Verify detection rules working correctly

**`"=== NON-ZERO ROWS COUNT ==="`**
- **Tool:** `countNonZeroRows()` menu item
- **Output:** Activity stats (impressions/clicks > 0)
- **Use:** Validate data quality

---

## 🔧 Diagnostic Workflows

### Workflow 1: QA Found 0 Violations

```
STEP 1: Check Raw Data
  → Open "Raw Data" sheet
  → Verify data exists (rows > 1)
  → Check sample advertisers
  
STEP 2: Run Debug Filtering
  → Menu: Debug Tools → Debug QA Filtering
  → Check logs: How many rows filtered?
  → If 100% filtered → Identify which filter
  
STEP 3: Check Filters
  → Monitored Networks empty? (Expected)
  → All advertisers in ignore list? (Problem)
  → All placements "DART Search"? (Check data)
  
STEP 4: Run Debug QA Logic
  → Menu: Debug Tools → Debug QA Logic
  → Check logs: Violations on first 100 rows?
  → If 0 violations → Data might be healthy
  
STEP 5: Verify Detection Rules
  → Check placement dates in Raw Data
  → Calculate CTR manually (clicks/impressions * 100)
  → Are metrics actually violating rules?
```

### Workflow 2: Email Not Sending

```
STEP 1: Check Date
  → Is today ≥ 15th of month?
  → If not → Use "FORCE Send Email Now"
  
STEP 2: Check Violations
  → Open "Violations" sheet
  → Verify rows exist (> 1)
  → If empty → Run QA first
  
STEP 3: Check Email List
  → Open "EMAIL LIST" sheet
  → Verify emails exist in Column A
  → Check for typos
  
STEP 4: Check Execution
  → View → Execution log
  → Look for "Email Stage" messages
  → Check for timeout errors
  
STEP 5: Check State
  → Run from Script Editor:
     PropertiesService.getDocumentProperties().getProperty('email_progress_v1')
  → If stuck → Clear state with clearAllStates()
```

### Workflow 3: Handled Placements Not Working

```
STEP 1: Verify Reply Format
  → Check sent email
  → Format: HANDLED {Network ID} {Placement ID}
  → Example: HANDLED 12345 987654321
  
STEP 2: Check Gmail Label
  → Gmail → Search: label:CM360 QA Replies
  → Is reply labeled?
  → If not → Create filter
  
STEP 3: Check Trigger
  → Script Editor → Triggers
  → Look for "processEmailReplies" daily trigger
  → If missing → Menu: "Install Reply Processor"
  
STEP 4: Verify Processing
  → Wait until 7am next day (trigger runs)
  → Check logs for "Processed X placement notes"
  
STEP 5: Check Handled Map
  → Run getHandledPlacements() from Script Editor
  → Verify key format: "NetworkID|||PlacementID"
  → Check timestamp and placement end date
```

### Workflow 4: Owner Assignment Not Working

```
STEP 1: Check Networks Sheet Structure
  → Open "Networks" sheet
  → Verify Columns P/Q/R/S populated
  → Column P: Network ID
  → Column R: Advertiser (EXACT match)
  → Column S: Owner email
  
STEP 2: Get Advertiser from Raw Data
  → Open "Raw Data" sheet
  → Find advertiser name (Column B)
  → Copy EXACT text (including spaces, caps)
  
STEP 3: Add to Networks Sheet
  → Networks sheet, new row:
  → Column P: Network ID from Raw Data
  → Column Q: Network name (for reference)
  → Column R: Paste advertiser (EXACT)
  → Column S: Owner email
  
STEP 4: Test Normalization
  → Run debugOwnerMapping() from Script Editor
  → Check if key matches
  → Key format: "NetworkID|||advertiserlowercase"
  
STEP 5: Re-run QA
  → Menu: Run QA Only
  → Check Violations sheet Column Y
  → Should show owner email
```

---

## 🛡️ Prevention Best Practices

### Weekly Checklist

- [ ] Review Networks sheet for "TO BE ADDED" entries
- [ ] Update friendly names for new networks
- [ ] Add advertiser → owner mappings for new networks
- [ ] Verify EMAIL LIST has current team members
- [ ] Check handled placements working (green rows in email)

### Monthly Checklist

- [ ] Add new test accounts to "Advertisers to ignore"
- [ ] Review stale metrics threshold (Networks!I2)
- [ ] Audit handled placements (auto-clears 1st of month)
- [ ] Verify violation rate ~4-5% (healthy benchmark)
- [ ] Check execution times (QA < 1min, Email < 3min)

### Quarterly Checklist

- [ ] Review all violation detection rules for accuracy
- [ ] Analyze violation trends (by category)
- [ ] Update documentation if workflow changed
- [ ] Clean up old/inactive networks
- [ ] Test FORCE send button before month-end
- [ ] Verify reply processor trigger working

---

## 📞 Getting Help

### Check Logs First
1. **View Logs:** Extensions → Apps Script → View → Execution log
2. **Search for:** Error messages (❌), warnings (⚠️)
3. **Note:** Timestamp, function name, error details

### Common Support Questions

**"How do I..."**
- Add a new network? → See README.md "Phase 2: Network Auto-Add"
- Change violation thresholds? → See README.md "Configuration"
- Test email without sending? → Use `debugQALogic()` or `sendEmailNow()`
- Clear stuck state? → Run `clearAllStates()` function

**"Why is..."**
- QA taking so long? → Normal: ~40s for 29K rows (chunked)
- Email so large? → 1,200 violations = ~65KB HTML + ~450KB XLSX (normal)
- Date restriction needed? → Safety - prevents accidental mid-month sends

**Contact Developer:**
- **Name:** Brian Kaufman (BK)
- **Email:** bkaufman@horizonmedia.com
- **Team:** Platform Solutions Automation
- **GitHub:** https://github.com/bkaufman7/main---END-OF-MONTH-CM360-CPC-CPM-FLIGHT-QA---main

---

**Last Updated:** February 4, 2026  
**Version:** 2.0.0
