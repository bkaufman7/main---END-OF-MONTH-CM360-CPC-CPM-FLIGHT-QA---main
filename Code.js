// =====================
// CM360 QA Tools Script
// =====================
// Adds custom menu, imports CM360 reports via Gmail, runs QA checks,
// filters out ignored advertisers, and emails a summary of violations.

// ---------------------
// onOpen: Menu Setup
// ---------------------
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("CM360 QA Tools")
    .addItem("▶️ Run It All (Immediate)", "runItAll")
    .addItem("🔄 Run It All (Auto-Resume)", "runItAllChunked")
    .addSeparator()
    .addItem("Pull Data (Immediate)", "importDCMReports")
    .addItem("Pull Data (Auto-Resume)", "importDCMReportsChunked")
    .addItem("Run QA Only (Immediate)", "runQAOnlyImmediate")
    .addItem("Run QA Only (Auto-Resume)", "runQAOnly")
    .addItem("Send Email Only (Immediate)", "sendEmailSummaryImmediate")
    .addItem("Send Email Only (Auto-Resume)", "sendEmailSummary")
    .addSeparator()
    .addItem("📊 System Status", "showSystemStatus")
    .addItem("🔄 Reset All State (if stuck)", "resetAllState")
    .addSeparator()
    .addItem("Authorize Email (one-time)", "authorizeMail_")
    .addItem("Create Daily Email Trigger (9am)", "createDailyEmailTrigger")
    .addSeparator()
    .addItem("Clear Violations", "clearViolations")
    .addToUi();
}



// ---------------------
// one-time MailApp authorization helper
// ---------------------
function authorizeMail_() {
  // Running this from the editor or from the menu will force the OAuth prompt
  MailApp.sendEmail({
    to: 'platformsolutionsadopshorizon@gmail.com',
    subject: 'Apps Script auth test',
    htmlBody: 'If you received this, MailApp is authorized.'
  });
}

// ---------------------
// Create an installable time trigger for the email-only run
// ---------------------
function createDailyEmailTrigger() {
  // Runs runDailyEmailSummary daily at 9am local time with full auth
  ScriptApp.newTrigger('runDailyEmailSummary')
    .timeBased()
    .atHour(9)       // change if you prefer another hour
    .everyDays(1)
    .create();
}




// ---------------------
// clearViolations
// ---------------------
function clearViolations() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Violations");
  if (!sheet) return;
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).clearContent();
  }
}

// ---------------------
// extractNetworkId
// ---------------------
function extractNetworkId(fileName) {
  const match = fileName.match(/^([^_]+)_/);
  return match ? String(match[1]) : "Unknown";
}

// ---------------------
// processCSV
// ---------------------
function processCSV(fileContent, networkId) {
  const lines = fileContent.split("\n").map(line => line.trim()).filter(Boolean);
  const startIndex = lines.findIndex(line => line.startsWith("Advertiser"));
  if (startIndex === -1) return [];
  const csvData = Utilities.parseCsv(lines.slice(startIndex).join("\n"));
  csvData.shift(); // remove header row in the attachment
  const reportDate = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
  return csvData.map(function(row){ return [networkId].concat(row).concat([reportDate]); });
}

function importDCMReports() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const dataSheet = ss.getSheetByName("Raw Data") || ss.insertSheet("Raw Data");
  const outputSheet = ss.getSheetByName("Violations") || ss.insertSheet("Violations");
  const label = "CM360 QA";
  const formattedToday = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy/MM/dd");

  const dataHeaders = [
    "Network ID","Advertiser","Placement ID","Placement","Campaign",
    "Placement Start Date","Placement End Date","Campaign Start Date","Campaign End Date",
    "Ad","Impressions","Clicks","Report Date"
  ];
  // APPENDED "Owner (Ops)" to be column Y (25th)
  const outputHeaders = [
    "Network ID","Report Date","Advertiser","Campaign","Campaign Start Date","Campaign End Date",
    "Ad","Placement ID","Placement","Placement Start Date","Placement End Date",
    "Impressions","Clicks","CTR (%)","Days Until Placement End","Flight Completion %",
    "Days Left in the Month","CPC Risk","$CPC","$CPM","Issue Type","Details",
    "Last Imp Change","Last Click Change","Owner (Ops)"
  ];

  dataSheet.clearContents().getRange(1,1,1,dataHeaders.length).setValues([dataHeaders]);
  outputSheet.clearContents().getRange(1,1,1,outputHeaders.length).setValues([outputHeaders]);

  const threads = GmailApp.search('label:' + label + ' after:' + formattedToday);
  let extractedData = [];

  threads.forEach(function(thread){
    thread.getMessages().forEach(function(message){
      message.getAttachments().forEach(function(att){
        const netId = extractNetworkId(att.getName());
        if (att.getContentType() === "text/csv" || att.getName().endsWith(".csv")) {
          extractedData = extractedData.concat(processCSV(att.getDataAsString(), netId));
        } else if (att.getContentType() === "application/zip") {
          Utilities.unzip(att.copyBlob()).forEach(function(file){
            if (file.getContentType() === "text/csv" || file.getName().endsWith(".csv")) {
              extractedData = extractedData.concat(processCSV(file.getDataAsString(), extractNetworkId(file.getName())));
            }
          });
        }
      });
    });
  });

  if (extractedData.length) {
    dataSheet.getRange(2, 1, extractedData.length, dataHeaders.length).setValues(extractedData);
  }
}

// ====== Chunked QA execution control ======
const QA_CHUNK_ROWS = 3500;
const QA_TIME_BUDGET_MS = 4.2 * 60 * 1000;
const QA_STATE_KEY = 'qa_progress_v2';      // DocumentProperties key

// ====== Chunked EMAIL execution control ======
const EMAIL_TIME_BUDGET_MS = 4.5 * 60 * 1000;
const EMAIL_STATE_KEY = 'email_progress_v1';
const EMAIL_TRIGGER_KEY = 'email_chunk_trigger_id';
const MAX_OWNERS_PER_CHUNK = 5;

// ====== Error notification ======
const ADMIN_EMAIL = 'bkaufman@horizonmedia.com';

// --- Auto-resume trigger control for QA chunks ---
const QA_TRIGGER_KEY = 'qa_chunk_trigger_id';   // ScriptProperties key for one-shot trigger
const QA_LOCK_KEY = 'qa_chunk_lock';            // logical name only

function getScriptProps_() { return PropertiesService.getScriptProperties(); }

function scheduleNextQAChunk_(minutesFromNow) {
  minutesFromNow = Math.max(1, Math.min(10, Math.floor(minutesFromNow || 1))); // 1..10 min
  const props = getScriptProps_();

  // If a trigger is already scheduled, do nothing (unless it no longer exists)
  const existingId = props.getProperty(QA_TRIGGER_KEY);
  if (existingId) {
    const stillThere = ScriptApp.getProjectTriggers().some(function(t){ return t.getUniqueId() === existingId; });
    if (stillThere) return;
    props.deleteProperty(QA_TRIGGER_KEY);
  }

  const trig = ScriptApp
    .newTrigger('runQAOnly')      // re-enter same function
    .timeBased()
    .after(minutesFromNow * 60 * 1000)
    .create();

  props.setProperty(QA_TRIGGER_KEY, trig.getUniqueId());
}

function cancelQAChunkTrigger_() {
  const props = getScriptProps_();
  const id = props.getProperty(QA_TRIGGER_KEY);
  if (!id) return;
  ScriptApp.getProjectTriggers().forEach(function(t){
    if (t.getUniqueId() === id) ScriptApp.deleteTrigger(t);
  });
  props.deleteProperty(QA_TRIGGER_KEY);
}

function getQAState_() {
  const raw = PropertiesService.getDocumentProperties().getProperty(QA_STATE_KEY);
  return raw ? JSON.parse(raw) : null;
}
function saveQAState_(obj) {
  PropertiesService.getDocumentProperties().setProperty(QA_STATE_KEY, JSON.stringify(obj));
}
function clearQAState_() {
  PropertiesService.getDocumentProperties().deleteProperty(QA_STATE_KEY);
}

// ====== Email State Management (parallel to QA state) ======
function getEmailState_() {
  const raw = PropertiesService.getDocumentProperties().getProperty(EMAIL_STATE_KEY);
  return raw ? JSON.parse(raw) : null;
}

function saveEmailState_(obj) {
  PropertiesService.getDocumentProperties().setProperty(EMAIL_STATE_KEY, JSON.stringify(obj));
}

function clearEmailState_() {
  PropertiesService.getDocumentProperties().deleteProperty(EMAIL_STATE_KEY);
}

function scheduleNextEmailChunk_(minutesFromNow) {
  minutesFromNow = Math.max(1, Math.min(10, Math.floor(minutesFromNow || 2)));
  const props = getScriptProps_();
  
  const existingId = props.getProperty(EMAIL_TRIGGER_KEY);
  if (existingId) {
    const stillThere = ScriptApp.getProjectTriggers().some(function(t){ return t.getUniqueId() === existingId; });
    if (stillThere) return;
    props.deleteProperty(EMAIL_TRIGGER_KEY);
  }
  
  const trig = ScriptApp
    .newTrigger('sendEmailSummary')
    .timeBased()
    .after(minutesFromNow * 60 * 1000)
    .create();
  
  props.setProperty(EMAIL_TRIGGER_KEY, trig.getUniqueId());
}

function cancelEmailChunkTrigger_() {
  const props = getScriptProps_();
  const id = props.getProperty(EMAIL_TRIGGER_KEY);
  if (!id) return;
  ScriptApp.getProjectTriggers().forEach(function(t){
    if (t.getUniqueId() === id) ScriptApp.deleteTrigger(t);
  });
  props.deleteProperty(EMAIL_TRIGGER_KEY);
}

// ====== Error Notification System ======
function sendFailureEmail_(functionName, error, additionalContext) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const today = new Date();
    const dateStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "MMM dd, yyyy h:mm a");
    
    const errorMsg = error ? (error.message || String(error)) : 'Unknown error';
    const errorStack = error && error.stack ? error.stack : '';
    
    let context = additionalContext || {};
    
    const subject = '⚠️ CM360 QA FAILURE - ' + functionName + ' - ' + dateStr;
    
    let body = '<html><body style="font-family: Arial, sans-serif;">';
    body += '<h2 style="color: #d9534f;">⚠️ CM360 QA Automation Failure</h2>';
    body += '<table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; margin: 20px 0;">';
    body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Function</td><td>' + functionName + '</td></tr>';
    body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Timestamp</td><td>' + dateStr + '</td></tr>';
    body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Error</td><td style="color: #d9534f;">' + errorMsg + '</td></tr>';
    
    if (context.stage) {
      body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Stage</td><td>' + context.stage + '</td></tr>';
    }
    if (context.duration) {
      body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Duration</td><td>' + context.duration + '</td></tr>';
    }
    if (context.rawDataRows) {
      body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Raw Data Rows</td><td>' + context.rawDataRows + '</td></tr>';
    }
    if (context.violations) {
      body += '<tr><td style="font-weight:bold; background: #f5f5f5;">Violations</td><td>' + context.violations + '</td></tr>';
    }
    
    body += '</table>';
    
    if (errorStack) {
      body += '<h3>Stack Trace:</h3>';
      body += '<pre style="background: #f5f5f5; padding: 10px; overflow: auto;">' + errorStack + '</pre>';
    }
    
    body += '<p><b>Action Required:</b> Check the Apps Script execution logs or run the function manually from the menu to see detailed output.</p>';
    body += '<p><a href="https://script.google.com/home/projects/' + ScriptApp.getScriptId() + '/executions">View Execution Logs</a></p>';
    body += '<p><a href="' + ss.getUrl() + '">Open Spreadsheet</a></p>';
    body += '<hr/>';
    body += '<p style="color: #666; font-size: 11px;"><i>Automated failure notification from CM360 QA Tools</i></p>';
    body += '</body></html>';
    
    MailApp.sendEmail({
      to: ADMIN_EMAIL,
      subject: subject,
      htmlBody: body
    });
    
    logAuditEntry_(functionName, 'FAILED', null, context.rawDataRows, context.violations, errorMsg);
  } catch (e) {
    Logger.log('❌ Failed to send failure notification: ' + e);
  }
}

function isManualRun_() {
  // Check if we're running from a time-based trigger
  const triggers = ScriptApp.getProjectTriggers();
  const currentFunction = new Error().stack.split('\n')[2].match(/at (\w+)/);
  if (!currentFunction) return true;
  
  const funcName = currentFunction[1];
  const hasMatchingTrigger = triggers.some(function(t){
    return t.getHandlerFunction() === funcName && 
           t.getEventType() === ScriptApp.EventType.CLOCK;
  });
  
  return !hasMatchingTrigger;
}

// ====== Audit Logging ======
function getAuditSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    let sh = ss.getSheetByName("_Execution Log");
    if (!sh) {
      sh = ss.insertSheet("_Execution Log");
      sh.hideSheet();
      sh.getRange(1, 1, 1, 7).setValues([["Timestamp", "Function", "Status", "Duration", "Raw Rows", "Violations", "Error"]]);
    }
    return sh;
  } finally {
    lock.releaseLock();
  }
}

function logAuditEntry_(functionName, status, durationMs, rawRows, violations, error) {
  try {
    const sh = getAuditSheet_();
    const now = new Date();
    const duration = durationMs ? fmtMs_(durationMs) : '';
    const errorMsg = error ? String(error).substring(0, 500) : '';
    
    sh.appendRow([now, functionName, status, duration, rawRows || '', violations || '', errorMsg]);
    
    // Keep only last 1000 entries
    if (sh.getLastRow() > 1001) {
      sh.deleteRows(2, sh.getLastRow() - 1001);
    }
  } catch (e) {
    Logger.log('Failed to log audit entry: ' + e);
  }
}

// ---------------------
// getHeaderMap
// ---------------------
function getHeaderMap(headers) {
  const map = {};
  headers.forEach(function(h,i){ map[String(h).trim()] = i; });
  return map;
}

// ===== Helpers for change detection cache (PERFORMANCE alert snapshots use a sheet) =====
function getPerfAlertCacheSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const name = "_Perf Alert Cache";

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    let sh = ss.getSheetByName(name);
    if (!sh) {
      sh = ss.insertSheet(name);
      sh.hideSheet();
    }

    const needed = ["date","key","impressions","clicks"];
    const current = sh.getRange(1, 1, 1, 4).getValues()[0] || [];
    const ok = current.length === 4 && current
      .map(function(v){ return String(v).toLowerCase(); })
      .every(function(v, i){ return v === needed[i]; });

    if (!ok) {
      sh.getRange(1, 1, 1, 4).setValues([needed]);
    }
    return sh;
  } finally {
    lock.releaseLock();
  }
}

// Returns a map of latest snapshot by key: { key: { date: 'yyyy-MM-dd', imp: number, clk: number } }
function loadLatestCacheMap_() {
  const sh = getPerfAlertCacheSheet_();
  const vals = sh.getDataRange().getValues();
  const map = {};
  for (let i = 1; i < vals.length; i++) {
    const d   = vals[i][0];
    const key = String(vals[i][1] || "");
    const imp = Number(vals[i][2] || 0);
    const clk = Number(vals[i][3] || 0);
    if (!key) continue;
    const ds = (d && d.getFullYear) ? Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(d || "");
    if (!map[key] || ds > map[key].date) {
      map[key] = { date: ds, imp: imp, clk: clk };
    }
  }
  return map;
}

// Appends today's snapshots for all evaluated rows
function appendTodaySnapshots_(rowsForSnapshot) {
  if (!rowsForSnapshot.length) return;
  const sh = getPerfAlertCacheSheet_();
  const tz = Session.getScriptTimeZone();
  const todayStr = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
  const out = rowsForSnapshot.map(function(r){ return [todayStr, r.key, r.imp, r.clk]; });
  sh.getRange(sh.getLastRow()+1, 1, out.length, 4).setValues(out);
}

// Compact PERF ALERT cache to last N days
function compactPerfAlertCache_(keepDays) {
  keepDays = keepDays || 35;
  const sh = getPerfAlertCacheSheet_();
  const cutoff = new Date(Date.now() - keepDays*86400000);
  const vals = sh.getDataRange().getValues();
  if (vals.length <= 1) return;

  const keep = [vals[0]];
  for (let i = 1; i < vals.length; i++) {
    const d = vals[i][0] instanceof Date ? vals[i][0] : new Date(vals[i][0]);
    if (d >= cutoff) keep.push(vals[i]);
  }
  sh.clearContents();
  sh.getRange(1,1,keep.length,4).setValues(keep);
}

// ---------------------
// Ignore Advertisers sheet
// ---------------------
function loadIgnoreAdvertisers() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Advertisers to ignore");
  if (!sheet) return new Set();
  const rows = sheet.getDataRange().getValues();
  const ignoreMap = {};

  for (let i = 1; i < rows.length; i++) {
    const name = rows[i][0] && rows[i][0].toString().trim().toLowerCase();
    if (name) ignoreMap[name] = { row: i + 1, set: new Set() };
  }

  const raw = ss.getSheetByName("Raw Data");
  if (raw) {
    const data = raw.getDataRange().getValues();
    const m = getHeaderMap(data[0]);
    data.slice(1).forEach(function(r){
      const adv = r[m["Advertiser"]] && r[m["Advertiser"]].toString().trim().toLowerCase();
      const net = r[m["Network ID"]];
      if (adv && ignoreMap[adv]) ignoreMap[adv].set.add(net);
    });
    Object.values(ignoreMap).forEach(function(o){
      sheet.getRange(o.row, 2).setValue(o.set.size);
    });
  }

  return new Set(Object.keys(ignoreMap));
}

// ---------------------
// sendPerformanceSpikeAlertIfPre15
// ---------------------
function sendPerformanceSpikeAlertIfPre15() {
  const today = new Date();
  const dayOfMonth = today.getDate();
  if (dayOfMonth >= 15) return; // Only before 15th

  // Ensures the cache sheet exists before proceeding
  getPerfAlertCacheSheet_();

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Violations");
  const recipientsSheet = ss.getSheetByName("EMAIL LIST");
  if (!sheet || !recipientsSheet) return;

  // Recipient list
  const emails = recipientsSheet.getRange("A2:A").getValues()
    .flat()
    .map(function(e){ return String(e || "").trim(); })
    .filter(Boolean);
  const uniqueEmails = Array.from(new Set(emails));
  if (uniqueEmails.length === 0) return;

  const values = sheet.getDataRange().getValues();
  if (values.length <= 1) return;

  const headers = values[0];
  const hMap = {};
  headers.forEach(function(h, i){ hMap[h] = i; });

  const req = [
    "Network ID", "Report Date", "Advertiser", "Campaign",
    "Placement ID", "Placement", "Impressions", "Clicks", "Issue Type", "Details"
  ];
  if (req.some(function(k){ return hMap[k] === undefined; })) return;

  const MATCH_TEXT = "🟨 PERFORMANCE: CTR ≥ 90% & CPM ≥ $10";
  const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
  const latestMap = loadLatestCacheMap_();

  const candidateRows = [];
  const snapshots = [];

  values.slice(1).forEach(function(r){
    const issueStr = String(r[hMap["Issue Type"]] || "");
    if (!issueStr.includes(MATCH_TEXT)) return;

    const rd = new Date(r[hMap["Report Date"]]);
    if (isNaN(rd) || rd < startOfMonth || rd > today) return;

    const netId = String(r[hMap["Network ID"]] || "");
    const adv   = String(r[hMap["Advertiser"]] || "");
    const camp  = String(r[hMap["Campaign"]] || "");
    const pid   = String(r[hMap["Placement ID"]] || "");
    const plc   = String(r[hMap["Placement"]] || "");
    const imp   = Number(r[hMap["Impressions"]] || 0);
    const clk   = Number(r[hMap["Clicks"]] || 0);
    const det   = String(r[hMap["Details"]] || "");

    const key = pid ? ('pid:' + pid) : ('k:' + netId + '|' + camp + '|' + plc);
    snapshots.push({ key: key, imp: imp, clk: clk });

    const prev = latestMap[key];
    const isNew = !prev;
    const changed = isNew || prev.imp !== imp || prev.clk !== clk;

    if (changed) {
      const trimmedCampaign  = camp.length > 20 ? camp.substring(0, 20) + "…" : camp;
      const trimmedPlacement = plc.length > 20 ? plc.substring(0, 20) + "…" : plc;

      candidateRows.push({
        netId: netId, adv: adv,
        camp: trimmedCampaign,
        pid: pid,
        plc: trimmedPlacement,
        imp: imp, clk: clk, det: det
      });
    }
  });

  appendTodaySnapshots_(snapshots);
  if (!candidateRows.length) { compactPerfAlertCache_(35); return; }

  const htmlRows = candidateRows.map(function(o){
    return (
      '<tr>' +
      '<td>' + o.netId + '</td>' +
      '<td>' + o.adv + '</td>' +
      '<td>' + o.camp + '</td>' +
      '<td>' + o.pid + '</td>' +
      '<td>' + o.plc + '</td>' +
      '<td>' + o.imp + '</td>' +
      '<td>' + o.clk + '</td>' +
      '<td>' + o.det + '</td>' +
      '</tr>'
    );
  }).join("");

  const table = ''
    + '<p><b>ALERT:</b> ' + MATCH_TEXT + '</p>'
    + '<p>This report lists placements that continue to meet the performance-alert criteria. Items drop off once metrics are corrected or fall below the thresholds, but will continue to be listed within the CM360 CPC/CPM FLIGHT QA reports.</p>'
    + '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;font-size:11px;">'
    + '<tr style="background:#f2f2f2;font-weight:bold;">'
    + '<th>Network ID</th><th>Advertiser</th><th>Campaign</th><th>Placement ID</th>'
    + '<th>Placement</th><th>Impressions</th><th>Clicks</th><th>Details</th>'
    + '</tr>'
    + htmlRows
    + '</table>'
    + '<br/>'
    + '<p><i>Brought to you by Platform Solutions Automation. (Made by: BK)</i></p>';

  const todayStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "M/d/yy");
  const subject = 'ALERT – PERFORMANCE (pre-monthly-summary) – ' + todayStr + ' – ' + candidateRows.length + ' changed/new row(s)';

  uniqueEmails.forEach(function(addr){
    try {
      MailApp.sendEmail({ to: addr, subject: subject, htmlBody: table });
      Utilities.sleep(300);
    } catch (err) {
      Logger.log('❌ Failed to email ' + addr + ': ' + err);
    }
  });

  compactPerfAlertCache_(35);
}




// ===== Violation last-change cache (sidecar workbook, retry & batched) =====
function withBackoff_(fn, label, maxTries) {
  label = label || "op";
  maxTries = maxTries || 5;
  let wait = 250;
  for (let i = 1; i <= maxTries; i++) {
    try { return fn(); } catch (e) {
      if (i === maxTries) throw e;
      Utilities.sleep(wait);
      wait = Math.min(wait * 2, 4000);
    }
  }
}

function getVChangeBook_() {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const props = PropertiesService.getScriptProperties();
    const id = props.getProperty('vChangeBookId');
    if (id) return withBackoff_(function(){ return SpreadsheetApp.openById(id); }, "open sidecar");
    const book = withBackoff_(function(){ return SpreadsheetApp.create("_CM360_QA_VChangeCache_" + Date.now()); }, "create sidecar");
    props.setProperty('vChangeBookId', book.getId());
    return book;
  } finally {
    lock.releaseLock();
  }
}

function getVChangeSheet_() {
  const book = getVChangeBook_();
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    let sh = withBackoff_(function(){ return book.getSheetByName("_Violation Change Cache"); }, "get sheet");
    if (!sh) {
      sh = withBackoff_(function(){ return book.insertSheet("_Violation Change Cache"); }, "insert sheet");
      withBackoff_(function(){ sh.hideSheet(); }, "hide sheet");
    }
    const header = ["key","pe","lastReport","lastImp","lastClk","lastImpChange","lastClkChange"];
    const cur = withBackoff_(function(){ return (sh.getRange(1,1,1,header.length).getValues()[0] || []); }, "read header");
    const ok = header.every(function(h,i){ return String(cur[i]||"").toLowerCase()===h.toLowerCase(); });
    if (!ok) withBackoff_(function(){ sh.getRange(1,1,1,header.length).setValues([header]); }, "write header");
    return sh;
  } finally {
    lock.releaseLock();
  }
}

function migrateViolationPropsToSheetOnce_() {
  const propsDoc = PropertiesService.getDocumentProperties();
  const raw = propsDoc.getProperty('violationChangeMap');
  if (!raw) return;
  let obj; try { obj = JSON.parse(raw); } catch(e) { obj = {}; }
  saveViolationChangeMap_(obj);
  propsDoc.deleteProperty('violationChangeMap');
}

function loadViolationChangeMap_() {
  migrateViolationPropsToSheetOnce_();
  const sh = getVChangeSheet_();
  const lastRow = withBackoff_(function(){ return sh.getLastRow(); }, "getLastRow");
  if (lastRow <= 1) return {};
  const vals = withBackoff_(function(){ return sh.getRange(2,1,lastRow-1,7).getValues(); }, "read cache rows");
  const map = {};
  for (let i = 0; i < vals.length; i++) {
    const r = vals[i];
    const key = String(r[0] || "").trim();
    if (!key) continue;
    map[key] = {
      key:            key,
      pe:            r[1] ? String(r[1]) : null,
      lastReport:    r[2] ? String(r[2]) : null,
      lastImp:       Number(r[3] || 0),
      lastClk:       Number(r[4] || 0),
      lastImpChange: r[5] ? String(r[5]) : null,
      lastClkChange: r[6] ? String(r[6]) : null
    };
  }
  return map;
}

function saveViolationChangeMap_(mapObj) {
  const sh = getVChangeSheet_();
  const keys = Object.keys(mapObj).sort();
  const rows = new Array(keys.length);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    const r = mapObj[k] || {};
    rows[i] = [
      k,
      r.pe || null,
      r.lastReport || null,
      Number(r.lastImp || 0),
      Number(r.lastClk || 0),
      r.lastImpChange || null,
      r.lastClkChange || null
    ];
  }

  const COLS = 7;
  const last = withBackoff_(function(){ return sh.getLastRow(); }, "getLastRow before clear");
  if (last > 1) withBackoff_(function(){ sh.getRange(2,1,last-1,COLS).clearContent(); }, "clear body");

  if (!rows.length) {
    PropertiesService.getDocumentProperties().deleteProperty('violationChangeMap');
    return;
  }

  const BATCH = 10000;
  for (let start = 0; start < rows.length; start += BATCH) {
    const chunk = rows.slice(start, start + BATCH);
    withBackoff_(function(){ sh.getRange(2 + start, 1, chunk.length, COLS).setValues(chunk); }, "write batch");
    Utilities.sleep(50);
  }

  PropertiesService.getDocumentProperties().deleteProperty('violationChangeMap');
}

function cleanupViolationCache_(mapObj, today) {
  for (const k in mapObj) {
    if (!mapObj.hasOwnProperty(k)) continue;
    const r = mapObj[k];
    const pe  = r.pe ? new Date(r.pe) : null;
    const lic = r.lastImpChange ? new Date(r.lastImpChange) : null;
    const lcc = r.lastClkChange ? new Date(r.lastClkChange) : null;
    if (pe && today > pe) {
      const impOk = !lic || lic <= pe;
      const clkOk = !lcc || lcc <= pe;
      if (impOk && clkOk) delete mapObj[k];
    }
  }
  const ninetyDaysAgo = new Date(Date.now() - 90 * 86400000);
  for (const k2 in mapObj) {
    if (!mapObj.hasOwnProperty(k2)) continue;
    const r2 = mapObj[k2];
    const lr = r2.lastReport ? new Date(r2.lastReport) : null;
    if (lr && lr < ninetyDaysAgo) delete mapObj[k2];
  }
  const remaining = Object.keys(mapObj).map(function(k3){
    const v = mapObj[k3];
    return [k3, v.lastReport ? new Date(v.lastReport).getTime() : 0];
  }).sort(function(a,b){ return b[1]-a[1]; });

  const MAX = 150000;
  if (remaining.length > MAX) {
    for (let i = MAX; i < remaining.length; i++) delete mapObj[remaining[i][0]];
  }
}

function upsertViolationChange_(mapObj, key, rd, imp, clk, pe) {
  const rdISO = rd ? Utilities.formatDate(rd, Session.getScriptTimeZone(), "yyyy-MM-dd") : null;
  const peISO = pe ? Utilities.formatDate(pe, Session.getScriptTimeZone(), "yyyy-MM-dd") : null;

  let rec = mapObj[key];
  if (!rec) {
    rec = mapObj[key] = {
      key: key,
      pe: peISO,
      lastReport: rdISO,
      lastImp: Number(imp || 0),
      lastClk: Number(clk || 0),
      lastImpChange: rdISO,
      lastClkChange: rdISO
    };
  } else {
    if (peISO && peISO !== rec.pe) rec.pe = peISO;
    if (!rec.lastReport || (rdISO && rdISO > rec.lastReport)) rec.lastReport = rdISO;
    if (typeof imp === "number" && imp !== Number(rec.lastImp || 0)) {
      rec.lastImp = Number(imp);
      rec.lastImpChange = rdISO;
    }
    if (typeof clk === "number" && clk !== Number(rec.lastClk || 0)) {
      rec.lastClk = Number(clk);
      rec.lastClkChange = rdISO;
    }
  }
  return {
    lastImpChange: rec.lastImpChange ? new Date(rec.lastImpChange) : null,
    lastClkChange: rec.lastClkChange ? new Date(rec.lastClkChange) : null
  };
}

// ---------------------
// Owner/Rep mapping helpers + lookup from "Networks" (prefer OPS in P–S)
// ---------------------
function normalizeAdv_(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/\(.*?\)/g, '')
    .replace(/\[.*?\]/g, '')
    .replace(/\b(inc|llc|ltd|corp|corporation|group)\b/g, '')
    .replace(/[^a-z0-9+]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function resolveRep_(ownerMap, netId, adv) {
  const rawKey  = netId + "|||" + String(adv || "").toLowerCase().trim();
  const normKey = netId + "|||" + normalizeAdv_(adv || "");
  const rr = ownerMap.byKey[rawKey];
  const nr = ownerMap.byKey[normKey];
  return (rr && rr.rep) || (nr && nr.rep) || "Unassigned";
}

function loadOwnerMapFromNetworks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("Networks");
  const byKey = {};

  if (!sh || sh.getLastRow() < 2) return { byKey: byKey };

  const vals = sh.getDataRange().getValues();
  const hdr  = vals[0].map(function(h){ return String(h || "").trim().toLowerCase(); });

  const idIdx = (function() {
    const cands = ["network id","network_id","networkid","cm360 network id"];
    for (let i = 0; i < cands.length; i++) { const c = cands[i]; const idx = hdr.indexOf(c); if (idx !== -1) return idx; }
    return -1;
  })();
  const advIdx = (function() {
    const cands = ["advertiser","advertiser name","advertiser_name","cm360 advertiser","cm360 advertiser name"];
    for (let i = 0; i < cands.length; i++) { const c = cands[i]; const idx = hdr.indexOf(c); if (idx !== -1) return idx; }
    return -1;
  })();

  function findOpsInRange_(hdrArr, start, end) {
    for (let i = start; i <= end && i < hdrArr.length; i++) {
      const name = hdrArr[i];
      if (/ops/.test(name)) return i;
    }
    return -1;
  }
  let repIdx = findOpsInRange_(hdr, 15, 18);

  if (repIdx === -1) {
    const repCands = [
      "account rep ops","rep ops","ops owner","ops member","ops",
      "owner (ops)","operations owner","account owner","owner","rep","sales rep","account lead"
    ];
    for (let i = 0; i < repCands.length; i++) {
      const c = repCands[i];
      const j = hdr.indexOf(c);
      if (j !== -1) { repIdx = j; break; }
    }
  }

  if (idIdx === -1 || advIdx === -1 || repIdx === -1) return { byKey: byKey };

  for (let r = 1; r < vals.length; r++) {
    const netId = String(vals[r][idIdx] || "").trim();
    const adv   = String(vals[r][advIdx] || "").trim();
    const theRep = String(vals[r][repIdx] || "").trim();
    if (!netId || !adv) continue;

    const rawKey  = netId + "|||" + adv.toLowerCase();
    const normKey = netId + "|||" + normalizeAdv_(adv);
    const payload = { rep: theRep || "Unassigned" };

    byKey[rawKey]  = payload;
    byKey[normKey] = payload;
  }

  return { byKey: byKey };
}

// Export a single Sheet as XLSX blob (robust via export endpoint)
function createXLSXFromSheet(sheet) {
  if (!sheet) throw new Error("createXLSXFromSheet: sheet is required");

  const tmp = SpreadsheetApp.create("TMP_EXPORT_" + Date.now());
  const tmpId = tmp.getId();
  const tmpSs = SpreadsheetApp.openById(tmpId);

  const copied = sheet.copyTo(tmpSs).setName(sheet.getName());
  tmpSs.getSheets().forEach(function(s){
    if (s.getSheetId() !== copied.getSheetId()) tmpSs.deleteSheet(s);
  });
  tmpSs.setActiveSheet(copied);
  tmpSs.moveActiveSheet(0);

  const url = 'https://docs.google.com/spreadsheets/d/' + tmpId + '/export?format=xlsx';
  const token = ScriptApp.getOAuthToken();
  const response = UrlFetchApp.fetch(url, { headers: { Authorization: 'Bearer ' + token } });

  DriveApp.getFileById(tmpId).setTrashed(true);
  return response.getBlob();
}

function getStaleThresholdDays_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const networksSheet = ss.getSheetByName("Networks");
  if (!networksSheet) return 7;

  const raw = String(networksSheet.getRange("H1").getDisplayValue() || "").trim();
  const m = raw.match(/-?\d+(\.\d+)?/);
  let v = m ? Number(m[0]) : NaN;

  if (!isFinite(v) || v <= 0) v = 7;
  v = Math.floor(v);
  Logger.log("Stale threshold days used (from Networks!H1): " + v + " (raw='" + raw + "')");
  return v;
}


/*******************************************************
 * Low-Priority Scoring — Lightweight (NO sheets/logging)
 *******************************************************/

// Keep these defaults (same signal quality, no sheet I/O)
const X_CH = "[x×✕]";
const DEFAULT_LP_PATTERNS = [
  ['Impression Pixel/Beacon', `\\b0\\s*${X_CH}\\s*0\\b|\\bzero\\s*by\\s*zero\\b`, 40, 'Zero-size creative', 'Y'],
  ['Impression Pixel/Beacon', `\\b1\\s*${X_CH}\\s*1\\b|\\b1\\s*by\\s*1\\b|\\b1x1(?:cc)?\\b`, 30, '1x1 variants', 'Y'],
  ['Impression Pixel/Beacon', `\\bpixel(?:\\s*only)?\\b|\\bbeacon\\b|\\bclear\\s*pixel\\b|\\btransparent\\s*pixel\\b|\\bspacer\\b|\\bshim\\b`, 20, 'Pixel-ish words', 'Y'],

  ['Click Tracker', `\\bclick\\s*tr(?:ac)?k(?:er)?\\b`, 28, 'click tracker', 'Y'],
  ['Click Tracker', `\\bclick[_-]?(?:trk|tr)\\b|\\bclk[_-]?trk\\b|\\bclktrk\\b|\\bctrk\\b`, 26, 'click/clk tracker shorthands', 'Y'],
  ['Click Tracker', `(^|[^A-Za-z0-9])ct(?:_?trk)\\b`, 22, 'bounded CT_TRK', 'Y'],
  ['Click Tracker', `tracking\\s*1\\s*${X_CH}\\s*1|track(?:ing)?\\s*1x1`, 20, 'tracking 1x1', 'Y'],
  ['Click Tracker', `dfa\\s*zero\\s*placement|zero\\s*placement`, 18, 'legacy DFA zero placement', 'Y'],

  ['VAST/CTV Tracking Tag', `\\bvid(?:eo)?[\\s_\\-]*tag\\b`, 25, 'VID_TAG / video tag', 'Y'],
  ['VAST/CTV Tracking Tag', `\\bvid[\\s_\\-]*:(?:06|15|30)s?\\b`, 22, 'VID:06/15/30 shorthand', 'Y'],
  ['VAST/CTV Tracking Tag', `\\bvast[\\s_\\-]*(?:tag|pixel|tracker)\\b`, 30, 'VAST tag/pixel/tracker', 'Y'],
  ['VAST/CTV Tracking Tag', `\\bdv[_\\-]?tag\\b|\\bgcm[_\\-]?(?:non[_\\-]?)?tag\\b|\\bgcm[_\\-]?dv[_\\-]?tag\\b`, 30, 'DV_TAG/GCM tags', 'Y'],
  ['VAST/CTV Tracking Tag', `\\bvpaid\\b|\\bomsdk\\b|\\bavoc\\b`, 18, 'VPAID/OMSDK/AVOC', 'Y'],

  ['Viewability/Verification', `\\bom(id)?\\b|\\bmoat\\b|\\bias\\b|\\bintegral\\s*ad\\s*science\\b|\\bdoubleverify\\b|\\bcomscore\\b|\\bpixalate\\b|\\bverification\\b|\\bviewability\\b`, 18, 'Verification vendors/terms', 'Y'],

  ['Placeholder/Tag-Only/Test', `\\b[_-]?tag\\b|\\bnon[_-]?tag\\b|\\bplaceholder\\b|\\bdefault\\s*tag\\b|\\bqa\\b|\\btest\\b|\\bsample\\b`, 15, 'Non-serving / test-ish', 'Y'],

  ['Impression-Only Keywords', `\\bimp(?:ression)?[\\s_\\-]*only\\b|\\bimpr[\\s_\\-]*only\\b|\\bview[\\s_\\-]*through\\b`, 20, 'Impr-only phrasing', 'Y'],

  ['Social/3P Pixel', `\\b(meta|facebook|tiktok|snap|pinterest|youtube)[\\s_\\-]*(pixel|tag)\\b`, 15, 'Social pixel/tag', 'Y'],
  ['Social/3P Pixel', `\\bfbq\\b|\\bttq\\b|\\bsnaptr\\b|\\bpintrk\\b|\\btwq\\b|\\bgads\\b`, 15, 'SDK shorthands', 'Y'],

  ['Descriptor Only', `\\b(?:added\\s*value|sponsorship)\\b`, 5, 'Descriptor-only if CPM-only', 'Y'],
  ['Signal', `\\bN\\/A\\b`, 10, 'N/A token in piped name', 'Y']
];

// Negatives used only to *reduce* likelihood when both metrics are present
const DEFAULT_NEG_PATTERNS = [
  ['DisplaySize', `\\b(120\\s*${X_CH}\\s*600|160\\s*${X_CH}\\s*600|300\\s*${X_CH}\\s*50|300\\s*${X_CH}\\s*100|300\\s*${X_CH}\\s*250|300\\s*${X_CH}\\s*600|320\\s*${X_CH}\\s*50|320\\s*${X_CH}\\s*100|336\\s*${X_CH}\\s*280|468\\s*${X_CH}\\s*60|728\\s*${X_CH}\\s*90|970\\s*${X_CH}\\s*90|970\\s*${X_CH}\\s*250|980\\s*${X_CH}\\s*120|980\\s*${X_CH}\\s*240|640\\s*${X_CH}\\s*360|1280\\s*${X_CH}\\s*720|1920\\s*${X_CH}\\s*1080)\\b`, 35, 'Standard creative sizes', 'Y'],
  ['AssetExt', `\\b(?:jpg|jpeg|png|gif|mp4|mov|webm)\\b`, 10, 'Creative file type mentioned', 'Y'],
  ['RealCreativeKeywords', `\\b(?:interstitial|masthead|takeover|homepage|roadblock)\\b`, 15, 'Likely real creatives', 'Y']
];

// Probability tuning (same math, no logging)
const LP_THRESHOLDS = { VERY_LIKELY: 85, LIKELY: 70, POSSIBLE: 55 };
const LP_BASE_SCORE = 40;

let _lpCompiled = null;
let _negCompiled = null;

function compileLPPatternsIfNeeded_() {
  if (_lpCompiled && _negCompiled) return;

  _lpCompiled = DEFAULT_LP_PATTERNS.map(function(r){
    let re = null; try { re = new RegExp(String(r[1]), 'i'); } catch (e) { /* noop */ }
    return {
      category: String(r[0]),
      re: re,
      weight: Number(r[2] || 0),
      label: String(r[0]) + ':' + String(r[1]),
      enabled: String(r[4] || 'Y').toUpperCase().startsWith('Y') && !!re
    };
  });

  _negCompiled = DEFAULT_NEG_PATTERNS.map(function(r){
    let re = null; try { re = new RegExp(String(r[1]), 'i'); } catch (e) { /* noop */ }
    return {
      category: r[0],
      re: re,
      weight: Number(r[2] || 0),
      label: String(r[0]) + ':' + String(r[1]),
      enabled: !!re
    };
  });
}

function normalizeName_(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[×✕]/g, 'x')
    .replace(/\|/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function clamp_(n, a, b) { return Math.max(a, Math.min(b, n)); }

/**
 * Lightweight classifier:
 * - NO sheet reads/writes
 * - Returns descriptor string or '' (no tag)
 * - gating: 'CPM-only' | 'CPC-only' | 'Mixed'
 */
function scoreAndLabelLowPriority_(placementName, clicks, impr, rowIdOrIndex, gating) {
  gating = gating || ((impr > 0 && clicks === 0) ? 'CPM-only' :
                      (impr === 0 && clicks > 0) ? 'CPC-only' : 'Mixed');

  compileLPPatternsIfNeeded_();

  if (gating === 'Mixed') {
    // Don’t LP-tag rows where both metrics present (or pathological both+clicks>impr)
    return '';
  }

  const s = normalizeName_(placementName);
  let pos = 0, neg = 0;
  const catScores = Object.create(null);

  for (var i=0; i<_lpCompiled.length; i++) {
    var p = _lpCompiled[i];
    if (!p.enabled || !p.re) continue;
    if (p.re.test(s)) {
      pos += p.weight;
      catScores[p.category] = (catScores[p.category] || 0) + p.weight;
    }
  }

  // If Mixed, we’d subtract negatives; for single-metric add a tiny boost when size present
  if (gating !== 'Mixed') {
    var sizeRgx = _negCompiled[0].re;
    if (sizeRgx && sizeRgx.test(s)) {
      pos += 15; // helps 1x1 & obvious “pixel-ish” names
      catScores['Impression Pixel/Beacon'] = (catScores['Impression Pixel/Beacon'] || 0) + 15;
    }
  } else {
    for (var j=0; j<_negCompiled.length; j++) {
      var n = _negCompiled[j];
      if (n.enabled && n.re && n.re.test(s)) neg += n.weight;
    }
  }

  var has0x0  = /\b0\s*x\s*0\b|\bzero\s*by\s*zero\b/.test(s);
  var hasTag  = /\bvid(?:eo)?[\s_\-]*tag\b/.test(s) || /\b(?:gcm|dv)[\s_\-]*(?:non[\s_\-]*)?tag\b|\bdv[_\-]?tag\b/.test(s);
  var hasDur  = /\bvid[\s_\-]*:(?:06|15|30)s?\b/.test(s);
  if (has0x0 && (hasTag || hasDur)) {
    pos += 20;
    catScores['VAST/CTV Tracking Tag'] = (catScores['VAST/CTV Tracking Tag'] || 0) + 20;
  }

  if (gating === 'CPC-only' && (catScores['Click Tracker'] || 0) > 0) {
    pos += 10;
  }
  if (gating === 'CPM-only' && (catScores['Impression Pixel/Beacon'] || 0) > 0) {
    pos += 10;
  }

  var probability = clamp_(LP_BASE_SCORE + pos - neg, 0, 100);
  var band = (probability >= LP_THRESHOLDS.VERY_LIKELY) ? 'Very likely'
          : (probability >= LP_THRESHOLDS.LIKELY)      ? 'Likely'
          : (probability >= LP_THRESHOLDS.POSSIBLE)    ? 'Possible'
          : 'Unlikely';

  if (band === 'Unlikely') return '';

  var topCat = '';
  var maxCatScore = -1;
  for (var cat in catScores) {
    if (catScores[cat] > maxCatScore) { maxCatScore = catScores[cat]; topCat = cat; }
  }
  if (!topCat) topCat = 'Impression Pixel/Beacon';

  // Descriptor only; no writes/logging
  return 'Low Priority — ' + topCat + ' (' + band + ')';
}




// ---------------------
// runQAOnly (auto-resume, chunked, lock-guarded)
// ---------------------
function runQAOnly() {
  // Prevent overlapping runs
  const dlock = LockService.getDocumentLock();
  if (!dlock.tryLock(5000)) { scheduleNextQAChunk_(2); return; }

  // Clear any stale scheduled id right as we start a chunk
  cancelQAChunkTrigger_();

  try {
    const ss  = SpreadsheetApp.getActiveSpreadsheet();
    const raw = ss.getSheetByName("Raw Data");
    const out = ss.getSheetByName("Violations");
    if (!raw || !out) return;

    const data = raw.getDataRange().getValues();
    if (!data || data.length <= 1) return;

    const headers = data[0];
    const m = getHeaderMap(headers);

    const ignoreSet = loadIgnoreAdvertisers();
    const ownerMap  = loadOwnerMapFromNetworks_();
    const vMap      = loadViolationChangeMap_();



    compileLPPatternsIfNeeded_();

    let state = getQAState_();
    const totalRows = data.length - 1; // excluding header
    const freshStart = !state || state.totalRows !== totalRows;

    if (freshStart) {
      clearViolations();
      state = { session: String(Date.now()), next: 2, totalRows: totalRows };
      saveQAState_(state);
      cancelQAChunkTrigger_();
    }

    const startTime = Date.now();
    const today = new Date();
    const firstOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

    // —— Tweak these constants in your file (outside this function) ——
    // const QA_CHUNK_ROWS = 3500;
    // const QA_TIME_BUDGET_MS = 4.2 * 60 * 1000;

    let processed = 0;
    const resultsChunk = [];

    for (let r = state.next; r < data.length; r++) {
      const row = data[r];
      const adv  = row[m["Advertiser"]] && String(row[m["Advertiser"]]).trim();
      const camp = row[m["Campaign"]]   || "";

      const advLower = adv ? adv.toLowerCase() : "";
      if (advLower && (ignoreSet.has(advLower) || advLower.includes("bidmanager"))) { state.next = r + 1; continue; }
      if (camp && String(camp).includes("DART Search"))                               { state.next = r + 1; continue; }
      if (adv === "Grand Total:")                                                     { state.next = r + 1; continue; }

      const imp = Number(row[m["Impressions"]] || 0);
      const clk = Number(row[m["Clicks"]] || 0);
      if (imp === 0 && clk === 0) { state.next = r + 1; continue; }

      const ctr = imp > 0 ? (clk / imp) * 100 : 0;

      // Your CPC/CPM formulas
      const cpc = clk * 0.008;
      const cpm = (imp / 1000) * 0.034;

      const ps  = new Date(row[m["Placement Start Date"]]);
      const pe  = new Date(row[m["Placement End Date"]]);
      const rd  = new Date(row[m["Report Date"]]);

      const daysRem  = Math.ceil((pe - rd) / 86400000);
      const eom      = new Date(rd.getFullYear(), rd.getMonth() + 1, 0);
      const daysLeft = Math.ceil((eom - rd) / 86400000);

      const flen = (pe - ps) / 86400000;
      const din  = (rd - ps) / 86400000;
      const pctComplete = pe.getTime() === ps.getTime()
        ? (rd > pe ? 100 : 0)
        : Math.min(100, Math.max(0, (din / flen) * 100));

      const issueTypes = [];
      const details    = [];
      let risk = "";

      // 🟥 BILLING
      if (pe < firstOfMonth && clk > imp) {
        issueTypes.push("🟥 BILLING: Expired CPC Risk");
        details.push("Ended " + pe.toDateString() + " with clicks (" + clk + ") > impressions (" + imp + ")");
        risk = "🚨 Expired Risk";
      } else if (pe < rd && clk > imp) {
        issueTypes.push("🟥 BILLING: Recently Expired CPC Risk");
        details.push("Ended " + pe.toDateString() + " and still has clicks > impressions");
        risk = "⚠️ Expired This Month";
      } else if (rd <= pe && clk > imp && cpc > 10) {
        issueTypes.push("🟥 BILLING: Active CPC Billing Risk");
        details.push("Active: clicks (" + clk + ") > impressions (" + imp + "), $CPC = $" + cpc.toFixed(2));
        risk = "⚠️ Active CPC Risk";
      }

      // 🟦 DELIVERY
      if (pe < firstOfMonth && rd >= firstOfMonth && (imp > 0 || clk > 0)) {
        issueTypes.push("🟦 DELIVERY: Post-Flight Activity");
        details.push("Ended " + pe.toDateString() + " but has " + imp + " impressions and " + clk + " clicks");
      }

      // 🟨 PERFORMANCE
      if (ctr >= 90 && cpm >= 10) {
        issueTypes.push("🟨 PERFORMANCE: CTR ≥ 90% & CPM ≥ $10");
        details.push("CTR = " + ctr.toFixed(2) + "%, $CPM = $" + cpm.toFixed(2));
      }

      // 🟩 COST
      let isCPMOnly = false;
      let isCPCOnly = false;
      if (cpc > 0 && cpm === 0 && cpc > 10) {
        issueTypes.push("🟩 COST: CPC Only > $10");
        details.push("No CPM spend, $CPC = $" + cpc.toFixed(2));
        if (imp === 0 && clk > 0) isCPCOnly = true;
      }
      if (cpm > 0 && cpc === 0 && cpm > 10) {
        issueTypes.push("🟩 COST: CPM Only > $10");
        details.push("No CPC spend, $CPM = $" + cpm.toFixed(2));
        if (imp > 0 && clk === 0) isCPMOnly = true;
      }
      if (cpc > 0 && cpm > 0 && clk > imp && cpc > 10) {
        issueTypes.push("🟩 COST: CPC+CPM Clicks > Impr & CPC > $10");
        details.push("Clicks > impressions with both CPC and CPM charges (CPC = $" + cpc.toFixed(2) + ")");
      }

      // --- Low-priority tagging via scorer (gating-aware) — no sheet writes ---
      const bothMetricsPresent = imp > 0 && clk > 0;
      const clicksExceedImprWithBoth = bothMetricsPresent && (clk > imp);
      const gating = (imp > 0 && clk === 0) ? 'CPM-only' :
                     (imp === 0 && clk > 0) ? 'CPC-only' : 'Mixed';

      if (!bothMetricsPresent && !clicksExceedImprWithBoth) {
        const placement = row[m["Placement"]];
        const rowIdOrIndex = String(row[m["Placement ID"]] || (r + 1));
        const lpDescriptor = scoreAndLabelLowPriority_(placement, clk, imp, rowIdOrIndex, gating);
        if (lpDescriptor) {
          issueTypes.push("🟩 COST: (Low Priority) " + lpDescriptor.replace(/^Low Priority —\s*/, ""));
        }
      }
      // --- end Low-priority tagging ---

      if (!issueTypes.length) { state.next = r + 1; continue; }

      const pid = String(row[m["Placement ID"]] || "");
      const key = pid ? ("pid:" + pid) : ("k:" + row[m["Network ID"]] + "|" + camp + "|" + row[m["Placement"]]);
      const changes = upsertViolationChange_(vMap, key, rd, imp, clk, pe);

      function daysSince_(lastChangeDate, reportDate) {
        if (!(lastChangeDate instanceof Date) || isNaN(lastChangeDate) || !(reportDate instanceof Date) || isNaN(reportDate)) return "";
        const ms = reportDate.getTime() - lastChangeDate.getTime();
        if (ms < 0) return "";
        return Math.floor(ms / 86400000);
      }
      const lastImpDays = changes.lastImpChange ? daysSince_(changes.lastImpChange, rd) : "";
      const lastClkDays = changes.lastClkChange ? daysSince_(changes.lastClkChange, rd) : "";

      const ownerOps = resolveRep_(ownerMap, String(row[m["Network ID"]] || ""), adv) || "Unassigned";

      resultsChunk.push([
        row[m["Network ID"]], row[m["Report Date"]], row[m["Advertiser"]], row[m["Campaign"]],
        row[m["Campaign Start Date"]], row[m["Campaign End Date"]], row[m["Ad"]], row[m["Placement ID"]],
        row[m["Placement"]], row[m["Placement Start Date"]], row[m["Placement End Date"]],
        imp, clk, ctr.toFixed(2) + "%", daysRem, pctComplete.toFixed(1) + "%", daysLeft,
        risk, "$" + cpc.toFixed(2), "$" + cpm.toFixed(2), issueTypes.join(", "), details.join(" | "),
        lastImpDays, lastClkDays, ownerOps
      ]);

      processed++;
      state.next = r + 1;

      // Respect chunk size & time budget
      if (processed >= QA_CHUNK_ROWS) break;
      if ((Date.now() - startTime) >= QA_TIME_BUDGET_MS) break;
    }

    // Persist violation-change snapshot
    cleanupViolationCache_(vMap, today);
    saveViolationChangeMap_(vMap);

    // Write this chunk's rows
    if (resultsChunk.length) {
      const width = resultsChunk[0].length;
      const startWriteRow = out.getLastRow() + 1;
      out.getRange(startWriteRow, 1, resultsChunk.length, width).setValues(resultsChunk);
    }

    // Decide: finished or schedule next chunk
    if (state.next >= (data.length)) {
      clearQAState_();
      cancelQAChunkTrigger_();
      Logger.log("✅ runQAOnly complete. Processed all " + totalRows + " data rows.");
    } else {
      saveQAState_(state);
      Logger.log("⏳ runQAOnly partial: processed " + processed + " rows this run. Next row index: "
        + state.next + " / " + (data.length - 1));
      scheduleNextQAChunk_(2); // resume soon
    }
  } finally {
    dlock.releaseLock();
  }
}




// === Helpers for "Immediate Attention" selection ===
function _parseMoney_(s) { // "$12.34" -> 12.34
  var n = String(s || "").replace(/[^\d.-]/g, "");
  var v = parseFloat(n);
  return isFinite(v) ? v : 0;
}
function _parsePct_(s) { // "95.00%" -> 95
  var n = String(s || "").replace(/[^\d.-]/g, "");
  var v = parseFloat(n);
  return isFinite(v) ? v : 0;
}






// ---------------------
// sendEmailSummary (size-safe, chunked execution) — UPDATED with extra buckets
// ---------------------
function sendEmailSummary() {
  sendEmailSummaryChunked_(true); // true = allow chunking
}

function sendEmailSummaryChunked_(allowChunking) {
  const startTime = Date.now();
  const isAuto = !isManualRun_();
  
  // Prevent overlapping runs
  const dlock = LockService.getDocumentLock();
  if (!dlock.tryLock(5000)) {
    if (allowChunking) scheduleNextEmailChunk_(2);
    return;
  }
  
  try {
    // Skip if QA is still running in chunks
    const _qaState = getQAState_();
    if (_qaState && _qaState.session) {
      Logger.log("sendEmailSummary skipped: QA still in progress (chunked).");
      if (allowChunking) scheduleNextEmailChunk_(5); // Check again in 5 min
      return;
    }
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const today = new Date();

    // Only send on/after the 15th
    if (today.getDate() < 15) {
      Logger.log("Email summary skipped: before the 15th of the month.");
      clearEmailState_();
      cancelEmailChunkTrigger_();
      return;
    }

    // Get or create state
    let state = getEmailState_();
    const freshStart = !state || !state.session;
    
    if (freshStart) {
      state = {
        session: String(Date.now()),
        stage: 'network_summary',
        cachedHtml: {},
        processedOwners: [],
        allOwners: []
      };
      saveEmailState_(state);
      cancelEmailChunkTrigger_();
    }

    const sheet = ss.getSheetByName("Violations");
    const rawSheet = ss.getSheetByName("Raw Data");
    const networksSheet = ss.getSheetByName("Networks");
    const recipientsSheet = ss.getSheetByName("EMAIL LIST");
    
    if (!sheet || !rawSheet || !recipientsSheet) {
      const error = new Error('Required sheets missing');
      if (isAuto) sendFailureEmail_('sendEmailSummary', error, { stage: state.stage });
      throw error;
    }

    const violations = sheet.getDataRange().getValues();
    const rawData = rawSheet.getDataRange().getValues();
    
    if (violations.length <= 1) {
      Logger.log("No violations to report");
      clearEmailState_();
      cancelEmailChunkTrigger_();
      return;
    }

    // === STAGE 1: Network Summary ===
    if (state.stage === 'network_summary') {
      Logger.log('📧 Email Stage 1/4: Building network summary...');
      
      state.cachedHtml.networkSummary = buildNetworkSummaryHtml_(violations, rawData, networksSheet);
      state.stage = 'grouped_summary';
      saveEmailState_(state);
      
      if (allowChunking && (Date.now() - startTime) > EMAIL_TIME_BUDGET_MS) {
        Logger.log('⏳ Email stage 1 complete, scheduling next chunk');
        scheduleNextEmailChunk_(2);
        return;
      }
    }

    // === STAGE 2: Grouped Summary ===
    if (state.stage === 'grouped_summary') {
      Logger.log('📧 Email Stage 2/4: Building grouped summary...');
      
      state.cachedHtml.groupedSummary = buildGroupedSummaryHtml_(violations);
      state.cachedHtml.staleHtml = buildStaleHtml_(violations);
      state.stage = 'immediate_attention';
      saveEmailState_(state);
      
      if (allowChunking && (Date.now() - startTime) > EMAIL_TIME_BUDGET_MS) {
        Logger.log('⏳ Email stage 2 complete, scheduling next chunk');
        scheduleNextEmailChunk_(2);
        return;
      }
    }

    // === STAGE 3: Immediate Attention (chunked by owner) ===
    if (state.stage === 'immediate_attention') {
      if (!state.cachedHtml.immediateAttention) state.cachedHtml.immediateAttention = '';
      
      // Build owner list if first time
      if (state.allOwners.length === 0) {
        const ownerData = buildImmediateAttentionData_(violations);
        state.allOwners = ownerData.owners;
        state.ownerMap = ownerData.perOwner;
        state.processedOwners = [];
      }
      
      // Process owners in chunks
      const remainingOwners = state.allOwners.filter(function(o){ return state.processedOwners.indexOf(o) === -1; });
      
      if (remainingOwners.length > 0) {
        const chunkSize = allowChunking ? MAX_OWNERS_PER_CHUNK : remainingOwners.length;
        const ownersThisChunk = remainingOwners.slice(0, chunkSize);
        
        Logger.log('📧 Email Stage 3/4: Processing ' + ownersThisChunk.length + ' owners (' + remainingOwners.length + ' remaining)...');
        
        const htmlChunk = buildImmediateAttentionHtmlForOwners_(ownersThisChunk, state.ownerMap);
        state.cachedHtml.immediateAttention += htmlChunk;
        state.processedOwners = state.processedOwners.concat(ownersThisChunk);
        saveEmailState_(state);
        
        if (allowChunking && remainingOwners.length > chunkSize && (Date.now() - startTime) > EMAIL_TIME_BUDGET_MS) {
          Logger.log('⏳ Email stage 3 partial, scheduling next chunk');
          scheduleNextEmailChunk_(2);
          return;
        }
      }
      
      // Wrap up immediate attention section
      if (state.cachedHtml.immediateAttention) {
        state.cachedHtml.immediateAttention = '<p><b>Immediate Attention — Key Issues (by Owner)</b></p>' + state.cachedHtml.immediateAttention;
      }
      
      state.stage = 'create_xlsx';
      saveEmailState_(state);
      
      if (allowChunking && (Date.now() - startTime) > EMAIL_TIME_BUDGET_MS) {
        Logger.log('⏳ Email stage 3 complete, scheduling next chunk');
        scheduleNextEmailChunk_(2);
        return;
      }
    }

    // === STAGE 4: Create XLSX ===
    if (state.stage === 'create_xlsx') {
      Logger.log('📧 Email Stage 4/4: Creating XLSX attachment...');
      
      const todayformatted = Utilities.formatDate(today, Session.getScriptTimeZone(), "M.d.yy");
      const fileName = "CM360_QA_Violations_" + todayformatted + ".xlsx";
      
      try {
        const xlsxBlob = createXLSXFromSheet(sheet).setName(fileName);
        
        // Store in Drive temporarily
        const tempFile = DriveApp.createFile(xlsxBlob);
        state.xlsxFileId = tempFile.getId();
        state.xlsxFileName = fileName;
      } catch (e) {
        Logger.log('❌ XLSX creation failed: ' + e.message);
        if (isAuto) sendFailureEmail_('sendEmailSummary', e, { stage: 'create_xlsx', rawDataRows: rawData.length - 1, violations: violations.length - 1 });
        throw e;
      }
      
      state.stage = 'send';
      saveEmailState_(state);
      
      if (allowChunking && (Date.now() - startTime) > EMAIL_TIME_BUDGET_MS) {
        Logger.log('⏳ Email stage 4 complete, scheduling next chunk');
        scheduleNextEmailChunk_(2);
        return;
      }
    }

    // === STAGE 5: Send Email ===
    if (state.stage === 'send') {
      Logger.log('📧 Email Stage 5/5: Assembling and sending email...');
      
      // Get recipients
      const emails = recipientsSheet.getRange("A2:A").getValues()
        .flat()
        .map(function(e){ return String(e || "").trim(); })
        .filter(Boolean);
      const uniqueEmails = Array.from(new Set(emails));
      
      if (uniqueEmails.length === 0) {
        Logger.log('⚠️ No recipients found');
        clearEmailState_();
        cancelEmailChunkTrigger_();
        return;
      }

      // Generate mid-flight drop HTML
      const midFlightHtml = generateMidFlightDropHtml_();

      // Assemble email
      const subject = "CM360 CPC/CPM FLIGHT QA – " + Utilities.formatDate(today, Session.getScriptTimeZone(), "M/d/yy");
      let htmlBody = state.cachedHtml.networkSummary +
                     '<p>The below is a table of the following Billing, Delivery, Performance and Cost issues:</p>' +
                     state.cachedHtml.groupedSummary +
                     (state.cachedHtml.immediateAttention ? ('<br/>' + state.cachedHtml.immediateAttention) : '') +
                     (midFlightHtml ? ('<br/>' + midFlightHtml) : '') +
                     '<br/>' + state.cachedHtml.staleHtml +
                     '<p><i>Brought to you by the Platform Solutions Automation. (Made by: BK)</i></p>';

      // Safety trim
      const MAX_HTML_CHARS = 90000;
      if (htmlBody.length > MAX_HTML_CHARS) {
        htmlBody = htmlBody.slice(0, MAX_HTML_CHARS - 1200) +
                  '<p><i>(trimmed for size — full detail in the attached XLSX)</i></p>';
      }

      // Get XLSX from Drive
      const xlsxFile = DriveApp.getFileById(state.xlsxFileId);
      const xlsxBlob = xlsxFile.getBlob().setName(state.xlsxFileName);

      // Send emails
      let failedRecipients = [];
      uniqueEmails.forEach(function(addr){
        try {
          MailApp.sendEmail({ to: addr, subject: subject, htmlBody: htmlBody, attachments: [xlsxBlob] });
          Utilities.sleep(300);
        } catch (err) {
          Logger.log("❌ Failed to email " + addr + ": " + err);
          failedRecipients.push(addr);
        }
      });

      // Cleanup
      try {
        xlsxFile.setTrashed(true);
      } catch (e) {
        Logger.log('⚠️ Could not delete temp XLSX: ' + e.message);
      }

      clearEmailState_();
      cancelEmailChunkTrigger_();

      const duration = Date.now() - startTime;
      Logger.log('✅ Email sent to ' + (uniqueEmails.length - failedRecipients.length) + '/' + uniqueEmails.length + ' recipients in ' + fmtMs_(duration));
      
      logAuditEntry_('sendEmailSummary', 'SUCCESS', duration, rawData.length - 1, violations.length - 1, null);

      if (failedRecipients.length > 0 && isAuto) {
        sendFailureEmail_('sendEmailSummary', new Error('Failed to send to: ' + failedRecipients.join(', ')), {
          stage: 'send',
          duration: fmtMs_(duration),
          rawDataRows: rawData.length - 1,
          violations: violations.length - 1
        });
      }
    }

  } catch (e) {
    Logger.log('❌ sendEmailSummary error: ' + e.message);
    if (isAuto) {
      const rawCount = rawSheet ? rawSheet.getLastRow() - 1 : 0;
      const violCount = sheet ? sheet.getLastRow() - 1 : 0;
      sendFailureEmail_('sendEmailSummary', e, {
        stage: state ? state.stage : 'unknown',
        duration: fmtMs_(Date.now() - startTime),
        rawDataRows: rawCount,
        violations: violCount
      });
    }
    throw e;
  } finally {
    dlock.releaseLock();
  }
}

// Helper functions for chunked email generation
function buildNetworkSummaryHtml_(violations, rawData, networksSheet) {
  const hMap = getHeaderMap(violations[0]);
  const rMap = getHeaderMap(rawData[0]);

  function buildNetworkNameMap_() {
    if (!networksSheet) return {};
    const vals = networksSheet.getDataRange().getValues();
    const map = {};
    for (let r = 1; r < vals.length; r++) {
      const idRaw = vals[r][0];
      const name  = String(vals[r][1] == null ? "" : vals[r][1]).replace(/\u00A0/g, " ").trim();
      if (!idRaw) continue;
      let id = "";
      if (typeof idRaw === "number") id = String(Math.trunc(idRaw));
      else {
        let s = String(idRaw).replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
        s = s.replace(/,/g, "");
        const digits = s.replace(/\D+/g, "");
        id = digits || s;
      }
      if (id) map[id] = name;
    }
    return map;
  }
  const networkNameMap = buildNetworkNameMap_();

  const placementCounts = {};
  rawData.slice(1).forEach(function(r){
    const id = String(r[rMap["Network ID"]] || "");
    if (id) placementCounts[id] = (placementCounts[id] || 0) + 1;
  });

  const violationCounts = {};
  violations.slice(1).forEach(function(r){
    const id = String(r[hMap["Network ID"]] || "");
    const types = String(r[hMap["Issue Type"]] || "").split(", ");
    if (!violationCounts[id]) {
      violationCounts[id] = { "🟥 BILLING": 0, "🟦 DELIVERY": 0, "🟨 PERFORMANCE": 0, "🟩 COST": 0 };
    }
    types.forEach(function(t){
      if (t.startsWith("🟥")) violationCounts[id]["🟥 BILLING"]++;
      if (t.startsWith("🟦")) violationCounts[id]["🟦 DELIVERY"]++;
      if (t.startsWith("🟨")) violationCounts[id]["🟨 PERFORMANCE"]++;
      if (t.startsWith("🟩")) violationCounts[id]["🟩 COST"]++;
    });
  });

  let html = '<p><b>Network-Level QA Summary</b></p>'
    + '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse: collapse; font-size: 11px;">'
    + '<tr style="background-color: #f2f2f2; font-weight: bold;">'
    + '<th>Network ID</th><th>Network Name</th><th>Placements Checked</th>'
    + '<th>🟥 BILLING</th><th>🟦 DELIVERY</th><th>🟨 PERFORMANCE</th><th>🟩 COST</th>'
    + '</tr>';

  Object.entries(networkNameMap)
    .filter(function(pair){
      const id = pair[0];
      const vc = violationCounts[id] || { "🟥 BILLING":0,"🟦 DELIVERY":0,"🟨 PERFORMANCE":0,"🟩 COST":0 };
      const total = vc["🟥 BILLING"] + vc["🟦 DELIVERY"] + vc["🟨 PERFORMANCE"] + vc["🟩 COST"];
      return total > 0;
    })
    .sort(function(a, b){ return a[1].localeCompare(b[1]); })
    .forEach(function(entry){
      const id = entry[0], name = entry[1];
      const pc = placementCounts[id] || 0;
      const vc = violationCounts[id] || { "🟥 BILLING":0,"🟦 DELIVERY":0,"🟨 PERFORMANCE":0,"🟩 COST":0 };
      html += '<tr>'
        + '<td>' + id + '</td><td>' + name + '</td><td>' + pc + '</td>'
        + '<td>' + vc["🟥 BILLING"] + '</td><td>' + vc["🟦 DELIVERY"] + '</td><td>' + vc["🟨 PERFORMANCE"] + '</td><td>' + vc["🟩 COST"] + '</td>'
        + '</tr>';
    });
  html += '</table><br/>';
  
  return html;
}

function buildGroupedSummaryHtml_(violations) {
  const hMap = getHeaderMap(violations[0]);
  const groupedCounts = { "🟥 BILLING": {}, "🟦 DELIVERY": {}, "🟨 PERFORMANCE": {}, "🟩 COST": {} };
  
  violations.slice(1).forEach(function(r){
    const types = String(r[hMap["Issue Type"]] || "").split(", ");
    types.forEach(function(t){
      const match = t.match(/^(🟥|🟦|🟨|🟩)\s(\w+):\s(.+)/);
      if (match) {
        const emoji = match[1], group = match[2], subtype = match[3];
        const key = emoji + " " + group;
        groupedCounts[key] = groupedCounts[key] || {};
        groupedCounts[key][subtype] = (groupedCounts[key][subtype] || 0) + 1;
      }
    });
  });
  
  let html = "";
  Object.entries(groupedCounts).forEach(function(entry){
    const groupLabel = entry[0], subtypes = entry[1];
    html += "<b>" + groupLabel + "</b><ul>";
    Object.entries(subtypes).forEach(function(st){
      const subtype = st[0], count = st[1];
      if (count > 0) html += "<li>" + subtype + ": " + count + "</li>";
    });
    html += "</ul>";
  });
  
  return html;
}

function buildStaleHtml_(violations) {
  const thresholdDays = getStaleThresholdDays_();
  let staleImp = 0, staleClk = 0;
  const hMap = getHeaderMap(violations[0]);
  const impIdx = hMap["Last Imp Change"], clkIdx = hMap["Last Click Change"];
  
  if (impIdx !== undefined || clkIdx !== undefined) {
    for (let i = 1; i < violations.length; i++) {
      const r = violations[i];
      const impDays = impIdx !== undefined ? Number(r[impIdx]) : NaN;
      const clkDays = clkIdx !== undefined ? Number(r[clkIdx]) : NaN;
      if (isFinite(impDays) && impDays >= thresholdDays) staleImp++;
      if (isFinite(clkDays) && clkDays >= thresholdDays) staleClk++;
    }
  }
  
  return "<b>Stale Metrics (this month)</b><ul>"
    + "<li>Placements with no new impressions since last change (≥ " + thresholdDays + " days): " + staleImp + "</li>"
    + "<li>Placements with no new clicks since last change (≥ " + thresholdDays + " days): " + staleClk + "</li>"
    + "</ul>";
}

function buildImmediateAttentionData_(violations) {
  const ownerMap = loadOwnerMapFromNetworks_();
  const hMap = getHeaderMap(violations[0]);
  const perOwner = {};
  
  const MAX_ROWS_PER_OWNER = 30;
  const MAX_TOTAL_OWNER_ROWS = 1000;

  const idx = {
    netId: hMap["Network ID"], adv: hMap["Advertiser"], camp: hMap["Campaign"],
    pid: hMap["Placement ID"], plc: hMap["Placement"], impr: hMap["Impressions"],
    clk: hMap["Clicks"], ctr: hMap["CTR (%)"], cpc$: hMap["$CPC"], cpm$: hMap["$CPM"],
    issues: hMap["Issue Type"], rd: hMap["Report Date"], pe: hMap["Placement End Date"]
  };

  const BUCKETS = { PERF: 1, COST_BIMBAL: 2, BILLING: 3, DELIV_STRICT: 4, DELIV_CPM_ONLY: 5, DELIV_GENERAL: 6 };
  
  const today = new Date();
  const firstOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

  function qualifies_(row) {
    const issues = String(row[idx.issues] || "");
    if (/\(Low Priority\)/i.test(issues)) return null;

    const imp = Number(row[idx.impr] || 0);
    const clk = Number(row[idx.clk] || 0);
    const both = imp > 0 && clk > 0;
    const clicksGtImpr = both && (clk > imp);

    const cpc = _parseMoney_(row[idx.cpc$]);
    const cpm = _parseMoney_(row[idx.cpm$]);
    const ctrPct = _parsePct_(row[idx.ctr]);

    const rd = new Date(row[idx.rd]);
    const pe = new Date(row[idx.pe]);
    const isPostFlight = pe < firstOfMonth && rd >= firstOfMonth;

    const isPerformance = /🟨\s*PERFORMANCE: CTR ≥ 90% & CPM ≥ \$?10/.test(issues) || (ctrPct >= 90 && cpm >= 10);
    const isCostBothMetricsClicksGtImpr = /🟩\s*COST: CPC\+CPM Clicks > Impr.*CPC > \$?10/i.test(issues) || (both && clicksGtImpr && cpc > 10);
    const isBillingActive = /🟥\s*BILLING: Active CPC Billing Risk/i.test(issues) && both && clicksGtImpr && cpc > 10;
    const isBillingRecent = /🟥\s*BILLING: Recently Expired CPC Risk/i.test(issues) && both && clicksGtImpr && cpc > 10;
    const isBillingExpired = /🟥\s*BILLING: Expired CPC Risk/i.test(issues) && both && clicksGtImpr && cpc > 10;
    const isDelivStrict = /🟦\s*DELIVERY: Post-Flight Activity/i.test(issues) && isPostFlight && both && clicksGtImpr && cpc > 10;
    const isDelivCpmOnly = /🟦\s*DELIVERY: Post-Flight Activity/i.test(issues) && isPostFlight && (imp > 0 && clk === 0) && cpm > 10;
    const isDelivGeneral = /🟦\s*DELIVERY: Post-Flight Activity/i.test(issues) && isPostFlight && (cpc > 10 || cpm > 10);

    const isCpcOnly = /🟩\s*COST:\s*CPC\s*Only\s*>\s*\$?10/i.test(issues) || (imp === 0 && clk > 0 && cpc > 10);
    const isCpmOnly = /🟩\s*COST:\s*CPM\s*Only\s*>\s*\$?10/i.test(issues) || (imp > 0 && clk === 0 && cpm > 10);
    if (isCpcOnly || isCpmOnly) return null;

    if (isPerformance) return { bucket: BUCKETS.PERF };
    if (isCostBothMetricsClicksGtImpr) return { bucket: BUCKETS.COST_BIMBAL };
    if (isBillingActive || isBillingRecent || isBillingExpired) return { bucket: BUCKETS.BILLING };
    if (isDelivStrict) return { bucket: BUCKETS.DELIV_STRICT };
    if (isDelivCpmOnly) return { bucket: BUCKETS.DELIV_CPM_ONLY };
    if (isDelivGeneral) return { bucket: BUCKETS.DELIV_GENERAL };

    return null;
  }

  for (let i = 1; i < violations.length; i++) {
    const row = violations[i];
    const q = qualifies_(row);
    if (!q) continue;

    const netId = String(row[idx.netId] || "").trim();
    const adv = String(row[idx.adv] || "").trim();
    const rep = resolveRep_(ownerMap, netId, adv);

    if (!perOwner[rep]) perOwner[rep] = [];
    perOwner[rep].push({
      bucket: q.bucket, adv: adv, camp: String(row[idx.camp] || ""),
      pid: String(row[idx.pid] || ""), plc: String(row[idx.plc] || ""),
      imp: Number(row[idx.impr] || 0), clk: Number(row[idx.clk] || 0),
      issue: String(row[idx.issues] || "")
    });
  }

  const owners = Object.keys(perOwner).sort(function(a,b){ return a.toLowerCase().localeCompare(b.toLowerCase()); });
  
  return { owners: owners, perOwner: perOwner };
}

function buildImmediateAttentionHtmlForOwners_(owners, perOwner) {
  const MAX_ROWS_PER_OWNER = 30;
  const MAX_TOTAL_OWNER_ROWS = 1000;
  
  let html = '';
  let totalRows = 0;

  for (let i = 0; i < owners.length; i++) {
    const rep = owners[i];
    if (totalRows >= MAX_TOTAL_OWNER_ROWS) break;
    
    const arr = perOwner[rep];
    arr.sort(function(a, b){
      if (a.bucket !== b.bucket) return a.bucket - b.bucket;
      const aAdv = String(a.adv||"").toLowerCase(), bAdv = String(b.adv||"").toLowerCase();
      if (aAdv !== bAdv) return aAdv.localeCompare(bAdv);
      if (b.clk !== a.clk) return b.clk - a.clk;
      if (b.imp !== a.imp) return b.imp - a.imp;
      return a.pid.localeCompare(b.pid);
    });

    const take = Math.min(arr.length, MAX_ROWS_PER_OWNER, MAX_TOTAL_OWNER_ROWS - totalRows);
    if (take <= 0) break;
    totalRows += take;

    html += "<p><b>" + rep + "</b> (Showing " + take + " of " + arr.length + ")</p>";
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse: collapse; font-size: 11px;">'
         +  '<tr style="background-color:#f9f9f9;font-weight:bold;">'
         +  '<th>Advertiser</th><th>Campaign</th><th>Placement ID</th><th>Placement</th><th>Impr</th><th>Clicks</th><th>Issue(s)</th>'
         +  '</tr>';

    for (let j = 0; j < take; j++) {
      const o = arr[j];
      const campShort = o.camp.length > 40 ? o.camp.substring(0, 40) + "…" : o.camp;
      const plcShort = o.plc.length > 30 ? o.plc.substring(0, 30) + "…" : o.plc;
      html += "<tr>"
           +  "<td>" + o.adv + "</td>"
           +  "<td>" + campShort + "</td>"
           +  "<td>" + o.pid + "</td>"
           +  "<td>" + plcShort + "</td>"
           +  "<td>" + o.imp + "</td>"
           +  "<td>" + o.clk + "</td>"
           +  "<td>" + o.issue + "</td>"
           +  "</tr>";
    }
    html += "</table><br/>";
  }

  return html;
}

function fmtMs_(ms) {
  if (ms < 0) ms = 0;
  var s = Math.floor(ms / 1000);
  var m = Math.floor(s / 60);
  var r = s % 60;
  return (m + 'm ' + r + 's');
}

function logStep_(label, fn, runStartMs, quotaMinutes) {
  var stepStart = Date.now();
  Logger.log('▶ ' + label + ' — START @ ' + new Date(stepStart).toISOString());
  try {
    var out = fn();
    SpreadsheetApp.flush();
    var stepMs = Date.now() - stepStart;
    var totalMs = Date.now() - runStartMs;
    var quotaMs = (quotaMinutes || 6) * 60 * 1000;
    var leftMs = quotaMs - totalMs;

    Logger.log('✅ ' + label + ' — DONE in ' + fmtMs_(stepMs)
      + ' (since run start: ' + fmtMs_(totalMs)
      + ', est. time left: ' + fmtMs_(leftMs) + ')');

    if (leftMs <= 60000) {
      Logger.log('⏳ WARNING: ~' + Math.max(0, Math.floor(leftMs/1000)) + 's left in Apps Script quota window.');
    }
    return out;
  } catch (e) {
    Logger.log('❌ ' + label + ' — ERROR: ' + (e && e.stack ? e.stack : e));
    throw e;
  }
}

// ---------------------
// runItAll (with execution logging per step) — MANUAL USE
// ---------------------
function runItAll() {
  var APPROX_QUOTA_MINUTES = 6; // leave at 6 unless your domain truly has more
  var runStart = Date.now();
  Logger.log('🚀 runItAll — START @ ' + new Date(runStart).toISOString()
             + ' (approx quota: ' + APPROX_QUOTA_MINUTES + ' min)');

  try {
    // 1) Prep & ingest
    logStep_('trimAllSheetsToData_', function(){ trimAllSheetsToData_(); }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('importDCMReports',     function(){ importDCMReports();      }, runStart, APPROX_QUOTA_MINUTES);

    // 2) If low on time, schedule QA and exit (handoff)
    var totalMs  = Date.now() - runStart;
    var quotaMs  = APPROX_QUOTA_MINUTES * 60 * 1000;
    var timeLeft = Math.max(0, quotaMs - totalMs);

    if (timeLeft < 2 * 60 * 1000) {
      Logger.log('⏭ Not enough time left for QA (' + Math.floor(timeLeft/1000) + 's). Scheduling QA handoff.');
      clearQAState_();           // ensure a fresh QA session
      cancelQAChunkTrigger_();   // clear any stale chunk trigger
      scheduleNextQAChunk_(1);   // kick off the first QA chunk shortly
      return;                    // exit cleanly to avoid hitting the 6-min wall
    }

    // 3) Otherwise, run at most one QA chunk now
    logStep_('runQAOnly (single chunk)', function(){ runQAOnly(); }, runStart, APPROX_QUOTA_MINUTES);

    // 4) Alerts & summary (summary already guards on QA completion & date)
    logStep_('sendPerformanceSpikeAlertIfPre15', function(){ sendPerformanceSpikeAlertIfPre15(); }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('sendMidFlightDropAlert',           function(){ sendMidFlightDropAlert();           }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('sendEmailSummary',                 function(){ sendEmailSummary();                 }, runStart, APPROX_QUOTA_MINUTES);
  } finally {
    var totalMs = Date.now() - runStart;
    Logger.log('🏁 runItAll — FINISHED in ' + fmtMs_(totalMs));
  }
}

// ---------------------
// runItAllMorning (no email, for time-driven trigger)
// ---------------------
function runItAllMorning() {
  var APPROX_QUOTA_MINUTES = 6;
  var runStart = Date.now();
  const isAuto = !isManualRun_();
  
  Logger.log('🚀 runItAllMorning — START @ ' + new Date(runStart).toISOString()
             + ' (approx quota: ' + APPROX_QUOTA_MINUTES + ' min)');

  try {
    // 1) Prep & ingest
    logStep_('trimAllSheetsToData_', function(){ trimAllSheetsToData_(); }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('importDCMReports',     function(){ importDCMReports();      }, runStart, APPROX_QUOTA_MINUTES);

    // 2) If low on time, schedule QA and exit (handoff)
    var totalMs  = Date.now() - runStart;
    var quotaMs  = APPROX_QUOTA_MINUTES * 60 * 1000;
    var timeLeft = Math.max(0, quotaMs - totalMs);

    if (timeLeft < 2 * 60 * 1000) {
      Logger.log('⏭ Not enough time left for QA (' + Math.floor(timeLeft/1000) + 's). Scheduling QA handoff.');
      clearQAState_();
      cancelQAChunkTrigger_();
      scheduleNextQAChunk_(1);
      logAuditEntry_('runItAllMorning', 'PARTIAL_HANDOFF', Date.now() - runStart, null, null, 'Handed off to QA chunks');
      return;
    }

    // 3) Run at most one QA chunk now
    logStep_('runQAOnly (single chunk)', function(){ runQAOnly(); }, runStart, APPROX_QUOTA_MINUTES);

    // 4) Performance spike alert (fast; safe to keep here)
    logStep_('sendPerformanceSpikeAlertIfPre15', function(){ sendPerformanceSpikeAlertIfPre15(); }, runStart, APPROX_QUOTA_MINUTES);

    // ❌ NO sendEmailSummary here — that gets its own trigger/window
    
    logAuditEntry_('runItAllMorning', 'SUCCESS', Date.now() - runStart, null, null, null);
  } catch (e) {
    Logger.log('❌ runItAllMorning failed: ' + e.message);
    if (isAuto) {
      sendFailureEmail_('runItAllMorning', e, {
        stage: 'morning execution',
        duration: fmtMs_(Date.now() - runStart)
      });
    }
    throw e;
  } finally {
    var totalMs = Date.now() - runStart;
    Logger.log('🏁 runItAllMorning — FINISHED in ' + fmtMs_(totalMs));
  }
}

// ---------------------
// runDailyEmailSummary (email only, for separate trigger)
// ---------------------
function runDailyEmailSummary() {
  var APPROX_QUOTA_MINUTES = 6;
  var runStart = Date.now();
  const isAuto = !isManualRun_();
  
  Logger.log('🚀 runDailyEmailSummary — START @ ' + new Date(runStart).toISOString()
             + ' (approx quota: ' + APPROX_QUOTA_MINUTES + ' min)');

  try {
    // sendEmailSummary already:
    //  - skips if QA still has an active session
    //  - skips before the 15th of the month
    //  - supports chunked execution
    logStep_('sendEmailSummary', function(){ sendEmailSummary(); }, runStart, APPROX_QUOTA_MINUTES);
  } catch (e) {
    Logger.log('❌ runDailyEmailSummary failed: ' + e.message);
    if (isAuto) {
      sendFailureEmail_('runDailyEmailSummary', e, {
        stage: 'email execution',
        duration: fmtMs_(Date.now() - runStart)
      });
    }
    throw e;
  } finally {
    var totalMs = Date.now() - runStart;
    Logger.log('🏁 runDailyEmailSummary — FINISHED in ' + fmtMs_(totalMs));
  }
}

// =====================================================================================================================
// =========================================== MID-FLIGHT DROP DETECTION ==============================================
// =====================================================================================================================

// ===== Performance Alert Cache Helpers =====
function getPerfAlertCacheSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const name = "_Perf Alert Cache";

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    let sh = ss.getSheetByName(name);
    if (!sh) {
      sh = ss.insertSheet(name);
      sh.hideSheet();
    }

    const needed = ["date","key","impressions","clicks"];
    const current = sh.getRange(1, 1, 1, 4).getValues()[0] || [];
    const ok = current.length === 4 && current
      .map(function(v){ return String(v).toLowerCase(); })
      .every(function(v, i){ return v === needed[i]; });

    if (!ok) {
      sh.getRange(1, 1, 1, 4).setValues([needed]);
    }
    return sh;
  } finally {
    lock.releaseLock();
  }
}

function loadLatestCacheMap_() {
  const sh = getPerfAlertCacheSheet_();
  const vals = sh.getDataRange().getValues();
  const map = {};
  for (let i = 1; i < vals.length; i++) {
    const d   = vals[i][0];
    const key = String(vals[i][1] || "");
    const imp = Number(vals[i][2] || 0);
    const clk = Number(vals[i][3] || 0);
    if (!key) continue;
    const ds = (d && d.getFullYear) ? Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(d || "");
    if (!map[key] || ds > map[key].date) {
      map[key] = { date: ds, imp: imp, clk: clk };
    }
  }
  return map;
}

function appendTodaySnapshots_(rowsForSnapshot) {
  if (!rowsForSnapshot.length) return;
  const sh = getPerfAlertCacheSheet_();
  const tz = Session.getScriptTimeZone();
  const todayStr = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
  const out = rowsForSnapshot.map(function(r){ return [todayStr, r.key, r.imp, r.clk]; });
  sh.getRange(sh.getLastRow()+1, 1, out.length, 4).setValues(out);
}

function compactPerfAlertCache_(keepDays) {
  keepDays = keepDays || 35;
  const sh = getPerfAlertCacheSheet_();
  const cutoff = new Date(Date.now() - keepDays*86400000);
  const vals = sh.getDataRange().getValues();
  if (vals.length <= 1) return;

  const keep = [vals[0]];
  for (let i = 1; i < vals.length; i++) {
    const d = vals[i][0] instanceof Date ? vals[i][0] : new Date(vals[i][0]);
    if (d >= cutoff) keep.push(vals[i]);
  }
  sh.clearContents();
  sh.getRange(1,1,keep.length,4).setValues(keep);
}

function getHistoricalData_(key) {
  const sh = getPerfAlertCacheSheet_();
  const vals = sh.getDataRange().getValues();
  const history = [];
  
  for (let i = 1; i < vals.length; i++) {
    const rowKey = String(vals[i][1] || "");
    if (rowKey !== key) continue;
    
    const d = vals[i][0];
    const dateStr = (d && d.getFullYear) ? Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(d || "");
    const imp = Number(vals[i][2] || 0);
    const clk = Number(vals[i][3] || 0);
    
    history.push({ date: dateStr, imp: imp, clk: clk });
  }
  
  // Sort by date descending (most recent first)
  history.sort(function(a, b){ return b.date.localeCompare(a.date); });
  return history;
}

function getDropThreshold_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const networksSheet = ss.getSheetByName("Networks");
  if (!networksSheet) return 0.75; // Default 75% if Networks sheet doesn't exist
  
  try {
    const thresholdCell = networksSheet.getRange("H3").getValue();
    const thresholdStr = String(thresholdCell || "75%").trim();
    const match = thresholdStr.match(/(\d+\.?\d*)%?/);
    if (match) {
      return parseFloat(match[1]) / 100; // Convert "20%" to 0.20
    }
  } catch (e) {
    Logger.log('Error reading threshold: ' + e.message);
  }
  
  return 0.75; // Default 75%
}

function detectMidFlightDrop_(key, currentImp, currentClk, threshold) {
  const history = getHistoricalData_(key);
  
  // Need at least 4 days of history to establish 3-day baseline
  if (history.length < 4) return null;
  
  // Calculate daily increments for last 3 days (excluding today)
  const increments = [];
  for (let i = 1; i < Math.min(4, history.length); i++) {
    const dailyImp = history[i-1].imp - history[i].imp;
    const dailyClk = history[i-1].clk - history[i].clk;
    if (dailyImp >= 0) increments.push({ imp: dailyImp, clk: dailyClk });
  }
  
  if (increments.length === 0) return null;
  
  // Calculate 3-day average daily delivery
  const avgDailyImp = increments.reduce(function(sum, d){ return sum + d.imp; }, 0) / increments.length;
  const avgDailyClk = increments.reduce(function(sum, d){ return sum + d.clk; }, 0) / increments.length;
  
  // Calculate today's increment
  const todayImp = currentImp - history[0].imp;
  const todayClk = currentClk - history[0].clk;
  
  // Calculate drop percentage
  const impDropPct = avgDailyImp > 0 ? ((avgDailyImp - todayImp) / avgDailyImp) : 0;
  const clkDropPct = avgDailyClk > 0 ? ((avgDailyClk - todayClk) / avgDailyClk) : 0;
  
  // Flag if either metric dropped by threshold or more
  if (impDropPct >= threshold || clkDropPct >= threshold) {
    return {
      avgDailyImp: Math.round(avgDailyImp),
      todayImp: Math.round(todayImp),
      impDropPct: Math.round(impDropPct * 100),
      avgDailyClk: Math.round(avgDailyClk),
      todayClk: Math.round(todayClk),
      clkDropPct: Math.round(clkDropPct * 100)
    };
  }
  
  return null;
}

function generateMidFlightDropHtml_() {
  const today = new Date();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName("Raw Data");
  if (!rawSheet) return "";

  const values = rawSheet.getDataRange().getValues();
  if (values.length <= 1) return "";

  const headers = values[0];
  const hMap = {};
  headers.forEach(function(h, i){ hMap[h] = i; });

  const req = [
    "Network ID", "Advertiser", "Placement ID", "Placement", "Campaign",
    "Placement Start Date", "Placement End Date", "Impressions", "Clicks"
  ];
  if (req.some(function(k){ return hMap[k] === undefined; })) return "";

  const threshold = getDropThreshold_();
  const snapshots = [];
  const dropAlerts = [];

  // CPC/CPM calculation constants
  const CPC_RATE = 0.008;  // $8 per 1000 clicks
  const CPM_RATE = 0.034;  // $0.034 per 1000 impressions

  values.slice(1).forEach(function(r){
    const netId = String(r[hMap["Network ID"]] || "");
    const adv   = String(r[hMap["Advertiser"]] || "");
    const camp  = String(r[hMap["Campaign"]] || "");
    const pid   = String(r[hMap["Placement ID"]] || "");
    const plc   = String(r[hMap["Placement"]] || "");
    const imp   = Number(r[hMap["Impressions"]] || 0);
    const clk   = Number(r[hMap["Clicks"]] || 0);
    
    const plcStart = r[hMap["Placement Start Date"]];
    const plcEnd   = r[hMap["Placement End Date"]];
    
    // Must be mid-flight
    const startDate = plcStart instanceof Date ? plcStart : new Date(plcStart);
    const endDate   = plcEnd instanceof Date ? plcEnd : new Date(plcEnd);
    
    if (isNaN(startDate) || isNaN(endDate)) return;
    if (today < startDate || today > endDate) return;
    
    // Calculate costs
    const cpc = clk > 0 ? (clk * CPC_RATE) : 0;
    const cpm = imp > 0 ? (imp * CPM_RATE) : 0;
    
    // Filter: Must have CPM >= $10 OR CPC >= $10
    if (cpc < 10 && cpm < 10) return;

    const key = pid ? ('pid:' + pid) : ('k:' + netId + '|' + camp + '|' + plc);
    snapshots.push({ key: key, imp: imp, clk: clk });

    // Check for performance drop
    const dropData = detectMidFlightDrop_(key, imp, clk, threshold);
    
    if (dropData) {
      const trimmedCampaign  = camp.length > 30 ? camp.substring(0, 30) + "…" : camp;
      const trimmedPlacement = plc.length > 30 ? plc.substring(0, 30) + "…" : plc;
      
      dropAlerts.push({
        netId: netId,
        adv: adv,
        camp: trimmedCampaign,
        pid: pid,
        plc: trimmedPlacement,
        avgDailyImp: dropData.avgDailyImp,
        todayImp: dropData.todayImp,
        impDropPct: dropData.impDropPct,
        avgDailyClk: dropData.avgDailyClk,
        todayClk: dropData.todayClk,
        clkDropPct: dropData.clkDropPct,
        totalImp: imp,
        totalClk: clk,
        cpc: cpc.toFixed(2),
        cpm: cpm.toFixed(2)
      });
    }
  });

  appendTodaySnapshots_(snapshots);
  compactPerfAlertCache_(35);

  if (!dropAlerts.length) return "";

  const htmlRows = dropAlerts.map(function(o){
    return (
      '<tr>' +
      '<td>' + o.netId + '</td>' +
      '<td>' + o.adv + '</td>' +
      '<td>' + o.camp + '</td>' +
      '<td>' + o.pid + '</td>' +
      '<td>' + o.plc + '</td>' +
      '<td style="text-align:right;">' + o.avgDailyImp.toLocaleString() + '</td>' +
      '<td style="text-align:right;">' + o.todayImp.toLocaleString() + '</td>' +
      '<td style="text-align:right; color:red; font-weight:bold;">↓' + o.impDropPct + '%</td>' +
      '<td style="text-align:right;">' + o.avgDailyClk.toLocaleString() + '</td>' +
      '<td style="text-align:right;">' + o.todayClk.toLocaleString() + '</td>' +
      '<td style="text-align:right; color:red; font-weight:bold;">↓' + o.clkDropPct + '%</td>' +
      '<td style="text-align:right;">$' + o.cpc + '</td>' +
      '<td style="text-align:right;">$' + o.cpm + '</td>' +
      '</tr>'
    );
  }).join("");

  const thresholdPct = Math.round(getDropThreshold_() * 100);

  return ''
    + '<h2 style="color:#d9534f;">⚠️ MID-FLIGHT PERFORMANCE DROP ALERT</h2>'
    + '<p><b>Action Required:</b> ' + dropAlerts.length + ' placement(s) mid-flight with <b>' + thresholdPct + '%+ drop</b> in daily delivery vs 3-day average.</p>'
    + '<p><b>Filters Applied:</b> Only mid-flight placements with CPM ≥ $10 OR CPC ≥ $10</p>'
    + '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:12px;">'
    + '<tr style="background:#f2f2f2;font-weight:bold;">'
    + '<th>Network ID</th><th>Advertiser</th><th>Campaign</th><th>Placement ID</th><th>Placement</th>'
    + '<th>3-Day Avg<br/>Imps</th><th>Today\'s<br/>Imps</th><th>Imp<br/>Drop</th>'
    + '<th>3-Day Avg<br/>Clicks</th><th>Today\'s<br/>Clicks</th><th>Click<br/>Drop</th>'
    + '<th>CPC</th><th>CPM</th>'
    + '</tr>'
    + htmlRows
    + '</table><br/>';
}

function sendMidFlightDropAlert() {
  const today = new Date();
  const dayOfMonth = today.getDate();
  
  // Only send as separate email before 15th
  if (dayOfMonth >= 15) return;
  
  getPerfAlertCacheSheet_();
  
  const htmlContent = generateMidFlightDropHtml_();
  if (!htmlContent) return; // No drops detected

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const recipientsSheet = ss.getSheetByName("EMAIL LIST");
  if (!recipientsSheet) return;

  const emails = recipientsSheet.getRange("A2:A").getValues()
    .flat()
    .map(function(e){ return String(e || "").trim(); })
    .filter(Boolean);
  const uniqueEmails = Array.from(new Set(emails));
  if (uniqueEmails.length === 0) return;

  const todayStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "M/d/yy");
  const thresholdPct = Math.round(getDropThreshold_() * 100);
  const subject = '⚠️ MID-FLIGHT DROP ALERT (' + thresholdPct + '%) – ' + todayStr;

  const fullHtml = htmlContent + '<p><i>Brought to you by Platform Solutions Automation. (Made by: BK)</i></p>';

  uniqueEmails.forEach(function(addr){
    try {
      MailApp.sendEmail({ to: addr, subject: subject, htmlBody: fullHtml });
      Utilities.sleep(500);
    } catch (err) {
      Logger.log('❌ Failed to email ' + addr + ': ' + err);
    }
  });
}

// ---------------------
// arrayToCsv (utility)
// ---------------------
function arrayToCsv(data) {
  return data.map(function(row){ return row.map(function(cell){ return '"' + cell + '"'; }).join(","); }).join("\n");
}

// ====== Manual Immediate Mode Functions (No Chunking) ======
function runQAOnlyImmediate() {
  const startTime = Date.now();
  Logger.log('🏃 runQAOnlyImmediate - Manual immediate mode (no chunking)');
  
  // Clear any existing state to prevent confusion
  clearQAState_();
  cancelQAChunkTrigger_();
  
  // Run the original QA logic without chunking - just process everything
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const raw = ss.getSheetByName("Raw Data");
  const out = ss.getSheetByName("Violations");
  if (!raw || !out) {
    Logger.log('❌ Missing required sheets');
    return;
  }

  const data = raw.getDataRange().getValues();
  if (!data || data.length <= 1) {
    Logger.log('⚠️ No data to process');
    return;
  }

  clearViolations();
  
  Logger.log('Processing ' + (data.length - 1) + ' rows immediately...');
  
  // Set up a fake state that processes all rows
  const state = { session: String(Date.now()), next: 2, totalRows: data.length - 1 };
  saveQAState_(state);
  
  // Call the regular runQAOnly but it will try to process everything in one go
  // This will timeout if too large, but user will see exactly where
  runQAOnly();
  
  const duration = Date.now() - startTime;
  Logger.log('✅ runQAOnlyImmediate completed in ' + fmtMs_(duration));
}

function sendEmailSummaryImmediate() {
  const startTime = Date.now();
  Logger.log('🏃 sendEmailSummaryImmediate - Manual immediate mode (no chunking)');
  
  // Clear any existing state
  clearEmailState_();
  cancelEmailChunkTrigger_();
  
  // Call the chunked version but tell it not to chunk
  try {
    sendEmailSummaryChunked_(false);
    const duration = Date.now() - startTime;
    Logger.log('✅ sendEmailSummaryImmediate completed in ' + fmtMs_(duration));
  } catch (e) {
    Logger.log('❌ sendEmailSummaryImmediate failed: ' + e.message);
    throw e;
  }
}

function importDCMReportsChunked() {
  Logger.log('🏃 importDCMReportsChunked - Auto-resume mode');
  // For now, importDCMReports is fast enough (1m 7s), so just call it
  // Could add chunking later if needed
  importDCMReports();
}

function runItAllChunked() {
  Logger.log('🏃 runItAllChunked - Manual auto-resume mode');
  // Just call the regular runItAllMorning which already supports chunking
  runItAllMorning();
}

// ====== System Status & Management Functions ======
function showSystemStatus() {
  const ui = SpreadsheetApp.getUi();
  
  const qaState = getQAState_();
  const emailState = getEmailState_();
  
  let status = '📊 CM360 QA System Status\n\n';
  
  // QA Status
  if (qaState && qaState.session) {
    const progress = Math.round((qaState.next / qaState.totalRows) * 100);
    status += '🔄 QA IN PROGRESS\n';
    status += '  Progress: ' + qaState.next + ' / ' + qaState.totalRows + ' (' + progress + '%)\n';
    status += '  Session: ' + new Date(Number(qaState.session)).toLocaleString() + '\n\n';
  } else {
    status += '✅ QA Idle\n\n';
  }
  
  // Email Status
  if (emailState && emailState.session) {
    status += '🔄 EMAIL GENERATION IN PROGRESS\n';
    status += '  Stage: ' + (emailState.stage || 'unknown') + '\n';
    status += '  Session: ' + new Date(Number(emailState.session)).toLocaleString() + '\n\n';
  } else {
    status += '✅ Email Idle\n\n';
  }
  
  // Check for scheduled triggers
  const triggers = ScriptApp.getProjectTriggers();
  const qaTriggersCount = triggers.filter(function(t){ return t.getHandlerFunction() === 'runQAOnly'; }).length;
  const emailTriggersCount = triggers.filter(function(t){ return t.getHandlerFunction() === 'sendEmailSummary'; }).length;
  
  if (qaTriggersCount > 0) {
    status += '⏰ ' + qaTriggersCount + ' QA resume trigger(s) scheduled\n';
  }
  if (emailTriggersCount > 0) {
    status += '⏰ ' + emailTriggersCount + ' Email resume trigger(s) scheduled\n';
  }
  
  // Last audit entries
  try {
    const auditSheet = getAuditSheet_();
    const lastRow = auditSheet.getLastRow();
    if (lastRow > 1) {
      const recent = auditSheet.getRange(Math.max(2, lastRow - 2), 1, Math.min(3, lastRow - 1), 4).getValues();
      status += '\n📋 Recent Executions:\n';
      recent.forEach(function(r){
        const ts = Utilities.formatDate(new Date(r[0]), Session.getScriptTimeZone(), 'M/d HH:mm');
        status += '  ' + ts + ' - ' + r[1] + ': ' + r[2] + ' (' + r[3] + ')\n';
      });
    }
  } catch (e) {
    status += '\n⚠️ Could not load audit log\n';
  }
  
  ui.alert('System Status', status, ui.ButtonSet.OK);
}

function resetAllState() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Reset All State',
    'This will clear all execution state and cancel pending triggers.\n\nUse this if the system is stuck.\n\nContinue?',
    ui.ButtonSet.YES_NO
  );
  
  if (response === ui.Button.YES) {
    clearQAState_();
    clearEmailState_();
    cancelQAChunkTrigger_();
    cancelEmailChunkTrigger_();
    
    // Cancel any orphaned triggers
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(function(t){
      const fn = t.getHandlerFunction();
      if (fn === 'runQAOnly' || fn === 'sendEmailSummary') {
        const props = getScriptProps_();
        const qaId = props.getProperty(QA_TRIGGER_KEY);
        const emailId = props.getProperty(EMAIL_TRIGGER_KEY);
        const id = t.getUniqueId();
        
        // Only delete if not the main daily triggers
        if (id !== qaId && id !== emailId && t.getEventType() === ScriptApp.EventType.CLOCK) {
          ScriptApp.deleteTrigger(t);
        }
      }
    });
    
    ui.alert('✅ Reset Complete', 'All execution state cleared and triggers canceled.', ui.ButtonSet.OK);
    Logger.log('✅ Manual reset completed');
  }
}

// ---------------------
// Trim all sheets' grids (reclaim cells)
// ---------------------
function trimAllSheetsToData_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.getSheets().forEach(function(sh){
    const lastRow = Math.max(1, sh.getLastRow());
    const lastCol = Math.max(1, sh.getLastColumn());

    const maxRows = sh.getMaxRows();
    const targetRows = Math.max(2, lastRow);
    if (maxRows > targetRows) {
      sh.deleteRows(targetRows + 1, maxRows - targetRows);
    }

    const maxCols = sh.getMaxColumns();
    const targetCols = Math.max(1, lastCol);
    if (maxCols > targetCols) {
      sh.deleteColumns(targetCols + 1, maxCols - targetCols);
    }
  });
}