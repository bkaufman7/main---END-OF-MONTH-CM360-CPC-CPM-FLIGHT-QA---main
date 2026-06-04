// =====================
// CM360 QA Tools Script
// =====================
// Adds custom menu, imports CM360 reports via Gmail, runs QA checks,
// filters out ignored advertisers, and emails a summary of violations.

// ---------------------
// GLOBAL CONSTANTS
// ---------------------
const CPC_RATE = 0.008;  // $0.008 per click ($8 per 1000 clicks)
const CPM_RATE = 0.034;  // $0.034 per 1000 impressions (3.4 cents per 1000)
const TEST_EMAIL_RECIPIENT = 'bkaufman@horizonmedia.com';
const UNASSIGNED_ALERT_RECIPIENTS = ['BKaufman@horizonmedia.com'];

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
    .addItem("Build Live Placement Pivot", "buildFilteredRawDataAndPivot")
    .addItem("Build Filtered Raw Data + Live Placement Pivot", "pullDataAndBuildLivePlacementPivot")
    .addItem("Run QA Only (Immediate)", "runQAOnlyImmediate")
    .addItem("Run QA Only (Auto-Resume)", "runQAOnly")
    .addItem("Send Email Only (Immediate)", "sendEmailSummaryImmediate")
    .addItem("Send Email Only (Auto-Resume)", "sendEmailSummary")
    .addItem("FORCE Send Email Now", "sendEmailNow")
    .addItem("Test Send Both Emails (BK only)", "sendTestEmailsToBk")
    .addItem("Mock Performance Email Preview (BK only)", "sendMockPerformanceEmailPreview")
    .addSeparator()
    .addItem("� Debug QA Filtering", "debugQAFiltering")
    .addItem("🔍 Count Non-Zero Rows", "countNonZeroRows")
    .addSeparator()
    .addItem("�📊 System Status", "showSystemStatus")
    .addItem("🔄 Reset All State (if stuck)", "resetAllState")
    .addSeparator()
    .addItem("Authorize Email (one-time)", "authorizeMail_")
    .addItem("Create Daily Email Trigger (9am)", "createDailyEmailTrigger")
    .addItem("Create Reply Processor Trigger (7am)", "createReplyProcessorTrigger")
    .addSeparator()
    .addItem("Process Email Replies (Manual)", "processEmailReplies")
    .addItem("Clear Handled Placements", "clearHandledPlacements")
    .addSeparator()
    .addItem("Process Network Removal Requests", "processNetworkRemovalRequests")
    .addItem("Backfill Source Email Links", "backfillSourceEmailLinks")
    .addSeparator()
    .addItem("💰 Show Current Month Overage", "showCurrentMonthOverage")
    .addSeparator()
    .addItem("Clear Violations", "clearViolations")
    .addSeparator()
    .addItem("🕰️ Historical Backfill (Pick Date)", "runHistoricalBackfill")
    .addItem("▶ Resume Historical Backfill", "resumeHistoricalBackfill")
    .addItem("📍 Historical Backfill Status", "showHistoricalBackfillStatus")
    .addToUi();
}

function pullDataAndBuildLivePlacementPivot() {
  // Builds Raw Data Filtered and Live Placements Pivot from the existing Raw Data tab.
  // Does NOT pull from Gmail — run "Pull Data" first to refresh Raw Data.
  // suppressUiAlert: true because this function is called from a time-driven trigger
  // and SpreadsheetApp.getUi() throws in that context.
  buildFilteredRawDataAndPivot({ suppressUiAlert: true });
}

// Alias kept so the existing time-driven trigger for 'runQABackup' doesn't error.
function runQABackup() {
  runQAOnly();
}



// ---------------------
// HISTORICAL BACKFILL — manual-only, completely separate from normal trigger workflow
// ---------------------
const HIST_BACKFILL_STATE_KEY = 'historical_backfill_state_v1';
const HIST_BACKFILL_TRIGGER_KEY = 'historical_backfill_trigger_id';

function runHistoricalBackfill() {
  const ui = SpreadsheetApp.getUi();
  try {
    const defaultDateIso = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    const html = HtmlService.createHtmlOutput(
      '<!doctype html>' +
      '<html><head><meta charset="utf-8"><style>' +
      'body{font-family:Arial,sans-serif;padding:12px;line-height:1.4;}' +
      'label{display:block;margin-bottom:6px;font-weight:600;}' +
      'input[type=date]{font-size:14px;padding:6px;width:100%;box-sizing:border-box;}' +
      '.note{font-size:12px;color:#555;margin-top:8px;}' +
      '.actions{margin-top:14px;display:flex;gap:8px;justify-content:flex-end;}' +
      'button{padding:6px 10px;font-size:13px;cursor:pointer;}' +
      '#status{margin-top:10px;font-size:12px;color:#444;}' +
      '</style></head>' +
      '<body>' +
      '<label for="bfDate">Select backfill date</label>' +
      '<input id="bfDate" type="date" value="' + defaultDateIso + '" />' +
      '<div class="note">This will overwrite Raw Data, Filtered Data, Live Placements Pivot, and Violations before sending the historical QA email.</div>' +
      '<div class="actions">' +
      '<button type="button" onclick="google.script.host.close()">Cancel</button>' +
      '<button type="button" onclick="startBackfill()">Start</button>' +
      '</div>' +
      '<div id="status"></div>' +
      '<script>' +
      'function startBackfill(){' +
      '  var el=document.getElementById("bfDate");' +
      '  var status=document.getElementById("status");' +
      '  if(!el.value){status.textContent="Pick a date first.";return;}' +
      '  var ok=confirm("Start historical backfill for " + el.value + "?");' +
      '  if(!ok){return;}' +
      '  status.textContent="Queueing backfill...";' +
      '  google.script.run' +
      '    .withSuccessHandler(function(res){' +
      '      status.textContent=(res && res.message) ? res.message : "Queued. Check Historical Backfill Status.";' +
      '      setTimeout(function(){google.script.host.close();}, 1200);' +
      '    })' +
      '    .withFailureHandler(function(err){status.textContent="Error: " + (err && err.message ? err.message : err);})' +
      '    .startHistoricalBackfillFromPicker(el.value);' +
      '}' +
      '</script>' +
      '</body></html>'
    ).setWidth(420).setHeight(280);

    Logger.log('Opening Historical Backfill date picker sidebar.');
    SpreadsheetApp.getActiveSpreadsheet().toast('Opening Historical Backfill date picker...', 'Historical Backfill', 5);
    ui.showSidebar(html.setTitle('Historical Backfill'));
  } catch (e) {
    Logger.log('Failed to open date picker UI, falling back to prompt: ' + e.message);
    runHistoricalBackfillPromptFallback_();
  }
}

function runHistoricalBackfillPromptFallback_() {
  const ui = SpreadsheetApp.getUi();
  const prompt = ui.prompt(
    'Historical Backfill (Fallback)',
    'Enter date as YYYY-MM-DD:',
    ui.ButtonSet.OK_CANCEL
  );
  if (prompt.getSelectedButton() !== ui.Button.OK) return;
  startHistoricalBackfillFromPicker(prompt.getResponseText());
}

function startHistoricalBackfillFromPicker(dateInput) {
  Logger.log('Historical Backfill start requested for ' + dateInput);
  return startHistoricalBackfillFromPicker_(dateInput);
}

function startHistoricalBackfillFromPicker_(dateInput) {
  const m = String(dateInput || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    throw new Error('Invalid date selection. Please pick a date from the calendar.');
  }

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const targetDate = new Date(year, month - 1, day);
  if (isNaN(targetDate.getTime())) {
    throw new Error('Invalid date. Please try again.');
  }

  const dateStr = Utilities.formatDate(targetDate, Session.getScriptTimeZone(), "M/d/yy");

  clearHistoricalBackfillState_();
  cancelHistoricalBackfillTrigger_();

  saveHistoricalBackfillState_({
    session: String(Date.now()),
    targetDateIso: targetDate.toISOString(),
    dateStr: dateStr,
    stage: 'import',
    qaStarted: false,
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    error: ''
  });

  // Queue asynchronous execution so the sidebar call returns immediately.
  scheduleHistoricalBackfillNextChunk_(1);
  return {
    ok: true,
    dateStr: dateStr,
    message: 'Backfill queued for ' + dateStr + '. It will auto-resume until complete.'
  };
}

function showHistoricalBackfillStatus() {
  const ui = SpreadsheetApp.getUi();
  const state = getHistoricalBackfillState_();
  const qaState = getQAState_();

  if (!state) {
    ui.alert('Historical backfill status', 'No historical backfill is currently in progress.', ui.ButtonSet.OK);
    return;
  }

  const lines = [
    'Date: ' + (state.dateStr || state.targetDateIso || 'Unknown'),
    'Stage: ' + (state.stage || 'Unknown'),
    'Last Updated: ' + (state.updatedAt || 'Unknown'),
    'Started: ' + (state.startedAt || 'Unknown'),
    'QA Running: ' + ((qaState && qaState.session) ? 'Yes' : 'No')
  ];

  if (state.error) lines.push('Last Error: ' + state.error);

  ui.alert('Historical backfill status', lines.join('\n'), ui.ButtonSet.OK);
}

function resumeHistoricalBackfill() {
  const state = getHistoricalBackfillState_();
  if (!state) {
    SpreadsheetApp.getUi().alert('No historical backfill is currently in progress.');
    return;
  }
  executeHistoricalBackfill();
}

function executeHistoricalBackfill() {
  Logger.log('Historical Backfill executor invoked.');
  return executeHistoricalBackfill_({ startedByMenu: false });
}

function executeHistoricalBackfill_(_opts) {
  const dlock = LockService.getDocumentLock();
  if (!dlock.tryLock(5000)) {
    Logger.log('⏳ Historical Backfill lock busy; rescheduling.');
    scheduleHistoricalBackfillNextChunk_(2);
    return;
  }

  cancelHistoricalBackfillTrigger_();

  try {
    const state = getHistoricalBackfillState_();
    if (!state || !state.targetDateIso) {
      Logger.log('ℹ️ Historical Backfill: no active state found. Nothing to do.');
      return;
    }

    // Backward compatibility for states created before qaStarted existed.
    if (state.stage === 'qa' && typeof state.qaStarted === 'undefined') {
      state.qaStarted = true;
      state.updatedAt = new Date().toISOString();
      saveHistoricalBackfillState_(state);
    }

    const targetDate = new Date(state.targetDateIso);
    if (isNaN(targetDate.getTime())) {
      throw new Error('Historical backfill state has an invalid target date.');
    }

    Logger.log('🕰️ Historical Backfill stage: ' + state.stage + ' [' + (state.dateStr || state.targetDateIso) + ']');

    if (state.stage === 'import') {
      Logger.log('▶ Step 1: importDCMReportsForDate_');
      importDCMReportsForDate_(targetDate);
      state.stage = 'build';
      state.updatedAt = new Date().toISOString();
      saveHistoricalBackfillState_(state);
    }

    if (state.stage === 'build') {
      Logger.log('▶ Step 2: buildFilteredRawDataAndPivot (overrideDate)');
      buildFilteredRawDataAndPivot({ suppressUiAlert: true, overrideDate: targetDate });
      state.stage = 'qa';
      state.updatedAt = new Date().toISOString();
      saveHistoricalBackfillState_(state);
    }

    if (state.stage === 'qa') {
      const qaState = getQAState_();
      if (qaState && qaState.session) {
        state.stage = 'qa';
        state.updatedAt = new Date().toISOString();
        saveHistoricalBackfillState_(state);
        scheduleHistoricalBackfillNextChunk_(2);
        Logger.log('⏳ Historical Backfill waiting for QA completion; will resume automatically.');
        return;
      }

      // If QA was already started in a previous run and no QA state remains,
      // QA has completed and we can proceed directly to email.
      if (state.qaStarted) {
        Logger.log('✅ Historical Backfill: QA is complete; advancing to email stage.');
      } else {
        Logger.log('▶ Step 3: runQAOnly (chunked auto-resume, overrideDate)');
        runQAOnly({ overrideDate: targetDate });
        state.qaStarted = true;

        // Re-check immediately after starting QA.
        const qaStateAfter = getQAState_();
        if (qaStateAfter && qaStateAfter.session) {
          state.stage = 'qa';
          state.updatedAt = new Date().toISOString();
          saveHistoricalBackfillState_(state);
          scheduleHistoricalBackfillNextChunk_(2);
          Logger.log('⏳ Historical Backfill waiting for QA completion; will resume automatically.');
          return;
        }
      }

      state.stage = 'email';
      state.updatedAt = new Date().toISOString();
      saveHistoricalBackfillState_(state);
    }

    if (state.stage === 'email') {
      Logger.log('▶ Step 4: sendHistoricalEmailSummary_');
      sendHistoricalEmailSummary_(targetDate);
      state.stage = 'done';
      state.finishedAt = new Date().toISOString();
      state.updatedAt = new Date().toISOString();
      saveHistoricalBackfillState_(state);
    }

    if (state.stage === 'done') {
      Logger.log('✅ Historical Backfill COMPLETE — ' + (state.dateStr || state.targetDateIso));
      clearHistoricalBackfillState_();
      cancelHistoricalBackfillTrigger_();
      return;
    }

    // Safety net: if a future stage is introduced, continue automatically.
    scheduleHistoricalBackfillNextChunk_(2);
  } catch (e) {
    const state = getHistoricalBackfillState_() || {};
    state.error = e.message;
    state.updatedAt = new Date().toISOString();
    saveHistoricalBackfillState_(state);
    scheduleHistoricalBackfillNextChunk_(5);
    Logger.log('❌ Historical Backfill FAILED (will retry): ' + e.message + '\n' + (e.stack || ''));
    throw e;
  } finally {
    dlock.releaseLock();
  }
}

function getHistoricalBackfillState_() {
  const raw = PropertiesService.getDocumentProperties().getProperty(HIST_BACKFILL_STATE_KEY);
  return raw ? JSON.parse(raw) : null;
}

function saveHistoricalBackfillState_(obj) {
  PropertiesService.getDocumentProperties().setProperty(HIST_BACKFILL_STATE_KEY, JSON.stringify(obj));
}

function clearHistoricalBackfillState_() {
  PropertiesService.getDocumentProperties().deleteProperty(HIST_BACKFILL_STATE_KEY);
}

function scheduleHistoricalBackfillNextChunk_(minutesFromNow) {
  minutesFromNow = Math.max(1, Math.min(10, Math.floor(minutesFromNow || 2)));
  const props = getScriptProps_();

  const existingId = props.getProperty(HIST_BACKFILL_TRIGGER_KEY);
  if (existingId) {
    ScriptApp.getProjectTriggers().forEach(function(t){
      if (t.getUniqueId() === existingId) ScriptApp.deleteTrigger(t);
    });
    props.deleteProperty(HIST_BACKFILL_TRIGGER_KEY);
  }

  const trig = ScriptApp
    .newTrigger('executeHistoricalBackfill')
    .timeBased()
    .after(minutesFromNow * 60 * 1000)
    .create();

  props.setProperty(HIST_BACKFILL_TRIGGER_KEY, trig.getUniqueId());
}

function cancelHistoricalBackfillTrigger_() {
  const props = getScriptProps_();
  const id = props.getProperty(HIST_BACKFILL_TRIGGER_KEY);
  if (!id) return;
  ScriptApp.getProjectTriggers().forEach(function(t){
    if (t.getUniqueId() === id) ScriptApp.deleteTrigger(t);
  });
  props.deleteProperty(HIST_BACKFILL_TRIGGER_KEY);
}

function importDCMReportsForDate_(targetDate) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const removedNetworks = getRemovedNetworks(ss);
  const dataSheet = ss.getSheetByName("Raw Data") || ss.insertSheet("Raw Data");
  const dataHeaders = [
    "Network ID","Advertiser","Placement ID","Placement","Campaign",
    "Placement Start Date","Placement End Date","Campaign Start Date","Campaign End Date",
    "Ad","Impressions","Clicks","Report Date"
  ];
  dataSheet.clearContents().getRange(1, 1, 1, dataHeaders.length).setValues([dataHeaders]);

  const outputSheet = ss.getSheetByName("Violations");
  if (outputSheet) {
    const outputHeaders = [
      "Network ID","Report Date","Advertiser","Campaign","Campaign Start Date","Campaign End Date",
      "Ad","Placement ID","Placement","Placement Start Date","Placement End Date",
      "Impressions","Clicks","CTR (%)","Days Until Placement End","Flight Completion %",
      "Days Left in the Month","CPC Risk","$CPC","$CPM","Issue Type","Details",
      "Last Imp Change","Last Click Change","Owner (Ops)"
    ];
    outputSheet.clearContents().getRange(1, 1, 1, outputHeaders.length).setValues([outputHeaders]);
  }

  const tz = Session.getScriptTimeZone();
  const afterStr      = Utilities.formatDate(targetDate, tz, "yyyy/MM/dd");
  const nextDay       = new Date(targetDate.getTime() + 86400000);
  const beforeStr     = Utilities.formatDate(nextDay, tz, "yyyy/MM/dd");
  const reportDateStr = Utilities.formatDate(targetDate, tz, "yyyy-MM-dd");

  const threads = GmailApp.search('label:CM360 QA after:' + afterStr + ' before:' + beforeStr);
  Logger.log('🕰️ importDCMReportsForDate_: ' + threads.length + ' thread(s) found for ' + afterStr);

  let extractedData = [];
  threads.forEach(function(thread) {
    thread.getMessages().forEach(function(message) {
      message.getAttachments().forEach(function(att) {
        const netId = extractNetworkId(att.getName());
        if (removedNetworks.has(netId)) { Logger.log('Skipping removed network: ' + netId); return; }
        if (att.getContentType() === 'text/csv' || att.getName().endsWith('.csv')) {
          extractedData = extractedData.concat(processCSVForDate_(att.getDataAsString(), netId, reportDateStr));
        } else if (att.getContentType() === 'application/zip') {
          Utilities.unzip(att.copyBlob()).forEach(function(file) {
            const nestedNetId = extractNetworkId(file.getName());
            if (removedNetworks.has(nestedNetId)) return;
            if (file.getContentType() === 'text/csv' || file.getName().endsWith('.csv')) {
              extractedData = extractedData.concat(processCSVForDate_(file.getDataAsString(), nestedNetId, reportDateStr));
            }
          });
        }
      });
    });
  });

  if (extractedData.length) {
    dataSheet.getRange(2, 1, extractedData.length, dataHeaders.length).setValues(extractedData);
  }
  Logger.log('🕰️ importDCMReportsForDate_: ' + extractedData.length + ' rows imported for ' + afterStr);
}

function processCSVForDate_(fileContent, networkId, reportDateStr) {
  const lines = fileContent.split('\n').map(function(l) { return l.trim(); }).filter(Boolean);
  const startIndex = lines.findIndex(function(l) { return l.startsWith('Advertiser'); });
  if (startIndex === -1) return [];
  const csvData = Utilities.parseCsv(lines.slice(startIndex).join('\n'));
  csvData.shift();
  return csvData.map(function(row) { return [networkId].concat(row).concat([reportDateStr]); });
}

function sendHistoricalEmailSummary_(targetDate) {
  const ss              = SpreadsheetApp.getActiveSpreadsheet();
  const sheet           = ss.getSheetByName('Violations');
  const rawSheet        = ss.getSheetByName('Raw Data');
  const networksSheet   = ss.getSheetByName('Networks');
  const recipientsSheet = ss.getSheetByName('EMAIL LIST');

  if (!sheet || !rawSheet || !recipientsSheet) {
    throw new Error('Required sheets missing (Violations, Raw Data, or EMAIL LIST).');
  }

  const violations = sheet.getDataRange().getValues();
  const rawData    = rawSheet.getDataRange().getValues();

  if (violations.length <= 1) {
    Logger.log('⚠️ sendHistoricalEmailSummary_: No violations found — email not sent.');
    return;
  }

  const tz          = Session.getScriptTimeZone();
  const dateLabel   = Utilities.formatDate(targetDate, tz, 'M/d/yy');
  const dateForFile = Utilities.formatDate(targetDate, tz, 'M.d.yy');

  Logger.log('📧 Historical email: building HTML sections for ' + dateLabel + '...');
  const networkSummary = buildNetworkSummaryHtml_(violations, rawData, networksSheet);
  const groupedSummary = buildGroupedSummaryHtml_(violations);
  const staleHtml      = buildStaleHtml_(violations);
  const ownerData      = buildImmediateAttentionData_(violations);
  const immediateHtml  = ownerData.owners.length > 0
    ? '<p><b>Immediate Attention \u2014 Key Issues (by Owner)</b></p>' +
      buildImmediateAttentionHtmlForOwners_(ownerData.owners, ownerData.perOwner)
    : '';

  const subject = 'CM360 CPC/CPM FLIGHT QA \u2013 ' + dateLabel + ' [Historical Backfill]';
  let htmlBody = networkSummary +
    '<p style="font-size:11px;">The below is a table of the following Billing, Delivery, Performance and Cost issues:</p>' +
    '<div style="font-size:11px;">' + groupedSummary + '</div>' +
    (immediateHtml ? '<br/>' + immediateHtml : '') +
    '<br/>' + staleHtml +
    '<hr/>' +
    buildReplyInstructionsFooterHtml_();

  const MAX_HTML_CHARS = 90000;
  if (htmlBody.length > MAX_HTML_CHARS) {
    htmlBody = htmlBody.slice(0, MAX_HTML_CHARS - 1200) +
      '<p><i>(trimmed for size — full detail in the attached XLSX)</i></p>';
  }

  const fileName = 'CM360_QA_Violations_' + dateForFile + '_backfill.xlsx';
  const xlsxBlob = createXLSXFromSheet(sheet).setName(fileName);
  const tempFile = DriveApp.createFile(xlsxBlob);

  const uniqueEmails = getRecipientEmails_(recipientsSheet, null);
  let sentCount = 0;
  uniqueEmails.forEach(function(addr) {
    try {
      MailApp.sendEmail({
        to: addr,
        subject: subject,
        htmlBody: htmlBody,
        attachments: [tempFile.getBlob().setName(fileName)]
      });
      sentCount++;
      Utilities.sleep(300);
    } catch (err) {
      Logger.log('❌ sendHistoricalEmailSummary_: failed to email ' + addr + ': ' + err);
    }
  });

  try { tempFile.setTrashed(true); } catch (e) { /* noop */ }
  Logger.log('✅ sendHistoricalEmailSummary_: sent to ' + sentCount + '/' + uniqueEmails.length + ' recipients');
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

function normalizeRecipientEmails_(emails) {
  return Array.from(new Set((emails || [])
    .map(function(email) { return String(email || '').trim(); })
    .filter(Boolean)));
}

function getRecipientEmails_(recipientsSheet, overrideRecipients) {
  if (overrideRecipients && overrideRecipients.length) {
    return normalizeRecipientEmails_(overrideRecipients);
  }

  if (!recipientsSheet) return [];

  return normalizeRecipientEmails_(recipientsSheet.getRange("A2:A").getValues().flat());
}

function sendTestEmailsToBk() {
  const ui = SpreadsheetApp.getUi();
  const testRecipients = [TEST_EMAIL_RECIPIENT];

  clearEmailState_();
  cancelEmailChunkTrigger_();

  const perfResult = sendPerformanceSpikeAlertIfPre15({
    skipDateCheck: true,
    overrideRecipients: testRecipients,
    testMode: true
  });

  sendEmailSummaryChunked_(true, true, testRecipients);

  const perfMessage = perfResult.sent
    ? ('Performance alert sent to ' + TEST_EMAIL_RECIPIENT + ' with ' + perfResult.rowCount + ' changed/new row(s).')
    : ('Performance alert not sent. ' + perfResult.reason);

  ui.alert(
    'BK test send started.\n\n'
    + perfMessage + '\n'
    + 'Monthly summary is being sent only to ' + TEST_EMAIL_RECIPIENT + '.\n'
    + 'If the summary needs to auto-resume, the follow-up chunks will keep using the BK-only recipient override.'
  );
}

function sendMockPerformanceEmailPreview() {
  const ui = SpreadsheetApp.getUi();
  const today = new Date();
  const todayStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "M/d/yy");

  const mockRows = [
    {
      netId: '1234567',
      adv: 'Sample Advertiser A',
      camp: 'Q2 Brand Awareness - East',
      pid: '99887766',
      plc: 'Homepage Billboard 300x250',
      imp: 125000,
      clk: 113500,
      det: 'CTR 90.8%, CPM $12.20 (mock preview row)'
    },
    {
      netId: '7654321',
      adv: 'Sample Advertiser B',
      camp: 'Prospecting - Video - April',
      pid: '88776655',
      plc: 'In-stream 15s Pre-roll',
      imp: 84200,
      clk: 76110,
      det: 'CTR 90.4%, CPM $10.75 (mock preview row)'
    }
  ];

  const htmlRows = mockRows.map(function(o) {
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
  }).join('');

  const htmlBody = ''
    + '<p><b>ALERT:</b> 🟨 PERFORMANCE: CTR ≥ 90% & CPM ≥ $10</p>'
    + '<p><i>Mock preview email for formatting validation. This is not sourced from live data.</i></p>'
    + '<p>This report lists placements that continue to meet the performance-alert criteria. Items drop off once metrics are corrected or fall below the thresholds, but will continue to be listed within the CM360 CPC/CPM FLIGHT QA reports.</p>'
    + '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;font-size:11px;">'
    + '<tr style="background:#f2f2f2;font-weight:bold;">'
    + '<th>Network ID</th><th>Advertiser</th><th>Campaign</th><th>Placement ID</th>'
    + '<th>Placement</th><th>Impressions</th><th>Clicks</th><th>Details</th>'
    + '</tr>'
    + htmlRows
    + '</table>'
    + '<br/>'
    + buildReplyInstructionsFooterHtml_();

  const subject = 'MOCK PREVIEW – ALERT – PERFORMANCE (pre-monthly-summary) – ' + todayStr + ' – ' + mockRows.length + ' row(s)';

  MailApp.sendEmail({
    to: TEST_EMAIL_RECIPIENT,
    subject: subject,
    htmlBody: htmlBody
  });

  ui.alert('Mock performance preview sent to ' + TEST_EMAIL_RECIPIENT + '.');
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
// Create an installable time trigger for processing email replies
// ---------------------
function createReplyProcessorTrigger() {
  // Runs processEmailReplies daily at 7am (before data pull)
  ScriptApp.newTrigger('processEmailReplies')
    .timeBased()
    .atHour(7)
    .everyDays(1)
    .create();
  SpreadsheetApp.getUi().alert('✅ Reply processor trigger created. Will run daily at 7am.');
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
// clearHandledPlacements
// ---------------------
function clearHandledPlacements() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Handled Placements");
  if (!sheet) return;
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).clearContent();
  }
  SpreadsheetApp.getUi().alert('✅ Handled Placements cleared');
}

// ---------------------
// processEmailReplies - Main function to parse email replies
// ---------------------
function processEmailReplies() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const violationsSheet = ss.getSheetByName("Violations");
  
  // Get or create Handled Placements sheet
  let handledSheet = ss.getSheetByName("Handled Placements");
  if (!handledSheet) {
    handledSheet = ss.insertSheet("Handled Placements");
    const headers = [
      "Advertiser", "Campaign", "Placement ID", "Placement", "Impr", "Clicks", 
      "Issue(s)", "Note", "Note-Date Last Updated", "Email Addresses"
    ];
    handledSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    handledSheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");
  }
  
  if (!violationsSheet) {
    Logger.log("❌ Violations sheet not found");
    return;
  }
  
  // Auto-clear on first of month
  const today = new Date();
  if (today.getDate() === 1) {
    Logger.log("🗓️ First of month - clearing Handled Placements");
    const lastRow = handledSheet.getLastRow();
    if (lastRow > 1) {
      handledSheet.getRange(2, 1, lastRow - 1, handledSheet.getLastColumn()).clearContent();
    }
  }
  
  // Search for reply emails this month
  const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
  const formattedStart = Utilities.formatDate(startOfMonth, Session.getScriptTimeZone(), "yyyy/MM/dd");
  
  const commandSearches = [
    'subject:"Re: CM360 CPC/CPM FLIGHT QA" after:' + formattedStart,
    'subject:"Re: ALERT – PERFORMANCE (pre-monthly-summary)" after:' + formattedStart
  ];

  const threadMap = {};
  commandSearches.forEach(function(q) {
    GmailApp.search(q).forEach(function(t) {
      threadMap[t.getId()] = t;
    });
  });
  const threads = Object.keys(threadMap).map(function(k) { return threadMap[k]; });
  
  Logger.log("📧 Found " + threads.length + " reply threads");
  
  let processedCount = 0;
  let networksRemovedCount = 0;
  let recipientsAddedCount = 0;
  let errorCount = 0;
  
  // Track which messages we've already processed (by message ID) across runs
  const processedMessageIds = getProcessedReplyMessageIds_();
  
  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    
    // Process each message in the thread
    messages.forEach(function(message) {
            const messageId = message.getId();
      
            // Skip if we've already processed this message
            if (processedMessageIds[messageId]) {
              return;
            }
      
      const sender = message.getFrom();
      
      // Only process replies (not the original email) - check if sender is NOT the script account
      const scriptEmail = Session.getActiveUser().getEmail() || "platformsolutionsadopshorizon@gmail.com";
      if (sender.toLowerCase().indexOf(scriptEmail.toLowerCase()) !== -1) {
        return; // Skip messages from the script account itself
      }
      
      const messageDate = message.getDate();
      const body = message.getPlainBody();
      
      // Parse the email body
      const parseResult = parseReplyEmail_(body);

      if (parseResult.shouldIgnore) {
        processedMessageIds[messageId] = Date.now();
        return;
      }
      
      if (parseResult.error) {
        Logger.log("❌ Parse error from " + sender + ": " + parseResult.error);
        sendReplyErrorEmail_(sender, parseResult.error);
        errorCount++;
          processedMessageIds[messageId] = Date.now(); // Mark as processed even on error
        return;
      }
      
      // Handle REMOVE NETWORK commands
      if (parseResult.type === 'REMOVE_NETWORK') {
        const removed = removeNetworks_(parseResult.networkIds, sender);
        networksRemovedCount += removed;
        Logger.log("🗑️ " + sender + " removed " + removed + " network(s): " + parseResult.networkIds.join(", "));
        processedMessageIds[messageId] = Date.now();
        return;
      }

      // Handle ADD RECIPIENT commands
      if (parseResult.type === 'ADD_RECIPIENT' && parseResult.recipientEmails.length > 0) {
        const recipientResult = addRecipientsFromReply_(parseResult.recipientEmails, sender, messageDate);
        recipientsAddedCount += recipientResult.added.length;
        Logger.log("📬 " + sender + " add recipient request: added=" + recipientResult.added.length + ", duplicates=" + recipientResult.duplicates.length + ", invalidDomain=" + recipientResult.invalidDomain.length);
        processedMessageIds[messageId] = Date.now();
        return;
      }
      
      // Handle placement notes (supports multiple blocks with different notes)
      if (parseResult.type === 'HANDLE_PLACEMENT' && parseResult.placementIds.length > 0) {
        const blocks = parseResult.placementBlocks && parseResult.placementBlocks.length > 0
          ? parseResult.placementBlocks
          : [{ note: parseResult.note, placementIds: parseResult.placementIds }];

        blocks.forEach(function(block) {
          const result = storeHandledPlacements_(
            block.placementIds,
            block.note,
            sender,
            messageDate,
            violationsSheet,
            handledSheet
          );
          processedCount += result.stored;
          if (result.invalid.length > 0) {
            Logger.log("⚠️ Invalid placement IDs from " + sender + ": " + result.invalid.join(", "));
          }
        });

        processedMessageIds[messageId] = Date.now();
      }
    });
  });

  saveProcessedReplyMessageIds_(processedMessageIds);
  
  Logger.log("✅ Processed " + processedCount + " placement notes from email replies");
  if (networksRemovedCount > 0) {
    Logger.log("✅ Removed " + networksRemovedCount + " network(s) from monitoring");
  }
  if (recipientsAddedCount > 0) {
    Logger.log("✅ Added " + recipientsAddedCount + " recipient(s) to EMAIL LIST");
  }
  if (errorCount > 0) {
    Logger.log("⚠️ " + errorCount + " emails had errors");
  }
}

function getProcessedReplyMessageIds_() {
  const raw = PropertiesService.getDocumentProperties().getProperty('reply_processed_message_ids_v1');
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (e) {
    return {};
  }
}

function saveProcessedReplyMessageIds_(processedMap) {
  const map = processedMap || {};
  const keys = Object.keys(map);

  // Keep only the newest 5000 processed IDs so the property doesn't grow unbounded.
  if (keys.length > 5000) {
    keys.sort(function(a, b) {
      return Number(map[b] || 0) - Number(map[a] || 0);
    });
    const trimmed = {};
    for (let i = 0; i < 5000; i++) {
      trimmed[keys[i]] = map[keys[i]];
    }
    PropertiesService.getDocumentProperties().setProperty('reply_processed_message_ids_v1', JSON.stringify(trimmed));
    return;
  }

  PropertiesService.getDocumentProperties().setProperty('reply_processed_message_ids_v1', JSON.stringify(map));
}

// ---------------------
// Helper function to strip formatting characters (hidden Unicode, zero-width chars, etc.)
// This handles text copied from formatted emails that contain invisible formatting artifacts
function stripFormattingChars_(text) {
  if (!text) return text;
  // Remove zero-width spaces, directional marks, and other invisible Unicode formatting
  return text
    .replace(/[\u200B\u200C\u200D\u200E\u200F\uFEFF]/g, '') // Zero-width chars, directional marks, BOM
    .replace(/[\u202A-\u202E]/g, '')  // Directional formatting characters
    .replace(/[\u061C]/g, '')         // Arabic letter mark
    .replace(/[\u180E]/g, '')         // Mongolian vowel separator
    .replace(/[\u2060\u2061\u2062\u2063]/g, ''); // Invisible operators and separators
}

// parseReplyEmail_ - Extract note and placement IDs from email body
// ---------------------
function parseReplyEmail_(body) {
  // Stop parsing at signature markers
  const stopMarkers = [
    '[[#]]',
    'From:',
    'Sent:',
    '________________________________',
    'Get Outlook for',
    'Sent from'
  ];
  
  let cleanBody = stripFormattingChars_(body);
  
  // Find earliest stop marker
  let stopIndex = cleanBody.length;
  stopMarkers.forEach(function(marker) {
    const idx = cleanBody.indexOf(marker);
    if (idx !== -1 && idx < stopIndex) {
      stopIndex = idx;
    }
  });
  
  cleanBody = cleanBody.substring(0, stopIndex).trim();
  
  // Also stop at quoted reply (lines starting with >)
  const lines = cleanBody.split('\n');
  const relevantLines = [];
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    // Stop at quoted replies
    if (line.startsWith('>') || line.startsWith('On ') && line.includes('wrote:')) {
      break;
    }
    if (line) {
      relevantLines.push(line);
    }
  }
  
  if (relevantLines.length === 0) {
    return { error: null, note: null, placementIds: [], networkIds: [], recipientEmails: [], type: null, shouldIgnore: true };
  }

  const normalizedRelevantBody = relevantLines.join('\n').toLowerCase();
  const hasDigitContent = /\d/.test(normalizedRelevantBody);
  const hasCommandKeyword = /(remove\s+network|add\s+recipient)/i.test(normalizedRelevantBody);
  const looksLikeAutoReply = /(automatic reply|auto-?reply|out of office|out-of-office|ooo|vacation)/i.test(normalizedRelevantBody);

  // Ignore obvious non-action messages (for example OOO or quick acknowledgements with no IDs).
  if (!hasDigitContent && !hasCommandKeyword) {
    return {
      error: null,
      note: null,
      placementIds: [],
      networkIds: [],
      recipientEmails: [],
      type: null,
      shouldIgnore: true,
      ignoreReason: looksLikeAutoReply ? 'auto-reply' : 'non-actionable'
    };
  }
  
  // Check if this is a REMOVE NETWORK command
  const networkIdsToRemove = [];
  for (let i = 0; i < relevantLines.length; i++) {
    const line = relevantLines[i].trim();
    const match = line.match(/^REMOVE\s+NETWORK\s+(\d+)$/i);
    if (match) {
      networkIdsToRemove.push(match[1]);
    }
  }
  
  if (networkIdsToRemove.length > 0) {
    return { 
      error: null, 
      type: 'REMOVE_NETWORK', 
      networkIds: networkIdsToRemove,
      note: null,
      placementIds: [],
      recipientEmails: []
    };
  }

  // Check if this is an ADD RECIPIENT command
  const recipientEmailsToAdd = [];
  for (let i = 0; i < relevantLines.length; i++) {
    const line = relevantLines[i].trim();
    const match = line.match(/^ADD\s+RECIPIENT\s+([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})$/i);
    if (match) {
      recipientEmailsToAdd.push(match[1].toLowerCase());
    }
  }

  if (recipientEmailsToAdd.length > 0) {
    return {
      error: null,
      type: 'ADD_RECIPIENT',
      recipientEmails: recipientEmailsToAdd,
      note: null,
      placementIds: [],
      networkIds: []
    };
  }
  
  // Parse as placement handling — supports multiple blocks separated by blank lines.
  // Each block: first line is the note, remaining lines are placement IDs.
  // Example:
  //   Note for first group
  //   12345678
  //   87654321
  //
  //   Different note for second group
  //   11112222
  //   33334444

  // Re-split preserving blank lines so we can detect block boundaries.
  // relevantLines already stripped quoted content; rebuild with blanks from cleanBody.
  const allLines = cleanBody.split('\n');
  const blocksRaw = [];
  let currentBlock = [];
  for (let i = 0; i < allLines.length; i++) {
    const line = allLines[i].trim();
    if (line.startsWith('>') || (line.startsWith('On ') && line.includes('wrote:'))) break;
    if (line === '') {
      if (currentBlock.length > 0) {
        blocksRaw.push(currentBlock);
        currentBlock = [];
      }
    } else {
      currentBlock.push(line);
    }
  }
  if (currentBlock.length > 0) blocksRaw.push(currentBlock);

  // Each block: first line = note, rest = placement IDs
  const placementBlocks = []; // [{ note, placementIds }]
  for (let b = 0; b < blocksRaw.length; b++) {
    const block = blocksRaw[b];
    const blockNote = block[0];
    const blockIds = [];
    for (let i = 1; i < block.length; i++) {
      const cleanedLine = stripFormattingChars_(block[i]).trim();
      if (/^\d+$/.test(cleanedLine)) blockIds.push(cleanedLine);
    }
    if (blockIds.length > 0) {
      placementBlocks.push({ note: blockNote, placementIds: blockIds });
    }
  }

  if (placementBlocks.length === 0) {
    return { 
      error: "No placement IDs found. Please format your reply as:\n\nYour note here\n12345678\n87654321\n\nFor different notes per group, separate blocks with a blank line:\n\nFirst note\n12345678\n\nSecond note\n87654321",
      note: null,
      placementIds: [],
      networkIds: [],
      recipientEmails: [],
      type: null,
      shouldIgnore: false
    };
  }

  // Flatten all placement IDs for backward-compat; store blocks for multi-note support
  const allPlacementIds = placementBlocks.reduce(function(acc, b) { return acc.concat(b.placementIds); }, []);

  return { error: null, type: 'HANDLE_PLACEMENT', note: placementBlocks[0].note, placementIds: allPlacementIds, placementBlocks: placementBlocks, networkIds: [], recipientEmails: [], shouldIgnore: false };
}

// ---------------------
// storeHandledPlacements_ - Validate and store placement notes
// ---------------------
function storeHandledPlacements_(placementIds, note, sender, messageDate, violationsSheet, handledSheet) {
  const violations = violationsSheet.getDataRange().getValues();
  const vHeaders = violations[0];
  
  // Build a map of placement ID to violation data
  const placementMap = {};
  const placementIdCol = vHeaders.indexOf("Placement ID");
  
  for (let i = 1; i < violations.length; i++) {
    const row = violations[i];
    const pid = String(row[placementIdCol] || "").trim();
    if (pid) {
      placementMap[pid] = {
        advertiser: row[vHeaders.indexOf("Advertiser")],
        campaign: row[vHeaders.indexOf("Campaign")],
        placementId: pid,
        placement: row[vHeaders.indexOf("Placement")],
        impr: row[vHeaders.indexOf("Impressions")],
        clicks: row[vHeaders.indexOf("Clicks")],
        issues: row[vHeaders.indexOf("Issue Type")]
      };
    }
  }
  
  // Get existing handled placements
  const handledData = handledSheet.getDataRange().getValues();
  const hHeaders = handledData[0];
  const handledMap = {};
  
  for (let i = 1; i < handledData.length; i++) {
    const row = handledData[i];
    const pid = String(row[hHeaders.indexOf("Placement ID")] || "").trim();
    if (pid) {
      handledMap[pid] = {
        rowIndex: i + 1,
        existingNote: String(row[hHeaders.indexOf("Note")] || "").trim(),
        emails: String(row[hHeaders.indexOf("Email Addresses")] || "").trim()
      };
    }
  }
  
  const dateStr = Utilities.formatDate(messageDate, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm");
  const senderEmail = extractEmail_(sender);
  const senderName = sender.split("<")[0].trim() || senderEmail; // Extract name from "John Doe <email@example.com>"
  
  let stored = 0;
  const invalid = [];
  
  placementIds.forEach(function(pid) {
    // Validate: must exist in Violations
    if (!placementMap[pid]) {
      invalid.push(pid);
      return;
    }
    
    const vData = placementMap[pid];
    
    // Check if already handled
    if (handledMap[pid]) {
      // Append new note instead of overwriting (prevent duplicates)
      const rowIdx = handledMap[pid].rowIndex;
      const existingNote = handledMap[pid].existingNote;
      const existingEmails = handledMap[pid].emails;
      
      // Check if this exact note entry already exists (prevent re-processing same email)
      const newNoteEntry = "[" + senderName + " - " + dateStr + "] " + note;
      if (existingNote.indexOf(newNoteEntry) !== -1) {
        // Already processed this exact note - skip
        return;
      }
      
      let emailList = existingEmails ? existingEmails.split(", ") : [];
      if (emailList.indexOf(senderEmail) === -1) {
        emailList.push(senderEmail);
      }
      
      // Prepend new note (newest first) instead of appending
      const combinedNote = existingNote ? newNoteEntry + "\n" + existingNote : newNoteEntry;
      
      handledSheet.getRange(rowIdx, hHeaders.indexOf("Note") + 1).setValue(combinedNote);
      handledSheet.getRange(rowIdx, hHeaders.indexOf("Note-Date Last Updated") + 1).setValue(dateStr);
      handledSheet.getRange(rowIdx, hHeaders.indexOf("Email Addresses") + 1).setValue(emailList.join(", "));
    } else {
      // Add new row with formatted note
      const formattedNote = "[" + senderName + " - " + dateStr + "] " + note;
      const newRow = [
        vData.advertiser,
        vData.campaign,
        vData.placementId,
        vData.placement,
        vData.impr,
        vData.clicks,
        vData.issues,
        formattedNote,
        dateStr,
        senderEmail
      ];
      handledSheet.appendRow(newRow);
    }
    
    stored++;
  });
  
  return { stored: stored, invalid: invalid };
}

// ---------------------
// getOrCreateMonitoredNetworks_ - Get or create Monitored Networks sheet
// ---------------------
function getOrCreateMonitoredNetworks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName("Monitored Networks");
  
  if (!sheet) {
    sheet = ss.insertSheet("Monitored Networks");
    const headers = ["Network ID", "Network Name", "Date Added"];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");
  }
  
  return sheet;
}

// ---------------------
// syncMonitoredNetworks_ - Auto-add new networks from Raw Data + Networks
// ---------------------
function syncMonitoredNetworks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName("Raw Data");
  const monitoredSheet = getOrCreateMonitoredNetworks_();

  // Get existing monitored networks
  const monitoredData = monitoredSheet.getDataRange().getValues();
  const existingIds = {};
  for (let i = 1; i < monitoredData.length; i++) {
    const id = normalizeNetworkId_(monitoredData[i][0]);
    if (id) existingIds[id] = true;
  }

  const newNetworkNames = {};

  // Source 1: Raw Data network IDs seen in imported files
  if (rawSheet && rawSheet.getLastRow() > 1) {
    const rawData = rawSheet.getDataRange().getValues();
    const rHeaders = rawData[0];
    const netIdCol = rHeaders.indexOf("Network ID");

    if (netIdCol !== -1) {
      for (let i = 1; i < rawData.length; i++) {
        const netId = normalizeNetworkId_(rawData[i][netIdCol]);
        if (netId && !existingIds[netId] && !newNetworkNames[netId]) {
          newNetworkNames[netId] = "TO BE ADDED";
        }
      }
    }
  }

  // Source 2: Networks tab (single source of truth for names/mappings)
  const networksSheet = ss.getSheetByName("Networks");
  if (networksSheet && networksSheet.getLastRow() > 1) {
    const vals = networksSheet.getDataRange().getValues();
    const hdr = vals[0].map(function(h){ return String(h || "").trim().toLowerCase(); });

    function findIdx_(cands) {
      for (let i = 0; i < cands.length; i++) {
        const idx = hdr.indexOf(cands[i]);
        if (idx !== -1) return idx;
      }
      return -1;
    }

    const idIdx = findIdx_(["network id", "network_id", "networkid", "cm360 network id"]);
    let nameIdx = findIdx_(["network name", "network", "name", "friendly name"]);
    if (nameIdx === -1 && vals[0].length >= 2) nameIdx = 1;

    if (idIdx !== -1) {
      for (let r = 1; r < vals.length; r++) {
        const netId = normalizeNetworkId_(vals[r][idIdx]);
        if (!netId || existingIds[netId]) continue;

        const netName = nameIdx !== -1 ? String(vals[r][nameIdx] || "").trim() : "";
        newNetworkNames[netId] = netName || newNetworkNames[netId] || "TO BE ADDED";
      }
    }
  }

  // Add new networks
  const today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
  Object.keys(newNetworkNames).forEach(function(netId) {
    monitoredSheet.appendRow([netId, newNetworkNames[netId], today]);
    Logger.log("📋 Auto-added network " + netId + " to monitoring");
  });
}

// ---------------------
// getMonitoredNetworkIds_ - Get list of monitored network IDs
// ---------------------
function getMonitoredNetworkIds_() {
  const sheet = getOrCreateMonitoredNetworks_();
  const data = sheet.getDataRange().getValues();
  const ids = [];
  
  for (let i = 1; i < data.length; i++) {
    const id = normalizeNetworkId_(data[i][0]);
    if (id) ids.push(id);
  }
  
  return ids;
}

// ---------------------
// removeNetworks_ - Remove networks from monitoring
// ---------------------
function removeNetworks_(networkIds, sender) {
  const sheet = getOrCreateMonitoredNetworks_();
  const data = sheet.getDataRange().getValues();
  const normalizedTargets = (networkIds || []).map(function(id) { return normalizeNetworkId_(id); });
  
  let removed = 0;
  // Go backwards to avoid index shifting
  for (let i = data.length - 1; i >= 1; i--) {
    const id = normalizeNetworkId_(data[i][0]);
    if (normalizedTargets.indexOf(id) !== -1) {
      sheet.deleteRow(i + 1);
      removed++;
      Logger.log("🗑️ Removed network " + id + " (requested by " + sender + ")");
    }
  }
  
  return removed;
}

// ---------------------
// extractEmail_ - Extract email address from "Name <email>" format
// ---------------------
function extractEmail_(fromString) {
  const match = fromString.match(/<([^>]+)>/);
  return match ? match[1] : fromString;
}

// ---------------------
// sendReplyErrorEmail_ - Notify user of parsing error
// ---------------------
function sendReplyErrorEmail_(recipient, errorMessage) {
  const recipientEmail = extractEmail_(recipient);
  
  const subject = "⚠️ CM360 QA - Reply Format Error";
  const body = '<html><body style="font-family: Arial, sans-serif;">'
    + '<h2 style="color: #d9534f;">⚠️ Email Reply Format Error</h2>'
    + '<p>Your email reply could not be processed due to a formatting issue:</p>'
    + '<p style="color: #d9534f; font-weight: bold;">' + errorMessage + '</p>'
    + '<hr/>'
    + '<h3>Correct Format:</h3>'
    + '<pre style="background: #f5f5f5; padding: 10px;">'
    + 'Your note describing what was done\n'
    + '12345678\n'
    + '87654321\n'
    + '\n'
    + 'Different note for a second group\n'
    + '11112222\n'
    + '33334444'
    + '</pre>'
    + '<p><b>Important:</b></p>'
    + '<ul>'
    + '<li>First line of each block is your note (e.g., "Handled by digital", "Addressed with billing team")</li>'
    + '<li>Following lines in each block are placement IDs, one per line</li>'
    + '<li>Separate blocks with a blank line to assign different notes to different placements</li>'
    + '<li>Only include placement IDs that are currently in the violations report</li>'
    + '</ul>'
    + '<p>Please reply again with the correct format.</p>'
    + '<hr/>'
    + '<p style="color: #666; font-size: 11px;"><i>Automated notification from CM360 QA Tools</i></p>'
    + '</body></html>';
  
  try {
    MailApp.sendEmail({
      to: recipientEmail,
      subject: subject,
      htmlBody: body
    });
  } catch (e) {
    Logger.log("❌ Failed to send error email to " + recipientEmail + ": " + e.message);
  }
}

// ---------------------
// buildReplyInstructionsFooterHtml_ - Shared instruction footer for report emails
// ---------------------
function buildReplyInstructionsFooterHtml_() {
  return ''
    + '<h3>📧 How to Mark Placements as Handled:</h3>'
    + '<p>Reply to this email with the following format:</p>'
    + '<pre style="background: #f5f5f5; padding: 10px;">'
    + 'Your note describing what was done\n'
    + '12345678\n'
    + '87654321\n'
    + '\n'
    + 'Different note for a second group\n'
    + '11112222\n'
    + '33334444'
    + '</pre>'
    + '<p>Each block (separated by a blank line) can have its own note — all placement IDs in a block will be tagged with that block\'s note.</p>'
    + '<p>Handled placements will appear at the bottom of your section in future reports with a green checkmark.</p>'
    + '<hr/>'
    + '<h3>📧 To Remove a Network from Monitoring:</h3>'
    + '<p>Reply to this email with "REMOVE NETWORK [ID]" in the body.</p>'
    + '<p><b>Example (for multiple networks):</b></p>'
    + '<pre style="background: #f5f5f5; padding: 10px;">'
    + 'REMOVE NETWORK 12345\n'
    + 'REMOVE NETWORK 67890\n'
    + 'REMOVE NETWORK 99999'
    + '</pre>'
    + '<hr/>'
    + '<h3>📧 To Add Email Recipients to This Report:</h3>'
    + '<p>Reply to this email with "ADD RECIPIENT [email]" in the body.</p>'
    + '<p><b>Only email addresses ending in @horizonmedia.com will be accepted through email replies.</b></p>'
    + '<p><b>Example (for multiple recipients):</b></p>'
    + '<pre style="background: #f5f5f5; padding: 10px;">'
    + 'EXAMPLE: ADD RECIPIENT first.last@horizonmedia.com\n'
    + 'EXAMPLE: ADD RECIPIENT team.alias@horizonmedia.com\n'
    + 'EXAMPLE: ADD RECIPIENT owner.name@horizonmedia.com'
    + '</pre>'
    + '<p><i>Tip: delete "EXAMPLE:" when sending a real add request.</i></p>'
    + '<p>Approved addresses will be added to the next available row in the EMAIL LIST sheet and included in future report sends.</p>'
    + '<hr/>'
    + '<h3>📋 How to Add a New Network Report:</h3>'
    + '<ol>'
    + '<li><b>Step 1:</b> Place this exact string into the AI helper in DCM Reports:<br/>'
    + '<code>Advertiser, Placement ID, Placement, Campaign, Placement Start Date, Placement End Date, Campaign Start Date, Campaign End Date, Ad, Impressions, Clicks, This Month</code></li>'
    + '<li><b>Step 2:</b> Set the report subject/label to exactly: <code>BKCM360 Global QA Check</code></li>'
    + '<li><b>Step 3:</b> Set schedule with end date of Jan 1, 2030</li>'
    + '<li><b>Step 4:</b> Ensure you CC this email exactly: <code>platformsolutionsadopshorizon@gmail.com</code></li>'
    + '</ol>'
    + '<hr/>'
    + '<p><i>Brought to you by the Platform Solutions Automation. (Made by: BK)</i></p>';
}

// ---------------------
// addRecipientsFromReply_ - Adds valid recipient emails from reply commands
// ---------------------
function addRecipientsFromReply_(recipientEmails, sender, messageDate) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let recipientsSheet = ss.getSheetByName("EMAIL LIST");

  if (!recipientsSheet) {
    recipientsSheet = ss.insertSheet("EMAIL LIST");
    recipientsSheet.getRange(1, 1).setValue("Email");
    recipientsSheet.getRange(1, 1).setFontWeight("bold");
  }

  const existing = recipientsSheet.getRange("A2:A").getValues()
    .flat()
    .map(function(e) { return String(e || "").trim().toLowerCase(); })
    .filter(Boolean);
  const existingSet = new Set(existing);

  const added = [];
  const duplicates = [];
  const invalidDomain = [];

  recipientEmails.forEach(function(email) {
    const normalized = String(email || "").trim().toLowerCase();
    if (!normalized) return;

    if (!/@horizonmedia\.com$/i.test(normalized)) {
      invalidDomain.push(normalized);
      return;
    }

    if (existingSet.has(normalized)) {
      duplicates.push(normalized);
      return;
    }

    const nextRow = Math.max(2, recipientsSheet.getLastRow() + 1);
    recipientsSheet.getRange(nextRow, 1).setValue(normalized);
    existingSet.add(normalized);
    added.push(normalized);
  });

  if (added.length > 0 || duplicates.length > 0 || invalidDomain.length > 0) {
    sendRecipientListUpdateAlert_(sender, messageDate, added, duplicates, invalidDomain);
  }

  return { added: added, duplicates: duplicates, invalidDomain: invalidDomain };
}

// ---------------------
// sendRecipientListUpdateAlert_ - Admin notification for recipient list updates
// ---------------------
function sendRecipientListUpdateAlert_(sender, messageDate, added, duplicates, invalidDomain) {
  const adminEmail = "bkaufman@horizonmedia.com";
  const dateStr = Utilities.formatDate(messageDate || new Date(), Session.getScriptTimeZone(), "MM/dd/yyyy HH:mm:ss");
  const senderEmail = extractEmail_(sender);

  let body = '<html><body style="font-family: Arial, sans-serif;">'
    + '<h2 style="color: #2c3e50;">CM360 QA Recipient List Update</h2>'
    + '<p><b>Requested By:</b> ' + sender + ' (' + senderEmail + ')</p>'
    + '<p><b>Request Time:</b> ' + dateStr + '</p>'
    + '<hr/>';

  body += '<h3 style="color:#28a745;">Added (' + added.length + ')</h3>';
  body += added.length ? ('<ul><li>' + added.join('</li><li>') + '</li></ul>') : '<p>None</p>';

  body += '<h3 style="color:#f0ad4e;">Duplicates (' + duplicates.length + ')</h3>';
  body += duplicates.length ? ('<ul><li>' + duplicates.join('</li><li>') + '</li></ul>') : '<p>None</p>';

  body += '<h3 style="color:#d9534f;">Rejected (Non-horizon domain) (' + invalidDomain.length + ')</h3>';
  body += invalidDomain.length ? ('<ul><li>' + invalidDomain.join('</li><li>') + '</li></ul>') : '<p>None</p>';

  body += '<hr/><p style="color:#666;font-size:11px;"><i>Automated notification from CM360 QA Tools</i></p>';
  body += '</body></html>';

  MailApp.sendEmail({
    to: adminEmail,
    subject: "CM360 QA Recipient List Updated",
    htmlBody: body
  });
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

// =====================
// REMOVED NETWORKS FEATURE
// =====================

/**
 * Gets the set of removed network IDs from the Removed Networks sheet
 * @param {SpreadsheetApp.Spreadsheet} ss - The active spreadsheet
 * @returns {Set<string>} Set of removed network IDs
 */
function getRemovedNetworks(ss) {
  const removedSheet = ss.getSheetByName("Removed Networks");
  const removedNetworks = new Set();
  
  if (removedSheet && removedSheet.getLastRow() > 1) {
    const data = removedSheet.getRange(2, 1, removedSheet.getLastRow() - 1, 1).getValues();
    data.forEach(function(row) {
      if (row[0]) removedNetworks.add(String(row[0]).trim());
    });
  }
  
  return removedNetworks;
}

/**
 * Ensures the Removed Networks audit sheet exists
 * @param {SpreadsheetApp.Spreadsheet} ss - The active spreadsheet
 * @returns {SpreadsheetApp.Sheet} The Removed Networks sheet
 */
function ensureRemovedNetworksSheet(ss) {
  try {
    let removedSheet = ss.getSheetByName("Removed Networks");
    
    if (!removedSheet) {
      removedSheet = ss.insertSheet("Removed Networks");
      const headers = ["Network ID", "Network Name", "Removed By", "Date Removed", "Source Email"];
      removedSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      removedSheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");
      Logger.log("Created Removed Networks sheet");
    }
    
    return removedSheet;
  } catch (error) {
    Logger.log("Failed to create Removed Networks sheet: " + error);
    throw error;
  }
}

/**
 * Processes network removal requests from email replies
 * Looks for "REMOVE NETWORK [ID]" commands in replies to QA emails
 * @returns {Array<Object>} Array of successfully removed networks
 */
function processNetworkRemovalRequests() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const networksSheet = ss.getSheetByName("Networks");
    const removedSheet = ensureRemovedNetworksSheet(ss);
    
    if (!networksSheet) {
      Logger.log("Networks sheet not found");
      return [];
    }
    
    const today = new Date();
    const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
    const formattedYesterday = Utilities.formatDate(yesterday, Session.getScriptTimeZone(), "yyyy/MM/dd");
    
    // Search for replies to QA Report emails
    const threads = GmailApp.search('subject:"CM360 CPC/CPM FLIGHT QA" after:' + formattedYesterday);
    const removalCommands = [];
    const regex = /REMOVE\s+NETWORK\s+(\d+)/gi;
    const exampleNetworkIds = new Set(["12345", "67890", "99999"]); // Skip example IDs from email instructions
  
    threads.forEach(function(thread) {
      thread.getMessages().forEach(function(message) {
        const body = message.getPlainBody();
        const from = message.getFrom();
        const messageId = message.getId();
        let match;
        
        while ((match = regex.exec(body)) !== null) {
          const networkId = match[1];
          
          // Skip example network IDs used in email instructions (silent — no log needed)
          if (exampleNetworkIds.has(networkId)) {
            continue;
          }
          
          removalCommands.push({
            networkId: networkId,
            from: from,
            date: message.getDate(),
            messageId: messageId
          });
        }
      });
    });
  
    if (removalCommands.length === 0) {
      Logger.log("No removal requests found.");
      return [];
    }
    
    // Deduplicate by network ID (keep latest request)
    const uniqueRemovals = new Map();
    removalCommands.forEach(function(cmd) {
      if (!uniqueRemovals.has(cmd.networkId) || uniqueRemovals.get(cmd.networkId).date < cmd.date) {
        uniqueRemovals.set(cmd.networkId, cmd);
      }
    });
    
    // Get existing removed networks to avoid duplicates
    const alreadyRemoved = getRemovedNetworks(ss);
    const successfulRemovals = [];
    
    uniqueRemovals.forEach(function(cmd, networkId) {
      if (alreadyRemoved.has(networkId)) {
        Logger.log("Network " + networkId + " already removed. Skipping.");
        return;
      }
      
      // Find network in Networks sheet
      const networksData = networksSheet.getDataRange().getValues();
      let networkName = "Unknown";
      let rowToDelete = -1;
      
      for (let i = 1; i < networksData.length; i++) {
        if (String(networksData[i][0]).trim() === networkId) {
          networkName = networksData[i][1] || "Unknown";
          rowToDelete = i + 1;
          break;
        }
      }
      
      // Add to Removed Networks sheet with Gmail source link
      const gmailLink = "https://mail.google.com/mail/u/0/#all/" + cmd.messageId;
      const newRow = [networkId, networkName, cmd.from, Utilities.formatDate(cmd.date, Session.getScriptTimeZone(), "MM/dd/yyyy HH:mm:ss"), gmailLink];
      removedSheet.appendRow(newRow);
      
      // Delete from Networks sheet if found
      if (rowToDelete > 0) {
        networksSheet.deleteRow(rowToDelete);
      }
      
      successfulRemovals.push({ networkId: networkId, networkName: networkName, from: cmd.from });
    });
    
    // Send confirmation email to bkaufman@horizonmedia.com
    if (successfulRemovals.length > 0) {
      let confirmBody = "<p>The following networks were removed from CM360 QA monitoring:</p>";
      confirmBody += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>";
      confirmBody += "<tr style='background-color: #f2f2f2; font-weight: bold;'><th>Network ID</th><th>Network Name</th><th>Requested By</th></tr>";
      
      successfulRemovals.forEach(function(removal) {
        confirmBody += "<tr><td>" + removal.networkId + "</td><td>" + removal.networkName + "</td><td>" + removal.from + "</td></tr>";
      });
      
      confirmBody += "</table>";
      
      MailApp.sendEmail({
        to: "bkaufman@horizonmedia.com",
        subject: "CM360 QA Networks Removed - Confirmation",
        htmlBody: confirmBody
      });
      Logger.log("Sent removal confirmation for " + successfulRemovals.length + " networks");
    }
    
    return successfulRemovals;
  } catch (error) {
    Logger.log("Failed to process network removal requests: " + error);
    // Send error notification to admin
    try {
      MailApp.sendEmail({
        to: "bkaufman@horizonmedia.com",
        subject: "ERROR: CM360 QA Network Removal Failed",
        body: "An error occurred while processing network removal requests:\n\n" + error
      });
    } catch (emailError) {
      Logger.log("Failed to send error notification email: " + emailError);
    }
    return [];
  }
}

/**
 * Backfills missing Source Email links in the Removed Networks sheet
 * Searches Gmail for the original removal request emails and adds links to column E
 */
function backfillSourceEmailLinks() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const removedSheet = ss.getSheetByName("Removed Networks");
    
    if (!removedSheet || removedSheet.getLastRow() < 2) {
      Logger.log("No removed networks to backfill");
      return 0;
    }
    
    const lastRow = removedSheet.getLastRow();
    const data = removedSheet.getRange(2, 1, lastRow - 1, 5).getValues(); // Get all columns A-E
    let updatedCount = 0;
    const exampleNetworkIds = new Set(["12345", "67890", "99999"]);
    
    for (let i = 0; i < data.length; i++) {
      const networkId = String(data[i][0]).trim();
      const sourceEmail = data[i][4]; // Column E (index 4)
      
      // Skip if already has a link or is an example ID
      if (sourceEmail || !networkId || exampleNetworkIds.has(networkId)) {
        continue;
      }
      
      // Search Gmail for this network ID removal request
      try {
        const threads = GmailApp.search('subject:"CM360 CPC/CPM FLIGHT QA" "REMOVE NETWORK ' + networkId + '"');
        
        if (threads.length > 0) {
          // Find the message with the removal command
          let foundMessageId = null;
          const regex = new RegExp("REMOVE\\s+NETWORK\\s+" + networkId, "i");
          
          for (let j = 0; j < threads.length; j++) {
            const messages = threads[j].getMessages();
            for (let k = 0; k < messages.length; k++) {
              if (regex.test(messages[k].getPlainBody())) {
                foundMessageId = messages[k].getId();
                break;
              }
            }
            if (foundMessageId) break;
          }
          
          if (foundMessageId) {
            const gmailLink = "https://mail.google.com/mail/u/0/#all/" + foundMessageId;
            removedSheet.getRange(i + 2, 5).setValue(gmailLink); // Row i+2, Column E
            updatedCount++;
            Logger.log("Added source email link for network " + networkId);
          } else {
            Logger.log("Could not find removal message for network " + networkId);
          }
        }
      } catch (searchError) {
        Logger.log("Failed to search for network " + networkId + ": " + searchError);
      }
      
      // Add a small delay to avoid quota issues
      if (updatedCount > 0 && updatedCount % 10 === 0) {
        Utilities.sleep(1000);
      }
    }
    
    Logger.log("Backfill complete: Updated " + updatedCount + " source email links");
    return updatedCount;
    
  } catch (error) {
    Logger.log("Failed to backfill source email links: " + error);
    try {
      MailApp.sendEmail({
        to: "bkaufman@horizonmedia.com",
        subject: "ERROR: Backfill Source Email Links Failed",
        body: "An error occurred while backfilling source email links:\n\n" + error
      });
    } catch (emailError) {
      Logger.log("Failed to send error notification: " + emailError);
    }
    return 0;
  }
}

function importDCMReports(skipViolationsClear) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // Process network removal requests FIRST (before importing data)
  processNetworkRemovalRequests();
  
  // Get list of removed networks to filter out
  const removedNetworks = getRemovedNetworks(ss);
  
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
  if (!skipViolationsClear) {
    outputSheet.clearContents().getRange(1,1,1,outputHeaders.length).setValues([outputHeaders]);
  }

  const threads = GmailApp.search('label:' + label + ' after:' + formattedToday);
  let extractedData = [];

  threads.forEach(function(thread){
    thread.getMessages().forEach(function(message){
      message.getAttachments().forEach(function(att){
        const netId = extractNetworkId(att.getName());
        
        // Skip if network is in the removed list
        if (removedNetworks.has(netId)) {
          Logger.log("Skipping removed network: " + netId);
          return;
        }
        
        if (att.getContentType() === "text/csv" || att.getName().endsWith(".csv")) {
          extractedData = extractedData.concat(processCSV(att.getDataAsString(), netId));
        } else if (att.getContentType() === "application/zip") {
          Utilities.unzip(att.copyBlob()).forEach(function(file){
            const nestedNetId = extractNetworkId(file.getName());
            
            // Skip if network is in the removed list
            if (removedNetworks.has(nestedNetId)) {
              Logger.log("Skipping removed network: " + nestedNetId);
              return;
            }
            
            if (file.getContentType() === "text/csv" || file.getName().endsWith(".csv")) {
              extractedData = extractedData.concat(processCSV(file.getDataAsString(), nestedNetId));
            }
          });
        }
      });
    });
  });

  if (extractedData.length) {
    dataSheet.getRange(2, 1, extractedData.length, dataHeaders.length).setValues(extractedData);
  }
  
  // Sync monitored networks after import
  syncMonitoredNetworks_();
  
  // Auto-add new networks to Networks tab
  autoAddNewNetworks_();
}

function buildFilteredRawDataAndPivot(options) {
  options = options || {};
  const showUiAlert = !options.suppressUiAlert;
  const ss = withBackoff_(function(){ return SpreadsheetApp.getActiveSpreadsheet(); }, "get active spreadsheet");
  const rawSheet = withBackoff_(function(){ return ss.getSheetByName("Raw Data"); }, "get Raw Data sheet");
  if (!rawSheet || rawSheet.getLastRow() < 2) {
    if (showUiAlert) {
      SpreadsheetApp.getUi().alert("Raw Data sheet is empty. Pull data first.");
    } else {
      Logger.log("buildFilteredRawDataAndPivot skipped: Raw Data sheet is empty.");
    }
    return { filteredRowCount: 0, pivotRows: [], unassignedRows: [] };
  }

  const required = [
    "Network ID", "Advertiser", "Placement ID", "Placement", "Campaign",
    "Placement Start Date", "Placement End Date", "Report Date"
  ];

  const headers = withBackoff_(function(){
    return rawSheet.getRange(1, 1, 1, rawSheet.getLastColumn()).getValues()[0];
  }, "read Raw Data headers");
  const m = getHeaderMap(headers);

  const missing = required.filter(function(h){ return m[h] === undefined; });
  if (missing.length) {
    const message = "Raw Data is missing required column(s): " + missing.join(", ");
    if (showUiAlert) {
      SpreadsheetApp.getUi().alert(message);
    } else {
      Logger.log(message);
    }
    return { filteredRowCount: 0, pivotRows: [], unassignedRows: [] };
  }

  const ignoreSet = loadIgnoreAdvertisers(true);
  const ownerMap = loadOwnerMapFromNetworks_();
  const networkNameMap = loadNetworkNameMapFromNetworks_();
  const now = options.overrideDate || new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const nextMonthStart = new Date(now.getFullYear(), now.getMonth() + 1, 1);

  // Build summary incrementally by chunk-reading Raw Data
  const pairToPlacementIds = {};
  const unassignedPlacementIds = {};
  const dedupe = {};
  let filteredRowCount = 0;
  
  const lastRow = withBackoff_(function(){ return rawSheet.getLastRow(); }, "get Raw Data lastRow");
  const chunkSize = 5000;

  for (let startRow = 2; startRow <= lastRow; startRow += chunkSize) {
    const endRow = Math.min(startRow + chunkSize - 1, lastRow);
    const chunkData = withBackoff_(function(){
      return rawSheet.getRange(startRow, 1, endRow - startRow + 1, headers.length).getValues();
    }, "read Raw Data chunk rows " + startRow + "-" + endRow);

    for (let i = 0; i < chunkData.length; i++) {
      const row = chunkData[i];
      const netId = String(row[m["Network ID"]] || "").trim();
      const adv = String(row[m["Advertiser"]] || "").trim();
      const pid = String(row[m["Placement ID"]] || "").trim();
      const campaign = String(row[m["Campaign"]] || "").trim();
      const ps = new Date(row[m["Placement Start Date"]]);
      const pe = new Date(row[m["Placement End Date"]]);
      const rd = new Date(row[m["Report Date"]]);

      if (!netId || !adv || !pid) continue;

      const advLower = adv.toLowerCase();
      if (advLower === "advertiser") continue; // repeated CSV header rows
      if (advLower.indexOf("grand total") !== -1) continue;
      if (ignoreSet.has(advLower) || advLower.indexOf("bidmanager") !== -1) continue;
      if (campaign.indexOf("DART Search") !== -1) continue;

      if (isNaN(rd) || rd < monthStart || rd >= nextMonthStart) continue;

      const rep = resolveRep_(ownerMap, netId, adv) || "Unassigned";
      const networkName = String(networkNameMap[netId] || "Unknown").trim() || "Unknown";
      const pairKey = rep + "|||" + adv;
      if (!pairToPlacementIds[pairKey]) pairToPlacementIds[pairKey] = {};
      pairToPlacementIds[pairKey][pid] = true;

      if (rep === "Unassigned") {
        const unassignedKey = netId + "|||" + networkName + "|||" + adv;
        if (!unassignedPlacementIds[unassignedKey]) unassignedPlacementIds[unassignedKey] = {};
        unassignedPlacementIds[unassignedKey][pid] = true;
      }

      const dedupeKey = netId + "|||" + pid;
      const candidate = [
        netId,
        networkName,
        adv,
        pid,
        String(row[m["Placement"]] || "").trim(),
        campaign,
        ps,
        pe,
        rd,
        rep,
        "TRUE"
      ];
      const existing = dedupe[dedupeKey];
      if (!existing || rd > existing.reportDate) {
        dedupe[dedupeKey] = { reportDate: rd, row: candidate };
      }
      filteredRowCount++;
    }
    Logger.log("    📊 Processed chunk " + startRow + "-" + endRow + ", accumulated " + filteredRowCount + " live placements");
  }

  Logger.log("✅ Filtered data: " + filteredRowCount + " live placements across " + Object.keys(pairToPlacementIds).length + " rep/advertiser pairs");

  const filteredHeaders = [
    "Network ID", "Network Name", "Advertiser", "Placement ID", "Placement", "Campaign",
    "Placement Start Date", "Placement End Date", "Report Date", "Owner (Ops)", "Is Live This Month"
  ];
  const filteredRows = Object.keys(dedupe)
    .map(function(k){ return dedupe[k].row; })
    .sort(function(a, b){
      return String(a[9]).localeCompare(String(b[9]))
        || String(a[2]).localeCompare(String(b[2]))
        || String(a[3]).localeCompare(String(b[3]));
    });

  let filteredSheet = withBackoff_(function(){ return ss.getSheetByName("Raw Data Filtered"); }, "get Raw Data Filtered sheet");
  if (!filteredSheet) {
    filteredSheet = withBackoff_(function(){ return ss.insertSheet("Raw Data Filtered"); }, "insert Raw Data Filtered sheet");
  }
  withBackoff_(function(){ filteredSheet.clearContents(); }, "clear Raw Data Filtered contents");
  withBackoff_(function(){
    filteredSheet.getRange(1, 1, 1, filteredHeaders.length).setValues([filteredHeaders]);
  }, "write Raw Data Filtered headers");
  if (filteredRows.length) {
    writeRowsInChunks_(filteredSheet, 2, 1, filteredRows, filteredHeaders.length, 3000, "write Raw Data Filtered");
    withBackoff_(function(){
      filteredSheet.getRange(2, 7, filteredRows.length, 3).setNumberFormat("yyyy-mm-dd");
    }, "format Raw Data Filtered date columns");
  }

  let pivotSheet = withBackoff_(function(){ return ss.getSheetByName("Live Placements Pivot"); }, "get Live Placements Pivot sheet");
  if (!pivotSheet) {
    pivotSheet = withBackoff_(function(){ return ss.insertSheet("Live Placements Pivot"); }, "insert Live Placements Pivot sheet");
  }
  withBackoff_(function(){ pivotSheet.clearContents(); }, "clear Live Placements Pivot contents");

  const pivotRows = Object.keys(pairToPlacementIds).map(function(key){
      const parts = key.split("|||");
      const owner = parts[0] || "Unassigned";
      const advertiser = parts[1] || "";
      const count = Object.keys(pairToPlacementIds[key]).length;
      return [owner, advertiser, count];
    }).sort(function(a, b){
      return String(a[0]).localeCompare(String(b[0])) || String(a[1]).localeCompare(String(b[1]));
    });

  const unassignedHeaders = ["Network ID", "Friendly Network Name", "Advertiser Name", "Advertiser ID", "Unassigned Placement Count"];
  const unassignedRows = Object.keys(unassignedPlacementIds).map(function(key){
      const parts = key.split("|||");
      const netId = parts[0] || "";
      const networkName = parts[1] || "Unknown";
      const advertiserName = parts[2] || "";
      const advertiserId = "N/A";
      const count = Object.keys(unassignedPlacementIds[key]).length;
      return [netId, networkName, advertiserName, advertiserId, count];
    }).sort(function(a, b){
      return String(a[0]).localeCompare(String(b[0])) || String(a[2]).localeCompare(String(b[2]));
    });

  if (filteredRowCount > 0) {
    const pivotHeaders = ["Owner (Ops)", "Advertiser", "Live Placement Count"];

    withBackoff_(function(){
      pivotSheet.getRange(1, 1, 1, pivotHeaders.length).setValues([pivotHeaders]);
    }, "write Live Placements Pivot headers");

    if (pivotRows.length) {
      writeRowsInChunks_(pivotSheet, 2, 1, pivotRows, pivotHeaders.length, 3000, "write Live Placements Pivot");
    }

    const unassignedStartRow = pivotRows.length + 4;
    withBackoff_(function(){
      pivotSheet.getRange(unassignedStartRow, 1).setValue("Unassigned Placement Coverage");
      pivotSheet.getRange(unassignedStartRow, 1, 1, 4).setFontWeight("bold");
      pivotSheet.getRange(unassignedStartRow + 1, 1, 1, unassignedHeaders.length).setValues([unassignedHeaders]);
    }, "write unassigned coverage headers");

    if (unassignedRows.length) {
      writeRowsInChunks_(pivotSheet, unassignedStartRow + 2, 1, unassignedRows, unassignedHeaders.length, 3000, "write unassigned coverage rows");
    } else {
      withBackoff_(function(){
        pivotSheet.getRange(unassignedStartRow + 2, 1).setValue("No unassigned rows found for current month.");
      }, "write no unassigned coverage message");
    }

    withBackoff_(function(){
      pivotSheet.getRange("A1").setNote("Pivot-style summary: live placement counts by Ops Rep and Advertiser.");
    }, "set Live Placements Pivot note");
  } else {
    withBackoff_(function(){
      pivotSheet.getRange(1, 1).setValue("No live placement rows found for the current month.");
    }, "write no-data pivot message");
  }

  if (showUiAlert) {
    SpreadsheetApp.getUi().alert(
      "Live placements pivot refreshed.\nLive placements found: " + filteredRowCount +
      "\nUnassigned advertiser rows: " + Object.keys(unassignedPlacementIds).length +
      "\nPivot tab: Live Placements Pivot"
    );
  }

  return {
    filteredRowCount: filteredRowCount,
    pivotRows: pivotRows,
    unassignedRows: unassignedRows
  };
}

function writeRowsInChunks_(sheet, startRow, startCol, rows, width, chunkSize, label) {
  if (!rows || !rows.length) return;
  const size = chunkSize || 3000;
  const tag = label || "chunk write";
  for (let i = 0; i < rows.length; i += size) {
    const chunk = rows.slice(i, i + size);
    withBackoff_(function(){
      sheet.getRange(startRow + i, startCol, chunk.length, width).setValues(chunk);
    }, tag + " (rows " + (i + 1) + "-" + (i + chunk.length) + ")");
  }
}

// ====== Auto-add new networks to Networks tab ======
function autoAddNewNetworks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawDataSheet = ss.getSheetByName("Raw Data");
  const networksSheet = ss.getSheetByName("Networks");
  
  if (!rawDataSheet || !networksSheet) {
    Logger.log("autoAddNewNetworks_: Required sheets not found");
    return;
  }
  
  // Get removed networks to exclude them
  const removedNetworks = getRemovedNetworks(ss);
  
  // Get all Network IDs from Raw Data (column A, skip header)
  const rawDataLastRow = rawDataSheet.getLastRow();
  if (rawDataLastRow < 2) {
    Logger.log("autoAddNewNetworks_: No data in Raw Data sheet");
    return;
  }
  
  const rawNetworkIds = rawDataSheet.getRange(2, 1, rawDataLastRow - 1, 1).getValues()
    .map(function(row){ return normalizeNetworkId_(row[0]); })
    .filter(function(id){ return id && id !== "Unknown" && !removedNetworks.has(id); });
  
  // Get unique Network IDs
  const uniqueRawNetIds = {};
  rawNetworkIds.forEach(function(id){ uniqueRawNetIds[id] = true; });
  const uniqueRawNetIdList = Object.keys(uniqueRawNetIds);
  
  if (uniqueRawNetIdList.length === 0) {
    Logger.log("autoAddNewNetworks_: No valid Network IDs in Raw Data");
    return;
  }
  
  // Get existing Network IDs from Networks tab (column A, skip header)
  const networksLastRow = networksSheet.getLastRow();
  const existingNetIds = {};
  
  if (networksLastRow >= 2) {
    const existingData = networksSheet.getRange(2, 1, networksLastRow - 1, 1).getValues();
    existingData.forEach(function(row){
      const id = normalizeNetworkId_(row[0]);
      if (id) existingNetIds[id] = true;
    });
  }
  
  // Find new Network IDs that need to be added
  const newNetIds = uniqueRawNetIdList.filter(function(id){
    return !existingNetIds[id];
  });
  
  if (newNetIds.length === 0) {
    Logger.log("autoAddNewNetworks_: No new networks to add");
    return;
  }
  
  // Append new networks to Networks tab
  // Format: [Network ID (A), "TO BE ADDED" (B)]
  const newRows = newNetIds.map(function(id){
    return [id, "TO BE ADDED"];
  });
  
  const nextRow = networksSheet.getLastRow() + 1;
  networksSheet.getRange(nextRow, 1, newRows.length, 2).setValues(newRows);
  
  Logger.log("autoAddNewNetworks_: Added " + newNetIds.length + " new network(s): " + newNetIds.join(", "));
}

// ====== Chunked QA execution control ======
const QA_CHUNK_ROWS = 3500;
const QA_TIME_BUDGET_MS = 4.2 * 60 * 1000;
const QA_STATE_KEY = 'qa_progress_v2';      // DocumentProperties key

// ====== Chunked EMAIL execution control ======
const EMAIL_TIME_BUDGET_MS = 4.5 * 60 * 1000;
const EMAIL_STATE_KEY = 'email_progress_v1';
const EMAIL_TRIGGER_KEY = 'email_chunk_trigger_id';
const UNASSIGNED_ALERT_CACHE_KEY = 'unassigned_alert_cache_v1';
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
  const funcStart = Date.now();
  Logger.log('      🔍 appendTodaySnapshots_: Adding ' + rowsForSnapshot.length + ' snapshots...');
  if (!rowsForSnapshot.length) return;
  const sh = getPerfAlertCacheSheet_();
  const tz = Session.getScriptTimeZone();
  const todayStr = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
  const out = rowsForSnapshot.map(function(r){ return [todayStr, r.key, r.imp, r.clk]; });
  sh.getRange(sh.getLastRow()+1, 1, out.length, 4).setValues(out);
  Logger.log('      ✅ appendTodaySnapshots_: Complete in ' + fmtMs_(Date.now() - funcStart));
}

// Compact PERF ALERT cache to last N days
function compactPerfAlertCache_(keepDays) {
  const funcStart = Date.now();
  Logger.log('      🔍 compactPerfAlertCache_: Starting (keepDays=' + keepDays + ')...');
  keepDays = keepDays || 35;
  const sh = getPerfAlertCacheSheet_();
  const cutoff = new Date(Date.now() - keepDays*86400000);
  const vals = sh.getDataRange().getValues();
  Logger.log('      📊 Cache has ' + (vals.length - 1) + ' rows');
  if (vals.length <= 1) {
    Logger.log('      ✅ compactPerfAlertCache_: Nothing to compact');
    return;
  }

  const keep = [vals[0]];
  for (let i = 1; i < vals.length; i++) {
    const d = vals[i][0] instanceof Date ? vals[i][0] : new Date(vals[i][0]);
    if (d >= cutoff) keep.push(vals[i]);
  }
  Logger.log('      📊 Keeping ' + (keep.length - 1) + ' rows, removing ' + (vals.length - keep.length) + ' rows');
  sh.clearContents();
  sh.getRange(1,1,keep.length,4).setValues(keep);
  Logger.log('      ✅ compactPerfAlertCache_: Complete in ' + fmtMs_(Date.now() - funcStart));
}

// ---------------------
// Ignore Advertisers sheet
// ---------------------
function loadIgnoreAdvertisers(skipSheetUpdates) {
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
  if (raw && !skipSheetUpdates) {
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

function getUnassignedAlertCache_() {
  const raw = PropertiesService.getDocumentProperties().getProperty(UNASSIGNED_ALERT_CACHE_KEY);
  return raw ? JSON.parse(raw) : null;
}

function saveUnassignedAlertCache_(obj) {
  PropertiesService.getDocumentProperties().setProperty(UNASSIGNED_ALERT_CACHE_KEY, JSON.stringify(obj));
}

function sendUnassignedPlacementCoverageAlert_(options) {
  options = options || {};

  const uniqueEmails = options.overrideRecipients && options.overrideRecipients.length
    ? normalizeRecipientEmails_(options.overrideRecipients)
    : normalizeRecipientEmails_(UNASSIGNED_ALERT_RECIPIENTS);
  if (uniqueEmails.length === 0) {
    return { sent: false, reason: 'No recipients found.', rowCount: 0, recipientCount: 0 };
  }

  const unassignedRows = (options.unassignedRows || []).slice().sort(function(a, b) {
    return String(a[0]).localeCompare(String(b[0])) || String(a[2]).localeCompare(String(b[2]));
  });
  if (!unassignedRows.length) {
    return { sent: false, reason: 'No unassigned placement coverage rows found.', rowCount: 0, recipientCount: uniqueEmails.length };
  }

  const today = new Date();
  const todayKey = Utilities.formatDate(today, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const signature = JSON.stringify(unassignedRows);
  const cache = getUnassignedAlertCache_();

  if (!options.forceSend && cache && cache.date === todayKey && cache.signature === signature) {
    return {
      sent: false,
      reason: 'Unassigned coverage alert already sent for this snapshot today.',
      rowCount: unassignedRows.length,
      recipientCount: uniqueEmails.length
    };
  }

  const htmlRows = unassignedRows.map(function(row) {
    return ''
      + '<tr>'
      + '<td>' + row[0] + '</td>'
      + '<td>' + row[1] + '</td>'
      + '<td>' + row[2] + '</td>'
      + '<td>' + row[3] + '</td>'
      + '<td>' + row[4] + '</td>'
      + '</tr>';
  }).join('');

  const htmlBody = ''
    + '<p><b>ALERT:</b> This advertiser in this network is not mapped yet.</p>'
    + '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;font-size:11px;">'
    + '<tr style="background:#f2f2f2;font-weight:bold;">'
    + '<th>Network ID</th><th>Friendly Network Name</th><th>Advertiser Name</th><th>Advertiser ID</th><th>Unassigned Placement Count</th>'
    + '</tr>'
    + htmlRows
    + '</table>';

  const todayStr = Utilities.formatDate(today, Session.getScriptTimeZone(), 'M/d/yy');
  const subject = 'ALERT - Unmapped advertiser detected - ' + todayStr;

  let sentCount = 0;
  uniqueEmails.forEach(function(addr) {
    try {
      MailApp.sendEmail({ to: addr, subject: subject, htmlBody: htmlBody });
      sentCount++;
      Utilities.sleep(300);
    } catch (err) {
      Logger.log('❌ Failed to email ' + addr + ': ' + err);
    }
  });

  if (sentCount > 0) {
    saveUnassignedAlertCache_({
      date: todayKey,
      signature: signature,
      rowCount: unassignedRows.length,
      sentAt: new Date().toISOString()
    });
  }

  return {
    sent: sentCount > 0,
    reason: sentCount > 0 ? '' : 'All unassigned coverage alert sends failed.',
    rowCount: unassignedRows.length,
    recipientCount: sentCount
  };
}

// ---------------------
// sendPerformanceSpikeAlertIfPre15
// ---------------------
function sendPerformanceSpikeAlertIfPre15(options) {
  options = options || {};
  const today = new Date();
  const dayOfMonth = today.getDate();
  if (!options.skipDateCheck && dayOfMonth >= 15) return { sent: false, reason: 'Blocked by pre-15 date window.', rowCount: 0, recipientCount: 0 }; // Only before 15th

  // Ensures the cache sheet exists before proceeding
  getPerfAlertCacheSheet_();

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Violations");
  const recipientsSheet = ss.getSheetByName("EMAIL LIST");
  if (!sheet) return { sent: false, reason: 'Violations sheet not found.', rowCount: 0, recipientCount: 0 };

  // Recipient list
  const uniqueEmails = getRecipientEmails_(recipientsSheet, options.overrideRecipients);
  if (uniqueEmails.length === 0) return { sent: false, reason: 'No recipients found.', rowCount: 0, recipientCount: 0 };

  const values = sheet.getDataRange().getValues();
  if (values.length <= 1) return { sent: false, reason: 'No violation rows found.', rowCount: 0, recipientCount: uniqueEmails.length };

  const headers = values[0];
  const hMap = {};
  headers.forEach(function(h, i){ hMap[h] = i; });

  const req = [
    "Network ID", "Report Date", "Advertiser", "Campaign",
    "Placement ID", "Placement", "Impressions", "Clicks", "Issue Type", "Details"
  ];
  if (req.some(function(k){ return hMap[k] === undefined; })) return { sent: false, reason: 'Required columns missing from Violations sheet.', rowCount: 0, recipientCount: uniqueEmails.length };

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
  if (!candidateRows.length) {
    compactPerfAlertCache_(35);
    return { sent: false, reason: 'No changed/new performance alert rows matched the current data.', rowCount: 0, recipientCount: uniqueEmails.length };
  }

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
    + buildReplyInstructionsFooterHtml_();

  const todayStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "M/d/yy");
  const subject = 'ALERT – PERFORMANCE (pre-monthly-summary) – ' + todayStr + ' – ' + candidateRows.length + ' changed/new row(s)';

  let sentCount = 0;
  uniqueEmails.forEach(function(addr){
    try {
      MailApp.sendEmail({ to: addr, subject: subject, htmlBody: table });
      sentCount++;
      Utilities.sleep(300);
    } catch (err) {
      Logger.log('❌ Failed to email ' + addr + ': ' + err);
    }
  });

  compactPerfAlertCache_(35);
  return {
    sent: sentCount > 0,
    reason: sentCount > 0 ? '' : 'All performance alert sends failed.',
    rowCount: candidateRows.length,
    recipientCount: sentCount
  };
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

function normalizeNetworkId_(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "number" && isFinite(v)) return String(Math.trunc(v));

  let s = String(v)
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim();

  if (!s) return "";
  s = s.replace(/,/g, "");

  // Normalize common sheet number rendering like "12345.0" to "12345"
  if (/^\d+(?:\.0+)?$/.test(s)) {
    return String(Math.trunc(Number(s)));
  }

  return s;
}

function resolveRep_(ownerMap, netId, adv) {
  const nNetId = normalizeNetworkId_(netId);
  const rawKey  = nNetId + "|||" + String(adv || "").toLowerCase().trim();
  const normKey = nNetId + "|||" + normalizeAdv_(adv || "");
  const rr = ownerMap.byKey[rawKey];
  const nr = ownerMap.byKey[normKey];
  const nb = ownerMap.byNetwork && ownerMap.byNetwork[nNetId];
  return (rr && rr.rep) || (nr && nr.rep) || (nb && nb.rep) || "Unassigned";
}

function loadOwnerMapFromNetworks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("Networks");
  const byKey = {};
  const byNetwork = {};

  if (!sh || sh.getLastRow() < 2) return { byKey: byKey, byNetwork: byNetwork };

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

  if (idIdx === -1 || repIdx === -1) return { byKey: byKey, byNetwork: byNetwork };

  for (let r = 1; r < vals.length; r++) {
    const netId = normalizeNetworkId_(vals[r][idIdx]);
    const adv   = advIdx !== -1 ? String(vals[r][advIdx] || "").trim() : "";
    const theRep = String(vals[r][repIdx] || "").trim();
    if (!netId) continue;

    if (theRep && !byNetwork[netId]) {
      byNetwork[netId] = { rep: theRep };
    }

    if (!adv) continue;

    const rawKey  = netId + "|||" + adv.toLowerCase();
    const normKey = netId + "|||" + normalizeAdv_(adv);
    const payload = { rep: theRep || (byNetwork[netId] && byNetwork[netId].rep) || "Unassigned" };

    byKey[rawKey]  = payload;
    byKey[normKey] = payload;
  }

  return { byKey: byKey, byNetwork: byNetwork };
}

function loadNetworkNameMapFromNetworks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("Networks");
  const map = {};
  if (!sh || sh.getLastRow() < 2) return map;

  const vals = sh.getDataRange().getValues();
  const hdr = vals[0].map(function(h){ return String(h || "").trim().toLowerCase(); });

  function findIdx_(cands) {
    for (let i = 0; i < cands.length; i++) {
      const idx = hdr.indexOf(cands[i]);
      if (idx !== -1) return idx;
    }
    return -1;
  }

  const idIdx = findIdx_(["network id", "network_id", "networkid", "cm360 network id"]);
  let nameIdx = findIdx_(["network name", "network", "name", "friendly name"]);

  // Backward-compatible fallback: many sheets use col B as name.
  if (nameIdx === -1 && vals[0].length >= 2) nameIdx = 1;
  if (idIdx === -1 || nameIdx === -1) return map;

  for (let r = 1; r < vals.length; r++) {
    const id = String(vals[r][idIdx] || "").trim();
    const name = String(vals[r][nameIdx] || "").trim();
    if (!id) continue;
    map[id] = name || "Unknown";
  }

  return map;
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

function isMidFlightDropEnabled_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const networksSheet = ss.getSheetByName("Networks");
  if (!networksSheet) return false; // Default OFF
  
  const raw = String(networksSheet.getRange("I1").getDisplayValue() || "").trim().toUpperCase();
  const enabled = (raw === "ON" || raw === "TRUE" || raw === "YES" || raw === "ENABLED");
  Logger.log("Mid-flight drop detection (from Networks!I1): " + (enabled ? "ENABLED" : "DISABLED") + " (raw='" + raw + "')");
  return enabled;
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
// Monthly Overage Tracking
// ---------------------

/**
 * Logs overage costs for CPC+CPM violations to Monthly Overages sheet (monthly rollup)
 * @param {string} networkId - Network ID
 * @param {string} advertiser - Advertiser name
 * @param {string} placementId - Placement ID
 * @param {string} placementName - Placement name
 * @param {number} overage - Calculated overage amount
 * @param {Date} reportDate - Report date
 */
function logMonthlyOverage_(networkId, advertiser, placementId, placementName, overage, reportDate) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let overageSheet = ss.getSheetByName("Monthly Overages");
    
    // Create sheet if it doesn't exist
    if (!overageSheet) {
      overageSheet = ss.insertSheet("Monthly Overages");
      const headers = ["Month", "Network ID", "Advertiser", "Placement ID", "Placement", "Total Overage", "Last Updated"];
      overageSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      overageSheet.getRange(1, 1, 1, headers.length).setFontWeight("bold");
      overageSheet.setFrozenRows(1);
    } else {
      // Migrate old daily format if needed
      const headerRow = overageSheet.getRange(1, 1, 1, 7).getValues()[0];
      if (String(headerRow[0]) === "Date") {
        const lastRow = overageSheet.getLastRow();
        const data = lastRow > 1 ? overageSheet.getRange(2, 1, lastRow - 1, 7).getValues() : [];
        const rollup = {};
        
        data.forEach(function(row) {
          const month = String(row[1] || "").trim();
          const netId = String(row[2] || "").trim();
          const adv = String(row[3] || "").trim();
          const pid = String(row[4] || "").trim();
          const plc = String(row[5] || "").trim();
          const amt = parseFloat(row[6]) || 0;
          if (!month || !netId || !pid) return;
          const key = month + "|" + netId + "|" + pid;
          if (!rollup[key]) {
            rollup[key] = { month: month, netId: netId, adv: adv, pid: pid, plc: plc, total: 0, lastUpdated: "" };
          }
          rollup[key].total += amt;
          rollup[key].lastUpdated = row[0] || rollup[key].lastUpdated;
        });
        
        const newHeaders = ["Month", "Network ID", "Advertiser", "Placement ID", "Placement", "Total Overage", "Last Updated"];
        overageSheet.clear();
        overageSheet.getRange(1, 1, 1, newHeaders.length).setValues([newHeaders]);
        overageSheet.getRange(1, 1, 1, newHeaders.length).setFontWeight("bold");
        overageSheet.setFrozenRows(1);
        
        const rows = Object.keys(rollup).map(function(key) {
          const r = rollup[key];
          return [r.month, r.netId, r.adv, r.pid, r.plc, r.total, r.lastUpdated];
        });
        if (rows.length) {
          overageSheet.getRange(2, 1, rows.length, newHeaders.length).setValues(rows);
        }
      }
    }
    
    // Format date as YYYY-MM-DD and month as YYYY-MM
    const dateStr = Utilities.formatDate(reportDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
    const monthStr = Utilities.formatDate(reportDate, Session.getScriptTimeZone(), "yyyy-MM");
    
    // Find existing row for this month + network + placement
    const lastRow = overageSheet.getLastRow();
    let rowToUpdate = -1;
    let currentTotal = 0;
    
    if (lastRow > 1) {
      const data = overageSheet.getRange(2, 1, lastRow - 1, 7).getValues();
      for (let i = 0; i < data.length; i++) {
        const rowMonth = String(data[i][0] || "").trim();
        const rowNetwork = String(data[i][1] || "").trim();
        const rowPlacementId = String(data[i][3] || "").trim();
        if (rowMonth === monthStr && rowNetwork === String(networkId) && rowPlacementId === String(placementId)) {
          rowToUpdate = i + 2; // sheet row number
          currentTotal = parseFloat(data[i][5]) || 0;
          break;
        }
      }
    }
    
    const newTotal = currentTotal + overage;
    
    if (rowToUpdate > 0) {
      overageSheet.getRange(rowToUpdate, 3).setValue(advertiser);
      overageSheet.getRange(rowToUpdate, 5).setValue(placementName);
      overageSheet.getRange(rowToUpdate, 6).setValue(newTotal);
      overageSheet.getRange(rowToUpdate, 7).setValue(dateStr);
    } else {
      overageSheet.appendRow([
        monthStr,
        networkId,
        advertiser,
        placementId,
        placementName,
        newTotal,
        dateStr
      ]);
    }
    
    // Apply filter to show only totals >= $10 and sort by Total Overage (desc)
    const updatedLastRow = overageSheet.getLastRow();
    if (updatedLastRow > 1) {
      const range = overageSheet.getRange(1, 1, updatedLastRow, 7);
      if (overageSheet.getFilter()) {
        overageSheet.getFilter().remove();
      }
      range.createFilter();
      const filter = overageSheet.getFilter();
      const criteria = SpreadsheetApp.newFilterCriteria().whenNumberGreaterThanOrEqualTo(10).build();
      filter.setColumnFilterCriteria(6, criteria); // Total Overage column
      range.sort({ column: 6, ascending: false });
    }
    
  } catch (error) {
    Logger.log("Failed to log monthly overage: " + error);
  }
}

/**
 * Gets total overages for a specific month (format: "YYYY-MM")
 * @param {string} monthStr - Month string (e.g., "2026-02")
 * @returns {number} Total overage for the month
 */
function getMonthlyOverageTotal(monthStr) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const overageSheet = ss.getSheetByName("Monthly Overages");
    
    if (!overageSheet || overageSheet.getLastRow() < 2) {
      return 0;
    }
    
    const data = overageSheet.getDataRange().getValues();
    let total = 0;
    
    // Skip header row (index 0)
    for (let i = 1; i < data.length; i++) {
      const month = data[i][0]; // Column A: Month
      const overage = parseFloat(data[i][5]) || 0; // Column F: Total Overage
      
      if (month === monthStr) {
        total += overage;
      }
    }
    
    return total;
  } catch (error) {
    Logger.log("Failed to calculate monthly overage total: " + error);
    return 0;
  }
}

/**
 * Gets current month's total overage
 * @returns {number} Total overage for current month
 */
function getCurrentMonthOverage() {
  const today = new Date();
  const monthStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "yyyy-MM");
  return getMonthlyOverageTotal(monthStr);
}


// ---------------------
// runQAOnly (auto-resume, chunked, lock-guarded)
// ---------------------
function runQAOnly(options) {
  options = options || {};
  const disableChunking = !!options.disableChunking;

  // Prevent overlapping runs
  const dlock = LockService.getDocumentLock();
  if (!dlock.tryLock(5000)) {
    if (!disableChunking) scheduleNextQAChunk_(2);
    return;
  }

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
    const monitoredNetworks = getMonitoredNetworkIds_();
    
    Logger.log("📋 Monitored networks: " + (monitoredNetworks.length > 0 ? monitoredNetworks.join(", ") : "NONE - will process ALL networks"));
    Logger.log("⚙️ runQAOnly mode: " + (disableChunking ? "manual immediate (no chunk limits)" : "chunked auto-resume"));

    compileLPPatternsIfNeeded_();

    let state = getQAState_();
    const totalRows = data.length - 1; // excluding header
    const freshStart = !state || state.totalRows !== totalRows;

    if (freshStart) {
      clearViolations();
      state = {
        session: String(Date.now()),
        next: 2,
        totalRows: totalRows,
        overrideDateIso: options.overrideDate ? new Date(options.overrideDate).toISOString() : ''
      };
      saveQAState_(state);
      cancelQAChunkTrigger_();
    } else if (options.overrideDate && !state.overrideDateIso) {
      state.overrideDateIso = new Date(options.overrideDate).toISOString();
      saveQAState_(state);
    }

    const startTime = Date.now();
    const persistedOverrideDate = state.overrideDateIso ? new Date(state.overrideDateIso) : null;
    const today = options.overrideDate || persistedOverrideDate || new Date();
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
      const netId = normalizeNetworkId_(row[m["Network ID"]]);

      // Filter by monitored networks (if any are specified)
      if (monitoredNetworks.length > 0 && netId && monitoredNetworks.indexOf(netId) === -1) { 
        state.next = r + 1; 
        continue; 
      }

      const advLower = adv ? adv.toLowerCase() : "";
      if (advLower && (ignoreSet.has(advLower) || advLower.includes("bidmanager"))) { state.next = r + 1; continue; }
      if (camp && String(camp).includes("DART Search"))                               { state.next = r + 1; continue; }
      if (adv === "Grand Total:")                                                     { state.next = r + 1; continue; }

      const imp = Number(row[m["Impressions"]] || 0);
      const clk = Number(row[m["Clicks"]] || 0);
      if (imp === 0 && clk === 0) { state.next = r + 1; continue; }

      const ctr = imp > 0 ? (clk / imp) * 100 : 0;

      // Your CPC/CPM formulas
      const cpc = clk * CPC_RATE;
      const cpm = (imp / 1000) * CPM_RATE;

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
        // Calculate overage: cost of extra clicks that exceed impressions
        const overage = (clk - imp) * CPC_RATE;
        details.push("Clicks > impressions with both CPC and CPM charges (CPC = $" + cpc.toFixed(2) + ", Overage = $" + overage.toFixed(2) + ")");
        
        // Track monthly overage
        logMonthlyOverage_(row[m["Network ID"]], row[m["Advertiser"]], row[m["Placement ID"]], row[m["Placement"]], overage, rd);
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
      const key = pid ? ("pid:" + pid) : ("k:" + netId + "|" + camp + "|" + row[m["Placement"]]);
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

      // Respect chunk size & time budget only in chunked mode.
      if (!disableChunking) {
        if (processed >= QA_CHUNK_ROWS) break;
        if ((Date.now() - startTime) >= QA_TIME_BUDGET_MS) break;
      }
    }

    // Persist violation-change snapshot
    cleanupViolationCache_(vMap, today);
    saveViolationChangeMap_(vMap);

    // Write this chunk's rows
    if (resultsChunk.length) {
      Logger.log("📝 Writing " + resultsChunk.length + " violations to sheet...");
      const width = resultsChunk[0].length;
      const startWriteRow = out.getLastRow() + 1;
      out.getRange(startWriteRow, 1, resultsChunk.length, width).setValues(resultsChunk);
      Logger.log("✅ Wrote to rows " + startWriteRow + " to " + (startWriteRow + resultsChunk.length - 1));
    } else {
      Logger.log("⚠️ No violations found in this chunk (processed " + processed + " rows)");
    }

    // Decide: finished or schedule next chunk
    if (state.next >= (data.length)) {
      clearQAState_();
      cancelQAChunkTrigger_();
      Logger.log("✅ runQAOnly complete. Processed all " + totalRows + " data rows.");

      // If a historical backfill is paused at QA stage, nudge it to continue to email.
      const histState = getHistoricalBackfillState_();
      if (histState && histState.stage === 'qa') {
        Logger.log("⏭️ runQAOnly complete: nudging Historical Backfill to continue.");
        scheduleHistoricalBackfillNextChunk_(1);
      }
    } else {
      saveQAState_(state);
      Logger.log("⏳ runQAOnly partial: processed " + processed + " rows this run. Next row index: "
        + state.next + " / " + (data.length - 1));
      if (!disableChunking) {
        scheduleNextQAChunk_(2); // resume soon
      } else {
        Logger.log("⚠️ Immediate mode exited before completion. Re-run Run QA Only (Immediate) to continue.");
      }
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

function sendEmailSummaryChunked_(allowChunking, skipDateCheck, overrideRecipients) {
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

    // Only send on/after the 15th (unless skipDateCheck is true)
    if (!skipDateCheck && today.getDate() < 15) {
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
        allOwners: [],
        overrideRecipients: normalizeRecipientEmails_(overrideRecipients)
      };
      saveEmailState_(state);
      cancelEmailChunkTrigger_();
    } else if (overrideRecipients && overrideRecipients.length) {
      state.overrideRecipients = normalizeRecipientEmails_(overrideRecipients);
      saveEmailState_(state);
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
      
      const stage2Start = Date.now();
      Logger.log('  ⏱️ Building grouped summary HTML...');
      state.cachedHtml.groupedSummary = buildGroupedSummaryHtml_(violations);
      Logger.log('  ✅ Grouped summary complete (' + fmtMs_(Date.now() - stage2Start) + ')');
      
      Logger.log('  ⏱️ Building stale metrics HTML...');
      const staleStart = Date.now();
      state.cachedHtml.staleHtml = buildStaleHtml_(violations);
      Logger.log('  ✅ Stale metrics complete (' + fmtMs_(Date.now() - staleStart) + ')');
      
      
      // COMMENTED OUT: Mid-flight drop detection (restore by uncommenting)
      // Logger.log('  ⏱️ Building mid-flight drop HTML...');
      // const midFlightStart = Date.now();
      // state.cachedHtml.midFlightHtml = today.getDate() < 15 ? generateMidFlightDropHtml_() : '';
      // Logger.log('  ✅ Mid-flight drop complete (' + fmtMs_(Date.now() - midFlightStart) + ')');
      state.cachedHtml.midFlightHtml = ''; // Disabled
      
      Logger.log('📊 Stage 2 total time: ' + fmtMs_(Date.now() - stage2Start));
      
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
      Logger.log('📧 Email Stage 3/4: Building immediate attention section...');
      const stage3Start = Date.now();
      if (!state.cachedHtml.immediateAttention) state.cachedHtml.immediateAttention = '';
      
      // Build owner list if first time
      if (state.allOwners.length === 0) {
        Logger.log('  ⏱️ Analyzing violations for immediate attention (' + (violations.length - 1) + ' rows)...');
        const ownerDataStart = Date.now();
        const ownerData = buildImmediateAttentionData_(violations);
        Logger.log('  ✅ Owner data built: ' + ownerData.owners.length + ' owners in ' + fmtMs_(Date.now() - ownerDataStart));
        state.allOwners = ownerData.owners;
        state.ownerMap = ownerData.perOwner;
        state.processedOwners = [];
      }
      
      // Process owners in chunks
      const remainingOwners = state.allOwners.filter(function(o){ return state.processedOwners.indexOf(o) === -1; });
      
      if (remainingOwners.length > 0) {
        const chunkSize = allowChunking ? MAX_OWNERS_PER_CHUNK : remainingOwners.length;
        const ownersThisChunk = remainingOwners.slice(0, chunkSize);
        
        Logger.log('  ⏱️ Processing ' + ownersThisChunk.length + ' owners (' + remainingOwners.length + ' remaining)...');
        const ownerHtmlStart = Date.now();
        
        const htmlChunk = buildImmediateAttentionHtmlForOwners_(ownersThisChunk, state.ownerMap);
        Logger.log('  ✅ Owner HTML generated in ' + fmtMs_(Date.now() - ownerHtmlStart));
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
      const uniqueEmails = getRecipientEmails_(recipientsSheet, state.overrideRecipients);
      
      if (uniqueEmails.length === 0) {
        Logger.log('⚠️ No recipients found');
        clearEmailState_();
        cancelEmailChunkTrigger_();
        return;
      }

      if (state.overrideRecipients && state.overrideRecipients.length) {
        Logger.log('📧 Test recipient override active: ' + uniqueEmails.join(', '));
      }

      // Assemble email
      const subject = "CM360 CPC/CPM FLIGHT QA – " + Utilities.formatDate(today, Session.getScriptTimeZone(), "M/d/yy");
      let htmlBody = state.cachedHtml.networkSummary +
                     '<p style="font-size:11px;">The below is a table of the following Billing, Delivery, Performance and Cost issues:</p>' +
                     '<div style="font-size:11px;">' + state.cachedHtml.groupedSummary + '</div>' +
                     (state.cachedHtml.immediateAttention ? ('<br/>' + state.cachedHtml.immediateAttention) : '') +
                     (state.cachedHtml.midFlightHtml ? ('<br/>' + state.cachedHtml.midFlightHtml) : '') +
                     '<br/>' + state.cachedHtml.staleHtml +
                     '<hr/>' +
                     buildReplyInstructionsFooterHtml_();

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

  function isPlaceholderNetworkName_(name) {
    const n = String(name || "").trim().toLowerCase();
    return !n || n === "to be added" || n === "unknown";
  }

  function buildNetworkNameMap_() {
    if (!networksSheet) return {};
    const vals = networksSheet.getDataRange().getValues();
    const map = {};
    for (let r = 1; r < vals.length; r++) {
      const idRaw = vals[r][0];
      const name  = String(vals[r][1] == null ? "" : vals[r][1]).replace(/\u00A0/g, " ").trim();
      if (!idRaw) continue;
      const id = normalizeNetworkId_(idRaw);
      if (!id) continue;

      // Prefer real names over placeholders when duplicate Network IDs exist.
      if (!map[id]) {
        map[id] = name;
      } else if (isPlaceholderNetworkName_(map[id]) && !isPlaceholderNetworkName_(name)) {
        map[id] = name;
      }
    }
    return map;
  }
  const networkNameMap = buildNetworkNameMap_();
  
  // Get all networks from the networks sheet (not Monitored Networks)
  // This ensures the summary always includes all networks in your networks tab
  const allNetworks = [];
  if (networksSheet) {
    const networkData = networksSheet.getDataRange().getValues();
    for (let i = 1; i < networkData.length; i++) {
      const id = normalizeNetworkId_(networkData[i][0]);
      const name = String(networkData[i][1] || "").trim();
      if (id) {
        allNetworks.push({ id: id, name: name || "TO BE ADDED" });
      }
    }
  }

  const placementCounts = {};
  const reportPresence = {};
  rawData.slice(1).forEach(function(r){
    const id = normalizeNetworkId_(r[rMap["Network ID"]]);
    if (!id) return;

    reportPresence[id] = true;

    const advertiser = String(r[rMap["Advertiser"]] || "").trim();
    const placementId = String(r[rMap["Placement ID"]] || "").trim();

    if (!placementId) return;
    if (advertiser === "Grand Total:") return;

    placementCounts[id] = (placementCounts[id] || 0) + 1;
  });

  const violationCounts = {};
  violations.slice(1).forEach(function(r){
    const id = normalizeNetworkId_(r[hMap["Network ID"]]);
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

  // Show all networks from the networks sheet
  const allNetworkIds = allNetworks.map(function(n){ return n.id; });
  
  allNetworks
    .sort(function(a, b){ return a.name.localeCompare(b.name); })
    .forEach(function(network){
      const id = network.id, name = network.name;
      const pc = placementCounts[id] || 0;
      const placementDisplay = pc > 0
        ? String(pc)
        : (reportPresence[id] ? "0 - report present" : "0 - no report present today");
      const vc = violationCounts[id] || { "🟥 BILLING":0,"🟦 DELIVERY":0,"🟨 PERFORMANCE":0,"🟩 COST":0 };
      html += '<tr>'
        + '<td>' + id + '</td><td>' + name + '</td><td>' + placementDisplay + '</td>'
        + '<td>' + vc["🟥 BILLING"] + '</td><td>' + vc["🟦 DELIVERY"] + '</td><td>' + vc["🟨 PERFORMANCE"] + '</td><td>' + vc["🟩 COST"] + '</td>'
        + '</tr>';
    });
  html += '</table>';

  // --- Violation type legend ---
  html += '<br/>'
    + '<p><b>What the Violations tab tracks</b></p>'
    + '<table border="0" cellpadding="3" cellspacing="0" style="font-size: 11px; border-collapse: collapse;">'

    + '<tr><td colspan="2" style="padding-top:6px; font-weight:bold;">🟥 BILLING</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">Expired CPC Risk</td>'
    + '<td style="padding-left:8px;">Ended before this month and clicks &gt; impressions.</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">Recently Expired CPC Risk</td>'
    + '<td style="padding-left:8px;">Ended earlier this month and still clicks &gt; impressions.</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">Active CPC Billing Risk</td>'
    + '<td style="padding-left:8px;">Active (report date &le; end date), clicks &gt; impressions, and $CPC &gt; $10.</td></tr>'

    + '<tr><td colspan="2" style="padding-top:6px; font-weight:bold;">🟦 DELIVERY</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">Post-Flight Activity</td>'
    + '<td style="padding-left:8px;">Ended before this month but shows impressions or clicks this month.</td></tr>'

    + '<tr><td colspan="2" style="padding-top:6px; font-weight:bold;">🟨 PERFORMANCE</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">CTR &ge; 90% &amp; CPM &ge; $10</td>'
    + '<td style="padding-left:8px;">Extreme CTR with meaningful CPM spend.</td></tr>'

    + '<tr><td colspan="2" style="padding-top:6px; font-weight:bold;">🟩 COST</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">CPC Only &gt; $10</td>'
    + '<td style="padding-left:8px;">No CPM spend and $CPC &gt; $10.</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">CPM Only &gt; $10</td>'
    + '<td style="padding-left:8px;">No CPC spend and $CPM &gt; $10.</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">CPC+CPM Clicks &gt; Impr &amp; CPC &gt; $10</td>'
    + '<td style="padding-left:8px;">Both CPC &amp; CPM active, clicks &gt; impressions, and $CPC &gt; $10.</td></tr>'
    + '<tr><td style="padding-left:16px; color:#555;">(Low Priority)</td>'
    + '<td style="padding-left:8px;">Kept for audit trail; de-prioritized in email sorting.</td></tr>'

    + '</table><br/>';

  return html;
}

function buildGroupedSummaryHtml_(violations) {
  const hMap = getHeaderMap(violations[0]);
  const groupedCounts = { "🟥 BILLING": {}, "🟦 DELIVERY": {}, "🟨 PERFORMANCE": {}, "🟩 COST": {} };
  
  violations.slice(1).forEach(function(r){
    const issueTypeStr = String(r[hMap["Issue Type"]] || "");
    
    // Skip rows with Low Priority issues
    if (/\(Low Priority\)/i.test(issueTypeStr)) return;
    
    const types = issueTypeStr.split(", ");
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
  const funcStart = Date.now();
  Logger.log('    🔍 buildStaleHtml_: Processing ' + (violations.length - 1) + ' violations...');
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
  Logger.log('    ✅ buildStaleHtml_: Complete in ' + fmtMs_(Date.now() - funcStart) + ' (staleImp=' + staleImp + ', staleClk=' + staleClk + ')');
  
  return "<b>Stale Metrics (this month)</b><ul>"
    + "<li>Placements with no new impressions since last change (≥ " + thresholdDays + " days): " + staleImp + "</li>"
    + "<li>Placements with no new clicks since last change (≥ " + thresholdDays + " days): " + staleClk + "</li>"
    + "</ul>";
}

function buildImmediateAttentionData_(violations) {
  const funcStart = Date.now();
  Logger.log('    🔍 buildImmediateAttentionData_: Starting with ' + (violations.length - 1) + ' violations...');
  const ownerMap = loadOwnerMapFromNetworks_();
  Logger.log('    📋 Loaded owner map with ' + Object.keys(ownerMap).length + ' entries');
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

  let qualified = 0;
  for (let i = 1; i < violations.length; i++) {
    const row = violations[i];
    const q = qualifies_(row);
    if (!q) continue;
    qualified++;

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
    
    if (i % 100 === 0) {
      Logger.log('    ⏱️ Processed ' + i + '/' + (violations.length - 1) + ' rows...');
    }
  }

  const owners = Object.keys(perOwner).sort(function(a,b){ return a.toLowerCase().localeCompare(b.toLowerCase()); });
  Logger.log('    ✅ buildImmediateAttentionData_: Complete in ' + fmtMs_(Date.now() - funcStart) + ' (' + qualified + ' qualified, ' + owners.length + ' owners)');
  
  return { owners: owners, perOwner: perOwner };
}

function buildImmediateAttentionHtmlForOwners_(owners, perOwner) {
  const funcStart = Date.now();
  Logger.log('    🔍 buildImmediateAttentionHtmlForOwners_: Processing ' + owners.length + ' owners...');
  const MAX_ROWS_PER_OWNER = 30;
  const MAX_TOTAL_OWNER_ROWS = 1000;
  
  // Get handled placements
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const handledSheet = ss.getSheetByName("Handled Placements");
  const handledMap = {};
  
  if (handledSheet) {
    const handledData = handledSheet.getDataRange().getValues();
    const hHeaders = handledData[0];
    
    for (let i = 1; i < handledData.length; i++) {
      const row = handledData[i];
      const pid = String(row[hHeaders.indexOf("Placement ID")] || "").trim();
      if (pid) {
        handledMap[pid] = {
          note: String(row[hHeaders.indexOf("Note")] || ""),
          date: String(row[hHeaders.indexOf("Note-Date Last Updated")] || "")
        };
      }
    }
  }
  
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
    
    // Separate handled vs unhandled
    const unhandled = [];
    const handled = [];
    
    arr.forEach(function(item) {
      if (handledMap[item.pid]) {
        handled.push({
          item: item,
          note: handledMap[item.pid].note,
          date: handledMap[item.pid].date
        });
      } else {
        unhandled.push(item);
      }
    });

    const takeUnhandled = Math.min(unhandled.length, MAX_ROWS_PER_OWNER, MAX_TOTAL_OWNER_ROWS - totalRows);
    const takeHandled = Math.min(handled.length, Math.max(0, MAX_TOTAL_OWNER_ROWS - totalRows - takeUnhandled));
    
    const totalShowing = takeUnhandled + takeHandled;
    if (totalShowing <= 0) break;
    totalRows += totalShowing;

    html += "<p><b>" + rep + "</b> (Showing " + totalShowing + " of " + arr.length + ")</p>";
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse: collapse; font-size: 11px;">'
         +  '<tr style="background-color:#f9f9f9;font-weight:bold;">'
         +  '<th>Advertiser</th><th>Campaign</th><th>Placement ID</th><th>Placement</th><th>Impr</th><th>Clicks</th><th>Issue(s)</th><th>Status</th>'
         +  '</tr>';

    // Show unhandled first
    for (let j = 0; j < takeUnhandled; j++) {
      const o = unhandled[j];
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
           +  "<td></td>"
           +  "</tr>";
    }
    
    // Show handled at bottom with note
    for (let j = 0; j < takeHandled; j++) {
      const h = handled[j];
      const o = h.item;
      const campShort = o.camp.length > 40 ? o.camp.substring(0, 40) + "…" : o.camp;
      const plcShort = o.plc.length > 30 ? o.plc.substring(0, 30) + "…" : o.plc;
      html += '<tr style="background-color:#e8f5e9;">'
           +  "<td>" + o.adv + "</td>"
           +  "<td>" + campShort + "</td>"
           +  "<td>" + o.pid + "</td>"
           +  "<td>" + plcShort + "</td>"
           +  "<td>" + o.imp + "</td>"
           +  "<td>" + o.clk + "</td>"
           +  "<td>" + o.issue + "</td>"
           +  '<td style="font-style:italic;">✓ ' + h.note.replace(/\n/g, '<br/>') + '</td>'
           +  "</tr>";
    }
    
    html += "</table><br/>";
    
    if ((i + 1) % 5 === 0) {
      Logger.log('    ⏱️ Processed ' + (i + 1) + '/' + owners.length + ' owners...');
    }
  }
  
  Logger.log('    ✅ buildImmediateAttentionHtmlForOwners_: Complete in ' + fmtMs_(Date.now() - funcStart) + ' (totalRows=' + totalRows + ')');

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
  var pivotResult;
  Logger.log('🚀 runItAll — START @ ' + new Date(runStart).toISOString()
             + ' (approx quota: ' + APPROX_QUOTA_MINUTES + ' min)');

  try {
    // 1) Prep & ingest
    logStep_('trimAllSheetsToData_', function(){ trimAllSheetsToData_(); }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('importDCMReports',     function(){ importDCMReports();      }, runStart, APPROX_QUOTA_MINUTES);
    pivotResult = logStep_('buildFilteredRawDataAndPivot', function(){
      return buildFilteredRawDataAndPivot({ suppressUiAlert: true });
    }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('sendUnassignedPlacementCoverageAlert_', function(){
      return sendUnassignedPlacementCoverageAlert_({
        unassignedRows: pivotResult ? pivotResult.unassignedRows : []
      });
    }, runStart, APPROX_QUOTA_MINUTES);

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
  var pivotResult;
  const isAuto = !isManualRun_();
  
  Logger.log('🚀 runItAllMorning — START @ ' + new Date(runStart).toISOString()
             + ' (approx quota: ' + APPROX_QUOTA_MINUTES + ' min)');

  try {
    // 1) Prep & ingest
    logStep_('trimAllSheetsToData_', function(){ trimAllSheetsToData_(); }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('importDCMReports',     function(){ importDCMReports();      }, runStart, APPROX_QUOTA_MINUTES);
    pivotResult = logStep_('buildFilteredRawDataAndPivot', function(){
      return buildFilteredRawDataAndPivot({ suppressUiAlert: true });
    }, runStart, APPROX_QUOTA_MINUTES);
    logStep_('sendUnassignedPlacementCoverageAlert_', function(){
      return sendUnassignedPlacementCoverageAlert_({
        unassignedRows: pivotResult ? pivotResult.unassignedRows : []
      });
    }, runStart, APPROX_QUOTA_MINUTES);

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
  const funcStart = Date.now();
  Logger.log('    🔍 generateMidFlightDropHtml_: Starting...');
  
  // Check if mid-flight drop detection is enabled
  if (!isMidFlightDropEnabled_()) {
    Logger.log('    ⚠️ generateMidFlightDropHtml_: DISABLED (set Networks!I1 to "ON" to enable)');
    return "";
  }
  
  const today = new Date();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  const rawSheet = ss.getSheetByName("Raw Data");
  if (!rawSheet) {
    Logger.log('    ⚠️ generateMidFlightDropHtml_: No Raw Data sheet');
    return "";
  }

  Logger.log('    📊 Reading Raw Data sheet...');
  const values = rawSheet.getDataRange().getValues();
  Logger.log('    📊 Raw Data has ' + (values.length - 1) + ' rows');
  if (values.length <= 1) return "";

  const headers = values[0];
  const hMap = {};
  headers.forEach(function(h, i){ hMap[h] = i; });

  const req = [
    "Network ID", "Advertiser", "Placement ID", "Placement", "Campaign",
    "Placement Start Date", "Placement End Date", "Impressions", "Clicks"
  ];
  if (req.some(function(k){ return hMap[k] === undefined; })) {
    Logger.log('    ⚠️ generateMidFlightDropHtml_: Missing required columns');
    return "";
  }

  Logger.log('    ⏱️ Getting drop threshold...');
  const threshold = getDropThreshold_();
  Logger.log('    📊 Drop threshold: ' + threshold + '%');
  const snapshots = [];
  const dropAlerts = [];

  Logger.log('    ⏱️ Processing rows for mid-flight drops...');
  let processedRows = 0;
  let checkedRows = 0;
  values.slice(1).forEach(function(r){
    processedRows++;
    if (processedRows % 1000 === 0) {
      Logger.log('    ⏱️ Processed ' + processedRows + '/' + (values.length - 1) + ' rows (found ' + checkedRows + ' mid-flight)...');
    }
    
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
    const cpm = imp > 0 ? ((imp / 1000) * CPM_RATE) : 0;
    
    // Filter: Must have CPM >= $10 OR CPC >= $10
    if (cpc < 10 && cpm < 10) return;
    
    checkedRows++;

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

  Logger.log('    ⏱️ Appending ' + snapshots.length + ' snapshots...');
  appendTodaySnapshots_(snapshots);
  Logger.log('    ⏱️ Compacting performance alert cache...');
  compactPerfAlertCache_(35);
  Logger.log('    ✅ Found ' + dropAlerts.length + ' drop alerts');

  if (!dropAlerts.length) {
    Logger.log('    ✅ generateMidFlightDropHtml_: Complete in ' + fmtMs_(Date.now() - funcStart) + ' (no alerts)');
    return "";
  }

  Logger.log('    ⏱️ Building HTML for ' + dropAlerts.length + ' alerts...');
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
  
  Logger.log('    ✅ generateMidFlightDropHtml_: Complete in ' + fmtMs_(Date.now() - funcStart));
  return html;
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

  // Don't clear violations here - runQAOnly() will do it when it detects freshStart
  Logger.log('Processing ' + (data.length - 1) + ' rows immediately (no chunk limits)...');

  // Call the regular QA engine in immediate mode (disables chunk caps/scheduling).
  runQAOnly({ disableChunking: true });
  
  const duration = Date.now() - startTime;
  Logger.log('✅ runQAOnlyImmediate completed in ' + fmtMs_(duration));
}

function sendEmailSummaryImmediate() {
  const startTime = Date.now();
  Logger.log('🏃 sendEmailSummaryImmediate - Manual mode (will auto-chunk if needed)');
  
  // Clear any existing state
  clearEmailState_();
  cancelEmailChunkTrigger_();
  
  // Call the chunked version - it will auto-chunk if it runs out of time
  try {
    sendEmailSummaryChunked_(true); // Changed to true to allow chunking if needed
    const duration = Date.now() - startTime;
    Logger.log('✅ sendEmailSummaryImmediate completed in ' + fmtMs_(duration));
  } catch (e) {
    Logger.log('❌ sendEmailSummaryImmediate failed: ' + e.message);
    throw e;
  }
}

function sendEmailNow() {
  const startTime = Date.now();
  Logger.log('🏃 sendEmailNow - FORCE SEND (bypasses date check)');
  
  // Clear any existing state
  clearEmailState_();
  cancelEmailChunkTrigger_();
  
  // Call the chunked version with date check disabled
  try {
    sendEmailSummaryChunked_(true, true); // true, true = allow chunking, skip date check
    const duration = Date.now() - startTime;
    Logger.log('✅ sendEmailNow completed in ' + fmtMs_(duration));
  } catch (e) {
    Logger.log('❌ sendEmailNow failed: ' + e.message);
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

// ---------------------
// showCurrentMonthOverage - Display current month's overage total
// ---------------------
function showCurrentMonthOverage() {
  const ui = SpreadsheetApp.getUi();
  
  try {
    const today = new Date();
    const monthStr = Utilities.formatDate(today, Session.getScriptTimeZone(), "MMMM yyyy");
    const monthKey = Utilities.formatDate(today, Session.getScriptTimeZone(), "yyyy-MM");
    
    const total = getMonthlyOverageTotal(monthKey);
    
    let message = '💰 Current Month Overage Report\n\n';
    message += 'Month: ' + monthStr + '\n';
    message += 'Total Overage: $' + total.toFixed(2) + '\n\n';
    message += 'This represents the extra cost charged due to CPC+CPM violations\n';
    message += '(placements with clicks > impressions where both CPC and CPM are billed).\n\n';
    message += 'View the "Monthly Overages" sheet for detailed breakdown.';
    
    ui.alert('Monthly Overage Total', message, ui.ButtonSet.OK);
  } catch (error) {
    ui.alert('Error', 'Failed to calculate monthly overage: ' + error, ui.ButtonSet.OK);
  }
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

/**
 * 🔍 DEBUG: Check what's being filtered out in QA logic
 * Run this to see why no violations are being detected
 */
function debugQAFiltering() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName("Raw Data");
  const ignoreSheet = ss.getSheetByName("Advertisers to ignore");
  
  if (!rawSheet) {
    Logger.log("❌ No Raw Data sheet found");
    return;
  }
  
  // Get ignored advertisers list
  let ignoredAdvs = [];
  if (ignoreSheet) {
    const ignoreData = ignoreSheet.getDataRange().getValues();
    ignoredAdvs = ignoreData.slice(1).map(row => String(row[0] || "").trim().toUpperCase()).filter(x => x);
    Logger.log("📋 Ignored advertisers: " + ignoredAdvs.length + " total");
    Logger.log("   First 10: " + ignoredAdvs.slice(0, 10).join(", "));
  } else {
    Logger.log("⚠️ No 'Advertisers to ignore' sheet found");
  }
  
  const values = rawSheet.getDataRange().getValues();
  Logger.log("📊 Total raw rows: " + (values.length - 1));
  
  const headers = values[0];
  const hMap = {};
  headers.forEach((h, i) => { hMap[h] = i; });
  
  Logger.log("📋 Headers: " + headers.join(", "));
  
  const counters = {
    total: 0,
    ignored_advertiser: 0,
    dart_search: 0,
    grand_total: 0,
    zero_metrics: 0,
    passed_filters: 0
  };
  
  // Test first 100 rows
  const testRows = values.slice(1, Math.min(101, values.length));
  Logger.log("\n🧪 Testing first " + testRows.length + " rows:");
  
  testRows.forEach((r, idx) => {
    counters.total++;
    const adv = String(r[hMap["Advertiser"]] || "");
    const camp = String(r[hMap["Campaign"]] || "");
    const imp = Number(r[hMap["Impressions"]] || 0);
    const clk = Number(r[hMap["Clicks"]] || 0);
    
    // Check filters
    const advUpper = adv.toUpperCase();
    
    if (ignoredAdvs.some(ig => advUpper.includes(ig))) {
      counters.ignored_advertiser++;
      if (idx < 5) Logger.log(`   Row ${idx + 2}: ❌ Ignored advertiser: "${adv}"`);
      return;
    }
    
    if (advUpper.includes("DART SEARCH") || camp.toUpperCase().includes("DART SEARCH")) {
      counters.dart_search++;
      if (idx < 5) Logger.log(`   Row ${idx + 2}: ❌ DART Search: "${adv}" / "${camp}"`);
      return;
    }
    
    if (camp.toUpperCase().includes("GRAND TOTAL")) {
      counters.grand_total++;
      if (idx < 5) Logger.log(`   Row ${idx + 2}: ❌ Grand Total: "${camp}"`);
      return;
    }
    
    if (imp === 0 && clk === 0) {
      counters.zero_metrics++;
      if (idx < 5) Logger.log(`   Row ${idx + 2}: ❌ Zero metrics`);
      return;
    }
    
    counters.passed_filters++;
    if (idx < 5) {
      Logger.log(`   Row ${idx + 2}: ✅ Passed - Adv: "${adv}", Imp: ${imp}, Clk: ${clk}`);
    }
  });
  
  Logger.log("\n📊 FILTERING RESULTS (first 100 rows):");
  Logger.log("   Total: " + counters.total);
  Logger.log("   ❌ Ignored advertiser: " + counters.ignored_advertiser);
  Logger.log("   ❌ DART Search: " + counters.dart_search);
  Logger.log("   ❌ Grand Total: " + counters.grand_total);
  Logger.log("   ❌ Zero metrics: " + counters.zero_metrics);
  Logger.log("   ✅ Passed filters: " + counters.passed_filters);
  
  const passRate = (counters.passed_filters / counters.total * 100).toFixed(1);
  Logger.log("\n📈 Pass rate: " + passRate + "%");
  
  if (counters.passed_filters === 0) {
    Logger.log("\n🚨 CRITICAL: NO ROWS PASSED FILTERS!");
    Logger.log("   Check 'Advertisers to ignore' sheet - might be over-filtering");
  }
}

/**
 * 🔍 DEBUG: Count rows with non-zero metrics
 */
function countNonZeroRows() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName("Raw Data");
  
  if (!rawSheet) {
    Logger.log("❌ No Raw Data sheet found");
    return;
  }
  
  const values = rawSheet.getDataRange().getValues();
  const headers = values[0];
  const hMap = {};
  headers.forEach((h, i) => { hMap[h] = i; });
  
  let nonZeroCount = 0;
  let totalRows = values.length - 1;
  
  values.slice(1).forEach(r => {
    const imp = Number(r[hMap["Impressions"]] || 0);
    const clk = Number(r[hMap["Clicks"]] || 0);
    
    if (imp > 0 || clk > 0) {
      nonZeroCount++;
    }
  });
  
  Logger.log("📊 Total raw rows: " + totalRows);
  Logger.log("✅ Rows with impressions > 0 OR clicks > 0: " + nonZeroCount);
  Logger.log("📈 Percentage: " + (nonZeroCount / totalRows * 100).toFixed(1) + "%");
  
  if (nonZeroCount === 0) {
    Logger.log("🚨 CRITICAL: ALL ROWS HAVE ZERO METRICS!");
  }
}

/**
 * 🔍 DEBUG: Run QA on first 100 rows with detailed logging
 */
function debugQALogic() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName("Raw Data");
  const ignoreSheet = ss.getSheetByName("Advertisers to ignore");
  
  if (!rawSheet) {
    Logger.log("❌ No Raw Data sheet found");
    return;
  }
  
  const values = rawSheet.getDataRange().getValues();
  const headers = values[0];
  const hMap = {};
  headers.forEach((h, i) => { hMap[h] = i; });
  
  // Get ignored advertisers
  let ignoredAdvs = [];
  if (ignoreSheet) {
    const ignoreData = ignoreSheet.getDataRange().getValues();
    ignoredAdvs = ignoreData.slice(1).map(row => String(row[0] || "").trim().toUpperCase()).filter(x => x);
  }
  
  Logger.log("🧪 Testing QA logic on first 100 rows...\n");
  
  const testRows = values.slice(1, Math.min(101, values.length));
  let violationCount = 0;
  
  testRows.forEach((row, idx) => {
    const adv = String(row[hMap["Advertiser"]] || "");
    const camp = String(row[hMap["Campaign"]] || "");
    const imp = Number(row[hMap["Impressions"]] || 0);
    const clk = Number(row[hMap["Clicks"]] || 0);
    const rd = row[hMap["Report Date"]];
    const ps = row[hMap["Placement Start Date"]];
    const pe = row[hMap["Placement End Date"]];
    
    // Check filters
    const advUpper = adv.toUpperCase();
    if (ignoredAdvs.some(ig => advUpper.includes(ig))) return;
    if (advUpper.includes("DART SEARCH") || camp.toUpperCase().includes("DART SEARCH")) return;
    if (camp.toUpperCase().includes("GRAND TOTAL")) return;
    if (imp === 0 && clk === 0) return;
    
    // Calculate metrics
    const ctr = (imp > 0) ? (clk / imp * 100) : 0;
    const cpc = (clk > 0) ? (clk * CPC_RATE) : 0;
    const cpm = (imp > 0) ? ((imp / 1000) * CPM_RATE) : 0;
    
    let violations = [];
    
    // Check for CTR violation
    if (ctr >= 90 && cpm >= 10) {
      violations.push("CTR=" + ctr.toFixed(2) + "%, CPM=$" + cpm.toFixed(2));
    }
    
    // Check for CPC violation
    if (cpc > 0 && cpm === 0 && cpc > 10) {
      violations.push("CPC-only=$" + cpc.toFixed(2));
    }
    
    // Check for CPM violation
    if (cpm > 0 && cpc === 0 && cpm > 10) {
      violations.push("CPM-only=$" + cpm.toFixed(2));
    }
    
    // Check for clicks > impressions
    if (cpc > 0 && cpm > 0 && clk > imp && cpc > 10) {
      violations.push("Clicks>Impr CPC=$" + cpc.toFixed(2));
    }
    
    if (violations.length > 0) {
      violationCount++;
      if (violationCount <= 10) {
        Logger.log("Row " + (idx + 2) + ": 🚨 VIOLATION");
        Logger.log("   Adv: " + adv.substring(0, 50));
        Logger.log("   Imp: " + imp + ", Clk: " + clk);
        Logger.log("   CTR: " + ctr.toFixed(2) + "%, CPC: $" + cpc.toFixed(2) + ", CPM: $" + cpm.toFixed(2));
        Logger.log("   Issues: " + violations.join(", "));
        Logger.log("");
      }
    }
  });
  
  Logger.log("[RESULTS]");
  Logger.log("   Rows tested: " + testRows.length);
  Logger.log("   Violations found: " + violationCount);
  Logger.log("   Violation rate: " + (violationCount / testRows.length * 100).toFixed(1) + "%");
  
  if (violationCount === 0) {
    Logger.log("\nNO VIOLATIONS FOUND - checking thresholds:");
    Logger.log("   CTR threshold: >= 90% AND CPM >= $10");
    Logger.log("   CPC threshold: > $10 (CPC-only)");
    Logger.log("   CPM threshold: > $10 (CPM-only)");
  }
}
