function importCampaignIdMapping() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Campaign ID Mapping Doc") || ss.insertSheet("Campaign ID Mapping Doc");
  sheet.clearContents().getRange(1, 1, 1, 4).setValues([["Network ID", "Campaign ID", "Campaign Name", "Report Date"]]);

  const label = "Campaign ID Mapping";
  const today = new Date();
  const formattedToday = Utilities.formatDate(today, Session.getScriptTimeZone(), "yyyy/MM/dd");
  const threads = GmailApp.search(`label:"${label}" after:${formattedToday}`);

  let allRows = [];

  threads.forEach(thread => {
    thread.getMessages().forEach(message => {
      const reportDate = Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), "yyyy-MM-dd");

      message.getAttachments().forEach(att => {
        const networkId = extractNetworkId(att.getName());

        if (att.getContentType() === "text/csv" || att.getName().endsWith(".csv")) {
          allRows = allRows.concat(parseCampaignCsv(att.getDataAsString(), networkId, reportDate));
        } else if (att.getContentType() === "application/zip") {
          Utilities.unzip(att.copyBlob()).forEach(file => {
            if (file.getContentType() === "text/csv" || file.getName().endsWith(".csv")) {
              const netId = extractNetworkId(file.getName());
              allRows = allRows.concat(parseCampaignCsv(file.getDataAsString(), netId, reportDate));
            }
          });
        }
      });
    });
  });

  if (allRows.length) {
    sheet.getRange(2, 1, allRows.length, 4).setValues(allRows);
  }
}

function parseCampaignCsv(csvContent, networkId, reportDate) {
  const lines = csvContent.split("\n").map(l => l.trim());

  const startIndex = lines.findIndex(line => {
    const cols = line.split(",");
    return cols.includes("Campaign ID") && cols.includes("Campaign");
  });

  if (startIndex < 0) {
    Logger.log("No valid header line found.");
    return [];
  }

  Logger.log(`Found header line at index: ${startIndex}`);

  const parsed = Utilities.parseCsv(lines.slice(startIndex).join("\n"));
  const headers = parsed[0];
  const data = parsed.slice(1);

  const headerMap = {};
  headers.forEach((h, i) => headerMap[h.trim()] = i);

  const campaignIdIdx = headerMap["Campaign ID"];
  const campaignNameIdx = headerMap["Campaign"];

  if (campaignIdIdx == null || campaignNameIdx == null) {
    Logger.log("Required headers not found.");
    return [];
  }

  const rows = data
  .filter(row => {
    const campaignId = row[campaignIdIdx]?.toString().trim().toLowerCase();
    const campaignName = row[campaignNameIdx]?.toString().trim().toLowerCase();
    const isGrandTotal = (campaignId && campaignId.includes("grand total")) ||
                         (campaignName && campaignName.includes("grand total"));
    return row[campaignIdIdx] && row[campaignNameIdx] && !isGrandTotal;
  })
  .map(row => [networkId, row[campaignIdIdx], row[campaignNameIdx], reportDate]);


  Logger.log(`Parsed ${rows.length} campaign rows from network ${networkId}.`);

  return rows;
}



function start() {
  importCampaignIdMapping();
}
