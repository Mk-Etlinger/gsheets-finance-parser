/**
 * gsheets.js
 * Parses a CSV and inserts it into Google Sheets
 */
const fs = require('fs');
const path = require("path")
const { GoogleSpreadsheet } = require('google-spreadsheet');
const Papa = require('papaparse');

const config = require('./config.js');
const { sheetId, apiCredsPath, serviceNameConfigKey } = config
const currentServiceConfig = config[serviceNameConfigKey];

const normalizeHeadersFunctions = {
  schwab: mapSchwabHeadersToSheetHeaders,
  capitalOne: mapCapitalOneHeadersToSheetHeaders,
}

if (!fs.existsSync(apiCredsPath)) {
  throw new Error(`
    You must provide API creds! 
    See google-spreadsheet docs: https://theoephraim.github.io/node-google-spreadsheet/#/getting-started/authentication?id=service-account`
  );
  process.exit();
}

const creds = require(apiCredsPath);

// TODO: Write tests
// Tests, this script can:
// 1. fetch a google sheet
// 2. log rows and headers
// 3. ingest a csv
// 4. transform a csv and insert into google sheet
// 5. stretch goal: can login to CC/Bank and download csv, tranform and insert into sheet
insertCsvToGoogleSheet(sheetId, creds);

/**
 * insertCsvToGoogleSheet
 * Handles inserting n rows into google sheets
 * @param {string} sheetId the id of the google sheet to which you're inserting data
 * @return {void}
 */
async function insertCsvToGoogleSheet(sheetId) {
  const sheet = await getGoogleSheet(sheetId);
  const dataToInsert = normalizeCsv();
  
  await sheet.addRows(dataToInsert, { insert: true });
};


/**
 * normalizeCsv
 * Handles normalizing data from a csv to be inserted into a google sheet
 * @return {object[]} An array of normalized objects
 */
function normalizeCsv() {
  // TODO: Do we keep config global? Or use a class?
  const csvStream = fs.createReadStream(currentServiceConfig.csvPath);
  const data = [];

  Papa.parse(csvStream, {
    header: true,
    step: async function(result) {
      // TODO: Make map function dynamic/able to handle different bank's csvs
      const normalizeHeadersFunction = normalizeHeadersFunctions[serviceNameConfigKey];
      const normalizedData = normalizeHeadersFunction(result.data);
      console.log('Parsing row: ', normalizedData);

      data.push(normalizedData);
    },
    error: function(error) {
      console.error(error);
    },
    complete: async function(results, file) {
      console.log('parsing complete');

      const csv = Papa.unparse(data);
      const filename = path.basename(currentServiceConfig.csvPath, '.csv');

      fs.writeFileSync(`./${filename}_transformed.csv`, csv);
    }
  });

  return data;
}

/**
 * mapCapitalOneHeadersToSheetHeaders
 * Handles normalizing data from csv to google sheet
 * @param {Object[]} data
 * @return {Object[]} normalized data
 */
function mapCapitalOneHeadersToSheetHeaders(data) {
  const normalizedData = {}
  for (const [key, value] of Object.entries(data)) {
    const keyNormalized = currentServiceConfig.headerNormalization[key];
    let valueCategorized;

    if (!keyNormalized) {
      normalizedData[key] = value;
    }
    if (key === 'Category') {
      valueCategorized = currentServiceConfig.categorize[value];
    }
    if (value === '' && (key === 'Debit' || key === 'Credit')) {
      continue;
    }
    
    normalizedData[keyNormalized] = valueCategorized || value;
  };

  return normalizedData;
}

/**
 * mapSchwabHeadersToSheetHeaders
 * Handles normalizing data from csv to google sheet
 * @param {Object[]} data
 * @return {Object[]} normalized data
 */
function mapSchwabHeadersToSheetHeaders(data) {
  const normalizedData = {}
  console.log('shwab row: ', data);
  
  for (let [key, value] of Object.entries(data)) {
    const keyNormalized = currentServiceConfig.headerNormalization[key];
    let valueCategorized;

    if (key === 'Check #') {
      continue;
    }
    if (key === 'Type') {
      value = normalizedData.Timestamp;
    }
    if (key === 'RunningBalance') {
      value = normalizedData['Item'];
      valueCategorized = currentServiceConfig.categorize[value];
    }
    if (!keyNormalized) {
      normalizedData[key] = value;
    }
    if (value === '' && (key === 'Withdrawal (-)' || key === 'Deposit (+)')) {
      continue;
    }
    
    normalizedData[keyNormalized] = valueCategorized || value;
  };

  return normalizedData;
}

/**
 * getGoogleSheet
 * Handles fetching a Google Spreadsheet by id
 * @param {object} args are passed in from the CLI flags like --sheetId
 * @return {object} a sheet translated to a JSON object
 */
async function getGoogleSheet(sheetId) {
  const doc = new GoogleSpreadsheet(sheetId)
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo();

  const sheet = doc.sheetsByIndex.find(sheet => sheet.title === config.sheetTitle);

  return sheet;
}
