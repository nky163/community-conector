var cc = DataStudioApp.createCommunityConnector();
var scriptProperties = PropertiesService.getScriptProperties();

var SERVICE_ACCOUNT_CREDS = 'SERVICE_ACCOUNT_CREDS';
var SERVICE_ACCOUNT_KEY = 'private_key';
var SERVICE_ACCOUNT_EMAIL = 'client_email';
var BILLING_PROJECT_ID = 'project_id';

function isAdminUser() {
  return true;
}

function getConfig(request) {
  var config = cc.getConfig();
  
  config
  .newTextArea()
  .setId('query')
  .setName('SELECT Query')
  .setHelpText('SELECT xxx, xxx, xxx , ... FROM xxx')
  .setPlaceholder('SELECT xxx, xxx, xxx , ... FROM xxx');
  
  config.setDateRangeRequired(true);
  config.setIsSteppedConfig(false);

  return config.build();
}

function getFields(response) {
  var fields = cc.getFields();
  var types = cc.FieldType;
  
  response.schema.fields.forEach(function(field) {
    fields.newDimension()
      .setId(field.name)
      .setName(field.name)
      .setType(types.TEXT);
  });
  
  return fields;
}

function getSchema(request) {
  
  var serviceAccountCreds = getServiceAccountCreds();
  var billingProjectId = serviceAccountCreds[BILLING_PROJECT_ID];
  
  var url = "https://bigquery.googleapis.com/bigquery/v2/projects/" + billingProjectId + "/queries";
  
  var response = sendRequest(url, {
    "method": "POST",
    "muteHttpExceptions": true,
    "contentType": "application/json",
    "payload": JSON.stringify({
      "query": request.configParams.query + " LIMIT 0",
      "useLegacySql": false
    }),
    "headers": { Authorization: "Bearer " + getOauthService().getAccessToken() }
  });
  
  return {schema: getFields(response).build()};
}

function sendRequest(url, fetchOptions) {
  return JSON.parse(UrlFetchApp.fetch(url, fetchOptions));
}



/**
 * Copy the entire credentials JSON file from creating a service account in GCP.
 */
function getServiceAccountCreds() {
  return JSON.parse(scriptProperties.getProperty(SERVICE_ACCOUNT_CREDS));
}

function getOauthService() {
  var serviceAccountCreds = getServiceAccountCreds();
  var serviceAccountKey = serviceAccountCreds[SERVICE_ACCOUNT_KEY];
  var serviceAccountEmail = serviceAccountCreds[SERVICE_ACCOUNT_EMAIL];
  var service = OAuth2.createService('RowLevelSecurity')
  .setAuthorizationBaseUrl('https://accounts.google.com/o/oauth2/auth')
  .setTokenUrl('https://accounts.google.com/o/oauth2/token')
  .setPrivateKey(serviceAccountKey)
  .setIssuer(serviceAccountEmail)
  .setPropertyStore(scriptProperties)
  .setCache(CacheService.getScriptCache())
  .setScope(['https://www.googleapis.com/auth/bigquery.readonly']);
  service.getLastError();
  return service;
}

// var BASE_SQL = 'SELECT id, key, value FROM `pristine-setup-339708.sample_dataset.sample_table`';
  //'SELECT d.region, d.sales, d.date ' +
  //'FROM `datastudio-solutions.row_level_security.data` d ' +
  //'INNER JOIN `datastudio-solutions.row_level_security.access` a ' +
  //'ON d.region = a.region ' +
  //'where a.email=@email';

function getData(request) {
  var accessToken = getOauthService().getAccessToken();
  
  var serviceAccountCreds = getServiceAccountCreds();
  var billingProjectId = serviceAccountCreds[BILLING_PROJECT_ID];
//  var email = Session.getEffectiveUser().getEmail();

  var bqTypes = DataStudioApp.createCommunityConnector().BigQueryParameterType;

  return cc
    .newBigQueryConfig()
    .setAccessToken(accessToken)
    .setBillingProjectId(billingProjectId)
    .setUseStandardSql(true)
    // .setQuery(BASE_SQL)
    .setQuery(request.configParams.query)
  //  .addQueryParameter('email', bqTypes.STRING, email)
    .build();
}
