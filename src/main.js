const cc = DataStudioApp.createCommunityConnector();
const DEFAULT_PACKAGE = 'googleapis';

// [START get_config]
// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {
    let config = cc.getConfig();

    config
        .newInfo()
        .setId('instructions')
        .setText(
            'Enter npm package names to fetch their download count. An invalid or blank entry will revert to the default value.'
        );

    config
        .newTextInput()
        .setId('package')
        .setName(
            'Enter a single package name or multiple names separated by commas (no spaces!)'
        )
        .setHelpText('e.g. "googleapis" or "package,somepackage,anotherpackage"')
        .setPlaceholder(DEFAULT_PACKAGE)
        .setAllowOverride(true);

    config.setDateRangeRequired(true);

    return config.build();
}

function getAuthType() {
    let cc = DataStudioApp.createCommunityConnector();
    return cc.newAuthTypeResponse()
        .setAuthType(cc.AuthType.USER_PASS)
        .setHelpUrl('https://www.example.org/connector-auth-help')
        .build();
}


// [END get_config]

// [START get_schema]
function getFields() {
    let fields = cc.getFields();
    let types = cc.FieldType;
    let aggregations = cc.AggregationType;

    fields
        .newDimension()
        .setId('packageName')
        .setName('Package')
        .setType(types.TEXT);

    fields
        .newDimension()
        .setId('day')
        .setName('Date')
        .setType(types.YEAR_MONTH_DAY);

    fields
        .newMetric()
        .setId('downloads')
        .setName('Downloads')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);

    return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
    return {schema: getFields().build()};
}

// [END get_schema]

// [START get_data]
// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
    request.configParams = validateConfig(request.configParams);

    let requestedFields = getFields().forIds(
        request.fields.map(function (field) {
            return field.name;
        })
    );

    try {
        let apiResponse = fetchDataFromApi(request);
        let normalizedResponse = normalizeResponse(request, apiResponse);
        let data = getFormattedData(normalizedResponse, requestedFields);
    } catch (e) {
        cc.newUserError()
            .setDebugText('Error fetching data from API. Exception details: ' + e)
            .setText(
                'The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists.'
            )
            .throwException();
    }

    return {
        schema: requestedFields.build(),
        rows: data
    };
}

/**
 * Gets response for UrlFetchApp.
 *
 * @param {Object} request Data request parameters.
 * @returns {GoogleAppsScript.URL_Fetch.HTTPResponse} Response text for UrlFetchApp.
 */
function fetchDataFromApi(request) {
    const url = [
        'https://api.npmjs.org/downloads/range/',
        request.dateRange.startDate,
        ':',
        request.dateRange.endDate,
        '/',
        request.configParams.package
    ].join('');

    return UrlFetchApp.fetch(url);
}

/**
 * Parses response string into an object. Also standardizes the object structure
 * for single vs multiple packages.
 *
 * @param {Object} request Data request parameters.
 * @param {string} responseString Response from the API.
 * @return {Object} Contains package names as keys and associated download count
 *     information(object) as values.
 */
function normalizeResponse(request, responseString) {
    let response = JSON.parse(responseString);
    let package_list = request.configParams.package.split(',');
    let mapped_response = {};

    if (package_list.length == 1) {
        mapped_response[package_list[0]] = response;
    } else {
        mapped_response = response;
    }

    return mapped_response;
}

/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Object} parsedResponse The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(response, requestedFields) {
    let data = [];
    Object.keys(response).map(function (packageName) {
        const package = response[packageName];
        let downloadData = package.downloads;
        let formattedData = downloadData.map(function (dailyDownload) {
            return formatData(requestedFields, packageName, dailyDownload);
        });
        data = data.concat(formattedData);
    });
    return data;
}

// [END get_data]

// https://developers.google.com/datastudio/connector/reference#isadminuser
function isAdminUser() {
    return true;
}

/**
 * Validates config parameters and provides missing values.
 *
 * @param {Object} configParams Config parameters from `request`.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
    configParams = configParams || {};
    configParams.package = configParams.package || DEFAULT_PACKAGE;

    configParams.package = configParams.package
        .split(',')
        .map(function (x) {
            return x.trim();
        })
        .join(',');

    return configParams;
}

/**
 * Formats a single row of data into the required format.
 *
 * @param {Object} requestedFields Fields requested in the getData request.
 * @param {string} packageName Name of the package who's download data is being
 *    processed.
 * @param {Object} dailyDownload Contains the download data for a certain day.
 * @returns {Object} Contains values for requested fields in predefined format.
 */
function formatData(requestedFields, packageName, dailyDownload) {
    let row = requestedFields.asArray().map(function (requestedField) {
        switch (requestedField.getId()) {
            case 'day':
                return dailyDownload.day.replace(/-/g, '');
            case 'downloads':
                return dailyDownload.downloads;
            case 'packageName':
                return packageName;
            default:
                return '';
        }
    });
    return {values: row};
}
