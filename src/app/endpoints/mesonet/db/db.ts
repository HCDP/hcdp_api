import express, { NextFunction } from "express";
import { rateLimit } from "express-rate-limit";
import moment, { DurationInputArg1, DurationInputArg2, Moment } from "moment-timezone";
import { mesonetDBAdmin, mesonetDBUser, pgStoreMesonetEmail, pgStoreSlowMesonetMeasurements } from "../../../modules/util/resourceManagers/db.js";
import * as fs from "fs";
import * as path from "path";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers.js";
import { apiURL, downloadRoot, mesonetLocations } from "../../../modules/util/config.js";
import { parseBoolParam, parseListParam, sendEmail, TwoWayMap, validateArray, validateRangedTimestamp, validateType } from "../../../modules/util/util.js";
import { stringify } from "csv-stringify/sync";
import * as crypto from "crypto";
import { parseParams } from "../../../modules/util/dbUtil.js";
import { slowDown } from "express-slow-down";
import Cursor from "pg-cursor";

export const router = express.Router();

interface QueryData {
  query: string,
  params: any[],
  index: string[]
}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////


const MESONET_EMAIL_MAX = 5
const MESONET_MEASURMENTS_MAX = 50;

const baseMesonetMeasurementSlow = slowDown({
  windowMs: 60 * 1000,
  delayAfter: MESONET_MEASURMENTS_MAX,
  delayMs: (used: number) => {
    return 1000 * (used - MESONET_MEASURMENTS_MAX);
  },
  store: pgStoreSlowMesonetMeasurements
});

function mesonetMeasurementSlow(req: any, res: any, next: NextFunction): void {
  baseMesonetMeasurementSlow(req, res, (err?: any) => {
    if(err) {
      return next(err);
    }

    if(req.slowDown) {
      const used = req.slowDown.current;
      const remaining = MESONET_MEASURMENTS_MAX - used;
      const delay = used <= MESONET_MEASURMENTS_MAX ? 0 : 1000 * (used - MESONET_MEASURMENTS_MAX);

      res.setHeader('X-Slowdown-Delay', `${delay}ms`);
      res.setHeader('X-Slowdown-Remaining', remaining.toString());

      if(req.slowDown.resetTime) {
        const msUntilReset = Math.max(0, req.slowDown.resetTime.getTime() - Date.now());
        res.setHeader('X-Slowdown-Reset-After', `${msUntilReset}ms`);
      }
    }

    next();
  });
};



const baseMesonetEmailLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minute window
  limit: MESONET_EMAIL_MAX, // Limit each IP to 5 requests per window
  standardHeaders: "draft-8",
  legacyHeaders: false,
  message: "Too many requests from this IP. Requests for this endpoint are limited to 5 per 15 minutes.",
  store: pgStoreMesonetEmail,
  handler: (req, res, next, options) => {
    // Inject headers even when blocked
    setRateLimitMetricsHeaders(req, res);
    res.status(options.statusCode).send(options.message);
  }
});

// Calculate and attach custom metric headers
function setRateLimitMetricsHeaders(req: any, res: any) {
  if (req.rateLimit) {
    // Subtract current hits from max limit
    const remaining = MESONET_EMAIL_MAX - req.rateLimit.current;
    
    // Calculate exact milliseconds remaining until window resets
    const msUntilReset = req.rateLimit.resetTime ? Math.max(0, req.rateLimit.resetTime.getTime() - Date.now()) : 0;

    res.setHeader('X-RateLimit-Remaining', `${remaining}`);
    res.setHeader('X-RateLimit-Reset-After', `${msUntilReset}ms`);
  }
}

// email limiter metrics middleware
function mesonetEmailLimiter(req: any, res: any, next: NextFunction) {
  baseMesonetEmailLimiter(req, res, (err?: any) => {
    if(err) {
      return next(err);
    }
    setRateLimitMetricsHeaders(req, res);
    next();
  });
};


/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////


function getMeasurementsTable(location: string) {
  return `${location}_measurements_tsdb`;
}

function getFlagTable(location: string) {
  return `${location}_measurement_flags`;
}




function constructBaseMeasurementsQuery(stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean, joinFlags: boolean, dataQuality: number[] = [], screeningMethod: number[] = []): QueryData {
  let measurementsTable = getMeasurementsTable(location);
  let flagTable = getFlagTable(location);

  let params: string[] = [];

  ///////////////////////////////////////////////////
  //////////// translations where clause ////////////
  ///////////////////////////////////////////////////

  let translationsWhereClauses = [];

  if(varIDs.length > 0) {
    parseParams(varIDs, params, translationsWhereClauses, "standard_name");
  }

  if(intervals.length > 0) {
    parseParams(intervals, params, translationsWhereClauses, "interval_seconds");
  }

  let translationsWhereClause = "";
  if(translationsWhereClauses.length > 0) {
    translationsWhereClause = `WHERE ${translationsWhereClauses.join(" AND ")}`;
  } 

  ///////////////////////////////////////////////////
  //////////////// main where clause ////////////////
  ///////////////////////////////////////////////////

  let mainWhereClauses: string[] = [];
  
  if(stationIDs.length > 0) {
    parseParams(stationIDs, params, mainWhereClauses, `${measurementsTable}.station_id`);
  }

  if(startDate) {
    params.push(startDate);
    mainWhereClauses.push(`${measurementsTable}.timestamp >= $${params.length}`);
  }

  if(endDate) {
    params.push(endDate);
    mainWhereClauses.push(`${measurementsTable}.timestamp <= $${params.length}`);
  }

  
  let flagJoinClause = "";
  if(joinFlags || dataQuality.length > 0 || screeningMethod.length > 0) {
      flagJoinClause = `LEFT JOIN ${flagTable} ON ${flagTable}.station_id = ${measurementsTable}.station_id AND ${flagTable}.timestamp = ${measurementsTable}.timestamp AND ${flagTable}.variable = variable_data.standard_name`;
  }
  
  if(dataQuality.length > 0) {
    let dqIncludeNulls = dataQuality.includes(0);
    
    let dqParams: string[] = [];
    for(let val of dataQuality) {
        params.push(val.toString());
        dqParams.push(`$${params.length}`);
    }
    
    if (dqIncludeNulls) {
        mainWhereClauses.push(`(${flagTable}.data_quality IN (${dqParams.join(", ")}) OR ${flagTable}.data_quality IS NULL)`);
    } else {
        mainWhereClauses.push(`${flagTable}.data_quality IN (${dqParams.join(", ")})`);
    }
  }
  
  if(screeningMethod.length > 0) {
    let smParams: string[] = [];
    for(let val of screeningMethod) {
        params.push(val.toString());
        smParams.push(`$${params.length}`);
    }
    mainWhereClauses.push(`${flagTable}.screening_method IN (${smParams.join(", ")})`);
  }


  let mainWhereClause = "";
  if(mainWhereClauses.length > 0) {
    mainWhereClause = `WHERE ${mainWhereClauses.join(" AND ")}`;
  }


  ///////////////////////////////////////////////////
  /////////////// limit offset clause ///////////////
  ///////////////////////////////////////////////////

  let limitOffsetClause = "";
  
  params.push(limit.toString());
  limitOffsetClause += `LIMIT $${params.length}`;
  if(offset) {
    params.push(offset.toString());
    limitOffsetClause += ` OFFSET $${params.length}`;
  }
  
  let selectList = `
    ${measurementsTable}.timestamp,
    ${measurementsTable}.station_id,
    variable_data.standard_name as variable,
    value
  `;
  if (joinFlags) {
      selectList += `,
      data_quality,
      flags,
      screening_method,
      notes
      `
  }

  let query = joinMetadata ? `
    WITH variable_metadata_combined AS (
      SELECT standard_name, display_name, unit_metadata.units, units_plain, units_expanded
      FROM variable_metadata
      LEFT JOIN unit_metadata ON variable_metadata.units = unit_metadata.units
    )
    SELECT
      ${selectList},
      units,
      units_plain,
      units_expanded,
      display_name AS variable_display_name,
      interval_seconds,
      name AS station_name,
      lat,
      lng,
      elevation,
      vegh,
      skn,
      nws_id,
      status
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${flagJoinClause}
    JOIN station_metadata ON station_metadata.station_id = ${measurementsTable}.station_id JOIN variable_metadata_combined ON variable_metadata_combined.standard_name = variable_data.standard_name
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  ` : `
    SELECT
      ${selectList}
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${flagJoinClause}
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  `;


  let index = ["station_id", "timestamp", "variable", "value"];
  if(joinFlags) {
    index = index.concat(["data_quality", "flags", "screening_method", "notes"])
  }
  
  if(joinMetadata) {
    index = index.concat(["units", "units_plain", "units_expanded", "variable_display_name", "interval_seconds", "station_name", "lat", "lng", "elevation"]);
  }

  return {
    query,
    params,
    index
  };
}

function wrapCrosstabMeasurementsQuery(vars: string[], baseQueryData: QueryData, joinMetadata: boolean): QueryData {
  let { query, params } = baseQueryData;
  query = mesonetDBUser.mogrify(query, params);
  let crosstabValuesString = `('${vars.join("'),('")}')`;
  let varListString = vars.join(",");
  let selectString = `timestamp, station_id, ${varListString}`;
  let colDefs = `timestamp timestamp, station_id varchar, ${vars.join(" varchar, ")} varchar`;
  let index = ["station_id", "timestamp", ...vars];

  query = `
    SELECT ${selectString} FROM crosstab(
      $$
        ${query}
      $$,
      $$
        VALUES ${crosstabValuesString}
      $$
    ) AS wide(${colDefs})
  `;
  if(joinMetadata) {
    query = `
      SELECT widetable.station_id, station_metadata.name AS station_name, station_metadata.lat, station_metadata.lng, station_metadata.elevation, widetable.timestamp, ${varListString}
      FROM (
        ${query}
      ) as widetable
      JOIN station_metadata ON station_metadata.station_id = widetable.station_id
    `;
    index = ["station_id", "station_name", "lat", "lng", "elevation", "timestamp", ...vars];
  }

  return {
    query,
    params: [],
    index
  };
}

async function constructMeasurementsQuery(crosstabQuery: boolean, stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean, joinFlags: boolean, dataQuality: number[], screeningMethod: number[]): Promise<QueryData> {
  let queryData: QueryData;
  
  if(crosstabQuery) {
    //don't join metadata for now, can only join station metadata, join after crosstab
    //flag cannot be included
    queryData = constructBaseMeasurementsQuery(stationIDs, startDate, endDate, varIDs, intervals, location, limit, offset, reverse, false, false, dataQuality, screeningMethod);
    let vars = await sanitizeExpandVarIDs(varIDs);
    if(vars.length > 0) {
      queryData = wrapCrosstabMeasurementsQuery(vars, queryData, joinMetadata);
      queryData.query += ";";
    }
    else {
      let index = joinMetadata ? ["station_id", "station_name", "lat", "lng", "elevation", "timestamp"] : ["station_id", "timestamp"];
      //no valid variables, don't do anything
      queryData = {
        query: null,
        params: [],
        index
      };
    }
  }
  else {
    queryData = constructBaseMeasurementsQuery(stationIDs, startDate, endDate, varIDs, intervals, location, limit, offset, reverse, joinMetadata, joinFlags, dataQuality, screeningMethod);
    queryData.query += ";";
  }

  return queryData;
}


async function sanitizeExpandVarIDs(varIDs: string[]) {
  let query = `
    SELECT DISTINCT standard_name
    FROM version_translations
  `;
  let params: string[] = [];
  if(varIDs.length > 0) {
    let clause: string[] = [];   
    parseParams(varIDs, params, clause, "standard_name");
    query += `WHERE ${clause[0]}`;
  }
  query += ";";

  let data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
    return await cursor.read(10000);
  }, { rowMode: "array" });

  data = data.flat();
  return data;
}







router.get("/mesonet/db/measurements", mesonetMeasurementSlow, async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_ids, start_date, end_date, var_ids, intervals, location, limit = 10000, offset, reverse, join_metadata, join_flags, numeric_flags, data_quality, screening_method, local_tz, row_mode }: any = req.query;
    reverse = parseBoolParam(reverse);
    join_metadata = parseBoolParam(join_metadata);
    join_flags = parseBoolParam(join_flags);
    numeric_flags = parseBoolParam(numeric_flags);
    local_tz = parseBoolParam(local_tz);

    let varIDs = parseListParam(var_ids);
    let stationIDs = parseListParam(station_ids);
    let intervalArr = parseListParam(intervals);
    
    let rawDataQualityArr = parseListParam(data_quality);
    let rawScreeningMethodArr = parseListParam(screening_method);
    
    let dataQualityArr: number[] = [];
    for(let val of rawDataQualityArr) {
      if(typeof val === 'number') {
        dataQualityArr.push(val);
      }
      else if(typeof val === 'string') {
        const numVal = parseInt(val, 10);
        if(!isNaN(numVal)) {
          dataQualityArr.push(numVal);
        }
        else {
          const mappedVal = DATA_QUALITY_TRANSLATIONS.lookup(val.toLowerCase() as DataQualityValuesKey);
          if(mappedVal !== undefined) {
            dataQualityArr.push(mappedVal);
          }
        }
      }
    }
    
    let screeningMethodArr: number[] = [];
    for(let val of rawScreeningMethodArr) {
      if(typeof val === 'number') {
        screeningMethodArr.push(val);
      }
      else if(typeof val === 'string') {
        const numVal = parseInt(val, 10);
        if(!isNaN(numVal)) {
          screeningMethodArr.push(numVal);
        }
        else {
          const mappedVal = FLAG_METHOD_TRANSLATIONS.lookup(val.toLowerCase() as FlagMethodValuesKey);
          if(mappedVal !== undefined) {
            screeningMethodArr.push(mappedVal);
          }
        }
      }
    }

    const MAX_QUERY = 1000000;

    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }

    let crosstabQuery = false;
    switch(row_mode) {
      case "wide_array": {
        row_mode = "array";
        crosstabQuery = true;
        break;
      }
      case "array": {
        break;
      }
      case "wide_json": {
        row_mode = undefined;
        crosstabQuery = true;
        break;
      }
      default: {
        row_mode = undefined;
      }
    }

    if(crosstabQuery) {
      join_flags = false;
    }

    if(offset) {
      offset = parseInt(offset, 10);
      if(isNaN(offset)) {
        offset = undefined;
      }
    }
    if(typeof limit === "string") {
      limit = parseInt(limit, 10);
      if(isNaN(limit)) {
        limit = 10000;
      }
    }
    if(limit < 1 || limit > MAX_QUERY) {
      limit = MAX_QUERY;
    }

    if(start_date) {
      try {
        let date = new Date(start_date);
        start_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send("Invalid start date format. Dates must be ISO 8601 compliant.");
      }
    }
  
    if(end_date) {
      try {
        let date = new Date(end_date);
        end_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send("Invalid end date format. Dates must be ISO 8601 compliant.");
      }
    }

    let data: any[] | { index: string[], data: any[] } = [];
    let { query, params, index } = await constructMeasurementsQuery(crosstabQuery, stationIDs, start_date, end_date, varIDs, intervalArr, location, limit, offset, reverse, join_metadata, join_flags, dataQualityArr, screeningMethodArr);
    
    if(query) {
      try {
        data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
          let rows = [];
          const chunkSize = 10000;
          let chunk: any[];
          
          let flagsIdx = -1;
          let smIdx = -1;
          let dqIdx = -1;
          if(row_mode === "array" && join_flags) {
            flagsIdx = index.indexOf("flags");
            smIdx = index.indexOf("screening_method");
            dqIdx = index.indexOf("data_quality");
          }

          do {
            chunk = await cursor.read(chunkSize);
            for(let row of chunk) {

              // TEMP
              if(row_mode === "array") {
                row.push(0);
              }
              else {
                row.flag = 0;
              }



              if(join_flags) {
                if(row_mode === "array") {
                  if(flagsIdx !== -1 && row[flagsIdx] != null) {
                    row[flagsIdx] = decodeFlags(row[flagsIdx], numeric_flags);
                  }
                  if(!numeric_flags && dqIdx !== -1 && row[dqIdx] != null) {
                    row[dqIdx] = DATA_QUALITY_TRANSLATIONS.reverseLookup(row[dqIdx]);
                  }
                  if(smIdx !== -1 && row[smIdx] != null) {
                    row[smIdx] = FLAG_METHOD_TRANSLATIONS.reverseLookup(row[smIdx]);
                  }
                }
                else {
                  if(row.flags != null) {
                    row.flags = decodeFlags(row.flags, numeric_flags);
                  }
                  if(!numeric_flags && row.data_quality != null) {
                    row.data_quality = DATA_QUALITY_TRANSLATIONS.reverseLookup(row.data_quality);
                  }
                  if(row.screening_method != null) {
                    row.screening_method = FLAG_METHOD_TRANSLATIONS.reverseLookup(row.screening_method);
                  }
                }
              }
              rows.push(row);
            }
          }
          while(chunk.length > 0)
          return rows;
        }, { rowMode: row_mode });
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
      }
    }

    if(data.length > 0 && local_tz) {
      let timezone = await getLocationTimezone(location);

      if(row_mode === "array") {
        let tsIndex = index.indexOf("timestamp");
        for(let row of data as any[]) {
          let converted = moment(row[tsIndex]).tz(timezone);
          row[tsIndex] = converted.format();
        }
      }
      else {
        for(let row of data as any[]) {
          let converted = moment(row.timestamp).tz(timezone);
          row.timestamp = converted.format();
        }
      }
    }

    if(row_mode === "array" || row_mode == "wide_array") {
      data = {
        index,
        data: data as any[]
      };
    }

    reqData.code = 200;
    return res.status(200).json(data);
  });
});



router.get("/mesonet/db/measurements/timing", mesonetMeasurementSlow, async (req, res) => {
  const startTime = performance.now();
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_ids, start_date, end_date, var_ids, intervals, location, limit = 10000, offset, reverse, join_metadata, join_flags, numeric_flags, data_quality, screening_method, local_tz, row_mode }: any = req.query;
    reverse = parseBoolParam(reverse);
    join_metadata = parseBoolParam(join_metadata);
    join_flags = parseBoolParam(join_flags);
    numeric_flags = parseBoolParam(numeric_flags);
    local_tz = parseBoolParam(local_tz);

    let varIDs = parseListParam(var_ids);
    let stationIDs = parseListParam(station_ids);
    let intervalArr = parseListParam(intervals);

    let rawDataQualityArr = parseListParam(data_quality);
    let rawScreeningMethodArr = parseListParam(screening_method);
    
    let dataQualityArr: number[] = [];
    for(let val of rawDataQualityArr) {
      if(typeof val === 'number') {
        dataQualityArr.push(val);
      }
      else if(typeof val === 'string') {
        const numVal = parseInt(val, 10);
        if(!isNaN(numVal)) {
          dataQualityArr.push(numVal);
        }
        else {
          const mappedVal = DATA_QUALITY_TRANSLATIONS.lookup(val.toLowerCase() as DataQualityValuesKey);
          if(mappedVal !== undefined) {
            dataQualityArr.push(mappedVal);
          }
        }
      }
    }
    
    let screeningMethodArr: number[] = [];
    for(let val of rawScreeningMethodArr) {
      if(typeof val === 'number') {
        screeningMethodArr.push(val);
      }
      else if(typeof val === 'string') {
        const numVal = parseInt(val, 10);
        if(!isNaN(numVal)) {
          screeningMethodArr.push(numVal);
        }
        else {
          const mappedVal = FLAG_METHOD_TRANSLATIONS.lookup(val.toLowerCase() as FlagMethodValuesKey);
          if(mappedVal !== undefined) {
            screeningMethodArr.push(mappedVal);
          }
        }
      }
    }

    const MAX_QUERY = 1000000;

    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }

    let crosstabQuery = false;
    switch(row_mode) {
      case "wide_array": {
        row_mode = "array";
        crosstabQuery = true;
        break;
      }
      case "array": {
        break;
      }
      case "wide_json": {
        row_mode = undefined;
        crosstabQuery = true;
        break;
      }
      default: {
        row_mode = undefined;
      }
    }

    if(crosstabQuery) {
      join_flags = false;
    }

    if(offset) {
      offset = parseInt(offset, 10);
      if(isNaN(offset)) {
        offset = undefined;
      }
    }
    if(typeof limit === "string") {
      limit = parseInt(limit, 10);
      if(isNaN(limit)) {
        limit = 10000;
      }
    }
    if(limit < 1 || limit > MAX_QUERY) {
      limit = MAX_QUERY;
    }

    if(start_date) {
      try {
        let date = new Date(start_date);
        start_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send("Invalid start date format. Dates must be ISO 8601 compliant.");
      }
    }

    if(end_date) {
      try {
        let date = new Date(end_date);
        end_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send("Invalid end date format. Dates must be ISO 8601 compliant.");
      }
    }

    let data: any[] | { index: string[], data: any[] } = [];
    let db_query_time_ms = 0;

    let { query, params, index } = await constructMeasurementsQuery(crosstabQuery, stationIDs, start_date, end_date, varIDs, intervalArr, location, limit, offset, reverse, join_metadata, join_flags, dataQualityArr, screeningMethodArr);
    if(query) {
      const safeQuery = query.trim().replace(/;$/, '');

      const wrappedQuery = `
        WITH _data AS MATERIALIZED (
          ${safeQuery}
        )
        SELECT _data.*, 
          (EXTRACT(EPOCH FROM (clock_timestamp() - statement_timestamp())) * 1000)::float AS db_query_time_ms
        FROM (SELECT 1) _force_row
        LEFT JOIN _data ON TRUE;
      `;

      try {
        data = await mesonetDBUser.query(wrappedQuery, params, async (cursor: Cursor) => {
          let rows = [];
          const chunkSize = 10000;
          let chunk: any[];
          
          let flagsIdx = -1;
          let smIdx = -1;
          let dqIdx = -1;
          if(row_mode === "array" && join_flags) {
            flagsIdx = index.indexOf("flags");
            smIdx = index.indexOf("screening_method");
            dqIdx = index.indexOf("data_quality");
          }

          do {
            chunk = await cursor.read(chunkSize);
            for(let row of chunk) {
              if(row_mode === "array") {
                let queryTimeVal = row.pop();
                if(queryTimeVal !== undefined && queryTimeVal !== null) {
                  db_query_time_ms = parseFloat(Number(queryTimeVal).toFixed(2));
                }
                
                if(join_flags) {
                  if(flagsIdx !== -1 && row[flagsIdx] != null) {
                    row[flagsIdx] = decodeFlags(row[flagsIdx], numeric_flags);
                  }
                  if(!numeric_flags && dqIdx !== -1 && row[dqIdx] != null) {
                    row[dqIdx] = DATA_QUALITY_TRANSLATIONS.reverseLookup(row[dqIdx]);
                  }
                  if(smIdx !== -1 && row[smIdx] != null) {
                    row[smIdx] = FLAG_METHOD_TRANSLATIONS.reverseLookup(row[smIdx]);
                  }
                }

                if(!row.every((val: any) => val === null)) {
                  rows.push(row);
                }
              }
              else {
                let queryTimeVal = row.db_query_time_ms;
                delete row.db_query_time_ms;
                if(queryTimeVal !== undefined && queryTimeVal !== null) {
                  db_query_time_ms = parseFloat(Number(queryTimeVal).toFixed(2));
                }
                
                if(join_flags) {
                  if(row.flags != null) {
                    row.flags = decodeFlags(row.flags, numeric_flags);
                  }
                  if(!numeric_flags && row.data_quality != null) {
                    row.data_quality = DATA_QUALITY_TRANSLATIONS.reverseLookup(row.data_quality);
                  }
                  if(row.screening_method != null) {
                    row.screening_method = FLAG_METHOD_TRANSLATIONS.reverseLookup(row.screening_method);
                  }
                }

                if(!Object.values(row).every((val: any) => val === null)) {
                  rows.push(row);
                }
              }
            }
          }
          while(chunk.length > 0)
          return rows;
        }, { rowMode: row_mode });
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
      }
    }

    if(Array.isArray(data) && data.length > 0 && local_tz) {
      let timezone = await getLocationTimezone(location);

      if(row_mode === "array") {
        let tsIndex = index.indexOf("timestamp");
        for(let row of data as any[]) {
          let converted = moment(row[tsIndex]).tz(timezone);
          row[tsIndex] = converted.format();
        }
      }
      else {
        for(let row of data as any[]) {
          let converted = moment(row.timestamp).tz(timezone);
          row.timestamp = converted.format();
        }
      }
    }

    if(row_mode === "array" || row_mode === "wide_array") {
      data = {
        index,
        data: data as any[]
      };
    }

    const endpoint_time_ms = parseFloat((performance.now() - startTime).toFixed(2));

    reqData.code = 200;
    return res.status(200).json({
      endpoint_time_ms,
      db_query_time_ms,
      data
    });
  });
});






function constructMeasurementsQueryEmail(stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean, dataQuality: number[], screeningMethod: number[]): QueryData {
  let measurementsTable = getMeasurementsTable(location);
  let flagTable = getFlagTable(location);

  let params: string[] = [];

  let translationsWhereClauses = [];

  if(varIDs.length > 0) {
    parseParams(varIDs, params, translationsWhereClauses, "standard_name");
  }

  if(intervals.length > 0) {
    parseParams(intervals, params, translationsWhereClauses, "interval_seconds");
  }

  let translationsWhereClause = "";
  if(translationsWhereClauses.length > 0) {
    translationsWhereClause = `WHERE ${translationsWhereClauses.join(" AND ")}`;
  } 

  let mainWhereClauses: string[] = [];
  
  if(stationIDs.length > 0) {
    parseParams(stationIDs, params, mainWhereClauses, `${measurementsTable}.station_id`);
  }

  if(startDate) {
    params.push(startDate);
    mainWhereClauses.push(`${measurementsTable}.timestamp >= $${params.length}`);
  }

  if(endDate) {
    params.push(endDate);
    mainWhereClauses.push(`${measurementsTable}.timestamp <= $${params.length}`);
  }

  let flagJoinClause = "";
  if(dataQuality.length > 0 || screeningMethod.length > 0) {
    flagJoinClause = `LEFT JOIN ${flagTable} ON ${flagTable}.station_id = ${measurementsTable}.station_id AND ${flagTable}.timestamp = ${measurementsTable}.timestamp AND ${flagTable}.variable = variable_data.standard_name`;
  }
  
  if(dataQuality.length > 0) {
    let dqIncludeNulls = dataQuality.includes(0);
    
    let dqParams: string[] = [];
    for(let val of dataQuality) {
      params.push(val.toString());
      dqParams.push(`$${params.length}`);
    }
    
    if(dqIncludeNulls) {
      mainWhereClauses.push(`(${flagTable}.data_quality IN (${dqParams.join(", ")}) OR ${flagTable}.data_quality IS NULL)`);
    }
    else {
      mainWhereClauses.push(`${flagTable}.data_quality IN (${dqParams.join(", ")})`);
    }
  }
  
  if(screeningMethod.length > 0) {
    let smParams: string[] = [];
    for(let val of screeningMethod) {
      params.push(val.toString());
      smParams.push(`$${params.length}`);
    }
    mainWhereClauses.push(`${flagTable}.screening_method IN (${smParams.join(", ")})`);
  }

  let mainWhereClause = "";
  if(mainWhereClauses.length > 0) {
    mainWhereClause = `WHERE ${mainWhereClauses.join(" AND ")}`;
  }

  let limitOffsetClause = "";
  if(Number.isFinite(limit)) {
    params.push(limit.toString());
    limitOffsetClause += `LIMIT $${params.length}`;
  }
  
  if(offset) {
    params.push(offset.toString());
    limitOffsetClause += ` OFFSET $${params.length}`;
  }
  
  let selectList = `
      ${measurementsTable}.timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value
  `;
  
  let query = joinMetadata ? `
    WITH variable_metadata_combined AS (
      SELECT standard_name, display_name, unit_metadata.units, units_plain, units_expanded
      FROM variable_metadata
      LEFT JOIN unit_metadata ON variable_metadata.units = unit_metadata.units
    )
    SELECT
      ${selectList},
      units,
      units_plain,
      units_expanded,
      display_name AS variable_display_name,
      interval_seconds,
      name AS station_name,
      lat,
      lng,
      elevation,
      vegh,
      skn,
      nws_id,
      status
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${flagJoinClause}
    JOIN station_metadata ON station_metadata.station_id = ${measurementsTable}.station_id JOIN variable_metadata_combined ON variable_metadata_combined.standard_name = variable_data.standard_name
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  ` : `
    SELECT
      ${selectList}
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${flagJoinClause}
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  `;

  let index = ["station_id", "timestamp", "variable", "value"];
  if(joinMetadata) {
    index = index.concat(["units", "units_plain", "units_expanded", "variable_display_name", "interval_seconds", "station_name", "lat", "lng", "elevation"]);
  }

  return {
    query,
    params,
    index
  };
}

function constructVariablesQuery(varIDs: string[], limit?: number, offset?: number): QueryData {
  let params: string[] = [];

  ////////////////////////////////////////////////////
  /////////////////// where clause ///////////////////
  ////////////////////////////////////////////////////

  let whereClauses: string[] = [];
  
  if(varIDs.length > 0) {
    parseParams(varIDs, params, whereClauses, "standard_name");
  }

  let whereClause = "";
  if(whereClauses.length > 0) {
    whereClause = `WHERE ${whereClauses.join(" AND ")}`;
  } 

  ///////////////////////////////////////////////////
  /////////////// limit offset clause ///////////////
  ///////////////////////////////////////////////////

  let limitOffsetClause = "";
  if(limit) {
    params.push(limit.toString());
    limitOffsetClause += `LIMIT $${params.length}`;
  }
  if(offset) {
    params.push(offset.toString());
    limitOffsetClause += ` OFFSET $${params.length}`;
  }

  let query = `
    SELECT standard_name, display_name, unit_metadata.units, units_plain, units_expanded
    FROM variable_metadata
    LEFT JOIN unit_metadata ON variable_metadata.units = unit_metadata.units
    ${whereClause}
    ${limitOffsetClause};
  `;

  let index = ["standard_name", "display_name", "units", "units_plain", "units_expanded"];

  return {
    query,
    params,
    index
  }
}

router.get("/mesonet/db/stations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_ids, location, limit, offset, row_mode }: any = req.query;

    let stationIDs = parseListParam(station_ids);

    if(row_mode !== "array") {
      row_mode = undefined;
    }

    let params: string[] = [];

    ////////////////////////////////////////////////////
    /////////////////// where clause ///////////////////
    ////////////////////////////////////////////////////

    let whereClauses: string[] = [];

    if(location) {
      params.push(location);
      whereClauses.push(`station_metadata.location = $${params.length}`);
    }
    
    if(stationIDs.length > 0) {
      parseParams(stationIDs, params, whereClauses, "station_id");
    }

    let whereClause = "";
    if(whereClauses.length > 0) {
      whereClause = `WHERE ${whereClauses.join(" AND ")}`;
    } 

    ///////////////////////////////////////////////////
    /////////////// limit offset clause ///////////////
    ///////////////////////////////////////////////////

    let limitOffsetClause = "";
    if(limit) {
      params.push(limit);
      limitOffsetClause += `LIMIT $${params.length}`;
    }
    if(offset) {
      params.push(offset);
      limitOffsetClause += ` OFFSET $${params.length}`;
    }

    let query = `
      SELECT station_id, name, full_name, lat, lng, elevation, vegh, skn, nws_id, status, station_metadata.location, timezone_map.timezone
      FROM station_metadata
      JOIN timezone_map ON station_metadata.location = timezone_map.location
      ${whereClause}
      ${limitOffsetClause};
    `;

    let data: any = [];
    try {
      data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
        let rows = [];
        const chunkSize = 10000;
        let chunk: any[];
        do {
          chunk = await cursor.read(chunkSize);
          for(let row of chunk) {
            rows.push(row);
          }
        }
        while(chunk.length > 0)
        return rows;
      }, {rowMode: row_mode});
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    if(row_mode === "array") {
      let index = ["station_id", "name", "lat", "lng", "elevation"];

      data = {
        index,
        data
      };
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});


router.get("/mesonet/db/variables", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { var_ids, limit, offset, row_mode }: any = req.query;

    let varIDs = parseListParam(var_ids);

    if(row_mode !== "array") {
      row_mode = undefined;
    }

    let { query, params, index } = constructVariablesQuery(varIDs, limit, offset);

    let data: any = [];
    try {
      data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
        let rows = [];
        const chunkSize = 10000;
        let chunk: any[];
        do {
          chunk = await cursor.read(chunkSize);
          for(let row of chunk) {
            rows.push(row);
          }
        }
        while(chunk.length > 0)
        return rows;
      }, {rowMode: row_mode});
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    if(row_mode === "array") {
      data = {
        index,
        data
      };
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);

  });
});

router.get("/mesonet/db/sensors", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { location, station_ids, var_ids, row_mode } = req.query;

    let stationIDs = parseListParam(station_ids);
    let varIDs = parseListParam(var_ids);
    if(!(typeof location === "string" && mesonetLocations.includes(location))) {
      location = undefined;
    }
    if(row_mode !== "array") {
      row_mode = undefined;
    }

    let params: string[] = [];
    let whereClauses: string[] = [];
    let joinClause = "";
    if(stationIDs.length > 0) {
      parseParams(stationIDs, params, whereClauses, "station_id");
    }
    if(varIDs.length > 0) {
      parseParams(varIDs, params, whereClauses, "standard_name");
    }
    if(location) {
      params.push(<string>location)
      whereClauses.push(`station_metadata.location = $${params.length}`);
      joinClause = "JOIN station_metadata ON station_metadata.station_id = sensor_positions.station_id";
    }
  
    let whereClause = "";
    if(whereClauses.length > 0) {
      whereClause = `WHERE ${whereClauses.join(" AND ")}`;
    }

    //sensor metadata
    let query = `
      SELECT sensor_positions.station_id, standard_name, sensor_number, sensor_height
      FROM sensor_positions
      ${joinClause}
      ${whereClause};
    `;
    let data: any;
    try {
      data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
        return await cursor.read(100000);
      });
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    if(row_mode === "array") {
      let index = ["station_id", "standard_name", "sensor_number", "sensor_height"];

      data = {
        index,
        data
      };
    }
    

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});


router.get("/mesonet/db/synopticData", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    let synopticData = {
      synoptic: {},
      locationData: {}
    };

    //synoptic data
    let query = `
      SELECT program, alias, synoptic_translations.standard_name, synoptic_name, unit_conversion_coefficient
      FROM synoptic_translations
      JOIN version_translations ON version_translations.standard_name = synoptic_translations.standard_name;
    `;

    let data = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      return await cursor.read(100000);
    });

    for(let row of data) {
      const { program, alias, standard_name, synoptic_name, unit_conversion_coefficient } = row;
      let programData = synopticData.synoptic[program];
      if(!programData) {
        programData = {};
        synopticData.synoptic[program] = programData;
      }
      programData[alias] = {
        standard_name,
        synoptic_name,
        unit_conversion_coefficient
      }
    }

    //station metadata
    query = `
      SELECT location, station_id, lat, lng, elevation
      FROM station_metadata;
    `;

    data = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      return await cursor.read(100000);
    });

    for(let row of data) {
      let { location, station_id, lat, lng, elevation } = row;

      //set up locations if first time seen, only needs to be done for this query since this is the source of the location field for all queries
      let locationData = synopticData.locationData[location];
      if(!locationData) {
        locationData = {
          stationMetadata: {},
          sensorMetadata: {},
          exclusions: {}
        };
        synopticData.locationData[location] = locationData;
      }

      locationData.stationMetadata[station_id] = {
        lat,
        lng,
        elevation
      };
    }

    //sensor metadata
    query = `
      SELECT station_metadata.location, sensor_positions.station_id, standard_name, sensor_number, sensor_height
      FROM sensor_positions
      JOIN station_metadata ON station_metadata.station_id = sensor_positions.station_id;
    `;

    data = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      return await cursor.read(100000);
    });

    for(let row of data) {
      let { location, station_id, standard_name, sensor_number, sensor_height } = row;
      let sensorMetadata = synopticData.locationData[location].sensorMetadata;
      let stationData = sensorMetadata[station_id];
      if(!stationData) {
        stationData = {};
        sensorMetadata[station_id] = stationData;
      }
      stationData[standard_name] = {
        sensor_number,
        sensor_height
      };
    }

    //exclusion data
    query = `
      SELECT station_metadata.location, synoptic_exclude.station_id, standard_name
      FROM synoptic_exclude
      JOIN station_metadata ON station_metadata.station_id = synoptic_exclude.station_id;
    `;

    data = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      return await cursor.read(100000);
    });

    for(let row of data) {
      let { location, station_id, standard_name } = row;
      let exclusions = synopticData.locationData[location].exclusions;
      let stationData = exclusions[station_id];
      if(!stationData) {
        stationData = {};
        exclusions[station_id] = stationData;
      }
      stationData[standard_name] = true;
    }

    reqData.code = 200;
    return res.status(200)
    .json(synopticData);
  });
});


router.get("/mesonet/db/sff", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    let { location }: any = req.query;

    if(!mesonetLocations.includes(location)) {
    	location = "hawaii";
    }

    const table_name = getMeasurementsTable(location);
    const flag_table = getFlagTable(location);

    res.set("Content-Type", "text/csv");
    res.set("Content-Disposition", `attachment; filename="sff_data.csv"`);
    let query = `
      SELECT ${table_name}.station_id, station_metadata.lat, station_metadata.lng, station_metadata.elevation, ${table_name}.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_height, CASE WHEN ${table_name}.value IS NOT NULL THEN CAST(${table_name}.value AS DECIMAL) * synoptic_translations.unit_conversion_coefficient ELSE NULL END AS value
      FROM ${table_name}
      JOIN version_translations ON version_translations.program = ${table_name}.version AND version_translations.alias = ${table_name}.variable
      JOIN synoptic_translations ON version_translations.standard_name = synoptic_translations.standard_name
      JOIN station_metadata ON station_metadata.station_id = ${table_name}.station_id
      LEFT JOIN sensor_positions ON sensor_positions.station_id = ${table_name}.station_id AND version_translations.standard_name = sensor_positions.standard_name
      LEFT JOIN ${flag_table} ON ${flag_table}.station_id = ${table_name}.station_id AND ${flag_table}.timestamp = ${table_name}.timestamp AND ${flag_table}.variable = version_translations.standard_name
      WHERE ${table_name}.timestamp >= NOW() - '6 hours'::INTERVAL AND (${flag_table}.data_quality = 0 OR ${flag_table}.data_quality IS NULL) AND NOT EXISTS (SELECT 1 FROM synoptic_exclude WHERE synoptic_exclude.station_id = ${table_name}.station_id AND synoptic_exclude.standard_name = version_translations.standard_name)
      ORDER BY ${table_name}.station_id, ${table_name}.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_number;
    `;

    let data = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      return await cursor.read(100000);
    });
    
    res.write("station_id,LAT [ddeg],LON [ddeg],date_time [UTC],ELEV [m],T [C],RH [%],FF [m/s],DD [deg],FFGUST [m/s],P [hPa],SOLRAD [W/m2],SOLOUT [W/m2],LWRAD [W/m2],LWOUT [W/m2],NETSWRAD [W/m2],NETLWRAD [W/m2],NETRAD [W/m2],PAR [umol/m2s],PCP5M [mm],BATV [volt],SOILT [C],SOILMP [%]\n");

    const varOrder = ["T [C]", "RH [%]", "FF [m/s]", "DD [deg]", "FFGUST [m/s]", "P [hPa]", "SOLRAD [W/m2]", "SOLOUT [W/m2]", "LWRAD [W/m2]", "LWOUT [W/m2]", "NETSWRAD [W/m2]", "NETLWRAD [W/m2]", "NETRAD [W/m2]", "PAR [umol/m2s]", "PCP5M [mm]", "BATV [volt]", "SOILT [C]", "SOILMP [%]"];
    let variableData = {};
    let i = 0;
    while(i < data.length) {
      let { station_id: sid, lat, lng, elevation, timestamp } = data[i];
      //row is sid, timestamp
      while(i < data.length && data[i].station_id == sid && data[i].timestamp == timestamp) {
        let { synoptic_name: synopticName } = data[i];
        variableData[synopticName] = {
          count: 0,
          data: {}
        };
        
        for(; i < data.length && data[i].synoptic_name == synopticName; i++) {
          let { sensor_height: sensorHeight, value } = data[i];
          variableData[synopticName].count++;
          let values = variableData[synopticName].data[sensorHeight];
          if(!values) {
            values = [];
            variableData[synopticName].data[sensorHeight] = values;
          }
          values.push(value);
        }
      }

      let sffRow = [sid, lat, lng, timestamp, elevation];

      for(let variable of varOrder) {
        if(!variableData[variable]) {
          sffRow.push("nan");
          continue;
        }
        let { count, data: heightData } = variableData[variable];
        if(count == 1) {
          sffRow.push(heightData[Object.keys(heightData)[0]][0]);
        }
        else {
          let heights: string[] = [];
          let values: string[] = [];
          for(let height in heightData) {
            let valueData = heightData[height];

            if(valueData.length > 1) {
              for(let j = 0; j < valueData.length; j++) {
                heights.push(`${height}${String.fromCharCode(97 + j)}`);
                values.push(valueData[j]);
              }
            }
            else {
              heights.push(height);
              values.push(valueData[0]);
            }
          }
          let colString = `${heights.join(";")}#${values.join(";")}`;
          sffRow.push(colString);
        }
      }
      res.write(`${sffRow.join(",")}\n`);
    }
    
    reqData.code = 200;
    res.status(200)
    .end();
  });
});





router.put("/mesonet/db/measurements/insert", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    let { overwrite, location, data }: any = req.body;
    overwrite = parseBoolParam(overwrite, true)

    if(!Array.isArray(data)) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(`Invalid data provided. Data must be a 2D array.`);
    }

    if(data.length < 1) {
      reqData.code = 200;
      return res.status(200).json({ modified: 0 });
    }

    if(!mesonetLocations.includes(location)) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(`Invalid location provided.`);
    }

    // transpose the 2D rows into 1D columns
    let station_ids: string[] = [];
    let timestamps: string[] = [];
    let variables: string[] = [];
    let versions: string[] = [];
    let value_ds: number[] = [];
    let values: number[] = [];

    for(let row of data) {
      if(!Array.isArray(row) || row.length != 6) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send(`Invalid data provided. Data must be a 2D array with 6 element rows.`);
      }
      station_ids.push(row[0]);
      timestamps.push(row[1]);
      variables.push(row[2]);
      versions.push(row[3]);
      value_ds.push(row[4]);
      values.push(row[5]);
    }

    let onConflict = overwrite ? `
      DO UPDATE SET
        version = EXCLUDED.version,
        value = EXCLUDED.value;
      ` : "DO NOTHING;";

    let query = `
      INSERT INTO ${getMeasurementsTable(location)} (timestamp, station_id, variable, version, value_d, value)
      SELECT * FROM UNNEST(
        $1::timestamptz[],            -- timestamp: timestamp with time zone
        $2::character(4)[],           -- station_id: character(4)
        $3::character varying(255)[], -- variable: varchar(255)
        $4::character varying(255)[], -- version: varchar(255)
        $5::double precision[],       -- value_d: double precision
        $6::character varying(255)[]  -- value: varchar(255)
      ) AS t(timestamp, station_id, variable, version, value_d, value)
      ORDER BY timestamp ASC
      ON CONFLICT (timestamp, station_id, variable)
      ${onConflict}
    `;

    // Pass value arrays as single parameters
    let params = [timestamps, station_ids, variables, versions, value_ds, values];

    try {
      let modified = await mesonetDBAdmin.queryNoRes(query, params);
      reqData.code = 200;
      return res.status(200).json({ modified });
    }
    catch(e: any) {
      if(e.code && e.code.startsWith("42")) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send(`Invalid query syntax. Please validate the data provided is correctly formatted.`);
      }
      else {
        throw e;
      }
    }
  });
});



router.post("/mesonet/db/measurements/email", mesonetEmailLimiter, async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { data, email, outputName } = req.body;
    if(!(data && email)) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(
        `Request body should include the following fields: 
        email: The email to send the package to 
        data: A JSON object with parameters for the Mesonet query 
        outputName (optional): What to name the produced data file. Default: data.csv`
      );
    }

    let { station_ids, start_date, end_date, var_ids, intervals, location, limit, offset, reverse, data_quality, screening_method, local_tz }: any = data;

    let varIDs = var_ids || [];
    let stationIDs = station_ids || [];
    let intervalArr = intervals || [];

    let rawDataQualityArr = parseListParam(data_quality);
    let rawScreeningMethodArr = parseListParam(screening_method);
    
    let dataQualityArr: number[] = [];
    for(let val of rawDataQualityArr) {
      if(typeof val === 'number') {
        dataQualityArr.push(val);
      }
      else if(typeof val === 'string') {
        const numVal = parseInt(val, 10);
        if(!isNaN(numVal)) {
          dataQualityArr.push(numVal);
        }
        else {
          const mappedVal = FLAG_TRANSLATIONS.lookup(val.toLowerCase() as FlagValuesKey);
          if(mappedVal !== undefined) {
            dataQualityArr.push(mappedVal);
          }
        }
      }
    }
    
    let screeningMethodArr: number[] = [];
    for(let val of rawScreeningMethodArr) {
      if(typeof val === 'number') {
        screeningMethodArr.push(val);
      }
      else if(typeof val === 'string') {
        const numVal = parseInt(val, 10);
        if(!isNaN(numVal)) {
          screeningMethodArr.push(numVal);
        }
        else {
          const mappedVal = FLAG_METHOD_TRANSLATIONS.lookup(val.toLowerCase() as FlagMethodValuesKey);
          if(mappedVal !== undefined) {
            screeningMethodArr.push(mappedVal);
          }
        }
      }
    }

    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }

    if(typeof offset === "string") {
      offset = parseInt(limit, 10)
    }
    if(offset === undefined || isNaN(offset) || offset < 0) {
      offset = 0;
    }

    if(typeof limit === "string") {
      limit = parseInt(limit, 10)
    }
    if(limit === undefined || isNaN(limit) || limit < 1) {
      limit = Infinity;
    }

    if(start_date) {
      try {
        let date = new Date(start_date);
        start_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send(`Invalid start date provided.`);
      }
    }
    else {
      start_date = await getStartDate(location, stationIDs);
    }
    
    if(!start_date) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(`Station data range could not be found. Please check the provided station IDs are valid.`);
    }

    if(end_date) {
      try {
        let date = new Date(end_date);
        end_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
        return res.status(400).send(`Invalid end date provided.`);
      }
    }

    let { query, params } = constructVariablesQuery(varIDs);

    let varMetadata: VariableMetadata[] = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
      return await cursor.read(10000);
    });

    if(varMetadata.length < 1) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(`None of the provided variables exist.`);
    }

    reqData.code = 202;
    res.status(202).send("Request received. Your query will be processed and emailed to you if successful.");
    try {
      let timezone = local_tz ? await getLocationTimezone(location) : "UTC";

      let uuid = crypto.randomUUID();
      let fname = outputName ? outputName : "data.csv";
      let outdir = path.join(downloadRoot, uuid);
      fs.mkdirSync(outdir);
      let outfile = path.join(outdir, fname);

      let writeManager = new MesonetCSVWriter(outfile, varMetadata, timezone, limit, offset);
  
      let e: any;
      try {
        let maxLimit = limit * varIDs.length + offset * varIDs.length;
        let slowStart = true;
        let backoffBaseMS = 1000;
        let minFailures = 0;
    
        let queryChunker = new QueryWindow(1, start_date, end_date, reverse);
        let window = queryChunker.window;

        while(window && maxLimit > 0 && !writeManager.finished) {
          let timeout = false;
          try {
            let [ startDate, endDate ] = window;
            ({ query, params } = constructMeasurementsQueryEmail(stationIDs, startDate, endDate, varIDs, intervalArr, location, maxLimit, 0, reverse, false, dataQualityArr, screeningMethodArr));

            await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
              const readChunkSize = 10000;
              let readChunk: MesonetMeasurementValue[];
              do {
                readChunk = await cursor.read(readChunkSize);
                await writeManager.write(readChunk);
                maxLimit -= writeManager.lastRecordsRead;
              }
              while(readChunk.length > 0 && maxLimit > 0 && !writeManager.finished)
            });
          }
          catch(e: any) {
            if(e.code != "57014") {
              throw e;
            }
            timeout = true;
          }
    
          if(timeout && queryChunker.windowSize == 1) {
            if(minFailures >= 10) {
              throw new Error("Minimum data timed out 10 times. Cannot retreive the minimum data threshold");
            }
            slowStart = false;
            await new Promise(resolve => setTimeout(resolve, backoffBaseMS * Math.pow(2, minFailures)));
            minFailures++;
          }
          else if(timeout) {
            slowStart = false;
            queryChunker.windowSize = Math.floor(queryChunker.windowSize / 2);
          }
          else if(slowStart) {
            minFailures = 0;
            queryChunker.advanceWindow();
            queryChunker.windowSize *= 2;
          }
          else {
            minFailures = 0;
            queryChunker.advanceWindow();
            queryChunker.windowSize++;
          }
          window = queryChunker.window;
        }
      }
      catch(err) {
        e = err;
      }
      await writeManager.end();

      if(e !== undefined) { throw e; }
  
      let ep = `${apiURL}/download/package`;
      let downloadUrlParams = `packageID=${uuid}&file=${fname}`;
      let downloadLink = `${ep}?${downloadUrlParams}`;
      let mailOptions = {
        to: email,
        text: "Your Mesonet data is ready. Please go to " + downloadLink + " to download it. This link will expire in three days, please download your data in that time.",
        html: "<p>Your Mesonet data is ready. Please click <a href=\"" + downloadLink + "\">here</a> to download it. This link will expire in three days, please download your data in that time.</p>"
      };
      let mailRes = await sendEmail(mailOptions);
  
      if(!mailRes.success) {
        reqData.success = false;
        throw new Error("Failed to send message to user " + email + ". Error: " + mailRes.error.toString());
      }
    }
    catch(e) {
      reqData.success = false;
      try {
        let message = "An error occurred while generating your Mesonet data. We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later or email us at hcdp@hawaii.edu for assistance.";
        let mailOptions = {
          to: email,
          subject: "Mesonet Data Error",
          text: message,
          html: "<p>" + message + "</p>"
        };
        await sendEmail(mailOptions);
      }
      catch(err) {}
      throw e;
    }
  });
});


router.get("/mesonet/db/stationMonitor", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { var_ids, location }: any = req.query;

    if(!mesonetLocations.includes(location)) {
    	location = "hawaii";
    }

    const tableName = `${location}_measurements_24hr`;

    let params = parseListParam(var_ids);

    let inlineParams = params.map((value, index) => { return `$${index + 1}`});

    let timezone = await getLocationTimezone(location);
    
    let query = `
      SELECT MIN(timestamp), MAX(timestamp)
      FROM ${tableName};
    `;

    let data: any[] = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      return await cursor.read(1);
    }, { rowMode: "array" });

    let [ startDate, endDate ] = data[0];

    let converted = moment(startDate).tz(timezone);
    let startDateString = converted.format();
    converted = moment(endDate).tz(timezone);
    let endDateString = converted.format();

    query = `
      WITH diff_pivot AS (
        SELECT
          station_id,
          MAX(value_d) FILTER (WHERE standard_name = 'Tair_1_Avg') AS Tair_1_Avg,
          MAX(value_d) FILTER (WHERE standard_name = 'Tair_2_Avg') AS Tair_2_Avg,
          MAX(value_d) FILTER (WHERE standard_name = 'RH_1_Avg') AS RH_1_Avg,
          MAX(value_d) FILTER (WHERE standard_name = 'RH_2_Avg') AS RH_2_Avg
        FROM ${tableName}
        WHERE standard_name IN ('Tair_1_Avg', 'Tair_2_Avg', 'RH_1_Avg', 'RH_2_Avg')
        GROUP BY station_id, timestamp
      )
      
      (
        SELECT station_id, standard_name, '24hr_min', MIN(value_d), NULL::timestamp with time zone
        FROM ${tableName}
        WHERE standard_name IN ('BattVolt', 'CellQlt', 'CellStr')
        GROUP BY station_id, standard_name
      )
      UNION ALL
      (
        SELECT station_id, standard_name, '24hr_max', MAX(value_d), NULL::timestamp with time zone
        FROM ${tableName}
        WHERE standard_name IN ('RHenc')
        GROUP BY station_id, standard_name
      )
      UNION ALL
      (
        SELECT station_id, standard_name, '24hr_>50', SUM(CASE WHEN value_d > 50 then 1 ELSE 0 END) / CAST(COUNT(value_d) AS FLOAT) * 100, NULL
        FROM ${tableName}
        WHERE standard_name IN ('RHenc')
        GROUP BY station_id, standard_name
      )
      UNION ALL
      (
        SELECT station_id, standard_name, '24hr_>75', SUM(CASE WHEN value_d > 75 then 1 ELSE 0 END) / CAST(COUNT(value_d) AS FLOAT) * 100, NULL
        FROM ${tableName}
        WHERE standard_name IN ('RHenc')
        GROUP BY station_id, standard_name
      )
      UNION ALL
      (
        SELECT station_id, 'Tair_Avg', '24hr_avg_diff', AVG(Tair_1_Avg - Tair_2_Avg), NULL::timestamp with time zone
        FROM diff_pivot
        GROUP BY station_id
      )
      UNION ALL
      (
        SELECT station_id, 'RH_Avg', '24hr_avg_diff', AVG(RH_1_Avg - RH_2_Avg), NULL::timestamp with time zone
        FROM diff_pivot
        GROUP BY station_id
      )
    `;

    if(params.length > 0) {
      query += `
        UNION ALL
        (
          SELECT DISTINCT ON (station_id, standard_name)
            station_id,
            standard_name,
            '24hr_latest',
            value_d,
            timestamp
          FROM ${tableName}
          WHERE standard_name IN (${inlineParams.join(",")})
        );
      `;
    }
    else {
      query += ";";
    }

    data = [];
    try {
      data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
        let rows = [];
        const chunkSize = 10000;
        let chunk: any[];
        do {
          chunk = await cursor.read(chunkSize);
          for(let row of chunk) {
            rows.push(row);
          }
        }
        while(chunk.length > 0)
        return rows;
      }, { rowMode: "array" });
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    let results = {};
    for(let row of data) {
      let [ stationID, variable, type, value, timestamp ] = row;
      let stationData = results[stationID];
      if(!stationData) {
        stationData = {};
        results[stationID] = stationData;
      }
      let typeData = stationData[type];
      if(!typeData) {
        typeData = {};
        stationData[type] = typeData;
      }
      if(type == "24hr_latest") {
        typeData[variable] = {
          value,
          timestamp
        };
      }
      else {
        typeData[variable] = value;
      }
    }

    results = {
      coverage: [startDateString, endDateString],
      data: results
    }

    reqData.code = 200;
    return res.status(200)
    .json(results);

  });
});



async function getLocationTimezone(location: string) {
  let query = `SELECT timezone FROM timezone_map WHERE location = $1`;
  let { timezone } = await mesonetDBUser.query(query, [location], async (cursor: Cursor) => {
    return (await cursor.read(1))[0];
  });
  return timezone;
}


async function getStartDate(location: string, stationIDs: string[]): Promise<string> {
  let measurementsTable = getMeasurementsTable(location);
  let mainWhereClauses: string[] = [];
  let params: string[] = [];
  if(stationIDs.length > 0) {
    parseParams(stationIDs, params, mainWhereClauses, `${measurementsTable}.station_id`);
  }
  let mainWhereClause = "";
  if(mainWhereClauses.length > 0) {
    mainWhereClause = `WHERE ${mainWhereClauses.join(" AND ")}`;
  }
  
  let query = `
    SELECT timestamp
    FROM ${measurementsTable}
    ${mainWhereClause}
    ORDER BY timestamp
    LIMIT 1;
  `;

  let data = await mesonetDBUser.query(query, params, async (cursor: Cursor) => {
    return await cursor.read(1);
  });

  let timestamp = null;
  if(data.length > 0) {
    timestamp = data[0].timestamp;
  }

  return timestamp;
}







class MesonetCSVWriter {
  private state: WriteStateConfig;
  private outstream: fs.WriteStream;
  private varMetadata: VariableMetadata[];
  private timezone: string;

  constructor(outfile: string, varMetadata: VariableMetadata[], timezone: string, limit: number, offset: number) {
    this.outstream = fs.createWriteStream(outfile);
    this.varMetadata = varMetadata;
    this.timezone = timezone;
    this.state = { limit, offset, totalRecordsRead: 0, lastRecordsRead: 0, totalRowsWritten: 0, lastRowsWritten: 0, writeHeader: true, finished: false };
  }

  async write(values: MesonetMeasurementValue[]): Promise<void> {
    if(this.state.finished) {
      throw new Error("Attempting to write to stream after finished");
    }
    
    let lastRecordsRead = 0;
    let lastRowsWritten = 0;
  
    if(!this.state.header || !this.state.index) {
      this.state.index = {};
      this.state.header = [`Timestamp (${this.timezone})`, "Station ID"];
      for(let item of this.varMetadata) {
        let { display_name, units, standard_name } = item;
        let headerVar = display_name;
        if(units) {
          headerVar += ` (${units})`;
        }
        this.state.index[standard_name] = this.state.header.length;
        this.state.header.push(headerVar);
      }
    }
    if(this.state.writeHeader) {
      await this.write2Outstream(this.state.header);
      this.state.writeHeader = false;
    }
  
    let pivotedRow: string[] = this.state.partialRow;
    let currentTS = pivotedRow ? pivotedRow[0] : "";
    let currentSID = pivotedRow ? pivotedRow[1] : "";
    for(let row of values) {
      this.state.totalRecordsRead += 1;
      lastRecordsRead += 1;
  
      let { timestamp, station_id, variable, value } = row;
      let converted = moment(timestamp).tz(this.timezone);
      timestamp = converted.format("YYYY-MM-DD HH:mm:ss");
      
      if(timestamp != currentTS || station_id != currentSID) {
        if(pivotedRow) {
          if(this.state.offset > 0) {
            this.state.offset--;
          }
          else {
            if(--this.state.limit < 1) {
              this.state.partialRow = pivotedRow;
              await this.end();
              lastRowsWritten += 1;
              break;
            }
            else {
              await this.write2Outstream(pivotedRow);
              this.state.totalRowsWritten += 1;
              lastRowsWritten += 1;
            }
          }
        }
        pivotedRow = new Array(this.state.header!.length).fill(null);
        pivotedRow[0] = timestamp;
        pivotedRow[1] = station_id;
      }
      let valueIndex = this.state.index![variable];
      pivotedRow![valueIndex] = value;
      
      currentTS = timestamp;
      currentSID = station_id;
    }
    this.state.partialRow = pivotedRow;
    this.state.lastRecordsRead = lastRecordsRead;
    this.state.lastRowsWritten = lastRowsWritten;
  }

  get totalRecordsRead() {
    return this.state.totalRecordsRead;
  }

  get lastRecordsRead(){
    return this.state.lastRecordsRead;
  }
  
  get totalRowsWritten(){
    return this.state.totalRowsWritten;
  }
  
  get lastRowsWritten(){
    return this.state.lastRowsWritten;
  }
  
  get finished(){
    return this.state.finished;
  }
  
  async end(): Promise<void> {
    if(!this.state.finished) {
      return new Promise<void>(async (accept) => {
        this.outstream.once("finish", () => {
          accept();
        });
        this.state.finished = true;
        await this.flush();
        this.outstream.end();
      });
    }
  }

  private async write2Outstream(row: string[]): Promise<void> {
    await new Promise<void>((accept, reject) => {
      let dataStr = stringify([row]);
      let written = this.outstream.write(dataStr, (e) => {
        if(e) {
          reject(e);
        }
      });
      if(written) {
        accept();
      }
      else {
        this.outstream.once("drain", () => {
          accept();
        });
      }
    });
  }

  private async flush() {
    if(this.state.partialRow) {
      if(this.state.offset > 0) {
        this.state.offset--;
      }
      else {
        await this.write2Outstream(this.state.partialRow);
        this.state.totalRowsWritten += 1;
        this.state.limit--;
      }
    }
  }
}


class QueryWindow {
  private static WINDOW_UNIT: DurationInputArg2 = "hour";
  private date: Moment;
  private startDate: Moment;
  private endDate: Moment;
  private _reverse: boolean;
  private _windowSize: [DurationInputArg1, DurationInputArg2];

  //if no start date provided need to query stations for earliest record
  constructor(windowSize: number, startDate: string, endDate?: string, reverse: boolean = false) {
    this._windowSize = [windowSize, QueryWindow.WINDOW_UNIT]
    this.startDate = moment(startDate);
    this.endDate = endDate ? moment(endDate) : moment();
    this.date = reverse? this.startDate.clone() : this.endDate.clone();
    this._reverse = reverse;
  }

  get windowUnit() {
    return this._windowSize[1];
  }

  get windowSize() {
    return <number>this._windowSize[0];
  }

  set windowSize(size: number) {
    this._windowSize[0] = Math.max(size, 1);
  }

  get window(): [string, string] | null {
    if(this._reverse) {
      return this.forwardWindow;
    }
    else {
      return this.backwardWindow;
    }
    
  } 

  advanceWindow() {
    if(this._reverse) {
      this.moveWindowForward();
    }
    else {
      this.moveWindowBackward();
    }
  }

  private get forwardWindow(): [string, string] | null {
    if(this.date.isSameOrAfter(this.endDate)) {
      return null;
    }
    let date = this.date.clone();
    let startDate = date.toISOString();
    date.add(...this._windowSize);
    if(date.isAfter(this.endDate)) {
      date = this.endDate.clone();
    }
    let endDate = date.toISOString();
    return [startDate, endDate];
  }

  private get backwardWindow(): [string, string] | null {
    if(this.date.isSameOrBefore(this.startDate)) {
      return null;
    }
    let date = this.date.clone();
    let endDate = date.toISOString();
    date.subtract(...this._windowSize);
    if(date.isBefore(this.startDate)) {
      date = this.startDate.clone();
    }
    let startDate = date.toISOString();
    return [startDate, endDate];
  }


  private moveWindowForward() {
    this.date.add(...this._windowSize);
    if(this.date.isAfter(this.endDate)) {
      this.date = this.endDate.clone();
    }
  }

  private moveWindowBackward() {
    this.date.subtract(...this._windowSize);
    if(this.date.isBefore(this.startDate)) {
      this.date = this.startDate.clone();
    }
  }

  get reverse() {
    return this._reverse;
  }
}


interface WriteStateConfig {
  limit: number,
  offset: number,
	writeHeader: boolean,
  totalRecordsRead: number,
  lastRecordsRead: number,
  totalRowsWritten: number,
  lastRowsWritten: number,
  finished: boolean,
	partialRow?: string[],
	header?: string[],
	index?: {[variable: string]: number}
}

interface VariableMetadata {
	display_name: string,
	units: string,
  units_plain: string,
  units_expanded: string,
	standard_name: string
}

interface StationMetadata {
	station_id: string,
	name: string,
  full_name: string,
	lat: number,
	lng: number,
	elevation: number,
  status: string,
  location: string,
  timezone: string
}

interface MesonetMeasurementValue {
	timestamp: string,
  station_id: string,
  variable: string,
  value: string,
  data_quality?: number,
  flags?: number,
  screening_method?: number,
  notes?: string,
	units?: string,
	units_plain?: string,
  units_expanded: string,
	variable_display_name?: string
	interval_seconds?: string,
	station_name?: string,
	lat?: number,
	lng?: number,
	elevation?: number
}                     

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////// flags //////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATA_QUALITY_VALUES = [
  "good",
  "suspect",
  "bad"
] as const;
type DataQualityValuesKey = typeof DATA_QUALITY_VALUES[number];
const DATA_QUALITY_TRANSLATIONS: TwoWayMap<DataQualityValuesKey, number> = new TwoWayMap<DataQualityValuesKey, number>([
  ["good", 0],
  ["suspect", 1],
  ["bad", 2]
]);
const FLAG_LEVEL_QUALITY_MAP: DataQualityValuesKey[] = ["good", "good", "suspect", "bad"];

const FLAG_METHOD_VALUES = [
  "manual",
  "automatic"
] as const;
type FlagMethodValuesKey = typeof FLAG_METHOD_VALUES[number];
const FLAG_METHOD_TRANSLATIONS: TwoWayMap<FlagMethodValuesKey, number> = new TwoWayMap<FlagMethodValuesKey, number>([
  ["manual", 0],
  ["automatic", 1]
]);
const FLAG_VALUES = [
  "na",
  "good",
  "suspect",
  "bad"
] as const;
type FlagValuesKey = typeof FLAG_VALUES[number];
const FLAG_TRANSLATIONS: TwoWayMap<FlagValuesKey, number> = new TwoWayMap<FlagValuesKey, number>([
  ["na", 0],
  ["good", 1],
  ["suspect", 2],
  ["bad", 3]
]);

const FLAGS = [
  "exc_range",
  "dual_sens_diff",
  "derived_var",
  "incosis_nearby",
  "timestamp_err",
  "stuck_err",
  "iqr_err",
  "roc_err"
] as const;
type FlagKey = typeof FLAGS[number];
type BitMap = {
  start: number,
  width: number
};
type FlagBits = Record<FlagKey, BitMap>;
const FLAG_BITS: FlagBits = {
  "exc_range": {
    start: 0,
    width: 2
  },
  "dual_sens_diff": {
    start: 2,
    width: 2
  },
  "derived_var": {
    start: 4,
    width: 2
  },
  "incosis_nearby": {
    start: 6,
    width: 2
  },
  "timestamp_err": {
    start: 8,
    width: 2
  },
  "stuck_err": {
    start: 10,
    width: 2
  },
  "iqr_err": {
    start: 12,
    width: 2
  },
  "roc_err": {
    start: 14,
    width: 2
  }
} as const;

function encodeFlags(flags: Partial<Record<FlagKey, number | FlagValuesKey>>) {
  let encodedValue = 0;
  for(let flag in flags) {
    const typedFlag = flag as FlagKey;
    const { start, width } = FLAG_BITS[typedFlag];
    const flagMask = (1 << width) - 1;
    const flagValue = flags[typedFlag];
    const parsedFlagValue = typeof flagValue === "number" ? flagValue : FLAG_TRANSLATIONS.lookup(flagValue.toLowerCase() as FlagValuesKey)
    const masked = parsedFlagValue & flagMask
    const encodedFlag = masked << start;
    encodedValue |= encodedFlag;
  }
  return encodedValue;
}

function decodeFlags(encodedValue: number, numeric: boolean): Record<FlagKey, FlagValuesKey | number> {
  const decoded = {} as Record<FlagKey, FlagValuesKey | number>;
  for (const flag of FLAGS) {
    const { start, width } = FLAG_BITS[flag];
    const flagMask = (1 << width) - 1;
    const flagValue = (encodedValue >>> start) & flagMask;
    decoded[flag] = numeric ? flagValue : FLAG_TRANSLATIONS.reverseLookup(flagValue);
  }

  return decoded;
}



type FlagData = {
  stationId: string,
  variable: string,
  timestamp: string | { start: string, end?: string },
  screeningMethod?: number | string,
  flags?: Partial<Record<FlagKey, number | FlagValuesKey>>,
  notes?: string
};

router.patch("/mesonet/db/setFlags", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    let { overwrite, location, data }: { overwrite: boolean, location: string, data: FlagData | FlagData[] } = req.body;
    
    overwrite = parseBoolParam(overwrite, true);

    if(!mesonetLocations.includes(location)) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(`Invalid location provided.`);
    }

    let measurementTable = getMeasurementsTable(location);
    let flagTable = getFlagTable(location);

    const validateFlagData = (flagData: FlagData) => {
      const { stationId, variable, timestamp, screeningMethod, flags, notes } = flagData;

      const validateFlagObject = () => {
        if(flags === undefined) return true;
        
        for(let flag in flags) {
          const typedFlag = flag as FlagKey;
          
          if(!FLAG_BITS[typedFlag]) {
            return false;
          }
          const flagValue = flags[typedFlag];
          if(flagValue !== undefined && !validateType(flagValue, ["string", "number"])) {
            return false;
          }
          if(typeof flagValue === "string" && FLAG_TRANSLATIONS.lookup(flagValue.toLowerCase() as FlagValuesKey) === undefined) {
            return false;
          }
          if(typeof flagValue === "number" && FLAG_TRANSLATIONS.reverseLookup(flagValue) === undefined) {
            return false;
          }
        }
        return true;
      }

      return validateType(stationId, ["string"]) &&
        validateType(variable, ["string"]) &&
        validateRangedTimestamp(timestamp) &&
        (
          screeningMethod === undefined ||
          (typeof screeningMethod === "string" && FLAG_METHOD_TRANSLATIONS.lookup(screeningMethod.toLowerCase() as FlagMethodValuesKey) !== undefined) ||
          (typeof screeningMethod === "number" && FLAG_METHOD_TRANSLATIONS.reverseLookup(screeningMethod) !== undefined)
        ) &&
        (notes === undefined || typeof notes === "string") &&
        validateFlagObject();
    }

    const isValidData = Array.isArray(data) ? validateArray(data, validateFlagData) : validateFlagData(data as FlagData);

    if(!data || !isValidData) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(`Invalid data object provided. Must be an object containing valid flag data or an array of valid flag data`);
    }

    const flagDataArray = Array.isArray(data) ? data : [data];
    if(flagDataArray.length === 0) {
      reqData.code = 200;
      return res.status(200).json({ modified: 0 });
    }

    // Prepare columnar arrays (nullable for partial updates)
    let stationIds: string[] = [];
    let variables: string[] = [];
    let startTimes: string[] = [];
    let endTimes: string[] = [];
    let dataQualities: (number | null)[] = [];
    let encodedFlagsArr: (number | null)[] = [];
    let screeningMethods: (number | null)[] = [];
    let notesArr: (string | null)[] = [];

    for(const item of flagDataArray) {
      const { stationId, variable, timestamp, screeningMethod, flags, notes } = item;

      let encodedFlags: number | null = null;
      let dataQuality: number | null = null;

      if(flags !== undefined) {
        encodedFlags = encodeFlags(flags);
        dataQuality = 0;
        let worstFlag = 0;
        for(let flag in flags) {
          const typedFlag = flag as FlagKey;
          const flagValue = flags[typedFlag];
          const parsedFlagValue = typeof flagValue === "number" ? flagValue : FLAG_TRANSLATIONS.lookup(flagValue.toLowerCase() as FlagValuesKey);
          if(worstFlag < parsedFlagValue) {
            worstFlag = parsedFlagValue;
          }
        }
        dataQuality = DATA_QUALITY_TRANSLATIONS.lookup(FLAG_LEVEL_QUALITY_MAP[worstFlag]);
      }

      let parsedScreeningMethod: number | null = null;
      if (screeningMethod !== undefined) {
        parsedScreeningMethod = typeof screeningMethod === "number" 
          ? screeningMethod 
          : FLAG_METHOD_TRANSLATIONS.lookup(screeningMethod.toLowerCase() as FlagMethodValuesKey);
      }

      let startStr: string;
      let endStr: string;
      
      if (typeof timestamp === "string") {
        startStr = new Date(timestamp).toISOString();
        endStr = startStr; 
      } else {
        startStr = new Date(timestamp.start).toISOString();
        // use infinity if no end provided to go through end of data
        endStr = timestamp.end ? new Date(timestamp.end).toISOString() : 'infinity';
      }

      stationIds.push(stationId);
      variables.push(variable);
      startTimes.push(startStr);
      endTimes.push(endStr);
      dataQualities.push(dataQuality);
      encodedFlagsArr.push(encodedFlags);
      screeningMethods.push(parsedScreeningMethod);
      notesArr.push(notes !== undefined ? notes : null);
    }

    let onConflict = overwrite ? `
      DO UPDATE SET
        data_quality = EXCLUDED.data_quality,
        flags = EXCLUDED.flags,
        screening_method = EXCLUDED.screening_method,
        notes = EXCLUDED.notes;
    ` : "DO NOTHING;";

    // Use LEFT JOIN to handle partial updates
    let query = `
      INSERT INTO ${flagTable} (timestamp, station_id, variable, data_quality, flags, screening_method, notes)
      SELECT DISTINCT ON (m.timestamp, m.station_id, v.standard_name)
        m.timestamp, 
        m.station_id, 
        v.standard_name, 
        COALESCE(i.data_quality, existing.data_quality, 0::smallint), 
        COALESCE(i.flags, existing.flags, 0::integer), 
        COALESCE(i.screening_method, existing.screening_method, 0::smallint),
        COALESCE(i.notes, existing.notes)
      FROM UNNEST(
        $1::character(4)[],           -- stationIds
        $2::character varying(255)[], -- variables
        $3::timestamptz[],            -- startTimes
        $4::timestamptz[],            -- endTimes
        $5::smallint[],               -- dataQualities
        $6::integer[],                -- encodedFlagsArr
        $7::smallint[],               -- screeningMethods
        $8::text[]                    -- notesArr
      ) AS i(station_id, variable, start_time, end_time, data_quality, flags, screening_method, notes)
      JOIN ${measurementTable} m ON m.station_id = i.station_id
           AND m.timestamp >= i.start_time 
           AND m.timestamp <= i.end_time
      JOIN version_translations v ON v.program = m.version 
           AND v.alias = m.variable
           AND v.standard_name = i.variable
      LEFT JOIN ${flagTable} existing ON existing.station_id = m.station_id 
           AND existing.timestamp = m.timestamp 
           AND existing.variable = v.standard_name
      ON CONFLICT (timestamp, station_id, variable)
      ${onConflict}
    `;

    let params = [
      stationIds, 
      variables, 
      startTimes, 
      endTimes, 
      dataQualities, 
      encodedFlagsArr, 
      screeningMethods,
      notesArr
    ];

    try {
      let modified = await mesonetDBAdmin.queryNoRes(query, params);
      
      reqData.code = 200;
      return res.status(200).json({ modified });
    }
    catch (e) {
      reqData.success = false;
      reqData.code = 500;
      return res.status(500).send(`Database error during batch update`);
    }
  });
});