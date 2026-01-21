import express from "express";
import { rateLimit } from "express-rate-limit";
import moment, { DurationInputArg1, DurationInputArg2, Moment } from "moment-timezone";
import { mesonetDBAdmin, mesonetDBUser, pgStoreMesonetEmail, pgStoreSlowMesonetMeasurements } from "../../../modules/util/resourceManagers/db.js";
import * as fs from "fs";
import * as path from "path";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers.js";
import { apiURL, downloadRoot, mesonetLocations } from "../../../modules/util/config.js";
import { sendEmail } from "../../../modules/util/util.js";
import { stringify } from "csv-stringify/sync";
import * as crypto from "crypto";
import { parseListParam, parseParams } from "../../../modules/util/dbUtil.js";
import { slowDown } from "express-slow-down";

export const router = express.Router();

interface QueryData {
  query: string,
  params: any[],
  index: string[]
}

const mesonetMeasurementSlow = slowDown({
	windowMs: 60 * 1000, // 1 minute window
	delayAfter: 50, // Dalay after 50 requests.
  delayMs: (hits) => 1000 * (hits - 50), // delay by 1 second * number of hits over 50
  store: pgStoreSlowMesonetMeasurements
});

const mesonetEmailLimiter = rateLimit({
	windowMs: 15 * 60 * 1000, // 1 minute window
	limit: 5, // Limit each IP to 100 requests per `window` (here, per 15 minutes).
	standardHeaders: "draft-8", // draft-6: `RateLimit-*` headers; draft-7 & draft-8: combined `RateLimit` header
	legacyHeaders: false, // Disable the `X-RateLimit-*` headers.
  message: "Too many requests from this IP. Requests for this endpoint are limited to 5 per 15 minutes.",
  store: pgStoreMesonetEmail
});

function constructBaseMeasurementsQuery(stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], flags: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean, selectFlag: boolean = true): QueryData {
  let measurementsTable = `${location}_measurements_tsdb`;

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
    mainWhereClauses.push(`timestamp >= $${params.length}`);
  }

  if(endDate) {
    params.push(endDate);
    mainWhereClauses.push(`timestamp <= $${params.length}`);
  }

  if(flags.length > 0) {
    parseParams(flags, params, mainWhereClauses, "flag");
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

  let query = joinMetadata ? `
    WITH variable_metadata_combined AS (
      SELECT standard_name, display_name, unit_metadata.units, units_plain, units_expanded
      FROM variable_metadata_2
      LEFT JOIN unit_metadata ON variable_metadata_2.units = unit_metadata.units
    )
    SELECT
      timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value,
      ${selectFlag ? "flag," : ""}
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
    JOIN station_metadata ON station_metadata.station_id = ${measurementsTable}.station_id JOIN variable_metadata_combined ON variable_metadata_combined.standard_name = variable_data.standard_name
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  ` : `
    SELECT
      timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value
      ${selectFlag ? ", flag" : ""}
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  `;


  let index = ["station_id", "timestamp", "variable", "value"];
  if(selectFlag) {
    index.push("flag");
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

async function constructMeasurementsQuery(crosstabQuery: boolean, stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], flags: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean): Promise<QueryData> {
  let queryData: QueryData;
  
  if(crosstabQuery) {
    //don't join metadata for now, can only join station metadata, join after crosstab
    //flag cannot be included
    queryData = constructBaseMeasurementsQuery(stationIDs, startDate, endDate, varIDs, intervals, flags, location, limit, offset, reverse, false, false);
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
    queryData = constructBaseMeasurementsQuery(stationIDs, startDate, endDate, varIDs, intervals, flags, location, limit, offset, reverse, joinMetadata);
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
  let queryHandler = await mesonetDBUser.query(query, params, { rowMode: "array" });
  let data = await queryHandler.read(10000);
  queryHandler.close();
  data = data.flat();
  return data;
}


router.get("/mesonet/db/measurements", mesonetMeasurementSlow, async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_ids, start_date, end_date, var_ids, intervals, flags, location, limit = 10000, offset, reverse, join_metadata, local_tz, row_mode }: any = req.query;

    // reqData.success = false;
    // reqData.code = 503;

    // return res.status(503)
    // .send("This resource is temporarily unavailable.");

    let varIDs = parseListParam(var_ids);
    let stationIDs = parseListParam(station_ids);
    let flagArr = parseListParam(flags);
    let intervalArr = parseListParam(intervals);

    const MAX_QUERY = 1000000;

    //validate location, can use direct in query
    //default to hawaii
    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }

    //check if should crosstab the query (wide mode) and if query should return results as array or JSON
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

    if(offset) {
      offset = parseInt(offset, 10);
      if(isNaN(offset)) {
        offset = undefined;
      }
    }
    if(typeof limit === "string") {
      limit = parseInt(limit, 10)
      if(isNaN(limit)) {
        limit = 10000;
      }
    }
    //limit must be less than max, translate 0 or negative numbers as max
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
  
        return res.status(400)
        .send("Invalid start date format. Dates must be ISO 8601 compliant.");
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
  
        return res.status(400)
        .send("Invalid end date format. Dates must be ISO 8601 compliant.");
      }
    }


    let data: any[] | { index: string[], data: any[] } = [];
    let { query, params, index } = await constructMeasurementsQuery(crosstabQuery, stationIDs, start_date, end_date, varIDs, intervalArr, flagArr, location, limit, offset, reverse, join_metadata);
    if(query) {
      try {
        let queryHandler = await mesonetDBUser.query(query, params, {rowMode: row_mode});
        const chunkSize = 10000;
        let chunk: any[];
        do {
          chunk = await queryHandler.read(chunkSize);
          data = data.concat(chunk);
        }
        while(chunk.length > 0)
        queryHandler.close();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
  
        return res.status(400)
        .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
      }
    }

    if(data.length > 0 && local_tz) {
      let query = `SELECT timezone FROM timezone_map WHERE location = $1`;
      let queryHandler = await mesonetDBUser.query(query, [location]);
      let { timezone } = (await queryHandler.read(1))[0];
      queryHandler.close();
      if(row_mode === "array") {
        let tsIndex = index.indexOf("timestamp");
        for(let row of data) {
          let converted = moment(row[tsIndex]).tz(timezone);
          row[tsIndex] = converted.format();
        }
      }
      else {
        for(let row of data) {
          let converted = moment(row.timestamp).tz(timezone);
          row.timestamp = converted.format();
        }
      }
    }
    //if array form wrap with index
    if(row_mode === "array" || row_mode == "wide_array") {
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



function constructMeasurementsQueryEmail(stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], flags: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean): QueryData {
	// varIDs = varIDs.map((id: string) => id.toLowerCase());

	let measurementsTable = `${location}_measurements_tsdb`;

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
    mainWhereClauses.push(`timestamp >= $${params.length}`);
  }

  if(endDate) {
    params.push(endDate);
    mainWhereClauses.push(`timestamp <= $${params.length}`);
  }

  if(flags.length > 0) {
    parseParams(flags, params, mainWhereClauses, "flag");
  }

  let mainWhereClause = "";
  if(mainWhereClauses.length > 0) {
    mainWhereClause = `WHERE ${mainWhereClauses.join(" AND ")}`;
  }


  ///////////////////////////////////////////////////
  /////////////// limit offset clause ///////////////
  ///////////////////////////////////////////////////

  let limitOffsetClause = "";
  if(Number.isFinite(limit)) {
    params.push(limit.toString());
    limitOffsetClause += `LIMIT $${params.length}`;
  }
  
  if(offset) {
    params.push(offset.toString());
    limitOffsetClause += ` OFFSET $${params.length}`;
  }
  
  let query = joinMetadata ? `
    WITH variable_metadata_combined AS (
      SELECT standard_name, display_name, unit_metadata.units, units_plain, units_expanded
      FROM variable_metadata_2
      LEFT JOIN unit_metadata ON variable_metadata_2.units = unit_metadata.units
    )
    SELECT
      timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value,
      flag,
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
    JOIN station_metadata ON station_metadata.station_id = ${measurementsTable}.station_id JOIN variable_metadata_combined ON variable_metadata_combined.standard_name = variable_data.standard_name
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  ` : `
    SELECT
      timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value,
      flag
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  `;


  let index = ["station_id", "timestamp", "variable", "value", "flag"];
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
    FROM variable_metadata_2
    LEFT JOIN unit_metadata ON variable_metadata_2.units = unit_metadata.units
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
      let queryHandler = await mesonetDBUser.query(query, params, {rowMode: row_mode});

      const chunkSize = 10000;
      let chunk: any[];
      do {
        chunk = await queryHandler.read(chunkSize);
        data = data.concat(chunk);
      }
      while(chunk.length > 0)
      queryHandler.close();
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
      let queryHandler = await mesonetDBUser.query(query, params, {rowMode: row_mode});

      const chunkSize = 10000;
      let chunk: any[];
      do {
        chunk = await queryHandler.read(chunkSize);
        data = data.concat(chunk);
      }
      while(chunk.length > 0)
      queryHandler.close();
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
      let queryHandler = await mesonetDBUser.query(query, params);
      data = await queryHandler.read(100000);
      queryHandler.close();
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
    let queryHandler = await mesonetDBUser.query(query, []);
    let data = await queryHandler.read(100000);
    queryHandler.close();
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
    queryHandler = await mesonetDBUser.query(query, []);
    data = await queryHandler.read(100000);
    queryHandler.close();
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
    queryHandler = await mesonetDBUser.query(query, []);
    data = await queryHandler.read(100000);
    queryHandler.close();
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
    queryHandler = await mesonetDBUser.query(query, []);
    data = await queryHandler.read(100000);
    queryHandler.close();
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

    const table_name = `${location}_measurements_tsdb`;

    res.set("Content-Type", "text/csv");
    res.set("Content-Disposition", `attachment; filename="sff_data.csv"`);
    let query = `
      SELECT ${table_name}.station_id, station_metadata.lat, station_metadata.lng, station_metadata.elevation, ${table_name}.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_height, CASE WHEN ${table_name}.value IS NOT NULL THEN CAST(${table_name}.value AS DECIMAL) * synoptic_translations.unit_conversion_coefficient ELSE NULL END AS value
      FROM ${table_name}
      JOIN version_translations ON version_translations.program = ${table_name}.version AND version_translations.alias = ${table_name}.variable
      JOIN synoptic_translations ON version_translations.standard_name = synoptic_translations.standard_name
      JOIN station_metadata ON station_metadata.station_id = ${table_name}.station_id
      LEFT JOIN sensor_positions ON sensor_positions.station_id = ${table_name}.station_id AND version_translations.standard_name = sensor_positions.standard_name
      WHERE timestamp >= NOW() - '6 hours'::INTERVAL AND flag = 0 AND NOT EXISTS (SELECT 1 FROM synoptic_exclude WHERE synoptic_exclude.station_id = ${table_name}.station_id AND synoptic_exclude.standard_name = version_translations.standard_name)
      ORDER BY ${table_name}.station_id, ${table_name}.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_number;
    `;

    let queryHandler = await mesonetDBUser.query(query, []);
    let data = await queryHandler.read(100000);
    queryHandler.close();
    
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


router.patch("/mesonet/db/setFlag", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_id: stationID, variable, timestamp, flag }: any = req.body;

    if(!(stationID && variable && /^[0-9]+$/.test(flag) && timestamp && ((typeof timestamp === "string" && !isNaN(Date.parse(timestamp))) || (!isNaN(Date.parse(timestamp.start)) && !isNaN(Date.parse(timestamp.end)))))) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`Request body should include the following fields: \n\
        station_id: The station ID to set the flag for \n\
        variable: The variable to set the flag for \n\
        timestamp: Either an ISO 8601 formatted timestamp string or a JSON object containing the properties "start" and "end" indicating a range of timestamps to set the flag for \n\
        flag: An integer indicating the value to set the flag to.`);
    }
    
    let query = `
        SELECT table_name
        FROM station_metadata
        JOIN measurement_table_map ON station_metadata.location = measurement_table_map.location
        WHERE station_metadata.station_id = $1;
    `;

    let data: {table_name: string}[] = []
    try {
      let queryHandler = await mesonetDBUser.query(query, [stationID]);
      data = await queryHandler.read(1);
      queryHandler.close();
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    if(data.length < 1) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send(`Station ID ${stationID} not found.`);
    }
    let tableName = data[0].table_name;

    query = `
      UPDATE ${tableName}
      SET flag = $1
      FROM version_translations
      WHERE version_translations.program = ${tableName}.version AND version_translations.alias = ${tableName}.variable AND station_id = $2 AND standard_name = $3
    `;
    let params = [flag, stationID, variable];
    if(typeof timestamp == "string") {
      timestamp = new Date(timestamp).toISOString();
      query += " AND timestamp = $4;";
      params.push(timestamp);
    }
    else {
      timestamp.start = new Date(timestamp.start).toISOString();
      timestamp.end = new Date(timestamp.end).toISOString();
      query += "AND timestamp >= $4 AND timestamp <= $5;";
      params.push(timestamp.start);
      params.push(timestamp.end);
    }

    let modified = 0;
    try {
      modified = await mesonetDBAdmin.queryNoRes(query, params);
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    reqData.code = 200;
    return res.status(200)
    .json({ modified });
  });
});


router.put("/mesonet/db/measurements/insert", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    let { overwrite, location, data }: any = req.body;

    if(!Array.isArray(data)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`Invalid data provided. Data must be a 2D array with 7 element rows.`);
    }

    if(data.length < 1) {
      reqData.code = 200;
      return res.status(200)
      .json({ modified: 0 });
    }

    if(!mesonetLocations.includes(location)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`Invalid location provided.`);
    }

    let onConflict = overwrite ? `
      DO UPDATE SET
        version = EXCLUDED.version,
        value = EXCLUDED.value,
        flag = EXCLUDED.flag;
      ` : "DO NOTHING;";

    let params: string[] = [];
    let valueClauseParts: string[] = [];
    for(let row of data) {
      if(!Array.isArray(row) || row.length != 7) {
        reqData.success = false;
        reqData.code = 400;

        return res.status(400)
        .send(`Invalid data provided. Data must be a 2D array with 7 element rows.`);
      }

      let rowParts: string[] = [];
      for(let value of row) {
        params.push(value);
        rowParts.push(`$${params.length}`);
      }
      valueClauseParts.push(rowParts.join(","));
    }
    let valueClause = `(${valueClauseParts.join("),(")})`;

    let query = `
      INSERT INTO ${location}_measurements_tsdb
      VALUES ${valueClause}
      ON CONFLICT (timestamp, station_id, variable)
      ${onConflict}
    `;

    try {
      let modified = await mesonetDBAdmin.queryNoRes(query, params);
      reqData.code = 200;
      return res.status(200)
      .json({ modified });
    }
    catch(e: any) {
      if(e.code.startsWith("42")) {
        reqData.success = false;
        reqData.code = 400;
  
        return res.status(400)
        .send(`Invalid query syntax. Please validate the data provided is correctly formatted.`);
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

      //send error
      return res.status(400)
      .send(
        `Request body should include the following fields: \n\
        email: The email to send the package to \n\
        data: A JSON object with parameters for the Mesonet query \n\
        outputName (optional): What to name the produced data file. Default: data.csv`
      );
    }

    let { station_ids, start_date, end_date, var_ids, intervals, flags, location, limit, offset, reverse, local_tz }: any = data;

    let varIDs = var_ids || [];
    let stationIDs = station_ids || [];
    let flagArr = flags || [];
    let intervalArr = intervals || [];

    if(!mesonetLocations.includes(location)) {
    	location = "hawaii";
    }

    if(typeof offset === "string") {
      offset = parseInt(limit, 10)
    }
    //translate negative numbers or undefined/invalid values as no offset
    if(offset === undefined || isNaN(offset) || offset < 0) {
      offset = 0;
    }

    if(typeof limit === "string") {
      limit = parseInt(limit, 10)
    }
    //translate 0, negative numbers, or undefined/invalid values as uncapped (queries batched)
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
    
          //send error
          return res.status(400)
          .send(
            `Invalid start date provided.`
          );
      }
    }
    else {
      start_date = await getStartDate(location, stationIDs);
    }
    //no start date provided and no data from get start date function, so sids must be invalid
    if(!start_date) {
      reqData.success = false;
      reqData.code = 400;

      //send error
      return res.status(400)
      .send(
        `Station data range could not be found. Please check the provided station IDs are valid.`
      );
    }

    if(end_date) {
      try {
          let date = new Date(end_date);
          end_date = date.toISOString();
      }
      catch(e) {
        reqData.success = false;
        reqData.code = 400;
  
        //send error
        return res.status(400)
        .send(
          `Invalid end date provided.`
        );
      }
    }

    let { query, params } = constructVariablesQuery(varIDs);
    let queryHandler = await mesonetDBUser.query(query, params);
    let varMetadata: VariableMetadata[] = await queryHandler.read(10000);
    queryHandler.close();

    if(varMetadata.length < 1) {
      reqData.success = false;
      reqData.code = 400;

      //send error
      return res.status(400)
      .send(
        `None of the provided variables exist.`
      );
    }

    //response should be sent immediately after basic parameter verification
    //202 accepted indicates request accepted but non-commital completion
    reqData.code = 202;
    res.status(202)
    .send("Request received. Your query will be processed and emailed to you if successful.");
    try {
      let timezone = local_tz ? await getLocationTimezone(location) : "UTC";

      let uuid = crypto.randomUUID();
      let fname = outputName ? outputName : "data.csv";
      let outdir = path.join(downloadRoot, uuid);
      //write paths to a file and use that, avoid potential issues from long cmd line params
      fs.mkdirSync(outdir);
      let outfile = path.join(outdir, fname);

      let writeManager = new MesonetCSVWriter(outfile, varMetadata, timezone, limit, offset);
  
      let e: any;
      try {
        let maxLimit = limit * varIDs.length + offset * varIDs.length;
  
        // let chunkSize: [DurationInputArg1, DurationInputArg2] = [1, "hour"];
        let slowStart = true;
        let backoffBaseMS = 1000;
        let minFailures = 0;
    
        let queryChunker = new QueryWindow(1, start_date, end_date, reverse);
        let window = queryChunker.window;

        while(window && maxLimit > 0 && !writeManager.finished) {
          let timeout = false;
          try {
            let [ startDate, endDate ] = window;
            ({ query, params } = constructMeasurementsQueryEmail(stationIDs, startDate, endDate, varIDs, intervalArr, flagArr, location, maxLimit, 0, reverse, false));
            queryHandler = await mesonetDBUser.query(query, params);
            const readChunkSize = 10000;
            let readChunk: MesonetMeasurementValue[];
            do {
              readChunk = await queryHandler.read(readChunkSize);
              await writeManager.write(readChunk);
              maxLimit -= writeManager.lastRecordsRead;
            }
            while(readChunk.length > 0 && maxLimit > 0 && !writeManager.finished)
            queryHandler.close();
          }
          catch(e: any) {
            //non-timeout error, rethrow error to be caught by outer handler
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
      //create download link and send in message body
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
      //set failure in status
      reqData.success = false;
      //attempt to send an error email to the user, ignore any errors
      try {
        let message = "An error occurred while generating your Mesonet data. We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later or email us at hcdp@hawaii.edu for assistance.";
        let mailOptions = {
          to: email,
          subject: "Mesonet Data Error",
          text: message,
          html: "<p>" + message + "</p>"
        };
        //try to send the error email, last try to actually notify user
        await sendEmail(mailOptions);
      }
      catch(err) {}
      //rethrow to be handled by main error handler
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

    let query = `SELECT timezone FROM timezone_map WHERE location = $1`;
    let queryHandler = await mesonetDBUser.query(query, [location]);
    let { timezone } = (await queryHandler.read(1))[0];
    
    query = `
      SELECT MIN(timestamp), MAX(timestamp)
      FROM ${tableName};
    `;

    queryHandler = await mesonetDBUser.query(query, [], { rowMode: "array" });
    let data: any[];

    data = await queryHandler.read(1);

    queryHandler.close();

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
      queryHandler = await mesonetDBUser.query(query, params, { rowMode: "array" });

      const chunkSize = 10000;
      let chunk: any[];
      do {
        chunk = await queryHandler.read(chunkSize);
        data = data.concat(chunk);
      }
      while(chunk.length > 0)
      queryHandler.close();
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
  let queryHandler = await mesonetDBUser.query(query, [location]);
  let { timezone } = (await queryHandler.read(1))[0];
  queryHandler.close();
  return timezone;
}


async function getStartDate(location: string, stationIDs: string[]): Promise<string> {
  let measurementsTable = `${location}_measurements_tsdb`;
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
  let queryHandler = await mesonetDBUser.query(query, params);
  let data = await queryHandler.read(1);
  let timestamp = null;
  if(data.length > 0) {
    timestamp = data[0].timestamp;
  }
  queryHandler.close();
  return timestamp;
}













// function convertWide(values: MesonetMeasurementValue[], varMetadata: VariableMetadata[], limit: number, offset: number, timezone: string): {[tag: string]: any}[] {
//   let pivotedData: {[tag: string]: any}[] = [];
//   let baseRow = {
//     timestamp: null,
//     station_id: null
//   };
//   for(let item of varMetadata) {
//     baseRow[item.standard_name] = null;
//   }

// 	let pivotedRow: {[tag: string]: any} | undefined;
// 	let currentTS = pivotedRow ? pivotedRow[0] : "";
// 	let currentSID = pivotedRow ? pivotedRow[1] : "";
// 	for(let row of values) {

// 		let {timestamp, station_id, variable, value} = row;
//     let converted = moment(timestamp).tz(timezone);
//     timestamp = converted.format();
    
// 		if(timestamp != currentTS || station_id != currentSID) {
// 			if(pivotedRow) {
//         if(offset > 0) {
//           offset--;
//         }
//         else {
//           pivotedData.push(pivotedRow);
//           if(--limit < 1) {
//             break;
//           }
//         }
// 			}
// 			pivotedRow = {
//         ...baseRow
//       };
// 			pivotedRow[0] = timestamp;
// 			pivotedRow[1] = station_id;
// 		}
//     pivotedRow![variable] = value;
// 		currentTS = timestamp;
// 		currentSID = station_id;
// 	}

// 	return pivotedData;
// }







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
    // let { limit, offset, writeHeader, partialRow, index, header, totalRecordsRead, totalRowsWritten } = state;
    let lastRecordsRead = 0;
    let lastRowsWritten = 0;
  
    if(!this.state.header || !this.state.index) {
      this.state.index = {};
      this.state.header = ["Timestamp", "Station ID"];
      for(let item of this.varMetadata) {
        let {display_name, units, standard_name} = item;
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
  
      let {timestamp, station_id, variable, value} = row;
      let converted = moment(timestamp).tz(this.timezone);
      timestamp = converted.format();
      
      if(timestamp != currentTS || station_id != currentSID) {
        if(pivotedRow) {
          if(this.state.offset > 0) {
            this.state.offset--;
          }
          else {
            if(--this.state.limit < 1) {
              //end will flush the partial row to the stream so no need to write again
              //set partial row to pivoted row
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
	flag: number,
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