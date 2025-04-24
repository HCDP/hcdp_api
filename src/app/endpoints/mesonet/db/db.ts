import express from "express";
import moment, { DurationInputArg1, DurationInputArg2, Moment } from "moment-timezone";
import { MesonetDBManager } from "../../../modules/util/resourceManagers/db.js";
import * as fs from "fs";
import * as path from "path";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers.js";
import { administrators, apiURL, downloadRoot, mesonetLocations, rawDataRoot } from "../../../modules/util/config.js";
import { sendEmail } from "../../../modules/util/util.js";
import { Stringifier, stringify } from "csv-stringify";
import * as crypto from "crypto";

export const router = express.Router();

interface QueryData {
  query: string,
  params: any[],
  index: string[]
}


function parseParams(paramListArr: string[], allParams: string[], whereClauses: string[], column: string) {
  let paramSet: string[] = [];
  for(let i = 0; i < paramListArr.length; i++) {
    allParams.push(paramListArr[i]);
    paramSet.push(`$${allParams.length}`);
  }
  whereClauses.push(`${column} IN (${paramSet.join(",")})`);
}

function constructBaseMeasurementsQuery(stationIDs: string[], startDate: string, endDate: string, varIDs: string[], intervals: string[], flags: string[], location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean, selectFlag: boolean = true): QueryData {
  let measurementsTable = `${location}_measurements`;

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

  let query = `
    SELECT
      timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value
      ${selectFlag ? ", flag" : ""}
      ${joinMetadata ? ", units, units_short, display_name AS variable_display_name, interval_seconds, name AS station_name, lat, lng, elevation" : ""}
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${joinMetadata ? "JOIN station_metadata ON station_metadata.station_id = " + measurementsTable + ".station_id JOIN variable_metadata ON variable_metadata.standard_name = variable_data.standard_name" : ""}
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, variable_data.standard_name
    ${limitOffsetClause}
  `;


  let index = ["station_id", "timestamp", "variable", "value"];
  if(selectFlag) {
    index.push("flag");
  }
  if(joinMetadata) {
    index = index.concat(["units", "units_short", "variable_display_name", "interval_seconds", "station_name", "lat", "lng", "elevation"]);
  }

  return {
    query,
    params,
    index
  };
}

function wrapCrosstabMeasurementsQuery(vars: string[], baseQueryData: QueryData, joinMetadata: boolean): QueryData {
  let { query, params } = baseQueryData;
  query = MesonetDBManager.mogrify(query, params);
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
  let queryHandler = await MesonetDBManager.query(query, params, { rowMode: "array" });
  let data = await queryHandler.read(10000);
  queryHandler.close();
  data = data.flat();
  return data;
}

router.get("/mesonet/db/measurements", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_ids, start_date, end_date, var_ids, intervals, flags, location, limit = 10000, offset, reverse, join_metadata, local_tz, row_mode }: any = req.query;

    let varIDs = var_ids?.split(",") || [];
    let stationIDs = station_ids?.split(",") || [];
    let flagArr = flags?.split(",") || [];
    let intervalArr = intervals?.split(",") || [];

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
        let queryHandler = await MesonetDBManager.query(query, params, {rowMode: row_mode});
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
        .send(`An error occured while handling your query. Please validate the prameters used. Error: ${e}`);
      }
    }

    if(data.length > 0 && local_tz) {
      let query = `SELECT timezone FROM timezone_map WHERE location = $1`;
      let queryHandler = await MesonetDBManager.query(query, [location]);
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

	let measurementsTable = `${location}_measurements`;

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

  let query = `
    SELECT
      timestamp,
      ${measurementsTable}.station_id,
      variable_data.standard_name as variable,
      value,
			flag
      ${joinMetadata ? ", units, units_short, display_name AS variable_display_name, interval_seconds, name AS station_name, lat, lng, elevation" : ""}
    FROM ${measurementsTable}
    JOIN (
      SELECT alias, standard_name, interval_seconds, program
      FROM version_translations
      ${translationsWhereClause}
    ) as variable_data ON variable_data.program = ${measurementsTable}.version AND variable_data.alias = ${measurementsTable}.variable
    ${joinMetadata ? "JOIN station_metadata ON station_metadata.station_id = " + measurementsTable + ".station_id JOIN variable_metadata ON variable_metadata.standard_name = variable_data.standard_name" : ""}
    ${mainWhereClause}
    ORDER BY timestamp ${reverse ? "" : "DESC"}, ${measurementsTable}.station_id, variable_data.standard_name
    ${limitOffsetClause}
  `;


  let index = ["station_id", "timestamp", "variable", "value", "flag"];
  if(joinMetadata) {
    index = index.concat(["units", "units_short", "variable_display_name", "interval_seconds", "station_name", "lat", "lng", "elevation"]);
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
    SELECT standard_name, units, units_short, display_name
    FROM variable_metadata
    ${whereClause}
    ${limitOffsetClause};
  `;

  let index = ["standard_name", "units", "units_short", "display_name"];

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

    let stationIDs = station_ids?.split(",") || [];

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
      SELECT station_id, name, full_name, lat, lng, elevation, status, station_metadata.location, timezone_map.timezone
      FROM station_metadata
      JOIN timezone_map ON station_metadata.location = timezone_map.location
      ${whereClause}
      ${limitOffsetClause};
    `;

    let data: any = [];
    try {
      let queryHandler = await MesonetDBManager.query(query, params, {rowMode: row_mode});

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
      .send(`An error occured while handling your query. Please validate the prameters used. Error: ${e}`);
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

    let varIDs = var_ids?.split(",") || [];

    if(row_mode !== "array") {
      row_mode = undefined;
    }

    let { query, params } = constructVariablesQuery(varIDs, limit, offset);

    let data: any = [];
    try {
      let queryHandler = await MesonetDBManager.query(query, params, {rowMode: row_mode});

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
      .send(`An error occured while handling your query. Please validate the prameters used. Error: ${e}`);
    }

    if(row_mode === "array") {
      let index = ["standard_name", "units", "units_short", "display_name"];

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

router.get("/mesonet/db/sff", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    const MAX_DELAY_SECONDS = 3600;

    res.set("Content-Type", "text/csv");
    res.set("Content-Disposition", `attachment; filename="sff_data.csv"`);

    let query = `
      SELECT timestamp
      FROM hawaii_measurements
      ORDER BY timestamp DESC
      LIMIT 1;
    `;
    let queryHandler = await MesonetDBManager.query(query, [], { rowMode: "array" });
    let lastTimestampString = (await queryHandler.read(1))[0];
    queryHandler.close();
    let lastTimestamp = moment(lastTimestampString);
    let boundaryTime = moment().subtract(MAX_DELAY_SECONDS, "seconds");
    if(lastTimestamp.isSameOrBefore(boundaryTime)) {
      if(administrators.length > 0) {
        let mailOptions = {
          to: administrators,
          subject: "Mesonet SFF Fallback",
          text: `Hawaii Mesonet database entries are more than an hour behind. A request to the /mesonet/db/sff endpoint is falling back to the raw data file.`,
          html: `<p>Hawaii Mesonet database entries are more than an hour behind. A request to the /mesonet/db/sff endpoint is falling back to the raw data file.</p>`
        };
        try {
          //attempt to send email to the administrators
          let emailStatus = await sendEmail(mailOptions);
          //if email send failed throw error for logging
          if(!emailStatus.success) {
            throw emailStatus.error;
          }
        }
        //if error while sending admin email write to stderr
        catch(e) {
          console.error(`Failed to send administrator notification email for SFF fallback: ${e}`);
        }
      }


      let file = path.join(rawDataRoot, "sff/sff_data.csv");
      fs.access(file, fs.constants.F_OK, (e) => {
        if(e) {
          reqData.success = false;
          reqData.code = 404;
          res.status(404)
          .send("The requested file could not be found");
        }
        else {
          //should the size of the file in bytes be added?
          reqData.sizeF = 1;
          reqData.code = 200;
          res.status(200)
          .sendFile(file);
        }
      });
    }

    else {
      query = `
        SELECT hawaii_measurements.station_id, station_metadata.lat, station_metadata.lng, station_metadata.elevation, hawaii_measurements.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_height, CASE WHEN hawaii_measurements.value IS NOT NULL THEN CAST(hawaii_measurements.value AS DECIMAL) * synoptic_translations.unit_conversion_coefficient ELSE NULL END AS value
        FROM hawaii_measurements
        JOIN version_translations ON version_translations.program = hawaii_measurements.version AND version_translations.alias = hawaii_measurements.variable
        JOIN synoptic_translations ON version_translations.standard_name = synoptic_translations.standard_name
        JOIN station_metadata ON station_metadata.station_id = hawaii_measurements.station_id
        LEFT JOIN sensor_positions ON sensor_positions.station_id = hawaii_measurements.station_id AND version_translations.standard_name = sensor_positions.standard_name
        WHERE timestamp >= NOW() - '6 hours'::INTERVAL AND flag = 0 AND NOT EXISTS (SELECT 1 FROM synoptic_exclude WHERE synoptic_exclude.station_id = hawaii_measurements.station_id AND synoptic_exclude.standard_name = version_translations.standard_name)
        ORDER BY hawaii_measurements.station_id, hawaii_measurements.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_number;
      `;

      queryHandler = await MesonetDBManager.query(query, []);
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
    }
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
      let queryHandler = await MesonetDBManager.query(query, [stationID]);
      data = await queryHandler.read(1);
      queryHandler.close();
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the prameters used. Error: ${e}`);
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
      modified = await MesonetDBManager.queryNoRes(query, params, { privileged: true });
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the prameters used. Error: ${e}`);
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
      .send(`Invalid data provided. Data must be a 2D array with 6 element rows.`);
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
      if(!Array.isArray(row) || row.length != 6) {
        reqData.success = false;
        reqData.code = 400;

        return res.status(400)
        .send(`Invalid data provided. Data must be a 2D array with 6 element rows.`);
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
      INSERT INTO ${location}_measurements
      VALUES ${valueClause}
      ON CONFLICT (timestamp, station_id, variable)
      ${onConflict}
    `;
    try {
      let modified = await MesonetDBManager.queryNoRes(query, params, { privileged: true });
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
















// async function convertTZJSON(data: MesonetMeasurementValue[], location: string) {
//   let timezone = await getLocationTimezone(location);
//   for(let item of data) {
//     let converted = moment(item.timestamp).tz(timezone);
//     item.timestamp = converted.toISOString();
//   }
// }
// async function convertTZArray(data: (string | number)[], index: string[], location: string) {
//   let timezone = await getLocationTimezone(location);

// }

// router.get("/mesonet/db/measurements", async (req, res) => {
//   const permission = "basic";
//   await handleReq(req, res, permission, async (reqData) => {
//     let { station_ids, start_date, end_date, var_ids, intervals, flags, location, limit = 10000, offset, reverse, join_metadata, local_tz, row_mode }: any = req.query;

//     let varIDs = var_ids?.split(",") || [];
//     let stationIDs = station_ids?.split(",") || [];
//     let flagArr = flags?.split(",") || [];
//     let intervalArr = intervals?.split(",") || [];

//     const MAX_QUERY = 1000000;

//     //validate location, can use direct in query
//     //default to hawaii
//     if(!mesonetLocations.includes(location)) {
//       location = "hawaii";
//     }

//     if(offset) {
//       offset = parseInt(offset, 10);
//       if(isNaN(offset)) {
//         offset = undefined;
//       }
//     }
//     if(typeof limit === "string") {
//       limit = parseInt(limit, 10)
//       if(isNaN(limit)) {
//         limit = 10000;
//       }
//     }
//     //limit must be less than max, translate 0 or negative numbers as max
//     if(limit < 1 || limit > MAX_QUERY) {
//       limit = MAX_QUERY;
//     }

//     if(start_date) {
//       try {
//         let date = new Date(start_date);
//         start_date = date.toISOString();
//       }
//       catch(e) {
//         reqData.success = false;
//         reqData.code = 400;
  
//         return res.status(400)
//         .send("Invalid start date format. Dates must be ISO 8601 compliant.");
//       }
//     }
  
//     if(end_date) {
//       try {
//         let date = new Date(end_date);
//         end_date = date.toISOString();
//       }
//       catch(e) {
//         reqData.success = false;
//         reqData.code = 400;
  
//         return res.status(400)
//         .send("Invalid end date format. Dates must be ISO 8601 compliant.");
//       }
//     }

//     let data: MesonetMeasurementValue[] | { index: string[], data: (string | number)[] }
//     switch(row_mode) {
//       case "json": {
//         data = handleMeasurementsJSON(stationIDs, start_date, end_date, varIDs, intervals, flags, location, limit, offset, reverse, join_metadata, local_tz);
//         break;
//       }
//       case "wide_json": {
//         data = handleMeasurementsWideJSON(stationIDs, start_date, end_date, varIDs, intervals, flags, location, limit, offset, reverse, join_metadata, local_tz);
//         break;
//       }
//       case "array": {
//         data = handleMeasurementsArray(stationIDs, start_date, end_date, varIDs, intervals, flags, location, limit, offset, reverse, join_metadata, local_tz);
//         break;
//       }
//       case "wide_array": {
//         data = handleMeasurementsWideArray(stationIDs, start_date, end_date, varIDs, intervals, flags, location, limit, offset, reverse, join_metadata, local_tz);
//         break;
//       }
//       default: {
//         data = handleMeasurementsJSON(stationIDs, start_date, end_date, varIDs, intervals, flags, location, limit, offset, reverse, join_metadata, local_tz);
//       }
//     }

//     reqData.code = 200;
//     return res.status(200)
//     .json(data);


//     let values: any[] = [];
//     let index: string[];
//     ({ query, params, index } = constructMeasurementsQuery(stationIDs, start_date, end_date, varIDs, intervalArr, flagArr, location, limit, offset, reverse, join_metadata));

//     //check if should crosstab the query (wide mode) and if query should return results as array or JSON
//     let wide = row_mode.startsWith("wide_");
//     let queryStyle: "array" | undefined = row_mode.startsWith("wide_") || row_mode.endsWith("json") ? undefined : "array";
//     let returnArray = row_mode.endsWith("array");

//     try {
//       let queryHandler = await MesonetDBManager.query(query, params, {rowMode: queryStyle});
//       const chunkSize = 10000;
//       let chunk: any[];
//       do {
//         chunk = await queryHandler.read(chunkSize);
//         values = values.concat(chunk);
//       }
//       while(chunk.length > 0)
//       queryHandler.close();
//     }
//     catch(e) {
//       reqData.success = false;
//       reqData.code = 400;

//       return res.status(400)
//       .send(`An error occured while handling your query. Please validate the prameters used. Error: ${e}`);
//     }

//     let timezone = local_tz ? await getLocationTimezone(location) : "UTC";

//     if(wide) {
//       let wideData = convertWide(values, varMetadata, limit, offset, timezone)
//       if(returnArray) {
        
//       }
//     }

//     if(data.length > 0 && local_tz) {
//       let query = `SELECT timezone FROM timezone_map WHERE location = $1`;
//       let queryHandler = await MesonetDBManager.query(query, [location]);
//       let { timezone } = (await queryHandler.read(1))[0];
//       queryHandler.close();
//       if(row_mode === "array") {
//         let tsIndex = index.indexOf("timestamp");
//         for(let row of data) {
//           let converted = moment(row[tsIndex]).tz(timezone);
//           row[tsIndex] = converted.format();
//         }
//       }
//       else {
//         for(let row of data) {
//           let converted = moment(row.timestamp).tz(timezone);
//           row.timestamp = converted.format();
//         }
//       }
//     }
//     //if array form wrap with index
//     if(row_mode === "array" || row_mode == "wide_array") {
//       data = {
//         index,
//         data
//       };
//     }

    
//   });
// });

router.post("/mesonet/db/measurements/email", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { data, email, outputName } = req.body;
    console.log(req.body);

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
    let queryHandler = await MesonetDBManager.query(query, params);
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
      const outstream = fs.createWriteStream(outfile);
      const stringifier = stringify();
      stringifier.pipe(outstream);
  
      let e: any;
      try {
        let maxLimit = limit * varIDs.length + offset * varIDs.length;
  
        let chunkSize: [DurationInputArg1, DurationInputArg2] = [1, "hour"];
        let slowStart = true;
        let backoffBaseMS = 1000;
        let minFailures = 0;
    
        let queryChunker = new QueryChunkGenerator(start_date, end_date, reverse);
        let chunk = queryChunker.next(chunkSize);
        let writeState: WriteStateConfig = { limit, offset, totalRecordsRead: 0, lastRecordsRead: 0, totalRowsWritten: 0, lastRowsWritten: 0, writeHeader: true, finished: false };
        while(chunk && !writeState.finished) {
          let timeout = false;
          try {
            let [ startDate, endDate ] = chunk;
            ({ query, params } = constructMeasurementsQueryEmail(stationIDs, startDate, endDate, varIDs, intervalArr, flagArr, location, maxLimit, 0, reverse, false));
            queryHandler = await MesonetDBManager.query(query, params);
            const readChunkSize = 10000;
            let readChunk: MesonetMeasurementValue[];
            do {
              readChunk = await queryHandler.read(readChunkSize);
              writeState = write2Stringifier(stringifier, readChunk, varMetadata, timezone, writeState);
              maxLimit -= writeState.lastRecordsRead;
            }
            while(readChunk.length > 0 && !writeState.finished)
            queryHandler.close();
            chunk = queryChunker.next(chunkSize);
          }
          catch(e: any) {
            //non-timeout error, rethrow error to be caught by outer handler
            if(e.code != "57014") {
              throw e;
            }
            //reset chunk
            queryChunker.previous(chunkSize);
            timeout = true;
          }
    
          if(timeout && chunkSize[0] == 1) {
            if(minFailures >= 10) {
              throw new Error("Minimum data timed out 10 times. Cannot retreive the minimum data threshold");
            }
            slowStart = false;
            await new Promise(resolve => setTimeout(resolve, backoffBaseMS * Math.pow(2, minFailures)));
            minFailures++;
          }
          else if(timeout) {
            slowStart = false;
            chunkSize[0] = Math.max(Math.floor((<number>chunkSize[0]) / 2), 1);
          }
          else if(slowStart) {
            minFailures = 0;
            chunkSize[0] = (<number>chunkSize[0]) * 2;
          }
          else {
            minFailures = 0;
            (<number>chunkSize[0])++;
          }
        }
      }
      catch(err) {
        e = err;
      }
      stringifier.end();
      outstream.end();

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




async function getLocationTimezone(location: string) {
  let query = `SELECT timezone FROM timezone_map WHERE location = $1`;
  let queryHandler = await MesonetDBManager.query(query, [location]);
  let { timezone } = (await queryHandler.read(1))[0];
  queryHandler.close();
  return timezone;
}


async function getStartDate(location: string, stationIDs: string[]): Promise<string> {
  let measurementsTable = `${location}_measurements`;
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
    ORDER BY timestamp
    ${mainWhereClause}
    LIMIT 1;
  `;
  let queryHandler = await MesonetDBManager.query(query, params);
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













function write2Stringifier(stringifier: Stringifier, values: MesonetMeasurementValue[], varMetadata: VariableMetadata[], timezone: string, state: WriteStateConfig): WriteStateConfig {
	let { limit, offset, writeHeader, partialRow, index, header, totalRecordsRead, totalRowsWritten } = state;
  let lastRecordsRead = 0;
  let lastRowsWritten = 0;
  let finished = false;

	if(!header || !index) {
		index = {};
		header = ["Timestamp", "Station ID"];
		for(let item of varMetadata) {
			let {display_name, units_short, standard_name} = item;
			let headerVar = display_name;
			if(units_short) {
				headerVar += ` (${units_short})`;
			}
			index[standard_name] = header.length;
			header.push(headerVar);
		}
	}
	if(writeHeader) {
		stringifier.write(header);
	}

	let pivotedRow: string[] | undefined = partialRow;
	let currentTS = pivotedRow ? pivotedRow[0] : "";
	let currentSID = pivotedRow ? pivotedRow[1] : "";
	for(let row of values) {
    totalRecordsRead += 1;
    lastRecordsRead += 1;

		let {timestamp, station_id, variable, value} = row;
    let converted = moment(timestamp).tz(timezone);
    timestamp = converted.format();
    
		if(timestamp != currentTS || station_id != currentSID) {
			if(pivotedRow) {
        if(offset > 0) {
          offset--;
        }
        else {
          stringifier.write(pivotedRow);
          totalRowsWritten += 1;
          lastRowsWritten += 1;
          if(--limit < 1) {
            finished = true;
            break;
          }
        }
			}
			pivotedRow = new Array(header!.length).fill(null);
			pivotedRow[0] = timestamp;
			pivotedRow[1] = station_id;
		}
		let valueIndex = index![variable];
		pivotedRow![valueIndex] = value;
		currentTS = timestamp;
		currentSID = station_id;
	}

	return {
    limit,
    offset,
    totalRecordsRead,
    lastRecordsRead,
    totalRowsWritten,
    lastRowsWritten,
    finished,
		writeHeader: false,
		partialRow: pivotedRow,
		header,
		index
	};
}


class QueryChunkGenerator {
  private date: Moment;
  private startDate: Moment | null;
  private endDate: Moment | null;
  private __reverse: boolean;

  //if no start date provided need to query stations for earliest record
  constructor(startDate: string, endDate?: string, reverse: boolean = false) {
    this.startDate = moment(startDate);
    this.endDate = endDate ? moment(endDate) : moment();
    this.date = reverse? this.startDate.clone() : this.endDate.clone();
    this.__reverse = reverse;
  }

  next(chunkSize: [DurationInputArg1, DurationInputArg2]): [string, string] | null {
    if(this.__reverse) {
      return this.forward(chunkSize);
    }
    return this.backward(chunkSize);
  }

  previous(chunkSize: [DurationInputArg1, DurationInputArg2]): [string, string] | null {
    if(this.__reverse) {
      return this.backward(chunkSize);
    }
    return this.forward(chunkSize);
  }

  private forward(chunkSize: [DurationInputArg1, DurationInputArg2]): [string, string] | null {
    if(this.endDate && this.date.isSame(this.endDate)) {
      return null;
    }
    let startDate = this.date.toISOString();
    this.date.add(...chunkSize);
    if(this.endDate && this.date.isAfter(this.endDate)) {
      this.date = this.endDate.clone();
    }
    let endDate = this.date.toISOString();
    return [startDate, endDate];
  }

  private backward(chunkSize: [DurationInputArg1, DurationInputArg2]): [string, string] | null {
    if(this.startDate && this.date.isSame(this.startDate)) {
      return null;
    }
    let endDate = this.date.toISOString();
    this.date.subtract(...chunkSize);
    if(this.startDate && this.date.isBefore(this.startDate)) {
      this.date = this.startDate.clone();
    }
    let startDate = this.date.toISOString();
    return [startDate, endDate];
  }

  get reverse() {
    return this.__reverse;
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
	units_short: string,
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
	units_short?: string,
	variable_display_name?: string
	interval_seconds?: string,
	station_name?: string,
	lat?: number,
	lng?: number,
	elevation?: number
}