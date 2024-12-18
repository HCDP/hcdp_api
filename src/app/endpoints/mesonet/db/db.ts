
import express from "express";
import moment from "moment-timezone";
import { MesonetDBManager } from "../../../modules/util/resourceManagers/db";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers";

export const router = express.Router();

interface QueryData {
  query: string | null,
  params: any[],
  index: string[]
}

function parseListParams(paramList: string, allParams: string[], whereClauses: string[], column: string) {
  let paramListArr = paramList.split(",");
  parseArrParams(paramListArr, allParams, whereClauses, column);
}
function parseArrParams(paramListArr: string[], allParams: string[], whereClauses: string[], column: string) {
  let paramSet: string[] = [];
  for(let i = 0; i < paramListArr.length; i++) {
    allParams.push(paramListArr[i]);
    paramSet.push(`$${allParams.length}`);
  }
  whereClauses.push(`${column} IN (${paramSet.join(",")})`);
}

function constructBaseMeasurementsQuery(stationIDs: string, startDate: string, endDate: string, varIDs: string, intervals: string, flags: string, location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean, selectFlag: boolean = true): QueryData {
  let measurementsTable = `${location}_measurements`;

  let params: string[] = [];

  ///////////////////////////////////////////////////
  //////////// translations where clause ////////////
  ///////////////////////////////////////////////////

  let translationsWhereClauses = [];

  if(varIDs) {
    parseListParams(varIDs, params, translationsWhereClauses, "standard_name");
  }

  if(intervals) {
    parseListParams(intervals, params, translationsWhereClauses, "interval_seconds");
  }

  let translationsWhereClause = "";
  if(translationsWhereClauses.length > 0) {
    translationsWhereClause = `WHERE ${translationsWhereClauses.join(" AND ")}`;
  } 

  ///////////////////////////////////////////////////
  //////////////// main where clause ////////////////
  ///////////////////////////////////////////////////

  let mainWhereClauses: string[] = [];
  
  if(stationIDs) {
    parseListParams(stationIDs, params, mainWhereClauses, `${measurementsTable}.station_id`);
  }

  if(startDate) {
    params.push(startDate);
    mainWhereClauses.push(`timestamp >= $${params.length}`);
  }

  if(endDate) {
    params.push(endDate);
    mainWhereClauses.push(`timestamp <= $${params.length}`);
  }

  if(flags) {
    parseListParams(flags, params, mainWhereClauses, "flag");
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

async function constructMeasurementsQuery(crosstabQuery: boolean, stationIDs: string, startDate: string, endDate: string, varIDs: string, intervals: string, flags: string, location: string, limit: number, offset: number, reverse: boolean, joinMetadata: boolean): Promise<QueryData> {
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


async function sanitizeExpandVarIDs(var_ids: string) {
  let query = `
    SELECT DISTINCT standard_name
    FROM version_translations
  `;
  let params: string[] = [];
  if(var_ids) {
    let clause: string[] = [];   
    parseListParams(var_ids, params, clause, "standard_name");
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

    const MAX_QUERY = 1000000;

    //validate location, can use direct in query
    //default to hawaii
    if(location !== "american_samoa") {
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
    let { query, params, index } = await constructMeasurementsQuery(crosstabQuery, station_ids, start_date, end_date, var_ids, intervals, flags, location, limit, offset, reverse, join_metadata);
    if(query) {
      let queryHandler = await MesonetDBManager.query(query, params, {rowMode: row_mode});
    
      const chunkSize = 10000;
      let maxLength = 0;
      do {
        let chunk = await queryHandler.read(chunkSize);
        data = data.concat(chunk);
        maxLength += chunkSize
      }
      while(data.length == maxLength)
      queryHandler.close();
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

router.get("/mesonet/db/stations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_ids, location, limit, offset, row_mode }: any = req.query;

    //validate location, can use direct in query
    //default to hawaii
    if(location !== "american_samoa") {
      location = "hawaii";
    }
    if(row_mode !== "array") {
      row_mode = undefined;
    }

    let params: string[] = [];

    ////////////////////////////////////////////////////
    /////////////////// where clause ///////////////////
    ////////////////////////////////////////////////////

    let whereClauses: string[] = [];

    params.push(location);
    whereClauses.push(`location = $${params.length}`);
    
    if(station_ids) {
      parseListParams(station_ids, params, whereClauses, "station_id");
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
      SELECT station_id, name, lat, lng, elevation
      FROM station_metadata
      ${whereClause}
      ${limitOffsetClause};
    `;

    let queryHandler = await MesonetDBManager.query(query, params, {rowMode: row_mode});

    const chunkSize = 10000;
    let data: any = [];
    let maxLength = 0;
    do {
      let chunk = await queryHandler.read(chunkSize);
      data = data.concat(chunk);
      maxLength += chunkSize
    }
    while(data.length == maxLength)
    queryHandler.close();

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

    if(row_mode !== "array") {
      row_mode = undefined;
    }

    let params: string[] = [];

    ////////////////////////////////////////////////////
    /////////////////// where clause ///////////////////
    ////////////////////////////////////////////////////

    let whereClauses: string[] = [];
    
    if(var_ids) {
      parseListParams(var_ids, params, whereClauses, "standard_name");
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
      SELECT standard_name, units, units_short, display_name
      FROM variable_metadata
      ${whereClause}
      ${limitOffsetClause};
    `;

    let queryHandler = await MesonetDBManager.query(query, params, {rowMode: row_mode});

    const chunkSize = 10000;
    let data: any = [];
    let maxLength = 0;
    do {
      let chunk = await queryHandler.read(chunkSize);
      data = data.concat(chunk);
      maxLength += chunkSize
    }
    while(data.length == maxLength)
    queryHandler.close();

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
    let query = `
      SELECT hawaii_measurements.station_id, station_metadata.lat, station_metadata.lng, station_metadata.elevation, hawaii_measurements.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_height, CASE WHEN hawaii_measurements.value IS NOT NULL THEN CAST(hawaii_measurements.value AS DECIMAL) * synoptic_translations.unit_conversion_coefficient ELSE NULL END AS value
      FROM hawaii_measurements
      JOIN version_translations ON version_translations.program = hawaii_measurements.version AND version_translations.alias = hawaii_measurements.variable
      JOIN synoptic_translations ON version_translations.standard_name = synoptic_translations.standard_name
      JOIN station_metadata ON station_metadata.station_id = hawaii_measurements.station_id
      LEFT JOIN sensor_positions ON sensor_positions.station_id = hawaii_measurements.station_id AND version_translations.standard_name = sensor_positions.standard_name
      WHERE timestamp >= NOW() - '6 hours'::INTERVAL AND flag = 0 AND NOT EXISTS (SELECT 1 FROM synoptic_exclude WHERE synoptic_exclude.station_id = hawaii_measurements.station_id AND synoptic_exclude.standard_name = version_translations.standard_name)
      ORDER BY hawaii_measurements.station_id, hawaii_measurements.timestamp, synoptic_translations.synoptic_name, sensor_positions.sensor_number;
    `;

    let queryHandler = await MesonetDBManager.query(query, []);
    let data = await queryHandler.read(100000);
    queryHandler.close();

    res.set("Content-Type", "text/csv");
    res.set("Content-Disposition", `attachment; filename="sff_data.csv"`);
    
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

    if(!(stationID && variable && /^[0-9]+$/.test(flag) && ((typeof timestamp === "string" && !isNaN(Date.parse(timestamp))) || (!isNaN(Date.parse(timestamp.start)) && !isNaN(Date.parse(timestamp.end)))))) {
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

    let queryHandler = await MesonetDBManager.query(query, [stationID]);
    let data = await queryHandler.read(1);
    queryHandler.close();
    if(data.length < 1) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send(`Station ID ${stationID} not found.`);
    }
    let { table_name: tableName } = data[0];

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

    let modified = await MesonetDBManager.queryNoRes(query, params, { privileged: true });

    reqData.code = 200;
    return res.status(200)
    .json({ modified });
  });
});