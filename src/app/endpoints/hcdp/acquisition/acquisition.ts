import express from "express";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { parseBoolParam, parseListParam, validateArray } from "../../../modules/util/util.js";
import { getTimezone } from "../../../modules/util/dates.js";
import moment from "moment-timezone";
import { parseParams } from "../../../modules/util/dbUtil.js";
import Cursor from "pg-cursor";
import { hcdpGeneralAdmin, hcdpGeneralUser } from "../../../modules/util/resourceManagers/db.js";

export const router = express.Router();

const ACQUISITION_VERSIONS = ["preliminary"] as const;
const ACQUISITION_LOCATIONS = ["hawaii"] as const;

const DATA_SCHEMAS = {
  hads: {
    stationMetadata: {
      cols: ["station_id", "station_name", "nesdis_id", "skn", "lat", "lng"],
      key: ["station_id"]
    },
    varMetadata: {
      cols: ["variable", "unit"],
      key: ["variable"]
    },
    data: {
      cols: ["station_id", "timestamp", "variable", "value", "mode"],
      key: ["station_id", "timestamp", "variable"]
    }
  },
  madis: {
    stationMetadata: {
      cols: ["station_id", "station_name", "data_provider", "skn", "lat", "lng", "source"],
      key: ["station_id"]
    },
    varMetadata: {
      cols: ["variable", "unit"],
      key: ["variable"]
    },
    data: {
      cols: ["station_id", "timestamp", "variable", "value"],
      key: ["station_id", "timestamp", "variable"]
    }
  },
  nws_api: {
    stationMetadata: {
      cols: ["station_id", "station_name", "data_provider", "skn", "lat", "lng"],
      key: ["station_id"]
    },
    varMetadata: {
      cols: ["variable", "unit"],
      key: ["variable"]
    },
    data: {
      cols: ["station_id", "timestamp", "variable", "value"],
      key: ["station_id", "timestamp", "variable"]
    }
  },
  nws_rr5: {
    stationMetadata: {
      cols: ["station_id", "station_name", "skn", "lat", "lng"],
      key: ["station_id"]
    },
    varMetadata: {
      cols: ["variable", "unit"],
      key: ["variable"]
    },
    data: {
      cols: ["station_id", "timestamp", "variable", "value"],
      key: ["station_id", "timestamp", "variable"]
    }
  }
} as const;

function getTableName(source: string, table: string, location?: string, version?: string) {
  let tableName: string;
  switch(table) {
    case "stationMetadata": {
      tableName = getStationMetaTable(source);
      break;
    }
    case "varMetadata": {
      tableName = getVarMetaTable(source);
      break;
    }
    case "data": {
      tableName = getDataTable(location, version, source);
      break;
    }
  }
  return tableName;
}

function getStationMetaTable(source: string) {
  return `acquisition.station_metadata_${source}`;
}

function getVarMetaTable(source: string) {
  return `acquisition.variable_metadata_${source}`;
}

function getDataTable(location: string, version: string, source: string) {
  return `acquisition.${location}_${version}_${source}`
}



router.get("/acquisition/:source(hads|madis|nws_api|nws_rr5)/data", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { source } = req.params;
    let {
      reverse = false,
      local_tz = false,
      location = "hawaii",
      version = "preliminary",
      station_ids = [],
      var_ids = [],
      start_date,
      end_date,
      row_mode,
      limit = 10000,
      offset
    }: any = req.query;

    const MAX_QUERY = 1000000;

    const e400 = (msg: string) => {
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(msg);
    }

    if(!ACQUISITION_VERSIONS.includes(version)) {
      return e400(`Invalid version. Version must be one of ${ACQUISITION_VERSIONS}`);
    }
    if(!ACQUISITION_LOCATIONS.includes(location)) {
      return e400(`Invalid location. Location must be one of ${ACQUISITION_LOCATIONS}`);
    }

    let rowMode: "array" | undefined = row_mode == "array" ? "array" : undefined;

    reverse = parseBoolParam(reverse);
    let localTz = parseBoolParam(local_tz);

    let stationIDs = parseListParam(station_ids);
    let varIDs = parseListParam(var_ids);
    

    let parsedLimit = parseInt(limit, 10);
    // default to max
    if(isNaN(parsedLimit)) {
      return e400("Invalid limit parameter. Limit must be an integer.");
    }
    else if(parsedLimit < 1 || parsedLimit > MAX_QUERY) {
      limit = MAX_QUERY;
    }
    else {
      limit = parsedLimit;
    }

    let parsedOffset: number | undefined;
    if(offset !== undefined) {
      parsedOffset = parseInt(offset, 10);
      if(isNaN(parsedOffset) || parsedOffset < 0) {
        return e400("Invalid offset parameter. Offset must be a non-negative integer.");
      }
      offset = parsedOffset;
    }

    const table = getDataTable(location, version, source);
    const schema: string[] = DATA_SCHEMAS[source].data.cols;

    let params: string[] = [];
    let whereClauses = [];

    if(stationIDs.length > 0) {
      parseParams(stationIDs, params, whereClauses, "station_id");
    }

    if(varIDs.length > 0) {
      parseParams(varIDs, params, whereClauses, "variable");
    }

    if(start_date) {
      let date = new Date(start_date);
      // ensure valid date produced by input
      if(isNaN(date.getTime())) {
        return e400("Invalid start date format. Dates must be ISO 8601 compliant.");
      }
      params.push(date.toISOString());
      whereClauses.push(`timestamp >= $${params.length}`);
    }
  
    if(end_date) {
      let date = new Date(end_date);
      // ensure valid date produced by input
      if(isNaN(date.getTime())) {
        return e400("Invalid end date format. Dates must be ISO 8601 compliant.");
      }
      params.push(date.toISOString());
      whereClauses.push(`timestamp <= $${params.length}`);
    }

    let whereClause = "";
    if(whereClauses.length > 0) {
      whereClause = `WHERE ${whereClauses.join(" AND ")}`;
    }

    let limitOffsetClause = "";
    params.push(limit.toString());
    limitOffsetClause += `LIMIT $${params.length}`;
    if(offset) {
      params.push(offset.toString());
      limitOffsetClause += ` OFFSET $${params.length}`;
    }


    let query = `
      SELECT ${schema.join(", ")}
      FROM ${table}
      ${whereClause}
      ORDER BY station_id, timestamp ${reverse ? "" : "DESC"}, variable
      ${limitOffsetClause};
    `

    let data: any[];
    try {
      data = await hcdpGeneralUser.query(query, params, async (cursor: Cursor) => {
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
      }, { rowMode: rowMode });
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    // convert timestamps to localTz if requested
    if(data.length > 0 && localTz) {
      let timezone = getTimezone(location);

      if(rowMode === "array") {
        let tsIndex = schema.indexOf("timestamp");
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

    reqData.code = 200;
    return res.status(200)
    .json(data);

  });
});

















router.get("/acquisition/:source(hads|madis|nws_api|nws_rr5)/variableMetadata", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { source } = req.params;
    let {
      var_ids = [],
      row_mode,
      limit = 10000,
      offset
    }: any = req.query;

    const MAX_QUERY = 1000000;

    const e400 = (msg: string) => {
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(msg);
    }

    let rowMode: "array" | undefined = row_mode == "array" ? "array" : undefined;

    let varIDs = parseListParam(var_ids);    

    let parsedLimit = parseInt(limit, 10);
    // default to max
    if(isNaN(parsedLimit)) {
      return e400("Invalid limit parameter. Limit must be an integer.");
    }
    else if(parsedLimit < 1 || parsedLimit > MAX_QUERY) {
      limit = MAX_QUERY;
    }
    else {
      limit = parsedLimit;
    }

    let parsedOffset: number | undefined;
    if(offset !== undefined) {
      parsedOffset = parseInt(offset, 10);
      if(isNaN(parsedOffset) || parsedOffset < 0) {
        return e400("Invalid offset parameter. Offset must be a non-negative integer.");
      }
      offset = parsedOffset;
    }

    const table = getVarMetaTable(source);
    const schema: string[] = DATA_SCHEMAS[source].varMetadata.cols;

    let params: string[] = [];
    let whereClauses = [];

    if(varIDs.length > 0) {
      parseParams(varIDs, params, whereClauses, "variable");
    }

    let whereClause = "";
    if(whereClauses.length > 0) {
      whereClause = `WHERE ${whereClauses.join(" AND ")}`;
    }

    let limitOffsetClause = "";
    params.push(limit.toString());
    limitOffsetClause += `LIMIT $${params.length}`;
    if(offset) {
      params.push(offset.toString());
      limitOffsetClause += ` OFFSET $${params.length}`;
    }


    let query = `
      SELECT ${schema.join(", ")}
      FROM ${table}
      ${whereClause}
      ORDER BY variable
      ${limitOffsetClause};
    `

    let data: any[];
    try {
      data = await hcdpGeneralUser.query(query, params, async (cursor: Cursor) => {
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
      }, { rowMode: rowMode });
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.get("/acquisition/:source(hads|madis|nws_api|nws_rr5)/stationMetadata", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { source } = req.params;
    let {
      station_ids = [],
      row_mode,
      limit = 10000,
      offset
    }: any = req.query;

    const MAX_QUERY = 1000000;

    const e400 = (msg: string) => {
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(msg);
    }

    let rowMode: "array" | undefined = row_mode == "array" ? "array" : undefined;

    let stationIDs = parseListParam(station_ids);    

    let parsedLimit = parseInt(limit, 10);
    // default to max
    if(isNaN(parsedLimit)) {
      return e400("Invalid limit parameter. Limit must be an integer.");
    }
    else if(parsedLimit < 1 || parsedLimit > MAX_QUERY) {
      limit = MAX_QUERY;
    }
    else {
      limit = parsedLimit;
    }

    let parsedOffset: number | undefined;
    if(offset !== undefined) {
      parsedOffset = parseInt(offset, 10);
      if(isNaN(parsedOffset) || parsedOffset < 0) {
        return e400("Invalid offset parameter. Offset must be a non-negative integer.");
      }
      offset = parsedOffset;
    }

    const table = getStationMetaTable(source);
    const schema: string[] = DATA_SCHEMAS[source].stationMetadata.cols;

    let params: string[] = [];
    let whereClauses = [];

    if(stationIDs.length > 0) {
      parseParams(stationIDs, params, whereClauses, "station_id");
    }

    let whereClause = "";
    if(whereClauses.length > 0) {
      whereClause = `WHERE ${whereClauses.join(" AND ")}`;
    }

    let limitOffsetClause = "";
    params.push(limit.toString());
    limitOffsetClause += `LIMIT $${params.length}`;
    if(offset) {
      params.push(offset.toString());
      limitOffsetClause += ` OFFSET $${params.length}`;
    }


    let query = `
      SELECT ${schema.join(", ")}
      FROM ${table}
      ${whereClause}
      ORDER BY station_id
      ${limitOffsetClause};
    `

    let data: any[];
    try {
      data = await hcdpGeneralUser.query(query, params, async (cursor: Cursor) => {
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
      }, { rowMode: rowMode });
    }
    catch(e) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(`An error occured while handling your query. Please validate the parameters used. Error: ${e}`);
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});















router.post("/acquisition/:source(hads|madis|nws_api|nws_rr5)/:table(stationMetadata|varMetadata|data)", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    const { source, table } = req.params;
    let { 
      overwrite = true,
      location = "hawaii",
      version = "preliminary",
      data
    } = req.body;

    const e400 = (msg: string) => {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400).send(msg);
    };

    if(table == "data") {
      if(!ACQUISITION_VERSIONS.includes(version)) {
        return e400(`Invalid version. Version must be one of ${ACQUISITION_VERSIONS}`);
      }
      if(!ACQUISITION_LOCATIONS.includes(location)) {
        return e400(`Invalid location. Location must be one of ${ACQUISITION_LOCATIONS}`);
      }
    }

    let tableName: string = getTableName(source, table, location, version);

    overwrite = parseBoolParam(overwrite);
    const schema: string[] = DATA_SCHEMAS[source][table].cols;
    const keyCols: string[] = DATA_SCHEMAS[source][table].key;

    let isValid = validateArray(data, (row) => {
    if(typeof row !== "object" || row === null) return false;
      // Ensure every column defined in the schema exists in the row
      return schema.every((col: string) => row[col] !== undefined);
    });

    if(!isValid) {
      return e400(`Invalid payload. 'data' must be an array of objects; each object must contain the following properties: ${schema.join(", ")}`);
    }

    if(data.length === 0) {
      reqData.code = 200;
      reqData.success = true;
      return res.status(200).json({ modified: 0 });
    }

    let params: any[] = [];
    let valuesLines: string[] = [];
    let paramIndex = 1;

    for(let row of data) {
      let rowArr = [];
      for(let col of schema) {
        params.push(row[col]);
        rowArr.push(`$${paramIndex++}`);
      }
      valuesLines.push(`(${rowArr.join(", ")})`);
    }

    
    const updateColumns = schema.filter(col => !keyCols.includes(col));

    let conflictAction = "DO NOTHING";
    if(overwrite && updateColumns.length > 0) {
      let setClause = updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(", ");
      conflictAction = `DO UPDATE SET ${setClause}`;
    }


    let query = `
      INSERT INTO ${tableName} (${schema.join(", ")})
      VALUES ${valuesLines.join(",")}
      ON CONFLICT (${keyCols.join(", ")})
      ${conflictAction};
    `;

    try {
      let modified = await hcdpGeneralAdmin.queryNoRes(query, params);
      
      reqData.code = 200;
      reqData.success = true;
      return res.status(200).json({ modified });
    }
    catch(e) {
      return e400(`An error occurred while inserting data. Validate your payload format. Error: ${e}`);
    }
  });
});