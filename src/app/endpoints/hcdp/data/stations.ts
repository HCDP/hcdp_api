import express from "express";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers.js";
import { stationMetadataHelper } from "../../../modules/util/resourceManagers/tapis.js";
import { processTapisError, validateArray, validateType } from "../../../modules/util/util.js";
import { githubWebhookSecret, dataPortalLocations } from "../../../modules/util/config.js";
import { HCDPTapisMetadataType } from "src/app/modules/tapisv3.js";
import CsvReadableStream from "csv-reader";
import detectDecodeStream from "autodetect-decoder-stream";
import safeCompare from "safe-compare";
import { Readable } from "stream";
import * as crypto from "crypto";
import fetchRetry from 'fetch-retry';
const rfetch = fetchRetry(fetch);

export const router = express.Router();

const HCDP_STATION_HEADER_TRANSLATIONS: { [key: string]: string } = {
  "SKN": "skn",
  "Station.Name": "name",
  "Observer": "observer",
  "Network": "network",
  "Island": "island",
  "ELEV.m.": "elevation_m",
  "LAT": "lat",
  "LON": "lng",
  "NCEI.id": "ncei_id",
  "NWS.id": "nws_id",
  "NESDIS.id": "nesdis_id",
  "SCAN.id": "scan_id",
  "SMART_NODE_RF.id": "smart_node_rf_id"
};

function signBlob(key, blob) {
  return "sha1=" + crypto.createHmac("sha1", key).update(blob).digest("hex");
}


router.get("/stations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const r400 = (message: string) => {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `${message}
        
        Request must include the following parameters:

        Required:
        q: Mongo DB style query for station documents.

        Optional:
        limit: An integer representing the maximum results to return. Default value 1000
        offset: An integer representing the number of results to skip. Default value 0`
      );
    }

    let { q, limit, offset }: any = req.query;

    if(!q) {
      return r400("No query provided");
    }

    // Convert limit and offset to integers
    limit = limit ? parseInt(limit) : undefined;
    offset = offset ? parseInt(offset) : undefined;

    // validate limit and offset were able to be parsed to valid integers
    if((limit !== undefined && isNaN(limit)) || (offset !== undefined && isNaN(offset))) {
      return r400("Limit and offset must be valid integers.");
    }

    let data = null;
    try {
      try {
        //parse query string to JSON
        q = JSON.parse(q.replace(/'/g, '"'));
      }
      catch {
        return r400("Unable to parse query");
      }
      
      data = await stationMetadataHelper.queryMetadataRaw(q, limit, offset);
      // wrap in legacy tapis response format
      data = {
        status: "success",
        message: null,
        version: null,
        result: data
      };

    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.get("/stations/:type(value|metadata)", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const r400 = (message: string) => {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `${message}
        
        Request must include the following parameters:

        Required:

        Optional:
        [filter_values]: document filter fields (e.g. datatype: rainfall, production: new, period: day, fill: partial)
        location: The location of the stations being requested (e.g. hawaii, american_samoa, guam). Default value hawaii
        limit: An integer representing the maximum results to return. Default value 1000
        offset: An integer representing the number of results to skip. Default value 0`
      );
    }

    const metadataType = req.params.type as HCDPTapisMetadataType;

    let { limit, offset, location, ...values }: any = req.query;

    if(!dataPortalLocations.includes(location)) {
      location = "hawaii";
    }

    // Convert limit and offset to integers
    limit = limit ? parseInt(limit) : undefined;
    offset = offset ? parseInt(offset) : undefined;

    // validate limit and offset were able to be parsed to valid integers
    if((limit !== undefined && isNaN(limit)) || (offset !== undefined && isNaN(offset))) {
      return r400("Limit and offset must be valid integers.");
    }

    let data = null;
    try {
      data = await stationMetadataHelper.queryMetadata(location, metadataType, values, limit, offset);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.post("/stations/:type(value|metadata)", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {

    const r400 = (message: string) => {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `${message}
        
        Request body must include the following parameters:

        Required:
        values: An array of values to be added. Each element should be a 1 level JSON object containing key value pairs
        keyFields: An array of fields that constitute the document key

        Optional:
        location: The location of the stations being requested (e.g. hawaii, american_samoa, guam). Default value hawaii
        replace: A boolean indicating if duplicate documents should be replaced. If false duplicates will be skipped. Default value true`
      );
    }

    const metadataType = req.params.type as HCDPTapisMetadataType;

    let { location, replace, values, keyFields }: any = req.body;

    // validate values and set defaults
    if(typeof replace !== "boolean") {
      replace = true;
    }
    if(!dataPortalLocations.includes(location)) {
      location = "hawaii";
    }
    if(!Array.isArray(values) || values.length < 1) {
      return r400("No values provided");
    }
    if(!Array.isArray(keyFields) || keyFields.length < 1) {
      return r400("No key fields provided");
    }
    if(!validateArray(keyFields, (value) => validateType(value, ["string"]))) {
      return r400("keyFields must be an array of strings");
    }

    for(let item of values) {
      for(let field in item) {
        let value = item[field];
        if(typeof value !== "string" && typeof value !== "number") {
          return r400("Found an invalid value");
        }
      }
    }

    let data = null;
    try {
      data = await stationMetadataHelper.createMetadata(location, metadataType, values, keyFields, replace);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});


//add middleware to get raw body, don't actually need body data so no need to do anything fancy to get parsed body as well
router.post("/addmetadata", express.raw({ limit: "50mb", type: () => true }), async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    //ensure this is coming from github by hashing with the webhook secret
    const receivedSig = req.headers['x-hub-signature'];
    const computedSig = signBlob(githubWebhookSecret, req.body);
    if(!safeCompare(receivedSig, computedSig)) {
      reqData.code = 401;
      return res.status(401).end();
    }
    //only process github push events
    if(req.headers["x-github-event"] != "push") {
      reqData.code = 204;
      return res.status(204).end();
    }
    
    const response = await rfetch("https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/Hawaii_Master_Station_Meta.csv", {
      retries: 3, 
      retryDelay: (attempt) => Math.pow(2, attempt) * 1000
    });

    if(!response.ok) {
      throw new Error(`Failed to fetch master metadata csv: ${response.status}, ${response.statusText}`);
    }
    if(!response.body) {
      throw new Error("Metadata csv response body is empty");
    }

    let header: string[] | null = null;
    let values: any[] = [];

    const numericFields = new Set(["elevation_m", "lat", "lng"]);

    const bodyStream = Readable.fromWeb(response.body as any);
    const csvStream = bodyStream
      .pipe(new detectDecodeStream({ defaultEncoding: "1255" }))
      .pipe(new CsvReadableStream({ parseNumbers: false, parseBooleans: false, trim: true }));

    for await (const row of csvStream) {
      if(header === null) {
        header = row.map((col: string) => HCDP_STATION_HEADER_TRANSLATIONS[col] || col);
      }
      else {
        let data: any = {
          station_group: "hawaii_climate_primary",
          id_field: "skn"
        };

        for(let i = 0; i < header.length; i++) {
          let property = header[i];
          let value = row[i];
          
          if(value === "NA" || value === "" || value == null) {
            continue;
          }

          if(numericFields.has(property)) {
            let num = Number(value);
            if(!isNaN(num)) {
              data[property] = num;
            }
          }
          else {
            data[property] = value;
          }
        }
        // validate skn, lat, and lng exist
        if(typeof data.skn === "string" && typeof data.lat === "number" && typeof data.lng === "number") {
          values.push(data);
        }
      }
    }

    await stationMetadataHelper.createMetadata("hawaii", "metadata", values, ["station_group", "skn"]);

    reqData.code = 204;
    return res.status(204).end();
  });
});

