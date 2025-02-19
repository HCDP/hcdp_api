import express from "express";
import moment from "moment-timezone";
import * as fs from "fs";
import * as child_process from "child_process";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { mesonetLocations, tapisV3Config } from "../../../modules/util/config.js";
import { tapisV3Manager } from "../../../modules/util/resourceManagers/tapis.js";
import { ProjectHandler } from "../../../modules/tapisHandlers.js";
import { downloadRoot, apiURL } from "../../../modules/util/config.js";
import { MesonetDataPackager } from "../../../modules/mesonetDataPackager.js";
import { handleSubprocess, sendEmail, processTapisError } from "../../../modules/util/util.js";

export const router = express.Router();

const projectHandlers: {[location: string]: ProjectHandler} = {};
for(let location in tapisV3Config.streams.projects) {
  let projectConfig = tapisV3Config.streams.projects[location];
  projectHandlers[location] = tapisV3Manager.streams.getProjectHandler(projectConfig.project, projectConfig.timezone);
}


function processMeasurementsError(res, reqData, e) {
  let {status, reason} = e;
  //if key error or empty data frame error just return no data
  if(status == 500 && (reason.includes("Unrecognized exception type: <class 'KeyError'>") || reason.includes("Unrecognized exception type: <class 'pandas.errors.EmptyDataError'>"))) {
    reqData.code = 200;
    res.status(200)
    .json({});
  }
  //otherwise process as proper error
  else {
    processTapisError(res, reqData, e);
  }
}


router.get("/mesonet/getStations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { location }: any = req.query;
    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }
    let projectHandler = projectHandlers[location];
    if(projectHandler == undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("Unknown location provided.");
    }
    try {
      const data = await projectHandler.listStations();
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }
  });
});

router.get("/mesonet/getVariables", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_id, location }: any = req.query;
    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }
    if(station_id === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        station_id: The ID of the station to query.
        location (optional): The sensor network location to retrieve data for. Default hawaii`
      );
    }
    let projectHandler = projectHandlers[location];
    if(projectHandler == undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("Unknown location provided.");
    }
    try {
      const data = await projectHandler.listVariables(station_id);
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }
  });
});


router.get("/mesonet/getMeasurements", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //options
    //start_date, end_date, limit, offset, var_ids (comma separated)
    let { station_id, location, ...options }: any = req.query;
    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }
    if(station_id === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        station_ids: A comma separated list of station IDs to query.
        limit (optional): A number indicating the maximum number of records to be returned for each variable.
        offset (optional): A number indicating an offset in the records returned from the first available record.
        start_date (optional): An ISO-8601 formatted date string indicating the date/time returned records should start at.
        end_date (optional): An ISO-8601 formatted date string indicating the date/time returned records should end at
        var_ids (optional): A comma separated list of variable IDs limiting what variables will be returned.
        location (optional): The sensor network location to retrieve data for. Default hawaii`
      );
    }
    let projectHandler = projectHandlers[location];
    if(projectHandler == undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("Unknown location provided.");
    }
    try {
      const data = await projectHandler.listMeasurements(station_id, options);
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      return processMeasurementsError(res, reqData, e);
    }
  });
});

function getBatchSize() {
  return [3, "months"];
}

function getDefaultStartDate() {
  return moment().subtract(...getBatchSize()).toISOString();
}

function getDefaultEndDate() {
  return moment().toISOString();
}

function batchDates(start: string, end: string): [string, string][] {
  let batches: [string, string][] = [];
  let date = moment(start);
  let endDate = moment(end);
  const batchSize = getBatchSize();
  let batchStart = date.toISOString();
  date.add(...batchSize);
  let batchEnd;
  while(date.isBefore(endDate)) {
    batchEnd = date.toISOString();
    batches.push([batchStart, batchEnd]);
    batchStart = batchEnd;
    date.add(...batchSize);
  }
  batches.push([batchStart, endDate.toISOString()]);
  return batches;
}

async function createMesonetPackage(projectHandler: ProjectHandler, stationIDs: string[], combine: boolean, ftype: "json" | "csv", csvMode: "matrix" | "table", options: any, reqData: any) {
  //set start and end dates to default if they do not exist to prevent very large queries that cannot be chunked
  if(options.start_date === undefined) {
    options.start_date = getDefaultStartDate();
  }
  if(options.endDate === undefined) {
    options.endDate = getDefaultEndDate();
  }
  const stationData = await projectHandler.listStations();
  //station data has all instruments and variables, just repack variables and strip out of station refs to reduce unnecessary size (no need to query vars)
  let packedStationData = {};
  let variableData = {};
  for(let station of stationData) {
    if(station.instruments[0].variables) {
      for(let variable of station.instruments[0].variables) {
        if(combine) {
          variableData[variable.var_id] = variable;
        }
        else {
          let stationVars = variableData[station.site_id];
          if(stationVars === undefined) {
            stationVars = {};
            variableData[station.site_id] = stationVars;
          }
          stationVars[variable.var_id] = variable;
        }
      }
    }
    delete station.instruments;
    packedStationData[station.site_id] = station;
  }

  let packager = new MesonetDataPackager(downloadRoot, variableData, packedStationData, combine, ftype, csvMode);

  let batches = batchDates(options.start_date, options.end_date);
  for(let stationID of stationIDs) {
    for(let batch of batches) {
      options.start_date = batch[0];
      options.end_date = batch[1];
      try {
        let measurements = await projectHandler.listMeasurements(stationID, options);
        await packager.write(stationID, measurements);
      }
      catch(e: any) {
        packager.complete();
        let {status, reason} = e;
        throw {
          status: status || 500,
          reason: reason || `An error occurred while writing a file: ${e}`
        }
      }
    }
  }
  let files = await packager.complete();

  let fpath = "";
  if(files.length < 1) {
    throw {
      status: 404,
      reason: "No data found"
    }
  }
  else {
    let zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen.sh", downloadRoot, packager.packageDir, "data.zip", ...files]);

    //write stdout (should be file name) to output accumulator
    let code = await handleSubprocess(zipProc, (data) => {
      fpath += data.toString();
    });
    //if zip process failed throw error for handling by main error handler  
    if(code !== 0) {
      throw new Error("Zip process failed with code " + code);
    }
  }
  //remove the packager directory, a new one was made with the zip contents
  fs.rmSync(packager.packageDir, {recursive: true, force: true});

  let split = fpath.split("/");
  let fname = split.pop();
  let packageID = split.pop();

  let ep = `${apiURL}/download/package`;
  let params = `packageID=${packageID}&file=${fname}`;
  let downloadLink = `${ep}?${params}`;

  //get package size
  let fstat = fs.statSync(fpath);
  let fsizeB = fstat.size;
  //set size of package for logging
  reqData.sizeB = fsizeB;
  reqData.sizeF = files.length;

  return downloadLink
}


router.get("/mesonet/createPackage/link", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //options
    //start_date, end_date, limit, offset, var_ids (comma separated)
    let { station_ids, location, email, combine, ftype, csvMode, ...options }: any = req.query;
    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }
    if(email) {
      reqData.user = email;
    }
    if(station_ids === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        station_ids: A comma separated list of IDs of the stations to query.
        email (optional): The requesting user's email address.
        combine (optional): A boolean indicating whether all values should be combined into a single file. Default value false.
        ftype (optional): The type of file(s) to pack the data into. Should be "json" or "csv" Default value "csv".
        csvMode (optional): How to pack CSV data if "csv" is selected for ftype. Should be "table" or "matrix" Default value "matrix".
        limit (optional): A number indicating the maximum number of records to be returned for each variable.
        offset (optional): A number indicating an offset in the records returned from the first available record.
        start_date (optional): An ISO-8601 formatted date string indicating the date/time returned records should start at. Default value is 3 months before the current date and time.
        end_date (optional): An ISO-8601 formatted date string indicating the date/time returned records should end at. Default value is the current date and time.
        var_ids (optional): A comma separated list of variable IDs limiting what variables will be returned.
        location (optional): The sensor network location to retrieve data for. Default hawaii`
      );
    }
    let projectHandler = projectHandlers[location];
    if(projectHandler == undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("Unknown location provided.");
    }

    let stationIDs = station_ids.split(",");
    let link: string;
    try {
      link = await createMesonetPackage(projectHandler, stationIDs, combine, ftype, csvMode, options, reqData);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }

    reqData.code = 200;
    res.status(200)
    .send(link);
  });
});


router.get("/mesonet/createPackage/email", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //options
    //start_date, end_date, limit, offset, var_ids (comma separated)
    let { station_ids, location, email, combine, ftype, csvMode, ...options }: any = req.query;
    if(!mesonetLocations.includes(location)) {
      location = "hawaii";
    }
    if(station_ids === undefined || email === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        station_ids: A comma separated list of IDs of the stations to query.
        email: The email address to send the data to once generated.
        combine (optional): A boolean indicating whether all values should be combined into a single file. Default value false.
        ftype (optional): The type of file(s) to pack the data into. Should be "json" or "csv" Default value "csv".
        csvMode (optional): How to pack CSV data if "csv" is selected for ftype. Should be "table" or "matrix" Default value "matrix".
        limit (optional): A number indicating the maximum number of records to be returned for each variable.
        offset (optional): A number indicating an offset in the records returned from the first available record.
        start_date (optional): An ISO-8601 formatted date string indicating the date/time returned records should start at. Default value is 3 months before the current date and time.
        end_date (optional): An ISO-8601 formatted date string indicating the date/time returned records should end at. Default value is the current date and time.
        var_ids (optional): A comma separated list of variable IDs limiting what variables will be returned.
        location (optional): The sensor network location to retrieve data for. Default hawaii`
      );
    }
    let projectHandler = projectHandlers[location];
    if(projectHandler == undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("Unknown location provided.");
    }

    //response should be sent immediately
    //202 accepted indicates request accepted but non-commital completion
    reqData.code = 202;
    res.status(202)
    .send("Request received. Generating download package");

    reqData.user = email;

    let handleError = async (clientError, serverError) => {
      //set failure in status
      reqData.success = false;
      //attempt to send an error email to the user, ignore any errors
      try {
        clientError += " We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later.";
        let mailOptions = {
          to: email,
          subject: "Mesonet Data Error",
          text: clientError,
          html: "<p>" + clientError + "</p>"
        };
        //try to send the error email, last try to actually notify user
        await sendEmail( mailOptions);
      }
      catch(e) {}
      //throw server error to be handled by main error handler
      throw new Error(serverError);
    }
    
    try {
      let stationIDs = station_ids.split(",");
      let link = await createMesonetPackage(projectHandler, stationIDs, combine, ftype, csvMode, options, reqData);
  
      let mailOptions = {
        to: email,
        subject: "Mesonet Data",
        text: "Your Mesonet download package is ready. Please go to " + link + " to download it. This link will expire in three days, please download your data in that time.",
        html: "<p>Your Mesonet download package is ready. Please click <a href=\"" + link + "\">here</a> to download it. This link will expire in three days, please download your data in that time.</p>"
      };
      let mailRes = await sendEmail(mailOptions);
      //if unsuccessful attempt to send error email
      if(!mailRes.success) {
        let serverError = "Failed to send message to user " + email + ". Error: " + mailRes.error.toString();
        let clientError = "There was an error sending your Mesonet download package to this email address.";
        handleError(clientError, serverError);
      }
    }
    catch(e: any) {
      let serverError = `Failed to generate download package for user ${email}. Spawn process failed with error ${e.toString()}.`
      let clientError = "There was an error generating your Mesonet download package.";
      handleError(clientError, serverError);
    }

  });
});