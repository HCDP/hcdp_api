import express from "express";
import { handleSubprocess, parseBoolParam } from "../../../modules/util/util.js";
import { getPaths, fnamePattern, getEmpty } from "../../../modules/fileIndexer.js";
import { DataPortalLocation } from "../../../modules/util/config.js";
import * as child_process from "child_process";
import * as fs from "fs";
import * as crypto from "crypto";
import { createTZDateFromParts } from "../../../modules/util/dates.js";
import { requireAuth } from "../../../modules/util/auth.js";

export const router = express.Router();



router.get("/raster/timeseries", requireAuth("basic"), async (req, res) => {
  let {start, end, row, col, index, lng, lat, ...properties} = req.query;
  let posParams;
  if(row !== undefined && col !== undefined) {
    posParams = ["-r", row, "-c", col];
  }
  else if(index !== undefined) {
    posParams = ["-i", index];
  }
  else if(lng !== undefined && lat !== undefined) {
    posParams = ["-x", lng, "-y", lat];
  }
  if(start === undefined || end === undefined || posParams === undefined) {
    res.status(400)
    .send(
      `Request must include the following parameters:
      start: An ISO 8601 formatted date string representing the start date of the timeseries.
      end: An ISO 8601 formatted date string representing the end date of the timeseries.
      {index: The 1D index of the data in the file.
      OR
      row AND col: The row and column of the data.
      OR
      lat AND lng: The geographic coordinates of the data}`
    );
  }
  else {
  let dataset = [{
    files: ["data_map"],
    range: {
    start,
    end
    },
    ...properties
  }];
  //need files directly, don't collapse
  let { numFiles, paths } = await getPaths(dataset, false);
  res.locals.setSizeF(numFiles);

  let proc: child_process.ChildProcessWithoutNullStreams;
  //error if paths empty
  let timeseries = {};
  //if no paths just return empty timeseries
  if(paths.length != 0) {
    //want to avoid argument too large errors for large timeseries
    //write very long path lists to temp file
    // getconf ARG_MAX = 2097152
    //should be alright if less than 10k paths
    if(paths.length < 10000) {
      proc = child_process.spawn("../assets/tiffextract.out", [...posParams, ...paths]);
    }
    //otherwise write paths to a file and use that
    else {
      let uuid = crypto.randomUUID();
      //write paths to a file and use that, avoid potential issues from long cmd line params
      fs.writeFileSync(uuid, paths.join("\n"));
  
      proc = child_process.spawn("../assets/tiffextract.out", ["-f", uuid, ...posParams]);
      //delete temp file on process exit
      proc.on("exit", () => {
          fs.unlinkSync(uuid);
      });
    } 
    
    let values = "";
    let code = await handleSubprocess(proc, (data) => {
      values += data.toString();
    });

    if(code !== 0) {
      //if extractor process failed throw error for handling by main error handler
      throw new Error(`Geotiff extract process failed with code ${code}`);
    }

    let valArr = values.trim().split(" ");
    if(valArr.length != paths.length) {
      //issue occurred in geotiff extraction if output does not line up, allow main error handler to process and notify admins
      throw new Error(`An issue occurred in the geotiff extraction process. The number of output values does not match the input. Output: ${values}`);
    }

    //order of values should match file order
    for(let i = 0; i < paths.length; i++) {
      //if the return value for that file was empty (error reading) then skip
      if(valArr[i] !== "_") {
        let path = paths[i];
        let match = path.match(fnamePattern);
        //should never be null otherwise wouldn't have matched file to begin with, just skip if it magically happens
        if(match !== null) {
          //capture date from fname and split on underscores
          let dateParts = match[1].split("_");
          //get parts
          const [year, month, day, hour, minute, second] = dateParts;
          const isoDateStr = createTZDateFromParts(<DataPortalLocation>properties.location || "hawaii", {year, month, day, hour, minute, second}, true).toISOString();
          timeseries[isoDateStr] = parseFloat(valArr[i]);
        }
      }
    }
  }
  res.status(200)
  .json(timeseries);
  }
});



router.get("/raster", requireAuth("basic"), async (req, res) => {
  //destructure query
  let {date, type, returnEmptyNotFound, ...properties} = req.query;

  let returnEmptyNotFoundBool = parseBoolParam(returnEmptyNotFound);

  if(!type) {
    type = "data_map";
  }

  let data: any[] = [{
    files: [type],
    range: {
        start: date,
        end: date
    },
    ...properties
  }];
  let files = await getPaths(data, false);
  res.locals.setSizeF(files.numFiles);
  let file = "";
  //should only be exactly one file
  if(files.numFiles == 0 && returnEmptyNotFoundBool) {
    let { location, extent } = data[0];
    file = getEmpty(location, extent);
  }
  else {
    file = files.paths[0];
  }
  
  if(!file) {
    //resources not found
    res.status(404)
    .send("The requested file could not be found");
  }
  else {
    res.status(200)
    .sendFile(file);
  }
});
