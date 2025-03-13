import express from "express";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { tapisManager } from "../../../modules/util/resourceManagers/tapis.js";
import { processTapisError, handleSubprocess } from "../../../modules/util/util.js";
import { getPaths, fnamePattern, getEmpty } from "../../../modules/fileIndexer.js";
import { urlRoot, dataRoot, apiURL } from "../../../modules/util/config.js";
import * as child_process from "child_process";
import * as path from "path";
import * as fs from "fs";
import * as crypto from "crypto";
import * as url from "url";

export const router = express.Router();

router.get("/raster/timeseries", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
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
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

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
      reqData.sizeF = numFiles;

      let proc;
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
                  //construct ISO date string from parts with defaults for missing values
                  const isoDateStr = `${year}-${month || "01"}-${day || "01"}T${hour || "00"}:${minute || "00"}:${second || "00"}`;
                  timeseries[isoDateStr] = parseFloat(valArr[i]);
              }
          }
          }
      }
      reqData.code = 200;
      res.status(200)
      .json(timeseries);
      }
  });

  });




router.get("/raster", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //destructure query
    let {date, returnEmptyNotFound, type, ...properties} = req.query;
    if(type === undefined) {
      type = "data_map";
    }

    let data = [{
      files: [type],
      range: {
          start: date,
          end: date
      },
      ...properties
    }];
    let files = await getPaths(data, false);
    reqData.sizeF = files.numFiles;
    let file = "";
    //should only be exactly one file
    if(files.numFiles == 0 && returnEmptyNotFound) {
      file = getEmpty(properties.extent);
    }
    else {
      file = files.paths[0];
    }
    
    if(!file) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 404;

      //resources not found
      res.status(404)
      .send("The requested file could not be found");
    }
    else {
      reqData.code = 200;
      res.status(200)
      .sendFile(file);
    }
  });
});


router.get("/production/list", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
      let data: any = req.query.data;
      data = JSON.parse(data);
      if(!Array.isArray(data)) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
          "Request must include the following parameters: \n\
          data: A JSON object representing the dataset to be listed."
      );
      }
      else {
      let files = await getPaths(data, false);
      reqData.sizeF = files.numFiles;
      let fileLinks = files.paths.map((file) => {
          file = path.relative(dataRoot, file);
          let fileLink = `${urlRoot}${file}`;
          return fileLink;
      });
      reqData.code = 200;
      res.status(200)
      .json(fileLinks);
      }
  });
  });

router.get("/stations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { q, limit, offset }: any = req.query;
    try {
      //parse query string to JSON
      q = JSON.parse(q.replace(/'/g, '"'));
    }
    catch {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        q: Mongo DB style query for station documents.
        limit (optional): A number indicating the maximum number of records to be returned for each variable.
        offset (optional): A number indicating an offset in the records returned from the first available record.`
      );
    }
    
    try {
      const data = await tapisManager.queryData(q, limit, offset);
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }
  });
});


router.get("/production/list", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
      let data: any = req.query.data;
      data = JSON.parse(data);
      if(!Array.isArray(data)) {
        //set failure and code in status
        reqData.success = false;
        reqData.code = 400;

        res.status(400)
        .send(
            "Request must include the following parameters: \n\
            data: A JSON object representing the dataset to be listed."
        );
      }
      else {
      let files = await getPaths(data, false);
      reqData.sizeF = files.numFiles;
      let fileLinks = files.paths.map((file) => {
          file = path.relative(dataRoot, file);
          let fileLink = `${urlRoot}${file}`;
          return fileLink;
      });
      reqData.code = 200;
      res.status(200)
      .json(fileLinks);
      }
  });
});

router.get(/^\/files\/explore(\/.*)?$/, async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const allowedDirs = ["NASA_downscaling", "production", "workflow_data", "raw", "backup_data_aqs"]
    const allowedPaths = allowedDirs.map((sub: string) => path.join(dataRoot, sub));
    const epBase = url.resolve(apiURL, "/files/explorer")
    const userPath = req.params[0];
    const dataPath = path.resolve(path.join(dataRoot, userPath));
    if(dataPath == path.resolve(dataRoot)) {
      const pathData: FileData[] = allowedDirs.map((dir: string) => {
        return {
          url: url.resolve(epBase, dir),
          name: dir,
          path: dir,
          sizeBytes: 4096,
          ext: "",
          type: "d"
        }
      });
      reqData.code = 200;
      return res.status(200)
      .json(pathData);
    }
    let allowed = false;
    for(let root in allowedPaths) {
      let rel = path.relative(root, dataPath);
      //if relative path is equivalent to one of the allowed paths or it is a subfolder (no .. and relative), then path is allowed
      if(rel.length == 0 || (!rel.startsWith("..") && !path.isAbsolute(rel))) {
        allowed = true;
        break;
      }
    }
    
    //if path is not allowed or does not exist return 404
    if(!(allowed && fs.existsSync(dataPath))) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("The requested file does not exist or is not accessible.");
    }

    let stat = fs.lstatSync(dataPath);
    if(stat.isFile()) {
      reqData.code = 200;
      return res.status(200)
      .sendFile(dataPath);
    }
    else if(stat.isDirectory()) {
      let data = fs.readdirSync(dataPath)
      .reduce((pathData: FileData[], file: string) => {
        let fpath = path.join(dataPath, file);
        let subStat = fs.lstatSync(fpath);
        if(subStat.isFile()) {
          pathData.push({
            url: url.resolve(epBase, fpath),
            name: file,
            path: fpath,
            sizeBytes: subStat.size,
            ext: path.extname(file),
            type: "f"
          });
        }
        else if(subStat.isDirectory()) {
          pathData.push({
            url: url.resolve(epBase, fpath),
            name: file,
            path: fpath,
            sizeBytes: subStat.size,
            ext: "",
            type: "d"
          });
        }
        return pathData;
      }, []);
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    else {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("The requested file does not exist or is not accessible.");
    }
  });
});

interface FileData {
  url: string,
  path: string,
  name: string,
  ext: string,
  sizeBytes: number,
  type: "f" | "d"
}