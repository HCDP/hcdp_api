import express from "express";
import moment from "moment-timezone";
import * as fs from "fs";
import * as path from "path";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers.js";
import { rawDataRoot, apiURL } from "../../../modules/util/config.js";
import { readdir } from "../../../modules/util/util.js";

export const router = express.Router();

router.get("/raw/download", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    //destructure query
    let { p }: any = req.query;

    if(!p) {
      reqData.success = false;
      reqData.code = 400;
      return res.status(400)
      .send(
        "Request must include the following parameters: \n\
        p: The path to the file to be served."
      );
    }
    //protect against leaving raw dir
    if(p.includes("..")) {
      reqData.success = false;
      reqData.code = 404;
      return res.status(404)
      .send("The requested file could not be found");
    }

    let file = path.join(rawDataRoot, p);

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
        res.set("Content-Disposition", `attachment; filename="${path.basename(file)}"`);
        res.status(200)
        .sendFile(file);
      }
    });
  });
});

router.get("/raw/sff", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
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
        res.set("Content-Disposition", `attachment; filename="sff_data.csv"`);
        res.status(200)
        .sendFile(file);
      }
    });
  });
});

router.get("/raw/list", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { date, startDate, endDate, station_id, location }: any = req.query;

    if(date) {
      startDate = date;
      endDate = date;
    }

    if(!(startDate && endDate)) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        "Request must include the following parameters: \n\
        date: An ISO 8601 formatted date string representing the date you would like the data for. \n\
        startDate: An ISO 8601 formatted date string representing the first date you would like the data for. \n\
        endDate: An ISO 8601 formatted date string representing the last date you would like the data for. \n\
        station_id (optional): The station ID you want to get files for. \n\
        location (optional): The sensor network location to retrieve files for. Default hawaii"
      );
    }
    else {
      if(location === undefined) {
        location = "hawaii"
      }
      let allFiles = [];
      let parsedDate = moment(startDate);
      let parsedEndDate = moment(endDate);
      while(parsedDate.isSameOrBefore(parsedEndDate)) {
        let year = parsedDate.format("YYYY");
        let month = parsedDate.format("MM");
        let day = parsedDate.format("DD");
        let dataDir = path.join(location, year, month, day);
        let sysDir = path.join(rawDataRoot, dataDir);
        let linkDir = `${apiURL}/raw/download?p=${dataDir}/`;
        let { err, files } = await readdir(sysDir);
        //no dir for requested date, just return empty
        if(err && err.code == "ENOENT") {
          files = [];
        }
        else if(err) {
          throw err;
        }
        //if a station ID was specified filter files by ones starting with that id
        if(station_id !== undefined) {
          files = files.filter((file) => {
            let fid = file.split("_")[0];
            return fid == station_id;
          });
        }
        files = files.map((file) => {
          let fileLink = `${linkDir}${file}`;
          return fileLink;
        });
        allFiles = allFiles.concat(files);
        parsedDate.add(1, "day");
      }
      
      reqData.sizeF = allFiles.length;
      reqData.code = 200;
      res.status(200)
      .json(allFiles);
    }
  });
});