import express from "express";
import { getPaths } from "../../../modules/fileIndexer.js";
import { urlRoot, dataRoot, apiURL } from "../../../modules/util/config.js";
import * as path from "path";
import * as fs from "fs";
import * as url from "url";
import { requireAuth } from "../../../modules/util/auth.js";


export const router = express.Router();


router.get(/^\/files\/download(\/.*)?$/, async (req, res) => {
  // Note must be rooted at HCDP folder, so alias from https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/
  const userPath = path.resolve(req.params[0] || "/");
  const dataPath = path.join(dataRoot, userPath);
  
  
  //if path is not allowed or does not exist return 404
  if(!fs.existsSync(dataPath)) {
    return res.status(404)
    .send("The requested file does not exist or is not accessible.");
  }

  let stat = fs.lstatSync(dataPath);
  if(stat.isFile()) {
    let fsizeB = stat.size;
    //set file size for logging
    res.locals.setSizeB(fsizeB);
    res.locals.setSizeF(1);
    return res.status(200)
    .sendFile(dataPath);
  }
  else {
    return res.status(400)
    .send("The provided path is not a file. For directory support use the files/explore endpoint");
  }
});


router.get("/files/production/list", requireAuth("basic"), async (req, res) => {
  let data: any = req.query.data;
  try {
    data = JSON.parse(data);
  }
  catch {
    data = null;
  }
  if(!Array.isArray(data)) {
    res.status(400)
    .send(
      "Request must include the following parameters: \n\
      data: A JSON object representing the dataset to be listed."
    );
  }
  else {
    let files = await getPaths(data, false);
    res.locals.setSizeF(files.numFiles);
    let fileLinks = files.paths.map((file) => {
      file = path.relative(dataRoot, file);
      let fileLink = `${urlRoot}${file}`;
      return fileLink;
    });
    res.status(200)
    .json(fileLinks);
  }
});



router.get("/files/production/retrieve", requireAuth("basic"), async (req, res) => {
  //destructure query
  let {date, file, ...properties} = req.query;

  if(typeof file !== "string" || !file) {
    file = "data_map";
  }
  
  let data: any = [{
    files: [file],
    ...properties
  }];

  if(date) {
    try {
      let parsedDate = new Date(date as string);
      date = parsedDate.toISOString();
      data[0].range = {
        start: date,
        end: date
      }
    }
    catch(e) {
      return res.status(400)
      .send("Invalid date, the date provided could not be parsed. When in doubt please provide dates in an ISO 8601 compliant format.");
    }
  }

  let files = await getPaths(data, false);
  res.locals.setSizeF(files.numFiles);
  let fpath = "";
  //should only be exactly one file
  if(files.numFiles > 0) {
    fpath = files.paths[0];
  }
  
  if(!fpath) {
    //resources not found
    res.status(404)
    .send("The requested file could not be found");
  }
  else {
    res.status(200)
    .sendFile(fpath);
  }
});


router.get(/^\/files\/explore(\/.*)?$/, requireAuth("basic"), async (req, res) => {
  const allowedDirs = ["NASA_downscaling", "production", "workflow_data", "raw", "backup_data_aqs", "kml_files", "empty", "ASCDP", "GCDP", "climate_report_data", "available"]
  const allowedPaths = allowedDirs.map((sub: string) => path.join(dataRoot, sub));
  const userPath = path.resolve(req.params[0] || "/");
  const dataPath = path.join(dataRoot, userPath);

  const getFileData = (file?: string): FileData => {
    let pathData: FileData = null;

    let fpath = dataPath;
    let subUserPath = userPath;
    if(file) {
      fpath = path.join(dataPath, file);
      subUserPath = path.join(userPath, file);
    }
    else {
      file = path.basename(userPath);
    }
    let subStat = fs.lstatSync(fpath);
    const { mtime, size } = subStat;
    const modified = mtime.toISOString();
    if(subStat.isFile()) {
      const subUrlPath = path.join("/files/download", subUserPath);
      pathData = {
        url: url.resolve(apiURL, subUrlPath),
        name: file,
        path: subUserPath,
        sizeBytes: size,
        modified,
        ext: path.extname(file),
        type: "f"
      };
    }
    else if(subStat.isDirectory()) {
      const subUrlPath = path.join("/files/explore", subUserPath);
      pathData = {
        url: url.resolve(apiURL, subUrlPath),
        name: file,
        path: subUserPath,
        sizeBytes: size,
        modified,
        ext: "",
        type: "d"
      };
    }
    return pathData;
  }
  const getPathData = (paths: string[]): FileData[] => {
    return paths.reduce((data: FileData[], file: string) => {
      const pathData = getFileData(file);
      data.push(pathData);
      return data;
    }, []);
  }

  let pathType: "f" | "d";
  let content: FileData[];

  // If root return allowed paths
  if(path.resolve(dataPath) == path.resolve(dataRoot)) {
    pathType = "d";
    content = getPathData(allowedDirs);
  }
  else {
    // Check if path is allowed
    let allowed = false;
    for(let root of allowedPaths) {
      let rel = path.relative(root, dataPath);
      //if relative path is equivalent to one of the allowed paths or it is a subfolder (no .. and relative), then path is allowed
      if(rel.length == 0 || (!rel.startsWith("..") && !path.isAbsolute(rel))) {
        allowed = true;
        break;
      }
    }
    
    //if path is not allowed or does not exist return 404
    if(!(allowed && fs.existsSync(dataPath))) {
      return res.status(404)
      .send("The requested file does not exist or is not accessible.");
    }

    let stat = fs.lstatSync(dataPath);

    if(stat.isFile()) {
      pathType = "f";
      content = [getFileData()];
    }
    else if(stat.isDirectory()) {
      pathType = "d";
      let paths = fs.readdirSync(dataPath);
      content = getPathData(paths);
    }
    else {
      return res.status(404)
      .send("The requested file does not exist or is not accessible.");
    }
  }

  let data: PathData = {
    pathType,
    content
  };

  return res.status(200)
  .json(data);
});


interface PathData {
  pathType: "f" | "d",
  content: FileData[]
}

interface FileData {
  url: string,
  path: string,
  name: string,
  ext: string,
  modified: string,
  sizeBytes: number,
  type: "f" | "d"
}