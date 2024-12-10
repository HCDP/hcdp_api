import express from "express";
import compression from "compression";
import cors from "cors";
import * as nodemailer from "nodemailer";
import * as https from "https";
import * as fs from "fs";
import * as child_process from "child_process";
import moment from "moment-timezone";
import * as path from "path";
import * as sanitize from "mongo-sanitize";
import CsvReadableStream from "csv-reader";
import * as detectDecodeStream from "autodetect-decoder-stream";
import * as crypto from "crypto";
import * as safeCompare from "safe-compare";
import sslRootCAs from "ssl-root-cas";

import { MesonetDataPackager } from "./modules/mesonetDataPackager.js";
import { DBManager, TapisManager, TapisV3Manager, ProjectHandler } from "./modules/tapisHandlers.js";
import { getPaths, fnamePattern, getEmpty } from "./modules/fileIndexer.js";
import { HCDPDBManager } from "./modules/hcdpDBManager.js";

//add timestamps to output
import consoleStamp from 'console-stamp';
consoleStamp(console);

const config = JSON.parse(fs.readFileSync("../assets/config.json", "utf8"));

// const githubMiddleware = require('github-webhook-middleware')({
//   secret: config.githubWebhookSecret,
//   limit: "25mb", //webhook json payload size limit. Default is '100kb' (25mb is github max, should never get that big for metadata, but want to make sure larger commits are processed)
// });

const keyFile = "../assets/privkey.pem";
const certFile = "../assets/fullchain.pem";
const hskey = fs.readFileSync(keyFile);
const hscert = fs.readFileSync(certFile);

const port = config.port;
const smtp = config.smtp;
const smtpPort = config.smtpPort;
const mailOptionsBase = config.email;
const defaultZipName = config.defaultZipName;

const dataRoot = config.dataRoot;
const urlRoot = config.urlRoot;
const rawDataDir = config.rawDataDir;
const downloadDir = config.downloadDir;
const userLog = config.userLog;
const administrators = config.administrators;
const tapisDBConfig = config.tapisDBConfig;
const hcdpDBConfig = config.hcdpDBConfig;
const productionDir = config.productionDir;
const licensePath = config.licenseFile;
const tapisConfig = config.tapisConfig;
const tapisV3Config = config.tapisV3Config;
const githubWebhookSecret = config.githubWebhookSecret;

const rawDataRoot = `${dataRoot}${rawDataDir}`;
const rawDataURLRoot = `${urlRoot}${rawDataDir}`;
const downloadRoot = `${dataRoot}${downloadDir}`;
const downloadURLRoot = `${urlRoot}${downloadDir}`;
const productionRoot = `${dataRoot}${productionDir}`;
const licenseFile = `${dataRoot}${licensePath}`;

const apiURL = "https://api.hcdp.ikewai.org";

const transporterOptions = {
  host: smtp,
  port: smtpPort,
  secure: false
};

//gmail attachment limit
const ATTACHMENT_MAX_MB = 25;

//process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";
process.env["NODE_ENV"] = "production";

const hcdpDBManagerMesonet = new HCDPDBManager(hcdpDBConfig.host, hcdpDBConfig.port, "mesonet", hcdpDBConfig.userCredentials, hcdpDBConfig.adminCredentials);
const hcdpDBManagerHCDP = new HCDPDBManager(hcdpDBConfig.host, hcdpDBConfig.port, "hcdp", hcdpDBConfig.userCredentials, hcdpDBConfig.adminCredentials);

const tapisDBManager = new DBManager(tapisDBConfig.server, tapisDBConfig.port, tapisDBConfig.username, tapisDBConfig.password, tapisDBConfig.db, tapisDBConfig.collection, tapisDBConfig.connectionRetryLimit, tapisDBConfig.queryRetryLimit);
const tapisManager = new TapisManager(tapisConfig.tenantURL, tapisConfig.token, tapisDBConfig.queryRetryLimit, tapisDBManager);
const tapisV3Manager = new TapisV3Manager(tapisV3Config.username, tapisV3Config.password, tapisV3Config.tenantURL, tapisDBConfig.queryRetryLimit, tapisManager);

const projectHandlers: {[location: string]: ProjectHandler} = {};
for(let location in tapisV3Config.streams.projects) {
  let projectConfig = tapisV3Config.streams.projects[location];
  projectHandlers[location] = tapisV3Manager.streams.getProjectHandler(projectConfig.project, projectConfig.timezone);
}

////////////////////////////////
//////////server setup//////////
////////////////////////////////

const app = express();

app.options('*', cors());
sslRootCAs.inject();

let options = {
    key: hskey,
    cert: hscert
};

const server = https.createServer(options, app)
.listen(port, () => {
  console.log("Server listening at port " + port);
});

app.use(express.json());
//compress all HTTP responses
app.use(compression());

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST");
  res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization, Range, Content-Range, Cache-Control");
  //pass to next layer
  next();
});

////////////////////////////////
////////////////////////////////

/////////////////////////////
///////signal handling///////
/////////////////////////////

const signals = {
  "SIGHUP": 1,
  "SIGINT": 2,
  "SIGTERM": 15
};

function shutdown(code) {
  tapisV3Manager.close();
  //stops new connections and completes existing ones before closing
  server.close(() => {
    console.log(`Server shutdown.`);
    process.exit(code);
  });
}

for(let signal in signals) {
  let signalVal = signals[signal];
  process.on(signal, () => {
    console.log(`Received ${signal}, shutting down server...`);
    shutdown(128 + signalVal);
  });
}

/////////////////////////////
/////////////////////////////


async function handleSubprocess(subprocess, dataHandler, errHandler?) {
  return new Promise((resolve, reject) => {
    if(!errHandler) {
      errHandler = () => {};
    }
    if(!dataHandler) {
      dataHandler = () => {};
    }
    //write content to res
    subprocess.stdout.on("data", dataHandler);
    subprocess.stderr.on("data", errHandler);
    subprocess.on("exit", (code) => {
      resolve(code);
    });
  });
}


async function readdir(dir): Promise<{err, files}> {
  return new Promise((resolve, reject) => {
    fs.readdir(dir, (err, files) => {
      resolve({err, files});
    });
  });
}

async function validateTokenAccess(token, permission) {
  let valid = false;
  let allowed = false;
  let user = "";

  let query = `
    SELECT user_label, permissions
    FROM auth_token_store
    WHERE token = $1;
  `;
  
  let queryHandler = await hcdpDBManagerHCDP.query(query, [token], {privileged: true});
  let queryRes = await queryHandler.read(1);
  queryHandler.close();
  if(queryRes.length > 0) {
    let { user_label, permissions } = queryRes[0];
    valid = true;
    user = user_label;
    const authorized = permissions.split(",");
    if(authorized.includes(permission)) {
      allowed = true;
    }
  }
  return {
    valid,
    allowed,
    token,
    user
  };
}

async function validateToken(req, permission) {
  let tokenData = {
    valid: false,
    allowed: false,
    token: "",
    user: ""
  };

  let auth = req.get("authorization");
  if(auth) {
    let authPattern = /^Bearer (.+)$/;
    let match = auth.match(authPattern);
    if(match) {
      //validate token is registered and has required permission
      tokenData = await validateTokenAccess(match[1], permission);
    }
  }
  return tokenData;
}

async function sendEmail(transporterOptions, mailOptions) {
  let combinedMailOptions = Object.assign({}, mailOptionsBase, mailOptions);
  let transporter = nodemailer.createTransport(transporterOptions);
  //have to be on uh netork
  return transporter.sendMail(combinedMailOptions)
  .then((info) => {
    //should parse response for success (should start with 250) 
    return {
      success: true,
      result: info,
      error: null
    };
  })
  .catch((error) => {
    return {
      success: false,
      result: null,
      error: error
    };
  });
}


function logReq(data) {
  const { user, code, success, sizeF, method, endpoint, token, sizeB, tokenUser } = data;
  const timestamp = new Date().toLocaleString("sv-SE", {timeZone:"Pacific/Honolulu"});
  let dataString = `[${timestamp}] ${method}:${endpoint}:${user}:${tokenUser}:${token}:${code}:${success}:${sizeB}:${sizeF}\n`;
  fs.appendFile(userLog, dataString, (err) => {
    if(err) {
      console.error(`Failed to write userlog.\nError: ${err}`);
    }
  });
}

async function handleReqNoAuth(req, res, handler) {
  //note include success since 202 status might not indicate success in generating download package
  //note sizeB will be 0 for everything but download packages
  let reqData = {
    user: "",
    code: 0,
    success: true,
    sizeF: 0,
    method: req.method,
    endpoint: req.path,
    token: "",
    sizeB: 0,
    tokenUser: ""
  };
  try {
    await handler(reqData);
  }
  catch(e) {
    //set failure occured in request
    reqData.success = false;
    let errorMsg = `method: ${reqData.method}\n\
      endpoint: ${reqData.endpoint}\n\
      error: ${e}`;
    let htmlErrorMsg = errorMsg.replace(/\n/g, "<br>");
    console.error(`An unexpected error occured:\n${errorMsg}`);
    //if request code not set by handler set to 500 and send response (otherwise response already sent and error was in post-processing)
    if(reqData.code == 0) {
      reqData.code = 500;
      res.status(500)
      .send("An unexpected error occurred");
    }
    //send the administrators an email logging the error
    if(administrators.length > 0) {
      let mailOptions = {
        to: administrators,
        subject: "HCDP API error",
        text: `An unexpected error occured in the HCDP API:\n${errorMsg}`,
        html: `<p>An error occured in the HCDP API:<br>${htmlErrorMsg}</p>`
      };
      try {
        //attempt to send email to the administrators
        let emailStatus = await sendEmail(transporterOptions, mailOptions);
        //if email send failed throw error for logging
        if(!emailStatus.success) {
          throw emailStatus.error;
        }
      }
      //if error while sending admin email erite to stderr
      catch(e) {
        console.error(`Failed to send administrator notification email: ${e}`);
      }
    }
  }
  logReq(reqData);
}

async function handleReq(req, res, permission, handler) {
  //note include success since 202 status might not indicate success in generating download package
  //note sizeB will be 0 for everything but download packages
  let reqData = {
    user: "",
    code: 0,
    success: true,
    sizeF: 0,
    method: req.method,
    endpoint: req.path,
    token: "",
    sizeB: 0,
    tokenUser: ""
  };
  try {
    const tokenData = await validateToken(req, permission);
    const { valid, allowed, token, user } = tokenData;
    reqData.token = token;
    reqData.tokenUser = user;
    //token was valid and user is allowed to perform this action, send to handler
    if(valid && allowed) {
      await handler(reqData);
    }
    //token was not provided or not in whitelist, return 401
    else if(!valid) {
      reqData.code = 401;
      res.status(401)
      .send("User not authorized. Please provide a valid API token in the request header. If you do not have an API token one can be requested from the administrators.");
    }
    //token was valid in whitelist but does not have permission to access this endpoint, return 403
    else {
      reqData.code = 403;
      res.status(403)
      .send("User does not have permission to perform this action.");
    }
  }
  catch(e) {
    //set failure occured in request
    reqData.success = false;
    let errorMsg = `method: ${reqData.method}\n\
      endpoint: ${reqData.endpoint}\n\
      error: ${e}`;
    let htmlErrorMsg = errorMsg.replace(/\n/g, "<br>");
    console.error(`An unexpected error occured:\n${errorMsg}`);
    //if request code not set by handler set to 500 and send response (otherwise response already sent and error was in post-processing)
    if(reqData.code == 0) {
      reqData.code = 500;
      res.status(500)
      .send("An unexpected error occurred");
    }
    //send the administrators an email logging the error
    if(administrators.length > 0) {
      let mailOptions = {
        to: administrators,
        subject: "HCDP API error",
        text: `An unexpected error occured in the HCDP API:\n${errorMsg}`,
        html: `<p>An error occured in the HCDP API:<br>${htmlErrorMsg}</p>`
      };
      try {
        //attempt to send email to the administrators
        let emailStatus = await sendEmail(transporterOptions, mailOptions);
        //if email send failed throw error for logging
        if(!emailStatus.success) {
          throw emailStatus.error;
        }
      }
      //if error while sending admin email erite to stderr
      catch(e) {
        console.error(`Failed to send administrator notification email: ${e}`);
      }
    }
  }
  logReq(reqData);
}


app.get("/raster/timeseries", async (req, res) => {
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
      let { numFiles, paths } = await getPaths(productionRoot, dataset, false);
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


app.post("/db/replace", async (req, res) => {
  const permission = "db";
  await handleReq(req, res, permission, async (reqData) => {
    const uuid = req.body.uuid;
    let value = req.body.value;

    if(typeof uuid !== "string" || value === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        uuid: A string representing the uuid of the document to have it's value replaced \n\
        value: The new value to set the document's 'value' field to`
      );
    }
    else {
      //sanitize value object to ensure no $ fields since this can be an arbitrary object
      value = sanitize(value);
      //note this only replaces value, should not be wrapped with name
      let replaced = await tapisDBManager.replaceRecord(uuid, value);
      reqData.code = 200;
      res.status(200)
      .send(replaced.toString());
    }
  });
});

app.post("/db/delete", async (req, res) => {
  const permission = "db";
  await handleReq(req, res, permission, async (reqData) => {
    const uuid = req.body.uuid;

    if(typeof uuid !== "string") {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      return res.status(400)
      .send(
        `Request body should include the following fields: \n\
        uuid: A string representing the uuid of the document to delete.`
      );
    }
    else {
      let deleted = await tapisDBManager.deleteRecord(uuid);
      reqData.code = 200;
      res.status(200)
      .send(deleted.toString());
    }
  });
});

app.post("/db/bulkDelete", async (req, res) => {
  const permission = "db";
  await handleReq(req, res, permission, async (reqData) => {
    const uuids = req.body.uuids;

    if(!Array.isArray(uuids)) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      return res.status(400)
      .send(
        `Request body should include the following fields: \n\
        uuids: An array string representing the uuids of the documents to delete`
      );
    }
    else {
      let deleted = await tapisDBManager.bulkDelete(uuids);
      reqData.code = 200;
      res.status(200)
      .send(deleted.toString());
    }
  });
});

app.get("/raster", async (req, res) => {
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
    let files = await getPaths(productionRoot, data, false);
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


app.get("/download/package", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    let e400 = () => {
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        packageID: A string representing the uuid of the package to be downloaded  \n\
        file: A string representing the name of the file to be downloaded`
      );
    }

    let { packageID, file }: any = req.query;
    if(!(packageID && file)) {
      return e400();
    }
    //check id and file name format
    let idRegex = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/g;
    let fileRegex = /^[\w\-. ]+$/g;
    if(packageID.match(idRegex) === null) {
      return e400();
    }
    if(file.match(fileRegex) === null) {
      return e400();
    }
    let downloadPath = path.join("/data/downloads", packageID, file);
    fs.access(downloadPath, fs.constants.F_OK, (e) => {
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
        res.set("Content-Disposition", `attachment; filename="${file}"`);
        res.status(200)
        .sendFile(downloadPath);
      }
    });
  });
});


app.post("/genzip/email", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let email = req.body.email;
    let data = req.body.data;
    let zipName = req.body.name || defaultZipName;

    if(email) {
      reqData.user = email;
    }

    //make sure required parameters exist and data is an array
    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip \n\
        email: The email to send the package to \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      reqData.code = 202;
      //response should be sent immediately after file check, don't wait for email to finish
      //202 accepted indicates request accepted but non-commital completion
      res.status(202)
      .send("Request received. Generating download package");

      let handleError = async (clientError, serverError) => {
        //set failure in status
        reqData.success = false;
        //attempt to send an error email to the user, ignore any errors
        try {
          clientError += " We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later.";
          let mailOptions = {
            to: email,
            subject: "HCDP Data Error",
            text: clientError,
            html: "<p>" + clientError + "</p>"
          };
          //try to send the error email, last try to actually notify user
          await sendEmail(transporterOptions, mailOptions);
        }
        catch(e) {}
        //throw server error to be handled by main error handler
        throw new Error(serverError);
      }
      //wrap in try catch so send error email if anything unexpected goes wrong
      try {
        //note no good way to validate email address, should have something in app saying that if email does not come to verify spelling
        //email should arrive in the next few minutes, if email does not arrive within 2 hours we may have been unable to send the email, check for typos, try again, or contact the site administrators

        /////////////////////////////////////
        // generate package and send email //
        /////////////////////////////////////
        
        //get paths
        let { paths, numFiles } = await getPaths(productionRoot, data);
        //add license file
        paths.push(licenseFile);
        numFiles += 1;

        //make relative so zip doesn't include production path
        paths = paths.map((file) => {
          return path.relative(productionRoot, file);
        });

        reqData.sizeF = numFiles;
        let zipPath = "";
        let zipProc;
        zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen.sh", downloadRoot, productionRoot, zipName, ...paths]);

        let code = await handleSubprocess(zipProc, (data) => {
          zipPath += data.toString();
        });

        if(code !== 0) {
          let serverError = `Failed to generate download package for user ${email}. Zip process failed with code ${code}.`
          let clientError = "There was an error generating your HCDP download package.";
          handleError(clientError, serverError);
        }
        else {
          let zipDec = zipPath.split("/");
          let zipRoot = zipDec.slice(0, -1).join("/");
          let [ packageID, fname ] = zipDec.slice(-2);

          //get package size
          let fstat = fs.statSync(zipPath);
          let fsizeB = fstat.size;
          //set size of package for logging
          reqData.sizeB = fsizeB;
          let fsizeMB = fsizeB / (1024 * 1024);

          let attachFile = fsizeMB < ATTACHMENT_MAX_MB;

          let mailRes;

          if(attachFile) {
            let attachments = [{
              filename: zipName,
              content: fs.createReadStream(zipPath)
            }];
            let mailOptions = {
              to: email,
              attachments: attachments,
              text: "Your HCDP data package is attached.",
              html: "<p>Your HCDP data package is attached.</p>"
            };
            
            mailOptions = Object.assign({}, mailOptionsBase, mailOptions);
            mailRes = await sendEmail(transporterOptions, mailOptions);
            //if an error occured fall back to link and try one more time
            if(!mailRes.success) {
              attachFile = false;
            }
          }

          //recheck, state may change if fallback on error
          if(!attachFile) {
            let ep = `${apiURL}/download/package`;
            let params = `packageID=${packageID}&file=${fname}`;
            //create download link and send in message body
            let downloadLink = `${ep}?${params}`;
            let mailOptions = {
              to: email,
              text: "Your HCDP download package is ready. Please go to " + downloadLink + " to download it. This link will expire in three days, please download your data in that time.",
              html: "<p>Your HCDP download package is ready. Please click <a href=\"" + downloadLink + "\">here</a> to download it. This link will expire in three days, please download your data in that time.</p>"
            };
            mailRes = await sendEmail(transporterOptions, mailOptions);
          }
          //cleanup file if attached
          //otherwise should be cleaned by chron task
          //no need error handling, if error chron should handle later
          else {
            child_process.exec("rm -r " + zipRoot);
          }

          //if unsuccessful attempt to send error email
          if(!mailRes.success) {
            let serverError = "Failed to send message to user " + email + ". Error: " + mailRes.error.toString();
            let clientError = "There was an error sending your HCDP download package to this email address.";
            handleError(clientError, serverError);
          }
        }
      }
      catch(e: any) {
        let serverError = `Failed to generate download package for user ${email}. Spawn process failed with error ${e.toString()}.`
        let clientError = "There was an error generating your HCDP download package.";
        handleError(clientError, serverError);
      }
    }
  });
});


app.post("/genzip/instant/content", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      reqData.user = email;
    }

    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging"
      );
    }
    else {
      let { paths, numFiles } = await getPaths(productionRoot, data);
      reqData.sizeF = numFiles;
      if(paths.length > 0) {
        res.contentType("application/zip");
  
        let zipProc = child_process.spawn("zip", ["-qq", "-r", "-", ...paths]);

        let code = await handleSubprocess(zipProc, (data) => {
          //get data chunk size
          let dataSizeB = data.length;
          //add size of data chunk
          reqData.sizeB += dataSizeB;
          //write data to stream
          res.write(data);
        });
        //if zip process failed throw error for handling by main error handler
        if(code !== 0) {
          throw new Error("Zip process failed with code " + code);
        }
        else {
          reqData.code = 200;
          res.status(200)
          .end();
        }
      }
      //just send empty if no files
      else {
        reqData.code = 200;
        res.status(200)
        .end();
      }
    }
  });
});


app.post("/genzip/instant/link", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let zipName = defaultZipName;
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      reqData.user = email;
    }

    //if not array then leave files as 0 length to be picked up by error handler
    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      let { paths, numFiles } = await getPaths(productionRoot, data);
      //add license file
      paths.push(licenseFile);
      numFiles += 1;

      //make relative so zip doesn't include production path
      paths = paths.map((file) => {
        return path.relative(productionRoot, file);
      });

      reqData.sizeF = numFiles;
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen.sh", downloadRoot, productionRoot, zipName, ...paths]);
      let zipPath = "";

      //write stdout (should be file name) to output accumulator
      let code = await handleSubprocess(zipProc, (data) => {
        zipPath += data.toString();
      });
      //if zip process failed throw error for handling by main error handler  
      if(code !== 0) {
        throw new Error("Zip process failed with code " + code);
      }
      else {
        let zipDec = zipPath.split("/");
        let [ packageID, fname ] = zipDec.slice(-2);

        let ep = `${apiURL}/download/package`;
        let params = `packageID=${packageID}&file=${fname}`;
        let downloadLink = `${ep}?${params}`;

        //get package size
        let fstat = fs.statSync(zipPath);
        let fsizeB = fstat.size;
        //set size of package for logging
        reqData.sizeB = fsizeB;

        reqData.code = 200;
        res.status(200)
        .send(downloadLink);
      }
    }
  });
});


app.post("/genzip/instant/splitlink", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      reqData.user = email;
    }

    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging`
      );
    }
    else {
      let { paths, numFiles } = await getPaths(productionRoot, data);
      //add license file
      paths.push(licenseFile);
      numFiles += 1;

      //make relative so zip doesn't include production path
      paths = paths.map((file) => {
        return path.relative(productionRoot, file);
      });

      reqData.sizeF = numFiles;
      res.contentType("application/zip");
      let zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen_parts.sh", downloadRoot, productionRoot, ...paths]);
      let zipOutput = "";

      //write stdout (should be file name) to output accumulator
      let code = await handleSubprocess(zipProc, (data) => {
        zipOutput += data.toString();
      });

      if(code !== 0) {
        throw new Error("Zip process failed with code " + code);
      }
      else {
        let parts = zipOutput.split(" ");
        let fileParts: string[] = [];
        let uuid = parts[0];
        for(let i = 1; i < parts.length; i++) {
          let fpart = parts[i];
          //make sure not empty
          if(fpart == "") {
            break;
          }

          let ep = `${apiURL}/download/package`
          let params = `packageID=${uuid}&file=${fpart}`;
          let downloadLink = `${ep}?${params}`;

          fileParts.push(downloadLink);

          //get part path
          let partPath = path.join(downloadRoot, uuid, fpart);
          //get part size
          let fstat = fs.statSync(partPath);
          let fsizeB = fstat.size;
          //add part size
          reqData.sizeB += fsizeB;
        }

        let data = {
          files: fileParts
        }
        reqData.code = 200;
        res.status(200)
        .json(data);
      }
    }
  });
});


app.get("/production/list", async (req, res) => {
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
        data: A string encoded JSON query representing an array of file data objects describing a set of files to zip."
      );
    }
    else {
      let files = await getPaths(productionRoot, data, false);
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


app.get("/raw/download", async (req, res) => {
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

app.get("/raw/sff", async (req, res) => {
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

app.get("/raw/list", async (req, res) => {
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


app.get("/apistats", async (req, res) => {
  try {
    //start with no params, might want to add date range, need to modify scripts or otherwise make additional processing
    //should migrate log locations to config
    const logfile = "/logs/userlog.txt";
    const logfileOld = "/logs/userlog_old_2.txt";
    const logscript = "/logs/utils/gen_report_json.sh";
    const logscriptOld = "/logs/utils/gen_report_old_json.sh";
    let resData: any[] = [];
    let procHandles = [child_process.spawn("/bin/bash", [logscript, logfile]), child_process.spawn("/bin/bash", [logscriptOld, logfileOld])].map((proc) => {
      return new Promise<void>(async (resolve, reject) => {
        try {
          let output = "";
          let code = await handleSubprocess(proc, (data) => {
            output += data.toString();
          });
          if(code == 0) {
            //strip out emails, can use this for additional processing if expanded on, don't want to provide to the public
            let json = JSON.parse(output);
            delete json.unique_emails;
            resData.push(json);
          }
          resolve();
        }
        catch {
          resolve();
        }
      });
    });
    Promise.all(procHandles).then(() => {
      res.status(200)
      .json(resData);
    });
  }
  catch(e) {
    res.status(500)
    .send("An unexpected error occurred.");
  }
});

function signBlob(key, blob) {
  return "sha1=" + crypto.createHmac("sha1", key).update(blob).digest("hex");
}

//add middleware to get raw body, don't actually need body data so no need to do anything fancy to get parsed body as well
app.post("/addmetadata", express.raw({ limit: "50mb", type: () => true }), async (req, res) => {
  try {
    //ensure this is coming from github by hashing with the webhook secret
    const receivedSig = req.headers['x-hub-signature'];
    const computedSig = signBlob(githubWebhookSecret, req.body);
    if(!safeCompare(receivedSig, computedSig)) {
      return res.status(401).end();
    }
    //only process github push events
    if(req.headers["x-github-event"] != "push") {
      return res.status(200).end();
    }
    let header: string[] | null = null;
    //might want to move file location/header translations to config
    https.get("https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/Hawaii_Master_Station_Meta.csv", (res) => {
      let docs: any[] = [];
      res.pipe(new detectDecodeStream({ defaultEncoding: "1255" }))
      //note old data does not parse numbers, maybe reprocess data with parsed numbers at some point, for now leave everything as strings though
      .pipe(new CsvReadableStream({ parseNumbers: false, parseBooleans: false, trim: true }))
      .on("data", (row) => {
        if(header === null) {
          let translations = {
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
          }
          header = [];
          for(let property of row) {
            let trans = translations[property] ? translations[property] : property;
            header.push(trans);
          }
        }
        else {
          let data = {
            station_group: "hawaii_climate_primary",
            id_field: "skn"
          };
          for(let i = 0; i < header.length; i++) {
            let property = header[i];
            let value = row[i];
            if(value != "NA") {
              data[property] = value;
            }
          }
          let doc = {
            name: "hcdp_station_metadata",
            value: data
          };
          docs.push(doc);
        }
      })
      .on("end", () => {
        //if there are a lot may want to add ability to process in chunks in the future, only a few thousand at the moment so just process all at once
        tapisManager.createMetadataDocs(docs)
        .catch((e) => {
          console.error(`Metadata ingestion failed. Errors: ${e}`);
        });

      })
      .on("error", (e) => {
        console.error(`Failed to get/read master metadata file. Error: ${e}`);
      });
    });
    res.status(202)
    .send("Metadata update processing.");
  }
  catch(e) {
    console.error(`An unexpected error occurred while processing the metadata request. Error: ${e}`);
    res.status(500)
    .send("An unexpected error occurred.");
  }
});




///////////////////////////////////////////////////
/////////////// mesonet eps ///////////////////////
///////////////////////////////////////////////////

function processTapisError(res, reqData, e) {
  let {status, reason} = e;
  if(status === undefined) {
    status = 500;
  }
  if(reason === undefined) {
    reason = "An error occured while processing the request.";
    console.error(`An unexpected error occurred while listing the measurements. Error: ${e}`);
  }
  reqData.code = status;
  res.status(status)
  .send(reason);
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
  query = hcdpDBManagerMesonet.mogrify(query, params);
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
  let queryHandler = await hcdpDBManagerMesonet.query(query, params, { rowMode: "array" });
  let data = await queryHandler.read(10000);
  queryHandler.close();
  data = data.flat();
  return data;
}


app.get("/mesonet/db/measurements", async (req, res) => {
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
      let queryHandler = await hcdpDBManagerMesonet.query(query, params, {rowMode: row_mode});
    
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
      let queryHandler = await hcdpDBManagerMesonet.query(query, [location]);
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

app.get("/mesonet/db/stations", async (req, res) => {
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

    let queryHandler = await hcdpDBManagerMesonet.query(query, params, {rowMode: row_mode});

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


app.get("/mesonet/db/variables", async (req, res) => {
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

    let queryHandler = await hcdpDBManagerMesonet.query(query, params, {rowMode: row_mode});

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

app.get("/mesonet/db/sff", async (req, res) => {
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

    let queryHandler = await hcdpDBManagerMesonet.query(query, []);
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

app.patch("/mesonet/db/setFlag", async (req, res) => {
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

    let queryHandler = await hcdpDBManagerMesonet.query(query, [stationID]);
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

    let modified = await hcdpDBManagerMesonet.queryNoRes(query, params, { privileged: true });

    reqData.code = 200;
    return res.status(200)
    .json({ modified });
  });
});


















app.get("/mesonet/getStations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { location }: any = req.query;
    if(location === undefined) {
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

app.get("/mesonet/getVariables", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_id, location }: any = req.query;
    if(location === undefined) {
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


app.get("/mesonet/getMeasurements", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //options
    //start_date, end_date, limit, offset, var_ids (comma separated)
    let { station_id, location, ...options }: any = req.query;
    if(location === undefined) {
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


app.get("/mesonet/createPackage/link", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //options
    //start_date, end_date, limit, offset, var_ids (comma separated)
    let { station_ids, location, email, combine, ftype, csvMode, ...options }: any = req.query;
    if(location === undefined) {
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


app.get("/mesonet/createPackage/email", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    //options
    //start_date, end_date, limit, offset, var_ids (comma separated)
    let { station_ids, location, email, combine, ftype, csvMode, ...options }: any = req.query;
    if(location === undefined) {
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
        await sendEmail(transporterOptions, mailOptions);
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
      let mailRes = await sendEmail(transporterOptions, mailOptions);
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


app.get("/stations", async (req, res) => {
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


app.post("/notify", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {
    const { recepients, source, type, message } = req.body;

    if(!Array.isArray(recepients) || recepients.length < 1) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body should be JSON with the fields:
        recepients: An array of email addresses to send the notification to.
        source (optional): The source of the notification.
        type (optional): The notification type (e.g. Error, Info, etc.).
        message (optional): The notification message.`
      );
    }

    let mailOptions = {
      to: recepients,
      subject: `HCDP Notifier: ${type}`,
      text: `${type}\nNotification source: ${source}\nNotification message: ${message}`,
      html: `<h3>${type}</h3><p>Notification source: ${source}</p><p>Notification message: ${message}</p>`
    };
    try {
      //attempt to send email to the recepients list
      let emailStatus = await sendEmail(transporterOptions, mailOptions);
      //if email send failed throw error for logging
      if(!emailStatus.success) {
        throw emailStatus.error;
      }
    }
    //if error while sending admin email erite to stderr
    catch(e) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 500;

      return res.status(500)
      .send(
        `The notification could not be sent. Error: ${e}`
      );
    }
    reqData.code = 200;
    return res.status(200)
    .send("Success! A notification has been sent to the requested recepients.");
  });
});


app.post("/registerTokenRequest", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {
    let { requestID, name, email, organization, position, reason } = req.body;
    
    if(!requestID || !name  || !email || !reason) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body should be JSON with the fields:
        requestID: The ID of the request to register.
        name: The name of the requestor.
        email: The email of the requestor.
        organization (optional): The organization the requestor belongs to.
        position (optional): The requestors title or position.
        reason: The reason for the request or expected API usage.`
      );
    }

    organization = organization || null;
    position = position || null;
    
    const timestamp = new Date().toISOString();
    let query = "INSERT INTO token_requests VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);";
    await hcdpDBManagerHCDP.queryNoRes(query, [requestID, timestamp, null, null, name, email, organization, position, reason], {privileged: true})

    reqData.code = 201;
    return res.status(201)
    .send("The request was registered successfully.");
  });
});


app.get("/respondTokenRequest", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    let { requestID, accept }: any = req.query;

    if(!requestID) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        requestID: The ID of the request to be responded to.
        accept (optional): Whether to accept the request (true or false). Will be interpretted as 'false' if a value other than 'true' is provided.`
      );
    }

    accept = accept == "true" ? true : false

    let query = `
      SELECT approved, name, email, organization
      FROM token_requests
      WHERE requestID = $1;
    `;
    let queryHandler = await hcdpDBManagerHCDP.query(query, [requestID], {privileged: true});
    let requestData = await queryHandler.read(1);
    queryHandler.close();
    const timestamp = new Date().toISOString();

    let updateRequest = async () => {
      query = `
        UPDATE token_requests
        SET approved = $1, responded = $2
        WHERE requestID = $3;
      `;

      await hcdpDBManagerHCDP.queryNoRes(query, [accept, timestamp, requestID], {privileged: true});
    };

    if(requestData.length > 0) {
      const { approved, name, email, organization } = requestData[0];

      if(approved === null && accept) {
        updateRequest();
        
        const apiToken = crypto.randomBytes(16).toString("hex");
  
        let userLabel = name.toLowerCase().replace(/\s/g, "_");
        if(organization) {
          userLabel += "_" + organization.toLowerCase().replace(/\s/g, "_");
        }
        
        query = `
          INSERT INTO auth_token_store VALUES ($1, $2, $3, $4, $5);
        `;
        await hcdpDBManagerHCDP.queryNoRes(query, [apiToken, timestamp, "basic", userLabel, requestID], {privileged: true});
        const emailContent = `Dear ${name},
  
          Thank you for your interest in using the HCDP API! Here is your HCDP API token:
          ${apiToken}
  
          If you have any questions please reach out to our team at hcdp@hawaii.edu and we will be happy to assist you.
  
          Thank you,
          The HCDP Team`;
  
        let mailOptions = {
          to: email,
          subject: "HCDP API access request",
          text: emailContent,
          html: "<p>" + emailContent.replaceAll("\n", "<br>") + "</p>"
        };
        await sendEmail(transporterOptions, mailOptions);
  
        reqData.code = 201;
        return res.status(201)
        .send("Success! A token has been generated and sent to the email address provided by the requestor.");
      }
      else if(approved === null) {
        updateRequest();
  
        const emailContent = `Dear ${name},
  
          Thank you for your interest in using the HCDP API! Unfortunately we were not able to approve your request at this time.
          
          If you have any questions, please reach out to our team at hcdp@hawaii.edu and we will be happy to assist you.
  
          Thank you,
          The HCDP Team`;
  
        let mailOptions = {
          to: email,
          subject: "HCDP API access request",
          text: emailContent,
          html: "<p>" + emailContent.replaceAll("\n", "<br>") + "</p>"
        };
        await sendEmail(transporterOptions, mailOptions);
  
        reqData.code = 200;
        return res.status(200)
        .send("The requestor has been notified that their request was rejected.");
      }
      else {
        reqData.code = 409;
        return res.status(409)
        .send(`The provided token request ID has already been responded to. The request was ${approved ? "accepted" : "rejected"}. Requests may only be responded to once. If you would like to ammend the request decision, please contact the database administrator.`);
      }
    }
    else {
      reqData.code = 404;
      return res.status(404)
      .send("Invalid token request ID. No request with the provided request ID has been registered. No token will be generated");
    }
  });
});


app.put("/updateTokenPermissions", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {
    const { token, permissions } = req.body;

    if(!token || !permissions) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body should be JSON with the fields:
        token: The token to update the permissions for.
        permissions: The permissions to apply to the given token`
      );
    }

    let permString = permissions.join(",");
    let query = `
      UPDATE auth_token_store
      SET permissions = $1 
      WHERE token = $2;
    `;
    await hcdpDBManagerHCDP.queryNoRes(query, [permString, token], {privileged: true});
    reqData.code = 204;
    return res.status(204).end();
  });
});