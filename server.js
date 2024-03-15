const express = require("express");
const compression = require("compression");
const nodemailer = require("nodemailer");
const https = require("https");
const fs = require("fs");
const config = require("./config.json");
const child_process = require("child_process");
const indexer = require("./fileIndexer");
const moment = require("moment");
const path = require("path");
const DBManager = require("./dbManager");
const sanitize = require("mongo-sanitize");
const csvReadableStream = require('csv-reader');
const detectDecodeStream = require('autodetect-decoder-stream');
const crypto = require('crypto');
const safeCompare = require('safe-compare');
//add timestamps to output
require("console-stamp")(console);

// const githubMiddleware = require('github-webhook-middleware')({
//   secret: config.githubWebhookSecret,
//   limit: "25mb", //webhook json payload size limit. Default is '100kb' (25mb is github max, should never get that big for metadata, but want to make sure larger commits are processed)
// });

const port = config.port;
const smtp = config.smtp;
const smtpPort = config.smtpPort;
const keyFile = config.key;
const certFile = config.cert;
const hskey = fs.readFileSync(keyFile);
const hscert = fs.readFileSync(certFile);
const mailOptionsBase = config.email;
const defaultZipName = config.defaultZipName;

const dataRoot = config.dataRoot;
const urlRoot = config.urlRoot;
const rawDataDir = config.rawDataDir;
const downloadDir = config.downloadDir;
const userLog = config.userLog;
const whitelist = config.whitelist;
const administrators = config.administrators;
const dbConfig = config.dbConfig;
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

process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;
process.env["NODE_ENV"] = "production";

const dbManager = new DBManager.DBManager(dbConfig.server, dbConfig.port, dbConfig.username, dbConfig.password, dbConfig.db, dbConfig.collection, dbConfig.connectionRetryLimit, dbConfig.queryRetryLimit);
const tapisManager = new DBManager.TapisManager(tapisConfig.tenantURL, tapisConfig.token, dbConfig.queryRetryLimit, dbManager);
const tapisV3Manager = new DBManager.TapisV3Manager(tapisV3Config.username, tapisV3Config.password, tapisV3Config.tenantURL, tapisV3Config.projectID);

////////////////////////////////
//////////server setup//////////
////////////////////////////////

const app = express();

let options = {
    key: hskey,
    cert: hscert
};

const server = https.createServer(options, app)
.listen(port, (err) => {
  if(err) {
    console.error(error);
  }
  else {
    console.log("Server listening at port " + port);
  }
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


async function handleSubprocess(subprocess, dataHandler, errHandler) {
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


async function readdir(dir) {
  return new Promise((resolve, reject) => {
    fs.readdir(dir, (err, files) => {
      resolve({err, files});
    });
  });
}

function validateTokenAccess(token, permission) {
  let valid = false;
  let allowed = false;
  let user = "";
  let tokenInfo = whitelist[token];
  if(tokenInfo) {
    valid = true;
    user = tokenInfo.user || "";
    //actions permissions user is authorzized for
    const authorized = tokenInfo.permissions;
    //check if authorized permissions for this token contains required permission for this request
    allowed = authorized.includes(permission);
  }
  return {
    valid,
    allowed,
    token,
    user
  }
}

function validateToken(req, permission) {
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
      //get tokens access rules
      tokenData = validateTokenAccess(match[1], permission);
    }
  }
  return tokenData;
}

async function sendEmail(transporterOptions, mailOptions) {
  combinedMailOptions = Object.assign({}, mailOptionsBase, mailOptions);

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
    const tokenData = validateToken(req, permission);
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
      let { numFiles, paths } = await indexer.getPaths(productionRoot, dataset, false);
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
          proc = child_process.spawn("./tiffextract.out", [...posParams, ...paths]);
        }
        //otherwise write paths to a file and use that
        else {
          let uuid = crypto.randomUUID();
          //write paths to a file and use that, avoid potential issues from long cmd line params
          fs.writeFileSync(uuid, paths.join("\n"));
    
          proc = child_process.spawn("./tiffextract.out", ["-f", uuid, ...posParams]);
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
            let match = path.match(indexer.fnamePattern);
            //should never be null otherwise wouldn't have matched file to begin with, just skip if it magically happens
            if(match !== null) {
                //capture date from fname and split on underscores
                dateParts = match[1].split("_");
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
      let replaced = await dbManager.replaceRecord(uuid, value);
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
        uuid: A string representing the uuid of the document to have it's value replaced`
      );
    }
    else {
      let deleted = await dbManager.deleteRecord(uuid);
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
    let {date, returnEmptyNotFound, ...properties} = req.query;
    fileType = "data_map";
    if(properties.type == "percent") {
      fileType = "data_map_change";
      properties.units = "percent";
    }
    else if(properties.type == "absolute") {
      fileType = "data_map_change";
    }

    let data = [{
      files: [fileType],
      range: {
        start: date,
        end: date
      },
      ...properties
    }];
    let files = await indexer.getPaths(productionRoot, data, false);
    reqData.sizeF = files.numFiles;
    let file = null;
    //should only be exactly one file
    if(files.numFiles == 0 && returnEmptyNotFound) {
      file = indexer.getEmpty(properties.extent);
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

    let { packageID, file } = req.query;
    if(!(packageID && file)) {
      return e400();
    }
    //check id and file name format
    idRegex = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/g;
    fileRegex = /^[\w\-. ]+$/g;
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
        let { paths, numFiles } = await indexer.getPaths(productionRoot, data);
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
        zipProc = child_process.spawn("sh", ["./zipgen.sh", downloadRoot, productionRoot, zipName, ...paths]);

        let code = await handleSubprocess(zipProc, (data) => {
          zipPath += data.toString();
        });

        if(code !== 0) {
          serverError = `Failed to generate download package for user ${email}. Zip process failed with code ${code}.`
          clientError = "There was an error generating your HCDP download package.";
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
            attachments = [{
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
      catch(e) {
        serverError = `Failed to generate download package for user ${email}. Spawn process failed with error ${e.toString()}.`
        clientError = "There was an error generating your HCDP download package.";
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
      let { paths, numFiles } = await indexer.getPaths(productionRoot, data);
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
      let { paths, numFiles } = await indexer.getPaths(productionRoot, data);
      //add license file
      paths.push(licenseFile);
      numFiles += 1;

      //make relative so zip doesn't include production path
      paths = paths.map((file) => {
        return path.relative(productionRoot, file);
      });

      reqData.sizeF = numFiles;
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["./zipgen.sh", downloadRoot, productionRoot, zipName, ...paths]);
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
      let { paths, numFiles } = await indexer.getPaths(productionRoot, data);
      //add license file
      paths.push(licenseFile);
      numFiles += 1;

      //make relative so zip doesn't include production path
      paths = paths.map((file) => {
        return path.relative(productionRoot, file);
      });

      reqData.sizeF = numFiles;
      res.contentType("application/zip");
      let zipProc = child_process.spawn("sh", ["./zipgen_parts.sh", downloadRoot, productionRoot, ...paths]);
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
        let fileParts = [];
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
    let data = req.query.data;
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
      let files = await indexer.getPaths(productionRoot, data, false);
      reqData.sizeF = files.numFiles;
      files = files.paths.map((file) => {
        file = path.relative(dataRoot, file);
        let fileLink = `${urlRoot}${file}`;
        return fileLink;
      });
      reqData.code = 200;
      res.status(200)
      .json(files);
    }
  });
});


app.get("/raw/download", async (req, res) => {
  await handleReqNoAuth(req, res, async (reqData) => {
    //destructure query
    let { p } = req.query;

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
    let date = req.query.date;

    if(!date) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        "Request must include the following parameters: \n\
        date: An ISO 8601 formatted date string representing the date you would like the data for."
      );
    }
    else {
      let parsedDate = moment(date);
      let year = parsedDate.format("YYYY");
      let month = parsedDate.format("MM");
      let day = parsedDate.format("DD");
  
      let dataDir = path.join("hawaii", year, month, day);
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
  
      files = files.map((file) => {
        let fileLink = `${linkDir}${file}`;
        return fileLink;
      });
      reqData.sizeF = files.length;
      reqData.code = 200;
      res.status(200)
      .json(files);
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
    resData = [];
    let procHandles = [child_process.spawn("/bin/bash", [logscript, logfile]), child_process.spawn("/bin/bash", [logscriptOld, logfileOld])].map((proc) => {
      return new Promise(async (resolve, reject) => {
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
    let header = null;
    //might want to move file location/header translations to config
    https.get("https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/Hawaii_Master_Station_Meta.csv", (res) => {
      let docs = [];
      res.pipe(new detectDecodeStream({ defaultEncoding: "1255" }))
      //note old data does not parse numbers, maybe reprocess data with parsed numbers at some point, for now leave everything as strings though
      .pipe(new csvReadableStream({ parseNumbers: false, parseBooleans: false, trim: true }))
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
          for(property of row) {
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


app.get("/mesonet/getStations", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    try {
      const data = await tapisV3Manager.listSites();
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      console.error(`An unexpected error occurred while listing the sites. Error: ${e}`);
      reqData.code = 500;
      return res.status(500)
      .send("An error occured while processing the request.");
    }
  });
});

app.get("/mesonet/getVariables", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_id, file_type } = req.query;
    if(station_id === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        station_id: The ID of the station to query.`
      );
    }
    if(file_type === undefined) {
      file_type = "MetData"
    }
    const siteID = `${station_id}_012`;
    const instrumentID = `${station_id}_012_${file_type}`;
    console.log(siteID, instrumentID);
    try {
      const data = await tapisV3Manager.listVariables(siteID, instrumentID);
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      console.error(`An unexpected error occurred while listing the sites. Error: ${e}`);
      reqData.code = 500;
      return res.status(500)
      .send("An error occured while processing the request.");
    }
  });
});


app.get("/mesonet/getMeasurements", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    let { station_id, file_type, ...options } = req.query;
    if(station_id === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters:
        station_id: The ID of the station to query.`
      );
    }
    if(file_type === undefined) {
      file_type = "MetData"
    }
    const siteID = `${station_id}_012`;
    const instrumentID = `${station_id}_012_${file_type}`;  
    //start_date, end_date, limit, offset, var_ids (comma separated)
    console.log(options);
    try {
      const data = await tapisV3Manager.listMeasurements(siteID, instrumentID, options);
      reqData.code = 200;
      return res.status(200)
      .json(data);
    }
    catch(e) {
      console.error(`An unexpected error occurred while listing the measurments. Error: ${e}`);
      reqData.code = 500;
      return res.status(500)
      .send("An error occured while processing the request.");
    }
  });
});