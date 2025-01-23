import express from "express";
import * as fs from "fs";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers.js";
import { sendEmail, handleSubprocess } from "../../../modules/util/util.js";
import { ATTACHMENT_MAX_MB, defaultZipName, downloadRoot, productionRoot, apiURL, licenseFile } from "../../../modules/util/config.js";
import { getPaths } from "../../../modules/fileIndexer.js";
import * as child_process from "child_process";
import * as path from "path";

export const router = express.Router();

router.get("/download/package", async (req, res) => {
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


router.post("/genzip/email", async (req, res) => {
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
          await sendEmail(mailOptions);
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
        let { paths, numFiles } = await getPaths(data);
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
            
            mailRes = await sendEmail(mailOptions);
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
            mailRes = await sendEmail(mailOptions);
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


router.post("/genzip/instant/content", async (req, res) => {
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
      let { paths, numFiles } = await getPaths(data);
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


router.post("/genzip/instant/link", async (req, res) => {
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
      let { paths, numFiles } = await getPaths(data);
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


router.post("/genzip/instant/splitlink", async (req, res) => {
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
      let { paths, numFiles } = await getPaths(data);
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