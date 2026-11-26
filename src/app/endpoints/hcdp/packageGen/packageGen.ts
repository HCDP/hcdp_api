import express from "express";
import * as fs from "fs";
import { handleSubprocess, sanitizeFilename } from "../../../modules/util/util.js";
import { sendEmail, MailRes, checkEmail } from "../../../modules/util/email.js";
import { defaultZipName, downloadRoot, apiURL, licenseFile } from "../../../modules/util/config.js";
import { getPaths } from "../../../modules/fileIndexer.js";
import * as child_process from "child_process";
import * as path from "path";
import { requireAuth } from "../../../modules/util/auth.js";

export const router = express.Router();

router.get("/download/package", async (req, res) => {
  let e400 = () => {
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
      res.status(404)
      .send("The requested file could not be found");
    }
    else {
      //should the size of the file in bytes be added?
      res.locals.setSizeF(1);
      res.set("Content-Disposition", `attachment; filename="${file}"`);
      res.status(200)
      .sendFile(downloadPath);
    }
  });
});


router.post("/genzip/email", requireAuth("basic"), async (req, res) => {
  let email = req.body.email;
  let data = req.body.data;
  let zipName = sanitizeFilename(req.body.zipName ?? req.body.name, "zip", defaultZipName);

  

  //make sure required parameters exist and data is an array
  if(!Array.isArray(data) || !checkEmail(email)) {
    //send error
    return res.status(400)
    .send(
      `Request body should include the following fields: \n\
      data: An array of file data objects describing a set of files to zip \n\
      email: The email to send the package to \n\
      zipName (optional): What to name the zip file. Default: ${defaultZipName}`
    );
  }

  res.locals.setEmail(email);

  res.locals.addBackgroundTask(
    async () => {
      let clientError = "There was an error generating your HCDP download package.";
      //wrap in try catch so send error email if anything unexpected goes wrong
      try {
        //note no good way to validate email address, should have something in app saying that if email does not come to verify spelling
        //email should arrive in the next few minutes, if email does not arrive within 2 hours we may have been unable to send the email, check for typos, try again, or contact the site administrators

        /////////////////////////////////////
        // generate package and send email //
        /////////////////////////////////////
        
        //get paths
        let { root, paths, numFiles } = await getPaths(data);
        //add license file
        let licensePath = path.join(root, licenseFile);
        paths.push(licensePath);
        numFiles += 1;

        //make relative so zip doesn't include production path
        paths = paths.map((file) => {
          return path.relative(root, file);
        });

        res.locals.setSizeF(numFiles);
        let zipPath = "";
        let zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen.sh", downloadRoot, root, zipName, ...paths]);

        let code = await handleSubprocess(zipProc, (data) => {
          zipPath += data.toString();
        });
        zipPath = zipPath.trim();

        if(code !== 0) {
          clientError = "There was an error generating your HCDP download package.";
          throw new Error(`Failed to generate download package for user ${email}. Zip process failed with code ${code}.`);
        }
        else {
          let zipDec = zipPath.split("/");
          let [ packageID, fname ] = zipDec.slice(-2);

          //get package size
          let fstat = await fs.promises.stat(zipPath);
          let fsizeB = fstat.size;
          //set size of package for logging
          res.locals.setSizeB(fsizeB);

          let ep = `${apiURL}/download/package`;
          let params = `packageID=${packageID}&file=${fname}`;
          //create download link and send in message body
          let downloadLink = `${ep}?${params}`;
          let mailOptions = {
            to: email,
            text: "Your HCDP download package is ready. Please go to " + downloadLink + " to download it. This link will expire in three days, please download your data in that time.",
            html: "<p>Your HCDP download package is ready. Please click <a href=\"" + downloadLink + "\">here</a> to download it. This link will expire in three days, please download your data in that time.</p>"
          };
          let mailRes: MailRes = await sendEmail(mailOptions);

          //if unsuccessful attempt to send error email
          if(!mailRes.success) {
            clientError = "There was an error sending your HCDP download package to this email address.";
            throw new Error(`Failed to send message to user ${email}. Error: ${mailRes.error.toString()}`)
          }
        }
      }
      catch(e: any) {
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
        catch(mailErr) {}
        //throw server error to be handled by main error handler
        throw e;
      }
      return true;
    }
  );


  res.status(202)
  .send("Request received. Generating download package");
});



router.post("/genzip/instant/content", requireAuth("basic"), async (req, res) => {
  let email = req.body.email;
  let data = req.body.data;

  if(email) {
    res.locals.setEmail(email);
  }

  if(!Array.isArray(data) || !email) {
    res.status(400)
    .send(
      "Request body should include the following fields: \n\
      data: An array of file data objects describing a set of files to zip. \n\
      email: The requestor's email address for logging"
    );
  }
  else {
    let { paths, numFiles } = await getPaths(data);
    res.locals.setSizeF(numFiles);
    if(paths.length > 0) {
      res.contentType("application/zip");

      let zipProc = child_process.spawn("zip", ["-qq", "-r", "-", ...paths]);

      let totalBytes = 0;
      let code = await handleSubprocess(zipProc, (data) => {
        //get data chunk size
        let dataSizeB = data.length;
        //add size of data chunk
        totalBytes += dataSizeB;
        //write data to stream
        res.write(data);
      });
      res.locals.setSizeB(totalBytes);
      //if zip process failed throw error for handling by error middleware
      if(code !== 0) {
        throw new Error("Zip process failed with code " + code);
      }
      else {
        res.status(200)
        .end();
      }
    }
    //just send empty if no files
    else {
      res.status(200)
      .end();
    }
  }
});



router.post("/genzip/instant/link", requireAuth("basic"), async (req, res) => {
    let zipName = defaultZipName;
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      res.locals.setEmail(email);
    }

    //if not array then leave files as 0 length to be picked up by error handler
    if(!Array.isArray(data) || !email) {
      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      let { root, paths, numFiles } = await getPaths(data);
      //add license file
      let licensePath = path.join(root, licenseFile);
      paths.push(licensePath);
      numFiles += 1;

      //make relative so zip doesn't include production path
      paths = paths.map((file) => {
        return path.relative(root, file);
      });

      res.locals.setSizeF(numFiles);
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen.sh", downloadRoot, root, zipName, ...paths]);
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
        res.locals.setSizeB(fsizeB);

        res.status(200)
        .send(downloadLink);
      }
    }
  });


router.post("/genzip/instant/splitlink", requireAuth("basic"), async (req, res) => {
  let email = req.body.email;
  let data = req.body.data;

  if(email) {
    res.locals.setEmail(email);
  }

  if(!Array.isArray(data) || !email) {
    res.status(400)
    .send(
      `Request body should include the following fields: \n\
      data: An array of file data objects describing a set of files to zip. \n\
      email: The requestor's email address for logging`
    );
  }
  else {
    let { root, paths, numFiles } = await getPaths(data);
    //add license file
    let licensePath = path.join(root, licenseFile);
    paths.push(licensePath);
    numFiles += 1;

    //make relative so zip doesn't include production path
    paths = paths.map((file) => {
      return path.relative(root, file);
    });

    res.locals.setSizeF(numFiles);
    res.contentType("application/zip");
    let zipProc = child_process.spawn("sh", ["../assets/scripts/zipgen_parts.sh", downloadRoot, root, ...paths]);
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
      let totalBytes = 0;
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
        totalBytes += fsizeB;
      }
      res.locals.setSizeB(totalBytes);

      let data = {
        files: fileParts
      }
      res.status(200)
      .json(data);
    }
  }
});