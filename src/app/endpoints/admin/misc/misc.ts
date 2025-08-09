import express from "express";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { handleSubprocess, sendEmail } from "../../../modules/util/util.js";
import { tapisManager } from "../../../modules/util/resourceManagers/tapis.js";
import { githubWebhookSecret } from "../../../modules/util/config.js";
import CsvReadableStream from "csv-reader";
import detectDecodeStream from "autodetect-decoder-stream";
import safeCompare from "safe-compare";
import * as crypto from "crypto";
import * as child_process from "child_process";
import * as https from "https";
import { apiDB } from "../../../modules/util/resourceManagers/db.js";
import { parseListParam, parseParams } from "../../../modules/util/dbUtil.js";

export const router = express.Router();

function signBlob(key, blob) {
  return "sha1=" + crypto.createHmac("sha1", key).update(blob).digest("hex");
}

router.get("/users/emails/apitokens", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {

    let { status }: any = req.query;

    //true, false, null approved
    let params = [];
    let whereClause = [];
    let query = `
      SELECT email
      FROM token_requests
    `;

    let sqlValueMap = {
      approved: true,
      rejected: false,
      pending: null
    }
    let statusList = parseListParam(status, new Set(["approved", "rejected", "pending"]));
    if(statusList === null) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("The status parameter must be a comma separated list of token request statuses or an array of values.");
    }
    statusList = statusList.map((value) => sqlValueMap[value]);
    parseParams(statusList, params, whereClause, "approved");
    if(statusList.length > 0) {
      query += `WHERE ${whereClause[0]}`;
    }
    query += ";";
    let handler = await apiDB.query(query, params, { rowMode: "array" });
    let data = await handler.read(100000);
    handler.close();
    let emails = data.flat();
    reqData.code = 200;
    return res.status(200)
    .json(emails);
  });
});

router.get("/users/emails/apiqueries", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {
    let proc = child_process.spawn("python3", ["/logs/utils/get_emails.py"]);

    let output = "";
    let code = await handleSubprocess(proc, (data: string) => {
      output += data.toString();
    }, (error: string) => {
      throw new Error(`Could not process log files, something went wrong: ${error}`);
    });
    if(code !== 0) {
      throw new Error(`Could not process log files, something went wrong, process exited with code ${code}`);
    }
    let emails = output.trim().split("\n");
    reqData.code = 200;
    return res.status(200)
    .json(emails);
  });
});


router.get("/apistats", async (req, res) => {
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



//add middleware to get raw body, don't actually need body data so no need to do anything fancy to get parsed body as well
router.post("/addmetadata", express.raw({ limit: "50mb", type: () => true }), async (req, res) => {
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


router.post("/notify", async (req, res) => {
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
      let emailStatus = await sendEmail(mailOptions);
      //if email send failed throw error for logging
      if(!emailStatus.success) {
        throw emailStatus.error;
      }
    }
    //if error while sending admin email write to stderr
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

router.get("/error", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {
    throw new Error("This is a test error.");
  });
});


router.post("/restart", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {
    reqData.code = 202;
    res.status(202)
    .send("Request received. API will attempt to restart");
    child_process.spawn("bash", ["docker stop api; /home/hcdp/hcdp-api/api/util/deploy.sh"]);
  });
});