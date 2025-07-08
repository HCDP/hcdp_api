import * as fs from "fs";
import * as nodemailer from "nodemailer";
import { userLog, emailConfig, smtp, smtpPort } from "./config.js";

const transporterOptions = {
  host: smtp,
  port: smtpPort,
  secure: false,
  ignoreTLS: true
};

export async function readdir(dir): Promise<{err, files}> {
  return new Promise((resolve, reject) => {
    fs.readdir(dir, (err, files) => {
      resolve({err, files});
    });
  });
}

export async function handleSubprocess(subprocess, dataHandler, errHandler?) {
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

export async function sendEmail(mailOptions): Promise<MailRes> {
  let combinedMailOptions = Object.assign({}, emailConfig, mailOptions);
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

export function logReq(data) {
  const { user, code, success, sizeF, method, endpoint, token, sizeB, tokenUser } = data;
  const timestamp = new Date().toLocaleString("sv-SE", {timeZone:"Pacific/Honolulu"});
  let dataString = `[${timestamp}] ${method}:${endpoint}:${user}:${tokenUser}:${token}:${code}:${success}:${sizeB}:${sizeF}\n`;
  fs.appendFile(userLog, dataString, (err) => {
    if(err) {
      console.error(`Failed to write userlog.\nError: ${err}`);
    }
  });
}

export function processTapisError(res, reqData, e) {
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

export interface MailRes {
  success: boolean,
  result: any,
  error: Error
}