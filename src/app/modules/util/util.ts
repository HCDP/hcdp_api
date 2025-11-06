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

export function parseBoolParam(param: any, defaultValue: boolean = false): boolean {
  let nonDefaultParamString = (!defaultValue).toString();
  let value = defaultValue;
  if(typeof param === "boolean") {
    value = param;
  }
  else if(typeof param === "string" && param.toLowerCase() === nonDefaultParamString) {
    value = !defaultValue;
  }
  return value;
}

export function checkEmail(email: string) {
  const emailRegex = /(?:[a-z0-9!#$%&'*+\x2f=?^_`\x7b-\x7d~\x2d]+(?:\.[a-z0-9!#$%&'*+\x2f=?^_`\x7b-\x7d~\x2d]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9\x2d]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9\x2d]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9\x2d]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])/;
  let valid = emailRegex.test(email);
  return valid;
}

export interface MailRes {
  success: boolean,
  result: any,
  error: Error
}
