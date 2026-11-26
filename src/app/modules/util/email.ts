import * as nodemailer from "nodemailer";
import * as fs from "fs";
import { emailConfig, smtp, smtpPort } from "./config.js";
import { administrators } from "./config.js";
import { apiDB } from "./resourceManagers/db.js";


const transporterOptions = {
  host: smtp,
  port: smtpPort,
  secure: false,
  ignoreTLS: true,
  pool: true,
  maxConnections: 1,
  maxMessages: 100
};
const transporter = nodemailer.createTransport(transporterOptions);
transporter.verify((error: Error | null) => {
  if(error) {
    console.error("SMTP Startup Error: Failed to connect to UH SMTP server", error.message);
  }
  else {
    console.log("SMTP Pool initialized and ready to send emails.");
  }
});


export async function sendEmail(mailOptions: MailOptions, retries = 3): Promise<MailRes> {
  let combinedMailOptions = Object.assign({}, emailConfig, mailOptions);
  try {
    let info = await transporter.sendMail(combinedMailOptions);
    const success = info.response.startsWith("250");
    return {
      success: success,
      result: info, 
      error: null
    };
  }
  catch(error: any) {
    if(retries > 0 && (error.code === "ECONNRESET" || error.code === "ETIMEDOUT" || error.code === "EPIPE")) {
      console.warn(`SMTP connection error (${error.code}). Retrying...`);
      return sendEmail(mailOptions, retries - 1);
    }
    return {
      success: false,
      result: null,
      error: error
    };
  }
}

export function checkEmail(email: string) {
  const emailRegex = /(?:[a-z0-9!#$%&'*+\x2f=?^_`\x7b-\x7d~\x2d]+(?:\.[a-z0-9!#$%&'*+\x2f=?^_`\x7b-\x7d~\x2d]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9\x2d]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9\x2d]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9\x2d]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])/;
  let valid = emailRegex.test(email);
  return valid;
}



async function sendErrorMessage(errorMsg: string) {
  let htmlErrorMsg = errorMsg.replace(/\n/g, "<br>");
  
  //send the administrators an email logging the error
  if(administrators.length > 0) {
    let mailOptions = {
      to: administrators,
      subject: "HCDP API error",
      text: `An unexpected error occured in the HCDP API:\n${errorMsg}`,
      html: `<p>An error occured in the HCDP API:<br>${htmlErrorMsg}</p>`
    };
    //attempt to send email to the administrators
    let emailStatus = await sendEmail(mailOptions);
    //if email send failed throw error for logging
    if(!emailStatus.success) {
      throw emailStatus.error;
    }
  }
}

export async function emailAdminError(errorMsg: string) {
  let sent = false;
  try {
    let query = `
      UPDATE error_throttle
      SET count = count - 1
      WHERE count > 0;
    `;

    let updated = (await apiDB.queryNoRes(query, [])) > 0;
    if(updated) {
      sendErrorMessage(errorMsg);
      sent = true;
    }
  }
  catch(e) {
    console.error(`Failed to send administrator notification email: ${e}`);
  }
  return sent;
}


export interface MailRes {
  success: boolean,
  result: any,
  error: Error
}

export interface MailOptions {
  to: string | string[],
  text: string,
  html: string,
  from?: string,
  subject?: string,
  attachments?: {
    filename: string,
    content: fs.ReadStream
  }[]
}
