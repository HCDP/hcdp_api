import { logReq, sendEmail } from "./util.js";
import { administrators } from "./config.js";
import { validateToken } from "./auth.js";

export async function handleReqNoAuth(req, res, handler) {
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
        let emailStatus = await sendEmail(mailOptions);
        //if email send failed throw error for logging
        if(!emailStatus.success) {
          throw emailStatus.error;
        }
      }
      //if error while sending admin email write to stderr
      catch(e) {
        console.error(`Failed to send administrator notification email: ${e}`);
      }
    }
  }
  logReq(reqData);
}

export async function handleReq(req, res, permission, handler) {
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
        let emailStatus = await sendEmail(mailOptions);
        //if email send failed throw error for logging
        if(!emailStatus.success) {
          throw emailStatus.error;
        }
      }
      //if error while sending admin email write to stderr
      catch(e) {
        console.error(`Failed to send administrator notification email: ${e}`);
      }
    }
  }
  logReq(reqData);
}