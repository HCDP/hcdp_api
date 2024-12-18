import express from "express";
import { handleReq, handleReqNoAuth } from "../../../modules/util/reqHandlers";
import { HCDPDBManager } from "../../../modules/util/resourceManagers/db";
import { sendEmail } from "../../../modules/util/util";
import * as crypto from "crypto";

export const router = express.Router();

router.post("/registerTokenRequest", async (req, res) => {
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
    await HCDPDBManager.queryNoRes(query, [requestID, timestamp, null, null, name, email, organization, position, reason], {privileged: true})

    reqData.code = 201;
    return res.status(201)
    .send("The request was registered successfully.");
  });
});


router.get("/respondTokenRequest", async (req, res) => {
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
    let queryHandler = await HCDPDBManager.query(query, [requestID], {privileged: true});
    let requestData = await queryHandler.read(1);
    queryHandler.close();
    const timestamp = new Date().toISOString();

    let updateRequest = async () => {
      query = `
        UPDATE token_requests
        SET approved = $1, responded = $2
        WHERE requestID = $3;
      `;

      await HCDPDBManager.queryNoRes(query, [accept, timestamp, requestID], {privileged: true});
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
        await HCDPDBManager.queryNoRes(query, [apiToken, timestamp, "basic", userLabel, requestID], {privileged: true});
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
        await sendEmail(mailOptions);
  
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
        await sendEmail(mailOptions);
  
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


router.put("/updateTokenPermissions", async (req, res) => {
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
    await HCDPDBManager.queryNoRes(query, [permString, token], {privileged: true});
    reqData.code = 204;
    return res.status(204).end();
  });
});