import express from "express";
import { apiDB } from "../../../modules/util/resourceManagers/db.js";
import { validateArray, validateType } from "../../../modules/util/util.js";
import { sendEmail } from "../../../modules/util/email.js";
import * as crypto from "crypto";
import Cursor from "pg-cursor";
import { requireAuth } from "../../../modules/util/auth.js";

export const router = express.Router();

router.post("/registerTokenRequest", requireAuth("admin"), async (req, res) => {
  let { requestID, name, email, organization, position, reason } = req.body;
  
  if(!requestID || !name  || !email || !reason) {
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
  await apiDB.queryNoRes(query, [requestID, timestamp, null, null, name, email, organization, position, reason])

  return res.status(201)
  .send("The request was registered successfully.");
});


router.get("/respondTokenRequest", async (req, res) => {
  let { requestID, accept }: any = req.query;

  if(!requestID) {
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

  let requestData = await apiDB.query(query, [requestID], async (cursor: Cursor) => {
    return await cursor.read(1);
  });
  const timestamp = new Date().toISOString();

  let updateRequest = async () => {
    query = `
      UPDATE token_requests
      SET approved = $1, responded = $2
      WHERE requestID = $3;
    `;

    await apiDB.queryNoRes(query, [accept, timestamp, requestID]);
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
      await apiDB.queryNoRes(query, [apiToken, timestamp, ["basic"], userLabel, requestID]);
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

      return res.status(200)
      .send("The requestor has been notified that their request was rejected.");
    }
    else {
      return res.status(409)
      .send(`The provided token request ID has already been responded to. The request was ${approved ? "accepted" : "rejected"}. Requests may only be responded to once. If you would like to ammend the request decision, please contact the database administrator.`);
    }
  }
  else {
    return res.status(404)
    .send("Invalid token request ID. No request with the provided request ID has been registered. No token will be generated");
  }
});



router.patch("/updateTokenPermissions", requireAuth("admin"), async (req, res) => {
  const { token, permissions } = req.body;

  const validPerms = validateArray(permissions, (value: any) => validateType(value, ["string"]))

  if(!token || !validPerms) {
    return res.status(400)
    .send(
      `Request body should be JSON with the fields:
      token: The token to update the permissions for.
      permissions: The permissions to apply to the given token`
    );
  }

  let query = `
    UPDATE auth_token_store
    SET permissions = $1 
    WHERE token = $2;
  `;
  await apiDB.queryNoRes(query, [permissions, token]);
  return res.status(204).end();
});


router.patch("/addTokenPermissions", requireAuth("admin"), async (req, res) => {
  const { token, permissions } = req.body;

  const validPerms = validateArray(permissions, (value: any) => validateType(value, ["string"]));

  if(!token || !validPerms) {
    return res.status(400)
    .send(
      `Request body should be JSON with the fields:
      token: The token to update the permissions for.
      permissions: An array of strings representing the permissions to add.`
    );
  }

  let query = `
    UPDATE auth_token_store
    SET permissions = ARRAY(
      SELECT DISTINCT UNNEST(permissions || $1::text[])
    )
    WHERE token = $2;
  `;
  
  await apiDB.queryNoRes(query, [permissions, token]);
  
  return res.status(204).end();
});


router.patch("/removeTokenPermissions", requireAuth("admin"), async (req, res) => {
  const { token, permissions } = req.body;

  const validPerms = validateArray(permissions, (value: any) => validateType(value, ["string"]));

  if(!token || !validPerms) {
    return res.status(400)
    .send(
      `Request body should be JSON with the fields:
      token: The token to update the permissions for.
      permissions: An array of strings representing the permissions to remove.`
    );
  }

  let query = `
    UPDATE auth_token_store
    SET permissions = ARRAY(
      SELECT UNNEST(permissions)
      EXCEPT 
      SELECT UNNEST($1::text[])
    )
    WHERE token = $2;
  `;
  
  await apiDB.queryNoRes(query, [permissions, token]);
  
  return res.status(204).end();
});






router.patch("/deprecateToken", requireAuth("admin"), async (req, res) => {
  const { token, email, revoke } = req.body;

  const newToken = crypto.randomBytes(16).toString("hex");

  if(!token) {
    return res.status(400)
    .send(
      `Request body should be JSON with the fields:
      token: The token to be deprecated and replaced.`
    );
  }

  let query = `
    UPDATE auth_token_store
    SET token = $1 
    WHERE token = $2;
  `;
  let updated = await apiDB.queryNoRes(query, [newToken, token]);
  if(!updated) {
    return res.status(400)
    .send(`The token provided could not be found. No changes have been made. Please verify the token to be updated is correct.`);
  }
  
  let message = `Your HCDP API token ${token} has been deprecated. Your new token is ${newToken}. Please email hcdp@hawaii.edu if you have any questions.`;
  let mailOptions = {
    to: email,
    subject: "New HCDP token.",
    text: message,
    html: "<p>" + message + "</p>"
  };
  try {
    //try to send the error email, last try to actually notify user
    await sendEmail(mailOptions);
  }
  catch(e) {
    return res.status(500)
    .send(`Token has been replaced with ${newToken} but an error occurred while sending the update to the user. Please send the user their new token.`);
  }
  
  return res.status(200)
  .send(`Token has been replaced with ${newToken} and the user has been emailed the new token.`);
});




router.get("/getTokenData", requireAuth("admin"), async (req, res) => {
  const { token }: any = req.query;

  if(!token) {
    return res.status(400)
    .send(
      `Request must include the following parameter:
      token: The token to retrieve data for.`
    );
  }

  let query = `
    SELECT token, created, permissions, user_label, requestid
    FROM auth_token_store
    WHERE token = $1;
  `;

  let tokenData = await apiDB.query(query, [token], async (cursor: Cursor) => {
    return await cursor.read(1);
  });

  if(tokenData.length > 0) {
    return res.status(200).json(tokenData[0]);
  }
  else {
    return res.status(404).send("Token not found.");
  }
});