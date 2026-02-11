import express from "express";
import { mesonetDBUser } from "../../../modules/util/resourceManagers/db.js";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { v4 as uuidv4, validate as isValidUUID } from "uuid";
import { checkEmail, MailOptions, sendEmail, validateArray, validateType } from "../../../modules/util/util.js";
import Cursor from "pg-cursor";

export const router = express.Router();

const REPORT_TYPES = ["ahupuaa", "county", "watershed", "moku"];

async function getUserID(email: string): Promise<string> {
  let query = `
    SELECT id
    FROM climate_report_register
    WHERE email = $1 AND ACTIVE = TRUE;
  `;

  let data = await mesonetDBUser.query(query, [email], async (cursor: Cursor) => {
    return await cursor.read(1);
  }, { rowMode: "array" });

  let id = null;
  if(data.length > 0) {
    id = data[0][0]
  }
  return id;
}



router.post("/mesonet/climate_report/subscribe", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { email } = req.body;
    if(typeof email !== "string" || !checkEmail(email)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must be a JSON object including the following parameters: \n\
        email: A valid email string to receive climate reports at. \n\
        ahupuaʻa (optional): An array of the names of ahupuaa to include in the climate report \n\
        county (optional): An array of the names of counties to include in the climate report \n\
        watershed (optional): An array of the names of watershed to include in the climate report \n\
        moku (optional): An array of the names of moku to include in the climate report`
      );
    }
    let id = uuidv4()
    const timestamp = new Date().toISOString();

    for(let type of REPORT_TYPES) {
      let value = req.body[type];
      if(value === undefined) {
        req.body[type] = [];
      }
      else if(!validateArray(value, (value) => typeof value === "string")) {
        reqData.success = false;
        reqData.code = 400;
  
        return res.status(400)
        .send(
          `Invalid value for ${type}, must be an array of strings.`
        );
      }
    }
    
    const { ahupuaa, county, watershed, moku } = req.body;
    
    let query = `
      INSERT INTO climate_report_register
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
      ON CONFLICT (email) DO UPDATE
      SET ahupuaa = $3, county = $4, watershed = $5, moku = $6, modified = $8, active = TRUE
      WHERE climate_report_register.active = FALSE;
    `;

    let modified = await mesonetDBUser.queryNoRes(query, [id, email, ahupuaa, county, watershed, moku, timestamp, timestamp]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("A user with this email address is already subscribed.");
    }

    id = await getUserID(email);
    reqData.code = 200;
    return res.status(200)
    .json({userID: id});
  });
});

router.get("/mesonet/climate_report/email_lookup", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { email } = req.query;
    if(typeof email !== "string") {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters: \n\
        email: A string representing the email to lookup.`
      );
    }

    const id = await getUserID(email);
    reqData.code = 200;
    return res.status(200)
    .json({userID: id});
  });
});

router.get("/mesonet/climate_report/subscription/:id", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let query = `
      SELECT email, ahupuaa, county, watershed, moku, created, modified, active
      FROM climate_report_register
      WHERE id = $1 AND active = TRUE;
    `;

    let data = await mesonetDBUser.query(query, [id], async (cursor: Cursor) => {
      return await cursor.read(1);
    });

    if(data.length < 1) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found or is inactive.");
    }
    reqData.code = 200;
    return res.status(200)
    .json(data[0]);
  });
});


router.patch("/mesonet/climate_report/subscription/:id", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let updateParams = [];
    let setParts = [];
    let i = 1;
    for(let type of REPORT_TYPES) {
      if(req.body[type] !== undefined) {
        if(!validateArray(req.body[type], (value) => typeof value === "string")) {
          reqData.success = false;
          reqData.code = 400;
    
          return res.status(400)
          .send(
            `Invalid value for ${type}, must be an array of strings.`
          );
        }
        updateParams.push(req.body[type]);
        setParts.push(`${type} = $${i++}`);
      }
    }
    if(setParts.length < 1) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must include at least one of the following fields to update: \n\
        ahupuaʻa: An array of the names of ahupuaa to include in the climate report \n\
        county: An array of the names of counties to include in the climate report \n\
        watershed: An array of the names of watershed to include in the climate report \n\
        moku: An array of the names of moku to include in the climate report`
      );
    }
    // update time record was modified
    const timestamp = new Date().toISOString();
    setParts.push(`modified = $${i++}`);

    // create list string for set clause
    let setString = setParts.join(", ");
    
    let query = `
      UPDATE climate_report_register
      SET ${setString}
      WHERE id = $${i} AND active = TRUE;
    `;

    let modified = await mesonetDBUser.queryNoRes(query, [...updateParams, timestamp, id]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found or is inactive. No changes have been made.");
    }
    reqData.code = 204;
    return res.status(204).end();
  });
});


router.patch("/mesonet/climate_report/subscription/:id/unsubscribe", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let query = `
      UPDATE climate_report_register
      SET active = FALSE
      WHERE id = $1;
    `;
    let modified = await mesonetDBUser.queryNoRes(query, [id]);
    if(!modified) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found. No changes have been made");
    }
    reqData.code = 204;
    return res.status(204).end();
  });
});


router.get("/mesonet/climate_report/subscriptions", async (req, res) => {
  const permission = "userdata";
  await handleReq(req, res, permission, async (reqData) => {
    let query = `
      SELECT id, email, ahupuaa, county, watershed, moku
      FROM climate_report_register
      WHERE active = TRUE;
    `;


    let data = await mesonetDBUser.query(query, [], async (cursor: Cursor) => {
      let rows = [];
      const chunkSize = 10000;
      let chunk: any[];
      do {
        chunk = await cursor.read(chunkSize);
        for(let row of chunk) {
          rows.push(row);
        }
      }
      while(chunk.length > 0)
      return rows;
    });
    
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.post("/mesonet/climate_report/subscription/:id/email", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;
    let { text, html } = req.body;

    if(!validateType(text, ["string"])) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must include the content you would like to send to the user: \n\
        text: A string containing the email content you would like to send. \n\
        html (optional): An HTML string containing the email content you would like to send. This will default to the text data provided wrapped in a paragraph block if not provided.`
      ); 
    }
    if(!validateType(html, ["string"])) {
      html = `<p>${text.replace("\n", "<br/>")}</p>`;
    }

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let query = `
      SELECT email
      FROM climate_report_register
      WHERE id = $1 AND active = TRUE;
    `;

    let data = await mesonetDBUser.query(query, [id], async (cursor: Cursor) => {
      return await cursor.read(1);
    }, { rowMode: "array" });

    if(data.length < 1) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found. Email has not been sent.");
    }

    const subject = "Your Monthly Hawaiʻi Climate Report";

    const socSite = "https://www.hawaii.edu/climate-data-portal/state-of-the-climate";
    const unsubscribeLink = `${socSite}/#/unsubscribe?id=${id}`;

    //text
    const introText = "Here is your monthly climate report:";
    const signoffText = "Thank you for subscribing! Sincerely,\nThe HCDP Team"
    const unsubscribeMessageText = `Subscription preferences can be updated at ${socSite} No longer interested in receiving these emails? Visit ${unsubscribeLink} to unsubscribe.`;
    let textParts = [introText, text, signoffText, unsubscribeMessageText];
    text = textParts.join("\n");

    //html
    const introHTML = "<h3>Here is your monthly climate report:</h3>";
    const signoffHTML = "<p>Thank you for subscribing! Sincerely,<br/>The HCDP Team</p>"
    const unsubscribeMessageHTML = `<p>Subscription preferences can be updated <a href="${socSite}">here</a>. No longer interested in receiving these emails? <a href="${unsubscribeLink}">Unsubscribe</a></p>`;
    let htmlParts = [html, signoffHTML, unsubscribeMessageHTML];
    html = `${introHTML}${htmlParts.join("<br/>")}`;
    
    
    let email = data[0][0];
    let mailOptions: MailOptions = {
      to: email,
      subject,
      text,
      html
    }

    let mailRes = await sendEmail(mailOptions);
    if(!mailRes.success) {
      reqData.success = false;
      throw new Error("Failed to email user " + email + ". Error: " + mailRes.error.toString());
    }
    
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});