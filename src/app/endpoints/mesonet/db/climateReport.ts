import express from "express";
import { mesonetDBUser } from "../../../modules/util/resourceManagers/db.js";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { v4 as uuidv4 } from "uuid";

export const router = express.Router();

async function getUserID(email: string): Promise<string> {
  let query = `
    SELECT id
    FROM climate_report_register
    WHERE email = $1 AND ACTIVE = TRUE;
  `;
  let queryHandler = await mesonetDBUser.query(query, [email], { rowMode: "array" });
  let data = await queryHandler.read(1);
  queryHandler.close();

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
    if(typeof email !== "string") {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must be a JSON object including the following parameters: \n\
        email: A string representing the email to lookup. \n\
        ahupuaʻa (optional): An array of the names of ahupuaa to include in the climate report \n\
        county (optional): An array of the names of counties to include in the climate report \n\
        watershed (optional): An array of the names of watershed to include in the climate report \n\
        moku (optional): An array of the names of moku to include in the climate report`
      );
    }
    let id = uuidv4()
    const timestamp = new Date().toISOString();

    const ahupuaa = req.body.ahupuaa || [];
    const county = req.body.county || [];
    const watershed = req.body.watershed || [];
    const moku = req.body.moku || [];
    
    let query = `
      INSERT INTO climate_report_register
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
      ON CONFLICT (email) WHERE active = FALSE DO UPDATE
      SET ahupuaa = $3, county = $4, watershed = $5, moku = $6, modified = $8, active = TRUE;
    `;

    let modified = await mesonetDBUser.queryNoRes(query, [id, email, ahupuaa, county, watershed, moku, timestamp, timestamp]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("A user with this email address is already subscribed.");
    }

    id = getUserID(email);
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

    const id = getUserID(email);
    reqData.code = 200;
    return res.status(200)
    .json({userID: id});
  });
});

router.get("/mesonet/climate_report/:id", async (req, res) => {
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

    let query = `
      SELECT email, ahupuaa, county, watershed, moku, created, modified, active
      FROM climate_report_register
      WHERE email = $1 AND active = TRUE;
    `;
    let queryHandler = await mesonetDBUser.query(query, [email]);
    let data = await queryHandler.read(1);
    queryHandler.close();

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


router.put("/mesonet/climate_report/:id", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { email } = req.body;
    const { id } = req.params;

    if(typeof email !== "string") {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must be a JSON object including the following parameters: \n\
        email: A string representing the email to lookup. \n\
        ahupuaʻa (optional): An array of the names of ahupuaa to include in the climate report \n\
        county (optional): An array of the names of counties to include in the climate report \n\
        watershed (optional): An array of the names of watershed to include in the climate report \n\
        moku (optional): An array of the names of moku to include in the climate report`
      );
    }

    const ahupuaa = req.body.ahupuaa || [];
    const county = req.body.county || [];
    const watershed = req.body.watershed || [];
    const moku = req.body.moku || [];

    const timestamp = new Date().toISOString();
    let query = `
      UPDATE climate_report_register
      SET ahupuaa = $1, county = $2, watershed = $3, moku = $4, modified = $5
      WHERE id = $6 AND active = TRUE;
    `;

    let modified = await mesonetDBUser.queryNoRes(query, [ahupuaa, county, watershed, moku, timestamp, id]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found or is inactive. No changes have been made.");
    }
    reqData.code = 204;
    return res.status(204);
  });
});


router.patch("/mesonet/climate_report/:id/unsubscribe", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;
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
      .send("User not found or is inactive. no changes have been made");
    }
    reqData.code = 204;
    return res.status(204);
  });
});


router.get("/mesonet/climate_report/subscriptions", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    let query = `
      SELECT id, email, ahupuaa, county, watershed, moku
      FROM climate_report_register
      WHERE active = TRUE;
    `;

    let data = [];
    let queryHandler = await mesonetDBUser.query(query, []);

    const chunkSize = 10000;
    let chunk: any[];
    do {
      chunk = await queryHandler.read(chunkSize);
      data = data.concat(chunk);
    }
    while(chunk.length > 0)
    queryHandler.close();
    
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});