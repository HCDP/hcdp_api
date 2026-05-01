import express from "express";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { logDir } from "../../../modules/util/config.js";
import * as fs from "fs";
import { apiDB } from "../../../modules/util/resourceManagers/db.js";
import { parseParams } from "../../../modules/util/dbUtil.js";
import { parseListParam } from "../../../modules/util/util.js";
import { join } from "path";
import Cursor from "pg-cursor";

export const router = express.Router();

router.get("/users/emails/apitokens", async (req, res) => {
  const permission = "userdata";
  await handleReq(req, res, permission, async (reqData) => {

    let { status }: any = req.query;

    //true, false, null approved
    let params = [];
    let whereClause = [];
    let query = `
      SELECT DISTINCT(email)
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
    let data = await apiDB.query(query, params, async (cursor: Cursor) => {
      return await cursor.read(100000);
    }, { rowMode: "array" });
    
    let emails = data.flat();
    reqData.code = 200;
    return res.status(200)
    .json(emails);
  });
});


router.get("/users/emails/apiqueries", async (req, res) => {
  const permission = "userdata";
  await handleReq(req, res, permission, async (reqData) => {
    let dataFile = join(logDir, "email_log/emails.json");
    let dataStr = await fs.promises.readFile(dataFile, "utf8");
    let data = JSON.parse(dataStr);
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.get("/error", async (req, res) => {
  const permission = "admin";
  await handleReq(req, res, permission, async (reqData) => {
    throw new Error("This is a test error.");
  });
});