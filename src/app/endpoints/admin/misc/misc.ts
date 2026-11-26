import express from "express";
import { logDir } from "../../../modules/util/config.js";
import * as fs from "fs";
import { apiDB } from "../../../modules/util/resourceManagers/db.js";
import { parseParams } from "../../../modules/util/dbUtil.js";
import { parseListParam } from "../../../modules/util/util.js";
import { join } from "path";
import Cursor from "pg-cursor";
import { requireAuth } from "../../../modules/util/auth.js";

export const router = express.Router();

router.get("/users/emails/apitokens", requireAuth("userdata"), async (req, res) => {
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
  return res.status(200)
  .json(emails);
});


router.get("/users/emails/apiqueries", requireAuth("userdata"), async (req, res) => {
  let dataFile = join(logDir, "email_log/emails.json");
  let dataStr = await fs.promises.readFile(dataFile, "utf8");
  let data = JSON.parse(dataStr);
  return res.status(200)
  .json(data);
});



router.get("/error", requireAuth("admin"), async (req, res) => {
  throw new Error("This is a test error.");
});