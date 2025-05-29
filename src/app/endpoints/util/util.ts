import express from "express";
import { apiDB, mesonetDBUser, mesonetDBAdmin } from "../../modules/util/resourceManagers/db.js";
import { fsHealthData } from "../../modules/util/config.js";
import { readFileSync } from "fs";

export const router = express.Router();

router.get("/health", async (req, res) => {
  const health = {
    apiDB: false,
    mesonetDBUser: false,
    mesonetDBAdmin: false,
    fs: false
  }
  try {
    await apiDB.queryNoRes("SELECT 1", []);
    health.apiDB = true;
  }
  catch(e) {}
  try {
    await mesonetDBUser.queryNoRes("SELECT 1", []);
    health.mesonetDBUser = true;
  }
  catch(e) {}
  try {
    await mesonetDBAdmin.queryNoRes("SELECT 1", []);
    health.mesonetDBAdmin = true;
  }
  catch(e) {}
  try {
    const { file, content } = fsHealthData;
    let data = readFileSync(file);
    if(data !== content) {
      throw new Error();
    }
    health.fs = true;
  }
  catch(e) {}
  return res.status(200)
  .json(health);
});