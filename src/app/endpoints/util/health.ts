import express from "express";
import { apiDB, mesonetDBUser, mesonetDBAdmin } from "../../modules/util/resourceManagers/db.js";
import { fsHealthData } from "../../modules/util/config.js";
import { readFileSync } from "fs";

export const router = express.Router();

async function checkHealth() {
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
  return health;
}

router.get("/healthmonitor", async (req, res) => {
  let health = await checkHealth();
  return res.status(200)
  .json(health);
});

router.get("/health", async (req, res) => {
  let health = await checkHealth();
  let isHealthy = true;
  for(let item in health) {
    isHealthy &&= health[item];
  }
  if(isHealthy) {
    return res.status(204)
    .end();
  }
  else {
    return res.status(503)
    .send("Some of the services the API relies on are unreachable or not available at this time. Some parts of the API may not function as expected until this is resolved.");
  }
});