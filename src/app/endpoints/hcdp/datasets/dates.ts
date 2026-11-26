import express from "express";
import { getDatasetDateRange } from "../../../modules/fileIndexer.js";
import { requireAuth } from "../../../modules/util/auth.js";

export const router = express.Router();

router.get("/datasets/date/range", requireAuth("basic"), async (req, res) => {
    let {...dataset} = req.query;
    let dateRange = await getDatasetDateRange(dataset);
    if(dateRange) {
      return res.status(200)
      .json(dateRange);
    }
    else {
      return res.status(404)
      .send("Could not find date range for the provided dataset.");
    }
  });

  
  router.get("/datasets/date/next", requireAuth("basic"), async (req, res) => {
    return res.status(501)
    .end();
  });

  
  router.get("/datasets/date/previous", requireAuth("basic"), async (req, res) => {
    return res.status(501)
    .end();
  });

  
  router.get("/datasets/date/nearest", requireAuth("basic"), async (req, res) => {
    return res.status(501)
    .end();
  });