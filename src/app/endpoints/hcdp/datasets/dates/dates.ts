import express from "express";
import { handleReq } from "../../../../modules/util/reqHandlers.js";
import { getDatasetDateRange } from "../../../../modules/fileIndexer.js";

export const router = express.Router();

router.get("/datasets/date/range", async (req, res) => {
    const permission = "basic";
    await handleReq(req, res, permission, async (reqData) => {
      let {...dataset} = req.query;
      let dateRange = await getDatasetDateRange(dataset);
      if(dateRange) {
        reqData.code = 200;
        return res.status(200)
        .json(dateRange);
      }
      else {
        reqData.code = 404;
        return res.status(404)
        .send("Could not find date range for the provided dataset.");
      }
    });
  });
  
  router.get("/datasets/date/next", async (req, res) => {
    const permission = "basic";
    await handleReq(req, res, permission, async (reqData) => {
      reqData.code = 501;
      return res.status(501)
      .end();
    });
  });
  
  router.get("/datasets/date/previous", async (req, res) => {
    const permission = "basic";
    await handleReq(req, res, permission, async (reqData) => {
      reqData.code = 501;
      return res.status(501)
      .end();
    });
  });
  
  router.get("/datasets/date/nearest", async (req, res) => {
    const permission = "basic";
    await handleReq(req, res, permission, async (reqData) => {
      reqData.code = 501;
      return res.status(501)
      .end();
    });
  });