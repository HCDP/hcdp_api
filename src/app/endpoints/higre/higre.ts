import express from "express";
import { handleReq } from "../../modules/util/reqHandlers.js";
import { higreMetadataHelper } from "../../modules/util/resourceManagers/tapis.js";
import { processTapisError, validateJSON, validateType } from "../../modules/util/util.js";


export const router = express.Router();


router.get("/higre/query", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const r400 = (message: string) => {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `${message}
        
        Request must include the following parameters:

        Required:
        q: A mongo query for the data being requested

        Optional:
        limit: An integer representing the maximum results to return. Default unlimited
        offset: An integer representing the number of results to skip. Default value 0`
      );
    }

    let { q, limit, offset }: any = req.query;

    if(q === undefined || !validateType(q, ["string"])) {
      return r400("q must be a string.");
    }

    // Convert limit and offset to integers
    limit = limit ? parseInt(limit) : undefined;
    offset = offset ? parseInt(offset) : undefined;

    // validate limit and offset were able to be parsed to valid integers
    if((limit !== undefined && isNaN(limit)) || (offset !== undefined && isNaN(offset))) {
      return r400("Limit and offset must be valid integers.");
    }

    let data = null;
    try {
      data = await higreMetadataHelper.queryMetadata(q, limit, offset);
    }
    catch(e) {
      return processTapisError(res, reqData, e);
    }

    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});