import express from "express";
import { higreMetadataHelper } from "../../modules/util/resourceManagers/tapis.js";
import { processTapisError } from "../../modules/util/util.js";
import { requireAuth } from "../../modules/util/auth.js";


export const router = express.Router();


router.get("/higre/query", requireAuth("basic"), async (req, res) => {
  const r400 = (message: string) => {
    return res.status(400)
    .send(
      `${message}
      
      Request must include the following parameters:

      Required:
      q: A JSON formatted mongo query for the data being requested

      Optional:
      limit: An integer representing the maximum results to return. Default unlimited
      offset: An integer representing the number of results to skip. Default value 0`
    );
  }

  let { q, limit, offset }: any = req.query;

  let parsedQuery: string;
  try {
    parsedQuery = JSON.parse(q);
  }
  catch (e) {
    return r400("q must be a valid JSON string.");
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
    data = await higreMetadataHelper.queryMetadata(parsedQuery, limit, offset);
  }
  catch(e) {
    return processTapisError(res, e);
  }

  return res.status(200)
  .json(data);
});