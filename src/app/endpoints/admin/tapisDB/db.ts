import express from "express";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { tapisDBManager } from "../../../modules/util/resourceManagers/tapis.js";
import * as sanitize from "mongo-sanitize";

export const router = express.Router();

router.post("/db/replace", async (req, res) => {
  const permission = "db";
  await handleReq(req, res, permission, async (reqData) => {
    const uuid = req.body.uuid;
    let value = req.body.value;

    if(typeof uuid !== "string" || value === undefined) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        uuid: A string representing the uuid of the document to have it's value replaced \n\
        value: The new value to set the document's 'value' field to`
      );
    }
    else {
      //sanitize value object to ensure no $ fields since this can be an arbitrary object
      value = sanitize(value);
      //note this only replaces value, should not be wrapped with name
      let replaced = await tapisDBManager.replaceRecord(uuid, value);
      reqData.code = 200;
      res.status(200)
      .send(replaced.toString());
    }
  });
});

router.post("/db/delete", async (req, res) => {
  const permission = "db";
  await handleReq(req, res, permission, async (reqData) => {
    const uuid = req.body.uuid;

    if(typeof uuid !== "string") {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      return res.status(400)
      .send(
        `Request body should include the following fields: \n\
        uuid: A string representing the uuid of the document to delete.`
      );
    }
    else {
      let deleted = await tapisDBManager.deleteRecord(uuid);
      reqData.code = 200;
      res.status(200)
      .send(deleted.toString());
    }
  });
});

router.post("/db/bulkDelete", async (req, res) => {
  const permission = "db";
  await handleReq(req, res, permission, async (reqData) => {
    const uuids = req.body.uuids;

    if(!Array.isArray(uuids)) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      return res.status(400)
      .send(
        `Request body should include the following fields: \n\
        uuids: An array string representing the uuids of the documents to delete`
      );
    }
    else {
      let deleted = await tapisDBManager.bulkDelete(uuids);
      reqData.code = 200;
      res.status(200)
      .send(deleted.toString());
    }
  });
});