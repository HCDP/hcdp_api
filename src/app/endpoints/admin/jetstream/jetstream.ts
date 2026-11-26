import express from "express";
import { execFile } from 'child_process';
import util from 'util';
import { validate as validateUUID } from 'uuid';
import { requireAuth } from "../../../modules/util/auth";

const execFilePromise = util.promisify(execFile);
export const router = express.Router();

function validateJS2Instance(allocation: string, instanceId: string) {
  let valid = true;
  let errMsg = "";

  const allocationRegex = /^[A-Za-z]{3}\d{6,7}_(UH|IU)$/i;
  if(!allocationRegex.test(allocation)) {
    valid = false;
    errMsg = `Invalid allocation identifier ${allocation}.`;
  }
  if(!validateUUID(instanceId)) {
    valid = false;
    errMsg = `Invalid server identifier ${instanceId}. You must provide a valid Instance ID (UUID).`;
  }
  return {
    valid,
    errMsg
  };
}

async function executeOpenstack(args: string[]) {
  let success = true;
  let stdout = "";
  let stderr = "";
  let error = null;
  try { 
    // Execute the OpenStack binary directly with the safe args array
    ({ stdout, stderr } = await execFilePromise("openstack", args));
  }
  catch(e: any) {
    success = false;
    error = e;
    stderr = e.stderr || ""; 
    stdout = e.stdout || "";
  }

  stdout = stdout.trim();
  stderr = stderr.trim();

  return {
    success,
    stdout,
    stderr,
    error
  };
}



router.post("/js2/manage/:action/:allocation/:instanceId", requireAuth("js2_admin"), async (req, res) => {
  const { allocation, instanceId, action } = req.params;

  // validations
  const validActions = ["shelve", "unshelve"];
  if(!validActions.includes(action)) {
    return res.status(400)
    .send(`Invalid action ${action}`);
  }
  
  const {valid, errMsg} = validateJS2Instance(allocation, instanceId);
  if(!valid) {
    return res.status(400)
    .send(errMsg);
  }

  const data = await executeOpenstack(["--os-cloud", allocation, "server", action, instanceId]);
  const { success, stdout, stderr, error } = data;
  if(!success) {
    if(stderr.includes("No server with a name or ID of")) {
      return res.status(404)
      .send(`Instance ${instanceId} not found.`);
    }

    return res.status(500)
    .send(`An error occurred while processing the openstack request: ${error}`);
  }

  return res.status(200)
  .json({ stdout, stderr });
});







router.get("/js2/status/:allocation/:instanceId", requireAuth("js2_admin"), async (req, res) => {
  const { allocation, instanceId } = req.params;

  // validations
  const {valid, errMsg} = validateJS2Instance(allocation, instanceId);
  if(!valid) {
    return res.status(400)
    .send(errMsg);
  }

  const data = await executeOpenstack(["--os-cloud", allocation, "server", "show", instanceId, "-c", "status", "-f", "value"]);
  const { success, stdout, stderr, error } = data;
  if(!success) {
    if(stderr.includes("No server with a name or ID of")) {
      return res.status(404)
      .send(`Instance ${instanceId} not found.`);
    }
    return res.status(500)
    .send(`An error occurred while processing the openstack request: ${error}`);
  }
  return res.status(200)
  .json({ stdout, stderr });
});