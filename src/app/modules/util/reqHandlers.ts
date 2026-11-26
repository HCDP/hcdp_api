import { emailAdminError } from "./email.js";
import { logReq, RequestLogData } from "./logging.js";
import { Request, Response, NextFunction } from "express";
import { API_VERSION } from "./version.js";
import { randomUUID } from "crypto";



export function initAndLog(req: Request, res: Response, next: NextFunction) {
  const startTime = new Date();
  const requestId = randomUUID();
  res.setHeader("X-Request-Id", requestId);

  const qIndex = req.originalUrl.indexOf("?");
  const query = qIndex !== -1 ? req.originalUrl.slice(qIndex + 1) : null;

  let resFinished = false;
  const backgroundTasks: (() => Promise<boolean>)[] = [];

  // register background tasks prior to sending response and prevent premature unhandled rejections
  res.locals.addBackgroundTask = (task: () => Promise<boolean>) => {
    backgroundTasks.push(task);
  };

  let reqData: RequestLogData = {
    apiVersion: API_VERSION,

    userIp: req.ip || null,
    userAgent: req.get("user-agent") || null,
    requestId,

    method: req.method,
    endpoint: req.path,
    template: null,
    query: query,

    startTime,
    returnTime: null,
    endTime: null,
    returnDurationMs: null,
    durationMs: null,

    token: null,
    tokenUser: null,
    email: null,

    sizeF: null,
    sizeB: null,

    code: null,
    success: null,
    errorHandled: null,
    error: null,
    adminEmailed: null
  };

  res.locals.setAuthUser = (token: string, tokenUser: string) => {
    reqData.token = token;
    reqData.tokenUser = tokenUser;
  };
  res.locals.setEmail = (email: string) => {
    reqData.email = email;
  };
  res.locals.setSizeF = (sizeF: number) => {
    reqData.sizeF = sizeF;
  };
  res.locals.setSizeB = (sizeB: number) => {
    reqData.sizeB = sizeB;
  };
  res.locals.setErrorMsg = (message: string) => {
    reqData.error = message;
  };
  res.locals.setSuccess = (success: boolean) => {
    reqData.success = success;
  };

  const handleUnhandledError = async (err: any) => {
    const errorTraceMsg = `error: ${err}\ntrace: ${err?.stack}`;
    reqData.error = errorTraceMsg;
    const adminErrorMsg = `method: ${reqData.method}\nendpoint: ${reqData.endpoint}\n${errorTraceMsg}`;
    console.error(`An unexpected error occurred:\n${adminErrorMsg}`);
    const adminEmailed = await emailAdminError(adminErrorMsg);
    reqData.adminEmailed = adminEmailed;
  };

  res.locals.recordUnhandledError = async (err: any) => {
    reqData.success = false;
    reqData.errorHandled = false;

    // defer handling to finish listener if response has not completed yet
    if(!resFinished) {
      res.locals.unhandledError = err;
    }
    // if response already finished, handle error and update log immediately
    else {
      const endTime = new Date();
      reqData.endTime = endTime;
      reqData.durationMs = endTime.getTime() - reqData.startTime.getTime();
      await handleUnhandledError(err);
      logReq(reqData);
    }
  };

  res.on("finish", async () => {
    resFinished = true;
    const returnTime = new Date();
    const durationMs = returnTime.getTime() - reqData.startTime.getTime();

    reqData.returnTime = returnTime;
    reqData.returnDurationMs = durationMs;
    let status = res.statusCode;
    reqData.code = status;

    if(req.route) {
      reqData.template = `${req.baseUrl}${req.route.path}`;
    }

    let code2xx = Math.floor(status / 100) === 2;
    let runBackground = backgroundTasks.length > 0 && code2xx;


    // if background tasks have not been registered, set completion info
    if(!runBackground) {
      reqData.endTime = returnTime;
      reqData.durationMs = durationMs;
      let success = code2xx;
      // if success was not manually set then use status to determine
      if(reqData.success === null) {
        reqData.success = success;
      }
      // an error was encountered and properly handled
      if(!reqData.success && reqData.errorHandled === null) {
        reqData.errorHandled = true;
      }
    }
    // if the error handler set an unhandledError, handle it
    if(res.locals.unhandledError) {
      await handleUnhandledError(res.locals.unhandledError);
    }
    // log the request at this point
    logReq(reqData);

    // if there were background tasks registered, wait for those to complete and update logs accordingly
    if(runBackground) {
      const tasks: Promise<boolean>[] = [];
      try {
        for(let task of backgroundTasks) {
          tasks.push((async () => task())());
        }
        let backgroundResults = await Promise.all(tasks);
        // if success status was not already set, check if all background tasks were successful (should return success bool)
        if(reqData.success === null) {
          reqData.success = backgroundResults.every((value: boolean) => value);
        }
        // an error was encountered in background tasks and properly handled
        if(!reqData.success && reqData.errorHandled === null) {
          reqData.errorHandled = true;
        }
      }
      // unhandled error
      catch(err: any) {
        reqData.success = false;
        reqData.errorHandled = false;
        // handle the unhandled error
        await handleUnhandledError(err);
      }
      // set task end timing
      const endTime = new Date();
      const endDurationMs = endTime.getTime() - reqData.startTime.getTime();
      reqData.endTime = endTime;
      reqData.durationMs = endDurationMs;
      // update the request data
      logReq(reqData);
    }
  });

  next();
}



export function errorHandler(err: any, req: Request, res: Response, next: NextFunction) {
  if(res.locals.recordUnhandledError) {
    void res.locals.recordUnhandledError(err);
  }
  else {
    console.error("Unhandled error before initAndLog:", err);
  }

  if(!res.headersSent) {
    res.status(500).send("An unexpected error occurred.");
  }
  else if(!res.writableEnded) {
    res.end();
  }
}