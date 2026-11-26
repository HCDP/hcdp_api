import Cursor from "pg-cursor";
import { apiDB } from "./resourceManagers/db.js";
import { Request, Response, NextFunction } from "express";

async function validateTokenAccess(token, permission) {
  let valid = false;
  let allowed = false;
  let user = "";

  let query = `
    SELECT user_label, ($2 = ANY(permissions)) as allowed
    FROM auth_token_store
    WHERE token = $1;
  `;

  let queryRes = await apiDB.query(query, [token, permission], async (cursor: Cursor) => {
    return await cursor.read(1);
  });
  
  if(queryRes.length > 0) {
    let { user_label, allowed: dbAllowed } = queryRes[0];
    valid = true;
    user = user_label;
    allowed = dbAllowed;
  }
  return {
    valid,
    allowed,
    token,
    user
  };
}

async function validateToken(req, permission) {
  let tokenData = {
    valid: false,
    allowed: false,
    token: "",
    user: ""
  };

  let auth = req.get("authorization");
  if(auth) {
    let authPattern = /^Bearer (.+)$/;
    let match = auth.match(authPattern);
    if(match) {
      //validate token is registered and has required permission
      tokenData = await validateTokenAccess(match[1], permission);
    }
  }
  return tokenData;
}



export function requireAuth(permission: string) {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { valid, allowed, token, user } = await validateToken(req, permission);
      res.locals.setAuthUser(token, user)

      if (!valid) {
        return res.status(401)
        .send("User not authorized. Please provide a valid API token in the request header. If you do not have an API token one can be requested by filling out the form at https://www.hawaii.edu/climate-data-portal/hcdp-hawaii-mesonet-api/.");
      }
      if (!allowed) {
        return res.status(403)
        .send("User does not have permission to perform this action.");
      }
      next();
    }
    catch(e) {
      next(e);
    }
  };
}