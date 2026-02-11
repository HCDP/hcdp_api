import Cursor from "pg-cursor";
import { apiDB } from "./resourceManagers/db.js";

async function validateTokenAccess(token, permission) {
  let valid = false;
  let allowed = false;
  let user = "";

  let query = `
    SELECT user_label, permissions
    FROM auth_token_store
    WHERE token = $1;
  `;

  let queryRes = await apiDB.query(query, [token], async (cursor: Cursor) => {
    return await cursor.read(1);
  });
  
  if(queryRes.length > 0) {
    let { user_label, permissions } = queryRes[0];
    valid = true;
    user = user_label;
    const authorized = permissions.split(",");
    if(authorized.includes(permission)) {
      allowed = true;
    }
  }
  return {
    valid,
    allowed,
    token,
    user
  };
}

export async function validateToken(req, permission) {
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