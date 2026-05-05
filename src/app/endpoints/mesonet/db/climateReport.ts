import express from "express";
import { hcdpGeneralUser, hcdpGeneralAdmin } from "../../../modules/util/resourceManagers/db.js";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { v4 as uuidv4, validate as isValidUUID } from "uuid";
import { checkEmail, MailOptions, sendEmail, validateArray, validateType } from "../../../modules/util/util.js";
import Cursor from "pg-cursor";
import moment from "moment-timezone";
import { parseParams } from "../../../modules/util/dbUtil.js";
import { parseListParam } from "../../../modules/util/util.js";

export const router = express.Router();

const REPORT_TYPES = ["ahupuaa", "county", "watershed", "moku"];
const STAT_TABLE_DATA = {
  rainfall_stats: ['island', 'division_type', 'name', 'date', 'mean', 'anomaly', 'pchange', 'rank', 'ytd_pnormal'],
  temperature_stats: ['island', 'division_type', 'name', 'date', 'mean', 'anomaly', 'pchange', 'rank', 'max'],
  drought_stats: ['island', 'division_type', 'name', 'date', 'd4', 'd3', 'd2', 'd1', 'd0', 'near_normal', 'w0', 'w1', 'w2', 'w3', 'w4'],
  rainfall_historical: ['island', 'division_type', 'name', 'date', 'value'],
  temperature_historical: ['island', 'division_type', 'name', 'date', 'value']
};


function createClimateReportWhereClause(date: string | undefined, startDate: string | undefined, endDate: string | undefined, islands: string[], divisionTypes: string[], names: string[]): { whereClause: string, params: string[] } {
  let params: any[] = [];
  let conditions: string[] = [];

  if(islands.length > 0) {
    parseParams(islands, params, conditions, "island");
  }
  if(divisionTypes.length > 0) {
    parseParams(divisionTypes, params, conditions, "division_type");
  }
  if(names.length > 0) {
    parseParams(names, params, conditions, "name");
  }

  // Filter by exact date, or range
  if(date) {
    if(typeof date !== "string" || !moment(date).isValid()) {
      throw new Error("Invalid date format");
    }
    params.push(moment(date).format('YYYY-MM-DD'));
    conditions.push(`date = $${params.length}`);
  }
  else {
    if(startDate){
      if(typeof startDate !== "string" || !moment(startDate).isValid()) {
        throw new Error("Invalid startDate format");
      }
      params.push(moment(startDate).format('YYYY-MM-DD'));
      conditions.push(`date >= $${params.length}`);
    }
    if(endDate) {
      if (typeof endDate !== "string" || !moment(endDate).isValid()) {
        throw new Error("Invalid endDate format");
      }
      params.push(moment(endDate).format('YYYY-MM-DD'));
      conditions.push(`date <= $${params.length}`);
    }
  }
  
  let whereClause = ""
  if(conditions.length > 0) {
    whereClause = `WHERE ${conditions.join(" AND ")}`;
  }
  return { whereClause, params };
}


async function getUserID(email: string): Promise<string> {
  let query = `
    SELECT id
    FROM climate_report.climate_report_register
    WHERE email = $1 AND ACTIVE = TRUE;
  `;

  let data = await hcdpGeneralAdmin.query(query, [email], async (cursor: Cursor) => {
    return await cursor.read(1);
  }, { rowMode: "array" });

  let id = null;
  if(data.length > 0) {
    id = data[0][0]
  }
  return id;
}



router.post("/mesonet/climate_report/subscribe", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { email } = req.body;
    if(typeof email !== "string" || !checkEmail(email)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must be a JSON object including the following parameters: \n\
        email: A valid email string to receive climate reports at. \n\
        ahupuaʻa (optional): An array of the names of ahupuaa to include in the climate report \n\
        county (optional): An array of the names of counties to include in the climate report \n\
        watershed (optional): An array of the names of watershed to include in the climate report \n\
        moku (optional): An array of the names of moku to include in the climate report`
      );
    }
    let id = uuidv4()
    const timestamp = new Date().toISOString();

    for(let type of REPORT_TYPES) {
      let value = req.body[type];
      if(value === undefined) {
        req.body[type] = [];
      }
      else if(!validateArray(value, (value) => typeof value === "string")) {
        reqData.success = false;
        reqData.code = 400;
  
        return res.status(400)
        .send(
          `Invalid value for ${type}, must be an array of strings.`
        );
      }
    }
    
    const { ahupuaa, county, watershed, moku } = req.body;
    
    let query = `
      INSERT INTO climate_report.climate_report_register
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
      ON CONFLICT (email) DO UPDATE
      SET ahupuaa = $3, county = $4, watershed = $5, moku = $6, modified = $8, active = TRUE
      WHERE climate_report.climate_report_register.active = FALSE;
    `;

    let modified = await hcdpGeneralAdmin.queryNoRes(query, [id, email, ahupuaa, county, watershed, moku, timestamp, timestamp]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("A user with this email address is already subscribed.");
    }

    id = await getUserID(email);
    reqData.code = 200;
    return res.status(200)
    .json({userID: id});
  });
});

router.get("/mesonet/climate_report/email_lookup", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { email } = req.query;
    if(typeof email !== "string") {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request must include the following parameters: \n\
        email: A string representing the email to lookup.`
      );
    }

    const id = await getUserID(email);
    reqData.code = 200;
    return res.status(200)
    .json({userID: id});
  });
});

router.get("/mesonet/climate_report/subscription/:id", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let query = `
      SELECT email, ahupuaa, county, watershed, moku, created, modified, active
      FROM climate_report.climate_report_register
      WHERE id = $1 AND active = TRUE;
    `;

    let data = await hcdpGeneralAdmin.query(query, [id], async (cursor: Cursor) => {
      return await cursor.read(1);
    });

    if(data.length < 1) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found or is inactive.");
    }
    reqData.code = 200;
    return res.status(200)
    .json(data[0]);
  });
});


router.patch("/mesonet/climate_report/subscription/:id", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let updateParams = [];
    let setParts = [];
    let i = 1;
    for(let type of REPORT_TYPES) {
      if(req.body[type] !== undefined) {
        if(!validateArray(req.body[type], (value) => typeof value === "string")) {
          reqData.success = false;
          reqData.code = 400;
    
          return res.status(400)
          .send(
            `Invalid value for ${type}, must be an array of strings.`
          );
        }
        updateParams.push(req.body[type]);
        setParts.push(`${type} = $${i++}`);
      }
    }
    if(setParts.length < 1) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must include at least one of the following fields to update: \n\
        ahupuaʻa: An array of the names of ahupuaa to include in the climate report \n\
        county: An array of the names of counties to include in the climate report \n\
        watershed: An array of the names of watershed to include in the climate report \n\
        moku: An array of the names of moku to include in the climate report`
      );
    }
    // update time record was modified
    const timestamp = new Date().toISOString();
    setParts.push(`modified = $${i++}`);

    // create list string for set clause
    let setString = setParts.join(", ");
    
    let query = `
      UPDATE climate_report.climate_report_register
      SET ${setString}
      WHERE id = $${i} AND active = TRUE;
    `;

    let modified = await hcdpGeneralAdmin.queryNoRes(query, [...updateParams, timestamp, id]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found or is inactive. No changes have been made.");
    }
    reqData.code = 204;
    return res.status(204).end();
  });
});


router.patch("/mesonet/climate_report/subscription/:id/unsubscribe", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let query = `
      UPDATE climate_report.climate_report_register
      SET active = FALSE
      WHERE id = $1;
    `;
    let modified = await hcdpGeneralAdmin.queryNoRes(query, [id]);
    if(!modified) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found. No changes have been made");
    }
    reqData.code = 204;
    return res.status(204).end();
  });
});


router.get("/mesonet/climate_report/subscriptions", async (req, res) => {
  const permission = "userdata";
  await handleReq(req, res, permission, async (reqData) => {
    let query = `
      SELECT id, email, ahupuaa, county, watershed, moku
      FROM climate_report.climate_report_register
      WHERE active = TRUE;
    `;


    let data = await hcdpGeneralAdmin.query(query, [], async (cursor: Cursor) => {
      let rows = [];
      const chunkSize = 10000;
      let chunk: any[];
      do {
        chunk = await cursor.read(chunkSize);
        for(let row of chunk) {
          rows.push(row);
        }
      }
      while(chunk.length > 0)
      return rows;
    });
    
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.post("/mesonet/climate_report/subscription/:id/email", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {
    const { id } = req.params;
    let { text, html } = req.body;

    if(!validateType(text, ["string"])) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body must include the content you would like to send to the user: \n\
        text: A string containing the email content you would like to send. \n\
        html (optional): An HTML string containing the email content you would like to send. This will default to the text data provided wrapped in a paragraph block if not provided.`
      ); 
    }
    if(!validateType(html, ["string"])) {
      html = `<p>${text.replace("\n", "<br/>")}</p>`;
    }

    if(!isValidUUID(id)) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Invalid UUID provided in url`
      );
    }

    let query = `
      SELECT email
      FROM climate_report.climate_report_register
      WHERE id = $1 AND active = TRUE;
    `;

    let data = await hcdpGeneralAdmin.query(query, [id], async (cursor: Cursor) => {
      return await cursor.read(1);
    }, { rowMode: "array" });

    if(data.length < 1) {
      reqData.success = false;
      reqData.code = 404;

      return res.status(404)
      .send("User not found. Email has not been sent.");
    }

    const subject = "Your Monthly Hawaiʻi Climate Report";

    const socSite = "https://www.hawaii.edu/climate-data-portal/state-of-the-climate";
    const unsubscribeLink = `${socSite}/#/unsubscribe?id=${id}`;

    //text
    const introText = "Here is your monthly climate report:";
    const signoffText = "Thank you for subscribing! Sincerely,\nThe HCDP Team"
    const unsubscribeMessageText = `Subscription preferences can be updated at ${socSite} No longer interested in receiving these emails? Visit ${unsubscribeLink} to unsubscribe.`;
    let textParts = [introText, text, signoffText, unsubscribeMessageText];
    text = textParts.join("\n");

    //html
    const introHTML = "<h3>Here is your monthly climate report:</h3>";
    const signoffHTML = "<p>Thank you for subscribing! Sincerely,<br/>The HCDP Team</p>"
    const unsubscribeMessageHTML = `<p>Subscription preferences can be updated <a href="${socSite}">here</a>. No longer interested in receiving these emails? <a href="${unsubscribeLink}">Unsubscribe</a></p>`;
    let htmlParts = [html, signoffHTML, unsubscribeMessageHTML];
    html = `${introHTML}${htmlParts.join("<br/>")}`;
    
    
    let email = data[0][0];
    let mailOptions: MailOptions = {
      to: email,
      subject,
      text,
      html
    }

    let mailRes = await sendEmail(mailOptions);
    if(!mailRes.success) {
      reqData.success = false;
      throw new Error("Failed to email user " + email + ". Error: " + mailRes.error.toString());
    }
    
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});




router.get("/mesonet/climate_report/:table", async (req, res) => {
  const permission = "basic";
  await handleReq(req, res, permission, async (reqData) => {
    const e400 = (reason: string) => {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(reason);
    }

    const { table } = req.params;
    const tableSchema = STAT_TABLE_DATA[table];

    if(!tableSchema) {
      reqData.success = false;
      reqData.code = 400;

      return e400(`Invalid table ${table}, valid tables: ${Object.keys(STAT_TABLE_DATA)}`); 
    }

    const { date, startDate, endDate, island, division_type, name } = req.query;
    let islands = parseListParam(island)
    let divisionTypes = parseListParam(division_type);
    let names = parseListParam(name);

    if(
      date && typeof date !== "string" ||
      startDate && typeof startDate !== "string" ||
      endDate && typeof endDate !== "string"
    ) {
      return e400("Invalid date format");
    }

    let whereClause: string;
    let params: string[];
    try {
      ({ whereClause, params } = createClimateReportWhereClause(date as string | undefined, startDate as string | undefined, endDate as string | undefined, islands, divisionTypes, names));
    }
    catch(e: any) {
      return e400(e?.message || e?.toString());
    }

    let columns = tableSchema.join(", ");

    let query = `
      SELECT ${columns}
      FROM climate_report.${table}
      ${whereClause};
    `;
    

    let data = await hcdpGeneralUser.query(query, params, async (cursor: Cursor) => {
      let rows = [];
      const chunkSize = 10000;
      let chunk: any[];
      do {
        chunk = await cursor.read(chunkSize);
        for(let row of chunk) {
          rows.push(row);
        }
      }
      while(chunk.length > 0)
      return rows;
    });
    
    reqData.code = 200;
    return res.status(200)
    .json(data);
  });
});



router.post("/mesonet/climate_report/:table", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {

    const e400 = (reason: string) => {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(reason);
    }

    const { table } = req.params;
    const tableSchema = STAT_TABLE_DATA[table];

    if(!tableSchema) {
      reqData.success = false;
      reqData.code = 400;

      return e400(`Invalid table ${table}, valid tables: ${Object.keys(STAT_TABLE_DATA)}`); 
    }

    let { overwrite, data } = req.body;

    if (!Array.isArray(data) || data.length === 0) {
      return e400("Data must be a non-empty 2D array.");
    }

    if(overwrite === undefined) {
      overwrite = true;
    }

    let onConflict = "DO NOTHING";
    if (overwrite) {
      const updateCols = tableSchema.filter(col => !['island', 'division_type', 'name', 'date'].includes(col));
      const setClause = updateCols.map(col => `${col} = EXCLUDED.${col}`).join(', ');
      onConflict = `DO UPDATE SET ${setClause}`;
    }

    let params: (string | number)[] = [];
    let valueClauseParts: string[] = [];
    // Parse the 2D array data
    for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
      let row = data[rowIndex];
      
      if (!Array.isArray(row)) {
        return e400(`Invalid data provided at index ${rowIndex}. Data must be a 2D array.`);
      }

      if (row.length !== tableSchema.length) {
        return e400(`Row ${rowIndex} has ${row.length} columns, but table ${table} expects ${tableSchema.length}.`);
      }

      let rowParts = [];
      
      for (let colIndex = 0; colIndex < row.length; colIndex++) {
        switch (colIndex) {
          case 0:
          case 1:
          case 2: {
            if (typeof row[colIndex] !== "string") {
              return e400(`Invalid string value provided at row ${rowIndex}, column ${colIndex}.`);
            }
            break;
          }
          case 3: {
            let parsedDate = moment(row[colIndex]);
            if (parsedDate.isValid()) {
              row[colIndex] = parsedDate.format('YYYY-MM-DD'); 
            } else {
              return e400(`Invalid date provided at row ${rowIndex}, column ${colIndex}.`);
            }
            break;
          }
          default: {
            if (row[colIndex] === "" || row[colIndex] === null || row[colIndex] === undefined) {
              row[colIndex] = null;
            }
            else {
              let value = Number(row[colIndex]);
              if (!isNaN(value)) {
                row[colIndex] = value;
              } else {
                return e400(`Invalid numeric value at row ${rowIndex}, column ${colIndex}.`);
              }
            }
            break;
          }
        }

        params.push(row[colIndex]);
        rowParts.push(`$${params.length}`);
      }
      valueClauseParts.push(`(${rowParts.join(',')})`);
    }

    let valueClause = valueClauseParts.join(",");
    let columnsClause = tableSchema.join(", ");

    let query = `
      INSERT INTO climate_report.${table} (${columnsClause})
      VALUES ${valueClause}
      ON CONFLICT (island, division_type, name, date)
      ${onConflict};
    `;

    let modified = await hcdpGeneralAdmin.queryNoRes(query, params);
    reqData.code = 200;
    return res.status(200)
    .json({ modified });
  });
});




router.delete("/mesonet/climate_report/:table", async (req, res) => {
  const permission = "meso_admin";
  await handleReq(req, res, permission, async (reqData) => {
    const e400 = (reason: string) => {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(reason);
    }

    const { table } = req.params;
    const tableSchema = STAT_TABLE_DATA[table];

    if(!tableSchema) {
      reqData.success = false;
      reqData.code = 400;

      return e400(`Invalid table ${table}, valid tables: ${Object.keys(STAT_TABLE_DATA)}`); 
    }

    let { date, startDate, endDate, island, division_type, name } = req.body;
    if(
      date && typeof date !== "string" ||
      startDate && typeof startDate !== "string" ||
      endDate && typeof endDate !== "string"
    ) {
      return e400("Invalid date format");
    }
    let islands = Array.isArray(island) ? island : (island ? [island] : []);
    let divisionTypes = Array.isArray(division_type) ? division_type : (division_type ? [division_type] : []);
    let names = Array.isArray(name) ? name : (name ? [name] : []);

    let whereClause: string;
    let params: string[];
    try {
      ({ whereClause, params } = createClimateReportWhereClause(date, startDate, endDate, islands, divisionTypes, names));
    }
    catch(e: any) {
      return e400(e?.message || e?.toString());
    }

    if(!whereClause || whereClause.trim() === "") {
      return e400("Must provide at least one condition for deletion");
    }

    let query = `
      DELETE FROM climate_report.${table}
      ${whereClause};
    `;

    // Execute the query using your existing admin driver
    let deleted = await hcdpGeneralAdmin.queryNoRes(query, params);
    
    reqData.code = 200;
    return res.status(200)
    .json({ deleted });

  });
});