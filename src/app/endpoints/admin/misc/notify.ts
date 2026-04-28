import { sendEmail } from "../../../modules/util/util.js";
import express from "express";
import { handleReq } from "../../../modules/util/reqHandlers.js";
import { apiDB } from "../../../modules/util/resourceManagers/db.js";
import { v4 as uuidv4 } from "uuid";
import Cursor from "pg-cursor";

export const router = express.Router();


async function getUserID(email: string): Promise<string> {
  let query = `
    SELECT id
    FROM notification_register
    WHERE email = $1 AND ACTIVE = TRUE;
  `;

  let data = await apiDB.query(query, [email], async (cursor: Cursor) => {
    return await cursor.read(1);
  }, { rowMode: "array" });

  let id = null;
  if(data.length > 0) {
    id = data[0][0]
  }
  return id;
}


router.post("/notify/emails", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {
    const { recepients, source, type, message } = req.body;

    if(!Array.isArray(recepients) || recepients.length < 1) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send(
        `Request body should be JSON with the fields:
        recepients: An array of email addresses to send the notification to.
        source (optional): The source of the notification.
        type (optional): The notification type (e.g. Error, Info, etc.).
        message (optional): The notification message.`
      );
    }

    let mailOptions = {
      to: recepients,
      subject: `HCDP Notifier: ${type}`,
      text: `${type}\nNotification source: ${source}\nNotification message: ${message}`,
      html: `<h3>${type}</h3><p>Notification source: ${source}</p><p>Notification message: ${message}</p>`
    };
    try {
      //attempt to send email to the recepients list
      let emailStatus = await sendEmail(mailOptions);
      //if email send failed throw error for logging
      if(!emailStatus.success) {
        throw emailStatus.error;
      }
    }
    //if error while sending admin email write to stderr
    catch(e) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 500;

      return res.status(500)
      .send(
        `The notification could not be sent. Error: ${e}`
      );
    }
    reqData.code = 200;
    return res.status(200)
    .send("Success! A notification has been sent to the requested recepients.");
  });
});



router.post("/notify/subscriptions", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {
    const { source, reason, type } = req.body;
  });
});

router.post("/notify/subscribe", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {
    const { email, source, reason, type } = req.body;

    let id = uuidv4()
    const timestamp = new Date().toISOString();

    let query = `
      INSERT INTO notification_register
      VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
      ON CONFLICT (email, source, reason, type) DO NOTHING;
    `;

    let modified = await apiDB.queryNoRes(query, [id, email, source, reason, type, timestamp, timestamp]);

    if(!modified) {
      reqData.success = false;
      reqData.code = 400;

      return res.status(400)
      .send("");
    }

    id = await getUserID(email);
    reqData.code = 200;
    return res.status(200)
    .json({userID: id});
  });
});

router.patch("/notify/subscription/:id/unsubscribe", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {

  });
});

router.patch("/notify/subscription/:id/delay", async (req, res) => {
  const permission = "notify";
  await handleReq(req, res, permission, async (reqData) => {

  });
});