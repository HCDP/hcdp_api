import { sendEmail } from "../../../modules/util/email.js";
import express from "express";
import { requireAuth } from "../../../modules/util/auth.js";

export const router = express.Router();


// async function getUserID(email: string): Promise<string> {
//   let query = `
//     SELECT id
//     FROM notification_register
//     WHERE email = $1 AND ACTIVE = TRUE;
//   `;

//   let data = await apiDB.query(query, [email], async (cursor: Cursor) => {
//     return await cursor.read(1);
//   }, { rowMode: "array" });

//   let id = null;
//   if(data.length > 0) {
//     id = data[0][0]
//   }
//   return id;
// }


router.post("/notify/emails", requireAuth("notify"), async (req, res) => {
  const { recepients, source, type, message } = req.body;

  if(!Array.isArray(recepients) || recepients.length < 1) {
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
    return res.status(500)
    .send(
      `The notification could not be sent. Error: ${e}`
    );
  }
  return res.status(200)
  .send("Success! A notification has been sent to the requested recepients.");
});




router.post("/notify/subscriptions", requireAuth("notify"), async (req, res) => {
  // const { source, reason, type } = req.body;
  res.status(501).send('Not Implemented');
});

router.post("/notify/subscribe", requireAuth("notify"), async (req, res) => {
  return res.status(501).send('Not Implemented');

  // const { email, source, reason, type } = req.body;

  // let id = uuidv4()
  // const timestamp = new Date().toISOString();

  // let query = `
  //   INSERT INTO notification_register
  //   VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
  //   ON CONFLICT (email, source, reason, type) DO NOTHING;
  // `;

  // let modified = await apiDB.queryNoRes(query, [id, email, source, reason, type, timestamp, timestamp]);

  // if(!modified) {
  //   reqData.success = false;
  //   reqData.code = 400;

  //   return res.status(400)
  //   .send("");
  // }

  // id = await getUserID(email);
  // reqData.code = 200;
  // return res.status(200)
  // .json({userID: id});
});

router.patch("/notify/subscription/:id/unsubscribe", requireAuth("notify"), async (req, res) => {
  res.status(501).send('Not Implemented');
});


router.patch("/notify/subscription/:id/delay", requireAuth("notify"), async (req, res) => {
  res.status(501).send('Not Implemented');
});
