import express from "express";
import compression from "compression";
import cors from "cors";
import * as https from "https";
import sslRootCAs from "ssl-root-cas";
import { hskey, hscert, port } from "./modules/util/config.js";
import { tapisV3Manager } from "./modules/util/resourceManagers/tapis.js";
import { router as r1 } from "./endpoints/admin/tapisDB/db.js";
import { router as r2 } from "./endpoints/mesonet/db/db.js";
import { router as r3 } from "./endpoints/mesonet/tapis/tapis.js";
import { router as r4 } from "./endpoints/admin/tokens/tokens.js";
import { router as r5 } from "./endpoints/admin/misc/misc.js";
import { router as r6 } from "./endpoints/hcdp/misc/misc.js";
import { router as r7 } from "./endpoints/mesonet/raw/raw.js";
import { router as r8 } from "./endpoints/hcdp/packageGen/packageGen.js";
import { router as r9 } from "./endpoints/hcdp/datasets/dates/dates.js";

//add timestamps to output
import consoleStamp from 'console-stamp';
consoleStamp(console);

const routers = [r1, r2, r3, r4, r5, r6, r7, r8, r9];

//process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";
process.env["NODE_ENV"] = "production";

////////////////////////////////
//////////server setup//////////
////////////////////////////////

const app = express();

app.options('*', cors());
sslRootCAs.inject();

let options = {
    key: hskey,
    cert: hscert
};

const server = https.createServer(options, app)
.listen(port, () => {
  console.log("Server listening at port " + port);
});

app.use(express.json());
//compress all HTTP responses
app.use(compression());

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST");
  res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization, Range, Content-Range, Cache-Control");
  //pass to next layer
  next();
});

////////////////////////////////
////////////////////////////////

/////////////////////////////
///////signal handling///////
/////////////////////////////

const signals = {
  "SIGHUP": 1,
  "SIGINT": 2,
  "SIGTERM": 15
};

function shutdown(code) {
  tapisV3Manager.close();
  //stops new connections and completes existing ones before closing
  server.close(() => {
    console.log(`Server shutdown.`);
    process.exit(code);
  });
}

for(let signal in signals) {
  let signalVal = signals[signal];
  process.on(signal, () => {
    console.log(`Received ${signal}, shutting down server...`);
    shutdown(128 + signalVal);
  });
}

for(let router of routers) {
  app.use("/", router);
}