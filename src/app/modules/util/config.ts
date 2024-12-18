
import * as fs from "fs";

const config = JSON.parse(fs.readFileSync("../assets/config.json", "utf8"));

export const dataRoot = config.dataRoot;

const rawDataDir = config.rawDataDir;
const downloadDir = config.downloadDir;
const productionDir = config.productionDir;
const licensePath = config.licenseFile;

export const rawDataRoot = `${dataRoot}${rawDataDir}`;
export const downloadRoot = `${dataRoot}${downloadDir}`;
export const productionRoot = `${dataRoot}${productionDir}`;
export const licenseFile = `${dataRoot}${licensePath}`;

export const port = config.port;
export const smtp = config.smtp;
export const smtpPort = config.smtpPort;
export const mailConfig = config.email;
export const defaultZipName = config.defaultZipName;
export const urlRoot = config.urlRoot;
export const userLog = config.userLog;
export const administrators = config.administrators;
export const tapisDBConfig = config.tapisDBConfig;
export const hcdpDBConfig = config.hcdpDBConfig;
export const tapisConfig = config.tapisConfig;
export const tapisV3Config = config.tapisV3Config;
export const githubWebhookSecret = config.githubWebhookSecret;

export const apiURL = "https://api.hcdp.ikewai.org";

const keyFile = "../assets/privkey.pem";
const certFile = "../assets/fullchain.pem";
export const hskey = fs.readFileSync(keyFile);
export const hscert = fs.readFileSync(certFile);

//gmail attachment limit
export const ATTACHMENT_MAX_MB = 25;