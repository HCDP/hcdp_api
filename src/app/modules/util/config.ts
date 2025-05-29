
import * as fs from "fs";
import { join } from "path";

const config = JSON.parse(fs.readFileSync("../assets/config.json", "utf8"));

export const dataRoot = config.dataRoot;

const rawDataDir = config.rawDataDir;
const downloadDir = config.downloadDir;
const productionDir = config.productionDir;
const licensePath = config.licenseFile;

export const rawDataRoot = join(dataRoot, rawDataDir);
export const downloadRoot = join(dataRoot, downloadDir);
export const productionRoot = join(dataRoot, productionDir);
export const licenseFile = join(dataRoot, licensePath);

export const fsHealthData = config.fsHealth;
fsHealthData.file = join(dataRoot, fsHealthData.file);

export const port = config.port;
export const smtp = config.smtp;
export const smtpPort = config.smtpPort;
export const mailConfig = config.email;
export const defaultZipName = config.defaultZipName;
export const urlRoot = config.urlRoot;
export const userLog = config.userLog;
export const administrators = config.administrators;
export const tapisDBConfig = config.tapisDBConfig;
export const databaseConnections = config.databaseConnections;
export const tapisConfig = config.tapisConfig;
export const tapisV3Config = config.tapisV3Config;
export const githubWebhookSecret = config.githubWebhookSecret;

export const mesonetLocations = ["american_samoa", "hawaii"];

export const apiURL = "https://api.hcdp.ikewai.org";

const keyFile = "../assets/privkey.pem";
const certFile = "../assets/fullchain.pem";
export const hskey = fs.readFileSync(keyFile);
export const hscert = fs.readFileSync(certFile);

//gmail attachment limit
export const ATTACHMENT_MAX_MB = 25;