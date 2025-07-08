
import * as fs from "fs";
import { join } from "path";

const config = JSON.parse(fs.readFileSync("../assets/config.json", "utf8"));

export const dataRoot = config.dataRoot;

//LICENSE FILE NEEDS TO BE MODIFIED IN USAGE
const { rawDataDir, downloadDir, productionData } = config;

export const productionLocations = Object.keys(productionData);
export const productionDirs = {};
for(let location in productionData) {
  productionDirs[location] = join(dataRoot, productionData[location])
}

export const rawDataRoot = join(dataRoot, rawDataDir);
export const downloadRoot = join(dataRoot, downloadDir);

export const fsHealthData = config.fsHealth;
fsHealthData.file = join(dataRoot, fsHealthData.file);

export const { port, smtp, smtpPort, emailConfig, defaultZipName, urlRoot, userLog, administrators, tapisDBConfig, databaseConnections, tapisConfig, githubWebhookSecret, licenseFile } = config;

export const mesonetLocations = ["american_samoa", "hawaii"];

export const apiURL = "https://api.hcdp.ikewai.org";

const { key, cert } = config.certFiles;
export const hskey = fs.readFileSync(key);
export const hscert = fs.readFileSync(cert);

//gmail attachment limit
export const ATTACHMENT_MAX_MB = 25;