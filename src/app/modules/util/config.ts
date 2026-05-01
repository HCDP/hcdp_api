
import * as fs from "fs";
import { join } from "path";

const config = JSON.parse(fs.readFileSync("../assets/config.json", "utf8"));

export const dataRoot = config.dataRoot;
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

export const { port, smtp, smtpPort, emailConfig, defaultZipName, urlRoot, logDir, administrators, databaseConnections, tapisV3Config, githubWebhookSecret, licenseFile } = config;

export const mesonetLocations = ["hawaii", "american_samoa"];
export const dataPortalLocations = ["hawaii", "american_samoa", "guam"] as const;
export type DataPortalLocation = typeof dataPortalLocations[number];

export const apiURL = "https://api.hcdp.ikewai.org";

//gmail attachment limit
export const ATTACHMENT_MAX_MB = 25;