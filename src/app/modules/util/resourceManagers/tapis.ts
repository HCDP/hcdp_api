
import { tapisV3Config } from "../../util/config.js";
import { TapisV3Manager, HCDPStationTapisMetadataHelper } from "../../tapisv3.js";

const { retryLimit, url, username, password, db } = tapisV3Config;

const tapisV3Manager: TapisV3Manager = new TapisV3Manager(retryLimit, url, username, password);
export const stationMetadataHelper: HCDPStationTapisMetadataHelper = new HCDPStationTapisMetadataHelper(tapisV3Manager, db);