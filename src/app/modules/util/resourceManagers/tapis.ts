
import { tapisDBConfig, tapisConfig, tapisV3Config } from "../../../modules/util/config";
import { DBManager, TapisManager, TapisV3Manager, ProjectHandler } from "../../../modules/tapisHandlers.js";

export const tapisDBManager = new DBManager(tapisDBConfig.server, tapisDBConfig.port, tapisDBConfig.username, tapisDBConfig.password, tapisDBConfig.db, tapisDBConfig.collection, tapisDBConfig.connectionRetryLimit, tapisDBConfig.queryRetryLimit);
export const tapisManager = new TapisManager(tapisConfig.tenantURL, tapisConfig.token, tapisDBConfig.queryRetryLimit, tapisDBManager);
export const tapisV3Manager = new TapisV3Manager(tapisV3Config.username, tapisV3Config.password, tapisV3Config.tenantURL, tapisDBConfig.queryRetryLimit, tapisManager);