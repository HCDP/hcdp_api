
import { tapisDBConfig, tapisConfig } from "../../util/config.js";
import { DBManager, TapisManager } from "../../../modules/tapisHandlers.js";

export const tapisDBManager = new DBManager(tapisDBConfig.server, tapisDBConfig.port, tapisDBConfig.username, tapisDBConfig.password, tapisDBConfig.db, tapisDBConfig.collection, tapisDBConfig.connectionRetryLimit, tapisDBConfig.queryRetryLimit);
export const tapisManager = new TapisManager(tapisConfig.tenantURL, tapisConfig.token, tapisDBConfig.queryRetryLimit, tapisDBManager);