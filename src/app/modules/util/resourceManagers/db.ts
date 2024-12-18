import { hcdpDBConfig } from "../../util/config.js";
import { PostgresDBManager } from "../../postgresDBManager.js";

export const MesonetDBManager = new PostgresDBManager(hcdpDBConfig.host, hcdpDBConfig.port, "mesonet", hcdpDBConfig.userCredentials, hcdpDBConfig.adminCredentials);
export const HCDPDBManager = new PostgresDBManager(hcdpDBConfig.host, hcdpDBConfig.port, "hcdp", hcdpDBConfig.userCredentials, hcdpDBConfig.adminCredentials);

