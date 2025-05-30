import { databaseConnections } from "../../util/config.js";
import { PostgresDBManager } from "../../postgresDBManager.js";

const dbManagers: {[key: string]: PostgresDBManager} = {};

for(let key in databaseConnections) {
  const { host, port, db, username, password, connections } = databaseConnections[key];
  dbManagers[key] = new PostgresDBManager(host, port, db, username, password, connections);
}

export const { mesonetDBUser, mesonetDBAdmin, apiDB } = dbManagers;
