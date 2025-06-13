import { databaseConnections } from "../../util/config.js";
import { PostgresDBManager } from "../../postgresDBManager.js";
import { PostgresStore } from "@acpr/rate-limit-postgresql";

const dbManagers: {[key: string]: PostgresDBManager} = {};
const postgresStores: {[key: string]: PostgresStore} = {};

for(let key in databaseConnections) {
  const { host, port, db, user, password, connections } = databaseConnections[key];
  dbManagers[key] = new PostgresDBManager(host, port, db, user, password, connections);

  if(key == "apiDB") {
    let dbConfig = {
      user,
      password,
      host,
      database: "rate_limit",
      port
    }
    postgresStores.pgStoreSlowAll = new PostgresStore(dbConfig, "slow_all");
    postgresStores.pgStoreLimitAll = new PostgresStore(dbConfig, "limit_all");
    postgresStores.pgStoreSlowMesonetMeasurements = new PostgresStore(dbConfig, "slow_meso_measurements");
    postgresStores.pgStoreMesonetEmail = new PostgresStore(dbConfig, "limit_meso_email");
  }
}

export const { mesonetDBUser, mesonetDBAdmin, apiDB } = dbManagers;
export const { pgStoreSlowAll, pgStoreLimitAll, pgStoreSlowMesonetMeasurements, pgStoreMesonetEmail } = postgresStores;
