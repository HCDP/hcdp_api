import pgPromise from "pg-promise";
import Cursor from "pg-cursor";
import pg from "pg";
import moment from "moment-timezone";

const pgTypes = pg.types;
pgTypes.setTypeParser(1114, function(timestamp: string) {
    return moment.tz(timestamp, "UTC").toISOString();
});

export interface Credentials {
    username: string,
    password: string
}

export interface QueryOptions {
    privileged?: boolean,
    rowMode?: "array" | undefined
}

class QueryHandler {
    conn: any;
    cursor: any;

    constructor(conn: any, cursor: any) {
        this.conn = conn;
        this.cursor = cursor;
    }

    async read(entries: number): Promise<any[]> {
        let rows = await this.cursor.read(entries);
        return rows;
    }

    async close() {
        this.cursor.close(this.conn.done);
    }
}

export class HCDPDBManager {
    userDBHandler: any;
    adminDBHandler: any;
    pgp: any;

    constructor(host: string, port: number, database: string, userCredentials: Credentials, adminCredentials: Credentials) {
        this.pgp = pgPromise();

        this.userDBHandler = this.pgp({
            host: host,
            port: port,
            database: database,
            user: userCredentials.username,
            password: userCredentials.password,
            max: 40
        });

        this.adminDBHandler = this.pgp({
            host: host,
            port: port,
            database: database,
            user: adminCredentials.username,
            password: adminCredentials.password,
            max: 40
        });
    }

    async query(query: string, params: string[], options: QueryOptions = { privileged: false }) {
        const conn = await (options.privileged ? this.adminDBHandler : this.userDBHandler).connect();

        const cursor = conn.client.query(new Cursor(query, params, {
            rowMode: options.rowMode
        }));
        return new QueryHandler(conn, cursor);
    }

    async queryNoRes(query: string, params: string[], options: QueryOptions = { privileged: false }) {
        return (options.privileged ? this.adminDBHandler : this.userDBHandler).query(query, params);
    }

    mogrify(query, params): string {
        return this.pgp.as.format(query, params);
    }
}