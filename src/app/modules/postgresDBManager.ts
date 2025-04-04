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

export class PostgresDBManager {
    userDBHandler: any;
    adminDBHandler: any;
    pgp: any;
    queryTimeout: number;

    constructor(host: string, port: number, database: string, userCredentials: Credentials, adminCredentials: Credentials, userCons: number, adminCons: number) {
        this.pgp = pgPromise();
        //move to config?
        this.queryTimeout = 12000;

        this.userDBHandler = this.pgp({
            host: host,
            port: port,
            database: database,
            user: userCredentials.username,
            password: userCredentials.password,
            max: userCons
        });

        this.adminDBHandler = this.pgp({
            host: host,
            port: port,
            database: database,
            user: adminCredentials.username,
            password: adminCredentials.password,
            max: adminCons
        });
    }

    async query(query: string, params: string[], options: QueryOptions = { privileged: false }): Promise<QueryHandler> {
        let conn: any = null;
        let cursor: any = null;
        try {
            let handler = (options.privileged ? this.adminDBHandler : this.userDBHandler);
            const conn = await handler.connect();
            cursor = conn.client.query(new Cursor(query, params, {
                rowMode: options.rowMode
            }));
        }
        catch(e) {
            if(cursor) {
                cursor.close(conn.done);
            }
            else if(conn) {
                conn.done();
            }
            throw e;
        }
        return new QueryHandler(conn, cursor);
    }

    async queryNoRes(query: string, params: string[], options: QueryOptions = { privileged: false }): Promise<number> {
        return (options.privileged ? this.adminDBHandler : this.userDBHandler).result(query, params, (r: any) => { return r.rowCount; });
    }

    mogrify(query, params): string {
        return this.pgp.as.format(query, params);
    }
}