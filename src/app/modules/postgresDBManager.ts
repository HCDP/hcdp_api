import pgPromise from "pg-promise";
import Cursor from "pg-cursor";
import pg from "pg";
import moment from "moment-timezone";

const pgTypes = pg.types;
pgTypes.setTypeParser(1114, function(timestamp: string) {
    return moment.tz(timestamp, "UTC").toISOString();
});


export interface QueryOptions {
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
    private dbHandler: any;
    private pgp: any;

    constructor(host: string, port: number, database: string, user: string, password: string, connections: number) {
        this.pgp = pgPromise();

        this.dbHandler = this.pgp({
            host,
            port,
            database,
            user,
            password,
            max: connections
        });
    }

    async query(query: string, params: string[], options: QueryOptions = {}): Promise<QueryHandler> {
        let conn: any = null;
        let cursor: any = null;
        try {
            conn = await this.dbHandler.connect();
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

    async queryNoRes(query: string, params: string[]): Promise<number> {
        return this.dbHandler.result(query, params, (r: any) => { return r.rowCount; });
    }

    mogrify(query: string, params: string[]): string {
        return this.pgp.as.format(query, params);
    }
}