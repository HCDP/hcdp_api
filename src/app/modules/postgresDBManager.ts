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

    async query<T>(query: string, params: string[], processor: (cursor: Cursor) => Promise<T>, options: QueryOptions = {}): Promise<T> {
        let conn: any = null;
        let cursor: Cursor = null;
        let result: T = null;
        try {
            conn = await this.dbHandler.connect();
            cursor = conn.client.query(new Cursor(query, params, {
                rowMode: options.rowMode
            }));
            result = await processor(cursor);
        }
        catch(e) {
            throw e;
        }
        finally {
            if(cursor) {
                await new Promise<void>((resolve) => {
                    cursor.close((err: any) => {
                        if(err) {
                            console.error(`Error closing cursor: ${err}`);
                        }
                        if(conn) {
                            conn.done(Boolean(err));
                        }
                        resolve();
                    });
                });
            }
            else if(conn) {
                conn.done();
            }
        }
        return result;
    }

    async queryNoRes(query: string, params: string[]): Promise<number> {
        return this.dbHandler.result(query, params, (r: any) => { return r.rowCount; });
    }

    mogrify(query: string, params: string[]): string {
        return this.pgp.as.format(query, params);
    }
}