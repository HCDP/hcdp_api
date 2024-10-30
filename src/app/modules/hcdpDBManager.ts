import pgPromise from "pg-promise";
import Cursor from "pg-cursor";

export interface Credentials {
    username: string,
    password: string
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

    constructor(host: string, port: number, database: string, userCredentials: Credentials, adminCredentials: Credentials) {
        const pgp = pgPromise();

        this.userDBHandler = pgp({
            host: host,
            port: port,
            database: database,
            user: userCredentials.username,
            password: userCredentials.password,
            max: 40
        });

        this.adminDBHandler = pgp({
            host: host,
            port: port,
            database: database,
            user: adminCredentials.username,
            password: adminCredentials.password,
            max: 40
        });
    }

    async query(query: string, params: string[], privileged: boolean = false) {
        const conn = await (privileged ? this.adminDBHandler : this.userDBHandler).connect();

        const cursor = conn.client.query(new Cursor(query, params));
        return new QueryHandler(conn, cursor);
    }

    async queryNoRes(query: string, params: string[], privileged: boolean = false) {
        return (privileged ? this.adminDBHandler : this.userDBHandler).query(query, params);
    }
}