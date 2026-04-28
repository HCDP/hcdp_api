import { TwoWayMap } from './util/util.js';
import fetchRetry from 'fetch-retry';
const rfetch = fetchRetry(fetch);

const TAPIS_MAX_PAGE_SIZE = 1000;

class TapisTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor); 
  }
}

class TapisV3AuthManager {
    private url: string;
    private username: string;
    private password: string;
    private token: Promise<string>;
    private reauthTimer: NodeJS.Timeout | null;

    constructor(url: string, username: string, password: string) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.reauthTimer = null;
        this.token = this.auth();
    }

    private async auth() {
        const body = {
            username: this.username,
            password: this.password,
            grant_type: "password",

        };

        const headers = {
            "Content-Type": "application/json"
        }

        let response = await rfetch(`${this.url}/oauth2/tokens`, {
            method: 'POST',
            headers,
            body: JSON.stringify(body),
            // attempt forever so not permanently broken
            retries: Number.MAX_VALUE,
            // exponential backoff seconds up to 5 minutes
            retryDelay: (attempt) => Math.min(Math.pow(2, attempt) * 1000, 5 * 60 * 1000)
        });
        let data = await response.json();
        let tokenData = data.result.access_token;
        let { access_token, expires_in } = tokenData;
        let reauthTime = expires_in < 60 ? expires_in * 5 * 1000 : (expires_in - (5 * 60)) * 1000;
        this.reauthTimer = setTimeout(() => {
            this.token = this.auth();
        }, reauthTime);
        return access_token;
    }

    private timeout(ms: number) {
        let cancelled = false;
        let cancel = () => {
            cancelled = true;
        }
        let timeoutPromise = new Promise<string>((accept, reject) => {
            if(cancelled) {
                accept("");
            }
            else {
                let timeout = setTimeout(() => reject(new TapisTimeoutError("Token generation timed out")), ms);
                cancel = () => {
                    clearTimeout(timeout);
                    accept("");
                }
            }
        });

        return {
            timer: timeoutPromise,
            cancel
        }
    }

    async getToken(timeout: number = 60 * 1000) {
        let timeoutHandler = this.timeout(timeout);
        try {
            let token = await Promise.race([timeoutHandler.timer, this.token]);
            return token;
        }
        finally {
            timeoutHandler.cancel();
        }
    }


    end() {
        if(this.reauthTimer) {
            clearTimeout(this.reauthTimer);
        }
    }
}


export class TapisV3MetadataHandler {
    private authManager: TapisV3AuthManager;
    private retryLimit: number;
    private url: string;

    constructor(retryLimit: number, url: string, authManager: TapisV3AuthManager) {
        this.authManager = authManager;
        this.retryLimit = retryLimit;
        this.url = url;
    }

    public async queryMetadata(query: { [field: string]: any }, db: string, collection: string, pagesize: number = TAPIS_MAX_PAGE_SIZE, page: number = 1) {
        const token = await this.authManager.getToken();
        const ep = `${this.url}/meta/${db}/${collection}`;
        
        let params = {
            filter: JSON.stringify(query),
            pagesize: pagesize.toString(),
            page: page.toString()
        }

        const queryString = new URLSearchParams(params).toString();
        const url = `${ep}?${queryString}`;

        let headers = {
            "X-Tapis-Token": token
        };

        let response = await rfetch(url, {
            headers,
            retries: this.retryLimit,
            // exponential backoff seconds
            retryDelay: (attempt) => Math.pow(2, attempt) * 1000
        });

        let data = await response.json();
        return data;
    }
}


export class TapisV3Manager {
    private authManager: TapisV3AuthManager;
    public meta: TapisV3MetadataHandler;

    constructor(retryLimit: number, url: string, username: string, password: string) {
        this.authManager = new TapisV3AuthManager(url, username, password);
        this.meta = new TapisV3MetadataHandler(retryLimit, url, this.authManager);
    }


    close() {
        this.authManager.end();
    }
}




export class HCDPStationTapisMetadataHelper {
    private tapisManager: TapisV3Manager;
    private database: string;
    private locationCdpTranslation: TwoWayMap<string, string>;

    constructor(tapisManager: TapisV3Manager, database: string) {
        this.tapisManager = tapisManager;
        this.database = database;
        this.locationCdpTranslation = new TwoWayMap([
            ["hawaii", "hcdp"],
            ["american_samoa", "ascdp"],
            ["guam", "gcdp"]
        ]);
    }

    public async addMetadata(docs: any[]) {
        throw new Error("Function not implemented");
    }

    public async queryMetadata(location: string, type: "value" | "metadata", values: { [field: string]: string }, limit?: number, offset?: number) {
        let queryValues: { [field: string]: any } = values;
        if( type === "value") {
            let { startDate, endDate, ...params } = values;
            queryValues = { ...params };
            if(startDate || endDate) {
                queryValues.date = {};
                if(startDate) {
                    queryValues.date.$gte = startDate;
                }
                if(endDate) {
                    queryValues.date.$lte = endDate;
                }
            }
        }

        let cdp = this.locationCdpTranslation.lookup(location);
        let name = `${cdp}_station_${type}`;
        let query: { [field: string]: any } = {
            name
        };
        for(let field in queryValues) {
            query[`value.${field}`] = queryValues[field];
        }
        let collection = `${location}_stations`;
        let data = await this.query(query, collection, limit, offset);
        return data;
    }

    public async queryMetadataRaw(query: { [field: string]: any }, limit?: number, offset?: number) {
        //workaround for extracting location from legacy query style
        const nameRegex = /"name":"(.+?cdp)_station_/;
        let cdp = JSON.stringify(query).match(nameRegex)[1];
        let location = this.locationCdpTranslation.reverseLookup(cdp);
        let collection = `${location}_stations`;
        let data = await this.query(query, collection, limit, offset);
        return data;
    }

    // chunk query by TAPIS_MAX_QUERY
    private async query(query: { [field: string]: any }, collection: string, limit: number = Infinity, offset: number = 0) {
        // determine starting page
        let page = Math.floor(offset / TAPIS_MAX_PAGE_SIZE) + 1;
        // get offset in page
        offset %= TAPIS_MAX_PAGE_SIZE;
        let chunk: any[] = await this.tapisManager.meta.queryMetadata(query, this.database, collection, TAPIS_MAX_PAGE_SIZE, page++);
        // if the chunk is not an array something went wrong, just return empty
        if (!Array.isArray(chunk)) {
            return [];
        }
        // offset page
        let data = chunk.slice(offset);

        // if chunk page was full and still below limit get next page and add to data
        while(data.length < limit && chunk.length === TAPIS_MAX_PAGE_SIZE) {
            chunk = await this.tapisManager.meta.queryMetadata(query, this.database, collection, TAPIS_MAX_PAGE_SIZE, page++);
            // if page is not an array or has no data break
            if(!Array.isArray(chunk) || chunk.length === 0) {
                break;
            }
            data.push(...chunk);
        }
        // truncate data to limit
        data.length = Math.min(data.length, limit);
        
        return data;
    }
}