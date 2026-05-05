import { DataPortalLocation } from './util/config.js';
import { deepEqual, TwoWayMap } from './util/util.js';
import fetchRetry from 'fetch-retry';
const rfetch = fetchRetry(fetch);

const TAPIS_MAX_PAGE_SIZE = 1000;

export type TapisMetadataDocument = { name: string, value: TapisMetadataValue };
export type TapisMetadataValue = { [field: string]: string | number };
export type HCDPTapisMetadataType = "value" | "metadata";


export class HCDPTapisMetadataKeyError extends Error {
    constructor(message: string) {
        super(message);
        this.name = this.constructor.name;
        Error.captureStackTrace(this, this.constructor); 
    }
}

export class HCDPTapisMetadataTypeError extends Error {
    constructor(message: string) {
        super(message);
        this.name = this.constructor.name;
        Error.captureStackTrace(this, this.constructor); 
    }
}

// mimic tapis error class
export class TapisHttpError extends Error {
    "http status code": number;

    constructor(statusCode: number, message: string) {
        super(message);
        this.name = this.constructor.name;
        this["http status code"] = statusCode;
        
        // Maintains proper stack trace
        Error.captureStackTrace(this, this.constructor); 
    }
}

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
        let data = await TapisV3Manager.handleTapisResponse(response);
        let tokenData = data.result.access_token;
        let { access_token, expires_in } = tokenData;
        let reauthTime = expires_in < 60 ? expires_in * 5 * 1000 : (expires_in - (5 * 60)) * 1000;
        this.reauthTimer = setTimeout(() => {
            this.token = this.auth();
        }, reauthTime).unref();
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

        let data = await TapisV3Manager.handleTapisResponse(response);
        return data;
    }



    public async createDocs(data: TapisMetadataDocument[], keyFields: Set<string>, db: string, collection: string, replace: boolean = true) {        
        let replaceDocs: { [key: string]: any } = {};
        let createDocsList: any[] = [];

        // Check for duplicates concurrently
        const duplicateTasks = data.map(doc => this.checkDuplicate(doc, keyFields, db, collection, replace));
        const duplicateData = await Promise.all(duplicateTasks);

        for(const { doc, uuid, action } of duplicateData) {
            if(action === "replace" && uuid) {
                replaceDocs[uuid] = doc;
            }
            else if (action === "create") {
                createDocsList.push(doc);
            }
        }

        const createTasks = Object.keys(replaceDocs).map(uuid => this.replaceDocument(uuid, replaceDocs[uuid], db, collection));
        if(createDocsList.length > 0) {
            createTasks.push(this.createDocsUnsafe(createDocsList, db, collection));
        }
        await Promise.all(createTasks);
        return {
            replaced: Object.keys(replaceDocs).length,
            created: createDocsList.length
        };
    }


    public async createDocsUnsafe(data: TapisMetadataDocument | TapisMetadataDocument[], db: string, collection: string) {
        const token = await this.authManager.getToken();
        const url = `${this.url}/meta/${db}/${collection}`;
        const headers = {
            "X-Tapis-Token": token,
            "Content-Type": "application/json"
        };
        // if only one item in array move out of array
        if(Array.isArray(data) && data.length == 1) {
            data = data[0];
        }

        if(Array.isArray(data)) {
            // Bulk ingest up to 500 docs at a time
            const chunkSize = 500;
            for (let i = 0; i < data.length; i += chunkSize) {
                const chunk = data.slice(i, i + chunkSize);
                let response = await rfetch(url, {
                    method: 'POST',
                    headers,
                    body: JSON.stringify(chunk),
                    retries: this.retryLimit,
                    retryDelay: (attempt) => Math.pow(2, attempt) * 1000
                });
                await TapisV3Manager.handleTapisResponse(response);
            }
        }
        else {
            let response = await rfetch(url, {
                method: "POST",
                headers,
                body: JSON.stringify(data),
                retries: this.retryLimit,
                retryDelay: (attempt) => Math.pow(2, attempt) * 1000
            });
            await TapisV3Manager.handleTapisResponse(response);
        }
    }

    public async replaceDocument(uuid: string, data: TapisMetadataDocument, db: string, collection: string) {
        const token = await this.authManager.getToken();
        const url = `${this.url}/meta/${db}/${collection}/${uuid}`;
        const headers = {
            "X-Tapis-Token": token,
            "Content-Type": "application/json"
        };

        let response = await rfetch(url, {
            method: "PUT",
            headers,
            body: JSON.stringify(data),
            retries: this.retryLimit,
            retryDelay: (attempt) => Math.pow(2, attempt) * 1000
        });
        await TapisV3Manager.handleTapisResponse(response);
    }


    private async checkDuplicate(doc: TapisMetadataDocument, keyFields: Set<string>, db: string, collection: string, replace: boolean = true) {
        let keyData: { [field: string]: any } = {
            name: doc.name
        };

        for (const field of keyFields) {
            keyData[`value.${field}`] = doc.value[field];
        }

        const matches = await this.queryMetadata(keyData, db, collection);

        let uuid: string | null = null;
        let action: "create" | "replace" | "skip" = "skip";

        if(matches.length > 1) {
            throw new TapisHttpError(500, "Multiple entries match the specified key data");
        }
        else if(matches.length > 0 && replace && !deepEqual(matches[0].value, doc.value)) {
            // parse Tapis V3 OID format
            uuid = matches[0]._id?.$oid || matches[0]._id;
            action = "replace";
        }
        else if(matches.length === 0) {
            action = "create";
        }

        return { doc, uuid, action };
    }
}


export class TapisV3Manager {
    private authManager: TapisV3AuthManager;
    public meta: TapisV3MetadataHandler;

    constructor(retryLimit: number, url: string, username: string, password: string) {
        this.authManager = new TapisV3AuthManager(url, username, password);
        this.meta = new TapisV3MetadataHandler(retryLimit, url, this.authManager);
    }

    public static async handleTapisResponse(response: Response) {
        const contentType = response.headers.get("content-type");

        // if the response failed (4xx or 5xx) process error and throw
        if (!response.ok) {
            let errorMessage = `Tapis API Error: ${response.status} ${response.statusText}`;
            
            if (contentType && contentType.includes("application/json")) {
                const errorBody = await response.json();
                errorMessage = errorBody.message || errorMessage; 
            }
            else {
                // If it's an HTML page (like a 502 Bad Gateway), grab the text
                const textBody = await response.text();
                errorMessage = textBody || errorMessage;
            }

            throw new TapisHttpError(response.status, errorMessage);
        }

        // if the response succeeded, ensure it's actually JSON
        if(contentType && contentType.includes("application/json")) {
            return await response.json();
        }
        // if not JSON throw 500
        else {
            throw new TapisHttpError(500, "Expected JSON response from Tapis, but received a different format.");
        }
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

    private getDefaultKeyFields(type: HCDPTapisMetadataType): string[] {
        let keyFields: string[];
        if(type == "metadata") {
            keyFields = ["station_group", "skn"];
        }
        else if(type == "value") {
            keyFields = ["station_id", "datatype", "period", "date", "fill"];
        }
        else {
            throw new HCDPTapisMetadataTypeError(`Invalid metadata type ${type} provided.`);
        }
        return keyFields;
    }

    private validateDataKeys(values: TapisMetadataValue[], keyFields: Set<string>): void {
        for(let value of values) {
            for(let key of keyFields) {
                if(!value[key]) {
                    throw new HCDPTapisMetadataKeyError(`Value does not include key value ${key} or the value is invalid`);
                }
            }
        }
    }

    public async createMetadata(location: DataPortalLocation, type: HCDPTapisMetadataType, values: TapisMetadataValue[], additionalKeyFields: string[] = [], replace: boolean = true) {
        let cdp = this.locationCdpTranslation.lookup(location);
        let name = `${cdp}_station_${type}`;
        let docs = values.map((value: { [field: string]: string | number }) => {
            let doc = {
                name,
                value
            };
            return doc;
        });
        let collection = `${location}_stations`;
        let defaultKeys = this.getDefaultKeyFields(type);
        let allKeyFields = new Set([...defaultKeys, ...additionalKeyFields]);
        this.validateDataKeys(values, allKeyFields);
        return await this.tapisManager.meta.createDocs(docs, allKeyFields, this.database, collection, replace);
    }

    public async queryMetadata(location: DataPortalLocation, type: HCDPTapisMetadataType, values: { [field: string]: string }, limit?: number, offset?: number) {
        let queryValues: { [field: string]: any } = values;
        if(type === "value") {
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

    public async queryMetadataRaw(query: any, limit?: number, offset?: number) {
        //workaround for extracting location from legacy query style
        const nameRegex = /"name":"(.+?cdp)_station_/;
        let match = JSON.stringify(query).match(nameRegex);
        if(!match) {
            throw new TapisHttpError(400, "Query does not contain the name parameter or the provided name is invalid");
        }
        let cdp = match[1];
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