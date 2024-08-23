import { Writable } from "stream";

import { v4 as uuidv4 } from "uuid";
import * as fs from "fs";
import * as path from "path";

export class MesonetDataPackager {
    packageDir: string;
    combine: boolean;
    ftype: "json" | "csv";
    csvMode: "matrix" | "table";
    files: string[];
    varData: any;
    stationData: any;
    handle: PackageFileHandle | undefined;
    stationID: string | undefined;

    constructor(root, varData, stationData, combine, ftype, csvMode) {
        const uuid = uuidv4();
        const packageDir = path.join(root, uuid);
        this.packageDir = packageDir;
        this.combine = combine === undefined ? false : combine;
        this.ftype = ftype || "csv";
        this.csvMode = csvMode || "matrix";
        this.files = [];
        this.varData = varData;
        this.stationData = stationData;

        if(!fs.existsSync(packageDir)) {
            fs.mkdirSync(packageDir, { recursive: true });
        }
    }

    async createHandle(fname, varData) {
        let outFile = path.join(this.packageDir, fname);
        if(this.handle) {
            await this.handle.complete();
        }
        if(this.ftype === "json") {
            this.handle = new JSONFileHandle(outFile);
        }
        else {
            this.handle = this.csvMode == "table" ? new CSVTableFileHandle(outFile, varData, this.stationData) : new CSVMatrixFileHandle(outFile, varData, this.stationData);
        }
        
        this.files.push(fname);
    }

    async write(stationID, measurements) {
        if(!this.combine && (this.stationID === undefined || this.stationID != stationID)) {
            this.stationID = stationID;
            let fname = `${stationID}.${this.ftype}`;
            await this.createHandle(fname, this.varData[stationID] || {});
        }
        else if(this.handle === undefined) {
            let fname = `data.${this.ftype}`;
            await this.createHandle(fname, this.varData);
        }
        return this.handle!.write(stationID, measurements);
    }

    async complete() {
        if(this.handle) {
            await this.handle.complete();
        }
        return this.files;
    }
}

abstract class PackageFileHandle {
    stream: Writable;
    ready: Promise<void>;

    constructor(outFile: string) {
        this.stream = fs.createWriteStream(outFile);
        
        this.ready = new Promise((resolve) => {
            this.stream.once("open", (fd) => {
                resolve();
            });
        });
    }

    abstract write(stationID: string, measurements: any): Promise<void>;

    public async complete(): Promise<void> {
        await this.close();
    }

    protected async writeData(data) {
        this.ready = this.ready.then(() => {
            if(!this.stream.write(data)) {
                return new Promise((resolve) => {
                    this.stream.once("drain", () => {
                        resolve();
                    });
                });
            }
            return;
        });
    }

    protected async close(final?): Promise<void> {
        await this.ready;
        return new Promise((resolve) => {
            this.stream.end(final, () => {
                resolve();
            });
        })
    }
}


class JSONFileHandle extends PackageFileHandle {
    first: boolean;
    measurements: any;
    currentStationID: string | undefined;

    constructor(outFile) {
        super(outFile);
        this.first = true;
        super.writeData("{");
    }

    //needs to be provided in chunks of same id to be combined
    async write(stationID: string, measurements) {
        if(stationID === this.currentStationID) {
            this.addMeasurements(measurements);
        }
        else {
            if(this.measurements) {
                await this.dumpStationMeasurements(this.currentStationID);
            }
            this.currentStationID = stationID;
            this.measurements = measurements;
        }
    }

    async complete() {
        if(this.measurements) {
            this.dumpStationMeasurements(this.currentStationID);
        }
        return this.close("}");
    }

    private dumpStationMeasurements(stationID) {
        let measurements = {};
        measurements[stationID] = this.measurements;
        let data = JSON.stringify(measurements, null, 4);
        data = data.slice(1, -1);
        data.replace("\n", "\n    ");
        data = "    " + data;
        if(!this.first) {
            data = "," + data
        }
        this.first = false;
        return super.writeData(data);
    }

    private addMeasurements(measurements) {
        for(let variable in measurements) {
            if(this.measurements[variable] === undefined) {
                this.measurements[variable] = {};
            }
            for(let timestamp in measurements[variable]) {
                this.measurements[variable][timestamp] = measurements[variable][timestamp];
            }
        }
    }
}

class CSVMatrixFileHandle extends PackageFileHandle {
    stationData: any;
    varData: any;
    varIDs: string[];

    constructor(outFile: string, varData, stationData) {
        super(outFile);
        this.stationData = stationData;
        this.varData = varData;
        this.varIDs = Object.keys(this.varData);
        this.writeHeader();
    }

    async writeHeader() {

        let varNames = this.varIDs.map((id) => {
          return this.varData[id].var_name;
        });
        let units = this.varIDs.map((id) => {
          return this.varData[id].unit;
        });
        super.writeData(`,,,"${this.varIDs.join("\",\"")}"\n`);
        super.writeData(`,,,"${units.join("\",\"")}"\n`);
        super.writeData(`station_name,station_id,timestamp,"${varNames.join("\",\"")}"\n`);
    }

    async write(stationID, measurements) {
        let stationName = this.stationData[stationID].site_name;
        let rows: {[timestamp: string]: string[]} = {};
        for(let i = 0; i < this.varIDs.length; i++) {
            let variable = this.varIDs[i];
            let values = measurements[variable] || {};
            for(let timestamp in values) {
                let row = rows[timestamp];
                if(row === undefined) {
                    row = [stationName, stationID, timestamp].concat(new Array(this.varIDs.length).fill("NA"));
                    rows[timestamp] = row;
                }
                let value = values[timestamp];
                if(value === null) {
                    row[i + 3] = "NA";
                }
                else {
                    row[i + 3] = value.toString();
                }
            }
        }
        let rowData = Object.values(rows).sort((a, b) => {
            return a[2] < b[2] ? -1 : 1;
        });
        for(let row of rowData) {
            await super.writeData(`"${row.join("\",\"")}"\n`);
        }
    }
}

class CSVTableFileHandle extends PackageFileHandle {
    stationData: any;
    varData: any;

    constructor(outFile, varData, stationData) {
        super(outFile);
        this.stationData = stationData;
        this.varData = varData;
        this.writeHeader();
    }

    async writeHeader() {
        super.writeData("station_name,station_id,timestamp,variable_name,variable_id,unit,value\n");
    }

    async write(stationID, measurements) {
        let data: string[][] = [];
        for(let variable in measurements) {
            for(let timestamp in measurements[variable]) {
                let value = measurements[variable][timestamp];
                if(value === null) {
                    value = "NA";
                }
                data.push([this.stationData[stationID].site_name, stationID, timestamp, this.varData[variable].var_name, variable, this.varData[variable].unit, value]);
            }
        }
        data.sort((a, b) => {
            return a[2] < b[2] ? -1 : 1;
        });
        for(let row of data) {
            await super.writeData(`"${row.join("\",\"")}"\n`);
        }
    }
}