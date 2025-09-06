import moment, { Moment } from "moment-timezone";
import { productionDirs, productionLocations } from "./util/config.js";
import * as fs from "fs";
import * as path from "path";
import { createTZDateFromString } from "./util/dates.js";

//property hierarchy (followed by file and date parts)
const hierarchy = ["datatype", "production", "aggregation", "period", "lead", "timescale",  "extent", "fill"];
const periodOrder: moment.unitOfTime.StartOf[] = ["year", "month", "day", "hour", "minute", "second"];
const periodFormats = ["YYYY", "MM", "DD", "HH", "mm", "ss"];

//should update everything to use this, for now just use for ds data
const hierarchies = {
    downscaling_rainfall: ["dsm", "season", "period"],
    downscaling_temperature: ["dsm", "period"]
}

export const fnamePattern = /^.+?([0-9]{4}(?:(?:_[0-9]{2}){0,5}|(?:_[0-9]{2}){5}\.[0-9]+))\.[a-zA-Z0-9]+$/;

function getDatasetPath(productionRoot: string, dataset: any): string {
    let datasetPath = productionRoot;
    //add properties to path in order of hierarchy
    for(let property of hierarchy) {
        let value = dataset[property];
        if(value !== undefined) {
            datasetPath = path.join(datasetPath, value);
        }
    }
    return datasetPath;
}

function fillDefaults(dataset: any) {
    let  { location, datatype, lead, extent, units, variable } = dataset;
    if(!productionLocations.includes(location)) {
        dataset.location = "hawaii";
    }
    if(dataset.location == "hawaii" && extent === undefined) {
        dataset.extent = "statewide"
    }
    if(datatype == "ignition_probability" && lead === undefined) {
        dataset.lead = "lead00";
    }
    if(location == "american_samoa" && datatype == "prism_climatology" && units === undefined) {
        if(variable == "rainfall") {
            dataset.units = "mm"
        }
        else if(variable =="air_temeprature") {
            dataset.units = "celcius"
        }
    }
}

export async function getDatasetDateRange(dataset: any): Promise<[string, string] | null> {
    fillDefaults(dataset);
    let productionRoot = productionDirs[dataset.location];
    let datasetPath = getDatasetPath(productionRoot, dataset);
    //use data maps as baseline, might have to modify if dataset with no maps
    datasetPath = path.join(datasetPath, "data_map");
    const descend = (root: string, direction: number): string | null => {
        let dirents = fs.readdirSync(root, {withFileTypes: true}).sort((a, b) => direction * a.name.localeCompare(b.name));
        let dirs = dirents.filter((dirent) => dirent.isDirectory());
        for(let i = 0; i < dirs.length; i++) {
            let dir = path.join(root, dirs[i].name);
            let leaf = descend(dir, direction);
            if(leaf !== null) {
                return leaf;
            }
        }
        let files = dirents.filter((dirent) => dirent.isFile());
        if(files.length > 0) {
            return files[0].name;
        }
        else {
            return null;
        }
    }
    if(!(await validate(datasetPath))) {
        return null;
    }
    let firstFile: string | null = descend(datasetPath, 1);
    let lastFile: string | null = descend(datasetPath, -1);
    if(firstFile === null || lastFile === null) {
        return null;
    }
    firstFile = path.parse(firstFile).name;
    lastFile = path.parse(lastFile).name;
    let parts = periodOrder.indexOf(dataset.period) + 1;
    //alternative
    if(parts < 1) {
        return null;
    }
    let firstDate = firstFile.split("_").slice(-parts).join("-");
    let lastDate = lastFile.split("_").slice(-parts).join("-");
    
    firstDate = createTZDateFromString(dataset.location, firstDate, true).toISOString();
    lastDate = createTZDateFromString(dataset.location, lastDate, true).toISOString();
    return [firstDate, lastDate];
}

//should change to import roots
export async function getDatasetNextDate(dataset: any, date: string, direction: number) {
    fillDefaults(dataset);
    let productionRoot = productionDirs[dataset.location];
    let datasetPath = getDatasetPath(productionRoot, dataset);
    datasetPath = path.join(datasetPath, "data_map");
}

export async function getDatasetNearestDate(dataset: any, date: string, direction: number) {
    fillDefaults(dataset);
    let productionRoot = productionDirs[dataset.location];
    direction = Math.sign(direction);
    let dateMoment = moment(date);
    let datasetPath = getDatasetPath(productionRoot, dataset);
    datasetPath = path.join(datasetPath, "data_map");
    let period = dataset.period;
    let parts = periodOrder.indexOf(period);
    let dateFormatParts = periodFormats.slice(0, parts);
    let format = dateFormatParts.join("_");

    const getMatchingPath = (root: string, depth: number): string | null => {
        let matchingPath: string | null = null;
        let partformat = dateFormatParts[depth];
        let target = dateMoment.format(partformat);
        let dirs = fs.readdirSync(datasetPath);
        if(dirs.includes(target)) {
            matchingPath = path.join(root, target);
        }
        return matchingPath;
    }

    //move to reusable funct
    const descendSide = (root: string, direction: number): string | null => {
        let dirents = fs.readdirSync(root, {withFileTypes: true}).sort((a, b) => direction * a.name.localeCompare(b.name));
        let dirs = dirents.filter((dirent) => dirent.isDirectory());
        for(let i = 0; i < dirs.length; i++) {
            let dir = path.join(root, dirs[i].name);
            let leaf = descendSide(dir, direction);
            if(leaf !== null) {
                return leaf;
            }
        }
        let files = dirents.filter((dirent) => dirent.isFile());
        if(files.length > 0) {
            return path.join(root, files[0].name);
        }
        else {
            return null;
        }
    }

    const getNearestPath = (root: string, depth: number): [string, number] => {
        let nearestPath = "";
        let descendSide = -direction;
        let dirs = fs.readdirSync(root);
        let partformat = dateFormatParts[depth];
        let target = dateMoment.format(partformat);
        dirs.push(target);
        dirs.sort();
        let sortedIndex = dirs.indexOf(target);

        if(sortedIndex == 0) {
            descendSide = -1;
            datasetPath = path.join(root, dirs[1]);
        }
        else if(sortedIndex == dirs.length - 1) {
            descendSide = 1;
            datasetPath = path.join(root, dirs[dirs.length - 2]);
        }
        else {
            datasetPath = path.join(root, dirs[sortedIndex + direction]);
        }
        return [nearestPath, descendSide];
    }

    //handle leaves
    const descendMatch = (root: string, depth: number): Moment | null => {
        //at leaves
        //move this to get matching path to handle overflow if not found
        if(depth == parts) {
            //
            let dirents = fs.readdirSync(root, {withFileTypes: true}).filter((dirent) => dirent.isFile()).sort((a, b) => a.name.localeCompare(b.name));
            let fparse = path.parse(dirents[0].name);
            let dsbase = fparse.name.split("_").slice(0, -parts);
            let targetDateStr = dateMoment.format(format);
            let target = `${dsbase}_${targetDateStr}.${fparse.ext}`;
            let i: number;
            for(i = 0; i < dirents.length; i++) {
                let file = dirents[i].name;
                let relative = target.localeCompare(file);
                if(relative == 0) {
                    return dateMoment;
                }
                //passed the target string, return current file date
                else if(relative < 0) {
                    fparse = path.parse(file);
                    let dateString = fparse.name.split("_").slice(-parts).join("_");
                    return moment(dateString, format);
                }
            }
            //need to return direction
            return null;
        }

        let matchingPath = getMatchingPath(root, depth);
        if(matchingPath) {
            return descendMatch(matchingPath, depth + 1);
        }
        else {
            let [nearestPath, direction] = getNearestPath(root, depth);
            let leaf = descendSide(nearestPath, direction);
            if(leaf === null) {
                return null;
            }
            //extract date from the file name
            let fname = path.parse(leaf).name;
            let dateString = fname.split("_").slice(-parts).join("_");
            let date = createTZDateFromString(dataset.location, dateString);
            return date;
        }
    }

    let nearestDate = descendMatch(datasetPath, 0);
    return nearestDate ? nearestDate.format() : null;
}


export async function getPaths(data: any, collapse: boolean = true) {
    let paths: string[] = [];
    let totalFiles = 0;
    let productionRoot = "";
 
    //maintain compatibility, only convert if new style TEMP
    if(data[0]?.fileData) {
        data = convert(data);
    }
    for(let item of data) {
        //at least for now just catchall and return files found before failure, maybe add more catching/skipping later, or 400?
        try {
            fillDefaults(item);
            productionRoot = productionDirs[item.location];
            //use simplified version for getting ds data
            if(item.datatype == "downscaling_temperature" || item.datatype == "downscaling_rainfall") {
                let files = await getDSFiles(productionRoot, item);
                paths = paths.concat(files);
                totalFiles += files.length;
            }
            else if(item.datatype.endsWith("_climatology")) {
                let files = await getClimatologyFiles(productionRoot, item);
                paths = paths.concat(files);
                totalFiles += files.length;
            }
            else {
                let fdir = productionRoot;
                let range = item.range;
                let ftypes = item.files;
                //add properties to path in order of hierarchy
                for(let property of hierarchy) {
                    let value = item[property];
                    if(value !== undefined) {
                        fdir = path.join(fdir, value);
                    }
                }

                for(let ftype of ftypes) {
                    if(item.datatype == "ignition_probability" && ftype == "metadata") {
                        let metaDir = path.join(fdir, "metadata");
                        let metaFiles = fs.readdirSync(metaDir, {withFileTypes: true}).filter((dirent: fs.Dirent) => dirent.isFile()).map((dirent: fs.Dirent) => path.join(metaDir, dirent.name));
                        paths = paths.concat(metaFiles);
                    }
                    else {
                        let fdirType = path.join(fdir, ftype);
                        let start = moment(range.start);
                        let end = moment(range.end);
                        let pathData = await getPathsBetweenDates(fdirType, start, end, collapse);
                        totalFiles += pathData.numFiles;
                        paths = paths.concat(pathData.paths);
                    }
                } 
            }
        }
        catch(e) {}
    }

    return {
        root: productionRoot,
        numFiles: totalFiles,
        paths
    };
}



//add folder with empty geotiffs for extents
export function getEmpty(location: string, extent?: string) {
    let emptyFile = path.join("/data/empty/", location, `${extent ? extent + "_" : ""}empty.tif`)
    return emptyFile;
}

/////////////////////////////////////////////////
///////////////// helper functs /////////////////
/////////////////////////////////////////////////


function combinations(variants) {
    return (function recurse(keys) {
        if (!keys.length) return [{}];
        let result = recurse(keys.slice(1));
        return variants[keys[0]].reduce( (acc, value) =>
            acc.concat( result.map( item => 
                Object.assign({}, item, { [keys[0]]: value }) 
            ) ),
            []
        );
    })(Object.keys(variants));
} 

//TEMP, convert new style packaged data to old
function convert(data) {
    let converted: any[] = [];
    for(let item of data) {
        for(let fileItem of item.fileData) {
            let files = fileItem.files;
            let expanded = combinations(fileItem.fileParams);
            for(let obj of expanded) {
                let convertedItem = {
                    files,
                    range: {
                        start: item.dates?.start,
                        end: item.dates?.end
                    },
                    ...item.params,
                    ...obj
                };
                converted.push(convertedItem);
            }
        }
    }
    return converted;
}


//validate file or dir exists
async function validate(file: string) {
    return new Promise((resolve, reject) => {
        if(file) {
            fs.access(file, fs.constants.F_OK, (e) => {
                e ? resolve(false) : resolve(true);
            });
        }
        else {
            resolve(false);
        }
    });
}


async function getClimatologyFiles(productionRoot: string, properties: {[tag: string]: string}) {
    let result: string[] = [];
    let {location, datatype, variable, aggregation, extent, mean_type, period, units, date, files} = properties;
    for(let file of files) {
        let fpath: string;
        if(file == "metadata") {
            let fext = location == "hawaii" ? "pdf" : "txt";
            let fname = `${datatype}_${variable}_metadata.${fext}`;
            fpath = path.join(productionRoot, datatype, variable, fname); 
        }
        else if(file == "data_map") {
            if(period === undefined) {
                switch(mean_type) {
                    case "mean_30yr_annual": {
                        let year = moment(date).year();
                        //get 30 year period
                        //offset by -10 to align with 30 year multiple from 0, then add back to realign with climatology period
                        let decadeEnd = Math.ceil((year - 10) / 30) * 30 + 10;
                        let decadeStart = decadeEnd - 29;
                        period = `${decadeStart}-${decadeEnd}`;
                    }
                    case "mean_annual_decadal": {
                        let year = moment(date).year();
                        //get decade
                        let decadeEnd = Math.ceil(year / 10) * 10;
                        let decadeStart = decadeEnd - 9;
                        period = `${decadeStart}-${decadeEnd}`;
                    }
                    case "mean_monthly": {
                        period = moment(date).format("MMMM").toLowerCase();
                    }
                }
            }
            
            let fileParts = [datatype, aggregation, variable, mean_type, extent, period, units].filter(part => part);
            let fname = fileParts.join("_");
            let pathParts = [productionRoot, datatype, variable, aggregation, mean_type, extent, fname].filter(part => part);
            fpath = path.join(...pathParts);
            console.log(fpath);
        }

        if(await validate(fpath)) {
            result.push(fpath);
        }
    }
    
    return result;
}


//expand to allow different units to be grabbed, for now just mm and celcius
async function getDSFiles(productionRoot: string, properties: any) {
    let files: string[] = [];
    let fileTags = properties.files;
    let file_suffix: string;
    let hierarchy = hierarchies[properties.location == "hawaii" ? properties.datatype : "downscaling_rainfall"];
    let values = [properties.datatype];
    let period = properties.period;
    for(let property of hierarchy) {
        let value = properties[property];
        values.push(value);
    }

    ////MAKE THIS MORE COHESIVE////
    let units: string;
    if(properties.units) {
        units = properties.units;
    }
    //defaults
    else if(properties.datatype == "downscaling_rainfall") {
        units = "mm";
    }
    else {
        units = "celcius";
    }
    for(let file of fileTags) {
        if(file == "data_map_change") {
            values.push(properties.model);
            file_suffix = `change_${units}.tif`;
        }
        else if(period != "present") {
            values.push(properties.model);
            file_suffix = `prediction_${units}.tif`;
        }
        else {
            file_suffix = `${units}.tif`;
        }
        let subpath = values.join("/");
        values.push(file_suffix);
        let fname = values.join("_");
        let fpath = path.join(productionRoot, subpath, fname);
        if(await validate(fpath)) {
            files.push(fpath);
        }
    }
    ///////////////////////////////
    return files;
}


function handleFile(fname: string, start: Moment, end: Moment) {
    let inRange = false;
    let match = fname.match(fnamePattern);
    //if null the file name does not match the regex, just return empty
    if(match !== null) {
        //capture date from fname and split on underscores
        let dateParts = match[1].split("_");
        let fileDateDepth = dateParts.length - 1;
        const fileStart = dateToDepth(start, fileDateDepth);
        const fileEnd = dateToDepth(end, fileDateDepth);
        //get parts
        const [year, month, day, hour, minute, second] = dateParts;
        //construct ISO date string from parts with defaults for missing values
        const isoDateStr = `${year}-${month || "01"}-${day || "01"}T${hour || "00"}:${minute || "00"}:${second || "00"}`;
        //create date object from ISO string
        let fileDate = moment(isoDateStr);
        //check if date is between the start and end date (inclusive at both ends)
        //if it is return the file, otherwise empty
        if(fileDate.isSameOrAfter(fileStart) && fileDate.isSameOrBefore(fileEnd)) {
            inRange = true;
        }
    }
    return inRange;
}

function dateToDepth(date: Moment, depth: number) {
    let period = periodOrder[depth];
    return dateToPeriod(date, period);
}

function dateToPeriod(date: Moment, period: moment.unitOfTime.StartOf) {
    return date.clone().startOf(period);
}

function setDatePartByDepth(date: Moment, part: string, depth: number) {
    let period = periodOrder[depth];
    return setDatePartByPeriod(date, part, period);
}

function setDatePartByPeriod(date: Moment, part: string, period: moment.unitOfTime.StartOf) {
    let partNum = datePartToNumber(part, period);
    return date.clone().set(<moment.unitOfTime.All>period, partNum);
}

function datePartToNumber(part: string, period: moment.unitOfTime.StartOf) {
    let partNum = Number(part);
    //moment months are 0 based
    if(period == "month") {
        partNum--;
    }
    return partNum;
}

//note root must start at date paths
async function getPathsBetweenDates(root: string, start: Moment, end: Moment, collapse: boolean = true, date = moment("0000"), depth: number = 0): Promise<any> {
    const dirStart = dateToDepth(start, depth);
    const dirEnd = dateToDepth(end, depth);

    let canCollapse = true;
    return new Promise(async (resolve) => {
        fs.readdir(root, {withFileTypes: true}, (e, dirents) => {
            //error, probably root does not exist, resolve empty
            if(e) {
                resolve({
                    paths: [],
                    collapse: false,
                    numFiles: 0
                });
            }
            else {
                let branchPromises: any[] = [];
                for(let dirent of dirents) {
                    let subpath = path.join(root, dirent.name);
                    //if file, parse date and return file if in between dates
                    if(dirent.isFile()) {
                        if(handleFile(subpath, start, end)) {
                            branchPromises.push(Promise.resolve({
                                paths: [subpath],
                                collapse: true,
                                numFiles: 1
                            }));
                        }
                        else {
                            canCollapse = false;
                        }
                    }
                    //otherwise if dir recursively descend
                    else if(dirent.isDirectory()) {
                        //check if should descend further, if folder outside range return empty
                        try {
                            let subDate = setDatePartByDepth(date, dirent.name, depth);
                            if(subDate.isSameOrAfter(dirStart) && subDate.isSameOrBefore(dirEnd)) {
                                branchPromises.push(
                                    getPathsBetweenDates(subpath, start, end, collapse, subDate, depth + 1)
                                    .catch((e) => {
                                        //if an error occured in the descent then just return empty
                                        return {
                                            files: [],
                                            collapse: false,
                                            numFiles: 0
                                        };
                                    })
                                );
                            }
                            //don't descend down branch, out of range
                            else {
                                canCollapse = false;
                            }
                        }
                        //if failed probably not a valid numeric folder name, just skip the folder and indicate cannot be collapsed
                        catch {
                            canCollapse = false;
                        }
                        
                    }
                    //if need to deal with symlinks need to expand, but for now just indicate that dir can't be collapsed
                    //for our purposes this should never trigger though
                    else {
                        canCollapse = false;
                    }
                }

                resolve(
                    Promise.all(branchPromises).then((results) => {
                        let data = results.reduce((agg, result) => {
                            agg.paths = agg.paths.concat(result.paths);
                            agg.collapse &&= result.collapse;
                            agg.numFiles += result.numFiles;
                            return agg;
                        }, {
                            paths: [],
                            collapse: canCollapse,
                            numFiles: 0
                        });
                        //if collapse is set and the subtree is collapsed then collapse files into root
                        if(collapse && data.collapse) {
                            data.paths = [root];
                        }
                        return data;
                    })
                    .catch((e) => {
                        return {
                            paths: [],
                            collapse: false,
                            numFiles: 0
                        };
                    })
                );
            }
        });
    }); 
}