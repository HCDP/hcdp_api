import moment from "moment";
import * as fs from "fs";
import * as path from "path";

//property hierarchy (followed by file and date parts)
const hierarchy = ["datatype", "production", "aggregation", "period", "extent", "fill"];

//empty file index
const emptyIndex = {
    statewide: "/data/empty/statewide_hi_NA.tif"
};

//should update everything to use this, for now just use for ds data
const hierarchies = {
    downscaling_rainfall: ["dsm", "season", "period"],
    downscaling_temperature: ["dsm", "period"]
}

export const fnamePattern = /^.+?([0-9]{4}(?:(?:_[0-9]{2}){0,5}|(?:_[0-9]{2}){5}\.[0-9]+))\.[a-zA-Z0-9]+$/;

export async function getPaths(root, data, collapse?) {
    let paths: string[] = [];
    let totalFiles = 0;
    //at least for now just catchall and return files found before failure, maybe add more catching/skipping later, or 400?
    try {
        //maintain compatibility, only convert if new style TEMP
        if(data[0]?.fileData) {
            data = convert(data);
        }
        for(let item of data) {
            //use simplified version for getting ds data
            if(item.datatype == "downscaling_temperature" || item.datatype == "downscaling_rainfall") {
                let files = await getDSFiles(root, item);
                paths = paths.concat(files);
                totalFiles += files.length;
            }
            else {
                let fdir = root;
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
    return {
        numFiles: totalFiles,
        paths
    };
}



//add folder with empty geotiffs for extents
export function getEmpty(extent) {
    let emptyFile = emptyIndex[extent] || null;
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
async function validate(file) {
    file = path.join(file);
    return new Promise((resolve, reject) => {
        fs.access(file, fs.constants.F_OK, (e) => {
            e ? resolve(false) : resolve(true);
        });
    });
}


//expand to allow different units to be grabbed, for now just mm and celcius
async function getDSFiles(root, properties) {
    let files: string[] = [];
    let fileTags = properties.files;
    let file_suffix: string;
    let hierarchy = hierarchies[properties.datatype];
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
        let fpath = path.join(root, subpath, fname);
        if(await validate(fpath)) {
            files.push(fpath);
        }
    }
    ///////////////////////////////
    return files;
}


function handleFile(fname, start, end) {
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

const periodOrder = ["year", "month", "day", "hour", "minute", "second"];
function dateToDepth(date, depth) {
    let period = periodOrder[depth];
    return dateToPeriod(date, period);
}

function dateToPeriod(date, period) {
    return date.clone().startOf(period);
}

function setDatePartByDepth(date, part, depth) {
    let period = periodOrder[depth];
    return setDatePartByPeriod(date, part, period);
}

function setDatePartByPeriod(date, part, period) {
    let partNum = datePartToNumber(part, period);
    return date.clone().set(period, partNum);
}

function datePartToNumber(part, period) {
    let partNum = Number(part);
    //moment months are 0 based
    if(period == "month") {
        partNum--;
    }
    return partNum;
}

//note root must start at date paths
async function getPathsBetweenDates(root, start, end, collapse = true, date = moment("0000"), depth = 0): Promise<any> {
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