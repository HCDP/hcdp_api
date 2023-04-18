const moment = require("moment");
const fs = require("fs");
const path = require("path");


//property hierarchy (followed by file and date parts)
const hierarchy = ["datatype", "production", "aggregation", "period", "extent", "fill"];
//details on file name period aggregations and file extensions
const fileDetails = {
    metadata: {
        agg: 0,
        ext: "txt"
    },
    data_map: {
        agg: 0,
        ext: "tif"
    },
    se: {
        agg: 0,
        ext: "tif"
    },
    anom: {
        agg: 0,
        ext: "tif"
    },
    anom_se: {
        agg: 0,
        ext: "tif"
    },
    station_metadata: {
        agg: null,
        ext: "csv"
    },
    station_data: {
        agg: 1,
        ext: "csv"
    }
}
//empty file index
const emptyIndex = {
    statewide: "/data/empty/statewide_hi_NA.tif"
};




//////////////////////////////////////////////////////////
////////////////////// new ///////////////////////////////
//////////////////////////////////////////////////////////


function createDateGroups(period, range) {
    let dateGroups = {};
    let start = range.start;
    let end = range.end;
    let startDate = new moment.utc(start);
    //zero out extraneous parts of the date
    startDate.startOf(period);
    let endDate = new moment.utc(end);
    endDate.startOf(period);
    //calculations are exclusive at the end so add one period
    endDate.add(1, period);

    let uncoveredDates = [startDate, endDate, null, endDate];

    let periods = ["year", "month", "day"];
    for(let i = 0; i < periods.length; i++) {
        let group = periods[i];
        
        let data = getGroupsBetween(uncoveredDates[0], uncoveredDates[1], group);
        dateGroups[group] = data.periods;
        uncoveredDates[1] = data.coverage[0];

        if(uncoveredDates[2]) {
            data = getGroupsBetween(uncoveredDates[2], uncoveredDates[3], group);
            dateGroups[group] = dateGroups[group].concat(data.periods);
        }

        if(group == period) {
            break;
        }

        uncoveredDates[2] = data.coverage[1];
    }

    return dateGroups;
}


function getGroupsBetween(start, end, period) {
    periods = [];
    coverage = [];
    date = start.clone();
    //move to start of period
    date.startOf(period);
    //if start of period is same as start date start there, otherwise advance by one period (initial not fully covered)
    if(!date.isSame(start)) {
        date.add(1, period);
    }

    let coverageStart = moment.min(date, end).clone();
    coverage.push(coverageStart);

    //need to see if period completely enclosed, so go to end of period
    date.endOf(period);
    while(date.isBefore(end)) {
        let clone = date.clone();
        //go to the start for simplicity (zero out lower properties)
        clone.startOf(period);
        periods.push(clone);
        date.add(1, period);
        //end points may not align (for months specifically), move to end of period
        date.endOf(period);
    }
    //end of coverage (exclusive)
    date.startOf(period);

    let coverageEnd = moment.min(date, end).clone();
    coverage.push(coverageEnd);

    //note coverage is [)
    data = {
        periods,
        coverage
    };
    return data;
}


function getFolderAndFileDateParts(period, range) {
    let groups = createDateGroups(period, range);
    let folderDateParts = [];
    let aggregateFolders = new Set();
    let fileDateParts = [];

    let periods = ["year", "month", "day"];
    for(let i = 0; i < periods.length; i++) {
        let group = periods[i];
        let groupData = groups[group];

        if(group == period) {
            for(let date of groupData) {
                //folder parts
                //folder grouped to one period up
                let folderGroup = periods[i - 1];
                let folderPart = createDateString(date, folderGroup, "/");
                //file parts
                let filePart = createDateString(date, group, "_");
                //used for aggregate files like station data
                //aggregate file should be only one in containing folder, so can just use folder
                //duplicates since files not separated
                aggregateFolders.add(folderPart);
                let fileData = [folderPart, filePart];
                fileDateParts.push(fileData);
            }
            //break after period of interest
            break;
        }
        else {
            for(let date of groupData) {
                let folderPart = createDateString(date, group, "/");
                folderDateParts.push(folderPart);
            }
        }
    }
    aggregateFolders = Array.from(aggregateFolders);
    return {
        folderDateParts,
        aggregateFolders,
        fileDateParts
    }
}


async function countFiles(root) {
    let fcount = 0;
    try {
        let stats = await fs.promises.lstat(root);
        if(stats.isDirectory()) {
            let content = await fs.promises.readdir(root);
            for(let item of content) {
                let subpath = path.join(root, item);
                fcount += await countFiles(subpath);
            }
        }
        else if(stats.isFile()) {
            fcount = 1;
        }
    }
    //just catch errors and return 0 (should mean the path does not exist)
    catch(e) {}
    return fcount;
}

//look this over
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

//TEMP
function convert(data) {
    // {
    //     files: ["data_map"],
    //     range: {
    //       start: date,
    //       end: date
    //     },
    //     ...properties
    //   }
    let converted = [];
    for(let item of data) {
        for(let fileItem of item.fileData) {
            files = fileItem.files;
            let expanded = combinations(fileItem.fileParams);
            for(obj of expanded) {
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

async function getPaths(root, data) {
    let paths = [];
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
                let fname = "";
                let period = item.period;
                let range = item.range;
                let ftypes = item.files;
                //add properties to path in order of hierarchy
                for(let property of hierarchy) {
                    let value = item[property];
                    if(value !== undefined) {
                        fdir = path.join(fdir, value);
                        fname = `${fname}_${value}`;
                    }
                }
    
                //strip leading underscore from fname
                fname = fname.substring(1);
    
                const handlePath = async (path) => {
                    //validate path exists and get number of files it contains
                    let numFiles = await countFiles(path);
                    //if numFiles is 0 should mean the path does not exist
                    if(numFiles) {
                        totalFiles += numFiles;
                        paths.push(path);
                    }
                }
    
                if(period && range) {
                    let dateParts = getFolderAndFileDateParts(period, range);
                    for(let ftype of ftypes) {
                        //add folder groups
                        let fdirType = path.join(fdir, ftype);
                        for(folderDatePart of dateParts.folderDateParts) {
                            let fdirFull = path.join(fdirType, folderDatePart);
                            await handlePath(fdirFull);
                        }
    
                        //add individual files
                        let details = fileDetails[ftype];
                        //note this is only set up for single tier agg, need to update if can be aggregated further
                        //if aggregated file then just add aggregated folders
                        if(details.agg) {
                            for(folderDatePart of dateParts.aggregateFolders) {
                                //combine dir with date part and add folder to list
                                let fdirFull = path.join(fdirType, folderDatePart);
                                await handlePath(fdirFull);
                            }
                        }
                        //otherwise create file name
                        else {
                            for(fileDateComponents of dateParts.fileDateParts) {
                                //deconstruct components
                                let [ folderDatePart, fileDatePart ] = fileDateComponents;
                                //create full dir
                                let fdirFull = path.join(fdirType, folderDatePart);
                                //create full file name
                                let fnameFull = `${fname}_${ftype}_${fileDatePart}.${details.ext}`;
                                //combine dir and file name
                                let fpathFull = path.join(fdirFull, fnameFull);
                                await handlePath(fpathFull);
                            }
                        }
                    }
                }
                //no date component
                else {
                    for(let ftype of ftypes) {
                        let details = fileDetails[ftype];
                        //add file part to path
                        let fdirComplete = path.join(fdir, ftype);
                        //add fname end to fname
                        let fnameComplete = `${fname}_${ftype}.${details.ext}`;
                        //construct complete file path
                        let fpath = path.join(fdirComplete, fnameComplete);
                        await handlePath(fpath);
                    }
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


//////////////////////////////////////////////////////////
////////////////////// new ///////////////////////////////
//////////////////////////////////////////////////////////


// async function getRainfallFiles(dateData, ) {

// }

async function getFiles(root, data) {
    let files = [];
    // try {
    //     for(let item of data) {
    //         switch(item.datatype) {
    //             case "rainfall": {
    //                 getRainfallFiles();
    //             }
    //             case "temperature": {
    //                 break;
    //             }
    //             case "downscaling_rainfall": {
    //                 break;
    //             }
    //             case "downscaling_temperature": {
    //                 break;
    //             }
    //         }
    //     }
    // }
    // catch(e) {}
    // return files;
    // return [path.join(root, "ndvi/16day/statewide/data_map/2021/12/ndvi_16day_statewide_data_map_2021_12_19.tif")];
    //at least for now just catchall and return files found before failure, maybe add more catching/skipping later, or 400?
    try {
        for(let item of data) {
            //use simplified version for getting ds data
            if(item.datatype == "downscaling_temperature" || item.datatype == "downscaling_rainfall") {
                files = files.concat(await getDSFiles(root, item));
            }
            else {
                let fdir = root;
                let fname = ""
                let period = item.period;
                let range = item.range;
                let ftypes = item.files;
                //add properties to path in order of hierarchy
                for(let property of hierarchy) {
                    let value = item[property];
                    if(value !== undefined) {
                        fdir = path.join(fdir, value);
                        fname = `${fname}_${value}`;
                    }
                }
                if(period && range) {
                    //expand out dates
                    dates = expandDates(period, range);
                    for(date of dates) {
                        for(let ftype of ftypes) {
                            let dirPeriod = shiftPeriod(period, 1);
                            //add file and date part of fdir
                            let fdirComplete = path.join(fdir, ftype, createDateString(date, dirPeriod, "/"));
                            //add fname end to fname
                            let fnameComplete = `${fname}_${getFnameEnd(ftype, period, date)}`;
                            //strip leading underscore
                            fnameComplete = fnameComplete.substring(1);
                            //construct complete file path
                            let fpath = path.join(fdirComplete, fnameComplete);
                            //validate file exists and push to file list if it does
                            if(await validate(fpath)) {
                                files.push(fpath);
                            }
                        }
                    } 
                }
                //no date component
                else {
                    for(let ftype of ftypes) {
                        //add file part to path
                        let fdirComplete = path.join(fdir, ftype);
                        //add fname end to fname
                        let fnameComplete = `${fname}_${getFnameEnd(ftype, undefined, undefined)}`;
                        //strip leading underscore
                        fnameComplete = fnameComplete.substring(1);
                        //construct complete file path
                        let fpath = path.join(fdirComplete, fnameComplete);
                        //validate file exists and append to file list if it does
                        if(await validate(fpath)) {
                            files.push(fpath);
                        }
                    }
                }
            }
        }
    }
    catch(e) {};
    return files;
}

//add folder with empty geotiffs for extents
function getEmpty(extent) {
    let emptyFile = emptyIndex[extent] || null;
    return emptyFile;
}

/////////////////////////////////////////////////
///////////////// helper functs /////////////////
/////////////////////////////////////////////////

//shift period by diff levels
function shiftPeriod(period, diff) {
    let periodOrder = ["day", "month", "year"];
    let periodIndex = periodOrder.indexOf(period);
    let shiftedPeriodIndex = periodIndex + diff;
    let shiftedPeriod = periodOrder[shiftedPeriodIndex]
    return shiftedPeriod;
}

//format date string using period and delimeter
function createDateString(date, period, delim) {
    let dateFormat = "";
    switch(period) {
        case "day": {
            dateFormat = `${delim}DD`;
        }
        case "month": {
            dateFormat = `${delim}MM${dateFormat}`;
        }
        case "year": {
            dateFormat = `YYYY${dateFormat}`;
            break;
        }
        default: {
            throw Error("Unrecognized period");
        }
    }

    let fdate = date.format(dateFormat);
    return fdate;
}

//get the end portion of file name
function getFnameEnd(file, period, date) {
    let details = fileDetails[file];
    let fnameEnd = file;
    let agg = details.agg;
    if(agg !== null) {
        aggPeriod = agg == 0 ? period : shiftPeriod(period, agg);
        datePart = createDateString(date, aggPeriod, "_")
        fnameEnd += `_${datePart}`;
    }
    fnameEnd += `.${details.ext}`;
    return fnameEnd;
}

//expand a group of date strings and wrap in moments
function expandDates(period, range) {
    let dates = [];
    let start = range.start;
    let end = range.end;
    let date = new moment(start);
    let endDate = new moment(end);
    while(date.isSameOrBefore(endDate)) {
        let clone = date.clone();
        dates.push(clone);
        date.add(1, period);
    }
    return dates;
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



//should update everything to use this, for now just use for ds data
const hierarchies = {
    downscaling_rainfall: ["dsm", "season", "period"],
    downscaling_temperature: ["dsm", "period"]
}

//expand to allow different units to be grabbed, for now just mm and celcius
async function getDSFiles(root, properties) {
    let files = [];
    let fileTags = properties.files;
    let file_suffix;
    let hierarchy = hierarchies[properties.datatype];
    let values = [properties.datatype];
    let period = properties.period;
    for(let property of hierarchy) {
        let value = properties[property];
        values.push(value);
    }

    ////MAKE THIS MORE COHESIVE////
    let units;
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

const fnamePattern = /^.+?([0-9]{4}(?:(?:_[0-9]{2}){0,5}|(?:_[0-9]{2}){5}\.[0-9]+))\.[a-zA-Z0-9]+$/;

function handleFile(fname, start, end) {
    let inRange = false;
    let match = fname.match(fnamePattern);
    //if null the file name does not match the regex, just return empty
    if(match !== null) {
        //capture date from fname and split on underscores
        dateParts = match[1].split("_");
        let fileDateDepth = dateParts.length - 1;
        const fileStart = dateToDepth(start, fileDateDepth);
        const fileEnd = dateToDepth(end, fileDateDepth);
        //get parts
        const [year, month, day, hour, minute, second] = dateParts;
        //construct ISO date string from parts with defaults for missing values
        const isoDateStr = `${year}-${month || "01"}-${day || "01"}T${hour || "00"}:${minute || "00"}:${second || "00"}`;
        //create date object from ISO string
        let fileDate = new moment(isoDateStr);
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
async function getPathsBetweenDates(root, start, end, collapse, date, depth) {
    if(collapse === undefined) {
        collapse = true;
    }
    if(!date) {
        date = new moment("0000")
    }
    if(depth === undefined) {
        depth = 0;
    }
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
                let branchPromises = [];
                for(let dirent of dirents) {
                    subpath = path.join(root, dirent.name);
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
                                console.log(subpath);
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
                        console.log(e);
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


exports.getFiles = getFiles;
exports.getEmpty = getEmpty;
exports.getPaths = getPaths;
exports.getPathsBetweenDates = getPathsBetweenDates;