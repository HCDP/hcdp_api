import * as fs from "fs";
import * as path from "path";


export class TwoWayMap<T, U> {
    private map: Map<T, U>;
    private reverseMap: Map<U, T>;

    constructor(map: [T, U][]) {
        this.map = new Map<T, U>(map);
        this.reverseMap = new Map<U, T>(map.map(([key, value]) => [value, key]));
    }

    public lookup(value: T) {
        return this.map.get(value);
    }

    public reverseLookup(value: U) {
        return this.reverseMap.get(value);
    }
}

export async function readdir(dir): Promise<{err, files}> {
  return new Promise((resolve, reject) => {
    fs.readdir(dir, (err, files) => {
      resolve({err, files});
    });
  });
}

export async function handleSubprocess(subprocess, dataHandler, errHandler?) {
  return new Promise((resolve, reject) => {
    if(!errHandler) {
      errHandler = () => {};
    }
    if(!dataHandler) {
      dataHandler = () => {};
    }
    //write content to res
    subprocess.stdout.on("data", dataHandler);
    subprocess.stderr.on("data", errHandler);
    subprocess.on("exit", (code) => {
      resolve(code);
    });
    subprocess.on("error", (err) => {
      reject(err);
    });
  });
}








export function processTapisError(res, e) {
  let status = e["http status code"];
  let reason = e["message"];
  if(status === undefined) {
    status = 500;
  }
  if(reason === undefined) {
    reason = "An error occured while processing the request.";
    console.error(`An unexpected error occurred during a Tapis operation. Error: ${e}`);
  }
  res.status(status)
  .send(reason);
}

function parseEscapedListString(value: string): string[] {
  let parsedValues = [];
  let escape = false;
  let parsedValue = "";
  for(let char of value) {
    switch(char) {
      // if not escaped, found item boundary, push value, reset, and break
      // otherwise fallthrough and use character verbatim
      case ",": {
        if(!escape) {
          parsedValues.push(parsedValue);
          parsedValue = "";
          break;
        }
      }
      case "\\": {
        // if not escaped set escape to true and break
        // otherwise fallthrough and use character verbatim
        if(!escape) {
          escape = true;
          break;
        }
      }
      default: {
        parsedValue += char;
        escape = false;
      }
    }
  }
  parsedValues.push(parsedValue);
  return parsedValues;
}

export function parseListParam(param: any, valid: Set<string> = null) {
  let parsed: string[] = [];
  if(Array.isArray(param)) {
    for(let item of param) {
      if(typeof item === "string") {
        parsed = parsed.concat(parseEscapedListString(item));
      }
    }
  }
  else if(typeof param === "string") {
    parsed = parseEscapedListString(param);
  }

  if(valid) {
    parsed = parsed.filter((value) => valid.has(value));
  }
  return parsed;
}

export function parseBoolParam(param: any, defaultValue: boolean = false): boolean {
  let nonDefaultParamString = (!defaultValue).toString();
  let value = defaultValue;
  if(typeof param === "boolean") {
    value = param;
  }
  else if(typeof param === "string" && param.toLowerCase() === nonDefaultParamString) {
    value = !defaultValue;
  }
  return value;
}



export function validateType(value: any, types: string[]) {
  return types.includes(typeof value);
}

export function validateArray(param: any, elementValidator?: (value: any, index?: number, array?: any[]) => boolean) {
  let valid = false;
  if(Array.isArray(param)) {
    if(elementValidator) {
      valid = param.every(elementValidator)
    }
    else {
      valid = true;
    }
  }
  return valid;
}

export function validateDateString(value: any): boolean {
  return validateType(value, ['string']) && !isNaN(Date.parse(value));
}

export function validateRangedTimestamp(timestamp: any): boolean {
  // Handle the primitive string case
  if(validateType(timestamp, ['string'])) {
    return validateDateString(timestamp);
  }
  if(validateType(timestamp, ["object"]) && timestamp !== null) {
    if(!validateDateString(timestamp.start)) {
      return false;
    }
    // end is optional, but if provided must be a valid date string
    if(timestamp.end !== undefined && !validateDateString(timestamp.end)) {
      return false;
    }
    return true;
  }
  return false;
}



export function validateJSON(value: string) {
  try {
    JSON.parse(value);
    return true;
  }
  catch (e) {
    return false;
  }
}

export function deepEqual(obj1: any, obj2: any): boolean {
  let match = false;
  if(obj1 === obj2) {
    match = true;
  }
  else if(typeof obj1 == "object" && typeof obj1 == typeof obj2 && obj1 && obj2) {
    let keys1 = Object.keys(obj1);
    let keys2 = Object.keys(obj2);
    if(keys1.length == keys2.length && keys1.every((key: string) => key in obj2 && deepEqual(obj1[key], obj2[key]))) {
      match = true;
    }
  }
  return match;
}



export function sanitizeFilename(input: unknown, requireExt: string | null = null, fallback = "data.csv"): string {
  if(typeof input !== "string") {
    return fallback;
  }

  // Normalize Windows backslashes, strip directory paths, and replace spaces/unsafe characters with "_"
  let cleaned = path.basename(input.trim().replace(/\\/g, "/"))
    .replace(/[^\w.\-]/g, "_")
    .replace(/^\.+/, ""); // Prevent ".", "..", and hidden dotfiles

  if(!cleaned || cleaned.length > 200) {
    return fallback;
  }

  if(requireExt) {
    const ext = requireExt.replace(/^\.+/, "").toLowerCase();
    if(!cleaned.toLowerCase().endsWith(`.${ext}`)) {
      cleaned += `.${ext}`;
    }
  }

  return cleaned;
}