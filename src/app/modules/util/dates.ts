import moment, { Moment } from "moment-timezone";


export type DPLocation = "hawaii" | "american_samoa";

export interface DateParts {
  year: number | string | undefined,
  month?: number | string | undefined,
  day?: number | string | undefined,
  hour?: number | string | undefined,
  minute?: number | string | undefined,
  second?: number | string | undefined,
  subsecond?: number | string | undefined
}

export function getTimezone(location: DPLocation) {
  const tzmap = {
    "hawaii": "Pacific/Honolulu",
    "american_samoa": "Pacific/Pago_Pago"
  }
  return tzmap[location];
}

export function setTimezone(location: DPLocation, isoDate: Moment, keepLocalTime: boolean = false) {
  return isoDate.clone().tz(getTimezone(location), keepLocalTime);
}

export function createTZDateFromString(location: DPLocation, dateString: string, keepLocalTime: boolean = false) {
  let isoDate = moment(dateString)
  return setTimezone(location, isoDate, keepLocalTime);
}

export function createISODateFromParts(parts: DateParts) {
  const { year, month, day, hour, minute, second, subsecond } = parts;
  const isoDateStr = `${year}-${month || "01"}-${day || "01"}T${hour || "00"}:${minute || "00"}:${second || "00"}.${subsecond || "000"}`;
  return moment(isoDateStr);
}

export function createTZDateFromParts(location: DPLocation, parts: DateParts, keepLocalTime: boolean = false) {
  let isoDate = createISODateFromParts(parts);
  return setTimezone(location, isoDate, keepLocalTime);
}