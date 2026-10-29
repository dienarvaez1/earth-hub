// An event's time in UTC and at the place it happened, for the map's popups.
//
// The time zone comes from the event's coordinates (tz-lookup bundles the world's time-zone
// boundaries, and the nautical zones Etc/GMT±N at sea); the browser's Intl does the rest,
// summer time included. The zone shows as its abbreviation where the browser has one (AKDT,
// HST, ...), then its offset from UTC in parentheses: "AKDT (UTC-8)". Where it has none,
// only the offset: "(UTC+9)".

import tzlookup from "@photostructure/tz-lookup";

export type LocalTime = { text: string; zone: string };

function zoneName(ms: number, timeZone: string, style: "short" | "shortOffset"): string {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone, timeZoneName: style }).formatToParts(
    new Date(ms),
  );
  return parts.find((p) => p.type === "timeZoneName")?.value ?? "";
}

/** "GMT-8" or "GMT+5:30" as "UTC-8" or "UTC+5:30"; UTC itself ("GMT" or "GMT+0") as "UTC". */
function offset(ms: number, timeZone: string): string {
  const name = zoneName(ms, timeZone, "shortOffset");
  return name === "GMT" || name === "GMT+0" ? "UTC" : name.replace(/^GMT/, "UTC");
}

function format(ms: number, timeZone: string): string {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    })
      .formatToParts(new Date(ms))
      .map((p) => [p.type, p.value]),
  );
  // Without an abbreviation of its own, the browser names the zone by its offset (GMT+9).
  const short = zoneName(ms, timeZone, "short");
  const abbreviation = short.startsWith("GMT") ? "" : `${short} `;
  const when = `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
  return `${when} ${abbreviation}(${offset(ms, timeZone)})`;
}

export function utcTime(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace("T", " ") + " UTC";
}

/** The time where the event happened, or null if its coordinates name no zone. */
export function localTime(ms: number, latitude: number, longitude: number): LocalTime | null {
  let zone: string;
  try {
    zone = tzlookup(latitude, longitude);
  } catch {
    return null; // coordinates out of range
  }
  return { text: format(ms, zone), zone };
}
