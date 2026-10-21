// An event's time in UTC and at the place it happened, for the map's popups.
//
// The time zone comes from the event's coordinates (tz-lookup bundles the world's time-zone
// boundaries, and the nautical zones Etc/GMT±N at sea); the browser's Intl does the rest,
// summer time included. Abbreviations are the browser's for the zone (EDT, CET, ...), or
// GMT±h where it has none.

import tzlookup from "@photostructure/tz-lookup";

export type LocalTime = { text: string; zone: string };

function format(ms: number, timeZone: string): string {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
      timeZoneName: "short",
    })
      .formatToParts(new Date(ms))
      .map((p) => [p.type, p.value]),
  );
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute} ${parts.timeZoneName}`;
}

export function utcTime(ms: number): string {
  return new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC";
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
