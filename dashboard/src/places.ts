// How the place menus show their options; the values, and the SQL behind them, stay as stored.
// The United States' states (a region) and territories (USGS names them as the country)
// are shown as "US-<name>".

// The state names src/earth_hub/validation/places.py stores as regions.
const US_STATES = new Set([
  "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
  "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
  "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
  "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
  "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
  "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
  "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
  "West Virginia", "Wisconsin", "Wyoming",
]); // prettier-ignore

// As USGS names them; "Samoa" alone is the independent state.
const US_TERRITORIES = new Set([
  "American Samoa", "Guam", "Northern Mariana Islands", "Puerto Rico", "U.S. Virgin Islands",
]); // prettier-ignore

const us = (name: string) => `US-${name.replace(/^U\.S\. /, "")}`;

/** A region option's label: "US-Alaska" for a state (or a territory, should one be a
 * region). Only the region menu holds states, so the country Georgia is never taken for the
 * state. */
export const regionLabel = (region: string) =>
  US_STATES.has(region) || US_TERRITORIES.has(region) ? us(region) : region;

/** A country option's label: "US-Puerto Rico", "US-Virgin Islands" for a territory. */
export const countryLabel = (country: string) =>
  US_TERRITORIES.has(country) ? us(country) : country;

const byLabel =
  <T>(value: (r: T) => string, label: (name: string) => string) =>
  (a: T, b: T) => {
    const [x, y] = [label(value(a)), label(value(b))];
    return x < y ? -1 : x > y ? 1 : 0;
  };

/** The country menu's order: Unknown (right after the menu's All), the countries as given,
 * then the US territories, alphabetically by label, right after the United States. */
export function countryOrder<T>(rows: T[], value: (r: T) => string): T[] {
  const unknown = rows.filter((r) => value(r) === "Unknown");
  const territories = rows
    .filter((r) => US_TERRITORIES.has(value(r)))
    .sort(byLabel(value, countryLabel));
  const others = rows.filter((r) => value(r) !== "Unknown" && !US_TERRITORIES.has(value(r)));
  const after = others.findIndex((r) => value(r) > "United States"); // the rows sort by name
  const at = after === -1 ? others.length : after;
  return [...unknown, ...others.slice(0, at), ...territories, ...others.slice(at)];
}

/** The region menu's order: Unknown (right after the menu's All), the other regions as
 * given, then the US states and after them any US territory, each alphabetically by label. */
export function regionOrder<T>(rows: T[], value: (r: T) => string): T[] {
  const is = (names: Set<string>) => (r: T) => names.has(value(r));
  return [
    ...rows.filter((r) => value(r) === "Unknown"),
    ...rows.filter((r) => value(r) !== "Unknown" && !is(US_STATES)(r) && !is(US_TERRITORIES)(r)),
    ...rows.filter(is(US_STATES)).sort(byLabel(value, regionLabel)),
    ...rows.filter(is(US_TERRITORIES)).sort(byLabel(value, countryLabel)),
  ];
}
