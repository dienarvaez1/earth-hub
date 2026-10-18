"""City, region and country derived from the USGS `place` text.

USGS writes most places as "<distance> <direction> of <city>, <area>", e.g. "86 km E of
Ugashik, Alaska". The text after the first " of " is split at commas: the first part is the
city and the last is the area. In the US the area is a state ("Alaska", or a code like "CA"),
so it becomes the region and the country is "United States". Elsewhere it is the country,
with any middle part as the region ("Mexicali, B.C., MX").

Without " of " ("Fiji region") nothing is derived. Without a comma after it ("south of the
Fiji Islands") the text names an area, not a city: it becomes the region. `place` itself is
always stored unchanged.
"""

from typing import NamedTuple

DELIMITER = " of "
UNITED_STATES = "United States"

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin",
    "WY": "Wyoming", "DC": "District of Columbia",
}  # fmt: skip
_STATE_NAMES = {name: name for name in US_STATES.values()} | US_STATES

# Abbreviations USGS uses for areas outside the US.
COUNTRY_CODES = {"MX": "Mexico"}
REGION_CODES = {"B.C.": "Baja California"}

# "Georgia" is both a US state and a country; the country lies east of Greenwich.
_AMBIGUOUS_WITH_COUNTRY = {"Georgia"}


class Location(NamedTuple):
    city: str | None
    region: str | None
    country: str | None


NOWHERE = Location(None, None, None)


def parse_place(place: str | None, longitude: float | None = None) -> Location:
    if not place or DELIMITER not in place:
        return NOWHERE
    rest = place.split(DELIMITER, 1)[1]
    parts = [p.strip() for p in rest.split(",")]
    if any(not p for p in parts):
        return NOWHERE
    if len(parts) == 1:
        area = parts[0]
        if _is_us_state(area, longitude):
            return Location(None, _STATE_NAMES[area], UNITED_STATES)
        return Location(None, area, None)
    city, *middle, area = parts
    if _is_us_state(area, longitude):
        return Location(city, _STATE_NAMES[area], UNITED_STATES)
    region = ", ".join(REGION_CODES.get(p, p) for p in middle) or None
    return Location(city, region, COUNTRY_CODES.get(area, area))


def _is_us_state(area: str, longitude: float | None) -> bool:
    if area not in _STATE_NAMES:
        return False
    return area not in _AMBIGUOUS_WITH_COUNTRY or longitude is None or longitude < 0
