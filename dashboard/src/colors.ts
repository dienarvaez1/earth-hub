// Magnitude colors, light to dark, blended between these stops: one definition for the map's
// dots and clusters, its legend, and the magnitude chart's bars. Below the first stop and
// above the last, colors stay at the end ones.

export const MAGNITUDE_STOPS: [number, string][] = [
  [0, "#fed976"],
  [2.5, "#fd8d3c"],
  [4.5, "#e31a1c"],
  [6, "#800026"],
];

const rgb = (hex: string) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));

/** The color of a magnitude, blended in RGB like the map's "interpolate" expression. */
export function colorAt(magnitude: number): string {
  const stops = MAGNITUDE_STOPS;
  if (magnitude <= stops[0][0]) return stops[0][1];
  for (let i = 1; i < stops.length; i++) {
    const [m1, c1] = stops[i];
    if (magnitude <= m1) {
      const [m0, c0] = stops[i - 1];
      const t = (magnitude - m0) / (m1 - m0);
      const [a, b] = [rgb(c0), rgb(c1)];
      const mixed = a.map((x, k) => Math.round(x + (b[k] - x) * t));
      return `#${mixed.map((x) => x.toString(16).padStart(2, "0")).join("")}`;
    }
  }
  return stops[stops.length - 1][1];
}

/** The bands the events-over-time bars are split into, bottom to top: the legend's stops as
 * lower edges (over_time.sql counts them the same way), then events without a magnitude. */
export const LEVELS = [
  { key: "below_2_5", label: "Below 2.5", color: MAGNITUDE_STOPS[0][1] },
  { key: "from_2_5", label: "2.5 to 4.5", color: MAGNITUDE_STOPS[1][1] },
  { key: "from_4_5", label: "4.5 to 6", color: MAGNITUDE_STOPS[2][1] },
  { key: "from_6", label: "6 and above", color: MAGNITUDE_STOPS[3][1] },
  { key: "no_magnitude", label: "No magnitude", color: "#a8a29e" },
] as const;
