// The time presets. Every period is whole UTC days, from its first day through its last:
// today, or for the previous day, week, month or year their own last day (the filter's end
// is the next midnight, exclusive), like the rest of the page.

export const PERIODS = {
  today: "Today",
  yesterday: "Yesterday",
  this_week: "This week",
  last_week: "Last week",
  this_month: "This month",
  last_month: "Last month",
  last_3_months: "Last 3 months",
  this_year: "This year",
  last_year: "Last year",
  last_3_years: "Last 3 years",
  custom: "Custom",
} as const;

export type Period = keyof typeof PERIODS;
export const DEFAULT_PERIOD: Period = "today";

/** The chart's grouping each preset starts with; Custom keeps the current one. */
export const BUCKET: Record<Exclude<Period, "custom">, "day" | "week" | "month"> = {
  today: "day",
  yesterday: "day",
  this_week: "day",
  last_week: "day",
  this_month: "day",
  last_month: "day",
  last_3_months: "week",
  this_year: "week",
  last_year: "week",
  last_3_years: "month",
};

/** "YYYY-MM-DD" of a moment, in UTC. */
export function day(moment: Date): string {
  return moment.toISOString().slice(0, 10);
}

function utcDay(year: number, month: number, date: number): Date {
  // Month arithmetic lands on the last day of a shorter month (31 May - 3 months is 28 or
  // 29 February), never spills into the next one.
  const last = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  return new Date(Date.UTC(year, month, Math.min(date, last)));
}

/** The first day of a preset period, as of `now`; null for custom. */
export function firstDay(period: Period, now: Date): string | null {
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  const monday = d - (now.getUTCDay() + 6) % 7; // ISO weeks start on Monday
  switch (period) {
    case "today":
      return day(utcDay(y, m, d));
    case "yesterday":
      return day(new Date(Date.UTC(y, m, d - 1)));
    case "this_week":
      return day(new Date(Date.UTC(y, m, monday)));
    case "last_week":
      return day(new Date(Date.UTC(y, m, monday - 7)));
    case "this_month":
      return day(utcDay(y, m, 1));
    case "last_month":
      return day(utcDay(y, m - 1, 1));
    case "last_3_months":
      return day(utcDay(y, m - 3, d));
    case "this_year":
      return day(utcDay(y, 0, 1));
    case "last_year":
      return day(utcDay(y - 1, 0, 1));
    case "last_3_years":
      return day(utcDay(y - 3, m, d));
    case "custom":
      return null;
  }
}

/** The last day of a preset period (included), as of `now`: today, except for the previous
 * day, week, month or year; null for custom. */
export function lastDay(period: Period, now: Date): string | null {
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  switch (period) {
    case "custom":
      return null;
    case "yesterday":
      return day(new Date(Date.UTC(y, m, d - 1)));
    case "last_week":
      return day(new Date(Date.UTC(y, m, d - (now.getUTCDay() + 6) % 7 - 1))); // Sunday
    case "last_month":
      return day(new Date(Date.UTC(y, m, 0))); // day 0 is the last of the month before
    case "last_year":
      return day(new Date(Date.UTC(y - 1, 11, 31)));
    default:
      return day(now);
  }
}
