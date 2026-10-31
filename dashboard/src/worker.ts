// The dashboard's Worker. Static files (the page) are served by Cloudflare before this
// runs; the Worker only answers /data/*, reading the snapshot from R2 through a binding,
// so the bucket stays private and the page holds no credentials.
//
// It also starts direct mode's rounds, and its daily reconcile, on its cron triggers
// (wrangler.template.jsonc): GitHub's own schedule fires late and skips runs (DEF-052), and
// in a public repository it disables a scheduled workflow, rounds included, after 60 days
// without commits (DEF-056); Cloudflare's runs on time. The handler is not reachable over HTTP.

interface Env {
  SNAPSHOTS: R2Bucket;
  GITHUB_API: string; // https://api.github.com; tests point it at a local stand-in
  GITHUB_REPO: string; // owner/name
  GITHUB_TOKEN?: string; // a secret: fine-grained, Actions read and write on this repository
}

// The workflow, the branch and the job a trigger runs (.github/workflows/direct.yml).
const WORKFLOW = "direct.yml";
const REF = "main";
const JOB = "round";
// The daily reconcile's trigger (the other one starts a round), and the job it runs.
const RECONCILE_CRON = "10 0 * * *";
const RECONCILE = "reconcile";
// `make trigger-check` fires the handler with this "cron", which no deployed trigger has,
// under `wrangler dev --remote --test-scheduled`: the deployed token, asked about, starts nothing.
const CHECK_CRON = "check";
// Who asked for the round, for its audit record (src/earth_hub/direct/audit.py).
const STARTED_BY = "cloudflare-trigger";

// Once a day, the round at this UTC hour (one the round cron fires in: every 4 hours from 00:05)
// also passes how many days the token has left; within REMIND_DAYS the workflow's token job
// fails, and GitHub e-mails its owner.
const CHECK_HOUR_UTC = 8;
const REMIND_DAYS = 14;
const DAY_MS = 86_400_000;

// Public paths and the only objects they can reach: nothing else in the bucket is exposed.
const OBJECTS: Record<string, string> = {
  "/data/earthquakes.parquet": "analytics/earthquakes.parquet",
  "/data/meta.json": "analytics/meta.json",
};

// Browsers revalidate after this (a 304 when nothing changed); exports run at most hourly.
const MAX_AGE_S = 300;

// Shared by link, not promoted: kept out of search engines, like the page's files
// (public/_headers).
const ROBOTS = "noindex, nofollow";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const response = await serve(request, env);
    response.headers.set("X-Robots-Tag", ROBOTS);
    return response;
  },

  async scheduled(controller: ScheduledController, env: Env): Promise<void> {
    if (controller.cron === CHECK_CRON) await checkToken(env, new Date(controller.scheduledTime));
    else if (controller.cron === RECONCILE_CRON) await dispatch(env, RECONCILE, {});
    else await startRound(env, new Date(controller.scheduledTime));
  },
} satisfies ExportedHandler<Env>;

/** Asks GitHub to run one direct round, once a day with the days the token has left. */
async function startRound(env: Env, now: Date): Promise<void> {
  const inputs: Record<string, string> = {};
  if (env.GITHUB_TOKEN && now.getUTCHours() === CHECK_HOUR_UTC) {
    const days = await tokenDaysLeft(env, now);
    if (days !== null) {
      inputs.token_days_left = String(days);
      if (days <= REMIND_DAYS) {
        console.warn(`trigger: the GitHub token expires in ${days} days: run make trigger-token`);
      }
    }
  }
  await dispatch(env, JOB, inputs);
}

/** Asks GitHub to run direct.yml's `job` on main. Without a token it only says so; a refusal
 * throws, so Cloudflare records the trigger as failed. */
async function dispatch(env: Env, job: string, extra: Record<string, string>): Promise<void> {
  if (!env.GITHUB_TOKEN) {
    console.warn(`trigger: GITHUB_TOKEN is not set, no direct ${job} started`);
    return;
  }
  const inputs = { job, started_by: STARTED_BY, ...extra };
  const response = await github(env, "POST", "/dispatches", { ref: REF, inputs });
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 300);
    throw new Error(`trigger: GitHub answered ${response.status}: ${detail}`);
  }
  console.log(`trigger: direct ${job} started`);
}

/** Asks GitHub about direct.yml with the token, as the daily check does, and logs the answer
 * and the days left; a refusal, or no token, throws. Reading the workflow starts nothing. */
async function checkToken(env: Env, now: Date): Promise<void> {
  if (!env.GITHUB_TOKEN) throw new Error("trigger check: GITHUB_TOKEN is not set");
  const response = await github(env, "GET", "");
  await response.body?.cancel();
  if (!response.ok) throw new Error(`trigger check: GitHub answered ${response.status}`);
  const expiry = parseExpiry(response.headers.get("GitHub-Authentication-Token-Expiration"));
  const days = expiry === null ? "never expires" : `${Math.floor((expiry - now.getTime()) / DAY_MS)} days left`;
  console.log(`trigger check: OK, GitHub accepts the token for ${WORKFLOW} (${days})`);
}

/** The days until the token expires, from the date GitHub states on every reply to a token
 * that expires; null for one that never does. Reading the workflow starts nothing. */
async function tokenDaysLeft(env: Env, now: Date): Promise<number | null> {
  const response = await github(env, "GET", "");
  await response.body?.cancel();
  const expiry = parseExpiry(response.headers.get("GitHub-Authentication-Token-Expiration"));
  return expiry === null ? null : Math.floor((expiry - now.getTime()) / DAY_MS);
}

const EXPIRY = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) (UTC|[+-]\d{4})$/;

/** GitHub's "2027-10-15 07:00:00 UTC" or "2027-10-15 00:00:00 -0700", in epoch ms. */
function parseExpiry(text: string | null): number | null {
  const parts = EXPIRY.exec(text?.trim() ?? "");
  if (parts === null) return null;
  const [, year, month, day, hour, minute, second, zone] = parts;
  const sign = zone.startsWith("-") ? -1 : 1;
  const offsetMinutes =
    zone === "UTC" ? 0 : sign * (Number(zone.slice(1, 3)) * 60 + Number(zone.slice(3)));
  const local = Date.UTC(+year, +month - 1, +day, +hour, +minute, +second);
  return local - offsetMinutes * 60_000;
}

function github(env: Env, method: string, path: string, body?: object): Promise<Response> {
  const url = `${env.GITHUB_API}/repos/${env.GITHUB_REPO}/actions/workflows/${WORKFLOW}${path}`;
  return fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "earth-hub-trigger",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

async function serve(request: Request, env: Env): Promise<Response> {
  const key = OBJECTS[new URL(request.url).pathname];
  if (key === undefined) {
    return text(404, "not found");
  }
  if (request.method !== "GET" && request.method !== "HEAD") {
    return text(405, "method not allowed", { Allow: "GET, HEAD" });
  }
  let object: R2Object | R2ObjectBody | null;
  try {
    object = await env.SNAPSHOTS.get(key, {
      onlyIf: request.headers,
      range: request.headers,
    });
  } catch {
    // An invalid Range header, for example.
    return text(416, "range not satisfiable");
  }
  if (object === null) {
    return text(404, "no snapshot yet: run make export-r2");
  }
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("ETag", object.httpEtag);
  headers.set("Cache-Control", `public, max-age=${MAX_AGE_S}`);
  headers.set("Accept-Ranges", "bytes");
  if (!("body" in object)) {
    return new Response(null, { status: 304, headers }); // If-None-Match matched
  }
  const range = object.range as { offset: number; length: number } | undefined;
  if (request.headers.has("Range") && range !== undefined) {
    // Checked here: storage may answer a range starting past the end with the whole
    // object (the local simulator does), which would go out labelled as a part.
    const start = /^bytes=(\d+)-/.exec(request.headers.get("Range") ?? "");
    if ((start !== null && Number(start[1]) >= object.size) || range.length <= 0) {
      return text(416, "range not satisfiable", { "Content-Range": `bytes */${object.size}` });
    }
    const end = range.offset + range.length - 1;
    headers.set("Content-Range", `bytes ${range.offset}-${end}/${object.size}`);
    headers.set("Content-Length", String(range.length));
    return new Response(request.method === "HEAD" ? null : object.body, {
      status: 206,
      headers,
    });
  }
  headers.set("Content-Length", String(object.size));
  return new Response(request.method === "HEAD" ? null : object.body, { headers });
}

function text(status: number, message: string, extra: Record<string, string> = {}) {
  return new Response(message + "\n", {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store", ...extra },
  });
}
