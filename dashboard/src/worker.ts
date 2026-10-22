// The dashboard's Worker. Static files (the page) are served by Cloudflare before this
// runs; the Worker only answers /data/*, reading the snapshot from R2 through a binding,
// so the bucket stays private and the page holds no credentials.
//
// It also starts direct mode's rounds on its cron trigger (wrangler.template.jsonc): GitHub's
// own schedule fires late and skips runs (DEF-052), Cloudflare's runs on time. The handler is
// not reachable over HTTP.

interface Env {
  SNAPSHOTS: R2Bucket;
  GITHUB_API: string; // https://api.github.com; tests point it at a local stand-in
  GITHUB_REPO: string; // owner/name
  GITHUB_TOKEN?: string; // a secret: fine-grained, Actions read and write on this repository
}

// The workflow and the job a trigger runs (.github/workflows/direct.yml).
const WORKFLOW = "direct.yml";
const DISPATCH = { ref: "main", inputs: { job: "round" } };

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

  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    await startRound(env);
  },
} satisfies ExportedHandler<Env>;

/** Asks GitHub to run one direct round. Without a token it only says so; a refusal throws,
 * so Cloudflare records the trigger as failed. */
async function startRound(env: Env): Promise<void> {
  if (!env.GITHUB_TOKEN) {
    console.warn("trigger: GITHUB_TOKEN is not set, no direct round started");
    return;
  }
  const url = `${env.GITHUB_API}/repos/${env.GITHUB_REPO}/actions/workflows/${WORKFLOW}/dispatches`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "earth-hub-trigger",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify(DISPATCH),
  });
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 300);
    throw new Error(`trigger: GitHub answered ${response.status}: ${detail}`);
  }
  console.log("trigger: direct round started");
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
