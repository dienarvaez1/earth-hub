import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vite";

// Vite lists the license of every package bundled into the page (build.license); this turns
// the list into dist/third-party-licenses.txt, linked from the page's footer, which the
// packages' licenses require copies to carry (DEF-035). A package that ships no license text
// needs one in licenses/, or the build fails.
const LIST = ".vite/licenses.json";
const NOTICES = "third-party-licenses.txt";
// Served by the page but not bundled: DuckDB's Parquet extension (public/duckdb-extensions/).
const SERVED = [{ name: "DuckDB Parquet extension", identifier: "MIT", file: "duckdb.txt" }];

interface Licensed {
  name: string;
  version?: string;
  identifier?: string;
  text?: string;
}

// DASHBOARD_LICENSES: another folder, for the test that a missing text fails the build.
const TEXTS = process.env.DASHBOARD_LICENSES ?? fileURLToPath(new URL("licenses", import.meta.url));

function supplied(file: string): string {
  const path = join(TEXTS, file);
  if (!existsSync(path)) {
    throw new Error(`no license text for ${file}: add dashboard/licenses/${file}`);
  }
  return readFileSync(path, "utf8").trim();
}

function notices(): Plugin {
  return {
    name: "earth-hub-notices",
    apply: "build",
    // After Vite has written its list of the bundled packages.
    writeBundle(options) {
      const dir = options.dir ?? "dist";
      const list = join(dir, LIST);
      if (!existsSync(list)) {
        this.error(`${LIST} was not generated`);
      }
      const packages: Licensed[] = JSON.parse(readFileSync(list, "utf8"));
      rmSync(join(dir, ".vite"), { recursive: true });
      const sections = [
        ...packages.map((p) => ({
          title: `${p.name} ${p.version ?? ""}`.trim(),
          identifier: p.identifier ?? "unknown",
          text: p.text?.trim() || supplied(`${p.name.replace("/", "__")}.txt`),
        })),
        ...SERVED.map((s) => ({ title: s.name, identifier: s.identifier, text: supplied(s.file) })),
      ];
      const rule = "=".repeat(78);
      writeFileSync(
        join(dir, NOTICES),
        "Third-party software in the Earth-Hub dashboard, with its licenses.\n\n" +
          sections.map((s) => `${rule}\n${s.title} (${s.identifier})\n${rule}\n\n${s.text}\n`).join("\n"),
      );
    },
  };
}

// The footer's version, from git when the page is built: `git describe --tags --always --dirty`,
// so v1.0.0 for a tagged build, v1.0.0-4-g790aa4b four commits after it (just the commit with
// no tags, as in CI's shallow checkout). A tag links to its release, anything else to its
// commit; without git, the project itself.
const REPO_URL = "https://github.com/dienarvaez1/earth-hub";
const TAG = /^v\d+\.\d+\.\d+$/;

function git(...args: string[]): string {
  return execFileSync("git", args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
}

export function build(): { text: string; url: string } {
  try {
    const text = git("describe", "--tags", "--always", "--dirty");
    const url = TAG.test(text) ? `${REPO_URL}/releases/tag/${text}` : `${REPO_URL}/commit/${git("rev-parse", "HEAD")}`;
    return { text, url };
  } catch {
    return { text: "Earth-Hub on GitHub", url: REPO_URL };
  }
}

function version(): Plugin {
  const { text, url } = build();
  return {
    name: "earth-hub-version",
    transformIndexHtml: {
      order: "pre",
      handler: (html) => html.replaceAll("%EARTH_HUB_BUILD_URL%", url).replaceAll("%EARTH_HUB_BUILD%", text),
    },
  };
}

// The page only: src/worker.ts is built by wrangler.
export default defineConfig({
  build: {
    outDir: "dist",
    emptyOutDir: true,
    chunkSizeWarningLimit: 2000,
    license: { fileName: LIST },
  },
  plugins: [notices(), version()],
  server: { fs: { allow: [".."] } }, // the SQL files live in dashboard/queries
});
