// DuckDB-WASM over the snapshot, running the SQL files in ../queries unchanged (the tests
// run the same files with DuckDB in Python).
import * as duckdb from "@duckdb/duckdb-wasm";

const QUERIES = import.meta.glob("../queries/*.sql", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

function sql(name: string): string {
  const text = QUERIES[`../queries/${name}.sql`];
  if (text === undefined) throw new Error(`no query ${name}`);
  return text;
}

export interface Filter {
  start_time: string | null; // inclusive, "YYYY-MM-DD HH:MM:SS" in UTC
  end_time: string | null; // exclusive
  min_magnitude: number | null;
  max_magnitude: number | null;
  country: string | null;
  region: string | null;
  event_type: string | null;
  include_deleted: boolean;
  time_bucket: "day" | "week" | "month";
  top_n: number;
  significant_magnitude: number;
}

// The parameter order of set_filter.sql.
const FILTER_ORDER: (keyof Filter)[] = [
  "start_time",
  "end_time",
  "min_magnitude",
  "max_magnitude",
  "country",
  "region",
  "event_type",
  "include_deleted",
  "time_bucket",
  "top_n",
  "significant_magnitude",
];

export const DEFAULT_FILTER: Filter = {
  start_time: null,
  end_time: null,
  min_magnitude: null,
  max_magnitude: null,
  country: null,
  region: null,
  event_type: null,
  include_deleted: false,
  time_bucket: "month",
  top_n: 10,
  significant_magnitude: 4.5,
};

export type Row = Record<string, string | number | boolean | null>;

export class Db {
  private constructor(
    private readonly conn: duckdb.AsyncDuckDBConnection,
    private readonly setFilterStatement: duckdb.AsyncPreparedStatement,
  ) {}

  /** Starts DuckDB at once and attaches the snapshot when it arrives: compiling DuckDB's
   * WebAssembly takes seconds, so it runs while the snapshot downloads. */
  static async open(
    parquet: Promise<Uint8Array>,
    timings: Record<string, number> = {},
  ): Promise<Db> {
    const t0 = performance.now();
    const mark = (name: string) => (timings[name] = performance.now() - t0);
    // The WebAssembly files are larger than a Worker's static files may be (25 MiB), so
    // they come from jsDelivr, at the version in package.json.
    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }),
    );
    const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), new Worker(workerUrl));
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    mark("duckdb_instantiate_ms");
    await db.registerFileBuffer("earthquakes.parquet", await parquet);
    const conn = await db.connect();
    // Parquet support is an extension: served with the page (scripts/fetch_duckdb_extensions.py).
    await step("the extension repository", () =>
      conn.query(`SET custom_extension_repository = '${location.origin}/duckdb-extensions'`),
    );
    await step("attaching the snapshot", () =>
      conn.query("CREATE VIEW snapshot AS SELECT * FROM read_parquet('earthquakes.parquet')"),
    );
    mark("duckdb_attach_ms");
    await step("setup.sql", () => conn.query(sql("setup")));
    const statement = await step("set_filter.sql", () => conn.prepare(sql("set_filter")));
    mark("duckdb_setup_ms");
    return new Db(conn, statement);
  }

  async setFilter(filter: Filter): Promise<void> {
    await this.setFilterStatement.query(...FILTER_ORDER.map((name) => filter[name]));
  }

  async query(name: string): Promise<Row[]> {
    const table = await step(`${name}.sql`, () => this.conn.query(sql(name)));
    return table.toArray().map((row) => plain(row.toJSON()));
  }
}

// Errors name the step that failed, for the page and for diagnosis.
async function step<T>(name: string, run: () => Promise<T>): Promise<T> {
  try {
    return await run();
  } catch (error) {
    throw new Error(`${name}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

// Arrow gives BIGINT counts as bigint and timestamps as epoch milliseconds; the page wants
// plain numbers.
function plain(row: Record<string, unknown>): Row {
  const out: Row = {};
  for (const [key, value] of Object.entries(row)) {
    if (typeof value === "bigint") out[key] = Number(value);
    else if (value instanceof Date) out[key] = value.getTime();
    else out[key] = value as Row[string];
  }
  return out;
}
