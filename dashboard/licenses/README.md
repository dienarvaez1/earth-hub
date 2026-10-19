# License texts the packages do not ship

`npm run build` writes `dist/third-party-licenses.txt`, the license of every package in the
page (vite.config.ts). It takes each text from the package; a package that ships none needs
its text here, as `<name>.txt` with `/` written `__`, or the build fails.

- `@duckdb__duckdb-wasm.txt`: from https://github.com/duckdb/duckdb-wasm/blob/main/LICENSE
- `duckdb.txt`: from https://github.com/duckdb/duckdb/blob/main/LICENSE, for the Parquet
  extension the page serves itself (`public/duckdb-extensions/`, not bundled by Vite)
