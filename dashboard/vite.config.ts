import { defineConfig } from "vite";

// The page only: src/worker.ts is built by wrangler.
export default defineConfig({
  build: { outDir: "dist", emptyOutDir: true, chunkSizeWarningLimit: 2000 },
  server: { fs: { allow: [".."] } }, // the SQL files live in dashboard/queries
});
