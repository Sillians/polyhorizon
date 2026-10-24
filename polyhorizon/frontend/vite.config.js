import { defineConfig } from "vite";

export default defineConfig({
  server: {
    host: true,
    port: 5173,
    proxy: {
      "/v1": process.env.REHEARSAL_API_URL || "http://localhost:8000",
      "/metrics": process.env.REHEARSAL_API_URL || "http://localhost:8000",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
