import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  build: { target: "esnext" },
  server: {
    proxy: {
      "/api": { target: "http://localhost:8080", ws: true },
      "/metrics": "http://localhost:8080",
    },
  },
});
