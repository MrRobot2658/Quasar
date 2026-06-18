import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 开发态把 /api 代理到 sql-engine（避免跨域）；生产由 nginx 同源转发。
export default defineConfig(({ command }) => ({
  // 生产(build)挂在网关 /console/ 下；dev 仍是根路径
  base: "/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.SQL_ENGINE_URL || "http://localhost:8002",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ""),
      },
      "/assistant": {
        target: process.env.ASSISTANT_URL || "http://localhost:8004",
        changeOrigin: true,
      },
    },
  },
}));
