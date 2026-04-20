import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  publicDir: resolve(__dirname, '../public'),
  server: {
    proxy: {
      // Proxy specific API routes to your Axum server
      '/classes': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      },
      '/objects': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      },
      '/openapi': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:3000',
        ws: true,
      },
    },
  },
});