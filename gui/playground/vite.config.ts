import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  build: {
    lib: {
      // Could also be a dictionary or array of multiple entry points
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'CoCo',
      // the proper extensions will be added
      fileName: 'index',
    },
    rollupOptions: {
      // make sure to externalize deps that shouldn't be bundled
      // into your library
      external: ['snabbdom', '@ratiosolver/flick'],
      output: {
        // Provide global variables to use in the UMD build
        // for externalized deps
        globals: {
          snabbdom: 'snabbdom',
          '@ratiosolver/flick': 'flick',
        },
      },
    },
  },
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