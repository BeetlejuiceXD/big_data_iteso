import { defineConfig } from 'vite';

// Same-origin development proxy. Only loopback targets are accepted.
const apiTarget = process.env.TRAMA_API_PROXY_TARGET || 'http://127.0.0.1:8000';
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(apiTarget)) throw new Error('TRAMA_API_PROXY_TARGET must be a loopback HTTP URL');
export default defineConfig({
  cacheDir: '.vite-cache',
  server: {
    host: '127.0.0.1',
    proxy: {
      '/api': { target: apiTarget, changeOrigin: false },
    },
  },
});
