import { defineConfig } from 'vite'
import react, { reactCompilerPreset } from '@vitejs/plugin-react'
import babel from '@rolldown/plugin-babel'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    babel({ presets: [reactCompilerPreset()] })
  ],
  server: {
    // Dev server proxy: forward all requests to backend and ensure
    // the backend receives paths prefixed with `/api`.
    host: true,
    proxy: {
      // Match all paths
      '/api': {
        target: process.env.BACKEND_URL || 'http://localhost:8000',
        changeOrigin: true,
        secure: false,
        ws: true,
        rewrite: (path) => {
          // If backend already expects /api prefix, avoid duplicating it
          return path
        },
      },
    },
  },
})
