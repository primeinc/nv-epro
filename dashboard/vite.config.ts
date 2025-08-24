import { defineConfig } from 'vite';

const repo = (process.env.GITHUB_REPOSITORY || '').split('/')[1] || '';
// If building for GitHub Pages repo site, base must be '/<repo>/'
const isCI = !!process.env.GITHUB_ACTIONS;
const base = isCI && repo ? `/${repo}/` : '/';

export default defineConfig({
  server: { port: 5173 },
  build: { outDir: 'dist' },
  base
});