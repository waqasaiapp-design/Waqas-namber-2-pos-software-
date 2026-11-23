import express from 'express';
import path from 'path';
import fs from 'fs';
import {
  createApiApp,
  startBackgroundMaintenanceTimers,
  closeAllSseClients,
  getRuntimeInfo,
} from './serverApp.ts';
import { APP_VERSION, APP_BUILD_ID } from './src/version.ts';

let unhandledHooksRegistered = false;
function registerProcessStabilityHandlers() {
  if (unhandledHooksRegistered) return;
  unhandledHooksRegistered = true;
  process.on('unhandledRejection', (reason) => {
    console.error('[Server] Unhandled promise rejection:', reason);
  });
  process.on('uncaughtException', (err) => {
    console.error('[Server] Uncaught exception:', err);
  });
}

async function start() {
  registerProcessStabilityHandlers();

  const PORT = process.env.PORT || 3000;
  const app = createApiApp();

  // Instant readiness probe for Cloud Run / load balancers (does not touch frontend bundle)
  app.get('/healthz', (_req, res) => {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.json({ ok: true, status: 'healthy', version: APP_VERSION, buildId: APP_BUILD_ID });
  });

  // PWA Manifest with explicit application/manifest+json MIME type
  app.get(['/manifest.json', '/manifest.webmanifest'], (_req, res) => {
    const manifestPath = path.resolve(process.cwd(), 'public', 'manifest.json');
    res.setHeader('Content-Type', 'application/manifest+json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, max-age=0');
    if (fs.existsSync(manifestPath)) {
      return res.sendFile(manifestPath);
    }
    return res.status(404).json({ success: false, message: 'Manifest not found' });
  });

  // PWA Service Worker with Service-Worker-Allowed header
  app.get('/sw.js', (_req, res) => {
    const swPath = path.resolve(process.cwd(), 'public', 'sw.js');
    res.setHeader('Content-Type', 'text/javascript; charset=utf-8');
    res.setHeader('Service-Worker-Allowed', '/');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    if (fs.existsSync(swPath)) {
      return res.sendFile(swPath);
    }
    return res.status(404).send('// Service worker not found');
  });

  const distPath = path.resolve(process.cwd(), 'dist');
  const distIndexPath = path.join(distPath, 'index.html');
  const hasDistBuild = fs.existsSync(distIndexPath);
  const isExplicitDev =
    process.env.NODE_ENV === 'development' ||
    process.env.npm_lifecycle_event === 'dev';
  const shouldServeDist =
    hasDistBuild && (process.env.NODE_ENV === 'production' || !isExplicitDev);

  if (shouldServeDist) {
    console.log(
      `[Server] Production mode v${APP_VERSION} (${APP_BUILD_ID}): serving prebuilt static assets from /dist`
    );
    app.use(
      express.static(distPath, {
        setHeaders: (res, filePath) => {
          if (filePath.endsWith('.html') || filePath.endsWith('sw.js')) {
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
          }
        },
      })
    );
    app.get('*', (_req, res) => {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.sendFile(distIndexPath);
    });
  } else {
    console.log('[Server] Development mode: mounting Vite middlewares');
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: process.env.DISABLE_HMR !== 'true',
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  }

  startBackgroundMaintenanceTimers();

  const runtime = getRuntimeInfo();
  const httpServer = app.listen(Number(PORT), '0.0.0.0', () => {
    console.log(
      `[WAQAS MART] Multi-Device Full-Stack Server v${APP_VERSION} (${runtime.storageMode}) listening on http://0.0.0.0:${PORT}`
    );
  });

  httpServer.keepAliveTimeout = 65000;
  httpServer.headersTimeout = 66000;

  let shuttingDown = false;
  const gracefulShutdown = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[Server] Received ${signal}, shutting down gracefully...`);
    try {
      closeAllSseClients();
    } catch {}
    httpServer.close(() => {
      process.exit(0);
    });
    const forceTimer = setTimeout(() => process.exit(0), 4000);
    forceTimer.unref?.();
  };

  process.once('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.once('SIGINT', () => gracefulShutdown('SIGINT'));
}

start().catch((err) => {
  console.error('[Server] Fatal startup error:', err);
  process.exit(1);
});
