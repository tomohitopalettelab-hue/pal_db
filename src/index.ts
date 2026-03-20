import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import type { Request, Response } from 'express';
import multer from 'multer';
import path from 'path';
import { existsSync, mkdirSync, createReadStream } from 'fs';
import { promises as fs } from 'fs';
import { fileURLToPath } from 'url';
import { renderWithFFmpeg } from './ffmpeg-renderer.js';
import {
  deleteAccountStatusOption,
  deleteAccount,
  deleteContract,
  deleteContractOption,
  deletePlan,
  deleteServiceSubscription,
  deleteMediaAsset,
  ensureTables,
  createMediaAsset,
  getPaletteServices,
  getPaletteSummary,
  hasChatLoginId,
  getMediaAssetById,
  listAccountStatusOptions,
  listAccounts,
  listContracts,
  listContractOptions,
  listMediaAssets,
  listPlans,
  listPalVideoJobs,
  listServiceSubscriptions,
  getPalVideoJob,
  upsertAccountStatusOption,
  upsertAccount,
  upsertContract,
  upsertContractOption,
  upsertPlan,
  upsertPalVideoJob,
  upsertServiceSubscription,
  verifyChatLogin,
} from './store.js';

dotenv.config();

if (!process.env.POSTGRES_URL) {
  process.env.POSTGRES_URL =
    process.env.DATABASE_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    process.env.POSTGRES_URL_NON_POOLING ||
    '';
}

const app = express();
const port = Number(process.env.PORT || 3100);
const corsOrigin = process.env.CORS_ORIGIN || '*';
const normalizeOrigin = (value: string) => value.trim().replace(/\/$/, '');

const corsOriginList = corsOrigin === '*'
  ? '*'
  : corsOrigin
      .split(',')
      .map((origin) => normalizeOrigin(origin))
      .filter(Boolean);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.resolve(__dirname, '../public');
const PALETTE_ID_REGEX = /^[A-Z][0-9]{4}$/;
const mediaRootDir = process.env.PAL_DB_MEDIA_DIR
  ? path.resolve(process.env.PAL_DB_MEDIA_DIR)
  : path.join(publicDir, 'media');
const mediaUploadMaxMb = Number(process.env.MEDIA_UPLOAD_MAX_MB || 12);
const mediaUploadMaxBytes = Math.max(mediaUploadMaxMb, 1) * 1024 * 1024;
const allowedMediaMimeTypes = new Set([
  'image/jpeg',
  'image/png',
  'image/webp',
  'image/heic',
  'image/heif',
  'video/mp4',
  'video/quicktime',
]);

app.use(cors({
  origin: (origin, callback) => {
    if (!origin || corsOriginList === '*') {
      callback(null, true);
      return;
    }
    const normalizedOrigin = normalizeOrigin(origin);
    if (Array.isArray(corsOriginList) && corsOriginList.includes(normalizedOrigin)) {
      callback(null, normalizedOrigin);
      return;
    }
    console.warn('[pal-db] blocked CORS origin:', normalizedOrigin);
    callback(null, false);
  },
}));
app.use(express.json({ limit: '2mb' }));
app.use(express.static(publicDir));
app.use('/media', express.static(mediaRootDir));

const normalizePaletteIdInput = (raw: unknown): string => {
  const value = String(raw || '').trim().toUpperCase();
  if (!PALETTE_ID_REGEX.test(value)) {
    throw new Error('paletteId must be 1 alphabet letter + 4 digits (例: A0001)');
  }
  return value;
};

const mediaStorage = multer.diskStorage({
  destination: (req, _file, cb) => {
    try {
      const paletteId = normalizePaletteIdInput(req.body?.paletteId || req.query?.paletteId);
      const targetDir = path.join(mediaRootDir, paletteId);
      if (!existsSync(targetDir)) {
        mkdirSync(targetDir, { recursive: true });
      }
      cb(null, targetDir);
    } catch (error) {
      cb(error as Error, mediaRootDir);
    }
  },
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname || '').toLowerCase();
    const safeExt = ext && ext.length <= 10 ? ext : '';
    const unique = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    cb(null, `${unique}${safeExt}`);
  },
});

const uploadMedia = multer({
  storage: mediaStorage,
  limits: { fileSize: mediaUploadMaxBytes },
  fileFilter: (_req, file, cb) => {
    const type = String(file.mimetype || '').toLowerCase();
    if (allowedMediaMimeTypes.has(type) || type.startsWith('image/') || type.startsWith('video/')) {
      cb(null, true);
      return;
    }
    cb(new Error('unsupported media type'));
  },
});

const uploadSingleMedia = (req: Request, res: Response, next: () => void) => {
  uploadMedia.single('file')(req, res, (err) => {
    if (err) {
      const message = err instanceof Error ? err.message : 'upload failed';
      res.status(400).json({ success: false, error: message });
      return;
    }
    next();
  });
};

const getPublicBaseUrl = (req: Request): string => {
  const configured = process.env.PAL_DB_PUBLIC_BASE_URL?.trim();
  if (configured) return configured.replace(/\/$/, '');
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = forwardedProto || req.protocol;
  const host = req.get('host');
  return host ? `${proto}://${host}` : '';
};

const CREATOMATE_API_URL = 'https://api.creatomate.com/v1/renders';
const CREATOMATE_API_KEY = String(process.env.CREATOMATE_API_KEY || '').trim();

/**
 * Creatomate は非同期レンダリング (202 Accepted)。
 * POST 直後の URL はまだファイルが存在しない "予約済みURL"。
 * このヘルパーで status === 'succeeded' になるまでポーリングし、
 * 実際にファイルが書き込まれた後の URL を返す。
 */
const pollCreatomateRender = async (renderId: string, maxWaitMs = 75000): Promise<Record<string, unknown> | null> => {
  const deadline = Date.now() + maxWaitMs;
  const intervalMs = 3000;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, intervalMs));
    try {
      const res = await fetch(`${CREATOMATE_API_URL}/${renderId}`, {
        headers: { Authorization: `Bearer ${CREATOMATE_API_KEY}` },
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) break;
      const item = (await res.json()) as Record<string, unknown>;
      console.log('[pal-db] creatomate poll', { renderId, status: item.status });
      if (item.status === 'succeeded' || item.status === 'failed') return item;
    } catch (e) {
      console.warn('[pal-db] creatomate poll error', e);
    }
  }
  return null;
};

// Dimensions per destination (投稿先)
const DESTINATION_DIMENSIONS: Record<string, [number, number]> = {
  instagram_reel:   [1080, 1920],
  instagram_story:  [1080, 1920],
  tiktok:           [1080, 1920],
  youtube_short:    [1080, 1920],
  line_voom:        [1080, 1350],
  x_twitter:        [1080, 1350],
  facebook:         [1080, 1350],
  instagram_feed:   [1080, 1080],
  youtube:          [1920, 1080],
  web_banner:       [1920, 1080],
};

// BGM tracks (royalty-free, SoundHelix — external hotlink allowed)
// Pixabay CDN blocks requests from render servers (403), so we use SoundHelix instead.
const BGM_URL_MAP: Record<string, string> = {
  bright_pop:    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3',
  cool_minimal:  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-9.mp3',
  cinematic:     'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-7.mp3',
  natural_warm:  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-2.mp3',
};

// カットの総尺を計算（BGM duration に渡す）
const calcPayloadDuration = (cuts: any[], defaultCutDur = 5): number =>
  Array.isArray(cuts) && cuts.length > 0
    ? cuts.reduce((acc: number, c: any) => acc + Number(c.duration || c.durationSec || defaultCutDur), 0)
    : defaultCutDur;

// ─────────────────────────────────────────────────────────────────────────────
// Module-level resolver functions (shared across template builders)
// ─────────────────────────────────────────────────────────────────────────────

const resolveSceneTransition = (transition: string, idx: number, colorPrimary: string, colorAccent: string) => {
  const t = String(transition || '').toLowerCase();
  if (t === 'none') return { animations: [], exit_animations: [] };

  if (t === 'fade') return {
    animations:      [{ type: 'fade', duration: 0.6, easing: 'quadratic-out' }],
    exit_animations: [{ type: 'fade', duration: 0.45, easing: 'quadratic-in' }],
  };
  if (t === 'slide') {
    const dirs = ['left', 'up', 'right', 'left', 'up'] as const;
    return {
      animations:      [{ type: 'slide', direction: dirs[idx % dirs.length], duration: 0.5, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'slide', direction: dirs[(idx + 2) % dirs.length], duration: 0.4, easing: 'quadratic-in' }],
    };
  }
  if (t === 'zoom') {
    const scales = ['112%', '88%', '115%', '85%'];
    const easings = ['back-out', 'elastic-out', 'quadratic-out', 'back-out'];
    return {
      animations:      [{ type: 'scale', start_scale: scales[idx % 4], end_scale: '100%', fade: true, duration: 0.65, easing: easings[idx % 4] }],
      exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: idx % 2 === 0 ? '92%' : '108%', fade: true, duration: 0.4, easing: 'quadratic-in' }],
    };
  }
  if (t === 'wipe') {
    const dirs = ['right', 'up', 'left', 'up', 'right'] as const;
    return {
      animations:      [{ type: 'wipe', direction: dirs[idx % dirs.length], duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }],
    };
  }
  if (t === 'color-wipe') {
    const dirs  = ['right', 'up', 'left', 'down', 'right'] as const;
    const color = idx % 2 === 0 ? colorAccent : colorPrimary;
    return {
      animations:      [{ type: 'color-wipe', direction: dirs[idx % dirs.length], color, duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }],
    };
  }
  if (t === 'flip') {
    const rots = ['-18°', '18°', '-22°', '22°'];
    return {
      animations:      [{ type: 'spin', rotation: rots[idx % 4], fade: true, duration: 0.7, easing: 'back-out' }],
      exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '90%', fade: true, duration: 0.4, easing: 'quadratic-in' }],
    };
  }
  if (t === 'blur') return {
    animations:      [{ type: 'scale', start_scale: '107%', end_scale: '100%', fade: true, duration: 0.65, easing: 'quadratic-out' }],
    exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '94%', fade: true, duration: 0.4, easing: 'quadratic-in' }],
  };
  if (t === 'bounce') return {
    animations:      [{ type: 'scale', start_scale: '75%', end_scale: '100%', fade: true, duration: 0.75, easing: 'elastic-out' }],
    exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '88%', fade: true, duration: 0.35, easing: 'quadratic-in' }],
  };
  if (t === 'push') {
    const dirsIn  = ['left', 'up', 'right', 'down'] as const;
    const dirsOut = ['right', 'down', 'left', 'up'] as const;
    return {
      animations:      [{ type: 'slide', direction: dirsIn[idx % 4],  duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'slide', direction: dirsOut[idx % 4], duration: 0.45, easing: 'quadratic-in' }],
    };
  }
  if (t === 'film-roll') return {
    animations:      [{ type: 'film-roll', direction: idx % 2 === 0 ? 'left' : 'right', duration: 0.7, easing: 'quadratic-out' }],
    exit_animations: [{ type: 'fade', duration: 0.3, easing: 'quadratic-in' }],
  };
  if (t === 'circular') return {
    animations:      [{ type: 'circular-wipe', direction: 'in', duration: 0.65, easing: 'quadratic-out' }],
    exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }],
  };
  if (t === 'stripe') return {
    animations:      [{ type: 'stripe', direction: idx % 2 === 0 ? 'right' : 'up', duration: 0.6, easing: 'quadratic-out' }],
    exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }],
  };
  // Auto: dramatic 16-pattern cycle
  const AUTO = [
    // 0: color-wipe accent right — bold entrance
    { animations: [{ type: 'color-wipe', direction: 'right', color: colorAccent, duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }] },
    // 1: elastic zoom in from small — energetic pop
    { animations: [{ type: 'scale', start_scale: '72%', end_scale: '100%', fade: true, duration: 0.8, easing: 'elastic-out' }],
      exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '92%', fade: true, duration: 0.4, easing: 'quadratic-in' }] },
    // 2: slide from left — clean modern
    { animations: [{ type: 'slide', direction: 'left', duration: 0.5, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'slide', direction: 'right', duration: 0.4, easing: 'quadratic-in' }] },
    // 3: color-wipe primary up — brand statement
    { animations: [{ type: 'color-wipe', direction: 'up', color: colorPrimary, duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }] },
    // 4: back-out zoom from large — cinematic reveal
    { animations: [{ type: 'scale', start_scale: '118%', end_scale: '100%', fade: true, duration: 0.7, easing: 'back-out' }],
      exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '96%', fade: true, duration: 0.45, easing: 'quadratic-in' }] },
    // 5: slide up — dynamic upward motion
    { animations: [{ type: 'slide', direction: 'up', duration: 0.5, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }] },
    // 6: spin rotation — dramatic flip
    { animations: [{ type: 'spin', rotation: '-14°', fade: true, duration: 0.65, easing: 'back-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }] },
    // 7: scale punch from tiny — very energetic
    { animations: [{ type: 'scale', start_scale: '60%', end_scale: '100%', fade: true, duration: 0.75, easing: 'elastic-out' }],
      exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '88%', fade: true, duration: 0.4, easing: 'quadratic-in' }] },
    // 8: wipe right — clean wipe
    { animations: [{ type: 'wipe', direction: 'right', duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }] },
    // 9: color-wipe accent down — vertical brand
    { animations: [{ type: 'color-wipe', direction: 'down', color: colorAccent, duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }] },
    // 10: slide from right — counter flow
    { animations: [{ type: 'slide', direction: 'right', duration: 0.5, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'slide', direction: 'left', duration: 0.4, easing: 'quadratic-in' }] },
    // 11: zoom out back-in — stylish pull-back
    { animations: [{ type: 'scale', start_scale: '88%', end_scale: '100%', fade: true, duration: 0.7, easing: 'back-out' }],
      exit_animations: [{ type: 'scale', start_scale: '100%', end_scale: '108%', fade: true, duration: 0.4, easing: 'quadratic-in' }] },
    // 12: spin reverse — cinematic
    { animations: [{ type: 'spin', rotation: '16°', fade: true, duration: 0.65, easing: 'back-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }] },
    // 13: color-wipe primary left
    { animations: [{ type: 'color-wipe', direction: 'left', color: colorPrimary, duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }] },
    // 14: elastic bounce in
    { animations: [{ type: 'scale', start_scale: '78%', end_scale: '100%', fade: true, duration: 0.8, easing: 'elastic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }] },
    // 15: wipe up — upward sweep
    { animations: [{ type: 'wipe', direction: 'up', duration: 0.55, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }] },
  ];
  return AUTO[idx % AUTO.length];
};

const resolveTitleAnim = (animation: string, idx: number, layout: string): any[] => {
  const a = String(animation || '').toLowerCase();
  if (a === 'none')       return [];
  if (a === 'fade')       return [{ type: 'fade', duration: 0.7, easing: 'quadratic-out' }];
  if (a === 'zoom')       return [{ type: 'scale', start_scale: '82%', end_scale: '100%', fade: true, duration: 0.7, easing: 'back-out' }];
  if (a === 'pop')        return [{ type: 'scale', start_scale: '68%', end_scale: '100%', fade: true, duration: 0.65, easing: 'back-out' }];
  if (a === 'elastic')    return [{ type: 'scale', start_scale: '55%', end_scale: '100%', fade: true, duration: 0.85, easing: 'elastic-out' }];
  if (a === 'blur')       return [{ type: 'scale', start_scale: '96%', end_scale: '100%', fade: true, duration: 0.85, easing: 'quadratic-out' }]; // blur→scale+fade (blur unsupported on text)
  if (a === 'wipe')       return [{ type: 'wipe',  direction: 'right', duration: 0.65, easing: 'quadratic-out' }];
  if (a === 'rise')       return [{ type: 'slide', direction: 'up', distance: '22%', fade: true, duration: 0.75, easing: 'quadratic-out' }];
  if (a === 'drop')       return [{ type: 'slide', direction: layout === 'top' || layout === 'billboard' ? 'down' : 'up', distance: '15%', fade: true, duration: 0.65, easing: 'back-out' }];
  if (a === 'typewriter') return [{ type: 'text-typewriter', duration: 0.8 }];
  if (a === 'text-slide') return [{ type: 'text-slide', direction: 'up', duration: 0.65, easing: 'back-out' }];
  if (a === 'spin')       return [{ type: 'spin', rotation: '-12°', fade: true, duration: 0.7, easing: 'back-out' }];

  // 'slide' or default — rich auto-cycle of 12 distinct animations based on layout+idx
  const AUTO_TITLE: any[][] = [
    [{ type: 'slide', direction: 'up',   distance: '12%', fade: true, duration: 0.6, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '70%', end_scale: '100%', fade: true, duration: 0.7, easing: 'elastic-out' }],
    [{ type: 'text-slide', direction: 'up', duration: 0.65, easing: 'back-out' }],
    [{ type: 'slide', direction: 'left', distance: '10%', fade: true, duration: 0.6, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '85%', end_scale: '100%', fade: true, duration: 0.65, easing: 'back-out' }],
    [{ type: 'wipe',  direction: 'right', duration: 0.65, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '92%', end_scale: '100%', fade: true, duration: 0.75, easing: 'quadratic-out' }], // was blur
    [{ type: 'slide', direction: 'right', distance: '10%', fade: true, duration: 0.6, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '58%', end_scale: '100%', fade: true, duration: 0.8, easing: 'elastic-out' }],
    [{ type: 'spin',  rotation: '-10°', fade: true, duration: 0.65, easing: 'back-out' }],
    [{ type: 'slide', direction: 'down', distance: '8%',  fade: true, duration: 0.6, easing: 'quadratic-out' }],
    [{ type: 'text-slide', direction: 'left', duration: 0.6, easing: 'quadratic-out' }],
  ];
  const baseIdx = layout === 'center' ? idx + 2
    : layout === 'billboard' ? idx + 4
    : layout === 'caption'   ? idx + 6
    : layout === 'top'       ? idx + 8
    : idx;
  return AUTO_TITLE[baseIdx % AUTO_TITLE.length];
};

const resolveSubAnim = (animation: string, idx: number): any[] => {
  const a = String(animation || '').toLowerCase();
  if (a === 'none')    return [];
  if (a === 'blur')    return [{ type: 'scale', start_scale: '95%', end_scale: '100%', fade: true, duration: 0.7, easing: 'quadratic-out' }]; // blur→scale+fade
  if (a === 'zoom')    return [{ type: 'scale', start_scale: '88%', end_scale: '100%', fade: true, duration: 0.6, easing: 'back-out' }];
  if (a === 'pop')     return [{ type: 'scale', start_scale: '80%', end_scale: '100%', fade: true, duration: 0.55, easing: 'back-out' }];
  if (a === 'elastic') return [{ type: 'scale', start_scale: '72%', end_scale: '100%', fade: true, duration: 0.65, easing: 'elastic-out' }];
  if (a === 'fade')    return [{ type: 'fade',  duration: 0.7, easing: 'quadratic-out' }];
  if (a === 'rise')    return [{ type: 'slide', direction: 'up',   distance: '14%', fade: true, duration: 0.65, easing: 'quadratic-out' }];
  if (a === 'wipe')    return [{ type: 'wipe',  direction: 'right', duration: 0.6, easing: 'quadratic-out' }];
  // rich auto-cycle: 8 distinct sub-animations
  const SUB_AUTO: any[][] = [
    [{ type: 'slide', direction: 'up',    distance: '8%',  fade: true, duration: 0.55, easing: 'quadratic-out' }],
    [{ type: 'fade',  duration: 0.7, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '93%', end_scale: '100%', fade: true, duration: 0.6, easing: 'quadratic-out' }], // was blur
    [{ type: 'slide', direction: 'left',  distance: '6%',  fade: true, duration: 0.55, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '90%', end_scale: '100%', fade: true, duration: 0.55, easing: 'back-out' }],
    [{ type: 'wipe',  direction: 'right', duration: 0.55, easing: 'quadratic-out' }],
    [{ type: 'slide', direction: 'right', distance: '6%',  fade: true, duration: 0.55, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '96%', end_scale: '100%', fade: true, duration: 0.5, easing: 'back-out' }],    // was blur
  ];
  return SUB_AUTO[idx % SUB_AUTO.length];
};

// ── Ken Burns: cinematic pan + zoom (more dramatic for professional feel) ────
const resolveKenBurns = (idx: number, dur: number): Record<string, any> => {
  // Alternating zoom-in and zoom-out patterns — more dramatic range (14-20%)
  const scales: [string, string][] = [
    ['100%', '119%'], // slow zoom in
    ['120%', '102%'], // dramatic zoom out
    ['100%', '117%'], // gentle zoom in
    ['118%', '100%'], // pull back
    ['102%', '120%'], // zoom in from slightly wide
    ['119%', '100%'], // zoom out to original
    ['100%', '116%'], // classic zoom in
    ['117%', '101%'], // subtle zoom out
  ];
  const [s0, s1] = scales[idx % scales.length];

  // Pan: more noticeable drift (3.5% range) for visible motion
  const pans = [
    { x0: '50%', y0: '50%',  x1: '53.5%', y1: '47.5%' }, // drift right+up
    { x0: '53%', y0: '53%',  x1: '49.5%', y1: '50.5%' }, // drift left+down
    { x0: '48%', y0: '51%',  x1: '51.5%', y1: '48.5%' }, // drift right+up
    { x0: '52%', y0: '48%',  x1: '49%',   y1: '52%'   }, // drift left+down
    { x0: '50%', y0: '53%',  x1: '53%',   y1: '49.5%' }, // drift right+up
    { x0: '52%', y0: '50%',  x1: '48.5%', y1: '52.5%' }, // drift left+down
    { x0: '48%', y0: '48%',  x1: '51.5%', y1: '51.5%' }, // diagonal drift
    { x0: '53%', y0: '52%',  x1: '50%',   y1: '48%'   }, // drift left+up
  ];
  const p = pans[idx % pans.length];
  const t = `${dur} s`;
  return {
    x:       [{ time: '0 s', value: p.x0 }, { time: t, value: p.x1, easing: 'quintic-in-out' }],
    y:       [{ time: '0 s', value: p.y0 }, { time: t, value: p.y1, easing: 'quintic-in-out' }],
    x_scale: [{ time: '0 s', value: s0   }, { time: t, value: s1,   easing: 'quadratic-in-out' }],
    y_scale: [{ time: '0 s', value: s0   }, { time: t, value: s1,   easing: 'quadratic-in-out' }],
  };
};

// ─────────────────────────────────────────────────────────────────────────────
// Collage template builder (白背景 / ポラロイドグリッド / Canvaスタイル)
// ─────────────────────────────────────────────────────────────────────────────
const buildCollageInlineSource = (payload: Record<string, unknown>) => {
  const destination = String(payload?.destination || payload?.purpose || 'instagram_story');
  const dims = (DESTINATION_DIMENSIONS[destination] || [1080, 1920]) as [number, number];
  const [w, h] = dims;
  const isVertical = h > w;
  const isWide = w > h;

  const bgColor     = String(payload?.bgColor     || '#FAF8F5');
  const textColor   = String(payload?.textColor   || '#1C1C1C');
  const accentColor = String(payload?.colorAccent || payload?.accentColor || '#9C7B5C');
  const bgmRaw = String(payload?.bgm || '');
  const bgmUrl = bgmRaw.startsWith('http') ? bgmRaw : (BGM_URL_MAP[bgmRaw] || '');

  const rawCuts: any[] = Array.isArray(payload?.cuts) && (payload.cuts as any[]).length > 0
    ? payload.cuts as any[]
    : [{
        mainText: String(payload?.mainText || payload?.telopMain || ''),
        subText:  String(payload?.subText  || payload?.telopSub  || ''),
        caption:  String(payload?.caption  || ''),
        images:   Array.isArray(payload?.images) ? payload.images : [payload?.imageUrl].filter(Boolean),
        duration: Number(payload?.duration || 9),
      }];

  // ── Card & image dimensions ────────────────────────────────────────────────
  const cardW = isVertical ? '44%' : '38%';
  const cardH = isVertical ? '29%' : '42%';
  const imgW  = isVertical ? '42%' : '36%';
  const imgH  = isVertical ? '27%' : '39%';

  // ── 2×2 grid positions (center of each card) ─────────────────────────────
  const GRID_POSITIONS = isVertical ? [
    { x: '25%', y: '14.5%' }, { x: '75%', y: '14.5%' },
    { x: '25%', y: '75.5%' }, { x: '75%', y: '75.5%' },
  ] : [
    { x: '22%', y: '25%' }, { x: '66%', y: '25%' },
    { x: '22%', y: '75%' }, { x: '66%', y: '75%' },
  ];

  const scenes = rawCuts.map((cut: any, i: number) => {
    const dur      = Number(cut.duration || cut.durationSec || 9);
    const mainText = String(cut.mainText || cut.title || cut.textMain || '');
    const subText  = String(cut.subText  || cut.subtitle || cut.textSub || '');
    const caption  = String(cut.caption  || '');
    const rawImgs  = Array.isArray(cut.images) ? cut.images : cut.imageUrl ? [cut.imageUrl] : [];
    const imgs     = rawImgs.map((u: any) => String(u || '')).filter((u: string) => u.startsWith('http'));
    const elements: any[] = [];

    // ── Background (cream/white) ────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 1, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: bgColor,
      width: '100%', height: '100%',
      x: '50%', y: '50%', x_anchor: '50%', y_anchor: '50%',
    });

    // ── 2×2 Polaroid Photo Grid ───────────────────────────────────────────────
    // Top row (pi=0,1) slides down from above; bottom row (pi=2,3) slides up from below
    // Top row appears at t=0, bottom row at t=0.18 (stagger)
    GRID_POSITIONS.forEach((pos, pi) => {
      const isBottomRow = pi >= 2;
      const delay   = isBottomRow ? 0.18 : 0;
      const imgUrl  = imgs[pi] || '';
      const hasPic  = imgUrl.length > 0;
      const slideDir: string = isBottomRow ? 'up' : 'down';

      // White polaroid card frame
      elements.push({
        type: 'shape', track: 2 + pi, time: delay,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: '#FFFFFF',
        width: cardW, height: cardH,
        x: pos.x, y: pos.y, x_anchor: '50%', y_anchor: '50%',
        border_radius: 12,
        shadow_color: 'rgba(0,0,0,0.10)',
        shadow_blur: 20, shadow_x: 0, shadow_y: 6,
        animations: [{ type: 'slide', direction: slideDir, distance: isVertical ? '2%' : '3%', fade: true, duration: 0.5, easing: 'quadratic-out' }],
      });

      // Photo image (or warm-gray placeholder)
      elements.push({
        ...(hasPic
          ? { type: 'image', source: imgUrl, dynamic: true, fill_mode: 'cover' }
          : { type: 'shape', path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z', fill_color: '#E0D5CB' }
        ),
        track: 6 + pi, time: delay,
        width: imgW, height: imgH,
        x: pos.x, y: pos.y, x_anchor: '50%', y_anchor: '50%',
        border_radius: 8,
        animations: [{ type: 'slide', direction: slideDir, distance: isVertical ? '2%' : '3%', fade: true, duration: 0.5, easing: 'quadratic-out' }],
      });
    });

    // ── Center Text Band ─────────────────────────────────────────────────────
    // Thin separator lines above/below text zone
    if (isVertical) {
      elements.push({
        type: 'shape', track: 11, time: 0.25,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: 'rgba(0,0,0,0.12)', width: '72%', height: '0.2 vmin',
        x: '50%', y: '43.8%', x_anchor: '50%', y_anchor: '50%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.55, easing: 'quadratic-out' }],
      });
      elements.push({
        type: 'shape', track: 12, time: 0.28,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: 'rgba(0,0,0,0.12)', width: '72%', height: '0.2 vmin',
        x: '50%', y: '56.2%', x_anchor: '50%', y_anchor: '50%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.55, easing: 'quadratic-out' }],
      });
    }

    // subText — small label (thin weight, wide letter-spacing)
    if (subText) {
      elements.push({
        type: 'text', track: 13, time: 0.30,
        text: subText, dynamic: true,
        x: '50%', y: isVertical ? '46.8%' : '46.8%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '76%' : '68%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '3.2 vmin' : '2.8 vmin',
        font_weight: '300',
        fill_color: textColor,
        letter_spacing: 3,
        text_align: 'center',
        animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-out' }],
      });
    }

    // mainText — big bold title (word-by-word slide-up)
    if (mainText) {
      elements.push({
        type: 'text', track: 14, time: 0.40,
        text: mainText, dynamic: true,
        x: '50%', y: isVertical ? '52%' : '52%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '82%' : '75%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '8.5 vmin' : '7 vmin',
        font_weight: '700',
        fill_color: textColor,
        letter_spacing: isVertical ? 6 : 4,
        text_align: 'center',
        animations: [{ type: 'text-slide', direction: 'up', duration: 0.6, easing: 'back-out' }],
      });
    }

    // caption — small bottom text
    if (caption) {
      elements.push({
        type: 'text', track: 15, time: 0.55,
        text: caption, dynamic: true,
        x: '50%', y: isWide ? '88%' : '88.5%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '70%' : '60%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '2.8 vmin' : '2.4 vmin',
        font_weight: '300',
        fill_color: 'rgba(28,28,28,0.60)',
        letter_spacing: 1,
        text_align: 'center',
        animations: [{ type: 'fade', duration: 0.5, easing: 'quadratic-out' }],
      });
      // Thin accent line below caption
      elements.push({
        type: 'shape', track: 16, time: 0.65,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: accentColor,
        width: '14%', height: '0.3 vmin',
        x: '50%', y: '91%', x_anchor: '50%', y_anchor: '50%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.5, easing: 'quadratic-out' }],
      });
    }

    const timeStart = rawCuts.slice(0, i).reduce((acc: number, c: any) => acc + Number(c.duration || 9), 0);
    return {
      type: 'composition', track: i + 1, time: timeStart, duration: dur,
      animations:      [{ type: 'fade', duration: 0.35, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in'  }],
      elements,
    };
  });

  const rootElements: any[] = [...scenes];
  if (bgmUrl) {
    rootElements.push({ name: 'bgm_track', type: 'audio', track: 90, time: 0, source: bgmUrl, duration: calcPayloadDuration(rawCuts), audio_fade_out: Math.min(1.5, calcPayloadDuration(rawCuts) * 0.05) });
  }

  return { output_format: 'mp4', width: w, height: h, frame_rate: 30, elements: rootElements };
};

// ─────────────────────────────────────────────────────────────────────────────
// Magazine template builder (サイドパネルマガジンスタイル)
// ─────────────────────────────────────────────────────────────────────────────
const buildMagazineInlineSource = (payload: Record<string, unknown>) => {
  const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
  const dims = (DESTINATION_DIMENSIONS[destination] || [1080, 1920]) as [number, number];
  const [w, h] = dims;
  const isVertical = h > w;
  const isWide = w > h;

  const colorPrimary = String(payload?.colorPrimary || '#0D1B2A');
  const colorAccent  = String(payload?.colorAccent  || '#E94560');
  const textColor    = String(payload?.textColor    || '#FFFFFF');
  const bgmRaw = String(payload?.bgm || '');
  const bgmUrl = bgmRaw.startsWith('http') ? bgmRaw : (BGM_URL_MAP[bgmRaw] || '');

  const rawCuts: any[] = Array.isArray(payload?.cuts) && (payload.cuts as any[]).length > 0
    ? payload.cuts as any[]
    : [{ mainText: String(payload?.telopMain || ''), subText: String(payload?.telopSub || ''), duration: 5, imageUrl: '' }];

  const brandName = String(payload?.title || '').toUpperCase() || 'BRAND';

  const scenes = rawCuts.slice(0, 7).map((cut: any, i: number) => {
    const dur       = Number(cut.duration || cut.durationSec || 5);
    const mainText  = String(cut.mainText || cut.title || cut.textMain || '');
    const subText   = String(cut.subText  || cut.subtitle || cut.textSub || '');
    const imgUrl    = String(cut.imageUrl || '').trim();
    const hasImg    = imgUrl.startsWith('http');
    const isLeft    = i % 2 === 0; // alternate panel side each scene
    const timeStart = rawCuts.slice(0, i).reduce((acc: number, c: any) => acc + Number(c.duration || 5), 0);
    const kbProps   = resolveKenBurns(i, dur);
    const elements: any[] = [];

    // ── Full bleed image (Ken Burns) ────────────────────────────────────────
    elements.push(hasImg ? {
      type: 'image', track: 1, time: 0, source: imgUrl, dynamic: true,
      width: '100%', height: '100%', x_anchor: '50%', y_anchor: '50%', fill_mode: 'cover',
      ...kbProps,
    } : {
      type: 'shape', track: 1, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_mode: 'linear',
      fill_color: [{ offset: 0, color: colorPrimary }, { offset: 1, color: colorAccent }],
      fill_x0: '0%', fill_y0: '0%', fill_x1: '100%', fill_y1: '100%',
      width: '100%', height: '100%', dynamic: true,
    });

    // ── Global dark scrim ────────────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 2, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: 'rgba(0,0,0,0.30)',
      width: '100%', height: '100%', x: '50%', y: '50%', x_anchor: '50%', y_anchor: '50%',
    });

    // ── Side color panel ─────────────────────────────────────────────────────
    const panelW = isVertical ? '50%' : '44%';
    const panelX = isLeft ? '0%' : '100%';
    const panelAnchorX = isLeft ? '0%' : '100%';
    elements.push({
      type: 'shape', track: 3, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_mode: 'linear',
      fill_color: [
        { offset: 0, color: colorPrimary + 'F0' },
        { offset: 1, color: colorPrimary + 'C0' },
      ],
      fill_x0: isLeft ? '0%' : '100%', fill_y0: '50%',
      fill_x1: isLeft ? '100%' : '0%', fill_y1: '50%',
      width: panelW, height: '100%',
      x: panelX, y: '0%', x_anchor: panelAnchorX, y_anchor: '0%',
      animations: [{ type: 'slide', direction: isLeft ? 'right' : 'left', distance: '6%', fade: true, duration: 0.6, easing: 'quadratic-out' }],
    });

    // ── Bright accent edge line ──────────────────────────────────────────────
    // panelW is '50%' (vertical) or '44%' (wide). Compute the right-side x numerically.
    const panelWNum = isVertical ? 50 : 44;
    const lineX = isLeft ? `${panelWNum}%` : `${100 - panelWNum}%`;
    elements.push({
      type: 'shape', track: 4, time: 0.18,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: colorAccent,
      width: '0.7 vmin', height: isVertical ? '65%' : '60%',
      x: lineX, y: '50%', x_anchor: '50%', y_anchor: '50%',
      animations: [{ type: 'wipe', direction: 'down', duration: 0.7, easing: 'quadratic-out' }],
    });

    const textX = isLeft ? '25%' : '75%';

    // ── Brand label ──────────────────────────────────────────────────────────
    elements.push({
      type: 'text', track: 5, time: 0.18,
      text: brandName,
      x: textX, y: isVertical ? '11%' : '9%',
      x_anchor: '50%', y_anchor: '50%',
      width: isVertical ? '46%' : '40%',
      font_family: 'Noto Sans JP',
      font_size: '1.8 vmin', font_weight: '200',
      fill_color: colorAccent,
      letter_spacing: 6, text_align: 'center',
      animations: [{ type: 'fade', duration: 0.5, easing: 'quadratic-out' }],
    });

    // ── Scene number ─────────────────────────────────────────────────────────
    elements.push({
      type: 'text', track: 6, time: 0.1,
      text: String(i + 1).padStart(2, '0'),
      x: isLeft ? '90%' : '10%', y: '7%',
      x_anchor: '50%', y_anchor: '50%',
      font_family: 'Noto Sans JP',
      font_size: '2.2 vmin', font_weight: '200',
      fill_color: 'rgba(255,255,255,0.45)',
      letter_spacing: 2,
      animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-out' }],
    });

    // ── Main title ───────────────────────────────────────────────────────────
    if (mainText) {
      elements.push({
        type: 'text', track: 7, time: 0.30,
        text: mainText, dynamic: true,
        x: textX, y: isVertical ? '46%' : '44%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '44%' : '38%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '5.8 vmin' : '4.8 vmin',
        font_weight: '900', fill_color: textColor,
        letter_spacing: 2, text_align: 'center', line_height: 1.25,
        shadow_color: 'rgba(0,0,0,0.4)', shadow_blur: 8, shadow_x: 0, shadow_y: 3,
        animations: [{ type: 'text-slide', direction: 'up', duration: 0.65, easing: 'back-out' }],
      });
    }

    // ── Subtitle ─────────────────────────────────────────────────────────────
    if (subText) {
      elements.push({
        type: 'text', track: 8, time: 0.45,
        text: subText, dynamic: true,
        x: textX, y: isVertical ? '57%' : '55%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '44%' : '38%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '2.8 vmin' : '2.4 vmin',
        font_weight: '300', fill_color: 'rgba(255,255,255,0.80)',
        letter_spacing: 1, text_align: 'center',
        animations: [{ type: 'slide', direction: 'up', distance: '5%', fade: true, duration: 0.55, easing: 'quadratic-out' }],
      });
    }

    // ── Accent wipe line ─────────────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 9, time: 0.5,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: colorAccent,
      width: isVertical ? '22%' : '18%', height: '0.5 vmin',
      x: textX, y: isVertical ? '62%' : '61%', x_anchor: '50%', y_anchor: '50%',
      animations: [{ type: 'wipe', direction: isLeft ? 'right' : 'left', duration: 0.55, easing: 'quadratic-out' }],
    });

    const { animations, exit_animations } = resolveSceneTransition(String(cut.transition || ''), i, colorPrimary, colorAccent);
    return { type: 'composition', track: i + 1, time: timeStart, duration: dur, animations, exit_animations, elements };
  });

  const rootElements: any[] = [...scenes];
  if (bgmUrl) rootElements.push({ name: 'bgm_track', type: 'audio', track: 90, time: 0, source: bgmUrl, duration: calcPayloadDuration(rawCuts), audio_fade_out: Math.min(1.5, calcPayloadDuration(rawCuts) * 0.05) });
  return { output_format: 'mp4', width: w, height: h, frame_rate: 30, elements: rootElements };
};

// ─────────────────────────────────────────────────────────────────────────────
// Minimal template builder (超ミニマル白背景 / 薄い書体 / 余白重視)
// ─────────────────────────────────────────────────────────────────────────────
const buildMinimalInlineSource = (payload: Record<string, unknown>) => {
  const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
  const dims = (DESTINATION_DIMENSIONS[destination] || [1080, 1920]) as [number, number];
  const [w, h] = dims;
  const isVertical = h > w;

  const colorAccent  = String(payload?.colorAccent  || '#888888');
  const textColor    = String(payload?.textColor    || '#1A1A1A');
  const bgColor      = String(payload?.bgColor      || '#F7F5F2');
  const bgmRaw = String(payload?.bgm || '');
  const bgmUrl = bgmRaw.startsWith('http') ? bgmRaw : (BGM_URL_MAP[bgmRaw] || '');

  const rawCuts: any[] = Array.isArray(payload?.cuts) && (payload.cuts as any[]).length > 0
    ? payload.cuts as any[]
    : [{ mainText: String(payload?.telopMain || ''), subText: String(payload?.telopSub || ''), duration: 5, imageUrl: '' }];

  const scenes = rawCuts.slice(0, 7).map((cut: any, i: number) => {
    const dur      = Number(cut.duration || cut.durationSec || 5);
    const mainText = String(cut.mainText || cut.title || cut.textMain || '');
    const subText  = String(cut.subText  || cut.subtitle || cut.textSub || '');
    const imgUrl   = String(cut.imageUrl || '').trim();
    const hasImg   = imgUrl.startsWith('http');
    const timeStart = rawCuts.slice(0, i).reduce((acc: number, c: any) => acc + Number(c.duration || 5), 0);
    const elements: any[] = [];

    // ── Clean background ────────────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 1, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: bgColor,
      width: '100%', height: '100%', x: '50%', y: '50%', x_anchor: '50%', y_anchor: '50%',
    });

    // ── Central image (subtle rounded card) ─────────────────────────────────
    if (hasImg) {
      // Card dimensions (slightly larger than image for white shadow frame effect)
      const imgWNum  = isVertical ? 82 : 70;
      const imgHNum  = isVertical ? 45 : 55;
      const cardWStr = `${imgWNum + 3}%`;
      const cardHStr = `${imgHNum + 2}%`;
      const imgWStr  = `${imgWNum}%`;
      const imgHStr  = `${imgHNum}%`;
      const imgY     = isVertical ? '38%' : '45%';
      // White card shadow behind image
      elements.push({
        type: 'shape', track: 2, time: 0,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: '#FFFFFF',
        width: cardWStr, height: cardHStr,
        x: '50%', y: imgY, x_anchor: '50%', y_anchor: '50%',
        border_radius: 8,
        shadow_color: 'rgba(0,0,0,0.08)', shadow_blur: 24, shadow_x: 0, shadow_y: 8,
        animations: [{ type: 'fade', duration: 0.8, easing: 'quadratic-out' }],
      });
      elements.push({
        type: 'image', track: 3, time: 0, source: imgUrl, dynamic: true,
        width: imgWStr, height: imgHStr,
        x: '50%', y: imgY, x_anchor: '50%', y_anchor: '50%',
        fill_mode: 'cover', border_radius: 6,
        animations: [{ type: 'scale', start_scale: '104%', end_scale: '100%', fade: true, duration: 1.4, easing: 'quadratic-out' }],
      });
    }

    // ── Thin top accent line ─────────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 4, time: 0.2,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: colorAccent,
      width: isVertical ? '10%' : '8%', height: '0.25 vmin',
      x: '50%', y: isVertical ? '5%' : '4%', x_anchor: '50%', y_anchor: '50%',
      animations: [{ type: 'wipe', direction: 'right', duration: 0.7, easing: 'quadratic-out' }],
    });

    // ── Main text — cycling animation for variety ─────────────────────────────
    const textY = hasImg
      ? (isVertical ? '70%' : '78%')
      : (isVertical ? '50%' : '50%');
    const MINIMAL_TITLE_ANIMS: any[][] = [
      [{ type: 'fade',  duration: 1.0, easing: 'quadratic-out' }],
      [{ type: 'slide', direction: 'up', distance: '4%', fade: true, duration: 0.8, easing: 'quadratic-out' }],
      [{ type: 'scale', start_scale: '94%', end_scale: '100%', fade: true, duration: 1.0, easing: 'quadratic-out' }],
      [{ type: 'wipe',  direction: 'right', duration: 0.9, easing: 'quadratic-out' }],
      [{ type: 'scale', start_scale: '96%', end_scale: '100%', fade: true, duration: 0.9, easing: 'quadratic-out' }],
      [{ type: 'slide', direction: 'up', distance: '3%', fade: true, duration: 0.9, easing: 'quadratic-out' }],
    ];
    const MINIMAL_SUB_ANIMS: any[][] = [
      [{ type: 'fade',  duration: 0.8, easing: 'quadratic-out' }],
      [{ type: 'slide', direction: 'up', distance: '3%', fade: true, duration: 0.7, easing: 'quadratic-out' }],
      [{ type: 'fade',  duration: 0.9, easing: 'quadratic-out' }],
      [{ type: 'slide', direction: 'left', distance: '3%', fade: true, duration: 0.7, easing: 'quadratic-out' }],
      [{ type: 'fade',  duration: 0.8, easing: 'quadratic-out' }],
      [{ type: 'scale', start_scale: '96%', end_scale: '100%', fade: true, duration: 0.7, easing: 'quadratic-out' }],
    ];
    if (mainText) {
      elements.push({
        type: 'text', track: 5, time: 0.35,
        text: mainText, dynamic: true,
        x: '50%', y: textY,
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '84%' : '75%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '5.5 vmin' : '4.8 vmin',
        font_weight: '200',
        fill_color: textColor,
        letter_spacing: 8, text_align: 'center', line_height: 1.4,
        animations: MINIMAL_TITLE_ANIMS[i % MINIMAL_TITLE_ANIMS.length],
      });
    }

    // ── Subtitle (very thin, spaced) ─────────────────────────────────────────
    if (subText) {
      elements.push({
        type: 'text', track: 6, time: 0.5,
        text: subText, dynamic: true,
        x: '50%', y: hasImg ? (isVertical ? '79%' : '86%') : (isVertical ? '58%' : '60%'),
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '70%' : '60%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '2.5 vmin' : '2.2 vmin',
        font_weight: '100',
        fill_color: `rgba(26,26,26,0.55)`,
        letter_spacing: 5, text_align: 'center',
        animations: MINIMAL_SUB_ANIMS[i % MINIMAL_SUB_ANIMS.length],
      });
    }

    // ── Bottom thin accent line ──────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 7, time: 0.45,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: colorAccent,
      width: isVertical ? '10%' : '8%', height: '0.25 vmin',
      x: '50%', y: '95%', x_anchor: '50%', y_anchor: '50%',
      animations: [{ type: 'wipe', direction: 'left', duration: 0.7, easing: 'quadratic-out' }],
    });

    // Minimal scene counter
    elements.push({
      type: 'text', track: 8, time: 0.3,
      text: `${i + 1}`,
      x: '90%', y: '6%', x_anchor: '50%', y_anchor: '50%',
      font_family: 'Noto Sans JP',
      font_size: '2.2 vmin', font_weight: '100',
      fill_color: `rgba(26,26,26,0.30)`,
      letter_spacing: 1,
      animations: [{ type: 'fade', duration: 0.5, easing: 'quadratic-out' }],
    });

    const { animations, exit_animations } = resolveSceneTransition(String(cut.transition || ''), i, colorAccent, colorAccent);
    return { type: 'composition', track: i + 1, time: timeStart, duration: dur, animations, exit_animations, elements };
  });

  const rootElements: any[] = [...scenes];
  if (bgmUrl) rootElements.push({ name: 'bgm_track', type: 'audio', track: 90, time: 0, source: bgmUrl, duration: calcPayloadDuration(rawCuts), audio_fade_out: Math.min(1.5, calcPayloadDuration(rawCuts) * 0.05) });
  return { output_format: 'mp4', width: w, height: h, frame_rate: 30, elements: rootElements };
};

const buildCreatomateInlineSource = (payload: Record<string, unknown>) => {
  const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
  const [w, h] = DESTINATION_DIMENSIONS[destination] || [1080, 1920];
  const isVertical = h > w;
  const isSquare   = h === w;
  const isWide     = w > h;

  const colorPrimary = String(payload?.colorPrimary || '#E95464');
  const colorAccent  = String(payload?.colorAccent  || '#1c9a8b');
  const textColor    = String(payload?.textColor    || '#ffffff');
  const bgmRaw = String(payload?.bgm || '');
  const bgmUrl = bgmRaw.startsWith('http') ? bgmRaw : (BGM_URL_MAP[bgmRaw] || '');
  const purpose = String(payload?.purpose || '');

  // ── CTA text based on purpose ─────────────────────────────────────────────────
  const CTA_BY_PURPOSE: Record<string, string> = {
    promotion:   'キャンペーン詳細はプロフィールから  →',
    sns_post:    'フォロー＆チェック  →',
    sns_ad:      '今すぐ詳細を確認  →',
    review:      'あなたも体験してみて  →',
    achievement: '実績の詳細はこちら  →',
  };
  const ctaText = CTA_BY_PURPOSE[purpose] || 'プロフィールをチェック  →';

  // ── 5 Layout types (cycling per scene) ───────────────────────────────────────
  // bottom:    lower-third gradient overlay (Instagram/TikTok classic)
  // top:       upper-third gradient overlay (fresh contrast)
  // center:    full-screen dark overlay + centered statement (cinematic)
  // caption:   solid dark band at bottom with slide-up entry (modern caption style)
  // billboard: giant headline at top, minimal overlay (magazine cover feel)
  type LayoutType = 'bottom' | 'top' | 'center' | 'caption' | 'billboard';
  const LAYOUT_SEQ: LayoutType[] = ['bottom', 'billboard', 'caption', 'center', 'bottom', 'caption', 'bottom'];

  type LayoutProps = {
    overlayMajorH: string | null; overlayMajorY: string; overlayMajorAnchorY: string;
    overlayMinorH: string | null; overlayMinorY: string | null; overlayMinorAnchorY: string | null;
    captionBandH: string | null; captionBandTopY: string | null;
    barX: string | null; barY: string | null; barW: string | null; barH: string | null;
    barAnchorY: string | null; barDir: 'up' | 'down' | null;
    textIndent: string; textWidth: string;
    titleY: string; subY: string; titleAnchorY: string; subAnchorY: string;
    titleSize: string; subSize: string; titleWeight: string; centerX: boolean;
  };

  const getLayoutProps = (layout: LayoutType): LayoutProps => {
    const barW = isVertical ? '1.1 vmin' : '0.75 vmin';
    const baseTextW = isWide ? '52%' : isVertical ? '82%' : '78%';

    if (layout === 'top') {
      return {
        overlayMajorH: isVertical ? '44%' : '47%', overlayMajorY: '0%', overlayMajorAnchorY: '0%',
        overlayMinorH: isVertical ? '22%' : '25%', overlayMinorY: '0%', overlayMinorAnchorY: '0%',
        captionBandH: null, captionBandTopY: null,
        barX: isWide ? '7%' : '10%', barY: isVertical ? '10%' : '9%',
        barW, barH: isWide ? '18%' : '16%', barAnchorY: '0%', barDir: 'down',
        textIndent: isVertical ? '14%' : isWide ? '10%' : '13%', textWidth: baseTextW,
        titleY: isVertical ? '11%' : '10%', subY: isVertical ? '20%' : '19%',
        titleAnchorY: '0%', subAnchorY: '0%',
        titleSize: isVertical ? '5.5 vmin' : isSquare ? '5 vmin' : '5.5 vmin',
        subSize:   isVertical ? '3.0 vmin' : isSquare ? '3.2 vmin' : '3.4 vmin',
        titleWeight: '700', centerX: false,
      };
    }
    if (layout === 'center') {
      return {
        overlayMajorH: '100%', overlayMajorY: '50%', overlayMajorAnchorY: '50%',
        overlayMinorH: null, overlayMinorY: null, overlayMinorAnchorY: null,
        captionBandH: null, captionBandTopY: null,
        barX: null, barY: null, barW: null, barH: null, barAnchorY: null, barDir: null,
        textIndent: '50%', textWidth: isWide ? '72%' : isVertical ? '80%' : '78%',
        titleY: isVertical ? '44%' : '42%', subY: isVertical ? '57%' : '55%',
        titleAnchorY: '50%', subAnchorY: '0%',
        titleSize: isVertical ? '7.5 vmin' : isSquare ? '7 vmin' : '7.5 vmin',
        subSize:   isVertical ? '3.5 vmin' : isSquare ? '3.8 vmin' : '3.8 vmin',
        titleWeight: '900', centerX: true,
      };
    }
    if (layout === 'caption') {
      const bandH = isVertical ? '31%' : '35%';
      // band top = 100% - bandH
      const bandTopY = isVertical ? '69%' : '65%';
      return {
        overlayMajorH: null, overlayMajorY: '100%', overlayMajorAnchorY: '100%',
        overlayMinorH: null, overlayMinorY: null, overlayMinorAnchorY: null,
        captionBandH: bandH, captionBandTopY: bandTopY,
        barX: null, barY: null, barW: null, barH: null, barAnchorY: null, barDir: null,
        textIndent: '50%', textWidth: isWide ? '88%' : isVertical ? '84%' : '84%',
        titleY: isVertical ? '72%' : '68%', subY: isVertical ? '81%' : '78%',
        titleAnchorY: '0%', subAnchorY: '0%',
        titleSize: isVertical ? '5.5 vmin' : isSquare ? '5 vmin' : '5.5 vmin',
        subSize:   isVertical ? '3.0 vmin' : isSquare ? '3.2 vmin' : '3.4 vmin',
        titleWeight: '700', centerX: true,
      };
    }
    if (layout === 'billboard') {
      return {
        overlayMajorH: isVertical ? '58%' : '62%', overlayMajorY: '0%', overlayMajorAnchorY: '0%',
        overlayMinorH: isVertical ? '32%' : '36%', overlayMinorY: '0%', overlayMinorAnchorY: '0%',
        captionBandH: null, captionBandTopY: null,
        barX: null, barY: null, barW: null, barH: null, barAnchorY: null, barDir: null,
        textIndent: '50%', textWidth: isWide ? '88%' : '86%',
        titleY: isVertical ? '8%' : '7%', subY: isVertical ? '21%' : '20%',
        titleAnchorY: '0%', subAnchorY: '0%',
        titleSize: isVertical ? '7.5 vmin' : isSquare ? '7 vmin' : '8 vmin',
        subSize:   isVertical ? '3.8 vmin' : isSquare ? '4 vmin' : '4.5 vmin',
        titleWeight: '900', centerX: true,
      };
    }
    // bottom (default)
    return {
      overlayMajorH: isVertical ? '45%' : '48%', overlayMajorY: '100%', overlayMajorAnchorY: '100%',
      overlayMinorH: isVertical ? '25%' : '28%', overlayMinorY: '100%', overlayMinorAnchorY: '100%',
      captionBandH: null, captionBandTopY: null,
      barX: isWide ? '7%' : '10%',
      barY: isVertical ? '64%' : isSquare ? '59%' : '57%',
      barW, barH: isWide ? '22%' : '20%', barAnchorY: '0%', barDir: 'up',
      textIndent: isVertical ? '14%' : isWide ? '10%' : '13%', textWidth: baseTextW,
      titleY: isVertical ? '66.5%' : isSquare ? '61%' : '59.5%',
      subY:   isVertical ? '73.5%' : isSquare ? '69.5%' : '68%',
      titleAnchorY: '100%', subAnchorY: '0%',
      titleSize: isVertical ? '5.8 vmin' : isSquare ? '5.2 vmin' : '5.8 vmin',
      subSize:   isVertical ? '3.2 vmin' : isSquare ? '3.5 vmin' : '3.6 vmin',
      titleWeight: '700', centerX: false,
    };
  };

  // ── Build cuts ────────────────────────────────────────────────────────────────
  let rawCuts: any[] = [];
  if (Array.isArray(payload?.cuts) && (payload.cuts as any[]).length > 0) {
    rawCuts = payload.cuts as any[];
  } else {
    const plan   = payload?.creatomatePlan as Record<string, unknown> | undefined;
    const scenes = Array.isArray(plan?.scenes) ? plan!.scenes : [];
    rawCuts = scenes.map((s: any) => ({
      duration:   Number(s?.durationSec || 4),
      imageUrl:   String(s?.imageUrl    || ''),
      mainText:   String(s?.title       || s?.textMain || ''),
      subText:    String(s?.subtitle    || s?.textSub  || ''),
      transition: String(s?.textTransition || ''),
      animation:  String(s?.textAnimation  || ''),
    }));
  }

  if (rawCuts.length === 0) {
    rawCuts = [{ duration: 5, imageUrl: '', mainText: String(payload?.telopMain || 'タイトル'), subText: String(payload?.telopSub || ''), transition: 'fade', animation: 'slide' }];
  }

  const totalCuts = Math.min(rawCuts.length, 7);

  // ── Build scene compositions ──────────────────────────────────────────────────
  const sceneCompositions = rawCuts.slice(0, 7).map((cut: any, i: number) => {
    const nn        = String(i + 1).padStart(2, '0');
    const timeStart = rawCuts.slice(0, i).reduce((acc: number, c: any) => acc + Number(c.duration || 4), 0);
    const dur       = Number(cut.duration || cut.durationSec || 4);
    const imgUrl    = String(cut.imageUrl || '').trim();
    const isLastCut = i === totalCuts - 1;
    const isFirstCut = i === 0;
    // Accent cut: every 4th middle cut → brand-color full-bleed, no photo (visual rhythm)
    const isAccentCut = !isFirstCut && !isLastCut && i % 4 === 3;
    const hasImg    = !isAccentCut && imgUrl.startsWith('http');

    // Layout: explicit on cut or cycling sequence
    const validLayouts: string[] = ['bottom', 'top', 'center', 'caption', 'billboard'];
    const layout: LayoutType = isAccentCut
      ? 'center'
      : validLayouts.includes(String(cut.layout || ''))
      ? (cut.layout as LayoutType)
      : LAYOUT_SEQ[i % LAYOUT_SEQ.length];
    const lp = getLayoutProps(layout);

    const { animations, exit_animations } = resolveSceneTransition(String(cut.transition || ''), i, colorPrimary, colorAccent);
    const titleAnim = resolveTitleAnim(String(cut.animation || ''), i, layout);
    const subAnim   = resolveSubAnim(String(cut.animation || ''), i);
    const kbProps   = resolveKenBurns(i, dur);

    // ── Background ─────────────────────────────────────────────────────────────
    const bgElement = hasImg ? {
      name: `bg_${nn}`, type: 'image', track: 1, time: 0, source: imgUrl, dynamic: true,
      width: '100%', height: '100%',
      x_anchor: '50%', y_anchor: '50%',
      fill_mode: 'cover',
      ...kbProps,  // Ken Burns: x/y pan + x_scale/y_scale zoom keyframes
    } : {
      // No image: beautiful brand gradient background
      name: `bg_${nn}`, type: 'shape', track: 1, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_mode: 'linear',
      fill_color: [
        { offset: 0, color: colorPrimary },
        { offset: 0.6, color: colorPrimary },
        { offset: 1, color: colorAccent },
      ],
      fill_x0: i % 2 === 0 ? '0%' : '100%', fill_y0: '0%',
      fill_x1: i % 2 === 0 ? '100%' : '0%', fill_y1: '100%',
      width: '100%', height: '100%', dynamic: true,
    };

    const elements: any[] = [bgElement];

    // ── Overlay layers (Canva-grade gradient treatments) ─────────────────────
    // isBottom = text at bottom, isTop/isBillboard = text at top
    const isBottom    = layout === 'bottom';
    const isTop       = layout === 'top';
    const isBillboard = layout === 'billboard';
    // For gradient direction: text at top → dark at top (gradY0='100%', gradY1='0%')
    //                         text at bottom → dark at bottom (gradY0='0%', gradY1='100%')

    if (layout === 'center') {
      // Cinematic: full gradient overlay darker at bottom, brand-tinted
      elements.push({
        type: 'shape', track: 2, time: 0,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_mode: 'linear',
        fill_color: hasImg
          ? [{ offset: 0, color: 'rgba(0,0,0,0.30)' }, { offset: 1, color: 'rgba(0,0,0,0.78)' }]
          : [{ offset: 0, color: 'rgba(0,0,0,0.20)' }, { offset: 1, color: 'rgba(0,0,0,0.55)' }],
        fill_x0: '50%', fill_y0: '0%', fill_x1: '50%', fill_y1: '100%',
        width: '100%', height: '100%', x: '50%', y: '50%', x_anchor: '50%', y_anchor: '50%',
        animations: [{ type: 'fade', duration: 0.6, easing: 'quadratic-out' }],
      });
    } else if (layout === 'caption') {
      // Light scrim across full frame
      if (hasImg) {
        elements.push({
          type: 'shape', track: 2, time: 0,
          path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
          fill_color: 'rgba(0,0,0,0.10)',
          width: '100%', height: '100%', x: '50%', y: '50%', x_anchor: '50%', y_anchor: '50%',
        });
      }
      // Caption band: gradient from transparent → brand dark
      elements.push({
        type: 'shape', track: 3, time: 0,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_mode: 'linear',
        fill_color: [
          { offset: 0, color: 'rgba(0,0,0,0)' },
          { offset: 0.3, color: 'rgba(10,10,10,0.88)' },
          { offset: 1, color: 'rgba(10,10,10,0.96)' },
        ],
        fill_x0: '50%', fill_y0: '0%', fill_x1: '50%', fill_y1: '100%',
        width: '100%', height: lp.captionBandH,
        x: '50%', y: '100%', x_anchor: '50%', y_anchor: '100%',
        animations: [{ type: 'slide', direction: 'up', distance: '4%', fade: true, duration: 0.45, easing: 'quadratic-out' }],
      });
      // Accent top-edge line
      elements.push({
        type: 'shape', track: 4, time: 0.15,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: colorAccent,
        width: '100%', height: '0.45 vmin',
        x: '50%', y: lp.captionBandTopY, x_anchor: '50%', y_anchor: '100%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.6, easing: 'quadratic-out' }],
      });
    } else {
      // bottom / top / billboard — directional gradient overlay (Canva-style)
      // billboard uses same direction as top (text at top → dark at top side)
      const gradY0 = (isTop || isBillboard) ? '100%' : '0%';  // transparent side
      const gradY1 = (isTop || isBillboard) ? '0%' : '100%';  // dark side
      // Full-frame vignette (subtle)
      elements.push({
        type: 'shape', track: 2, time: 0,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: hasImg ? 'rgba(0,0,0,0.08)' : 'rgba(0,0,0,0)',
        width: '100%', height: '100%', x: '50%', y: '50%', x_anchor: '50%', y_anchor: '50%',
      });
      // Main directional gradient overlay
      if (lp.overlayMajorH) {
        elements.push({
          type: 'shape', track: 3, time: 0,
          path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
          fill_mode: 'linear',
          fill_color: hasImg
            ? [{ offset: 0, color: 'rgba(0,0,0,0)' }, { offset: 0.5, color: 'rgba(0,0,0,0.45)' }, { offset: 1, color: 'rgba(0,0,0,0.82)' }]
            : [{ offset: 0, color: 'rgba(0,0,0,0)' }, { offset: 1, color: 'rgba(0,0,0,0.60)' }],
          fill_x0: '50%', fill_y0: gradY0, fill_x1: '50%', fill_y1: gradY1,
          width: '100%', height: lp.overlayMajorH,
          x: '50%', y: lp.overlayMajorY, x_anchor: '50%', y_anchor: lp.overlayMajorAnchorY,
          animations: [{ type: 'fade', duration: 0.5, easing: 'quadratic-out' }],
        });
      }
      // Secondary overlay: extra depth near text
      if (lp.overlayMinorH) {
        elements.push({
          type: 'shape', track: 4, time: 0,
          path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
          fill_mode: 'linear',
          fill_color: [
            { offset: 0, color: 'rgba(0,0,0,0)' },
            { offset: 1, color: 'rgba(0,0,0,0.38)' },
          ],
          fill_x0: '50%', fill_y0: gradY0, fill_x1: '50%', fill_y1: gradY1,
          width: '100%', height: lp.overlayMinorH,
          x: '50%', y: lp.overlayMinorY, x_anchor: '50%', y_anchor: lp.overlayMinorAnchorY,
        });
      }
    }

    // ── Accent elements (Canva-style horizontal wipe lines) ─────────────────
    if (layout === 'center') {
      // Double-line cinematic accent (Canva hallmark)
      elements.push({
        type: 'shape', track: 5, time: 0.35,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: colorAccent,
        width: isVertical ? '22%' : '16%', height: '0.45 vmin',
        x: '50%', y: isVertical ? '53%' : '52%', x_anchor: '50%', y_anchor: '0%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.55, easing: 'quadratic-out' }],
      });
      elements.push({
        type: 'shape', track: 5, time: 0.42,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: 'rgba(255,255,255,0.25)',
        width: isVertical ? '14%' : '10%', height: '0.3 vmin',
        x: '50%', y: isVertical ? '54.6%' : '53.5%', x_anchor: '50%', y_anchor: '0%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.45, easing: 'quadratic-out' }],
      });
    } else if (layout === 'billboard') {
      // Bold underline sweep below headline
      elements.push({
        type: 'shape', track: 5, time: 0.25,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: colorAccent,
        width: isVertical ? '28%' : '20%', height: '0.6 vmin',
        x: '50%', y: isVertical ? '21%' : '20%', x_anchor: '50%', y_anchor: '0%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.6, easing: 'quadratic-out' }],
      });
    } else if (layout !== 'caption') {
      // bottom / top: short accent line above title (Canva-style pre-text marker)
      const accentY = isTop
        ? (isVertical ? '9%' : '8%')
        : (isVertical ? '62.5%' : '58%');
      const accentAnchorY = isTop ? '0%' : '100%';
      elements.push({
        type: 'shape', track: 5, time: 0.08,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_color: colorAccent,
        width: isWide ? '8%' : isVertical ? '12%' : '10%', height: '0.5 vmin',
        x: lp.textIndent, y: accentY, x_anchor: '0%', y_anchor: accentAnchorY,
        animations: [{ type: 'wipe', direction: 'right', duration: 0.5, easing: 'quadratic-out' }],
      });
    }

    // ── Top brand bar (first cut only) ───────────────────────────────────────
    if (isFirstCut) {
      elements.push({
        type: 'shape', track: 5, time: 0,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_mode: 'linear',
        fill_color: [
          { offset: 0, color: colorPrimary },
          { offset: 1, color: colorAccent },
        ],
        fill_x0: '0%', fill_y0: '50%', fill_x1: '100%', fill_y1: '50%',
        width: '100%', height: '0.8 vmin',
        x: '50%', y: '0%', x_anchor: '50%', y_anchor: '0%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.8, easing: 'quadratic-out' }],
        exit_animations: [{ type: 'fade', duration: 0.3, easing: 'quadratic-in' }],
      });
    }

    // ── Scene number dot (minimal, Canva-style) ──────────────────────────────
    elements.push({
      type: 'text', track: 6, time: 0.1,
      text: `${i + 1} / ${totalCuts}`,
      x: isWide ? '93%' : '90%', y: '4.5%',
      x_anchor: '100%', y_anchor: '50%',
      font_family: 'Noto Sans JP',
      font_size: isVertical ? '2.2 vmin' : '1.9 vmin',
      font_weight: '400', fill_color: 'rgba(255,255,255,0.70)',
      letter_spacing: 1,
      animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-out' }],
    });

    // ── Title text (Canva-grade typography) ──────────────────────────────────
    const mainTextStr = String(cut.mainText || cut.title || cut.textMain || '');
    const isCinematic = (layout === 'center' || layout === 'billboard');
    const titleEl: Record<string, any> = {
      name: `title_${nn}`, type: 'text', track: 7, time: 0.22,
      text: mainTextStr, dynamic: true,
      x: lp.textIndent, y: lp.titleY,
      x_anchor: lp.centerX ? '50%' : '0%', y_anchor: lp.titleAnchorY,
      width: lp.textWidth, font_family: 'Noto Sans JP',
      font_size: lp.titleSize, font_weight: lp.titleWeight,
      fill_color: textColor,
      line_height: isCinematic ? 1.2 : 1.15,
      letter_spacing: isCinematic ? 3 : 2,
      shadow_color: 'rgba(0,0,0,0.55)', shadow_blur: 6, shadow_x: 0, shadow_y: 3,
      animations: titleAnim,
    };
    if (lp.centerX) titleEl.text_align = 'center';
    elements.push(titleEl);

    // ── Subtitle text ─────────────────────────────────────────────────────────
    const subText = String(cut.subText || cut.subtitle || cut.textSub || '');
    if (subText) {
      const subEl: Record<string, any> = {
        name: `sub_${nn}`, type: 'text', track: 8, time: 0.42,
        text: subText, dynamic: true,
        x: lp.textIndent, y: lp.subY,
        x_anchor: lp.centerX ? '50%' : '0%', y_anchor: lp.subAnchorY,
        width: lp.textWidth, font_family: 'Noto Sans JP',
        font_size: lp.subSize, fill_color: textColor === '#ffffff' ? 'rgba(255,255,255,0.88)' : textColor,
        font_weight: '300',
        line_height: 1.35, letter_spacing: isCinematic ? 4 : 2.5,
        shadow_color: 'rgba(0,0,0,0.40)', shadow_blur: 4, shadow_x: 0, shadow_y: 2,
        animations: subAnim,
      };
      if (lp.centerX) subEl.text_align = 'center';
      elements.push(subEl);
    }

    // ── CTA on last cut (Canva-style: clean button + sweep line) ─────────────
    if (isLastCut) {
      // Wide accent sweep line
      elements.push({
        type: 'shape', track: 9, time: 0.48,
        path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
        fill_mode: 'linear',
        fill_color: [
          { offset: 0, color: colorAccent },
          { offset: 1, color: colorPrimary },
        ],
        fill_x0: '0%', fill_y0: '50%', fill_x1: '100%', fill_y1: '50%',
        width: isVertical ? '70%' : '55%', height: '0.5 vmin',
        x: '50%', y: isVertical ? '80%' : '76%',
        x_anchor: '50%', y_anchor: '0%',
        animations: [{ type: 'wipe', direction: 'right', duration: 0.6, easing: 'quadratic-out' }],
      });
      // CTA pill
      elements.push({
        type: 'text', track: 10, time: 0.65,
        text: ctaText,
        x: '50%', y: isVertical ? '84%' : '80%',
        x_anchor: '50%', y_anchor: '0%',
        width: isVertical ? '72%' : '56%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '2.8 vmin' : '2.3 vmin',
        font_weight: '700', fill_color: '#ffffff', text_align: 'center',
        letter_spacing: 2,
        background_color: colorAccent,
        background_x_padding: 22, background_y_padding: 12,
        shadow_color: 'rgba(0,0,0,0.30)', shadow_blur: 8, shadow_x: 0, shadow_y: 4,
        animations: [{ type: 'scale', start_scale: '78%', end_scale: '100%', fade: true, duration: 0.55, easing: 'back-out' }],
      });
    }

    return {
      type: 'composition', track: i + 1, time: timeStart, duration: dur,
      elements, animations, exit_animations,
    };
  });

  const rootElements: any[] = [...sceneCompositions];

  // BGM track
  if (bgmUrl) {
    rootElements.push({
      name:          'bgm_track',
      type:          'audio',
      track:         90,
      time:          0,
      source:        bgmUrl,
      duration:       calcPayloadDuration(rawCuts),
      audio_fade_out: Math.min(1.5, calcPayloadDuration(rawCuts) * 0.05),
    });
  }

  // Invisible brand-colour reference elements (useful for dynamic replacements)
  rootElements.push(
    { name: 'brand_primary',   type: 'shape', track: 91, time: 0, path: 'M 0 0 L 1 0 L 1 1 L 0 1 Z', fill_color: colorPrimary, width: '0.1 vmin', height: '0.1 vmin', x: '0%', y: '0%', dynamic: true },
    { name: 'brand_secondary', type: 'shape', track: 92, time: 0, path: 'M 0 0 L 1 0 L 1 1 L 0 1 Z', fill_color: colorAccent,  width: '0.1 vmin', height: '0.1 vmin', x: '0%', y: '0%', dynamic: true },
  );

  return {
    output_format: 'mp4',
    width:  w,
    height: h,
    frame_rate: 30,
    elements: rootElements,
  };
};

// ─────────────────────────────────────────────────────────────────────────────
// Gradient template builder (フルカラーグラデーション / 写真不要 / テキスト主役)
// ─────────────────────────────────────────────────────────────────────────────
const buildGradientInlineSource = (payload: Record<string, unknown>) => {
  const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
  const dims = (DESTINATION_DIMENSIONS[destination] || [1080, 1920]) as [number, number];
  const [w, h] = dims;
  const isVertical = h > w;

  const colorPrimary = String(payload?.colorPrimary || '#E95464');
  const colorAccent  = String(payload?.colorAccent  || '#1c9a8b');
  const textColor    = String(payload?.textColor    || '#ffffff');
  const bgmRaw = String(payload?.bgm || '');
  const bgmUrl = bgmRaw.startsWith('http') ? bgmRaw : (BGM_URL_MAP[bgmRaw] || '');

  const rawCuts: any[] = Array.isArray(payload?.cuts) && (payload.cuts as any[]).length > 0
    ? payload.cuts as any[]
    : [{ mainText: String(payload?.telopMain || ''), subText: String(payload?.telopSub || ''), duration: 5 }];

  // Gradient variations per scene
  const GRADIENTS = [
    // 0: diagonal top-left → bottom-right (primary → accent)
    { fill_x0: '0%', fill_y0: '0%', fill_x1: '100%', fill_y1: '100%', c0: colorPrimary, c1: colorAccent },
    // 1: diagonal bottom-left → top-right (accent → primary)
    { fill_x0: '0%', fill_y0: '100%', fill_x1: '100%', fill_y1: '0%', c0: colorAccent, c1: colorPrimary },
    // 2: vertical (primary dark → accent)
    { fill_x0: '50%', fill_y0: '0%', fill_x1: '50%', fill_y1: '100%', c0: colorPrimary, c1: colorAccent },
    // 3: horizontal (accent → primary)
    { fill_x0: '0%', fill_y0: '50%', fill_x1: '100%', fill_y1: '50%', c0: colorAccent, c1: colorPrimary },
    // 4: diagonal opposite
    { fill_x0: '100%', fill_y0: '0%', fill_x1: '0%', fill_y1: '100%', c0: colorPrimary, c1: colorAccent },
    // 5: vertical reversed
    { fill_x0: '50%', fill_y0: '100%', fill_x1: '50%', fill_y1: '0%', c0: colorAccent, c1: colorPrimary },
  ];

  const TITLE_ANIMS = [
    [{ type: 'text-slide', direction: 'up', duration: 0.7, easing: 'back-out' }],
    [{ type: 'scale', start_scale: '60%', end_scale: '100%', fade: true, duration: 0.8, easing: 'elastic-out' }],
    [{ type: 'spin', rotation: '-14°', fade: true, duration: 0.75, easing: 'back-out' }],
    [{ type: 'wipe', direction: 'right', duration: 0.7, easing: 'quadratic-out' }],
    [{ type: 'slide', direction: 'up', distance: '12%', fade: true, duration: 0.7, easing: 'quadratic-out' }],
    [{ type: 'scale', start_scale: '72%', end_scale: '100%', fade: true, duration: 0.75, easing: 'elastic-out' }],
  ];

  const scenes = rawCuts.slice(0, 8).map((cut: any, i: number) => {
    const dur       = Number(cut.duration || cut.durationSec || 5);
    const mainText  = String(cut.mainText || cut.title || cut.textMain || '');
    const subText   = String(cut.subText  || cut.subtitle || cut.textSub || '');
    const imgUrl    = String(cut.imageUrl || '').trim();
    const hasImg    = imgUrl.startsWith('http');
    const timeStart = rawCuts.slice(0, i).reduce((acc: number, c: any) => acc + Number(c.duration || 5), 0);
    const grad      = GRADIENTS[i % GRADIENTS.length];
    const elements: any[] = [];

    // ── Gradient background ───────────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 1, time: 0,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_mode: 'linear',
      fill_color: [{ offset: 0, color: grad.c0 }, { offset: 1, color: grad.c1 }],
      fill_x0: grad.fill_x0, fill_y0: grad.fill_y0,
      fill_x1: grad.fill_x1, fill_y1: grad.fill_y1,
      width: '100%', height: '100%',
    });

    // ── Optional: overlay image with opacity (if available) ──────────────────
    if (hasImg) {
      elements.push({
        type: 'image', track: 2, time: 0, source: imgUrl, dynamic: true,
        width: '100%', height: '100%', x_anchor: '50%', y_anchor: '50%',
        fill_mode: 'cover', opacity: 0.18,
        ...resolveKenBurns(i, dur),
      });
    }

    // ── Large decorative geometric: top-right circle accent ──────────────────
    elements.push({
      type: 'shape', track: 3, time: 0,
      path: 'M 50 0 A 50 50 0 1 1 49.9999 0 Z',  // circle
      fill_color: 'rgba(255,255,255,0.08)',
      width: isVertical ? '80%' : '60%', height: isVertical ? '42%' : '60%',
      x: '90%', y: '-5%', x_anchor: '50%', y_anchor: '50%',
    });

    // ── Bottom decorative circle ───────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 4, time: 0,
      path: 'M 50 0 A 50 50 0 1 1 49.9999 0 Z',
      fill_color: 'rgba(255,255,255,0.06)',
      width: isVertical ? '55%' : '40%', height: isVertical ? '28%' : '40%',
      x: '10%', y: '105%', x_anchor: '50%', y_anchor: '50%',
    });

    // ── Horizontal accent line (wipe reveal) ─────────────────────────────────
    elements.push({
      type: 'shape', track: 5, time: 0.2,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: 'rgba(255,255,255,0.30)',
      width: isVertical ? '28%' : '20%', height: '0.5 vmin',
      x: '50%', y: isVertical ? '41%' : '39%', x_anchor: '50%', y_anchor: '50%',
      animations: [{ type: 'wipe', direction: 'right', duration: 0.7, easing: 'quadratic-out' }],
    });

    // ── Scene counter top-right ───────────────────────────────────────────────
    elements.push({
      type: 'text', track: 6, time: 0.1,
      text: String(i + 1).padStart(2, '0'),
      x: isVertical ? '88%' : '92%', y: '6%',
      x_anchor: '50%', y_anchor: '50%',
      font_family: 'Noto Sans JP',
      font_size: '2.5 vmin', font_weight: '100',
      fill_color: 'rgba(255,255,255,0.40)',
      letter_spacing: 2,
      animations: [{ type: 'fade', duration: 0.5, easing: 'quadratic-out' }],
    });

    // ── Main title ───────────────────────────────────────────────────────────
    if (mainText) {
      elements.push({
        type: 'text', track: 7, time: 0.28,
        text: mainText, dynamic: true,
        x: '50%', y: isVertical ? '46%' : '44%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '86%' : '78%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '8 vmin' : '7 vmin',
        font_weight: '900', fill_color: textColor,
        letter_spacing: 4, text_align: 'center', line_height: 1.15,
        shadow_color: 'rgba(0,0,0,0.25)', shadow_blur: 10, shadow_x: 0, shadow_y: 4,
        animations: TITLE_ANIMS[i % TITLE_ANIMS.length],
      });
    }

    // ── Subtitle ─────────────────────────────────────────────────────────────
    if (subText) {
      elements.push({
        type: 'text', track: 8, time: 0.48,
        text: subText, dynamic: true,
        x: '50%', y: isVertical ? '58%' : '56%',
        x_anchor: '50%', y_anchor: '50%',
        width: isVertical ? '74%' : '66%',
        font_family: 'Noto Sans JP',
        font_size: isVertical ? '3.2 vmin' : '2.8 vmin',
        font_weight: '300', fill_color: 'rgba(255,255,255,0.85)',
        letter_spacing: 3, text_align: 'center',
        animations: [{ type: 'slide', direction: 'up', distance: '5%', fade: true, duration: 0.6, easing: 'quadratic-out' }],
      });
    }

    // ── Bottom accent line ────────────────────────────────────────────────────
    elements.push({
      type: 'shape', track: 9, time: 0.4,
      path: 'M 0 0 L 100 0 L 100 100 L 0 100 Z',
      fill_color: 'rgba(255,255,255,0.30)',
      width: isVertical ? '28%' : '20%', height: '0.5 vmin',
      x: '50%', y: isVertical ? '63%' : '61%', x_anchor: '50%', y_anchor: '50%',
      animations: [{ type: 'wipe', direction: 'left', duration: 0.65, easing: 'quadratic-out' }],
    });

    const { animations, exit_animations } = resolveSceneTransition(String(cut.transition || ''), i, colorPrimary, colorAccent);
    return { type: 'composition', track: i + 1, time: timeStart, duration: dur, animations, exit_animations, elements };
  });

  const rootElements: any[] = [...scenes];
  if (bgmUrl) rootElements.push({ name: 'bgm_track', type: 'audio', track: 90, time: 0, source: bgmUrl, duration: calcPayloadDuration(rawCuts), audio_fade_out: Math.min(1.5, calcPayloadDuration(rawCuts) * 0.05) });
  return { output_format: 'mp4', width: w, height: h, frame_rate: 30, elements: rootElements };
};

const renderCreatomateJob = async (_req: Request, job: any) => {
  const payload = (job.payload || {}) as Record<string, unknown>;

  const style = String(payload?.style || 'standard');
  const source = style === 'collage'  ? buildCollageInlineSource(payload)
    : style === 'magazine' ? buildMagazineInlineSource(payload)
    : style === 'gradient' ? buildGradientInlineSource(payload)
    : style === 'minimal'  ? buildMinimalInlineSource(payload)
    : buildCreatomateInlineSource(payload);

  const bodyJson = JSON.stringify({ source });
  console.log('[pal-db] creatomate render request', {
    jobId: job.id,
    destination: String(payload?.destination || payload?.purpose || ''),
    sceneCount: (payload?.cuts as any[] | undefined)?.length ?? 0,
    dimensions: `${source.width}x${source.height}`,
    sourcePreview: bodyJson.slice(0, 3000),
  });

  const response = await fetch(CREATOMATE_API_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CREATOMATE_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: bodyJson,
    signal: AbortSignal.timeout(80000),
  });

  const data = await response.json().catch(() => ({}));
  console.log('[pal-db] creatomate render response', {
    jobId: job.id,
    status: response.status,
    renderId: Array.isArray(data) ? data[0]?.id : data?.id,
    errorData: response.ok ? undefined : JSON.stringify(data),
  });
  if (!response.ok) {
    const errData  = Array.isArray(data) ? data[0] : data;
    const errMsg   = errData?.message || errData?.hint || errData?.error || JSON.stringify(errData) || 'creatomate render failed';
    throw new Error(`Creatomate ${response.status}: ${errMsg}`);
  }

  const renderItem = Array.isArray(data) ? data[0] : data;
  const renderId   = String(renderItem?.id  || '').trim();

  // Creatomate は非同期。202 レスポンスの url はまだファイルが存在しない予約URL。
  // ポーリングして succeeded になってから URL を取得する。
  let finalItem = renderItem as Record<string, unknown>;
  if (renderId && renderItem?.status !== 'succeeded') {
    console.log('[pal-db] creatomate render queued, polling...', { renderId });
    const polled = await pollCreatomateRender(renderId);
    if (polled) {
      finalItem = polled;
      if (polled.status === 'failed') {
        const errMsg = polled.error_message || polled.error || polled.errorMessage || 'unknown';
        console.error('[pal-db] creatomate render failed', { renderId, errMsg, polled });
        throw new Error(`Creatomate render failed: ${JSON.stringify(errMsg)}`);
      }
    } else {
      console.warn('[pal-db] creatomate polling timeout, using initial url');
    }
  }

  const previewUrl = String(finalItem?.url || '').trim() || null;
  const nextStatus = job.status === '承認済' ? job.status : '確認中';

  const updated = await upsertPalVideoJob({
    id: job.id,
    paletteId: job.paletteId,
    planCode: job.planCode,
    status: nextStatus,
    payload: {
      ...payload,
      creatomateRenderId: renderId,
    },
    previewUrl,
    youtubeUrl: job.youtubeUrl,
  });

  return { updated, renderId, previewUrl };
};

app.get('/admin', (_req: Request, res: Response) => {
  res.sendFile(path.join(publicDir, 'customers.html'));
});

app.get('/admin/customers', (_req: Request, res: Response) => {
  res.sendFile(path.join(publicDir, 'customers.html'));
});

app.get('/admin/customers/:id', (_req: Request, res: Response) => {
  res.sendFile(path.join(publicDir, 'admin.html'));
});

app.get('/admin/media', (_req: Request, res: Response) => {
  res.sendFile(path.join(publicDir, 'media.html'));
});

app.get('/health', async (_req: Request, res: Response) => {
  try {
    await ensureTables();
    return res.json({ success: true, service: 'pal-db' });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'health check failed' });
  }
});

app.get('/api/accounts', async (_req: Request, res: Response) => {
  try {
    const accounts = await listAccounts();
    return res.json({ success: true, accounts });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list accounts' });
  }
});

app.post('/api/verify-chat-login', async (req: Request, res: Response) => {
  try {
    const id = String(req.body?.id || '').trim();
    const password = String(req.body?.password || '');
    if (!id || !password) {
      return res.status(400).json({ success: false, error: 'id and password are required' });
    }

    const result = await verifyChatLogin(id, password);
    if (!result.success) {
      return res.status(401).json({ success: false, error: 'invalid credentials' });
    }

    return res.json({
      success: true,
      accountId: result.accountId,
      paletteId: result.paletteId,
      accountName: result.accountName,
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to verify login' });
  }
});

app.post('/api/accounts', async (req: Request, res: Response) => {
  try {
    const account = await upsertAccount(req.body || {});
    return res.json({ success: true, account });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save account';
    return res.status(400).json({ success: false, error: message });
  }
});

app.get('/api/account-status-options', async (req: Request, res: Response) => {
  try {
    const includeInactive = String(req.query.includeInactive || '') === '1';
    const options = await listAccountStatusOptions(includeInactive);
    return res.json({ success: true, options });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list account status options' });
  }
});

app.post('/api/account-status-options', async (req: Request, res: Response) => {
  try {
    const option = await upsertAccountStatusOption(req.body || {});
    return res.json({ success: true, option });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save account status option';
    return res.status(400).json({ success: false, error: message });
  }
});

app.get('/api/plans', async (req: Request, res: Response) => {
  try {
    const includeInactive = String(req.query.includeInactive || '') === '1';
    const plans = await listPlans(includeInactive);
    return res.json({ success: true, plans });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list plans' });
  }
});

app.post('/api/plans', async (req: Request, res: Response) => {
  try {
    const plan = await upsertPlan(req.body || {});
    return res.json({ success: true, plan });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save plan';
    return res.status(400).json({ success: false, error: message });
  }
});

app.get('/api/contracts', async (req: Request, res: Response) => {
  try {
    const accountId = String(req.query.accountId || '').trim() || undefined;
    const paletteId = String(req.query.paletteId || '').trim() || undefined;
    const activeOn = String(req.query.activeOn || '').trim() || undefined;
    const contracts = await listContracts({ accountId, paletteId, activeOn });
    return res.json({ success: true, contracts });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list contracts' });
  }
});

app.post('/api/contracts', async (req: Request, res: Response) => {
  try {
    const contract = await upsertContract(req.body || {});
    return res.json({ success: true, contract });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save contract';
    return res.status(400).json({ success: false, error: message });
  }
});

app.get('/api/contract-options', async (req: Request, res: Response) => {
  try {
    const optionType = String(req.query.optionType || '').trim();
    if (optionType !== 'phase' && optionType !== 'status') {
      return res.status(400).json({ success: false, error: 'optionType must be phase or status' });
    }
    const includeInactive = String(req.query.includeInactive || '') === '1';
    const options = await listContractOptions(optionType, includeInactive);
    return res.json({ success: true, options });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list contract options' });
  }
});

app.post('/api/contract-options', async (req: Request, res: Response) => {
  try {
    const option = await upsertContractOption(req.body || {});
    return res.json({ success: true, option });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save contract option';
    return res.status(400).json({ success: false, error: message });
  }
});

app.delete('/api/accounts/:id', async (req: Request, res: Response) => {
  try {
    await deleteAccount(String(req.params.id));
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete account' });
  }
});

app.delete('/api/plans/:id', async (req: Request, res: Response) => {
  try {
    await deletePlan(String(req.params.id));
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete plan' });
  }
});

app.delete('/api/contracts/:id', async (req: Request, res: Response) => {
  try {
    await deleteContract(String(req.params.id));
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete contract' });
  }
});

app.delete('/api/account-status-options/:id', async (req: Request, res: Response) => {
  try {
    await deleteAccountStatusOption(String(req.params.id));
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete account status option' });
  }
});

app.delete('/api/contract-options/:id', async (req: Request, res: Response) => {
  try {
    await deleteContractOption(String(req.params.id));
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete contract option' });
  }
});

app.get('/api/service-subscriptions', async (req: Request, res: Response) => {
  try {
    const accountId = String(req.query.accountId || '').trim() || undefined;
    const paletteId = String(req.query.paletteId || '').trim() || undefined;
    const activeOn = String(req.query.activeOn || '').trim() || undefined;
    const services = await listServiceSubscriptions({ accountId, paletteId, activeOn });
    return res.json({ success: true, services });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list services' });
  }
});

app.get('/api/media', async (req: Request, res: Response) => {
  try {
    const paletteId = normalizePaletteIdInput(req.query.paletteId);
    const assets = await listMediaAssets(paletteId);
    return res.json({ success: true, assets });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to list media';
    return res.status(400).json({ success: false, error: message });
  }
});

app.post('/api/media/upload', uploadSingleMedia, async (req: Request, res: Response) => {
  try {
    const paletteId = normalizePaletteIdInput(req.body?.paletteId || req.query?.paletteId);
    const file = (req as Request & { file?: Express.Multer.File }).file;
    if (!file) {
      return res.status(400).json({ success: false, error: 'file is required' });
    }

    const url = `${getPublicBaseUrl(req)}/media/${encodeURIComponent(paletteId)}/${encodeURIComponent(file.filename)}`;
    const asset = await createMediaAsset({
      paletteId,
      fileName: file.filename,
      originalName: file.originalname,
      mimeType: file.mimetype,
      sizeBytes: file.size,
      url,
    });

    return res.json({ success: true, asset });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to upload media';
    return res.status(400).json({ success: false, error: message });
  }
});

app.delete('/api/media/:id', async (req: Request, res: Response) => {
  try {
    const assetId = String(req.params.id || '').trim();
    if (!assetId) return res.status(400).json({ success: false, error: 'id is required' });

    const asset = await getMediaAssetById(assetId);
    if (!asset) return res.status(404).json({ success: false, error: 'media not found' });

    const filePath = path.join(mediaRootDir, asset.paletteId, asset.fileName);
    if (existsSync(filePath)) {
      try {
        await fs.unlink(filePath);
      } catch (error) {
        console.warn('[pal-db] failed to remove media file', error);
      }
    }

    await deleteMediaAsset(assetId);
    return res.json({ success: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to delete media';
    return res.status(400).json({ success: false, error: message });
  }
});

app.get('/api/pal-video/jobs', async (req: Request, res: Response) => {
  try {
    const paletteId = String(req.query.paletteId || '').trim() || undefined;
    const status = String(req.query.status || '').trim() || undefined;
    const limit = req.query.limit ? Number(req.query.limit) : undefined;
    const jobs = await listPalVideoJobs({ paletteId, status, limit });
    return res.json({ success: true, jobs });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to list pal_video jobs' });
  }
});

app.get('/api/pal-video/jobs/:id', async (req: Request, res: Response) => {
  try {
    const jobId = String(req.params.id || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'id is required' });
    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });
    return res.json({ success: true, job });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to get pal_video job' });
  }
});

app.post('/api/pal-video/jobs', async (req: Request, res: Response) => {
  try {
    const b = req.body || {};
    // Accept both camelCase (internal) and snake_case (HTTP API) keys
    const normalized = {
      ...b,
      paletteId: b.paletteId ?? b.palette_id,
      planCode:  b.planCode  ?? b.plan_code,
      previewUrl: b.previewUrl ?? b.preview_url,
      youtubeUrl: b.youtubeUrl ?? b.youtube_url,
    };
    const job = await upsertPalVideoJob(normalized);
    return res.json({ success: true, job });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save pal_video job';
    return res.status(400).json({ success: false, error: message });
  }
});

// デバッグ用: source生成 + Creatomate送信 + ポーリングでエラー詳細取得
app.post('/api/pal-video/debug-source', async (req: Request, res: Response) => {
  try {
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });
    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });
    const payload = (job.payload || {}) as Record<string, unknown>;
    const style = String(payload?.style || 'standard');
    const source = style === 'collage'  ? buildCollageInlineSource(payload)
      : style === 'magazine' ? buildMagazineInlineSource(payload)
      : style === 'gradient' ? buildGradientInlineSource(payload)
      : style === 'minimal'  ? buildMinimalInlineSource(payload)
      : buildCreatomateInlineSource(payload);
    const bodyJson = JSON.stringify({ source });

    // Creatomateへの実際送信テスト
    let creatomateStatus: number | null = null;
    let creatomateInitialResponse: unknown = null;
    let creatomateFinalResponse: unknown = null;
    if (CREATOMATE_API_KEY) {
      const ctRes = await fetch(CREATOMATE_API_URL, {
        method: 'POST',
        headers: { Authorization: `Bearer ${CREATOMATE_API_KEY}`, 'Content-Type': 'application/json' },
        body: bodyJson,
      });
      creatomateStatus = ctRes.status;
      creatomateInitialResponse = await ctRes.json().catch(() => null);

      // ポーリングしてエラー詳細を取得
      const initItem = Array.isArray(creatomateInitialResponse) ? (creatomateInitialResponse as any[])[0] : creatomateInitialResponse;
      const renderId = initItem?.id;
      if (renderId) {
        creatomateFinalResponse = await pollCreatomateRender(renderId, 90000);
      }
    }

    return res.json({
      success: true,
      payloadCutsCount: (payload.cuts as any[] | undefined)?.length ?? 0,
      sourceElementsCount: source.elements.length,
      sourceSizeBytes: bodyJson.length,
      creatomateStatus,
      creatomateInitialResponse,
      creatomateFinalResponse,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed';
    return res.status(500).json({ success: false, error: message });
  }
});

// ── FFmpeg render helper ──────────────────────────────────────────────────────
const renderWithFFmpegAndSave = async (job: any, host: string, preview = false): Promise<{ updated: any; previewUrl: string }> => {
  const payload = (job.payload || {}) as Record<string, unknown>;

  const filePath = await renderWithFFmpeg(payload, job.id, async (progress) => {
    // カット完了ごとにDBの payload.renderProgress を更新
    console.log(`[pal-db] onProgress job=${job.id}`, JSON.stringify(progress));
    await upsertPalVideoJob({
      id: job.id, paletteId: job.paletteId, planCode: job.planCode,
      status: 'レンダリング中',
      payload: { ...payload, renderProgress: progress },
      previewUrl: null, youtubeUrl: job.youtubeUrl,
    }).catch((e) => console.error('[pal-db] onProgress DB write failed:', e));
  }, preview);

  // Public URL served by this Express server
  const previewUrl = `${host}/api/pal-video/files/${job.id}`;

  const updated = await upsertPalVideoJob({
    id:         job.id,
    paletteId:  job.paletteId,
    planCode:   job.planCode,
    status:     '確認中',
    payload:    { ...payload },
    previewUrl,
    youtubeUrl: job.youtubeUrl,
  });

  return { updated, previewUrl };
};

// バックグラウンドレンダー共通ハンドラ
const startBackgroundRender = (job: any, host: string, preview = false) => {
  setImmediate(async () => {
    try {
      // 初期ステータスをここで await して書く（fire-and-forgetにすると onProgress 書き込みと競合する）
      await upsertPalVideoJob({ id: job.id, paletteId: job.paletteId, planCode: job.planCode,
        status: 'レンダリング中', payload: job.payload, previewUrl: null, youtubeUrl: job.youtubeUrl,
      }).catch(() => {});

      const { previewUrl } = await renderWithFFmpegAndSave(job, host, preview);
      console.log('[pal-db] render complete:', job.id, previewUrl);
    } catch (err) {
      console.error('[pal-db] background render failed:', job.id, err);
      await upsertPalVideoJob({ id: job.id, paletteId: job.paletteId, planCode: job.planCode,
        status: 'エラー', payload: { ...job.payload, renderError: (err as Error).message },
        previewUrl: null, youtubeUrl: job.youtubeUrl,
      }).catch(() => {});
    }
  });
};

app.post('/api/pal-video/generate', async (req: Request, res: Response) => {
  try {
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });

    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });

    // 既にレンダリング中の場合、5分以内に更新があれば重複起動を防ぐ
    // 5分以上更新がなければサーバー再起動等でスタックしたと判断して再起動
    if (job.status === 'レンダリング中') {
      const lastUpdate = new Date(job.updatedAt || 0).getTime();
      if (Date.now() - lastUpdate < 5 * 60 * 1000) {
        return res.json({ success: true, status: 'rendering', jobId: job.id });
      }
      console.log('[pal-db] stale rendering job, restarting:', job.id);
    }

    const host = `${req.protocol}://${req.get('host')}`;
    startBackgroundRender(job, host, true); // preview=true: 半解像度で高速生成
    return res.json({ success: true, status: 'rendering', jobId: job.id });
  } catch (error) {
    console.error('[pal-db] pal-video generate failed', error);
    const message = error instanceof Error ? error.message : 'failed to generate pal_video';
    return res.status(500).json({ success: false, error: message });
  }
});

app.post('/api/pal-video/render', async (req: Request, res: Response) => {
  try {
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });

    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });

    // 既にレンダリング中の場合、5分以内に更新があれば重複起動を防ぐ
    // 5分以上更新がなければサーバー再起動等でスタックしたと判断して再起動
    if (job.status === 'レンダリング中') {
      const lastUpdate = new Date(job.updatedAt || 0).getTime();
      if (Date.now() - lastUpdate < 5 * 60 * 1000) {
        return res.json({ success: true, status: 'rendering', jobId: job.id });
      }
      console.log('[pal-db] stale rendering job, restarting:', job.id);
    }

    const host = `${req.protocol}://${req.get('host')}`;
    startBackgroundRender(job, host);
    return res.json({ success: true, status: 'rendering', jobId: job.id });
  } catch (error) {
    console.error('[pal-db] pal-video render failed', error);
    const message = error instanceof Error ? error.message : 'failed to render pal_video';
    return res.status(500).json({ success: false, error: message });
  }
});

// ── 生成済み MP4 ファイル配信 ──────────────────────────────────────────────────
app.get('/api/pal-video/files/:jobId', async (req: Request, res: Response) => {
  const jobId = String(req.params.jobId || '').replace(/[^a-zA-Z0-9_\-]/g, '');
  const filePath = `/tmp/pal-video/${jobId}_output.mp4`;
  try {
    const stat = await fs.stat(filePath);
    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Length', stat.size);
    res.setHeader('Content-Disposition', `inline; filename="video-${jobId}.mp4"`);
    res.setHeader('Cache-Control', 'public, max-age=3600');
    createReadStream(filePath).pipe(res);
  } catch {
    res.status(404).json({ success: false, error: 'file not found — re-render to regenerate' });
  }
});

app.post('/api/service-subscriptions', async (req: Request, res: Response) => {
  try {
    const service = await upsertServiceSubscription(req.body || {});
    return res.json({ success: true, service });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save service';
    return res.status(400).json({ success: false, error: message });
  }
});

app.delete('/api/service-subscriptions/:id', async (req: Request, res: Response) => {
  try {
    await deleteServiceSubscription(String(req.params.id));
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete service' });
  }
});

app.get('/api/palette-summary', async (req: Request, res: Response) => {
  try {
    const paletteId = String(req.query.paletteId || '').trim();
    const activeOn = String(req.query.activeOn || '').trim() || undefined;
    if (!paletteId) return res.status(400).json({ success: false, error: 'paletteId is required' });

    const summary = await getPaletteSummary(paletteId, activeOn);
    if (!summary) return res.status(404).json({ success: false, error: 'paletteId not found' });

    return res.json({ success: true, ...summary });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to fetch summary' });
  }
});

app.get('/api/palette-services', async (req: Request, res: Response) => {
  try {
    const paletteId = String(req.query.paletteId || '').trim();
    const activeOn = String(req.query.activeOn || '').trim() || undefined;
    if (!paletteId) return res.status(400).json({ success: false, error: 'paletteId is required' });

    const result = await getPaletteServices(paletteId, activeOn);
    if (!result) return res.status(404).json({ success: false, error: 'paletteId not found' });

    return res.json({ success: true, ...result });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to fetch services' });
  }
});

app.get('/api/chat-auth/check-id', async (req: Request, res: Response) => {
  try {
    const loginId = String(req.query.loginId || '').trim();
    if (!loginId) {
      return res.status(400).json({ success: false, error: 'loginId is required' });
    }
    const exists = await hasChatLoginId(loginId);
    return res.json({ success: true, exists });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to check login id' });
  }
});

app.post('/api/chat-auth/verify', async (req: Request, res: Response) => {
  try {
    const loginId = String(req.body?.loginId || '').trim();
    const password = String(req.body?.password || '');
    if (!loginId || !password) {
      return res.status(400).json({ success: false, error: 'loginId and password are required' });
    }

    const verified = await verifyChatLogin(loginId, password);
    if (!verified.success) {
      return res.status(401).json({ success: false, error: 'invalid credentials' });
    }

    return res.json(verified);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to verify credentials' });
  }
});

// ── pal_opt settings endpoints ──────────────────────────────────────────────

app.get('/api/pal-opt-settings', async (req: Request, res: Response) => {
  try {
    await ensureTables();
    const paletteId = String(req.query.paletteId || '').trim().toUpperCase();
    if (!paletteId) {
      return res.status(400).json({ success: false, error: 'paletteId is required' });
    }
    const { sql } = await import('@vercel/postgres');
    const { rows } = await sql`SELECT * FROM pal_opt_settings WHERE palette_id = ${paletteId} LIMIT 1`;
    return res.json({ success: true, settings: rows[0] || null });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to fetch pal_opt settings' });
  }
});

app.post('/api/pal-opt-settings', async (req: Request, res: Response) => {
  try {
    await ensureTables();
    const { sql } = await import('@vercel/postgres');
    const b = req.body;
    const paletteId = String(b.paletteId || '').trim().toUpperCase();
    if (!paletteId) return res.status(400).json({ success: false, error: 'paletteId is required' });
    const { randomUUID } = await import('crypto');
    const id = randomUUID();
    const keywords = JSON.stringify(Array.isArray(b.targetKeywords) ? b.targetKeywords : []);
    await sql`
      INSERT INTO pal_opt_settings (
        id, palette_id, ig_access_token, ig_business_account_id,
        gbp_access_token, gbp_refresh_token, gbp_location_id,
        blog_url, blog_wp_username, blog_api_key,
        target_keywords, goals, default_tone, has_pal_studio, has_pal_trust
      ) VALUES (
        ${id}, ${paletteId}, ${b.igAccessToken ?? null}, ${b.igBusinessAccountId ?? null},
        ${b.gbpAccessToken ?? null}, ${b.gbpRefreshToken ?? null}, ${b.gbpLocationId ?? null},
        ${b.blogUrl ?? null}, ${b.blogWpUsername ?? null}, ${b.blogApiKey ?? null},
        ${keywords}::jsonb, ${b.goals ?? null}, ${b.defaultTone ?? 'professional'},
        ${Boolean(b.hasPalStudio)}, ${Boolean(b.hasPalTrust)}
      )
      ON CONFLICT (palette_id) DO UPDATE SET
        ig_access_token = EXCLUDED.ig_access_token,
        ig_business_account_id = EXCLUDED.ig_business_account_id,
        gbp_access_token = EXCLUDED.gbp_access_token,
        gbp_refresh_token = EXCLUDED.gbp_refresh_token,
        gbp_location_id = EXCLUDED.gbp_location_id,
        blog_url = EXCLUDED.blog_url,
        blog_wp_username = EXCLUDED.blog_wp_username,
        blog_api_key = EXCLUDED.blog_api_key,
        target_keywords = EXCLUDED.target_keywords,
        goals = EXCLUDED.goals,
        default_tone = EXCLUDED.default_tone,
        has_pal_studio = EXCLUDED.has_pal_studio,
        has_pal_trust = EXCLUDED.has_pal_trust,
        updated_at = NOW()
    `;
    const { rows } = await sql`SELECT * FROM pal_opt_settings WHERE palette_id = ${paletteId} LIMIT 1`;
    return res.json({ success: true, settings: rows[0] || null });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to upsert pal_opt settings' });
  }
});

// ── pal_opt posts endpoints ──────────────────────────────────────────────────

app.get('/api/pal-opt-posts', async (req: Request, res: Response) => {
  try {
    await ensureTables();
    const { sql } = await import('@vercel/postgres');
    const paletteId = String(req.query.paletteId || '').trim().toUpperCase();
    const status = String(req.query.status || '').trim();
    const limit = Math.min(Number(req.query.limit || 20), 100);
    let rows;
    if (paletteId && status) {
      ({ rows } = await sql`SELECT * FROM pal_opt_posts WHERE palette_id = ${paletteId} AND status = ${status} ORDER BY updated_at DESC LIMIT ${limit}`);
    } else if (paletteId) {
      ({ rows } = await sql`SELECT * FROM pal_opt_posts WHERE palette_id = ${paletteId} ORDER BY updated_at DESC LIMIT ${limit}`);
    } else {
      ({ rows } = await sql`SELECT * FROM pal_opt_posts ORDER BY updated_at DESC LIMIT ${limit}`);
    }
    return res.json({ success: true, posts: rows });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to fetch pal_opt posts' });
  }
});

app.delete('/api/pal-opt-posts/:id', async (req: Request, res: Response) => {
  try {
    await ensureTables();
    const { sql } = await import('@vercel/postgres');
    const postId = String(req.params.id || '').trim();
    if (!postId) return res.status(400).json({ success: false, error: 'post id is required' });
    await sql`DELETE FROM pal_opt_posts WHERE id = ${postId}`;
    return res.json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ success: false, error: 'failed to delete pal_opt post' });
  }
});

app.listen(port, async () => {
  try {
    await ensureTables();
    console.log(`[pal-db] running on http://localhost:${port}`);
  } catch (error) {
    console.error('[pal-db] init error', error);
    if (!process.env.POSTGRES_URL) {
      console.error('[pal-db] POSTGRES_URL が未設定です。.env に設定してください。');
      console.error('[pal-db] 例: POSTGRES_URL=postgres://USER:PASSWORD@HOST:5432/DB');
    }
  }
});
