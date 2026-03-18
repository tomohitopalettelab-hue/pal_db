import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import type { Request, Response } from 'express';
import multer from 'multer';
import path from 'path';
import { existsSync, mkdirSync } from 'fs';
import { promises as fs } from 'fs';
import { fileURLToPath } from 'url';
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

// BGM tracks (royalty-free)
const BGM_URL_MAP: Record<string, string> = {
  bright_pop:    'https://cdn.pixabay.com/audio/2022/05/27/audio_1808fbf07a.mp3',
  cool_minimal:  'https://cdn.pixabay.com/audio/2022/03/10/audio_270f49d28e.mp3',
  cinematic:     'https://cdn.pixabay.com/audio/2022/01/20/audio_d0bd90a6d6.mp3',
  natural_warm:  'https://cdn.pixabay.com/audio/2021/11/13/audio_7b6e8dd7bf.mp3',
};

const buildCreatomateInlineSource = (payload: Record<string, unknown>) => {
  const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
  const [w, h] = DESTINATION_DIMENSIONS[destination] || [1080, 1920];
  const isVertical = h > w;
  const isSquare   = h === w;
  const isWide     = w > h;

  const colorPrimary = String(payload?.colorPrimary || '#E95464');
  const colorAccent  = String(payload?.colorAccent  || '#1c9a8b');
  const bgmRaw = String(payload?.bgm || '');
  const bgmUrl = bgmRaw.startsWith('http') ? bgmRaw : (BGM_URL_MAP[bgmRaw] || '');

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

  // ── Scene transition resolver (10 types + 12-pattern auto-cycle) ──────────────
  const resolveSceneTransition = (transition: string, idx: number) => {
    const t = String(transition || '').toLowerCase();
    if (t === 'none') return { animations: [], exit_animations: [] };

    if (t === 'fade') return {
      animations:      [{ type: 'fade', duration: 0.5, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }],
    };
    if (t === 'slide') {
      const dirs = ['left', 'up', 'right', 'left', 'up'] as const;
      return {
        animations:      [{ type: 'slide', direction: dirs[idx % dirs.length], duration: 0.45, fade: false, easing: 'quadratic-out' }],
        exit_animations: [{ type: 'fade', duration: 0.3, easing: 'quadratic-in' }],
      };
    }
    if (t === 'zoom') {
      const startScale = idx % 2 === 0 ? '108%' : '93%';
      const easing = idx % 2 === 0 ? 'quadratic-out' : 'back-out';
      return {
        animations:      [{ type: 'scale', start_scale: startScale, end_scale: '100%', fade: true, duration: 0.55, easing }],
        exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }],
      };
    }
    if (t === 'wipe') {
      const dirs = ['right', 'up', 'left', 'up', 'right'] as const;
      return {
        animations:      [{ type: 'wipe', direction: dirs[idx % dirs.length], duration: 0.5, easing: 'quadratic-out' }],
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
      const rot = idx % 2 === 0 ? '-14°' : '14°';
      return {
        animations:      [{ type: 'spin', rotation: rot, fade: true, duration: 0.6, easing: 'quadratic-out' }],
        exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }],
      };
    }
    // 'blur' は Composition レベルでは無効 → scale+fadeで代替
    if (t === 'blur') return {
      animations:      [{ type: 'scale', start_scale: '105%', end_scale: '100%', fade: true, duration: 0.6, easing: 'quadratic-out' }],
      exit_animations: [{ type: 'fade', duration: 0.4, easing: 'quadratic-in' }],
    };
    if (t === 'bounce') return {
      animations:      [{ type: 'scale', start_scale: '82%', end_scale: '100%', fade: true, duration: 0.65, easing: 'back-out' }],
      exit_animations: [{ type: 'fade', duration: 0.35, easing: 'quadratic-in' }],
    };
    if (t === 'push') {
      const dirs = ['left', 'up', 'right', 'left'] as const;
      return {
        animations:      [{ type: 'slide', direction: dirs[idx % dirs.length], duration: 0.5, fade: false, easing: 'quadratic-out' }],
        exit_animations: [{ type: 'slide', direction: dirs[(idx + 2) % dirs.length], duration: 0.4, fade: false, easing: 'quadratic-in' }],
      };
    }
    // Auto: smart 12-pattern cycle
    const AUTO = [
      { animations: [{ type: 'fade',       duration: 0.5,  easing: 'quadratic-out' }],                                                    exit_animations: [{ type: 'fade',  duration: 0.4,  easing: 'quadratic-in' }] },
      { animations: [{ type: 'slide',      direction: 'left',  duration: 0.45, fade: false, easing: 'quadratic-out' }],                    exit_animations: [{ type: 'fade',  duration: 0.3,  easing: 'quadratic-in' }] },
      { animations: [{ type: 'scale',      start_scale: '108%', end_scale: '100%', fade: true, duration: 0.55, easing: 'quadratic-out' }], exit_animations: [{ type: 'fade',  duration: 0.35, easing: 'quadratic-in' }] },
      { animations: [{ type: 'color-wipe', direction: 'right', color: colorAccent, duration: 0.55, easing: 'quadratic-out' }],             exit_animations: [{ type: 'fade',  duration: 0.4,  easing: 'quadratic-in' }] },
      { animations: [{ type: 'slide',      direction: 'up',    duration: 0.45, fade: false, easing: 'quadratic-out' }],                    exit_animations: [{ type: 'fade',  duration: 0.3,  easing: 'quadratic-in' }] },
      { animations: [{ type: 'scale',      start_scale: '92%', end_scale: '100%', fade: true, duration: 0.6, easing: 'back-out' }],        exit_animations: [{ type: 'fade',  duration: 0.35, easing: 'quadratic-in' }] },
      { animations: [{ type: 'wipe',       direction: 'right', duration: 0.5,  easing: 'quadratic-out' }],                                 exit_animations: [{ type: 'fade',  duration: 0.35, easing: 'quadratic-in' }] },
      { animations: [{ type: 'scale',      start_scale: '106%', end_scale: '100%', fade: true, duration: 0.55, easing: 'quadratic-out' }], exit_animations: [{ type: 'fade',  duration: 0.35, easing: 'quadratic-in' }] },
      { animations: [{ type: 'color-wipe', direction: 'up',    color: colorPrimary, duration: 0.55, easing: 'quadratic-out' }],            exit_animations: [{ type: 'fade',  duration: 0.4,  easing: 'quadratic-in' }] },
      { animations: [{ type: 'slide',      direction: 'right', duration: 0.45, fade: false, easing: 'quadratic-out' }],                    exit_animations: [{ type: 'fade',  duration: 0.3,  easing: 'quadratic-in' }] },
      { animations: [{ type: 'scale',      start_scale: '82%', end_scale: '100%', fade: true, duration: 0.65, easing: 'back-out' }],       exit_animations: [{ type: 'fade',  duration: 0.35, easing: 'quadratic-in' }] },
      { animations: [{ type: 'spin',       rotation: '-10°', fade: true, duration: 0.55, easing: 'quadratic-out' }],                       exit_animations: [{ type: 'fade',  duration: 0.35, easing: 'quadratic-in' }] },
    ];
    return AUTO[idx % AUTO.length];
  };

  // ── Text animation resolvers (10 types) ──────────────────────────────────────
  const resolveTitleAnim = (animation: string, idx: number, layout: LayoutType): any[] => {
    const a = String(animation || '').toLowerCase();
    if (a === 'none')    return [];
    if (a === 'fade')    return [{ type: 'fade',  duration: 0.6, easing: 'quadratic-out' }];
    if (a === 'zoom')    return [{ type: 'scale', start_scale: '86%', end_scale: '100%', fade: true, duration: 0.65, easing: 'back-out' }];
    if (a === 'pop')     return [{ type: 'scale', start_scale: '76%', end_scale: '100%', fade: true, duration: 0.55, easing: 'back-out' }];
    if (a === 'elastic') return [{ type: 'scale', start_scale: '62%', end_scale: '100%', fade: true, duration: 0.7,  easing: 'back-out' }];
    if (a === 'blur')    return [{ type: 'blur',  blur: 22, fade: true, duration: 0.7, easing: 'quadratic-out' }];
    if (a === 'wipe')    return [{ type: 'wipe',  direction: 'right', duration: 0.6, easing: 'quadratic-out' }];
    if (a === 'rise')    return [{ type: 'slide', direction: 'up',   distance: '18%', fade: true, duration: 0.7, easing: 'quadratic-out' }];
    if (a === 'drop') {
      const dir = (layout === 'top' || layout === 'billboard') ? 'down' : 'up';
      return [{ type: 'slide', direction: dir, distance: '12%', fade: true, duration: 0.6, easing: 'quadratic-out' }];
    }
    // 'slide' or default — layout-aware direction
    if (layout === 'center' || layout === 'caption')
      return [{ type: 'scale', start_scale: '88%', end_scale: '100%', fade: true, duration: 0.6, easing: 'back-out' }];
    if (layout === 'billboard') {
      const dirs = ['down', 'left', 'down', 'right'] as const;
      return [{ type: 'slide', direction: dirs[idx % dirs.length], distance: '10%', fade: true, duration: 0.55, easing: 'quadratic-out' }];
    }
    if (layout === 'top') {
      const dirs = ['down', 'left', 'down', 'right'] as const;
      return [{ type: 'slide', direction: dirs[idx % dirs.length], distance: '8%', fade: true, duration: 0.55, easing: 'quadratic-out' }];
    }
    const dirs = ['up', 'right', 'up', 'left', 'up', 'right', 'up'] as const;
    const dir  = dirs[idx % dirs.length];
    return [{ type: 'slide', direction: dir, distance: dir === 'up' ? '10%' : '7%', fade: true, duration: 0.55, easing: 'quadratic-out' }];
  };

  const resolveSubAnim = (animation: string, idx: number): any[] => {
    const a = String(animation || '').toLowerCase();
    if (a === 'none')    return [];
    if (a === 'blur')    return [{ type: 'blur',  blur: 14, fade: true, duration: 0.65, easing: 'quadratic-out' }];
    if (a === 'zoom')    return [{ type: 'scale', start_scale: '92%', end_scale: '100%', fade: true, duration: 0.55, easing: 'back-out' }];
    if (a === 'pop')     return [{ type: 'scale', start_scale: '86%', end_scale: '100%', fade: true, duration: 0.5,  easing: 'back-out' }];
    if (a === 'elastic') return [{ type: 'scale', start_scale: '78%', end_scale: '100%', fade: true, duration: 0.6,  easing: 'back-out' }];
    if (a === 'fade')    return [{ type: 'fade',  duration: 0.65, easing: 'quadratic-out' }];
    if (a === 'rise')    return [{ type: 'slide', direction: 'up', distance: '12%', fade: true, duration: 0.6, easing: 'quadratic-out' }];
    if (a === 'wipe')    return [{ type: 'wipe',  direction: 'right', duration: 0.55, easing: 'quadratic-out' }];
    const cycle = idx % 4;
    if (cycle === 0) return [{ type: 'slide', direction: 'up', distance: '6%', fade: true, duration: 0.5,  easing: 'quadratic-out' }];
    if (cycle === 1) return [{ type: 'fade',  duration: 0.65, easing: 'quadratic-out' }];
    if (cycle === 2) return [{ type: 'blur',  blur: 10, fade: true, duration: 0.55, easing: 'quadratic-out' }];
    return                  [{ type: 'slide', direction: 'up', distance: '4%', fade: true, duration: 0.5,  easing: 'quadratic-out' }];
  };

  // ── Ken Burns: pan + zoom via keyframes (Canva-grade cinematic motion) ──────
  const resolveKenBurns = (idx: number, dur: number): Record<string, any> => {
    // zoom: alternating in/out
    const scales: [string, string][] = [
      ['100%', '115%'], ['116%', '102%'], ['100%', '114%'], ['113%', '100%'],
      ['102%', '116%'], ['115%', '100%'], ['100%', '113%'], ['112%', '100%'],
    ];
    const [s0, s1] = scales[idx % scales.length];
    // pan: subtle drift in 8 directions
    const pans = [
      { x0: '50%', y0: '50%', x1: '52.5%', y1: '48.5%' },
      { x0: '52%', y0: '52%', x1: '49.5%', y1: '50%'   },
      { x0: '49%', y0: '51%', x1: '51.5%', y1: '49%'   },
      { x0: '52%', y0: '49%', x1: '50%',   y1: '51.5%' },
      { x0: '50%', y0: '52%', x1: '52%',   y1: '50%'   },
      { x0: '51%', y0: '50%', x1: '49%',   y1: '52%'   },
      { x0: '49%', y0: '49%', x1: '51%',   y1: '51%'   },
      { x0: '52%', y0: '51%', x1: '50%',   y1: '49%'   },
    ];
    const p = pans[idx % pans.length];
    const t = `${dur} s`;
    return {
      x:       [{ time: '0 s', value: p.x0 }, { time: t, value: p.x1, easing: 'quintic-in-out' }],
      y:       [{ time: '0 s', value: p.y0 }, { time: t, value: p.y1, easing: 'quintic-in-out' }],
      x_scale: [{ time: '0 s', value: s0   }, { time: t, value: s1,   easing: 'linear' }],
      y_scale: [{ time: '0 s', value: s0   }, { time: t, value: s1,   easing: 'linear' }],
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
    const hasImg    = imgUrl.startsWith('http');
    const isLastCut = i === totalCuts - 1;
    const isFirstCut = i === 0;

    // Layout: explicit on cut or cycling sequence
    const validLayouts: string[] = ['bottom', 'top', 'center', 'caption', 'billboard'];
    const layout: LayoutType = validLayouts.includes(String(cut.layout || ''))
      ? (cut.layout as LayoutType)
      : LAYOUT_SEQ[i % LAYOUT_SEQ.length];
    const lp = getLayoutProps(layout);

    const { animations, exit_animations } = resolveSceneTransition(String(cut.transition || ''), i);
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
    // isBottom = text at bottom, isTop = text at top
    const isBottom = layout === 'bottom';
    const isTop    = layout === 'top';

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
      const gradY0 = isTop ? '100%' : '0%';   // gradient start (transparent side)
      const gradY1 = isTop ? '0%' : '100%';   // gradient end (dark side)
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
      fill_color: '#ffffff',
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
        font_size: lp.subSize, fill_color: 'rgba(255,255,255,0.88)',
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
        text: 'プロフィールをチェック  →',
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
      audio_fade_out: 1.5,
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

const renderCreatomateJob = async (_req: Request, job: any) => {
  const payload = (job.payload || {}) as Record<string, unknown>;

  const source = buildCreatomateInlineSource(payload);

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
    throw new Error((Array.isArray(data) ? data[0]?.message : data?.message) || 'creatomate render failed');
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
    const source = buildCreatomateInlineSource(payload);
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

app.post('/api/pal-video/generate', async (req: Request, res: Response) => {
  try {
    if (!CREATOMATE_API_KEY) {
      return res.status(500).json({ success: false, error: 'CREATOMATE_API_KEY is missing' });
    }
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });

    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });
    const result = await renderCreatomateJob(req, job);
    return res.json({ success: true, job: result.updated, renderId: result.renderId, previewUrl: result.previewUrl });
  } catch (error) {
    console.error('[pal-db] pal-video generate failed', error);
    const message = error instanceof Error ? error.message : 'failed to generate pal_video';
    return res.status(500).json({ success: false, error: message });
  }
});

app.post('/api/pal-video/render', async (req: Request, res: Response) => {
  try {
    if (!CREATOMATE_API_KEY) {
      return res.status(500).json({ success: false, error: 'CREATOMATE_API_KEY is missing' });
    }
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });

    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });
    const result = await renderCreatomateJob(req, job);
    return res.json({ success: true, job: result.updated, renderId: result.renderId, previewUrl: result.previewUrl });
  } catch (error) {
    console.error('[pal-db] pal-video render failed', error);
    const message = error instanceof Error ? error.message : 'failed to render pal_video';
    return res.status(500).json({ success: false, error: message });
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
