import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import type { Request, Response } from 'express';
import multer from 'multer';
import path from 'path';
import { spawn } from 'child_process';
import { existsSync, mkdirSync, unlinkSync } from 'fs';
import { promises as fs } from 'fs';
import { tmpdir } from 'os';
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

const parseResolution = (raw?: string | null) => {
  const match = String(raw || '').match(/(\d+)\s*x\s*(\d+)/i);
  const width = match ? Number(match[1]) : 1080;
  const height = match ? Number(match[2]) : 1920;
  return {
    width: Number.isFinite(width) ? width : 1080,
    height: Number.isFinite(height) ? height : 1920,
  };
};

const escapeDrawtext = (raw: string) => {
  return String(raw || '')
    .replace(/\\/g, '\\\\')
    .replace(/:/g, '\\:')
    .replace(/'/g, "\\'")
    .replace(/\n/g, '\\n');
};

const runFfmpeg = async (args: string[]) => {
  await new Promise<void>((resolve, reject) => {
    const proc = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    proc.stderr.on('data', (data) => {
      stderr += String(data || '');
    });
    proc.on('error', (error) => reject(error));
    proc.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(stderr || `ffmpeg exited with ${code}`));
    });
  });
};

const MAX_IMAGE_BYTES = 8 * 1024 * 1024;
const DOWNLOAD_TIMEOUT_MS = 8000;

const downloadImage = async (url: string, dir: string): Promise<string | null> => {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DOWNLOAD_TIMEOUT_MS);
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeoutId);
    if (!response.ok) return null;
    const contentLength = Number(response.headers.get('content-length') || 0);
    if (contentLength && contentLength > MAX_IMAGE_BYTES) return null;
    const contentType = response.headers.get('content-type') || '';
    const extension = contentType.includes('png') ? 'png' : 'jpg';
    const name = `pal-video-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${extension}`;
    const filePath = path.join(dir, name);
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.byteLength > MAX_IMAGE_BYTES) return null;
    await fs.writeFile(filePath, buffer);
    return filePath;
  } catch (error) {
    console.error('[pal-db] image download failed', error);
    return null;
  }
};

const resolveFontFile = () => {
  const candidates = [
    process.env.PAL_VIDEO_FONT_FILE?.trim(),
    '/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf',
    '/usr/share/fonts/opentype/noto/NotoSansJP-Regular.otf',
    '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
    '/usr/share/fonts/truetype/noto/NotoSansCJKjp-Regular.otf',
    '/usr/share/fonts/truetype/noto/NotoSansJP-Regular.otf',
    '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
  ].filter(Boolean) as string[];
  const found = candidates.find((candidate) => existsSync(candidate));
  return found || '';
};

const FONT_FILE = resolveFontFile();
const FONT_FAMILY = 'Noto Sans CJK JP';
const CREATOMATE_API_URL = 'https://api.creatomate.com/v2/renders';
const CREATOMATE_API_KEY = String(process.env.CREATOMATE_API_KEY || '').trim();
const CREATOMATE_TEMPLATE_ID = String(process.env.CREATOMATE_TEMPLATE_ID || '').trim();

const escapeFfmpegExpr = (expr: string) => expr.replace(/,/g, '\\,');

const buildDrawtextFilters = (
  mainText: string,
  subText: string,
  durationSec: number,
  animation: string,
  transition: string,
) => {
  const filters: string[] = [];
  const fontPart = FONT_FILE
    ? `:fontfile=${FONT_FILE}`
    : `:font=${FONT_FAMILY}`;
  const fadeIn = 0.35;
  const fadeOut = 0.35;
  const safeAnimation = String(animation || '').toLowerCase();
  const safeTransition = String(transition || '').toLowerCase();
  const useFade = safeTransition !== 'none';
  const useSlide = safeAnimation === 'slide';
  const useFloat = safeAnimation === 'float';
  const usePop = safeAnimation === 'pop';
  const baseAlpha = useFade
    ? escapeFfmpegExpr(
        `if(lt(t,${fadeIn}),t/${fadeIn},if(lt(t,${Math.max(durationSec - fadeOut, fadeIn)}),1,((${durationSec}-t)/${fadeOut})))`,
      )
    : '1';
  const mainSlide = useSlide ? escapeFfmpegExpr('if(lt(t,0.6),(0.6-t)*40,0)') : '0';
  const subSlide = useSlide ? escapeFfmpegExpr('if(lt(t,0.6),(0.6-t)*24,0)') : '0';
  const mainFloat = useFloat ? escapeFfmpegExpr('sin(t*2*PI/3)*6') : '0';
  const subFloat = useFloat ? escapeFfmpegExpr('sin(t*2*PI/3)*4') : '0';
  const mainFontScale = usePop ? escapeFfmpegExpr('1+0.06*exp(-t*6)') : '1';
  const subFontScale = usePop ? escapeFfmpegExpr('1+0.04*exp(-t*6)') : '1';
  if (mainText) {
    filters.push(
      `drawtext=text='${escapeDrawtext(mainText)}'${fontPart}:x=(w-text_w)/2:y=h*0.64+${mainSlide}+${mainFloat}:fontsize=h*0.06*${mainFontScale}:fontcolor=white:alpha=${baseAlpha}:box=1:boxcolor=black@0.35:boxborderw=16`,
    );
  }
  if (subText) {
    filters.push(
      `drawtext=text='${escapeDrawtext(subText)}'${fontPart}:x=(w-text_w)/2:y=h*0.75+${subSlide}+${subFloat}:fontsize=h*0.035*${subFontScale}:fontcolor=white:alpha=${baseAlpha}:box=1:boxcolor=black@0.3:boxborderw=12`,
    );
  }
  return filters;
};

const PAL_VIDEO_TEMPLATE_MAP: Record<string, string> = {
  instagram_feed: 'a02095a2-9469-4f52-9bcd-66fc884453a1',
  promotion: '516cafa1-15cc-44e3-8a39-af5a07862bc0',
  youtube: '979f7579-5567-4d7b-a615-777d825d9f9d',
};

const resolvePalVideoTemplateCandidates = (purpose: string) => {
  const mapped = PAL_VIDEO_TEMPLATE_MAP[purpose];
  return [mapped, CREATOMATE_TEMPLATE_ID || 'pal_video_fixed_v1'].filter(Boolean);
};

const buildCreatomateFallbackPlan = (payload: Record<string, unknown>) => {
  const cuts = Array.isArray(payload?.cuts) ? payload.cuts : [];
  const durationSec = Number(payload?.durationSec || 30);
  const sceneCount = cuts.length > 0 ? cuts.length : Math.max(1, Math.min(7, Math.ceil(durationSec / 4)));
  const baseDuration = 4;
  const lastDuration = Math.max(1, durationSec - baseDuration * (sceneCount - 1));
  const purpose = String(payload?.purpose || 'instagram_reel');
  const templateCandidates = resolvePalVideoTemplateCandidates(purpose);
  const safeCuts = cuts.length > 0
    ? cuts
    : Array.from({ length: sceneCount }).map((_, index) => ({
        durationSec: index === sceneCount - 1 ? lastDuration : baseDuration,
        imageUrl: (payload?.imageUrls as string[] | undefined)?.[index] || (payload?.imageUrls as string[] | undefined)?.[0] || '',
        textMain: index === 0 ? payload?.telopMain : `ポイント${index + 1}`,
        textSub: index === 0 ? payload?.telopSub : '',
        templateId: templateCandidates[index % templateCandidates.length],
        textAnimation: 'none',
        textTransition: 'none',
      }));

  const templateMode = safeCuts.some((cut: any) => Boolean(cut?.templateId)) ? 'dynamic' : 'fixed';

  return {
    templateId: CREATOMATE_TEMPLATE_ID || 'pal_video_fixed_v1',
    templateMode,
    scenes: safeCuts.map((cut: any) => ({
      durationSec: Number(cut.durationSec || baseDuration),
      imageUrl: String(cut.imageUrl || ''),
      title: String(cut.textMain || payload?.telopMain || ''),
      subtitle: String(cut.textSub || payload?.telopSub || ''),
      templateId: String(cut.templateId || templateCandidates[0] || CREATOMATE_TEMPLATE_ID || 'pal_video_fixed_v1'),
      textAnimation: String(cut.textAnimation || 'none'),
      textTransition: String(cut.textTransition || 'none'),
    })),
    style: {
      primaryColor: String(payload?.colorPrimary || '#E95464'),
      accentColor: String(payload?.colorAccent || '#1c9a8b'),
      font: 'NotoSansJP',
    },
    audio: { bgm: String(payload?.bgm || '') },
    dynamicTemplateCandidates: [],
  };
};

const buildCreatomateModifications = (plan: Record<string, unknown>) => {
  const scenes = Array.isArray(plan?.scenes) ? plan.scenes : [];
  const style = (plan?.style || {}) as Record<string, unknown>;
  const audio = (plan?.audio || {}) as Record<string, unknown>;
  const modifications: Array<{ id: string; source?: string; text?: string; color?: string; value?: string }> = [];

  scenes.slice(0, 7).forEach((scene: any, index: number) => {
    const slot = String(index + 1).padStart(2, '0');
    const bg = String(scene?.imageUrl || '').trim();
    const title = String(scene?.title || '').trim();
    const sub = String(scene?.subtitle || '').trim();
    if (bg) modifications.push({ id: `scene_${slot}`, source: bg });
    if (title) modifications.push({ id: `scene_${slot}_title`, text: title });
    if (sub) modifications.push({ id: `scene_${slot}_sub`, text: sub });
    if (scene?.durationSec) {
      modifications.push({ id: `scene_${slot}_duration`, value: String(scene.durationSec) });
    }
  });

  const primary = String(style?.primaryColor || '').trim();
  const accent = String(style?.accentColor || '').trim();
  if (primary) modifications.push({ id: 'accent_primary', color: primary });
  if (accent) modifications.push({ id: 'accent_secondary', color: accent });

  const bgm = String(audio?.bgm || '').trim();
  if (bgm.startsWith('http')) {
    modifications.push({ id: 'bgm_track', source: bgm });
  }

  return modifications;
};

const MAX_VIDEO_BYTES = 160 * 1024 * 1024;

const downloadVideo = async (url: string, dir: string): Promise<string | null> => {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DOWNLOAD_TIMEOUT_MS * 2);
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeoutId);
    if (!response.ok) return null;
    const contentLength = Number(response.headers.get('content-length') || 0);
    if (contentLength && contentLength > MAX_VIDEO_BYTES) return null;
    const name = `pal-video-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.mp4`;
    const filePath = path.join(dir, name);
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.byteLength > MAX_VIDEO_BYTES) return null;
    await fs.writeFile(filePath, buffer);
    return filePath;
  } catch (error) {
    console.error('[pal-db] video download failed', error);
    return null;
  }
};

const buildCreatomateSceneModifications = (
  scene: Record<string, unknown>,
  style: Record<string, unknown>,
  audio: Record<string, unknown>,
) => {
  const modifications: Array<{ id: string; source?: string; text?: string; color?: string; value?: string }> = [];
  const bg = String(scene?.imageUrl || '').trim();
  const title = String(scene?.title || '').trim();
  const sub = String(scene?.subtitle || '').trim();
  if (bg) modifications.push({ id: 'scene_01', source: bg });
  if (title) modifications.push({ id: 'scene_01_title', text: title });
  if (sub) modifications.push({ id: 'scene_01_sub', text: sub });
  if (scene?.durationSec) {
    modifications.push({ id: 'scene_01_duration', value: String(scene.durationSec) });
  }

  const primary = String(style?.primaryColor || '').trim();
  const accent = String(style?.accentColor || '').trim();
  if (primary) modifications.push({ id: 'accent_primary', color: primary });
  if (accent) modifications.push({ id: 'accent_secondary', color: accent });

  const bgm = String(audio?.bgm || '').trim();
  if (bgm.startsWith('http')) {
    modifications.push({ id: 'bgm_track', source: bgm });
  }

  return modifications;
};

const renderCreatomateScene = async (templateId: string, modifications: unknown[]) => {
  const response = await fetch(CREATOMATE_API_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CREATOMATE_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      template_id: templateId,
      modifications,
    }),
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data?.error || 'creatomate render failed');
  }

  const renderItem = Array.isArray(data) ? data[0] : data?.render || data;
  const renderId = String(renderItem?.id || renderItem?.render_id || '').trim();
  const previewUrl = String(renderItem?.url || '').trim();
  if (!previewUrl) {
    throw new Error('creatomate render url missing');
  }
  return { renderId, previewUrl };
};

const concatVideoFiles = async (files: string[], outputPath: string) => {
  if (files.length === 1) {
    await fs.copyFile(files[0], outputPath);
    return;
  }
  const listPath = path.join(path.dirname(outputPath), `concat-${Date.now()}.txt`);
  const listContent = files
    .map((file) => `file '${file.replace(/'/g, "'\\''")}'`)
    .join('\n');
  await fs.writeFile(listPath, listContent);
  try {
    await runFfmpeg([
      '-y',
      '-hide_banner',
      '-loglevel', 'error',
      '-f', 'concat',
      '-safe', '0',
      '-i', listPath,
      '-c:v', 'libx264',
      '-r', '24',
      '-pix_fmt', 'yuv420p',
      '-c:a', 'aac',
      '-b:a', '128k',
      outputPath,
    ]);
  } finally {
    if (existsSync(listPath)) {
      unlinkSync(listPath);
    }
  }
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
        unlinkSync(filePath);
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
    const job = await upsertPalVideoJob(req.body || {});
    return res.json({ success: true, job });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'failed to save pal_video job';
    return res.status(400).json({ success: false, error: message });
  }
});

app.get('/api/pal-video/ffmpeg-check', async (_req: Request, res: Response) => {
  try {
    await runFfmpeg(['-version']);
    return res.json({ success: true, installed: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'ffmpeg check failed';
    return res.status(500).json({ success: false, installed: false, error: message });
  }
});

app.post('/api/pal-video/generate', async (req: Request, res: Response) => {
  try {
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });

    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });

    const payload = (job.payload || {}) as Record<string, unknown>;
    const rawResolution = parseResolution(String(payload?.resolution || '1080x1920'));
    const maxPreviewWidth = 720;
    const scaleRatio = rawResolution.width > maxPreviewWidth
      ? maxPreviewWidth / rawResolution.width
      : 1;
    const resolution = {
      width: Math.round(rawResolution.width * scaleRatio),
      height: Math.round(rawResolution.height * scaleRatio),
    };
    const colorPrimary = String(payload?.colorPrimary || '#E95464').trim() || '#E95464';
    const imageUrls = Array.isArray(payload?.imageUrls) ? payload.imageUrls : [];
    const templateId = String(payload?.templateId || '').trim();
    const rawCuts = Array.isArray(payload?.cuts) ? payload.cuts : [];
    const allowedTransitions = new Set(['fade', 'none']);
    const normalizeTransition = (value: unknown) => {
      const next = String(value || '').toLowerCase();
      return allowedTransitions.has(next) ? next : 'none';
    };
    const normalizeAnimation = (value: unknown) => {
      const next = String(value || '').toLowerCase();
      if (next === 'none' || next === 'fade' || next === 'slide' || next === 'float' || next === 'pop') return next;
      return 'none';
    };
    const cuts = rawCuts
      .map((cut) => ({
        durationSec: Math.max(1, Math.round(Number((cut as Record<string, unknown>)?.durationSec || 0) || 0)),
        imageUrl: String((cut as Record<string, unknown>)?.imageUrl || '').trim(),
        textMain: String((cut as Record<string, unknown>)?.textMain || '').trim(),
        textSub: String((cut as Record<string, unknown>)?.textSub || '').trim(),
        textTransition: normalizeTransition((cut as Record<string, unknown>)?.textTransition),
        textAnimation: normalizeAnimation((cut as Record<string, unknown>)?.textAnimation),
      }))
      .filter((cut) => cut.durationSec > 0);

    const outputDir = path.join(publicDir, 'pal-video');
    await fs.mkdir(outputDir, { recursive: true });
    const outputName = `${jobId}.mp4`;
    const outputPath = path.join(outputDir, outputName);
    const tempOutputPath = path.join(tmpdir(), `pal-video-${jobId}.mp4`);

    const tempDir = path.join(tmpdir(), 'pal-video');
    await fs.mkdir(tempDir, { recursive: true });
    const templates = new Map<string, { label: string; durationSec: number; scenes: number }>([
      ['modern_15', { label: 'Modern: シンプル & クリーン', durationSec: 15, scenes: 4 }],
      ['pop_15', { label: 'Pop: 元気 & 親しみ', durationSec: 15, scenes: 4 }],
      ['corporate_30', { label: 'Corporate: 信頼と実績', durationSec: 30, scenes: 7 }],
      ['elegant_30', { label: 'Elegant: ラグジュアリー', durationSec: 30, scenes: 7 }],
    ]);

    const resolvedTemplate = templates.get(templateId) || templates.get('modern_15')!;
    const hasCuts = cuts.length > 0;
    const durationSec = hasCuts
      ? cuts.reduce((sum, cut) => sum + cut.durationSec, 0)
      : Math.min(Number(payload?.durationSec || resolvedTemplate.durationSec) || resolvedTemplate.durationSec, resolvedTemplate.durationSec);
    const sceneCount = hasCuts ? cuts.length : resolvedTemplate.scenes;
    const baseSceneSeconds = Math.floor(durationSec / sceneCount);
    const remainder = durationSec - baseSceneSeconds * sceneCount;
    const sceneDurations = hasCuts
      ? cuts.map((cut) => cut.durationSec)
      : Array.from({ length: sceneCount }).map((_, index) => baseSceneSeconds + (index < remainder ? 1 : 0));

    const hearingAnswers = Array.isArray(payload?.hearingAnswers) ? payload.hearingAnswers : [];
    const hearingQueue = [...hearingAnswers];
    const pickAnswerText = () => {
      const next = hearingQueue.shift();
      return next?.a ? String(next.a) : '';
    };
    const purposeText = String(payload?.purpose || '用途未設定');
    const telopMainText = String(payload?.telopMain || '').trim() || 'テロップ未設定';
    const telopSubText = String(payload?.telopSub || '').trim() || 'サブテロップ未設定';

    const fallbackTexts = Array.from({ length: sceneCount }).map((_, index) => {
      if (index === 0) return { main: telopMainText, sub: telopSubText };
      if (index === sceneCount - 1) return { main: 'お問い合わせはこちら', sub: 'ご相談お待ちしています' };
      if (index === 1) return { main: `用途: ${purposeText}`, sub: '' };
      const answer = pickAnswerText();
      return { main: answer || `ポイント${index}`, sub: '' };
    });

    const sceneTexts = hasCuts
      ? cuts.map((cut, index) => ({
          main: cut.textMain || fallbackTexts[index]?.main || '',
          sub: cut.textSub || fallbackTexts[index]?.sub || '',
        }))
      : fallbackTexts;

    console.log('[pal-db] pal-video generate start', {
      jobId,
      durationSec,
      resolution,
      templateId: resolvedTemplate.label,
      sceneCount,
      hasImage: imageUrls.length > 0,
      fontFile: FONT_FILE || null,
    });

    const downloadedImages: (string | null)[] = [];
    if (hasCuts) {
      for (const cut of cuts) {
        const url = cut.imageUrl;
        if (!url) {
          downloadedImages.push(null);
          continue;
        }
        const downloaded = await downloadImage(String(url), tempDir);
        downloadedImages.push(downloaded);
      }
    } else {
      for (const url of imageUrls) {
        const downloaded = await downloadImage(String(url), tempDir);
        if (downloaded) downloadedImages.push(downloaded);
      }
    }

    const sceneFiles: string[] = [];
    for (let i = 0; i < sceneCount; i += 1) {
      const sceneDuration = sceneDurations[i];
      const imagePath = hasCuts
        ? downloadedImages[i]
        : (downloadedImages.length > 0 ? downloadedImages[i % downloadedImages.length] : null);
      const baseFilters: string[] = [];
      if (imagePath) {
        baseFilters.push(`scale=${resolution.width}:${resolution.height}:force_original_aspect_ratio=increase`);
        baseFilters.push(`crop=${resolution.width}:${resolution.height}`);
      }
      baseFilters.push('format=yuv420p');
      const animation = hasCuts ? cuts[i]?.textAnimation : 'slide';
      const transition = hasCuts ? cuts[i]?.textTransition : 'fade';
      const drawtextFilters = buildDrawtextFilters(
        sceneTexts[i].main,
        sceneTexts[i].sub,
        sceneDuration,
        animation,
        transition,
      );
      const vf = [...baseFilters, ...drawtextFilters].join(',');
      const sceneFile = path.join(tempDir, `scene-${jobId}-${i}.mp4`);

      const args = imagePath
        ? [
            '-y',
            '-hide_banner',
            '-loglevel', 'error',
            '-loop', '1',
            '-i', imagePath,
            '-t', String(sceneDuration),
            '-vf', vf,
            '-r', '24',
            '-c:v', 'libx264',
            '-preset', 'ultrafast',
            '-threads', '1',
            '-pix_fmt', 'yuv420p',
            sceneFile,
          ]
        : [
            '-y',
            '-hide_banner',
            '-loglevel', 'error',
            '-f', 'lavfi',
            '-i', `color=c=${colorPrimary}:s=${resolution.width}x${resolution.height}:d=${sceneDuration}`,
            '-vf', vf,
            '-r', '24',
            '-c:v', 'libx264',
            '-preset', 'ultrafast',
            '-threads', '1',
            '-pix_fmt', 'yuv420p',
            sceneFile,
          ];

      await runFfmpeg(args);
      sceneFiles.push(sceneFile);
    }

    if (sceneFiles.length === 1) {
      await fs.copyFile(sceneFiles[0], tempOutputPath);
    } else {
      const transitionSec = 0.4;
      const inputArgs = sceneFiles.flatMap((file) => ['-i', file]);
      let filter = '';
      let currentLabel = '[0:v]';
      let timeline = sceneDurations[0];
      for (let i = 0; i < sceneFiles.length - 1; i += 1) {
        const nextLabel = `[${i + 1}:v]`;
        const outputLabel = `[vxf${i + 1}]`;
        const offset = Math.max(timeline - transitionSec, 0);
        filter += `${currentLabel}${nextLabel}xfade=transition=fade:duration=${transitionSec}:offset=${offset}${outputLabel};`;
        currentLabel = outputLabel;
        timeline += sceneDurations[i + 1] - transitionSec;
      }
      filter += `${currentLabel}format=yuv420p[vid]`;

      await runFfmpeg([
        '-y',
        '-hide_banner',
        '-loglevel', 'error',
        ...inputArgs,
        '-filter_complex', filter,
        '-map', '[vid]',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-threads', '1',
        '-pix_fmt', 'yuv420p',
        '-movflags', '+faststart',
        tempOutputPath,
      ]);
    }

    await fs.rename(tempOutputPath, outputPath).catch(async () => {
      await fs.copyFile(tempOutputPath, outputPath);
      await fs.unlink(tempOutputPath).catch(() => undefined);
    });

    console.log('[pal-db] pal-video generate done', { jobId, outputName });

    const previewUrl = `${getPublicBaseUrl(req)}/pal-video/${encodeURIComponent(outputName)}`;
    const nextStatus = job.status === '承認済' ? job.status : '確認中';
    const updated = await upsertPalVideoJob({
      id: job.id,
      paletteId: job.paletteId,
      planCode: job.planCode,
      status: nextStatus,
      payload: job.payload,
      previewUrl,
      youtubeUrl: job.youtubeUrl,
    });

    return res.json({ success: true, job: updated });
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

    const payload = (job.payload || {}) as Record<string, unknown>;
    const plan = (payload?.creatomatePlan as Record<string, unknown> | undefined) || buildCreatomateFallbackPlan(payload);
    const scenes = Array.isArray(plan?.scenes) ? plan.scenes : [];
    const templateMode = String(plan?.templateMode || '').trim().toLowerCase();
    const hasPerSceneTemplate = scenes.some((scene: any) => Boolean(scene?.templateId));
    const useDynamic = templateMode === 'dynamic' && scenes.length > 0 && hasPerSceneTemplate;

    if (useDynamic) {
      const outputDir = path.join(publicDir, 'pal-video');
      await fs.mkdir(outputDir, { recursive: true });
      const tempDir = path.join(tmpdir(), `pal-video-creatomate-${jobId}`);
      await fs.mkdir(tempDir, { recursive: true });
      const outputName = `${jobId}-dynamic.mp4`;
      const outputPath = path.join(outputDir, outputName);
      const renderIds: string[] = [];
      const sceneFiles: string[] = [];

      for (const scene of scenes.slice(0, 7)) {
        const sceneTemplateId = String(scene?.templateId || plan?.templateId || CREATOMATE_TEMPLATE_ID || '').trim();
        if (!sceneTemplateId) {
          return res.status(500).json({ success: false, error: 'CREATOMATE_TEMPLATE_ID is missing' });
        }
        const modifications = buildCreatomateSceneModifications(
          scene as Record<string, unknown>,
          (plan?.style || {}) as Record<string, unknown>,
          (plan?.audio || {}) as Record<string, unknown>,
        );
        const { renderId, previewUrl } = await renderCreatomateScene(sceneTemplateId, modifications);
        renderIds.push(renderId);
        const downloaded = await downloadVideo(previewUrl, tempDir);
        if (!downloaded) {
          return res.status(502).json({ success: false, error: 'creatomate clip download failed' });
        }
        sceneFiles.push(downloaded);
      }

      await concatVideoFiles(sceneFiles, outputPath);
      const previewUrl = `${getPublicBaseUrl(req)}/pal-video/${encodeURIComponent(outputName)}`;
      const nextStatus = job.status === '承認済' ? job.status : '確認中';
      const updated = await upsertPalVideoJob({
        id: job.id,
        paletteId: job.paletteId,
        planCode: job.planCode,
        status: nextStatus,
        payload: {
          ...payload,
          creatomatePlan: plan,
          creatomateTemplateId: String(plan?.templateId || CREATOMATE_TEMPLATE_ID || 'pal_video_fixed_v1'),
          creatomateRenderId: renderIds[0] || '',
          creatomateRenderIds: renderIds,
        },
        previewUrl,
        youtubeUrl: job.youtubeUrl,
      });

      return res.json({ success: true, job: updated, renderId: renderIds[0] || '', previewUrl });
    }

    const templateId = String(plan?.templateId || CREATOMATE_TEMPLATE_ID || '').trim();
    if (!templateId) {
      return res.status(500).json({ success: false, error: 'CREATOMATE_TEMPLATE_ID is missing' });
    }

    const modifications = buildCreatomateModifications(plan);
    const modificationSummary = {
      count: modifications.length,
      ids: modifications.map((item) => item.id),
      sample: modifications.slice(0, 6),
    };
    console.log('[pal-db] creatomate render request', {
      jobId,
      templateId,
      modificationSummary,
    });
    const response = await fetch(CREATOMATE_API_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${CREATOMATE_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        template_id: templateId,
        modifications,
      }),
    });

    const data = await response.json().catch(() => ({}));
    console.log('[pal-db] creatomate render response', {
      jobId,
      status: response.status,
      data,
    });
    if (!response.ok) {
      return res.status(502).json({ success: false, error: data?.error || 'creatomate render failed' });
    }

    const renderItem = Array.isArray(data) ? data[0] : data?.render || data;
    const renderId = String(renderItem?.id || renderItem?.render_id || '').trim();
    const previewUrl = String(renderItem?.url || '').trim() || null;
    const nextStatus = job.status === '承認済' ? job.status : '確認中';

    const updated = await upsertPalVideoJob({
      id: job.id,
      paletteId: job.paletteId,
      planCode: job.planCode,
      status: nextStatus,
      payload: {
        ...payload,
        creatomatePlan: plan,
        creatomateTemplateId: templateId,
        creatomateRenderId: renderId,
      },
      previewUrl,
      youtubeUrl: job.youtubeUrl,
    });

    return res.json({ success: true, job: updated, renderId, previewUrl });
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
