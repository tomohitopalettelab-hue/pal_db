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
const corsOriginList = corsOrigin === '*'
  ? '*'
  : corsOrigin
      .split(',')
      .map((origin) => origin.trim())
      .filter(Boolean);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.resolve(__dirname, '../public');
const PALETTE_ID_REGEX = /^[A-Z][0-9]{4}$/;
const mediaRootDir = path.join(publicDir, 'media');
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
    if (Array.isArray(corsOriginList) && corsOriginList.includes(origin)) {
      callback(null, origin);
      return;
    }
    callback(new Error('CORS not allowed'), false);
  },
}));
app.use(express.json({ limit: '2mb' }));
app.use(express.static(publicDir));

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

const downloadImage = async (url: string, dir: string): Promise<string | null> => {
  try {
    const response = await fetch(url);
    if (!response.ok) return null;
    const contentType = response.headers.get('content-type') || '';
    const extension = contentType.includes('png') ? 'png' : 'jpg';
    const name = `pal-video-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${extension}`;
    const filePath = path.join(dir, name);
    const buffer = Buffer.from(await response.arrayBuffer());
    await fs.writeFile(filePath, buffer);
    return filePath;
  } catch (error) {
    console.error('[pal-db] image download failed', error);
    return null;
  }
};

const buildDrawtextFilters = (mainText: string, subText: string) => {
  const filters: string[] = [];
  if (mainText) {
    filters.push(
      `drawtext=text='${escapeDrawtext(mainText)}':x=(w-text_w)/2:y=h*0.66:fontsize=h*0.06:fontcolor=white:box=1:boxcolor=black@0.35:boxborderw=16`,
    );
  }
  if (subText) {
    filters.push(
      `drawtext=text='${escapeDrawtext(subText)}':x=(w-text_w)/2:y=h*0.76:fontsize=h*0.035:fontcolor=white:box=1:boxcolor=black@0.3:boxborderw=12`,
    );
  }
  return filters;
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

app.post('/api/pal-video/generate', async (req: Request, res: Response) => {
  try {
    const jobId = String(req.body?.jobId || '').trim();
    if (!jobId) return res.status(400).json({ success: false, error: 'jobId is required' });

    const job = await getPalVideoJob(jobId);
    if (!job) return res.status(404).json({ success: false, error: 'job not found' });

    const payload = (job.payload || {}) as Record<string, unknown>;
    const resolution = parseResolution(String(payload?.resolution || '1080x1920'));
    const durationSec = Number(payload?.durationSec || 30) || 30;
    const mainText = String(payload?.telopMain || '').trim();
    const subText = String(payload?.telopSub || '').trim();
    const colorPrimary = String(payload?.colorPrimary || '#E95464').trim() || '#E95464';
    const imageUrls = Array.isArray(payload?.imageUrls) ? payload.imageUrls : [];

    const outputDir = path.join(publicDir, 'pal-video');
    await fs.mkdir(outputDir, { recursive: true });
    const outputName = `${jobId}.mp4`;
    const outputPath = path.join(outputDir, outputName);

    const tempDir = path.join(tmpdir(), 'pal-video');
    await fs.mkdir(tempDir, { recursive: true });
    const imagePath = imageUrls.length > 0
      ? await downloadImage(String(imageUrls[0]), tempDir)
      : null;

    const baseFilters: string[] = [];
    if (imagePath) {
      baseFilters.push(`scale=${resolution.width}:${resolution.height}:force_original_aspect_ratio=cover`);
      baseFilters.push(`crop=${resolution.width}:${resolution.height}`);
    }
    baseFilters.push('format=yuv420p');
    const drawtextFilters = buildDrawtextFilters(mainText, subText);
    const vf = [...baseFilters, ...drawtextFilters].join(',');

    const args = imagePath
      ? [
          '-y',
          '-loop', '1',
          '-i', imagePath,
          '-t', String(durationSec),
          '-vf', vf,
          '-r', '30',
          outputPath,
        ]
      : [
          '-y',
          '-f', 'lavfi',
          '-i', `color=c=${colorPrimary}:s=${resolution.width}x${resolution.height}:d=${durationSec}`,
          '-vf', vf,
          '-r', '30',
          outputPath,
        ];

    await runFfmpeg(args);

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
