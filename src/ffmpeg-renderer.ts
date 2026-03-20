/**
 * FFmpeg-based video renderer — Creatomate の代替
 * 画像+テキスト+BGM を合成して MP4 を生成する
 */
import { exec } from 'child_process';
import { createWriteStream, createReadStream } from 'fs';
import { promises as fs } from 'fs';
import path from 'path';
import { promisify } from 'util';
import { get as httpsGet } from 'https';
import { get as httpGet } from 'http';
import { IncomingMessage } from 'http';
import { createRequire } from 'module';

const _require = createRequire(import.meta.url);
const _ffmpegStaticPath: string | null = (() => {
  try { return _require('ffmpeg-static') as string | null; } catch { return null; }
})();

const execAsync = promisify(exec);

// 永続ディスク優先、なければ /tmp
const DATA_DIR = process.env.PAL_DB_MEDIA_DIR ? '/var/data/pal-video' : '/tmp/pal-video';
const TMP = DATA_DIR;

// リポジトリにバンドルされたフォント (fonts/NotoSansJP-Bold.otf)
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname_ts = path.dirname(__filename);
// dist/ffmpeg-renderer.js → ../fonts/NotoSansJP-Bold.otf (distの一つ上 = プロジェクトルート)
const BUNDLED_FONT = path.resolve(__dirname_ts, '../fonts/NotoSansJP-Bold.otf');

// System font candidates (Render.com Ubuntu: apt install fonts-noto-cjk)
const SYSTEM_FONTS = [
  '/usr/share/fonts/opentype/noto/NotoSansCJKjp-Bold.otf',
  '/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc',
  '/usr/share/fonts/opentype/noto/NotoSansCJKsc-Bold.otf',
  '/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc',
  '/usr/share/fonts/noto-cjk/NotoSansCJK-Bold.ttc',
];

const DESTINATION_DIMENSIONS: Record<string, [number, number]> = {
  instagram_reel:  [1080, 1920],
  instagram_story: [1080, 1920],
  tiktok:          [1080, 1920],
  youtube_short:   [1080, 1920],
  line_voom:       [1080, 1350],
  x_twitter:       [1080, 1350],
  facebook:        [1080, 1350],
  instagram_feed:  [1080, 1080],
  youtube:         [1920, 1080],
  web_banner:      [1920, 1080],
};

const BGM_URLS: Record<string, string> = {
  bright_pop:   'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3',
  cool_minimal: 'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-9.mp3',
  cinematic:    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-7.mp3',
  natural_warm: 'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-2.mp3',
};

// ─── Utilities ────────────────────────────────────────────────────────────────

const esc = (s: string) =>
  s.replace(/\\/g, '\\\\')
   .replace(/'/g, "\u2019")   // smart apostrophe avoids quote issues
   .replace(/:/g, '\\:')
   .replace(/\[/g, '\\[')
   .replace(/\]/g, '\\]')
   .replace(/,/g, '\\,')
   .replace(/;/g, '\\;');

const hexToFF = (hex: string) => `0x${hex.replace('#', '')}FF`;

// ─── File downloader with redirect support ────────────────────────────────────

const downloadFile = (url: string, dest: string, depth = 0): Promise<void> =>
  new Promise((resolve, reject) => {
    if (depth > 5) { reject(new Error('too many redirects')); return; }
    const getter = url.startsWith('https') ? httpsGet : httpGet;
    const req = getter(url, { headers: { 'User-Agent': 'pal-video-ffmpeg/1.0' } }, (res: IncomingMessage) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const location = res.headers.location;
        res.resume();
        downloadFile(location, dest, depth + 1).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      const file = createWriteStream(dest);
      res.pipe(file);
      file.on('finish', () => file.close(() => resolve()));
      file.on('error', (e) => { fs.unlink(dest).catch(() => {}); reject(e); });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`timeout: ${url}`)); });
  });

// ─── Font discovery / download ────────────────────────────────────────────────

let _fontPath: string | null = null;

export const ensureFont = async (): Promise<string> => {
  if (_fontPath) return _fontPath;

  // 1. リポジトリバンドルフォントを最優先
  try { await fs.access(BUNDLED_FONT); _fontPath = BUNDLED_FONT; return BUNDLED_FONT; } catch {}

  // 2. システムフォント (apt install fonts-noto-cjk)
  for (const f of SYSTEM_FONTS) {
    try { await fs.access(f); _fontPath = f; return f; } catch {}
  }

  throw new Error(`フォントが見つかりません。バンドルフォント: ${BUNDLED_FONT}`);
};

// ─── FFmpeg binary ────────────────────────────────────────────────────────────

let _ffmpegBin: string | null = null;

const getFFmpegBin = async (): Promise<string> => {
  if (_ffmpegBin) return _ffmpegBin;

  // Try system ffmpeg first
  try {
    await execAsync('which ffmpeg');
    _ffmpegBin = 'ffmpeg';
    return 'ffmpeg';
  } catch {}

  // Fall back to ffmpeg-static
  if (_ffmpegStaticPath) { _ffmpegBin = _ffmpegStaticPath; return _ffmpegStaticPath; }

  _ffmpegBin = 'ffmpeg';
  return 'ffmpeg';
};

// ─── xfade transition name map ────────────────────────────────────────────────

const xfadeOf = (transition: string, idx: number): string => {
  const dirs4 = ['slideleft', 'slideup', 'slideright', 'slidedown'] as const;
  const map: Record<string, string> = {
    fade:        'fade',
    slide:       dirs4[idx % 4],
    wipe:        'wipeleft',
    'color-wipe':'fade',
    zoom:        'smoothup',
    bounce:      'fadewhite',
    push:        dirs4[idx % 4],
    'film-roll': 'fade',
    circular:    'circleopen',
    flip:        'fadegrays',
    blur:        'fade',
    none:        'fade',
  };
  return map[transition] || 'fade';
};

// ─── Text overlay filter ─────────────────────────────────────────────────────

const textFilter = (
  mainText: string, subText: string,
  layout: string, w: number, h: number, font: string,
): string => {
  if (!mainText && !subText) return '';

  const isPortrait = h > w;
  const mSize = Math.round(h * (isPortrait ? 0.036 : 0.042));
  const sSize = Math.round(h * (isPortrait ? 0.021 : 0.025));
  const margin = Math.round(h * (isPortrait ? 0.065 : 0.055));

  const atTop    = layout === 'top' || layout === 'billboard';
  const atCenter = layout === 'center';

  const mY = atTop    ? `${margin}`
           : atCenter ? `(h-text_h)/2${subText ? `-${Math.round(mSize/2 + 8)}` : ''}`
           :            `h-${margin + (subText ? mSize + sSize + 14 : mSize)}`;
  const sY = atTop    ? `${margin + mSize + 10}`
           : atCenter ? `(h+text_h)/2${mainText ? `+8` : ''}`
           :            `h-${margin + sSize}`;

  const shadow = 'shadowcolor=black@0.75:shadowx=2:shadowy=2';
  const parts: string[] = [];

  if (mainText) parts.push(
    `drawtext=fontfile='${font}':text='${esc(mainText)}':fontsize=${mSize}:fontcolor=white:x=(w-text_w)/2:y=${mY}:${shadow}`
  );
  if (subText) parts.push(
    `drawtext=fontfile='${font}':text='${esc(subText)}':fontsize=${sSize}:fontcolor=white@0.88:x=(w-text_w)/2:y=${sY}:${shadow}`
  );
  return parts.join(',');
};

// ─── Per-cut clip renderer ────────────────────────────────────────────────────

interface Cut {
  id: string;
  duration: number;
  imageUrl: string | null;
  mainText: string;
  subText: string;
  layout: string;
  transition: string;
}

const renderClip = async (
  cut: Cut, index: number, jobId: string,
  w: number, h: number,
  colorPrimary: string, colorAccent: string,
  font: string, ffmpeg: string,
  preview = false,
): Promise<string> => {
  const dur = cut.duration;
  const frames = Math.ceil(dur * 30);
  const clipPath = `${TMP}/${jobId}_clip_${index}.mp4`;
  const fadeDur = Math.min(0.35, dur * 0.08);

  // プレビュー時は半解像度
  const pw = preview ? Math.round(w / 2) : w;
  const ph = preview ? Math.round(h / 2) : h;

  let inputPart: string;
  let vfBase: string;

  if (cut.imageUrl) {
    const imgPath = `${TMP}/${jobId}_img_${index}.jpg`;
    try {
      await downloadFile(cut.imageUrl, imgPath);
    } catch (e) {
      console.warn(`[ffmpeg] image download failed for cut ${index}, using solid color:`, (e as Error).message);
      cut.imageUrl = null;
    }

    if (cut.imageUrl) {
      if (preview) {
        // プレビュー: シンプルscale（zoompanはメモリ大量消費のため省略）
        inputPart = `-loop 1 -t ${dur} -i "${imgPath}"`;
        vfBase = `[0:v]scale=${pw}:${ph}:force_original_aspect_ratio=increase,crop=${pw}:${ph},setpts=PTS-STARTPTS`;
      } else {
        // 最終: Ken Burns ゆっくりズームイン（高品質）
        const zoom = `min(zoom+0.0012,1.08)`;
        inputPart = `-loop 1 -t ${dur + 1} -i "${imgPath}"`;
        vfBase = `[0:v]scale=${pw}:${ph}:force_original_aspect_ratio=increase,crop=${pw}:${ph},` +
                 `zoompan=z='${zoom}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=${frames}:s=${pw}x${ph}:fps=30,` +
                 `trim=duration=${dur},setpts=PTS-STARTPTS`;
      }
    } else {
      inputPart = `-f lavfi -t ${dur} -i "color=c=${hexToFF(colorPrimary)}:s=${pw}x${ph}:r=30"`;
      vfBase = `[0:v]format=yuv420p`;
    }
  } else {
    inputPart = `-f lavfi -t ${dur} -i "color=c=${hexToFF(colorPrimary)}:s=${pw}x${ph}:r=30"`;
    vfBase = `[0:v]format=yuv420p`;
  }

  const txt = textFilter(cut.mainText, cut.subText, cut.layout, pw, ph, font);
  const fadeOut = dur - fadeDur;
  const filters = [
    vfBase,
    ...(txt ? [txt] : []),
    `format=yuv420p`,
    `fade=t=in:st=0:d=${fadeDur}`,
    `fade=t=out:st=${fadeOut}:d=${fadeDur}`,
  ].join(',');

  const preset = preview ? 'veryfast' : 'fast';
  const crf    = preview ? 26 : 20;
  const cmd = `"${ffmpeg}" -y ${inputPart} -filter_complex "${filters}" ` +
    `-c:v libx264 -preset ${preset} -crf ${crf} -r 30 -an "${clipPath}"`;

  await execAsync(cmd, { timeout: 600000 });
  return clipPath;
};

// ─── Main export ──────────────────────────────────────────────────────────────

export type RenderProgress = {
  step: 'clip' | 'concat' | 'bgm' | 'done';
  current: number;   // 完了したカット数 (clip時)
  total: number;     // 総カット数
  label: string;     // 表示ラベル
};

export const renderWithFFmpeg = async (
  payload: Record<string, unknown>,
  jobId: string,
  onProgress?: (p: RenderProgress) => Promise<void> | void,
  preview = false,
): Promise<string> => {
  await fs.mkdir(TMP, { recursive: true });

  const [font, ffmpeg] = await Promise.all([ensureFont(), getFFmpegBin()]);
  console.log(`[ffmpeg] bin=${ffmpeg}, font=${font}, preview=${preview}`);

  const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
  const [fw, fh] = DESTINATION_DIMENSIONS[destination] || [1080, 1920];
  // プレビューは半解像度（ピクセル数1/4でzoompanが大幅高速化）
  const w = preview ? Math.round(fw / 2) : fw;
  const h = preview ? Math.round(fh / 2) : fh;
  const colorPrimary = String(payload?.colorPrimary || '#1A1A2E');
  const colorAccent  = String(payload?.colorAccent  || '#E95464');
  const bgmKey       = String(payload?.bgm           || '');
  const bgmUrl       = bgmKey.startsWith('http') ? bgmKey : (BGM_URLS[bgmKey] || '');

  const rawCuts: Cut[] = ((Array.isArray(payload?.cuts) ? payload.cuts : []) as any[])
    .slice(0, 7)
    .map((c: any) => ({
      id:         String(c.id       || ''),
      duration:   Number(c.duration || 4),
      imageUrl:   String(c.imageUrl || '').startsWith('http') ? String(c.imageUrl) : null,
      mainText:   String(c.mainText || ''),
      subText:    String(c.subText  || ''),
      layout:     String(c.layout   || 'bottom'),
      transition: String(c.transition || 'fade'),
    }));

  if (rawCuts.length === 0) throw new Error('cuts が空です');
  const total = rawCuts.length;

  // ── 1. Render clips sequentially ──────────────────────────────────────────
  const clipPaths: string[] = [];
  for (let i = 0; i < rawCuts.length; i++) {
    console.log(`[ffmpeg] cut ${i + 1}/${total}…`);
    await onProgress?.({ step: 'clip', current: i, total, label: `カット ${i + 1} / ${total} をレンダリング中...` });
    const p = await renderClip(rawCuts[i], i, jobId, w, h, colorPrimary, colorAccent, font, ffmpeg, preview);
    clipPaths.push(p);
    await onProgress?.({ step: 'clip', current: i + 1, total, label: `カット ${i + 1} / ${total} 完了` });
  }

  // ── 2. Concat with xfade transitions ──────────────────────────────────────
  const TRANS_DUR = 0.5;
  let concatPath: string;

  if (clipPaths.length === 1) {
    concatPath = clipPaths[0];
  } else {
    concatPath = `${TMP}/${jobId}_concat.mp4`;
    const inputArgs = clipPaths.map(p => `-i "${p}"`).join(' ');

    let filterParts = '';
    let currentStream = '[0:v]';
    let timeOffset = 0;

    for (let i = 0; i < clipPaths.length - 1; i++) {
      timeOffset += rawCuts[i].duration - TRANS_DUR;
      const isLast = i === clipPaths.length - 2;
      const outLabel = isLast ? '[vout]' : `[v${i}]`;
      const trans = xfadeOf(rawCuts[i + 1].transition, i);
      filterParts += `${currentStream}[${i + 1}:v]xfade=transition=${trans}:duration=${TRANS_DUR}:offset=${timeOffset.toFixed(3)}${outLabel};`;
      currentStream = outLabel;
    }

    const cmd = `"${ffmpeg}" -y ${inputArgs} ` +
      `-filter_complex "${filterParts.replace(/;$/, '')}" -map "[vout]" ` +
      `-c:v libx264 -preset fast -crf 20 -r 30 -an "${concatPath}"`;

    await onProgress?.({ step: 'concat', current: total, total, label: 'クリップを結合中...' });
    console.log('[ffmpeg] concatenating…');
    await execAsync(cmd, { timeout: 300000 });
  }

  // ── 3. Add BGM ────────────────────────────────────────────────────────────
  const outputPath = `${TMP}/${jobId}_output.mp4`;

  if (bgmUrl) {
    const bgmPath = `${TMP}/${jobId}_bgm.mp3`;
    try {
      await onProgress?.({ step: 'bgm', current: total, total, label: 'BGMを追加中...' });
      await downloadFile(bgmUrl, bgmPath);
      const totalDur = rawCuts.reduce((a, c) => a + c.duration, 0) - TRANS_DUR * (rawCuts.length - 1);
      const fadeStart = Math.max(0, totalDur - 1.5);

      const cmd = `"${ffmpeg}" -y -i "${concatPath}" -i "${bgmPath}" ` +
        `-filter_complex "[1:a]atrim=0:${totalDur.toFixed(3)},asetpts=PTS-STARTPTS,` +
        `afade=t=out:st=${fadeStart.toFixed(3)}:d=1.5,volume=0.65[a]" ` +
        `-map 0:v -map "[a]" -c:v copy -c:a aac -b:a 128k -shortest "${outputPath}"`;

      console.log('[ffmpeg] adding BGM…');
      await execAsync(cmd, { timeout: 60000 });
    } catch (e) {
      console.warn('[ffmpeg] BGM failed, using video-only:', (e as Error).message);
      await fs.copyFile(concatPath, outputPath);
    }
  } else {
    await fs.copyFile(concatPath, outputPath);
  }

  // ── 4. Cleanup ────────────────────────────────────────────────────────────
  const toDelete = [
    ...clipPaths,
    ...(concatPath !== clipPaths[0] ? [concatPath] : []),
    `${TMP}/${jobId}_bgm.mp3`,
    ...rawCuts.map((_, i) => `${TMP}/${jobId}_img_${i}.jpg`),
  ].filter(p => p !== outputPath);

  await Promise.all(toDelete.map(p => fs.unlink(p).catch(() => {})));

  console.log(`[ffmpeg] done → ${outputPath}`);
  return outputPath;
};
