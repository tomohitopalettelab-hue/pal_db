/**
 * FFmpeg-based video renderer — Creatomate の代替
 * 画像+テキスト+BGM を合成して MP4 を生成する
 */
import { spawn } from 'child_process';
import { createWriteStream, createReadStream } from 'fs';
import { promises as fs } from 'fs';
import path from 'path';
import { get as httpsGet } from 'https';
import { get as httpGet } from 'http';
import { IncomingMessage } from 'http';
import { createRequire } from 'module';

const _require = createRequire(import.meta.url);
const _ffmpegStaticPath: string | null = (() => {
  try { return _require('ffmpeg-static') as string | null; } catch { return null; }
})();

// ─── FFmpeg runner (spawn, stdio:pipe for stderr only) ────────────────────────
// exec のように stdout/stderr を丸ごとバッファしないため OOM を防ぐ
const runFFmpeg = (bin: string, args: string[], timeoutMs = 600000): Promise<void> =>
  new Promise((resolve, reject) => {
    const stderrChunks: Buffer[] = [];
    const proc = spawn(bin, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    proc.stderr.on('data', (chunk: Buffer) => {
      // stderr は最大 200KB だけ保持（デバッグ用）
      if (stderrChunks.reduce((s, c) => s + c.length, 0) < 200 * 1024) {
        stderrChunks.push(chunk);
      }
    });
    const timer = setTimeout(() => { proc.kill('SIGKILL'); reject(new Error('FFmpeg timeout')); }, timeoutMs);
    proc.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) {
        resolve();
      } else {
        const stderr = Buffer.concat(stderrChunks).toString('utf8').slice(0, 2000);
        reject(new Error(`FFmpeg exit ${code}: ${stderr}`));
      }
    });
    proc.on('error', (e) => { clearTimeout(timer); reject(e); });
  });

// 永続ディスク優先、なければ /tmp
export const DATA_DIR = process.env.PAL_DB_MEDIA_DIR ? '/var/data/pal-video' : '/tmp/pal-video';
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
  s.replace(/\n|\r/g, ' ')
   .replace(/\\/g, '\\\\')
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
    await runFFmpeg('ffmpeg', ['-version'], 5000);
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

// ─── Ken Burns バリエーション — overlay ベース実装 ───────────────────────────
//
// crop の eval=frame は古い FFmpeg ビルドで "Option not found" になる。
// overlay フィルターは x,y を常に per-frame で評価（t = タイムスタンプ秒）
// するため、eval オプション不要で全バージョンで動作する。
//
// 手法:
//   1. 画像を 1.07× にスケールしパン余白を確保
//   2. 黒キャンバス [0] の上に画像 [1] をオーバーレイ
//   3. overlay の x,y 式に t を使ってカメラパンを表現
//
const getBurnsFilter = (
  index: number, dur: number, w: number, h: number,
): { sw: number; sh: number; xExpr: string; yExpr: string } => {
  const ZOOM = 1.07;
  const sw = Math.round(w * ZOOM);     // スケール後幅
  const sh = Math.round(h * ZOOM);     // スケール後高さ
  const dx = sw - w;                   // 横パン可能量 (px)
  const dy = sh - h;                   // 縦パン可能量 (px)
  const cx = Math.round(dx / 2);       // 中央X オフセット
  const cy = Math.round(dy / 2);       // 中央Y オフセット
  const d  = (dur + 0.5).toFixed(3);  // 分母に使う秒数（少し大きめにしてオーバーランを防ぐ）

  // overlay の x,y はキャンバス上の画像左上座標
  // 負値 = 画像をキャンバス外にシフト → 別の領域が見える（パン効果）
  let xExpr: string;
  let yExpr: string;

  switch (index % 6) {
    case 0: // 左上→右下ドリー（左上端から右下端へ）
      xExpr = `floor(-${dx}*t/${d})`; yExpr = `floor(-${dy}*t/${d})`; break;
    case 1: // 右下→左上ドリー（右下端から左上端へ）
      xExpr = `floor(-${dx}*(1-t/${d}))`; yExpr = `floor(-${dy}*(1-t/${d}))`; break;
    case 2: // 左→右パン（Y 中央固定）
      xExpr = `floor(-${dx}*t/${d})`; yExpr = String(-cy); break;
    case 3: // 右→左パン（Y 中央固定）
      xExpr = `floor(-${dx}*(1-t/${d}))`; yExpr = String(-cy); break;
    case 4: // 上→下パン（X 中央固定）
      xExpr = String(-cx); yExpr = `floor(-${dy}*t/${d})`; break;
    default: // 中央固定（最も安定）
      xExpr = String(-cx); yExpr = String(-cy); break;
  }

  return { sw, sh, xExpr, yExpr };
};

// ─── スタイル別カラーグレーディング ─────────────────────────────────────────

const getColorGrade = (style: string): string => {
  switch (style) {
    case 'magazine':
      // 高コントラスト・彩度高め・ウォームトーン
      return 'eq=contrast=1.18:brightness=0.02:saturation=1.35,colorchannelmixer=rr=1.06:gg=1.0:bb=0.92';
    case 'minimal':
      // 低彩度・明るめ・クリーン
      return 'eq=contrast=0.95:brightness=0.06:saturation=0.75';
    case 'gradient':
      // 深みのある色・青みがかったクール
      return 'eq=contrast=1.2:brightness=-0.02:saturation=1.3,colorchannelmixer=rr=0.95:gg=0.98:bb=1.08';
    case 'collage':
      // ビビッド・鮮やか
      return 'eq=contrast=1.12:brightness=0.0:saturation=1.45';
    default: // standard
      return 'eq=contrast=1.08:brightness=0.01:saturation=1.15';
  }
};

// ─── Shape + Text overlay filter ─────────────────────────────────────────────

const overlayFilter = (
  mainText: string, subText: string,
  layout: string, w: number, h: number, font: string,
  colorAccent: string, dur: number, cutIndex: number,
): string => {
  const isPortrait = h > w;
  const mSize  = Math.round(h * (isPortrait ? 0.036 : 0.042));
  const sSize  = Math.round(h * (isPortrait ? 0.021 : 0.025));
  const margin = Math.round(h * (isPortrait ? 0.065 : 0.055));
  const bandH  = Math.round(h * (isPortrait ? 0.27 : 0.23));
  const fadeIn = Math.min(0.6, dur * 0.15).toFixed(2);
  const slide  = Math.round(h * 0.035); // スライド量（px）
  const accent = colorAccent.replace('#', '');

  const atTop    = layout === 'top' || layout === 'billboard';
  const atCenter = layout === 'center';

  // NotoSansCJK の実描画高さは fontsize の約1.45倍（ascender+descender込み）
  // Y座標計算ではこの実高さを使わないと重なりが発生する
  const CJK = 1.45;
  const mActH = Math.round(mSize * CJK);  // メインテキスト実描画高さ
  const sActH = Math.round(sSize * CJK);  // サブテキスト実描画高さ
  const lineGap = Math.round(mSize * 0.35); // テキスト間の追加余白

  // 数値で Y 位置を計算（スライドアニメーション用）
  // ※ drawtext の y= はバウンディングボックス上端座標
  const mYpx = atTop
    ? margin
    : atCenter
    ? Math.round(h / 2) - Math.round((mActH + (subText ? lineGap + sActH : 0)) / 2)
    : h - margin - (subText ? sActH + lineGap + mActH : mActH);
  const sYpx = atTop
    ? margin + mActH + lineGap
    : atCenter
    ? Math.round(h / 2) + Math.round((mActH + (mainText ? lineGap : 0)) / 2) - Math.round(sActH / 2)
    : h - margin - sActH;

  // スライド方向: top は上から、それ以外は下から
  const dir = atTop ? -1 : 1;
  const mYanim = `'if(lt(t,${fadeIn}),${mYpx}+${dir * slide}*(1-t/${fadeIn}),${mYpx})'`;
  const sYanim = `'if(lt(t,${fadeIn}),${sYpx}+${dir * slide}*(1-t/${fadeIn}),${sYpx})'`;

  const parts: string[] = [];

  // ── シェイプ: レイアウト別に異なるデザイン ────────────────────
  if (atTop) {
    // グラデーション風の上部バンド（2段重ね）
    parts.push(`drawbox=x=0:y=0:w=iw:h=${bandH}:color=0x000000@0.6:t=fill`);
    parts.push(`drawbox=x=0:y=0:w=iw:h=${Math.round(bandH * 0.5)}:color=0x000000@0.2:t=fill`);
    parts.push(`drawbox=x=0:y=${bandH - 3}:w=iw:h=3:color=0x${accent}@0.95:t=fill`);
    // 左端縦ライン
    parts.push(`drawbox=x=0:y=0:w=4:h=${bandH}:color=0x${accent}@0.95:t=fill`);
  } else if (atCenter) {
    // 中央バンド + 両サイドライン
    const bandY = Math.round(h / 2 - bandH / 2);
    parts.push(`drawbox=x=0:y=${bandY}:w=iw:h=${bandH}:color=0x000000@0.6:t=fill`);
    parts.push(`drawbox=x=0:y=${bandY}:w=5:h=${bandH}:color=0x${accent}@0.95:t=fill`);
    parts.push(`drawbox=x=iw-5:y=${bandY}:w=5:h=${bandH}:color=0x${accent}@0.95:t=fill`);
    parts.push(`drawbox=x=0:y=${bandY - 1}:w=iw:h=2:color=0x${accent}@0.5:t=fill`);
    parts.push(`drawbox=x=0:y=${bandY + bandH}:w=iw:h=2:color=0x${accent}@0.5:t=fill`);
  } else {
    // 下部バンド: グラデーション風（濃淡2段）+ トップライン
    const bandY = h - bandH;
    parts.push(`drawbox=x=0:y=${bandY}:w=iw:h=${bandH}:color=0x000000@0.6:t=fill`);
    parts.push(`drawbox=x=0:y=${bandY + Math.round(bandH * 0.5)}:w=iw:h=${Math.round(bandH * 0.5)}:color=0x000000@0.2:t=fill`);
    parts.push(`drawbox=x=0:y=${bandY}:w=iw:h=3:color=0x${accent}@0.95:t=fill`);
    // カット番号によって右端装飾を変える
    if (cutIndex % 2 === 0) {
      parts.push(`drawbox=x=iw-5:y=${bandY}:w=5:h=${bandH}:color=0x${accent}@0.5:t=fill`);
    }
  }

  if (!mainText && !subText) return parts.join(',');

  // ── テキスト: スライドイン + フェードイン ────────────────────
  const shadow = 'shadowcolor=black@0.8:shadowx=2:shadowy=2';
  const alpha  = `'if(lt(t,${fadeIn}),t/${fadeIn},1)'`;

  if (mainText) parts.push(
    `drawtext=fontfile='${font}':text='${esc(mainText)}':fontsize=${mSize}:fontcolor=white:x=(w-text_w)/2:y=${mYanim}:${shadow}:alpha=${alpha}`
  );
  if (subText) parts.push(
    `drawtext=fontfile='${font}':text='${esc(subText)}':fontsize=${sSize}:fontcolor=white@0.9:x=(w-text_w)/2:y=${sYanim}:${shadow}:alpha=${alpha}`
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
  style = 'standard',
): Promise<string> => {
  const dur = cut.duration;
  const frames = Math.ceil(dur * 30);
  const clipPath = `${TMP}/${jobId}_clip_${index}.mp4`;
  const fadeDur = Math.min(0.35, dur * 0.08);

  // 解像度は renderWithFFmpeg 側で preview/final を切り替え済み → ここでは使用
  const pw = w;
  const ph = h;

  let inputArgs: string[];
  let vfBase: string;
  let hasImage = false;

  if (cut.imageUrl) {
    const imgPath = `${TMP}/${jobId}_img_${index}.jpg`;
    try {
      await downloadFile(cut.imageUrl, imgPath);
    } catch (e) {
      console.warn(`[ffmpeg] image download failed for cut ${index}, using solid color:`, (e as Error).message);
      cut.imageUrl = null;
    }

    if (cut.imageUrl) {
      hasImage = true;
      if (preview) {
        // プレビュー: シンプルscale（メモリ節約）
        inputArgs = ['-loop', '1', '-t', String(dur), '-i', imgPath];
        vfBase = `[0:v]scale=${pw}:${ph}:force_original_aspect_ratio=increase,crop=${pw}:${ph},setpts=PTS-STARTPTS`;
      } else {
        // 最終: Ken Burns — overlay ベース（crop eval=frame 非対応 FFmpeg 対策）
        // [0] = 黒キャンバス (lavfi color)  [1] = 画像ファイル
        // overlay フィルターは t (秒) で x,y を per-frame 評価するため eval オプション不要
        const burns = getBurnsFilter(index, dur, pw, ph);
        inputArgs = [
          '-f', 'lavfi', '-i', `color=c=black:s=${pw}x${ph}:r=30`, // [0] canvas
          '-loop', '1', '-t', String(dur + 1), '-i', imgPath,       // [1] image
        ];
        vfBase =
          // 画像をフレームにフィット → 1.07× スケールアップ → ラベル付け
          `[1:v]scale=${pw}:${ph}:force_original_aspect_ratio=increase,crop=${pw}:${ph},` +
          `scale=${burns.sw}:${burns.sh}:flags=lanczos,setsar=1[_big];` +
          // 黒キャンバスに画像をオーバーレイ（t で per-frame パン）
          `[0:v][_big]overlay=x='${burns.xExpr}':y='${burns.yExpr}':shortest=1,` +
          `trim=duration=${dur},setpts=PTS-STARTPTS`;
      }
    } else {
      inputArgs = ['-f', 'lavfi', '-t', String(dur), '-i', `color=c=${hexToFF(colorPrimary)}:s=${pw}x${ph}:r=30`];
      vfBase = `[0:v]format=yuv420p`;
    }
  } else {
    inputArgs = ['-f', 'lavfi', '-t', String(dur), '-i', `color=c=${hexToFF(colorPrimary)}:s=${pw}x${ph}:r=30`];
    vfBase = `[0:v]format=yuv420p`;
  }

  const overlay = overlayFilter(cut.mainText, cut.subText, cut.layout, pw, ph, font, colorAccent, dur, index);
  const colorGrade = hasImage && !preview ? getColorGrade(style) : '';
  const fadeOut = dur - fadeDur;

  const filterChain = [
    vfBase,
    ...(colorGrade ? [colorGrade] : []),
    ...(hasImage && !preview ? ['vignette=angle=0.52'] : []),
    ...(overlay ? [overlay] : []),
    `format=yuv420p`,
    `fade=t=in:st=0:d=${fadeDur}`,
    `fade=t=out:st=${fadeOut}:d=${fadeDur}`,
  ];

  const preset = preview ? 'veryfast' : 'fast';
  const crf    = preview ? 26 : 20;

  console.log(`[ffmpeg] clip ${index} (${pw}x${ph}, preview=${preview})`);
  await runFFmpeg(ffmpeg, [
    '-y', '-loglevel', 'error',
    ...inputArgs,
    '-filter_complex', filterChain.join(','),
    '-c:v', 'libx264', '-preset', preset,
    '-crf', String(crf), '-r', '30', '-threads', '2', '-an',
    clipPath,
  ]);
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
  const style        = String(payload?.style         || 'standard');
  const bgmKey       = String(payload?.bgm           || '');
  const bgmUrl       = bgmKey.startsWith('http') ? bgmKey : (BGM_URLS[bgmKey] || '');

  const rawCuts: Cut[] = ((Array.isArray(payload?.cuts) ? payload.cuts : []) as any[])
    .slice(0, 5) // 最大5カット: xfade 同時デコード数を抑えメモリを節約
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

  // ── 尺補正: payload.duration と cuts の合計が乖離している場合にスケーリング ──
  const targetDuration = Number(payload?.duration || 0);
  if (targetDuration > 0) {
    const actualDuration = rawCuts.reduce((sum, c) => sum + c.duration, 0);
    if (actualDuration > 0 && Math.abs(actualDuration - targetDuration) > 1) {
      console.log(`[ffmpeg] scaling cut durations: ${actualDuration.toFixed(1)}s → ${targetDuration}s`);
      const scale = targetDuration / actualDuration;
      rawCuts.forEach(c => { c.duration = Math.max(1, parseFloat((c.duration * scale).toFixed(2))); });
    }
  }

  const total = rawCuts.length;

  // ── 1. Render clips sequentially ──────────────────────────────────────────
  // 画像は各クリップ完了後すぐ削除してディスク/メモリを解放
  const clipPaths: string[] = [];
  for (let i = 0; i < rawCuts.length; i++) {
    console.log(`[ffmpeg] cut ${i + 1}/${total}…`);
    await onProgress?.({ step: 'clip', current: i, total, label: `カット ${i + 1} / ${total} をレンダリング中...` });
    const p = await renderClip(rawCuts[i], i, jobId, w, h, colorPrimary, colorAccent, font, ffmpeg, preview, style);
    clipPaths.push(p);
    // 画像ファイルをクリップ完成直後に削除（ディスク節約）
    await fs.unlink(`${TMP}/${jobId}_img_${i}.jpg`).catch(() => {});
    await onProgress?.({ step: 'clip', current: i + 1, total, label: `カット ${i + 1} / ${total} 完了` });
  }

  // ── 2. Concat ──────────────────────────────────────────────────────────────
  // xfade を全クリップに一括適用するとデコーダーが同時に走りメモリが爆発する。
  // 代わりに「2 クリップずつ逐次 xfade → 中間 mp4 に書き出し」を繰り返す。
  // メモリ使用量: 常に 2 入力 × 1 xfade バッファ = ~90MB 以下に収まる。
  const TRANS_DUR = 0.5;
  let concatPath: string;

  if (clipPaths.length === 1) {
    concatPath = clipPaths[0];
  } else if (preview) {
    // プレビュー: concat demuxer → ultrafast 再エンコード（メモリほぼゼロ）
    concatPath = `${TMP}/${jobId}_concat.mp4`;
    const listPath = `${TMP}/${jobId}_list.txt`;
    const listContent = clipPaths.map(p => `file '${p.replace(/'/g, "'\\''")}'`).join('\n');
    await fs.writeFile(listPath, listContent, 'utf8');

    await onProgress?.({ step: 'concat', current: total, total, label: 'クリップを結合中...' });
    console.log('[ffmpeg] concat demuxer (preview)…');
    await runFFmpeg(ffmpeg, [
      '-y', '-loglevel', 'error',
      '-f', 'concat', '-safe', '0', '-i', listPath,
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '26', '-r', '30', '-threads', '2', '-an',
      '-movflags', '+faststart',
      concatPath,
    ], 120000);
    await fs.unlink(listPath).catch(() => {});
  } else {
    // 最終: 2 クリップずつ xfade で逐次結合
    // これにより FFmpeg が同時にデコードするストリームを常に 2 本に限定できる
    await onProgress?.({ step: 'concat', current: total, total, label: 'クリップを結合中...' });
    console.log('[ffmpeg] sequential pairwise xfade…');

    let currentPath = clipPaths[0];
    let currentDur  = rawCuts[0].duration;

    for (let i = 1; i < clipPaths.length; i++) {
      const nextPath  = clipPaths[i];
      const nextDur   = rawCuts[i].duration;
      const trans     = xfadeOf(rawCuts[i].transition, i - 1);
      const offset    = Math.max(0, currentDur - TRANS_DUR);
      const outPath   = `${TMP}/${jobId}_merge_${i}.mp4`;

      console.log(`[ffmpeg] xfade merge ${i}/${clipPaths.length - 1}: trans=${trans}, offset=${offset.toFixed(3)}s`);
      await runFFmpeg(ffmpeg, [
        '-y', '-loglevel', 'error',
        '-i', currentPath,
        '-i', nextPath,
        '-filter_complex',
          `[0:v][1:v]xfade=transition=${trans}:duration=${TRANS_DUR}:offset=${offset.toFixed(3)}[vout]`,
        '-map', '[vout]',
        '-c:v', 'libx264', '-preset', 'fast', '-crf', '20', '-r', '30', '-threads', '1', '-an',
        '-movflags', '+faststart',
        outPath,
      ], 300000);

      // 前の中間ファイルを削除（元の clipPath は残す ← cleanup ステップで削除）
      if (currentPath !== clipPaths[0] && !clipPaths.includes(currentPath)) {
        await fs.unlink(currentPath).catch(() => {});
      }
      currentPath  = outPath;
      currentDur  += nextDur - TRANS_DUR;

      await onProgress?.({ step: 'concat', current: i, total, label: `結合中 ${i}/${clipPaths.length - 1}...` });
    }
    concatPath = currentPath;
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

      console.log('[ffmpeg] adding BGM…');
      await runFFmpeg(ffmpeg, [
        '-y', '-loglevel', 'error',
        '-i', concatPath, '-i', bgmPath,
        '-filter_complex',
        `[1:a]atrim=0:${totalDur.toFixed(3)},asetpts=PTS-STARTPTS,afade=t=out:st=${fadeStart.toFixed(3)}:d=1.5,volume=0.65[a]`,
        '-map', '0:v', '-map', '[a]',
        '-c:v', 'copy', '-c:a', 'aac', '-b:a', '128k', '-shortest',
        '-movflags', '+faststart',
        outputPath,
      ], 60000);
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
