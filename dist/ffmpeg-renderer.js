/**
 * FFmpeg-based video renderer — Creatomate の代替
 * 画像+テキスト+BGM を合成して MP4 を生成する
 */
import { spawn } from 'child_process';
import { createWriteStream } from 'fs';
import { promises as fs } from 'fs';
import path from 'path';
import { get as httpsGet } from 'https';
import { get as httpGet } from 'http';
import { createRequire } from 'module';
const _require = createRequire(import.meta.url);
const _ffmpegStaticPath = (() => {
    try {
        return _require('ffmpeg-static');
    }
    catch {
        return null;
    }
})();
// ─── FFmpeg runner (spawn, stdio:pipe for stderr only) ────────────────────────
// exec のように stdout/stderr を丸ごとバッファしないため OOM を防ぐ
const runFFmpeg = (bin, args, timeoutMs = 600000) => new Promise((resolve, reject) => {
    const stderrChunks = [];
    const proc = spawn(bin, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    proc.stderr.on('data', (chunk) => {
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
        }
        else {
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
const DESTINATION_DIMENSIONS = {
    instagram_reel: [1080, 1920],
    instagram_story: [1080, 1920],
    tiktok: [1080, 1920],
    youtube_short: [1080, 1920],
    line_voom: [1080, 1350],
    x_twitter: [1080, 1350],
    facebook: [1080, 1350],
    instagram_feed: [1080, 1080],
    youtube: [1920, 1080],
    web_banner: [1920, 1080],
};
const BGM_URLS = {
    bright_pop: 'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3',
    cool_minimal: 'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-9.mp3',
    cinematic: 'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-7.mp3',
    natural_warm: 'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-2.mp3',
};
// ─── Utilities ────────────────────────────────────────────────────────────────
const esc = (s) => s.replace(/\n|\r/g, ' ')
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\u2019") // smart apostrophe avoids quote issues
    .replace(/:/g, '\\:')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/,/g, '\\,')
    .replace(/;/g, '\\;');
const hexToFF = (hex) => `0x${hex.replace('#', '')}FF`;
// ─── File downloader with redirect support ────────────────────────────────────
const downloadFile = (url, dest, depth = 0) => new Promise((resolve, reject) => {
    if (depth > 5) {
        reject(new Error('too many redirects'));
        return;
    }
    const getter = url.startsWith('https') ? httpsGet : httpGet;
    const req = getter(url, { headers: { 'User-Agent': 'pal-video-ffmpeg/1.0' } }, (res) => {
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
        file.on('error', (e) => { fs.unlink(dest).catch(() => { }); reject(e); });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`timeout: ${url}`)); });
});
// ─── Font discovery / download ────────────────────────────────────────────────
let _fontPath = null;
export const ensureFont = async () => {
    if (_fontPath)
        return _fontPath;
    // 1. リポジトリバンドルフォントを最優先
    try {
        await fs.access(BUNDLED_FONT);
        _fontPath = BUNDLED_FONT;
        return BUNDLED_FONT;
    }
    catch { }
    // 2. システムフォント (apt install fonts-noto-cjk)
    for (const f of SYSTEM_FONTS) {
        try {
            await fs.access(f);
            _fontPath = f;
            return f;
        }
        catch { }
    }
    throw new Error(`フォントが見つかりません。バンドルフォント: ${BUNDLED_FONT}`);
};
// ─── FFmpeg binary ────────────────────────────────────────────────────────────
let _ffmpegBin = null;
const getFFmpegBin = async () => {
    if (_ffmpegBin)
        return _ffmpegBin;
    // 優先順位:
    //   1. /usr/local/bin/ffmpeg — render.yaml で DL した John Van Sickle 静的ビルド (最新)
    //   2. ffmpeg-static npm パッケージ
    //   3. システム ffmpeg (apt = 4.4.2 など古い可能性あり)
    const candidates = [
        '/usr/local/bin/ffmpeg', // 静的ビルド (Render.com ビルドステップで配置)
        ..._ffmpegStaticPath ? [_ffmpegStaticPath] : [],
        'ffmpeg', // PATH 上のシステム ffmpeg
    ];
    for (const bin of candidates) {
        try {
            await runFFmpeg(bin, ['-version'], 5000);
            console.log(`[ffmpeg] using: ${bin}`);
            _ffmpegBin = bin;
            return bin;
        }
        catch (e) {
            console.warn(`[ffmpeg] ${bin} not available:`, e.message.slice(0, 80));
        }
    }
    // 最終フォールバック (失敗しても呼び出し元でエラーになる)
    _ffmpegBin = 'ffmpeg';
    return 'ffmpeg';
};
// ─── xfade transition name map ────────────────────────────────────────────────
// idx を使って同じ transition 種別でも毎回方向を変え単調さを防ぐ
// render.yaml で John Van Sickle 最新静的ビルドを /usr/local/bin/ffmpeg に配置済み。
// FFmpeg 7.x 以上を前提として全 xfade transition を使用する。
const xfadeOf = (transition, idx) => {
    const slides = ['slideleft', 'slideright', 'slideup', 'slidedown'];
    const wipes = ['wipeleft', 'wiperight', 'wipeup', 'wipedown'];
    const covers = ['coverleft', 'coverright', 'coverup', 'coverdown'];
    const reveals = ['revealleft', 'revealright', 'revealup', 'revealdown'];
    const diags = ['diagtl', 'diagtr', 'diagbl', 'diagbr'];
    const slices = ['hlslice', 'hrslice', 'vuslice', 'vdslice'];
    const smooths = ['smoothleft', 'smoothright', 'smoothup', 'smoothdown'];
    const map = {
        'fade': ['fade', 'dissolve', 'distance'],
        'slide': slides,
        'wipe': wipes,
        'color-wipe': ['fadewhite', 'fadeblack'],
        'zoom': ['zoomin', ...smooths],
        'bounce': ['fadewhite', 'distance', 'pixelize'],
        'push': covers,
        'film-roll': slices,
        'circular': ['circleopen', 'circleclose', 'radial'],
        'flip': ['fadegrays', 'pixelize', 'flyeye'],
        'blur': ['hblur', 'fade', 'dissolve'],
        'stripe': [...reveals, ...diags],
        'none': ['fade'],
    };
    const opts = map[transition] || ['fade'];
    return opts[idx % opts.length];
};
// ─── Ken Burns / カメラワーク — overlay ベース実装 ───────────────────────────
//
// crop の eval=frame は古い FFmpeg ビルドで "Option not found" になる。
// overlay フィルターは x,y を常に per-frame で評価（t = タイムスタンプ秒）
// するため、eval オプション不要で全バージョンで動作する。
//
// 手法:
//   1. 画像をズーム倍率でスケールしパン余白を確保
//   2. 黒キャンバス [0] の上に画像 [1] をオーバーレイ
//   3. overlay の x,y 式に t を使ってカメラパンを表現
//
// animation 文字列から適切なカメラワークを選択する。
// 未知の animation は index % 6 のフォールバックパターンを使用。
//
const getBurnsFilter = (animation, index, dur, w, h) => {
    // アニメーション種別によりズーム倍率を変える
    // zoom_in/zoom_out 系: 大きめのズームでドリー感を演出
    // static 系: ほぼ動かないため最小ズーム
    const LARGE_MOVE = ['zoom_in_fast', 'fast_zoom_in', 'zoom_out', 'fast_zoom_out', 'diagonal_zoom'];
    const MINIMAL = ['static', 'wide_view', 'flash', 'brightness', 'blur', 'fade_to_black'];
    const ZOOM = LARGE_MOVE.includes(animation) ? 1.32
        : MINIMAL.includes(animation) ? 1.02
            : 1.07;
    const sw = Math.round(w * ZOOM);
    const sh = Math.round(h * ZOOM);
    const dx = sw - w;
    const dy = sh - h;
    const cx = Math.round(dx / 2);
    const cy = Math.round(dy / 2);
    const d = (dur + 0.5).toFixed(3);
    let xExpr;
    let yExpr;
    switch (animation) {
        // ─ ズーム系（コーナー→中心 = ドリープッシュ / 中心→コーナー = プルバック） ─
        case 'zoom_in_fast':
        case 'fast_zoom_in':
            xExpr = `floor(-${cx}*t/${d})`;
            yExpr = `floor(-${cy}*t/${d})`;
            break;
        case 'zoom_out':
        case 'fast_zoom_out':
            xExpr = `floor(-${cx}*(1-t/${d}))`;
            yExpr = `floor(-${cy}*(1-t/${d}))`;
            break;
        // ─ 水平パン ─
        case 'pan_right':
            xExpr = `floor(-${dx}*t/${d})`;
            yExpr = String(-cy);
            break;
        case 'pan_left':
            xExpr = `floor(-${dx}*(1-t/${d}))`;
            yExpr = String(-cy);
            break;
        // ─ 垂直パン ─
        case 'pan_down':
            xExpr = String(-cx);
            yExpr = `floor(-${dy}*t/${d})`;
            break;
        case 'pan_up':
            xExpr = String(-cx);
            yExpr = `floor(-${dy}*(1-t/${d}))`;
            break;
        // ─ 斜めズーム ─
        case 'diagonal_zoom':
            xExpr = `floor(-${cx}*t/${d})`;
            yExpr = `floor(-${dy}*t/${d})`;
            break;
        // ─ Static 系（フィルターエフェクトのみ、カメラは中央固定） ─
        case 'static':
        case 'wide_view':
        case 'flash':
        case 'brightness':
        case 'blur':
        case 'fade_to_black':
            xExpr = String(-cx);
            yExpr = String(-cy);
            break;
        default: {
            // フォールバック: index ベースの 6 パターン
            const FALLBACK_ZOOM = 1.07;
            const fSw = Math.round(w * FALLBACK_ZOOM);
            const fSh = Math.round(h * FALLBACK_ZOOM);
            const fdx = fSw - w;
            const fdy = fSh - h;
            const fcx = Math.round(fdx / 2);
            const fcy = Math.round(fdy / 2);
            const patterns = [
                [`floor(-${fdx}*t/${d})`, `floor(-${fdy}*t/${d})`],
                [`floor(-${fdx}*(1-t/${d}))`, `floor(-${fdy}*(1-t/${d}))`],
                [`floor(-${fdx}*t/${d})`, String(-fcy)],
                [`floor(-${fdx}*(1-t/${d}))`, String(-fcy)],
                [String(-fcx), `floor(-${fdy}*t/${d})`],
                [String(-fcx), String(-fcy)],
            ];
            return { sw: fSw, sh: fSh, xExpr: patterns[index % 6][0], yExpr: patterns[index % 6][1] };
        }
    }
    return { sw, sh, xExpr, yExpr };
};
// ─── スタイル別カラーグレーディング ─────────────────────────────────────────
const getColorGrade = (style) => {
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
// ─── Shape + Text overlay filter (CSS card デザインに準拠) ───────────────────
//
// CSS プレビューカード (CutPreviewCard) の以下要素を FFmpeg で再現:
//   pv-strip  : 左側縦アクセントストリップ (accent色, 幅11.5%)
//   白ドット   : ストリップ上部の白い小ドット
//   グラデ     : 画像上の半透明オーバーレイ (layout別グラデーション)
//   アクセントライン: テキスト上の短い横線
//   pv-plus1/2: 「+」デコレーション
//   pv-main/sub: テキスト (ストリップ右から左揃え, フェードイン)
//
const overlayFilter = (mainText, subText, layout, w, h, font, colorAccent, colorPrimary, dur, hasImage) => {
    const isPortrait = h > w;
    const accent = colorAccent.replace('#', '');
    const primary = colorPrimary.replace('#', '');
    // ─ CSS card 比率に合わせたサイズ計算 ─
    const stripW = Math.round(w * 0.115); // 左ストリップ幅 (11.5%)
    const textX = stripW + Math.round(w * 0.028); // テキスト開始X
    const mSize = Math.round(h * (isPortrait ? 0.034 : 0.042));
    const sSize = Math.round(h * (isPortrait ? 0.020 : 0.025));
    const margin = Math.round(h * 0.065);
    const lineW = Math.round(w * 0.060); // アクセントライン幅
    const lineH = Math.max(2, Math.round(h * 0.006)); // アクセントライン高さ
    const CJK = 1.45;
    const mActH = Math.round(mSize * CJK);
    const sActH = Math.round(sSize * CJK);
    const lineGap = Math.round(mSize * 0.40);
    const lineToText = Math.round(mSize * 0.35);
    const atTop = layout === 'top' || layout === 'billboard';
    const atCenter = layout === 'center';
    // ─ アクセントライン・テキスト Y 座標 ─
    let lineY, mY, sY;
    if (atTop) {
        lineY = margin;
        mY = lineY + lineH + lineToText;
        sY = mY + mActH + lineGap;
    }
    else if (atCenter) {
        const totalH = lineH + lineToText + mActH + (subText ? lineGap + sActH : 0);
        lineY = Math.round(h / 2 - totalH / 2);
        mY = lineY + lineH + lineToText;
        sY = mY + mActH + lineGap;
    }
    else {
        sY = h - margin - sActH;
        mY = sY - lineGap - mActH;
        lineY = mY - lineToText - lineH;
    }
    const fadeIn = Math.min(0.6, dur * 0.15).toFixed(2);
    const shadow = 'shadowcolor=black@0.8:shadowx=2:shadowy=2';
    const alpha = `'if(lt(t,${fadeIn}),t/${fadeIn},1)'`;
    const parts = [];
    // 1. 画像グラデーションオーバーレイ (CSS の gradient overlay に対応)
    if (hasImage) {
        if (atTop) {
            // linear-gradient(160deg, primary+BB 0%, primary+44 45%, transparent)
            parts.push(`drawbox=x=${stripW}:y=0:w=iw:h=${Math.round(h * 0.50)}:color=0x${primary}@0.73:t=fill`);
            parts.push(`drawbox=x=${stripW}:y=${Math.round(h * 0.50)}:w=iw:h=${Math.round(h * 0.25)}:color=0x${primary}@0.27:t=fill`);
        }
        else if (atCenter) {
            // colorPrimary + alpha 0.33
            parts.push(`drawbox=x=${stripW}:y=0:w=iw:h=ih:color=0x${primary}@0.33:t=fill`);
        }
        else {
            // linear-gradient(to top, primary+DD 0%, primary+66 40%, transparent 75%)
            const darkY = Math.round(h * 0.60);
            parts.push(`drawbox=x=${stripW}:y=${darkY}:w=iw:h=${h - darkY}:color=0x${primary}@0.87:t=fill`);
            parts.push(`drawbox=x=${stripW}:y=${Math.round(h * 0.25)}:w=iw:h=${Math.round(h * 0.35)}:color=0x${primary}@0.40:t=fill`);
        }
    }
    // 2. 左アクセントストリップ (pv-strip)
    parts.push(`drawbox=x=0:y=0:w=${stripW}:h=ih:color=0x${accent}FF:t=fill`);
    // 3. ストリップ上部の白ドット
    const dotSz = Math.max(3, Math.round(stripW * 0.18));
    const dotX = Math.round((stripW - dotSz) / 2);
    parts.push(`drawbox=x=${dotX}:y=${Math.round(h * 0.025)}:w=${dotSz}:h=${dotSz}:color=0xFFFFFF@0.95:t=fill`);
    // 4. テキスト上のアクセントライン
    parts.push(`drawbox=x=${textX}:y=${lineY}:w=${lineW}:h=${lineH}:color=0x${accent}FF:t=fill`);
    // 5. + デコレーション (右上・白)
    const plus1Sz = Math.round(h * 0.045);
    parts.push(`drawtext=fontfile='${font}':text='+':fontsize=${plus1Sz}:fontcolor=0xFFFFFF@0.65:x=${w - Math.round(w * 0.05)}:y=${Math.round(h * 0.018)}`);
    // 6. + デコレーション (左下・アクセントカラー)
    const plus2Sz = Math.round(h * 0.032);
    parts.push(`drawtext=fontfile='${font}':text='+':fontsize=${plus2Sz}:fontcolor=0x${accent}FF:x=${textX}:y=${h - Math.round(h * 0.058)}`);
    // 7. メインテキスト (フェードイン)
    if (mainText) {
        parts.push(`drawtext=fontfile='${font}':text='${esc(mainText)}':fontsize=${mSize}:fontcolor=white:x=${textX}:y=${mY}:${shadow}:alpha=${alpha}`);
    }
    // 8. サブテキスト (フェードイン)
    if (subText) {
        parts.push(`drawtext=fontfile='${font}':text='${esc(subText)}':fontsize=${sSize}:fontcolor=white@0.85:x=${textX}:y=${sY}:${shadow}:alpha=${alpha}`);
    }
    return parts.join(',');
};
const renderClip = async (cut, index, jobId, isFirst, isLast, w, h, colorPrimary, colorAccent, font, ffmpeg, preview = false, style = 'standard') => {
    const dur = cut.duration;
    const frames = Math.ceil(dur * 30);
    const clipPath = `${TMP}/${jobId}_clip_${index}.mp4`;
    const isFadeToBlack = cut.animation === 'fade_to_black';
    // preview: 全クリップにフェード（concat demuxer でのフェードスルー効果）
    // final:   先頭クリップのみフェードイン、末尾クリップのみフェードアウト
    //          （中間クリップはxfadeフィルターがトランジションを担当）
    const needFadeIn = preview || isFirst;
    const needFadeOut = preview || isLast || isFadeToBlack;
    const fadeDurIn = needFadeIn ? Math.min(0.4, dur * 0.10) : 0;
    const fadeDurOut = isFadeToBlack ? Math.min(dur * 0.45, 2.0)
        : (needFadeOut ? Math.min(0.4, dur * 0.10) : 0);
    const fadeDur = fadeDurOut; // fadeOut 計算に使用
    // 解像度は renderWithFFmpeg 側で preview/final を切り替え済み → ここでは使用
    const pw = w;
    const ph = h;
    let inputArgs;
    let vfBase;
    let hasImage = false;
    if (cut.imageUrl) {
        const imgPath = `${TMP}/${jobId}_img_${index}.jpg`;
        try {
            await downloadFile(cut.imageUrl, imgPath);
        }
        catch (e) {
            console.warn(`[ffmpeg] image download failed for cut ${index}, using solid color:`, e.message);
            cut.imageUrl = null;
        }
        if (cut.imageUrl) {
            hasImage = true;
            if (preview) {
                // プレビュー: シンプルscale（メモリ節約）
                inputArgs = ['-loop', '1', '-t', String(dur), '-i', imgPath];
                vfBase = `[0:v]scale=${pw}:${ph}:force_original_aspect_ratio=increase,crop=${pw}:${ph},setpts=PTS-STARTPTS`;
            }
            else {
                // 最終: Ken Burns / カメラワーク — overlay ベース（crop eval=frame 非対応 FFmpeg 対策）
                const burns = getBurnsFilter(cut.animation, index, dur, pw, ph);
                inputArgs = [
                    '-f', 'lavfi', '-i', `color=c=black:s=${pw}x${ph}:r=30`, // [0] canvas
                    '-loop', '1', '-t', String(dur + 1), '-i', imgPath, // [1] image
                ];
                vfBase =
                    // 画像をフレームにフィット → 1.07× スケールアップ → ラベル付け
                    `[1:v]scale=${pw}:${ph}:force_original_aspect_ratio=increase,crop=${pw}:${ph},` +
                        `scale=${burns.sw}:${burns.sh}:flags=lanczos,setsar=1[_big];` +
                        // 黒キャンバスに画像をオーバーレイ（t で per-frame パン）
                        `[0:v][_big]overlay=x='${burns.xExpr}':y='${burns.yExpr}':shortest=1,` +
                        `trim=duration=${dur},setpts=PTS-STARTPTS`;
            }
        }
        else {
            inputArgs = ['-f', 'lavfi', '-t', String(dur), '-i', `color=c=${hexToFF(colorPrimary)}:s=${pw}x${ph}:r=30`];
            vfBase = `[0:v]format=yuv420p`;
        }
    }
    else {
        inputArgs = ['-f', 'lavfi', '-t', String(dur), '-i', `color=c=${hexToFF(colorPrimary)}:s=${pw}x${ph}:r=30`];
        vfBase = `[0:v]format=yuv420p`;
    }
    const overlay = overlayFilter(cut.mainText, cut.subText, cut.layout, pw, ph, font, colorAccent, colorPrimary, dur, hasImage);
    const colorGrade = hasImage && !preview ? getColorGrade(style) : '';
    const fadeOut = dur - fadeDurOut;
    // ─ アニメーション別スペシャルエフェクト ─
    // flash     : 白フラッシュ（冒頭 0.15s で白からノーマルに）
    // brightness: 明るさ・彩度アップ（ポジティブ・感情カット）
    // blur      : ソフトフォーカス（テキスト強調カット）
    // 注: fade_to_black は fadeDurOut の延長で対応
    const specialFx = [];
    if (!preview) {
        if (cut.animation === 'flash') {
            specialFx.push('fade=t=in:st=0:d=0.12:color=white');
        }
        else if (cut.animation === 'brightness') {
            specialFx.push('eq=brightness=0.10:saturation=1.22:contrast=1.06');
        }
        else if (cut.animation === 'blur') {
            specialFx.push('boxblur=5:1');
        }
    }
    const filterChain = [
        vfBase,
        ...(colorGrade ? [colorGrade] : []),
        ...(hasImage && !preview ? ['vignette=angle=0.52'] : []),
        ...(overlay ? [overlay] : []),
        ...(specialFx.length > 0 ? specialFx : []),
        `format=yuv420p`,
        ...(fadeDurIn > 0 ? [`fade=t=in:st=0:d=${fadeDurIn}`] : []),
        ...(fadeDurOut > 0 ? [`fade=t=out:st=${fadeOut}:d=${fadeDurOut}`] : []),
    ];
    // ultrafast: ルックアヘッド・B-frame を無効化してメモリ使用量を最小化
    // Render.com 512MB 制限対策（fast は lookahead で +90MB 使用する）
    const preset = 'ultrafast';
    const crf = preview ? 26 : 20;
    console.log(`[ffmpeg] clip ${index} (${pw}x${ph}, preview=${preview})`);
    await runFFmpeg(ffmpeg, [
        '-y', '-loglevel', 'error',
        ...inputArgs,
        '-filter_complex', filterChain.join(','),
        '-c:v', 'libx264', '-preset', preset,
        '-crf', String(crf), '-r', '30', '-threads', '1', '-an',
        clipPath,
    ]);
    return clipPath;
};
export const renderWithFFmpeg = async (payload, jobId, onProgress, preview = false) => {
    await fs.mkdir(TMP, { recursive: true });
    const [font, ffmpeg] = await Promise.all([ensureFont(), getFFmpegBin()]);
    console.log(`[ffmpeg] bin=${ffmpeg}, font=${font}, preview=${preview}`);
    const destination = String(payload?.destination || payload?.purpose || 'instagram_reel');
    const [fw, fh] = DESTINATION_DIMENSIONS[destination] || [1080, 1920];
    // Render.com 512MB 制限対策: preview/final ともに半解像度（540x960 等）
    // 1080x1920 フルHDは Ken Burns overlay だけで ~300MB使用しOOMになる
    // 半解像度でフレームバッファが 1/4 に削減され余裕が生まれる
    const w = Math.round(fw / 2);
    const h = Math.round(fh / 2);
    const colorPrimary = String(payload?.colorPrimary || '#1A1A2E');
    const colorAccent = String(payload?.colorAccent || '#E95464');
    const style = String(payload?.style || 'standard');
    const bgmKey = String(payload?.bgm || '');
    const bgmUrl = bgmKey.startsWith('http') ? bgmKey : (BGM_URLS[bgmKey] || '');
    const logoUrl = String(payload?.logoUrl || '').startsWith('http') ? String(payload.logoUrl) : '';
    const rawCuts = (Array.isArray(payload?.cuts) ? payload.cuts : [])
        .slice(0, 20) // 最大20カット（60秒テンプレート対応）
        .map((c) => ({
        id: String(c.id || ''),
        duration: Number(c.duration || 4),
        imageUrl: String(c.imageUrl || '').startsWith('http') ? String(c.imageUrl) : null,
        mainText: String(c.mainText || ''),
        subText: String(c.subText || ''),
        layout: String(c.layout || 'bottom'),
        transition: String(c.transition || 'fade'),
        animation: String(c.animation || ''),
    }));
    if (rawCuts.length === 0)
        throw new Error('cuts が空です');
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
    const clipPaths = [];
    for (let i = 0; i < rawCuts.length; i++) {
        const isFirst = i === 0;
        const isLast = i === rawCuts.length - 1;
        console.log(`[ffmpeg] cut ${i + 1}/${total}…`);
        await onProgress?.({ step: 'clip', current: i, total, label: `カット ${i + 1} / ${total} をレンダリング中...` });
        const p = await renderClip(rawCuts[i], i, jobId, isFirst, isLast, w, h, colorPrimary, colorAccent, font, ffmpeg, preview, style);
        clipPaths.push(p);
        // 画像ファイルをクリップ完成直後に削除（ディスク節約）
        await fs.unlink(`${TMP}/${jobId}_img_${i}.jpg`).catch(() => { });
        await onProgress?.({ step: 'clip', current: i + 1, total, label: `カット ${i + 1} / ${total} 完了` });
    }
    // ── 2. Concat ──────────────────────────────────────────────────────────────
    // xfade を全クリップに一括適用するとデコーダーが同時に走りメモリが爆発する。
    // 代わりに「2 クリップずつ逐次 xfade → 中間 mp4 に書き出し」を繰り返す。
    // メモリ使用量: 常に 2 入力 × 1 xfade バッファ = ~90MB 以下に収まる。
    const TRANS_DUR = 0.5;
    let concatPath;
    if (clipPaths.length === 1) {
        concatPath = clipPaths[0];
    }
    else if (preview) {
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
        await fs.unlink(listPath).catch(() => { });
    }
    else {
        // 最終: 単一パス xfade チェーン — 全クリップを1回のFFmpeg呼び出しで処理
        // pairwise O(n²) の「蓄積動画を毎回再エンコード」問題を解消し O(n) に短縮
        // 20 入力デコーダーが同時に走るが、540×960 半解像度では合計 ~100MB 以内
        concatPath = `${TMP}/${jobId}_concat.mp4`;
        await onProgress?.({ step: 'concat', current: 1, total, label: 'トランジションを適用中...' });
        console.log(`[ffmpeg] single-pass xfade chain (${clipPaths.length} clips)…`);
        // filter_complex を組み立て: [0:v][1:v]xfade...[v01];[v01][2:v]xfade...[v02];...
        const filterParts = [];
        let cumOffset = 0;
        for (let i = 0; i < clipPaths.length - 1; i++) {
            // offset は前段の出力の末尾から TRANS_DUR 前 (累積)
            cumOffset += rawCuts[i].duration - TRANS_DUR;
            const transName = xfadeOf(rawCuts[i + 1].transition, i + 1);
            const offset = Math.max(0, cumOffset).toFixed(3);
            const inLabel = i === 0 ? '[0:v]' : `[xv${i}]`;
            const outLabel = i === clipPaths.length - 2 ? '[vout]' : `[xv${i + 1}]`;
            filterParts.push(`${inLabel}[${i + 1}:v]xfade=transition=${transName}:duration=${TRANS_DUR}:offset=${offset}${outLabel}`);
            console.log(`  xfade ${i + 1}/${clipPaths.length - 1}: ${transName} offset=${offset}s`);
        }
        const inputs = clipPaths.flatMap(p => ['-i', p]);
        await runFFmpeg(ffmpeg, [
            '-y', '-loglevel', 'error',
            ...inputs,
            '-filter_complex', filterParts.join(';'),
            '-map', '[vout]',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '20', '-r', '30', '-threads', '2', '-an',
            '-movflags', '+faststart',
            concatPath,
        ], 600000); // 10 分タイムアウト（単一パスで全クリップを処理）
    }
    // ── 3. Add BGM ────────────────────────────────────────────────────────────
    const outputPath = `${TMP}/${jobId}_output.mp4`;
    if (bgmUrl) {
        const bgmPath = `${TMP}/${jobId}_bgm.mp3`;
        try {
            await onProgress?.({ step: 'bgm', current: total, total, label: 'BGMを追加中...' });
            await downloadFile(bgmUrl, bgmPath);
            // 実際の映像尺: preview=concat demuxer（重複なし）, final=xfade（TRANS_DUR × N-1 重複）
            const totalDur = rawCuts.reduce((a, c) => a + c.duration, 0)
                - (preview ? 0 : TRANS_DUR * (rawCuts.length - 1));
            const fadeStart = Math.max(0, totalDur - 1.5);
            console.log('[ffmpeg] adding BGM…');
            await runFFmpeg(ffmpeg, [
                '-y', '-loglevel', 'error',
                '-i', concatPath, '-i', bgmPath,
                '-filter_complex',
                `[1:a]atrim=0:${totalDur.toFixed(3)},asetpts=PTS-STARTPTS,afade=t=out:st=${fadeStart.toFixed(3)}:d=1.5,volume=0.65[a]`,
                '-map', '0:v', '-map', '[a]',
                // -shortest は省略: totalDur を正確に計算済みなので映像/音声の長さが一致する
                '-c:v', 'copy', '-c:a', 'aac', '-b:a', '128k',
                '-movflags', '+faststart',
                outputPath,
            ], 90000);
        }
        catch (e) {
            console.warn('[ffmpeg] BGM failed, using video-only:', e.message);
            await fs.copyFile(concatPath, outputPath);
        }
    }
    else {
        await fs.copyFile(concatPath, outputPath);
    }
    // ── 4. Logo overlay ───────────────────────────────────────────────────────
    if (logoUrl && !preview) {
        const logoPath = `${TMP}/${jobId}_logo.png`;
        const logoOutput = `${TMP}/${jobId}_logo_out.mp4`;
        try {
            await downloadFile(logoUrl, logoPath);
            const logoW = Math.round(w * 0.18); // 動画幅の18%
            const pad = Math.round(w * 0.03); // 右下の余白
            console.log('[ffmpeg] adding logo overlay…');
            await runFFmpeg(ffmpeg, [
                '-y', '-loglevel', 'error',
                '-i', outputPath,
                '-i', logoPath,
                '-filter_complex',
                `[1:v]scale=${logoW}:-1:flags=lanczos,format=rgba[logo];` +
                    `[0:v][logo]overlay=x=W-w-${pad}:y=H-h-${pad}:format=auto[vout]`,
                '-map', '[vout]', '-map', '0:a?', // 0:a? = 音声なしでもエラーにしない
                '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '20',
                '-c:a', 'copy',
                '-movflags', '+faststart',
                logoOutput,
            ], 120000);
            await fs.unlink(outputPath).catch(() => { });
            await fs.rename(logoOutput, outputPath);
            console.log('[ffmpeg] logo overlay done');
        }
        catch (e) {
            console.warn('[ffmpeg] logo overlay failed, skipping:', e.message);
            await fs.unlink(logoPath).catch(() => { });
            await fs.unlink(logoOutput).catch(() => { });
        }
        await fs.unlink(logoPath).catch(() => { });
    }
    // ── 5. Cleanup ────────────────────────────────────────────────────────────
    const toDelete = [
        ...clipPaths,
        ...(concatPath !== clipPaths[0] ? [concatPath] : []),
        `${TMP}/${jobId}_bgm.mp3`,
        ...rawCuts.map((_, i) => `${TMP}/${jobId}_img_${i}.jpg`),
    ].filter(p => p !== outputPath);
    await Promise.all(toDelete.map(p => fs.unlink(p).catch(() => { })));
    console.log(`[ffmpeg] done → ${outputPath}`);
    return outputPath;
};
