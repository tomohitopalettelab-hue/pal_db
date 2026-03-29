/**
 * pal-opt-scheduler.ts
 * Cron job that runs every minute to publish scheduled posts.
 */

import cron from 'node-cron';
import { sql } from '@vercel/postgres';
import {
  publishToInstagram,
  publishToGBP,
  publishToX,
  type PalOptPostRow,
  type PalOptSettingsRow,
} from './pal-opt-publisher.js';

/**
 * Process a single scheduled post: publish to all applicable platforms.
 */
const processPost = async (post: PalOptPostRow): Promise<void> => {
  const postId = post.id;
  const paletteId = post.palette_id;

  try {
    // 1. Mark as 'publishing' to prevent double execution
    const { rowCount } = await sql`
      UPDATE pal_opt_posts
      SET status = 'publishing', updated_at = NOW()
      WHERE id = ${postId} AND status = 'scheduled'
    `;

    // If rowCount is 0, another instance already picked it up
    if (!rowCount || rowCount === 0) {
      console.log(`[pal-opt-scheduler] post ${postId} already picked up, skipping`);
      return;
    }

    // 2. Get settings for this palette
    const { rows: settingsRows } = await sql`
      SELECT * FROM pal_opt_settings WHERE palette_id = ${paletteId} LIMIT 1
    `;
    const settings = settingsRows[0] as PalOptSettingsRow | undefined;

    if (!settings) {
      console.error(`[pal-opt-scheduler] no settings for palette_id=${paletteId}, post=${postId}`);
      await sql`
        UPDATE pal_opt_posts
        SET status = 'failed',
            error_log = 'API設定が見つかりません。',
            updated_at = NOW()
        WHERE id = ${postId}
      `;
      return;
    }

    // 3. Determine which platforms to publish
    const alreadyPublished: string[] = Array.isArray(post.published_platforms)
      ? post.published_platforms
      : [];
    const publishedPlatforms = [...alreadyPublished];
    const errors: string[] = [];

    // Instagram
    if (
      !alreadyPublished.includes('instagram') &&
      post.instagram_caption &&
      settings.ig_access_token &&
      settings.ig_business_account_id
    ) {
      const result = await publishToInstagram(post, settings);
      if (result.success) {
        publishedPlatforms.push('instagram');
        if (result.postId) {
          await sql`UPDATE pal_opt_posts SET instagram_post_id = ${result.postId} WHERE id = ${postId}`;
        }
      } else {
        errors.push(`instagram: ${result.error}`);
      }
    }

    // GBP
    if (
      !alreadyPublished.includes('gbp') &&
      post.gbp_summary &&
      settings.gbp_access_token &&
      settings.gbp_location_id
    ) {
      const result = await publishToGBP(post, settings);
      if (result.success) {
        publishedPlatforms.push('gbp');
        if (result.postId) {
          await sql`UPDATE pal_opt_posts SET gbp_post_id = ${result.postId} WHERE id = ${postId}`;
        }
      } else {
        errors.push(`gbp: ${result.error}`);
      }
    }

    // X (Twitter)
    if (
      !alreadyPublished.includes('x') &&
      post.x_text &&
      settings.x_access_token
    ) {
      const result = await publishToX(post, settings);
      if (result.success) {
        publishedPlatforms.push('x');
        if (result.postId) {
          await sql`UPDATE pal_opt_posts SET x_post_id = ${result.postId} WHERE id = ${postId}`;
        }
      } else {
        errors.push(`x: ${result.error}`);
      }
    }

    // 4. Determine final status
    const hasNewPublishes = publishedPlatforms.length > alreadyPublished.length;
    const hasErrors = errors.length > 0;

    let finalStatus: string;
    if (hasErrors && !hasNewPublishes) {
      finalStatus = 'failed';
    } else if (hasErrors) {
      // Some platforms succeeded, some failed
      finalStatus = 'failed';
    } else {
      finalStatus = 'published';
    }

    const errorLog = hasErrors ? errors.join('\n') : null;
    const publishedAt = finalStatus === 'published' ? new Date().toISOString() : post.published_at;
    const platformsJson = JSON.stringify(publishedPlatforms);

    await sql`
      UPDATE pal_opt_posts
      SET status = ${finalStatus},
          published_platforms = ${platformsJson}::jsonb,
          error_log = ${errorLog},
          published_at = ${publishedAt}::timestamptz,
          updated_at = NOW()
      WHERE id = ${postId}
    `;

    console.log(
      `[pal-opt-scheduler] post ${postId}: status=${finalStatus}, platforms=${publishedPlatforms.join(',')}${hasErrors ? ', errors=' + errors.join('; ') : ''}`,
    );
  } catch (error) {
    console.error(`[pal-opt-scheduler] unexpected error processing post ${postId}:`, error);
    try {
      await sql`
        UPDATE pal_opt_posts
        SET status = 'failed',
            error_log = ${error instanceof Error ? error.message : 'unknown scheduler error'},
            updated_at = NOW()
        WHERE id = ${postId}
      `;
    } catch (updateError) {
      console.error(`[pal-opt-scheduler] failed to update post ${postId} after error:`, updateError);
    }
  }
};

/**
 * Main tick: query for due scheduled posts and process them.
 */
const tick = async (): Promise<void> => {
  try {
    const { rows } = await sql`
      SELECT * FROM pal_opt_posts
      WHERE status = 'scheduled' AND scheduled_at <= NOW()
      ORDER BY scheduled_at ASC
      LIMIT 50
    `;

    if (rows.length === 0) return;

    console.log(`[pal-opt-scheduler] found ${rows.length} scheduled post(s) due for publishing`);

    for (const row of rows) {
      await processPost(row as unknown as PalOptPostRow);
    }
  } catch (error) {
    console.error('[pal-opt-scheduler] tick error:', error);
  }
};

/**
 * Start the scheduler cron job (runs every minute).
 */
export const startScheduler = (): void => {
  cron.schedule('* * * * *', () => {
    tick().catch((err) => console.error('[pal-opt-scheduler] unhandled tick error:', err));
  });
  console.log('[pal-opt-scheduler] started (every minute)');
};
