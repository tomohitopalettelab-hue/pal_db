/**
 * pal-opt-publisher.ts
 * Scheduled post publishing logic for Instagram, GBP, and X (Twitter).
 * Called by pal-opt-scheduler.ts cron job.
 */

// ---------- Types (snake_case matching DB columns) ----------

export type PalOptPostRow = {
  id: string;
  palette_id: string;
  title: string;
  topic: string;
  keywords: string[];
  target_audience: string | null;
  image_urls: string[];
  status: string;
  instagram_caption: string | null;
  instagram_image_url: string | null;
  instagram_post_id: string | null;
  blog_title: string | null;
  blog_body_html: string | null;
  blog_slug: string | null;
  blog_post_id: string | null;
  gbp_summary: string | null;
  gbp_call_to_action: string | null;
  gbp_post_id: string | null;
  x_text: string | null;
  x_post_id: string | null;
  published_platforms: string[];
  error_log: string | null;
  approved_at: string | null;
  published_at: string | null;
  scheduled_at: string | null;
  template_id: string | null;
  plan_id: string | null;
  created_at: string;
  updated_at: string;
};

export type PalOptSettingsRow = {
  id: string;
  palette_id: string;
  ig_access_token: string | null;
  ig_business_account_id: string | null;
  gbp_access_token: string | null;
  gbp_refresh_token: string | null;
  gbp_location_id: string | null;
  blog_url: string | null;
  blog_wp_username: string | null;
  blog_api_key: string | null;
  target_keywords: string[];
  goals: string | null;
  default_tone: string;
  has_pal_studio: boolean;
  has_pal_trust: boolean;
  x_access_token: string | null;
  x_refresh_token: string | null;
  notification_type: string | null;
  notification_email: string | null;
  line_user_id: string | null;
  created_at: string;
  updated_at: string;
};

export type PublishResult = {
  success: boolean;
  postId?: string;
  error?: string;
};

// ---------- Instagram (Meta Graph API) ----------

export const publishToInstagram = async (
  post: PalOptPostRow,
  settings: PalOptSettingsRow,
): Promise<PublishResult> => {
  try {
    const token = settings.ig_access_token;
    const accountId = settings.ig_business_account_id;

    if (!token || !accountId) {
      return { success: false, error: 'Instagram API設定が不足しています。' };
    }
    if (!post.instagram_caption) {
      return { success: false, error: 'Instagram投稿文がありません。' };
    }
    if (!post.instagram_image_url) {
      return { success: false, error: 'Instagramへの投稿には画像が必要です。' };
    }

    // Step 1: Create media container
    const containerParams = new URLSearchParams({
      access_token: token,
      caption: post.instagram_caption,
      image_url: post.instagram_image_url,
    });

    const containerRes = await fetch(
      `https://graph.facebook.com/v19.0/${accountId}/media`,
      { method: 'POST', body: containerParams },
    );
    const containerBody = await containerRes.json();

    if (!containerRes.ok || !containerBody?.id) {
      return {
        success: false,
        error: `メディアコンテナ作成失敗: ${containerBody?.error?.message || 'unknown'}`,
      };
    }

    // Step 2: Publish container
    const publishParams = new URLSearchParams({
      access_token: token,
      creation_id: containerBody.id,
    });

    const publishRes = await fetch(
      `https://graph.facebook.com/v19.0/${accountId}/media_publish`,
      { method: 'POST', body: publishParams },
    );
    const publishBody = await publishRes.json();

    if (!publishRes.ok || !publishBody?.id) {
      return {
        success: false,
        error: `Instagram投稿失敗: ${publishBody?.error?.message || 'unknown'}`,
      };
    }

    return { success: true, postId: String(publishBody.id) };
  } catch (error: unknown) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Instagram投稿エラー',
    };
  }
};

// ---------- Google Business Profile ----------

export const publishToGBP = async (
  post: PalOptPostRow,
  settings: PalOptSettingsRow,
): Promise<PublishResult> => {
  try {
    const token = settings.gbp_access_token;
    const locationId = settings.gbp_location_id;

    if (!token || !locationId) {
      return { success: false, error: 'GBP API設定が不足しています。' };
    }
    if (!post.gbp_summary) {
      return { success: false, error: 'GBP投稿文がありません。' };
    }

    const gbpApiUrl = `https://mybusiness.googleapis.com/v4/${locationId}/localPosts`;

    const ctaTypes: Record<string, string> = {
      'ウェブサイトを見る': 'LEARN_MORE',
      '電話する': 'CALL',
      '予約する': 'BOOK',
      '詳細を見る': 'LEARN_MORE',
    };
    const ctaActionType = ctaTypes[post.gbp_call_to_action || ''] || 'LEARN_MORE';

    const gbpBody: Record<string, unknown> = {
      languageCode: 'ja',
      summary: post.gbp_summary,
      callToAction: { actionType: ctaActionType },
      topicType: 'STANDARD',
    };

    if (post.instagram_image_url) {
      gbpBody.media = [{ mediaFormat: 'PHOTO', sourceUrl: post.instagram_image_url }];
    }

    const gbpRes = await fetch(gbpApiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(gbpBody),
    });

    const gbpResBody = await gbpRes.json();

    if (!gbpRes.ok || !gbpResBody?.name) {
      return {
        success: false,
        error: `GBP投稿失敗: ${gbpResBody?.error?.message || 'unknown'}`,
      };
    }

    return { success: true, postId: String(gbpResBody.name) };
  } catch (error: unknown) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'GBP投稿エラー',
    };
  }
};

// ---------- X (Twitter) ----------

export const publishToX = async (
  post: PalOptPostRow,
  settings: PalOptSettingsRow,
): Promise<PublishResult> => {
  try {
    const token = settings.x_access_token;

    if (!token) {
      return { success: false, error: 'X (Twitter) API設定が不足しています。' };
    }
    if (!post.x_text) {
      return { success: false, error: 'X投稿文がありません。' };
    }

    const xRes = await fetch('https://api.twitter.com/2/tweets', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ text: post.x_text }),
    });

    const xBody = await xRes.json();

    if (!xRes.ok || !xBody?.data?.id) {
      return {
        success: false,
        error: `X投稿失敗: ${xBody?.detail || xBody?.title || 'unknown'}`,
      };
    }

    return { success: true, postId: String(xBody.data.id) };
  } catch (error: unknown) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'X投稿エラー',
    };
  }
};
