import { sql } from '@vercel/postgres';
import { randomBytes, scryptSync, timingSafeEqual } from 'crypto';
let initialized = false;
const PALETTE_ID_REGEX = /^[A-Z][0-9]{4}$/;
const generateId = (prefix) => `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const asString = (value, fallback = '') => {
    if (value === null || value === undefined)
        return fallback;
    return String(value);
};
const normalizePaletteId = (value) => {
    const normalized = asString(value).trim().toUpperCase();
    if (!PALETTE_ID_REGEX.test(normalized)) {
        throw new Error('paletteId must be 1 alphabet letter + 4 digits (例: A0001)');
    }
    return normalized;
};
const normalizeChatLoginId = (value) => {
    const normalized = asString(value).trim().toUpperCase();
    return normalized ? normalized : null;
};
const normalizePasswordInput = (value) => {
    return asString(value).normalize('NFKC').trim();
};
const normalizeServiceKey = (value) => {
    const key = asString(value).trim().toLowerCase();
    if (!key)
        return key;
    const compact = key.replace(/-/g, '_');
    if (compact === 'ai' || compact === 'palette_ai' || compact === 'paletteai' || compact === 'pal_ai') {
        return 'palette_ai';
    }
    if (compact === 'studio' || compact === 'pal_studio' || compact === 'palstudio') {
        return 'pal_studio';
    }
    if (compact === 'trust' || compact === 'pal_trust') {
        return 'pal_trust';
    }
    return compact;
};
const hashPassword = (plainPassword) => {
    const normalized = normalizePasswordInput(plainPassword);
    const salt = randomBytes(16).toString('hex');
    const hash = scryptSync(normalized, salt, 64).toString('hex');
    return `${salt}:${hash}`;
};
const verifyPassword = (plainPassword, storedHash) => {
    const normalized = normalizePasswordInput(plainPassword);
    const [salt, hash] = asString(storedHash).split(':');
    if (!salt || !hash) {
        return normalized === normalizePasswordInput(storedHash);
    }
    const computed = scryptSync(normalized, salt, 64);
    const target = Buffer.from(hash, 'hex');
    if (computed.length !== target.length)
        return false;
    return timingSafeEqual(computed, target);
};
const generateNextPaletteId = async () => {
    const result = await sql `
    SELECT palette_id
    FROM accounts
    WHERE palette_id ~ '^[A-Z][0-9]{4}$'
  `;
    const used = new Set(result.rows
        .map((row) => asString(row.palette_id).toUpperCase())
        .filter((paletteId) => PALETTE_ID_REGEX.test(paletteId)));
    for (let letterCode = 65; letterCode <= 90; letterCode += 1) {
        const prefix = String.fromCharCode(letterCode);
        for (let num = 1; num <= 9999; num += 1) {
            const candidate = `${prefix}${String(num).padStart(4, '0')}`;
            if (!used.has(candidate)) {
                return candidate;
            }
        }
    }
    throw new Error('paletteId の自動採番上限に達しました');
};
const asNullableString = (value) => {
    if (value === null || value === undefined)
        return null;
    const text = String(value).trim();
    return text ? text : null;
};
const asNumber = (value, fallback = 0) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
};
const asBoolean = (value) => {
    return value === true || value === 'true' || value === 1 || value === '1';
};
const toIso = (value, fallback = new Date()) => {
    const date = value ? new Date(String(value)) : fallback;
    if (Number.isNaN(date.getTime()))
        return fallback.toISOString();
    return date.toISOString();
};
const toDateOnly = (value, fallback) => {
    if (!value && fallback)
        return fallback;
    const raw = String(value || '').slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw))
        return raw;
    const date = new Date(String(value || ''));
    if (Number.isNaN(date.getTime()))
        return fallback || new Date().toISOString().slice(0, 10);
    return date.toISOString().slice(0, 10);
};
const normalizeAccount = (row) => ({
    id: asString(row.id),
    paletteId: asString(row.palette_id),
    name: asString(row.name),
    contactEmail: asNullableString(row.contact_email),
    status: asString(row.status, 'active'),
    notes: asNullableString(row.notes),
    chatLoginId: asNullableString(row.chat_login_id),
    chatPassword: asNullableString(row.chat_password_plain),
    chatPasswordSet: Boolean(asNullableString(row.chat_password_hash)),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
});
const normalizePlan = (row) => ({
    id: asString(row.id),
    code: asString(row.code),
    name: asString(row.name),
    billingCycle: asString(row.billing_cycle, 'monthly'),
    defaultPriceYen: asNumber(row.default_price_yen),
    description: asNullableString(row.description),
    isActive: asBoolean(row.is_active),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
});
const normalizeContract = (row) => ({
    id: asString(row.id),
    accountId: asString(row.account_id),
    planId: asString(row.plan_id),
    phase: asString(row.phase, 'active'),
    priceInitial: asNumber(row.price_initial),
    startDate: toDateOnly(row.start_date),
    endDate: row.end_date ? toDateOnly(row.end_date) : null,
    priceYen: asNumber(row.price_yen),
    term: asNullableString(row.term),
    paymentMethod: asNullableString(row.payment_method),
    payDate: asNullableString(row.pay_date),
    dateContract: row.date_contract ? toDateOnly(row.date_contract) : null,
    dateDelivery: row.date_delivery ? toDateOnly(row.date_delivery) : null,
    status: asString(row.status, 'active'),
    memo: asNullableString(row.memo),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
});
const normalizeContractOption = (row) => ({
    id: asString(row.id),
    optionType: asString(row.option_type ?? row.type),
    value: asString(row.value ?? row.key ?? row.code),
    label: asString(row.label),
    sortOrder: asNumber(row.sort_order, 0),
    isActive: asBoolean(row.is_active),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
});
const normalizeServiceSubscription = (row) => ({
    id: asString(row.id),
    accountId: asString(row.account_id),
    serviceKey: asString(row.service_key),
    status: asString(row.status, 'active'),
    startDate: toDateOnly(row.start_date),
    endDate: row.end_date ? toDateOnly(row.end_date) : null,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
});
const normalizeAccountStatusOption = (row) => ({
    id: asString(row.id),
    value: asString(row.value),
    label: asString(row.label),
    sortOrder: asNumber(row.sort_order, 0),
    isActive: asBoolean(row.is_active),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
});
export const ensureTables = async () => {
    if (initialized)
        return;
    await sql `
    CREATE TABLE IF NOT EXISTS accounts (
      id TEXT PRIMARY KEY,
      palette_id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      contact_email TEXT,
      status TEXT NOT NULL DEFAULT 'active',
      notes TEXT,
      chat_login_id TEXT,
      chat_password_hash TEXT,
      chat_password_plain TEXT,
      chat_password_updated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
    await sql `
    ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS chat_login_id TEXT
  `;
    await sql `
    ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS chat_password_hash TEXT
  `;
    await sql `
    ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS chat_password_plain TEXT
  `;
    await sql `
    ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS chat_password_updated_at TIMESTAMPTZ
  `;
    await sql `
    CREATE TABLE IF NOT EXISTS service_plans (
      id TEXT PRIMARY KEY,
      code TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      billing_cycle TEXT NOT NULL DEFAULT 'monthly',
      default_price_yen INTEGER NOT NULL DEFAULT 0,
      description TEXT,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
    await sql `
    CREATE TABLE IF NOT EXISTS contracts (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      plan_id TEXT NOT NULL REFERENCES service_plans(id),
      phase TEXT NOT NULL DEFAULT 'active',
      price_initial INTEGER NOT NULL DEFAULT 0,
      start_date DATE NOT NULL,
      end_date DATE,
      price_yen INTEGER NOT NULL,
      term TEXT,
      payment_method TEXT,
      pay_date TEXT,
      date_contract DATE,
      date_delivery DATE,
      status TEXT NOT NULL DEFAULT 'active',
      memo TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT 'active'
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS price_initial INTEGER NOT NULL DEFAULT 0
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS term TEXT
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS payment_method TEXT
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS pay_date TEXT
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS date_contract DATE
  `;
    await sql `
    ALTER TABLE contracts
    ADD COLUMN IF NOT EXISTS date_delivery DATE
  `;
    await sql `
    CREATE TABLE IF NOT EXISTS contract_options (
      id TEXT PRIMARY KEY,
      option_type TEXT NOT NULL,
      value TEXT NOT NULL,
      label TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(option_type, value)
    )
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS option_type TEXT
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS type TEXT
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS value TEXT
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS "key" TEXT
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS code TEXT
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS label TEXT
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `;
    await sql `
    ALTER TABLE contract_options
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `;
    await sql `
    CREATE UNIQUE INDEX IF NOT EXISTS contract_options_type_value_idx
    ON contract_options (option_type, value)
  `;
    await sql `
    CREATE UNIQUE INDEX IF NOT EXISTS contract_options_legacy_type_key_idx
    ON contract_options (type, "key")
  `;
    await sql `
    UPDATE contract_options
    SET
      option_type = COALESCE(option_type, type),
      value = COALESCE(value, "key", code),
      type = COALESCE(type, option_type),
      "key" = COALESCE("key", value, code),
      code = COALESCE(code, value, "key")
  `;
    await sql `
    CREATE TABLE IF NOT EXISTS service_subscriptions (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      service_key TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      start_date DATE NOT NULL,
      end_date DATE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(account_id, service_key, start_date)
    )
  `;
    await sql `
    CREATE TABLE IF NOT EXISTS account_status_options (
      id TEXT PRIMARY KEY,
      value TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
    await sql `CREATE INDEX IF NOT EXISTS accounts_palette_id_idx ON accounts (palette_id)`;
    await sql `CREATE UNIQUE INDEX IF NOT EXISTS accounts_chat_login_id_uidx ON accounts (chat_login_id) WHERE chat_login_id IS NOT NULL`;
    await sql `CREATE INDEX IF NOT EXISTS contracts_account_id_idx ON contracts (account_id)`;
    await sql `CREATE INDEX IF NOT EXISTS contracts_status_idx ON contracts (status)`;
    await sql `CREATE INDEX IF NOT EXISTS contracts_phase_idx ON contracts (phase)`;
    await sql `CREATE INDEX IF NOT EXISTS service_subscriptions_account_id_idx ON service_subscriptions (account_id)`;
    await sql `CREATE INDEX IF NOT EXISTS service_subscriptions_service_key_idx ON service_subscriptions (service_key)`;
    await sql `CREATE INDEX IF NOT EXISTS contract_options_type_active_idx ON contract_options (option_type, is_active, sort_order, updated_at)`;
    await sql `CREATE INDEX IF NOT EXISTS account_status_options_active_idx ON account_status_options (is_active, sort_order, updated_at)`;
    await sql `
    WITH normalized AS (
      SELECT
        ctid,
        account_id,
        start_date,
        updated_at,
        CASE
          WHEN LOWER(REPLACE(service_key, '-', '_')) IN ('ai', 'palette_ai', 'paletteai', 'pal_ai') THEN 'palette_ai'
          WHEN LOWER(REPLACE(service_key, '-', '_')) IN ('studio', 'pal_studio', 'palstudio') THEN 'pal_studio'
          WHEN LOWER(REPLACE(service_key, '-', '_')) IN ('trust', 'pal_trust') THEN 'pal_trust'
          ELSE LOWER(REPLACE(service_key, '-', '_'))
        END AS normalized_key
      FROM service_subscriptions
    ),
    duplicates AS (
      SELECT
        ctid,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, start_date, normalized_key
          ORDER BY updated_at DESC, ctid DESC
        ) AS rn
      FROM normalized
    )
    DELETE FROM service_subscriptions src
    USING duplicates d
    WHERE src.ctid = d.ctid AND d.rn > 1
  `;
    await sql `
    UPDATE service_subscriptions
    SET service_key = CASE
      WHEN LOWER(REPLACE(service_key, '-', '_')) IN ('ai', 'palette_ai', 'paletteai', 'pal_ai') THEN 'palette_ai'
      WHEN LOWER(REPLACE(service_key, '-', '_')) IN ('studio', 'pal_studio', 'palstudio') THEN 'pal_studio'
      WHEN LOWER(REPLACE(service_key, '-', '_')) IN ('trust', 'pal_trust') THEN 'pal_trust'
      ELSE LOWER(REPLACE(service_key, '-', '_'))
    END
  `;
    await sql `
    INSERT INTO contract_options (id, option_type, value, type, code, "key", label, sort_order)
    VALUES
      ('opt-phase-lead', 'phase', 'lead', 'phase', 'lead', 'lead', 'リード', 10),
      ('opt-phase-negotiating', 'phase', 'negotiating', 'phase', 'negotiating', 'negotiating', '商談中', 20),
      ('opt-phase-active', 'phase', 'active', 'phase', 'active', 'active', '運用中', 30),
      ('opt-phase-closed', 'phase', 'closed', 'phase', 'closed', 'closed', '終了', 40),
      ('opt-status-active', 'status', 'active', 'status', 'active', 'active', '稼働中', 10),
      ('opt-status-suspended', 'status', 'suspended', 'status', 'suspended', 'suspended', '停止中', 20),
      ('opt-status-expired', 'status', 'expired', 'status', 'expired', 'expired', '満了', 30)
    ON CONFLICT (option_type, value) DO NOTHING
  `;
    await sql `
    UPDATE contract_options
    SET label = CASE
      WHEN COALESCE(option_type, type) = 'phase' AND COALESCE(value, "key", code) = 'lead' THEN 'リード'
      WHEN COALESCE(option_type, type) = 'phase' AND COALESCE(value, "key", code) = 'negotiating' THEN '商談中'
      WHEN COALESCE(option_type, type) = 'phase' AND COALESCE(value, "key", code) = 'active' THEN '運用中'
      WHEN COALESCE(option_type, type) = 'phase' AND COALESCE(value, "key", code) = 'closed' THEN '終了'
      WHEN COALESCE(option_type, type) = 'status' AND COALESCE(value, "key", code) = 'active' THEN '稼働中'
      WHEN COALESCE(option_type, type) = 'status' AND COALESCE(value, "key", code) = 'suspended' THEN '停止中'
      WHEN COALESCE(option_type, type) = 'status' AND COALESCE(value, "key", code) = 'expired' THEN '満了'
      ELSE label
    END
    WHERE COALESCE(option_type, type) IN ('phase', 'status')
  `;
    await sql `
    INSERT INTO account_status_options (id, value, label, sort_order)
    VALUES
      ('acc-status-active', 'active', '稼働中', 10),
      ('acc-status-inactive', 'inactive', '停止中', 20)
    ON CONFLICT (value) DO NOTHING
  `;
    initialized = true;
};
export const listAccounts = async () => {
    await ensureTables();
    const result = await sql `
    SELECT id, palette_id, name, contact_email, status, notes, chat_login_id, chat_password_hash, chat_password_plain, created_at, updated_at
    FROM accounts
    ORDER BY updated_at DESC
  `;
    return result.rows.map((row) => normalizeAccount(row));
};
export const upsertAccount = async (input) => {
    await ensureTables();
    let id = asString(input.id, generateId('acc'));
    const paletteIdInput = asString(input.paletteId).trim();
    let paletteId = '';
    if (paletteIdInput) {
        paletteId = normalizePaletteId(paletteIdInput);
    }
    else if (asString(input.id).trim()) {
        const existingById = await sql `
      SELECT palette_id
      FROM accounts
      WHERE id = ${id}
      LIMIT 1
    `;
        const existingPaletteId = asString(existingById.rows?.[0]?.palette_id).trim();
        paletteId = existingPaletteId ? normalizePaletteId(existingPaletteId) : await generateNextPaletteId();
    }
    else {
        paletteId = await generateNextPaletteId();
    }
    const existing = await sql `SELECT id FROM accounts WHERE palette_id = ${paletteId} LIMIT 1`;
    const matchedId = existing.rows?.[0]?.id;
    if (matchedId)
        id = asString(matchedId);
    const existingAccountRes = await sql `
    SELECT chat_login_id, chat_password_hash, chat_password_plain, chat_password_updated_at
    FROM accounts
    WHERE id = ${id}
    LIMIT 1
  `;
    const existingAccount = (existingAccountRes.rows?.[0] || {});
    const name = asString(input.name, '新規顧客');
    const contactEmail = asNullableString(input.contactEmail);
    const status = asString(input.status, 'active');
    const notes = asNullableString(input.notes);
    const hasChatLoginId = Object.prototype.hasOwnProperty.call(input, 'chatLoginId');
    const hasChatPassword = Object.prototype.hasOwnProperty.call(input, 'chatPassword');
    const chatLoginId = hasChatLoginId
        ? normalizeChatLoginId(input.chatLoginId)
        : asNullableString(existingAccount.chat_login_id);
    const nextPasswordInput = hasChatPassword ? normalizePasswordInput(input.chatPassword) : '';
    const chatPasswordHash = nextPasswordInput
        ? hashPassword(nextPasswordInput)
        : asNullableString(existingAccount.chat_password_hash);
    const chatPasswordPlain = nextPasswordInput
        ? nextPasswordInput
        : asNullableString(existingAccount.chat_password_plain);
    const chatPasswordUpdatedAt = nextPasswordInput
        ? toIso(new Date())
        : (existingAccount.chat_password_updated_at ? toIso(existingAccount.chat_password_updated_at) : null);
    const updatedAt = toIso(input.updatedAt || new Date());
    const result = await sql `
    INSERT INTO accounts (id, palette_id, name, contact_email, status, notes, chat_login_id, chat_password_hash, chat_password_plain, chat_password_updated_at, updated_at)
    VALUES (${id}, ${paletteId}, ${name}, ${contactEmail}, ${status}, ${notes}, ${chatLoginId}, ${chatPasswordHash}, ${chatPasswordPlain}, ${chatPasswordUpdatedAt}, ${updatedAt}::timestamptz)
    ON CONFLICT (id)
    DO UPDATE SET
      palette_id = EXCLUDED.palette_id,
      name = EXCLUDED.name,
      contact_email = EXCLUDED.contact_email,
      status = EXCLUDED.status,
      notes = EXCLUDED.notes,
      chat_login_id = EXCLUDED.chat_login_id,
      chat_password_hash = EXCLUDED.chat_password_hash,
      chat_password_plain = EXCLUDED.chat_password_plain,
      chat_password_updated_at = EXCLUDED.chat_password_updated_at,
      updated_at = EXCLUDED.updated_at
    RETURNING id, palette_id, name, contact_email, status, notes, chat_login_id, chat_password_hash, chat_password_plain, created_at, updated_at
  `;
    return normalizeAccount(result.rows[0]);
};
export const listPlans = async (includeInactive = false) => {
    await ensureTables();
    const result = includeInactive
        ? await sql `SELECT id, code, name, billing_cycle, default_price_yen, description, is_active, created_at, updated_at FROM service_plans ORDER BY is_active DESC, updated_at DESC`
        : await sql `SELECT id, code, name, billing_cycle, default_price_yen, description, is_active, created_at, updated_at FROM service_plans WHERE is_active = TRUE ORDER BY updated_at DESC`;
    return result.rows.map((row) => normalizePlan(row));
};
export const upsertPlan = async (input) => {
    await ensureTables();
    const id = asString(input.id, generateId('plan'));
    const code = asString(input.code).trim() || id;
    const name = asString(input.name, '新規プラン');
    const billingCycle = asString(input.billingCycle, 'monthly');
    const defaultPriceYen = asNumber(input.defaultPriceYen, 0);
    const description = asNullableString(input.description);
    const isActive = input.isActive ?? true;
    const updatedAt = toIso(input.updatedAt || new Date());
    const result = await sql `
    INSERT INTO service_plans (id, code, name, billing_cycle, default_price_yen, description, is_active, updated_at)
    VALUES (${id}, ${code}, ${name}, ${billingCycle}, ${defaultPriceYen}, ${description}, ${Boolean(isActive)}, ${updatedAt}::timestamptz)
    ON CONFLICT (id)
    DO UPDATE SET
      code = EXCLUDED.code,
      name = EXCLUDED.name,
      billing_cycle = EXCLUDED.billing_cycle,
      default_price_yen = EXCLUDED.default_price_yen,
      description = EXCLUDED.description,
      is_active = EXCLUDED.is_active,
      updated_at = EXCLUDED.updated_at
    RETURNING id, code, name, billing_cycle, default_price_yen, description, is_active, created_at, updated_at
  `;
    return normalizePlan(result.rows[0]);
};
export const listContracts = async (options) => {
    await ensureTables();
    const accountId = options?.accountId?.trim();
    const paletteId = options?.paletteId?.trim();
    const activeOn = options?.activeOn?.trim();
    if (accountId) {
        const result = activeOn
            ? await sql `
          SELECT
            id,
            account_id,
            plan_id,
            phase,
            price_initial,
            start_date,
            end_date,
            price_yen,
            term,
            payment_method,
            pay_date,
            date_contract,
            date_delivery,
            status,
            memo,
            created_at,
            updated_at
          FROM contracts
          WHERE account_id = ${accountId}
            AND start_date <= ${activeOn}::date
            AND (end_date IS NULL OR end_date >= ${activeOn}::date)
          ORDER BY start_date DESC, updated_at DESC
        `
            : await sql `
          SELECT
            id,
            account_id,
            plan_id,
            phase,
            price_initial,
            start_date,
            end_date,
            price_yen,
            term,
            payment_method,
            pay_date,
            date_contract,
            date_delivery,
            status,
            memo,
            created_at,
            updated_at
          FROM contracts
          WHERE account_id = ${accountId}
          ORDER BY start_date DESC, updated_at DESC
        `;
        return result.rows.map((row) => normalizeContract(row));
    }
    if (paletteId) {
        const result = activeOn
            ? await sql `
          SELECT
            c.id,
            c.account_id,
            c.plan_id,
            c.phase,
            c.price_initial,
            c.start_date,
            c.end_date,
            c.price_yen,
            c.term,
            c.payment_method,
            c.pay_date,
            c.date_contract,
            c.date_delivery,
            c.status,
            c.memo,
            c.created_at,
            c.updated_at
          FROM contracts c
          INNER JOIN accounts a ON a.id = c.account_id
          WHERE a.palette_id = ${paletteId}
            AND c.start_date <= ${activeOn}::date
            AND (c.end_date IS NULL OR c.end_date >= ${activeOn}::date)
          ORDER BY c.start_date DESC, c.updated_at DESC
        `
            : await sql `
          SELECT
            c.id,
            c.account_id,
            c.plan_id,
            c.phase,
            c.price_initial,
            c.start_date,
            c.end_date,
            c.price_yen,
            c.term,
            c.payment_method,
            c.pay_date,
            c.date_contract,
            c.date_delivery,
            c.status,
            c.memo,
            c.created_at,
            c.updated_at
          FROM contracts c
          INNER JOIN accounts a ON a.id = c.account_id
          WHERE a.palette_id = ${paletteId}
          ORDER BY c.start_date DESC, c.updated_at DESC
        `;
        return result.rows.map((row) => normalizeContract(row));
    }
    const result = await sql `
    SELECT
      id,
      account_id,
      plan_id,
      phase,
      price_initial,
      start_date,
      end_date,
      price_yen,
      term,
      payment_method,
      pay_date,
      date_contract,
      date_delivery,
      status,
      memo,
      created_at,
      updated_at
    FROM contracts
    ORDER BY start_date DESC, updated_at DESC
  `;
    return result.rows.map((row) => normalizeContract(row));
};
export const upsertContract = async (input) => {
    await ensureTables();
    const id = asString(input.id, generateId('ctr'));
    const accountId = asString(input.accountId).trim();
    const planId = asString(input.planId).trim();
    if (!accountId)
        throw new Error('accountId is required');
    if (!planId)
        throw new Error('planId is required');
    const phase = asString(input.phase, 'active');
    const priceInitial = asNumber(input.priceInitial, 0);
    const startDate = toDateOnly(input.startDate, new Date().toISOString().slice(0, 10));
    const endDate = input.endDate ? toDateOnly(input.endDate) : null;
    const priceYen = asNumber(input.priceYen, 0);
    const term = asNullableString(input.term);
    const paymentMethod = asNullableString(input.paymentMethod);
    const payDate = asNullableString(input.payDate);
    const dateContract = input.dateContract ? toDateOnly(input.dateContract) : null;
    const dateDelivery = input.dateDelivery ? toDateOnly(input.dateDelivery) : null;
    const status = asString(input.status, 'active');
    const memo = asNullableString(input.memo);
    const updatedAt = toIso(input.updatedAt || new Date());
    const result = await sql `
    INSERT INTO contracts (
      id,
      account_id,
      plan_id,
      phase,
      price_initial,
      start_date,
      end_date,
      price_yen,
      term,
      payment_method,
      pay_date,
      date_contract,
      date_delivery,
      status,
      memo,
      updated_at
    )
    VALUES (
      ${id},
      ${accountId},
      ${planId},
      ${phase},
      ${priceInitial},
      ${startDate}::date,
      ${endDate},
      ${priceYen},
      ${term},
      ${paymentMethod},
      ${payDate},
      ${dateContract},
      ${dateDelivery},
      ${status},
      ${memo},
      ${updatedAt}::timestamptz
    )
    ON CONFLICT (id)
    DO UPDATE SET
      account_id = EXCLUDED.account_id,
      plan_id = EXCLUDED.plan_id,
      phase = EXCLUDED.phase,
      price_initial = EXCLUDED.price_initial,
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date,
      price_yen = EXCLUDED.price_yen,
      term = EXCLUDED.term,
      payment_method = EXCLUDED.payment_method,
      pay_date = EXCLUDED.pay_date,
      date_contract = EXCLUDED.date_contract,
      date_delivery = EXCLUDED.date_delivery,
      status = EXCLUDED.status,
      memo = EXCLUDED.memo,
      updated_at = EXCLUDED.updated_at
    RETURNING
      id,
      account_id,
      plan_id,
      phase,
      price_initial,
      start_date,
      end_date,
      price_yen,
      term,
      payment_method,
      pay_date,
      date_contract,
      date_delivery,
      status,
      memo,
      created_at,
      updated_at
  `;
    return normalizeContract(result.rows[0]);
};
export const listServiceSubscriptions = async (options) => {
    await ensureTables();
    const accountId = options?.accountId?.trim();
    const paletteId = options?.paletteId?.trim();
    const activeOn = options?.activeOn?.trim();
    if (accountId) {
        const result = activeOn
            ? await sql `
          SELECT id, account_id, service_key, status, start_date, end_date, created_at, updated_at
          FROM service_subscriptions
          WHERE account_id = ${accountId}
            AND start_date <= ${activeOn}::date
            AND (end_date IS NULL OR end_date >= ${activeOn}::date)
          ORDER BY start_date DESC, updated_at DESC
        `
            : await sql `
          SELECT id, account_id, service_key, status, start_date, end_date, created_at, updated_at
          FROM service_subscriptions
          WHERE account_id = ${accountId}
          ORDER BY start_date DESC, updated_at DESC
        `;
        return result.rows.map((row) => normalizeServiceSubscription(row));
    }
    if (paletteId) {
        const result = activeOn
            ? await sql `
          SELECT s.id, s.account_id, s.service_key, s.status, s.start_date, s.end_date, s.created_at, s.updated_at
          FROM service_subscriptions s
          INNER JOIN accounts a ON a.id = s.account_id
          WHERE a.palette_id = ${paletteId}
            AND s.start_date <= ${activeOn}::date
            AND (s.end_date IS NULL OR s.end_date >= ${activeOn}::date)
          ORDER BY s.start_date DESC, s.updated_at DESC
        `
            : await sql `
          SELECT s.id, s.account_id, s.service_key, s.status, s.start_date, s.end_date, s.created_at, s.updated_at
          FROM service_subscriptions s
          INNER JOIN accounts a ON a.id = s.account_id
          WHERE a.palette_id = ${paletteId}
          ORDER BY s.start_date DESC, s.updated_at DESC
        `;
        return result.rows.map((row) => normalizeServiceSubscription(row));
    }
    const result = await sql `
    SELECT id, account_id, service_key, status, start_date, end_date, created_at, updated_at
    FROM service_subscriptions
    ORDER BY start_date DESC, updated_at DESC
  `;
    return result.rows.map((row) => normalizeServiceSubscription(row));
};
export const upsertServiceSubscription = async (input) => {
    await ensureTables();
    const id = asString(input.id, generateId('svc'));
    const accountId = asString(input.accountId).trim();
    const serviceKey = normalizeServiceKey(input.serviceKey);
    if (!accountId)
        throw new Error('accountId is required');
    if (!serviceKey)
        throw new Error('serviceKey is required');
    const status = asString(input.status, 'active');
    const startDate = toDateOnly(input.startDate, new Date().toISOString().slice(0, 10));
    const endDate = input.endDate ? toDateOnly(input.endDate) : null;
    const updatedAt = toIso(input.updatedAt || new Date());
    const result = await sql `
    INSERT INTO service_subscriptions (id, account_id, service_key, status, start_date, end_date, updated_at)
    VALUES (${id}, ${accountId}, ${serviceKey}, ${status}, ${startDate}::date, ${endDate}, ${updatedAt}::timestamptz)
    ON CONFLICT (id)
    DO UPDATE SET
      account_id = EXCLUDED.account_id,
      service_key = EXCLUDED.service_key,
      status = EXCLUDED.status,
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date,
      updated_at = EXCLUDED.updated_at
    RETURNING id, account_id, service_key, status, start_date, end_date, created_at, updated_at
  `;
    return normalizeServiceSubscription(result.rows[0]);
};
export const getPaletteSummary = async (paletteId, activeOn) => {
    await ensureTables();
    const accountRes = await sql `
    SELECT id, palette_id, name, contact_email, status, notes, created_at, updated_at
    FROM accounts
    WHERE palette_id = ${paletteId}
    LIMIT 1
  `;
    if (!accountRes.rows.length)
        return null;
    const account = normalizeAccount(accountRes.rows[0]);
    const contracts = await listContracts({ accountId: account.id, activeOn });
    const subscriptions = await listServiceSubscriptions({ accountId: account.id, activeOn });
    const planIds = Array.from(new Set(contracts.map((contract) => contract.planId)));
    const allPlans = await listPlans(true);
    const plans = allPlans.filter((plan) => planIds.includes(plan.id));
    return {
        account,
        contracts,
        plans,
        services: subscriptions,
    };
};
export const getPaletteServices = async (paletteId, activeOn) => {
    const summary = await getPaletteSummary(paletteId, activeOn);
    if (!summary)
        return null;
    const serviceKeys = Array.from(new Set(summary.services.map((item) => item.serviceKey)));
    return {
        paletteId,
        account: summary.account,
        serviceKeys,
        services: summary.services,
    };
};
export const deleteAccount = async (id) => {
    await ensureTables();
    await sql `DELETE FROM accounts WHERE id = ${id}`;
};
export const deletePlan = async (id) => {
    await ensureTables();
    await sql `DELETE FROM service_plans WHERE id = ${id}`;
};
export const deleteContract = async (id) => {
    await ensureTables();
    await sql `DELETE FROM contracts WHERE id = ${id}`;
};
export const deleteServiceSubscription = async (id) => {
    await ensureTables();
    await sql `DELETE FROM service_subscriptions WHERE id = ${id}`;
};
export const listContractOptions = async (optionType, includeInactive = false) => {
    await ensureTables();
    const result = includeInactive
        ? await sql `
        SELECT
          id,
          COALESCE(option_type, type) AS option_type,
          COALESCE(value, "key", code) AS value,
          label,
          sort_order,
          is_active,
          created_at,
          updated_at
        FROM contract_options
        WHERE COALESCE(option_type, type) = ${optionType}
        ORDER BY sort_order ASC, updated_at DESC
      `
        : await sql `
        SELECT
          id,
          COALESCE(option_type, type) AS option_type,
          COALESCE(value, "key", code) AS value,
          label,
          sort_order,
          is_active,
          created_at,
          updated_at
        FROM contract_options
        WHERE COALESCE(option_type, type) = ${optionType} AND is_active = TRUE
        ORDER BY sort_order ASC, updated_at DESC
      `;
    return result.rows.map((row) => normalizeContractOption(row));
};
export const upsertContractOption = async (input) => {
    await ensureTables();
    const optionType = asString(input.optionType);
    if (optionType !== 'phase' && optionType !== 'status') {
        throw new Error('optionType must be phase or status');
    }
    const value = asString(input.value).trim();
    const label = asString(input.label).trim();
    if (!value || !label) {
        throw new Error('value and label are required');
    }
    const existing = await sql `
    SELECT id
    FROM contract_options
    WHERE COALESCE(option_type, type) = ${optionType}
      AND COALESCE(value, "key", code) = ${value}
    LIMIT 1
  `;
    const id = asString(existing.rows?.[0]?.id, asString(input.id, generateId('opt')));
    const sortOrder = asNumber(input.sortOrder, 0);
    const isActive = input.isActive ?? true;
    const updatedAt = toIso(input.updatedAt || new Date());
    const result = await sql `
    INSERT INTO contract_options (id, option_type, value, type, code, "key", label, sort_order, is_active, updated_at)
    VALUES (${id}, ${optionType}, ${value}, ${optionType}, ${value}, ${value}, ${label}, ${sortOrder}, ${Boolean(isActive)}, ${updatedAt}::timestamptz)
    ON CONFLICT (id)
    DO UPDATE SET
      option_type = EXCLUDED.option_type,
      value = EXCLUDED.value,
      type = EXCLUDED.type,
      code = EXCLUDED.code,
      "key" = EXCLUDED."key",
      label = EXCLUDED.label,
      sort_order = EXCLUDED.sort_order,
      is_active = EXCLUDED.is_active,
      updated_at = EXCLUDED.updated_at
    RETURNING
      id,
      COALESCE(option_type, type) AS option_type,
      COALESCE(value, "key", code) AS value,
      label,
      sort_order,
      is_active,
      created_at,
      updated_at
  `;
    return normalizeContractOption(result.rows[0]);
};
export const deleteContractOption = async (id) => {
    await ensureTables();
    await sql `DELETE FROM contract_options WHERE id = ${id}`;
};
export const listAccountStatusOptions = async (includeInactive = false) => {
    await ensureTables();
    const result = includeInactive
        ? await sql `
        SELECT id, value, label, sort_order, is_active, created_at, updated_at
        FROM account_status_options
        ORDER BY sort_order ASC, updated_at DESC
      `
        : await sql `
        SELECT id, value, label, sort_order, is_active, created_at, updated_at
        FROM account_status_options
        WHERE is_active = TRUE
        ORDER BY sort_order ASC, updated_at DESC
      `;
    return result.rows.map((row) => normalizeAccountStatusOption(row));
};
export const upsertAccountStatusOption = async (input) => {
    await ensureTables();
    const value = asString(input.value).trim();
    const label = asString(input.label).trim();
    if (!value || !label)
        throw new Error('value and label are required');
    const existing = await sql `SELECT id FROM account_status_options WHERE value = ${value} LIMIT 1`;
    const id = asString(input.id, asString(existing.rows?.[0]?.id, generateId('acc-status')));
    const sortOrder = asNumber(input.sortOrder, 0);
    const isActive = input.isActive ?? true;
    const updatedAt = toIso(input.updatedAt || new Date());
    const result = await sql `
    INSERT INTO account_status_options (id, value, label, sort_order, is_active, updated_at)
    VALUES (${id}, ${value}, ${label}, ${sortOrder}, ${Boolean(isActive)}, ${updatedAt}::timestamptz)
    ON CONFLICT (id)
    DO UPDATE SET
      value = EXCLUDED.value,
      label = EXCLUDED.label,
      sort_order = EXCLUDED.sort_order,
      is_active = EXCLUDED.is_active,
      updated_at = EXCLUDED.updated_at
    RETURNING id, value, label, sort_order, is_active, created_at, updated_at
  `;
    return normalizeAccountStatusOption(result.rows[0]);
};
export const deleteAccountStatusOption = async (id) => {
    await ensureTables();
    await sql `DELETE FROM account_status_options WHERE id = ${id}`;
};
export const hasChatLoginId = async (loginIdInput) => {
    await ensureTables();
    const loginId = normalizeChatLoginId(loginIdInput);
    if (!loginId)
        return false;
    const result = await sql `
    SELECT 1
    FROM accounts
    WHERE chat_login_id = ${loginId} OR palette_id = ${loginId}
    LIMIT 1
  `;
    return result.rows.length > 0;
};
export const verifyChatLogin = async (loginIdInput, passwordInput) => {
    await ensureTables();
    const loginId = normalizeChatLoginId(loginIdInput);
    const password = normalizePasswordInput(passwordInput);
    if (!loginId || !password)
        return { success: false };
    const result = await sql `
    SELECT id, palette_id, name, chat_password_hash
    FROM accounts
    WHERE chat_login_id = ${loginId} OR palette_id = ${loginId}
    LIMIT 1
  `;
    if (!result.rows.length)
        return { success: false };
    const row = (result.rows[0] || {});
    const storedHash = asNullableString(row.chat_password_hash);
    if (!storedHash || !verifyPassword(password, storedHash)) {
        return { success: false };
    }
    return {
        success: true,
        accountId: asString(row.id),
        paletteId: asString(row.palette_id),
        accountName: asString(row.name, '顧客名未設定'),
    };
};
