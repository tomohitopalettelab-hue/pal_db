# pal-db

Palette System 共通の顧客/契約DBサービス。

## 目的

- サービス非依存で顧客情報を管理
- 各サービス（palette_ai など）へ契約情報を配信
- `paletteId` を軸に利用可能サービスを返却

## 責務

- `pal-db`: 顧客管理のみ（取引先/プラン/契約/サービス購読）
- `pal_studio`: HP生成などの制作管理
- `palette_ai`: 生成機能（管理UIは持たない）

## セットアップ

```bash
cd pal-db
npm install
cp .env.example .env
npm run dev
```

デフォルト起動ポート: `3100`

## 管理UI

- `http://localhost:3100/admin`（顧客一覧 + 新規顧客作成）
- `http://localhost:3100/admin/customers/:id`（顧客詳細）
- 顧客詳細で契約・サービス購読を管理

### 接続文字列エラーが出たとき

`missing_connection_string` が出る場合は、`pal-db/.env` にDB接続文字列を設定してください。

```bash
POSTGRES_URL=postgres://USER:PASSWORD@HOST:5432/DB
```

`POSTGRES_URL` が無い場合、以下も自動参照します。

- `DATABASE_URL`
- `POSTGRES_PRISMA_URL`
- `POSTGRES_URL_NON_POOLING`

`palette_ai/.env` に `POSTGRES_URL` が既にある場合は、同じ値を `pal-db/.env` にコピーすればOKです。

## 主要API

- `GET /health`
- `GET /admin`
- `GET /api/accounts`
- `POST /api/accounts`
- `DELETE /api/accounts/:id`
- `GET /api/plans`
- `POST /api/plans`
- `DELETE /api/plans/:id`
- `GET /api/contracts`
- `POST /api/contracts`
- `DELETE /api/contracts/:id`
- `GET /api/palette-summary?paletteId=<id>&activeOn=YYYY-MM-DD`
- `GET /api/palette-services?paletteId=<id>&activeOn=YYYY-MM-DD`
- `GET /api/service-subscriptions?paletteId=<id>`
- `POST /api/service-subscriptions`
- `DELETE /api/service-subscriptions/:id`
