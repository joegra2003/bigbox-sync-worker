# BigBox Sync Worker

CDC sync worker that reads Maxwell change-data-capture events from Redis and upserts them into Supabase (PostgreSQL).

## Setup

```bash
npm install
cp .env.example .env   # or create .env with the vars below
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis server host | `127.0.0.1` |
| `REDIS_PORT` | Redis server port | `6379` |
| `REDIS_KEY` | Redis list key for CDC events | `bigbox:cdc` |
| `SUPABASE_DB_URL` | PostgreSQL connection string | — |
| `LOG_LEVEL` | Log level: debug, info, warn, error | `info` |

## Running

```bash
# Production
npm start

# Development (auto-restart on changes)
npm run dev
```

## Migration

Run the sync_errors table migration before first start:

```bash
psql "$SUPABASE_DB_URL" -f migrations/001_sync_errors.sql
```

## How It Works

1. Worker BRPOP's from `bigbox:cdc` Redis list (5s timeout, loops forever)
2. Parses Maxwell CDC event JSON
3. Routes event by `database` + `table` to the correct transformer
4. Transformer maps MySQL columns → BigBox schema columns
5. Upserts into Supabase using `ON CONFLICT (source, source_id)`
6. Delete events set `deleted_at = now()` (soft delete)
7. Errors are logged to `crm.sync_errors` for replay

## Replaying Failed Events

Query failed events:

```sql
SELECT * FROM crm.sync_errors WHERE retried = false ORDER BY created_at;
```

To replay, push the `raw_event` back into Redis:

```bash
# From psql or a script:
# For each failed event, LPUSH the raw_event JSON back to bigbox:cdc
```

Mark as retried after replay:

```sql
UPDATE crm.sync_errors SET retried = true WHERE id = '<uuid>';
```

## Supported Tables

### callbox_pipeline2
- `clients` → `crm.tenants`
- `contracts` → `crm.contracts`
- `contract_quotes` → `crm.contract_quotes`
- `contracts_signature` → `crm.contract_signatures`
- `employees` → `auth.users` + `identity.people`

### callbox_crm
- `accounts` → `crm.tenants` (status=prospect)
- `users` → `auth.users` + `identity.people`
- `campaigns` → `crm.campaigns`

### callbox_hubspot_reports
- All tables: skipped (Phase 2)
