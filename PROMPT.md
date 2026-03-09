Build a Node.js CDC sync worker for the BigBox project. This reads Maxwell CDC events from Redis and upserts them into Supabase (PostgreSQL).

## Context

**Redis source:**
- Host: on-prem server (configurable via env)
- Key: bigbox:cdc
- Format: BRPOP (blocking pop from left)
- Maxwell event shape: {"database":"callbox_pipeline2","table":"clients","type":"insert","data":{...},"old":{...}}
- type can be: insert | update | delete

**Supabase destination:**
- Connection string (from env): SUPABASE_DB_URL
- Use pg (node-postgres) for direct connection, NOT the Supabase JS client
- Upsert pattern: all tables have source + source_id columns with UNIQUE INDEX for idempotent upserts

**Source MySQL schemas to sync:**
- callbox_pipeline2 (pipeline/contracts)
- callbox_crm (CRM/accounts/users)
- callbox_hubspot_reports (HubSpot data - skip for now, Phase 2)

## File Structure

sync-worker/
  index.js          - entry point, Redis BRPOP loop
  config.js         - env config (Redis host/port/key, Supabase DB URL)
  db.js             - Supabase pg connection pool
  redis.js          - Redis client (ioredis)
  transformers/
    index.js        - dispatcher: routes by database+table to correct transformer
    pipeline2.js    - transformers for callbox_pipeline2 tables
    crm.js          - transformers for callbox_crm tables
    hubspot.js      - transformers for callbox_hubspot_reports (stub, Phase 2)
  sync/
    upsert.js       - generic upsert handler using source+source_id
    delete.js       - soft delete handler (sets deleted_at, does NOT hard delete)
  logger.js         - simple structured logger (no external deps, JSON to stdout)
  errors/
    sync_errors.js  - log failed events to crm.sync_errors table for replay
  migrations/
    001_sync_errors.sql - CREATE TABLE crm.sync_errors

## Key Table Mappings (priority order)

callbox_pipeline2:
- clients -> crm.tenants (source=mysql, source_id=client_id)
- contracts -> crm.contracts
- contract_quotes -> crm.contract_quotes
- contracts_signature -> crm.contract_signatures
- employees -> auth.users + identity.people

callbox_crm:
- accounts -> crm.tenants (status=prospect)
- users -> auth.users + identity.people
- campaigns -> crm.campaigns

callbox_hubspot_reports:
- All tables: skip (log warning, Phase 2)

## Upsert Pattern

INSERT INTO {schema}.{table} (columns)
VALUES (values)
ON CONFLICT (source, source_id) DO UPDATE SET columns = EXCLUDED.columns, updated_at = now()

## Delete Handling

For delete events: set deleted_at = now() (soft delete) — do NOT hard delete.
All BigBox tables have deleted_at timestamptz NULL.

## Error Handling

- On transformer error: log to crm.sync_errors (raw_event jsonb, error text, table_name, retried boolean)
- On DB error: log to crm.sync_errors, continue processing (do not crash)
- Unknown table: log warning, skip event

## crm.sync_errors table

CREATE TABLE IF NOT EXISTS crm.sync_errors (
  id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  raw_event jsonb NOT NULL,
  error_message text,
  table_name varchar,
  retried boolean DEFAULT false,
  created_at timestamptz DEFAULT now()
);

## .env file to create

REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_KEY=bigbox:cdc
SUPABASE_DB_URL=postgresql://postgres:cictac-mobno3-baXvis@db.ynlpbhtztwzdktfyguwq.supabase.co:5432/postgres
LOG_LEVEL=info

## package.json

- name: bigbox-sync-worker
- dependencies: ioredis, pg, dotenv
- scripts: start (node index.js), dev (node --watch index.js)

## README.md

Include setup instructions, env vars, how to start, how to replay sync_errors.

## Important Notes

- Use CommonJS (not ESM) for simplicity
- Keep transformers simple - just map column names, no complex logic
- If a column does not exist in BigBox schema, skip it gracefully
- Log every event processed with: database, table, type, source_id
- The worker should run forever - BRPOP with 5s timeout, loop on empty result
- Column name mapping: MySQL snake_case often matches BigBox snake_case directly; map explicitly where they differ

When completely finished, run this command to notify:
openclaw system event --text "Done: BigBox sync worker built in ~/callbox/apps/bigbox/sync-worker" --mode now
