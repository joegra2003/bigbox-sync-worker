/**
 * initial-load-contacts.mjs
 *
 * Backfills crm.contacts from callbox_pipeline2.client_list_details.
 *
 * Source chain:
 *   client_list_details → crm.contacts
 *     target_detail_id  → identity.people (person_id)
 *     client_list_id    → crm.lists → crm.campaign_lists (campaign_list_id)
 *     event_tm_ob_txn_id → event_state_lkp.funnel_stage (lead_status)
 *     x                 → active / archived
 *
 * Strategy:
 *   1. Load all FK maps upfront (people, lists, campaign_lists)
 *   2. Batch INSERT 500 rows at a time with ON CONFLICT (source, source_id)
 *
 * Run: MYSQL_USER=app_pipe MYSQL_PASSWORD=a33-pipe node scripts/initial-load-contacts.mjs
 */

import 'dotenv/config';
import mysql from 'mysql2/promise';
import pg    from 'pg';

const log = {
  info:  (msg, meta) => console.log(`[INFO]  ${msg}`, meta != null ? JSON.stringify(meta) : ''),
  error: (msg, meta) => console.error(`[ERR]   ${msg}`, meta != null ? JSON.stringify(meta) : ''),
  warn:  (msg, meta) => console.warn(`[WARN]  ${msg}`, meta != null ? JSON.stringify(meta) : ''),
};

const BATCH = 500;

// ─── CONNECTIONS ──────────────────────────────────────────────────────────────

const mysqlPool = await mysql.createPool({
  host:     process.env.MYSQL_HOST     || '127.0.0.1',
  port:     parseInt(process.env.MYSQL_PORT, 10) || 3306,
  user:     process.env.MYSQL_USER     || 'app_pipe',
  password: process.env.MYSQL_PASSWORD || 'a33-pipe',
  database: 'callbox_pipeline2',
  waitForConnections: true,
  connectionLimit: 5,
});

const pgPool = new pg.Pool({ connectionString: process.env.SUPABASE_DB_URL });

// ─── BATCH UPSERT ─────────────────────────────────────────────────────────────

async function batchUpsert(schema, table, rows, conflictCols, updateCols) {
  if (!rows.length) return 0;
  let total = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const cols  = Object.keys(batch[0]);
    const vals  = [];
    const ph    = batch.map((row, ri) => {
      return `(${cols.map((c, ci) => {
        vals.push(row[c] ?? null);
        return `$${ri * cols.length + ci + 1}`;
      }).join(', ')})`;
    });

    const updateClause = updateCols
      .map(c => `${c} = EXCLUDED.${c}`)
      .concat(['updated_at = now()'])
      .join(', ');

    const sql = `
      INSERT INTO ${schema}.${table} (${cols.join(', ')})
      VALUES ${ph.join(', ')}
      ON CONFLICT (${conflictCols.join(', ')}) DO UPDATE SET ${updateClause}
    `;
    await pgPool.query(sql, vals);
    total += batch.length;
    if (total % 5000 === 0 || total === rows.length) {
      log.info(`  ${schema}.${table}: ${total}/${rows.length}`);
    }
  }
  return total;
}

// ─── STEP 0: LOAD LOOKUP MAPS ────────────────────────────────────────────────

log.info('Loading lookup maps from Supabase...');

// Ensure unique index on crm.contacts (source, source_id)
await pgPool.query(`
  CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_source
  ON crm.contacts (source, source_id)
`).catch(() => {}); // ignore if already exists

const [peopleRes, campaignListsRes] = await Promise.all([
  pgPool.query(`SELECT source_id, id FROM identity.people WHERE source = 'pipeline2'`),
  pgPool.query(`
    SELECT l.source_id AS list_source_id, cl.id AS campaign_list_id
    FROM crm.campaign_lists cl
    JOIN crm.lists l ON l.id = cl.list_id
    WHERE l.source IN ('pipeline2', 'client')
  `),
]);

// target_detail_id → identity.people.id
const peopleMap        = new Map(peopleRes.rows.map(r => [r.source_id, r.id]));
// client_list_id → crm.campaign_lists.id
const campaignListMap  = new Map(campaignListsRes.rows.map(r => [r.list_source_id, r.campaign_list_id]));

log.info(`Loaded ${peopleMap.size} people, ${campaignListMap.size} campaign lists`);

// ─── STEP 1: LOAD FUNNEL STAGE MAP FROM MYSQL ────────────────────────────────

// Funnel stage map — try multiple DB locations, default to empty map if unavailable
log.info('Loading event_state_lkp funnel stages from MySQL...');

let funnelMap = new Map();
const funnelDbs = ['callbox_pipeline2', 'callbox_misc'];

for (const db of funnelDbs) {
  try {
    const [funnelRows] = await mysqlPool.query(`
      SELECT txn.event_tm_ob_txn_id, esl.funnel_stage
      FROM ${db}.events_tm_ob_txn txn
      JOIN ${db}.events_tm_ob_lkp lkp
        ON lkp.event_tm_ob_lkp_id = txn.event_tm_ob_lkp_id
      JOIN ${db}.event_state_lkp esl
        ON esl.event_state_lkp_id = lkp.event_state_lkp_id
      WHERE esl.funnel_stage IS NOT NULL AND esl.funnel_stage != ''
    `);
    funnelMap = new Map(
      funnelRows.map(r => [String(r.event_tm_ob_txn_id), r.funnel_stage.toLowerCase()])
    );
    log.info(`Loaded ${funnelMap.size} funnel stage mappings from ${db}`);
    break;
  } catch (err) {
    log.warn(`event_state_lkp not found in ${db}, trying next...`);
  }
}

if (!funnelMap.size) {
  log.warn('No funnel stage map loaded — all contacts will default to identification');
}

// ─── STEP 2: LOAD + TRANSFORM client_list_details ───────────────────────────

log.info('Loading client_list_details from MySQL...');

const [details] = await mysqlPool.query(`
  SELECT client_list_detail_id, client_list_id, target_detail_id,
         event_tm_ob_txn_id, x, timestamp
  FROM callbox_pipeline2.client_list_details
  WHERE x != 'deleted'
`);
log.info(`Found ${details.length} client_list_details rows`);

const contactRows = [];
let skipped = 0;

for (const d of details) {
  const person_id        = peopleMap.get(String(d.target_detail_id));
  const campaign_list_id = campaignListMap.get(String(d.client_list_id));

  if (!person_id || !campaign_list_id) {
    skipped++;
    continue;
  }

  const lead_status = (d.event_tm_ob_txn_id
    ? funnelMap.get(String(d.event_tm_ob_txn_id))
    : null) ?? 'identification';

  const status = d.x === 'inactive' ? 'archived' : 'active';

  contactRows.push({
    person_id,
    campaign_list_id,
    lead_status,
    status,
    source:    'pipeline2',
    source_id: String(d.client_list_detail_id),
  });
}

log.info(`Built ${contactRows.length} contact rows (skipped: ${skipped})`);

// ─── STEP 3: BATCH UPSERT INTO crm.contacts ──────────────────────────────────

log.info('Upserting into crm.contacts...');

const inserted = await batchUpsert(
  'crm', 'contacts', contactRows,
  ['source', 'source_id'],
  ['person_id', 'campaign_list_id', 'lead_status', 'status']
);

// ─── SUMMARY ─────────────────────────────────────────────────────────────────

log.info('✅ Contacts load complete', {
  inserted,
  skipped,
  total: details.length,
});

await mysqlPool.end();
await pgPool.end();
