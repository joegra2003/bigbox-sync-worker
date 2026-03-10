/**
 * initial-load-campaigns.mjs
 *
 * Backfills the campaign chain into BigBox Supabase:
 *   client_job_orders → crm.contracts
 *   client_accounts   → crm.campaigns
 *   client_lists      → crm.lists + crm.campaign_lists
 *
 * Must run AFTER initial-load-all.mjs (tenants must exist).
 * Must complete BEFORE initial-load-contacts.mjs (contacts need campaign_list_id).
 *
 * Run: node scripts/initial-load-campaigns.mjs
 */

import 'dotenv/config';
import mysql from 'mysql2/promise';
import pg    from 'pg';

const logger = {
  info:  (...a) => console.log('[INFO]',  ...a),
  error: (...a) => console.error('[ERR]',  ...a),
  warn:  (...a) => console.warn('[WARN]',  ...a),
};

// ─── DB CONNECTIONS ───────────────────────────────────────────────────────────

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

// ─── HELPERS ──────────────────────────────────────────────────────────────────

const stats = { contracts: 0, campaigns: 0, lists: 0, campaign_lists: 0, skipped: 0, errors: 0 };

async function upsert(schema, table, record, conflictKey = '(source, source_id)') {
  const cols  = Object.keys(record);
  const vals  = Object.values(record);
  const ph    = cols.map((_, i) => `$${i + 1}`);
  const sets  = cols
    .filter(c => c !== 'source' && c !== 'source_id')
    .map(c => `${c} = EXCLUDED.${c}`);
  sets.push('updated_at = now()');

  const sql = `
    INSERT INTO ${schema}.${table} (${cols.join(', ')})
    VALUES (${ph.join(', ')})
    ON CONFLICT ${conflictKey} DO UPDATE SET ${sets.join(', ')}
    RETURNING id
  `;
  const result = await pgPool.query(sql, vals);
  return result.rows[0]?.id ?? null;
}

/**
 * SELECT-then-INSERT for tables without a usable unique constraint.
 * Looks up by source + source_id; inserts only if not found.
 * Returns the existing or newly created id.
 */
async function insertIfNew(schema, table, record) {
  // Check if already exists
  const existing = await pgPool.query(
    `SELECT id FROM ${schema}.${table} WHERE source = $1 AND source_id = $2 LIMIT 1`,
    [record.source, record.source_id]
  );
  if (existing.rows.length) return existing.rows[0].id;

  // Insert
  const cols = Object.keys(record);
  const vals = Object.values(record);
  const ph   = cols.map((_, i) => `$${i + 1}`);
  const sql  = `INSERT INTO ${schema}.${table} (${cols.join(', ')}) VALUES (${ph.join(', ')}) RETURNING id`;
  const result = await pgPool.query(sql, vals);
  return result.rows[0]?.id ?? null;
}

/**
 * For junction tables (no source/source_id) — check by natural key before inserting.
 */
async function insertJunctionIfNew(schema, table, record, checkCols) {
  const checks = checkCols.map((c, i) => `${c} = $${i + 1}`).join(' AND ');
  const vals   = checkCols.map(c => record[c]);
  const existing = await pgPool.query(
    `SELECT id FROM ${schema}.${table} WHERE ${checks} LIMIT 1`, vals
  );
  if (existing.rows.length) return existing.rows[0].id;

  const cols   = Object.keys(record);
  const allVals = Object.values(record);
  const ph     = cols.map((_, i) => `$${i + 1}`);
  const sql    = `INSERT INTO ${schema}.${table} (${cols.join(', ')}) VALUES (${ph.join(', ')}) RETURNING id`;
  const result = await pgPool.query(sql, allVals);
  return result.rows[0]?.id ?? null;
}

// ─── STEP 1: client_job_orders → crm.contracts ───────────────────────────────

logger.info('Step 1: loading client_job_orders → crm.contracts');

const [jobOrders] = await mysqlPool.query(
  `SELECT client_job_order_id, job_order, contract_type, date_order, x
   FROM client_job_orders
   WHERE x != 'deleted'`
);

const contractTypeMap = {
  'appointment setting': 'prospect',
  'lead generation':     'prospect',
  'sales':               'prospect',
  'profiling':           'prospect',
  'webinar':             'prospect',
  'none':                'other',
};
const contractStatusMap = { active: 'active', inactive: 'ended', deleted: 'terminated' };

for (const jo of jobOrders) {
  try {
    await upsert('crm', 'contracts', {
      contract_number: jo.job_order ?? `JO-${jo.client_job_order_id}`,
      contract_type:   contractTypeMap[jo.contract_type] ?? 'other',
      status:          contractStatusMap[jo.x] ?? 'active',
      contract_start:  jo.date_order && jo.date_order !== '0000-00-00' ? jo.date_order : null,
      source:          'pipeline2',
      source_id:       String(jo.client_job_order_id),
    }, '(contract_number)');
    stats.contracts++;
  } catch (err) {
    logger.error({ err: err.message, id: jo.client_job_order_id }, 'contract upsert failed');
    stats.errors++;
  }
}
logger.info({ count: stats.contracts }, 'contracts done');

// ─── STEP 2: client_accounts → crm.campaigns ─────────────────────────────────

logger.info('Step 2: loading client_accounts → crm.campaigns');

const [accounts] = await mysqlPool.query(
  `SELECT ca.client_account_id, ca.client_id, ca.account_number,
          ca.campaign_start_date, ca.campaign_end_date, ca.x,
          MIN(jo.client_job_order_id) AS primary_job_order_id
   FROM client_accounts ca
   LEFT JOIN client_job_orders jo
     ON jo.client_account_id = ca.client_account_id AND jo.x != 'deleted'
   WHERE ca.x != 'deleted'
   GROUP BY ca.client_account_id`
);

const campaignStatusMap = { active: 'active', inactive: 'ended', onhold: 'on_hold', deleted: 'suspended' };

for (const acc of accounts) {
  try {
    // Resolve tenant_id
    const tenantRes = await pgPool.query(
      `SELECT id FROM crm.tenants WHERE source = 'pipeline2' AND source_id = $1 LIMIT 1`,
      [String(acc.client_id)]
    );
    if (!tenantRes.rows.length) { stats.skipped++; continue; }
    const tenant_id = tenantRes.rows[0].id;

    // Resolve contract_id from primary job order
    if (!acc.primary_job_order_id) { stats.skipped++; continue; }
    const contractRes = await pgPool.query(
      `SELECT id FROM crm.contracts WHERE source = 'pipeline2' AND source_id = $1 LIMIT 1`,
      [String(acc.primary_job_order_id)]
    );
    if (!contractRes.rows.length) { stats.skipped++; continue; }
    const contract_id = contractRes.rows[0].id;

    await insertIfNew('crm', 'campaigns', {
      tenant_id,
      contract_id,
      name:       acc.account_number ?? `Account #${acc.client_account_id}`,
      status:     campaignStatusMap[acc.x] ?? 'active',
      started_at: acc.campaign_start_date ?? null,
      ended_at:   acc.campaign_end_date   ?? null,
      source:     'pipeline2',
      source_id:  String(acc.client_account_id),
    });
    stats.campaigns++;
  } catch (err) {
    logger.error({ err: err.message, id: acc.client_account_id }, 'campaign upsert failed');
    stats.errors++;
  }
}
logger.info({ count: stats.campaigns }, 'campaigns done');

// ─── STEP 3: client_lists → crm.lists + crm.campaign_lists ───────────────────

logger.info('Step 3: loading client_lists → crm.lists + crm.campaign_lists');

const [lists] = await mysqlPool.query(
  `SELECT cl.client_list_id, cl.list, cl.dynamic, cl.list_source,
          cl.client_job_order_id, cl.x, cl.date_time_created,
          jo.client_account_id
   FROM client_lists cl
   JOIN client_job_orders jo ON jo.client_job_order_id = cl.client_job_order_id
   WHERE cl.x != 'deleted'`
);

const listStatusMap = { active: 'active', inactive: 'archived', deleted: 'archived' };

for (const lst of lists) {
  try {
    // Resolve campaign_id
    const campaignRes = await pgPool.query(
      `SELECT id, tenant_id FROM crm.campaigns
       WHERE source = 'pipeline2' AND source_id = $1 LIMIT 1`,
      [String(lst.client_account_id)]
    );
    if (!campaignRes.rows.length) { stats.skipped++; continue; }
    const { id: campaign_id, tenant_id } = campaignRes.rows[0];

    // Insert list (SELECT-then-INSERT — no unique constraint on source/source_id)
    const list_id = await insertIfNew('crm', 'lists', {
      tenant_id,
      name:      lst.list ?? `List #${lst.client_list_id}`,
      list_type: lst.dynamic === 'yes' ? 'dynamic' : 'static',
      status:    listStatusMap[lst.x] ?? 'active',
      source:    lst.list_source === 'client' ? 'client' : 'pipeline2',
      source_id: String(lst.client_list_id),
      ...(lst.date_time_created ? { created_at: lst.date_time_created } : {}),
    });
    stats.lists++;

    // Insert campaign_lists junction (check campaign_id + list_id)
    if (list_id) {
      await insertJunctionIfNew('crm', 'campaign_lists', {
        campaign_id,
        list_id,
        list_role: 'target',
      }, ['campaign_id', 'list_id']);
      stats.campaign_lists++;
    }
  } catch (err) {
    logger.error({ err: err.message, id: lst.client_list_id }, 'list upsert failed');
    stats.errors++;
  }
}
logger.info({ count: stats.lists, junctions: stats.campaign_lists }, 'lists done');

// ─── SUMMARY ──────────────────────────────────────────────────────────────────

logger.info({ stats }, '✅ Campaign chain load complete');
logger.info('G3 (crm.contacts) is now unblocked — run initial-load-contacts.mjs next');

await mysqlPool.end();
await pgPool.end();
