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
 * Run: MYSQL_USER=app_pipe MYSQL_PASSWORD=a33-pipe node scripts/initial-load-campaigns.mjs
 */

import 'dotenv/config';
import mysql from 'mysql2/promise';
import pg    from 'pg';

const log = {
  info:  (msg, meta = '') => console.log(`[INFO]  ${msg}`, meta ? JSON.stringify(meta) : ''),
  error: (msg, meta = '') => console.error(`[ERR]   ${msg}`, meta ? JSON.stringify(meta) : ''),
  warn:  (msg, meta = '') => console.warn(`[WARN]  ${msg}`, meta ? JSON.stringify(meta) : ''),
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

const stats = { contracts: 0, campaigns: 0, lists: 0, campaign_lists: 0, skipped: 0, errors: 0 };

// ─── HELPERS ──────────────────────────────────────────────────────────────────

/** SELECT-then-INSERT by source + source_id. Returns id. */
async function insertIfNew(schema, table, record) {
  const existing = await pgPool.query(
    `SELECT id FROM ${schema}.${table} WHERE source = $1 AND source_id = $2 LIMIT 1`,
    [record.source, record.source_id]
  );
  if (existing.rows.length) return existing.rows[0].id;

  const cols   = Object.keys(record);
  const vals   = Object.values(record);
  const ph     = cols.map((_, i) => `$${i + 1}`);
  const result = await pgPool.query(
    `INSERT INTO ${schema}.${table} (${cols.join(', ')}) VALUES (${ph.join(', ')}) RETURNING id`,
    vals
  );
  return result.rows[0]?.id ?? null;
}

/** SELECT-then-INSERT for junction tables without source/source_id. */
async function insertJunctionIfNew(schema, table, record, checkCols) {
  const whereParts = checkCols.map((c, i) => `${c} = $${i + 1}`).join(' AND ');
  const checkVals  = checkCols.map(c => record[c]);
  const existing   = await pgPool.query(
    `SELECT id FROM ${schema}.${table} WHERE ${whereParts} LIMIT 1`, checkVals
  );
  if (existing.rows.length) return existing.rows[0].id;

  const cols   = Object.keys(record);
  const vals   = Object.values(record);
  const ph     = cols.map((_, i) => `$${i + 1}`);
  const result = await pgPool.query(
    `INSERT INTO ${schema}.${table} (${cols.join(', ')}) VALUES (${ph.join(', ')}) RETURNING id`,
    vals
  );
  return result.rows[0]?.id ?? null;
}

// ─── STEP 1: client_job_orders → crm.contracts ───────────────────────────────

log.info('Step 1: client_job_orders → crm.contracts');

const [jobOrders] = await mysqlPool.query(
  `SELECT client_job_order_id, job_order, contract_type, date_order, x
   FROM client_job_orders WHERE x != 'deleted'`
);
log.info(`Found ${jobOrders.length} job orders`);

const contractTypeMap = {
  'appointment setting': 'prospect', 'lead generation': 'prospect',
  'sales': 'prospect', 'profiling': 'prospect', 'webinar': 'prospect', 'none': 'other',
};
const contractStatusMap = { active: 'active', inactive: 'ended', deleted: 'terminated' };

for (const jo of jobOrders) {
  try {
    await insertIfNew('crm', 'contracts', {
      contract_number: jo.job_order ?? `JO-${jo.client_job_order_id}`,
      contract_type:   contractTypeMap[jo.contract_type] ?? 'other',
      status:          contractStatusMap[jo.x] ?? 'active',
      contract_start:  jo.date_order && jo.date_order !== '0000-00-00' ? jo.date_order : null,
      source:          'pipeline2',
      source_id:       String(jo.client_job_order_id),
    });
    stats.contracts++;
    if (stats.contracts % 500 === 0) log.info(`contracts: ${stats.contracts}`);
  } catch (err) {
    log.error(`contract failed id=${jo.client_job_order_id}`, { err: err.message });
    stats.errors++;
  }
}
log.info(`Step 1 done`, { contracts: stats.contracts, errors: stats.errors });

// ─── STEP 2: client_accounts → crm.campaigns ─────────────────────────────────

log.info('Step 2: client_accounts → crm.campaigns');

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
log.info(`Found ${accounts.length} accounts`);

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

    // Resolve contract_id (nullable in live DB — skip gracefully if not found)
    let contract_id = null;
    if (acc.primary_job_order_id) {
      const contractRes = await pgPool.query(
        `SELECT id FROM crm.contracts WHERE source = 'pipeline2' AND source_id = $1 LIMIT 1`,
        [String(acc.primary_job_order_id)]
      );
      if (contractRes.rows.length) contract_id = contractRes.rows[0].id;
    }

    const record = {
      tenant_id,
      name:       acc.account_number ?? `Account #${acc.client_account_id}`,
      status:     campaignStatusMap[acc.x] ?? 'active',
      source:     'pipeline2',
      source_id:  String(acc.client_account_id),
    };
    if (contract_id)               record.contract_id  = contract_id;
    if (acc.campaign_start_date)   record.started_at   = acc.campaign_start_date;
    if (acc.campaign_end_date)     record.ended_at     = acc.campaign_end_date;

    await insertIfNew('crm', 'campaigns', record);
    stats.campaigns++;
    if (stats.campaigns % 200 === 0) log.info(`campaigns: ${stats.campaigns}`);
  } catch (err) {
    log.error(`campaign failed id=${acc.client_account_id}`, { err: err.message });
    stats.errors++;
  }
}
log.info(`Step 2 done`, { campaigns: stats.campaigns, errors: stats.errors });

// ─── STEP 3: client_lists → crm.lists + crm.campaign_lists ───────────────────

log.info('Step 3: client_lists → crm.lists + crm.campaign_lists');

const [lists] = await mysqlPool.query(
  `SELECT cl.client_list_id, cl.list, cl.dynamic, cl.list_source,
          cl.client_job_order_id, cl.x, cl.date_time_created,
          jo.client_account_id
   FROM client_lists cl
   JOIN client_job_orders jo ON jo.client_job_order_id = cl.client_job_order_id
   WHERE cl.x != 'deleted'`
);
log.info(`Found ${lists.length} lists`);

const listStatusMap = { active: 'active', inactive: 'archived', deleted: 'archived' };

for (const lst of lists) {
  try {
    // Resolve campaign
    const campaignRes = await pgPool.query(
      `SELECT id, tenant_id FROM crm.campaigns WHERE source = 'pipeline2' AND source_id = $1 LIMIT 1`,
      [String(lst.client_account_id)]
    );
    if (!campaignRes.rows.length) { stats.skipped++; continue; }
    const { id: campaign_id, tenant_id } = campaignRes.rows[0];

    // Insert list
    const listRecord = {
      tenant_id,
      name:         lst.list ?? `List #${lst.client_list_id}`,
      list_type:    lst.dynamic === 'yes' ? 'dynamic' : 'static',
      status:       listStatusMap[lst.x] ?? 'active',
      record_count: 0,
      source:       lst.list_source === 'client' ? 'client' : 'pipeline2',
      source_id:    String(lst.client_list_id),
    };
    if (lst.date_time_created) listRecord.created_at = lst.date_time_created;

    const list_id = await insertIfNew('crm', 'lists', listRecord);
    stats.lists++;

    // Insert campaign_lists junction
    if (list_id) {
      await insertJunctionIfNew('crm', 'campaign_lists', {
        campaign_id,
        list_id,
        list_role: 'target',
      }, ['campaign_id', 'list_id']);
      stats.campaign_lists++;
    }

    if (stats.lists % 500 === 0) log.info(`lists: ${stats.lists}`);
  } catch (err) {
    log.error(`list failed id=${lst.client_list_id}`, { err: err.message });
    stats.errors++;
  }
}
log.info(`Step 3 done`, { lists: stats.lists, campaign_lists: stats.campaign_lists, errors: stats.errors });

// ─── SUMMARY ──────────────────────────────────────────────────────────────────

log.info('✅ Campaign chain load complete', stats);
log.info('G3 (crm.contacts) is now unblocked — run initial-load-contacts.mjs next');

await mysqlPool.end();
await pgPool.end();
