/**
 * initial-load-campaigns.mjs  (v2 — batch mode)
 *
 * Backfills the campaign chain into BigBox Supabase:
 *   client_job_orders → crm.contracts
 *   client_accounts   → crm.campaigns
 *   client_lists      → crm.lists + crm.campaign_lists
 *
 * Strategy:
 *   1. Fetch all FK lookup maps from Supabase upfront (4 queries total)
 *   2. Build all rows in memory
 *   3. Batch INSERT 500 rows at a time with ON CONFLICT ... DO UPDATE
 *
 * Requires unique indexes (already applied):
 *   crm.contracts     (source, source_id)
 *   crm.campaigns     (source, source_id)
 *   crm.lists         (source, source_id)
 *   crm.campaign_lists (campaign_id, list_id)
 *
 * Run: MYSQL_USER=app_pipe MYSQL_PASSWORD=a33-pipe node scripts/initial-load-campaigns.mjs
 */

import 'dotenv/config';
import mysql from 'mysql2/promise';
import pg    from 'pg';

const log = {
  info:  (msg, meta) => console.log(`[INFO]  ${msg}`, meta != null ? JSON.stringify(meta) : ''),
  error: (msg, meta) => console.error(`[ERR]   ${msg}`, meta != null ? JSON.stringify(meta) : ''),
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
  let inserted = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const cols  = Object.keys(batch[0]);
    const vals  = [];
    const ph    = batch.map((row, ri) => {
      const rowPh = cols.map((c, ci) => {
        vals.push(row[c] ?? null);
        return `$${ri * cols.length + ci + 1}`;
      });
      return `(${rowPh.join(', ')})`;
    });

    const conflictClause = `(${conflictCols.join(', ')})`;
    const updateClause   = updateCols
      .map(c => `${c} = EXCLUDED.${c}`)
      .concat(['updated_at = now()'])
      .join(', ');

    const sql = `
      INSERT INTO ${schema}.${table} (${cols.join(', ')})
      VALUES ${ph.join(', ')}
      ON CONFLICT ${conflictClause} DO UPDATE SET ${updateClause}
    `;

    await pgPool.query(sql, vals);
    inserted += batch.length;
    log.info(`  ${schema}.${table}: ${inserted}/${rows.length}`);
  }
  return inserted;
}

/** Batch INSERT for junction tables — ON CONFLICT DO NOTHING */
async function batchInsertJunction(schema, table, rows, conflictCols) {
  if (!rows.length) return 0;
  let inserted = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const cols  = Object.keys(batch[0]);
    const vals  = [];
    const ph    = batch.map((row, ri) => {
      const rowPh = cols.map((c, ci) => {
        vals.push(row[c] ?? null);
        return `$${ri * cols.length + ci + 1}`;
      });
      return `(${rowPh.join(', ')})`;
    });

    const conflictClause = `(${conflictCols.join(', ')})`;
    const sql = `
      INSERT INTO ${schema}.${table} (${cols.join(', ')})
      VALUES ${ph.join(', ')}
      ON CONFLICT ${conflictClause} DO NOTHING
    `;
    await pgPool.query(sql, vals);
    inserted += batch.length;
  }
  return inserted;
}

// ─── STEP 0: LOAD LOOKUP MAPS FROM SUPABASE ───────────────────────────────────

log.info('Loading lookup maps from Supabase...');

const [tenantsRes, contractsRes] = await Promise.all([
  // tenants loaded by initial-load-all.mjs used source='mysql'; CDC now uses 'pipeline2'
  pgPool.query(`SELECT source_id, id FROM crm.tenants WHERE source IN ('pipeline2','mysql')`),
  pgPool.query(`SELECT source_id, id FROM crm.contracts WHERE source = 'pipeline2'`),
]);

const tenantMap   = new Map(tenantsRes.rows.map(r => [r.source_id, r.id]));
const contractMap = new Map(contractsRes.rows.map(r => [r.source_id, r.id]));

log.info(`Loaded ${tenantMap.size} tenants, ${contractMap.size} existing contracts`);

// ─── STEP 1: client_job_orders → crm.contracts ───────────────────────────────

log.info('Step 1: client_job_orders → crm.contracts');

const [jobOrders] = await mysqlPool.query(
  `SELECT client_job_order_id, job_order, contract_type, date_order, x
   FROM client_job_orders WHERE x != 'deleted'`
);
log.info(`Found ${jobOrders.length} job orders`);

const contractTypeMap  = {
  'appointment setting': 'prospect', 'lead generation': 'prospect',
  'sales': 'prospect', 'profiling': 'prospect', 'webinar': 'prospect', 'none': 'other',
};
const contractStatusMap = { active: 'active', inactive: 'ended', deleted: 'terminated' };

const contractRows = jobOrders.map(jo => ({
  contract_number: `${jo.job_order || 'JO'}-${jo.client_job_order_id}`,
  contract_type:   contractTypeMap[jo.contract_type] ?? 'other',
  status:          contractStatusMap[jo.x] ?? 'active',
  contract_start:  jo.date_order && jo.date_order !== '0000-00-00' ? jo.date_order : null,
  source:          'pipeline2',
  source_id:       String(jo.client_job_order_id),
}));

const contractsInserted = await batchUpsert(
  'crm', 'contracts', contractRows,
  ['source', 'source_id'],
  ['contract_number', 'contract_type', 'status', 'contract_start']
);
log.info('Step 1 done', { contracts: contractsInserted });

// Reload contract map with freshly inserted rows
const contractsRes2 = await pgPool.query(
  `SELECT source_id, id FROM crm.contracts WHERE source = 'pipeline2'`
);
const contractMapFull = new Map(contractsRes2.rows.map(r => [r.source_id, r.id]));

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
let skippedCampaigns = 0;

const campaignRows = [];
for (const acc of accounts) {
  const tenant_id = tenantMap.get(String(acc.client_id));
  if (!tenant_id) { skippedCampaigns++; continue; }

  const contract_id = acc.primary_job_order_id
    ? contractMapFull.get(String(acc.primary_job_order_id)) ?? null
    : null;

  const row = {
    tenant_id,
    name:      acc.account_number ?? `Account #${acc.client_account_id}`,
    status:    campaignStatusMap[acc.x] ?? 'active',
    source:    'pipeline2',
    source_id: String(acc.client_account_id),
  };
  if (contract_id)             row.contract_id  = contract_id;
  if (acc.campaign_start_date) row.started_at   = acc.campaign_start_date;
  if (acc.campaign_end_date)   row.ended_at     = acc.campaign_end_date;

  campaignRows.push(row);
}

const campaignsInserted = await batchUpsert(
  'crm', 'campaigns', campaignRows,
  ['source', 'source_id'],
  ['name', 'status', 'contract_id', 'started_at', 'ended_at']
);
log.info('Step 2 done', { campaigns: campaignsInserted, skipped: skippedCampaigns });

// Reload campaign map
const campaignsRes = await pgPool.query(
  `SELECT source_id, id, tenant_id FROM crm.campaigns WHERE source = 'pipeline2'`
);
const campaignMap = new Map(campaignsRes.rows.map(r => [r.source_id, { id: r.id, tenant_id: r.tenant_id }]));

// ─── STEP 3: client_lists → crm.lists + crm.campaign_lists ───────────────────

log.info('Step 3: client_lists → crm.lists + crm.campaign_lists');

const [lists] = await mysqlPool.query(
  `SELECT cl.client_list_id, cl.list, cl.dynamic, cl.list_source,
          cl.x, cl.date_time_created, jo.client_account_id
   FROM client_lists cl
   JOIN client_job_orders jo ON jo.client_job_order_id = cl.client_job_order_id
   WHERE cl.x != 'deleted'`
);
log.info(`Found ${lists.length} lists`);

const listStatusMap = { active: 'active', inactive: 'archived', deleted: 'archived' };
let skippedLists = 0;

const listRows = [];
const listCampaignIndex = []; // parallel array: campaign_id per list row

for (const lst of lists) {
  const campaign = campaignMap.get(String(lst.client_account_id));
  if (!campaign) { skippedLists++; continue; }

  const row = {
    tenant_id:    campaign.tenant_id,
    name:         lst.list ?? `List #${lst.client_list_id}`,
    list_type:    lst.dynamic === 'yes' ? 'dynamic' : 'static',
    status:       listStatusMap[lst.x] ?? 'active',
    record_count: 0,
    source:       lst.list_source === 'client' ? 'client' : 'pipeline2',
    source_id:    String(lst.client_list_id),
  };
  const createdAt = lst.date_time_created ? new Date(lst.date_time_created) : null;
  if (createdAt && !isNaN(createdAt.getTime()) && createdAt.getFullYear() > 1970) {
    row.created_at = createdAt.toISOString();
  }

  listRows.push(row);
  listCampaignIndex.push(campaign.id);
}

await batchUpsert(
  'crm', 'lists', listRows,
  ['source', 'source_id'],
  ['name', 'list_type', 'status', 'record_count']
);
log.info(`lists inserted: ${listRows.length}, skipped: ${skippedLists}`);

// Reload list map, build campaign_lists rows
const listsRes = await pgPool.query(
  `SELECT source_id, id FROM crm.lists WHERE source IN ('pipeline2','client')`
);
const listMap = new Map(listsRes.rows.map(r => [r.source_id, r.id]));

const junctionRows = [];
for (let i = 0; i < listRows.length; i++) {
  const list_id    = listMap.get(listRows[i].source_id);
  const campaign_id = listCampaignIndex[i];
  if (list_id && campaign_id) {
    junctionRows.push({ campaign_id, list_id, list_role: 'target' });
  }
}

const junctionsInserted = await batchInsertJunction(
  'crm', 'campaign_lists', junctionRows, ['campaign_id', 'list_id']
);
log.info('Step 3 done', { lists: listRows.length, campaign_lists: junctionsInserted });

// ─── SUMMARY ──────────────────────────────────────────────────────────────────

log.info('✅ Campaign chain load complete', {
  contracts:      contractsInserted,
  campaigns:      campaignsInserted,
  lists:          listRows.length,
  campaign_lists: junctionsInserted,
});
log.info('G3 (crm.contacts) is now unblocked — run initial-load-contacts.mjs next');

await mysqlPool.end();
await pgPool.end();
