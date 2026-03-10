const pipeline2 = require('./pipeline2');
const crm       = require('./crm');
const hubspot   = require('./hubspot');
const logger    = require('../logger');

const ROUTES = {
  callbox_pipeline2: {
    // sync
    clients:              pipeline2.clients,
    contracts:            pipeline2.contracts,
    contract_quotes:      pipeline2.contract_quotes,
    contracts_signature:  pipeline2.contracts_signature,
    employees:            pipeline2.employees,
    // async — campaign chain
    client_job_orders:    pipeline2.client_job_orders,
    client_accounts:      pipeline2.client_accounts,
    client_lists:         pipeline2.client_lists,
    // async — G3, G5
    client_list_details:  pipeline2.client_list_details,
    events_tm_ob_txn:     pipeline2.events_tm_ob_txn,
  },
  callbox_crm: {
    accounts: crm.accounts,
    users:    crm.users,
    campaigns: crm.campaigns,
  },
};

/**
 * Transform a Maxwell CDC event into one or more upsert targets.
 *
 * Sync transformers: return { schema, table, record } or array thereof.
 * Async transformers: return Promise (receive { pg, mysql } as second arg).
 *
 * Returns array of targets, or null if skipped.
 */
async function transform(event, { pg, mysql } = {}) {
  const { database, table, data } = event;

  if (database === 'callbox_hubspot_reports') {
    hubspot.skip(table, data);
    return null;
  }

  const dbRoutes = ROUTES[database];
  if (!dbRoutes) {
    logger.warn('Unknown database, skipping', { database, table });
    return null;
  }

  const fn = dbRoutes[table];
  if (!fn) {
    logger.warn('Unknown table, skipping', { database, table });
    return null;
  }

  // Async transformers receive db connections; sync transformers ignore the arg
  const result = await fn(data, { pg, mysql });
  if (!result) return null;

  return Array.isArray(result) ? result : [result];
}

module.exports = { transform };
