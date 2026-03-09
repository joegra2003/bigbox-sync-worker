const pipeline2 = require('./pipeline2');
const crm = require('./crm');
const hubspot = require('./hubspot');
const logger = require('../logger');

const ROUTES = {
  callbox_pipeline2: {
    clients: pipeline2.clients,
    contracts: pipeline2.contracts,
    contract_quotes: pipeline2.contract_quotes,
    contracts_signature: pipeline2.contracts_signature,
    employees: pipeline2.employees,
  },
  callbox_crm: {
    accounts: crm.accounts,
    users: crm.users,
    campaigns: crm.campaigns,
  },
};

/**
 * Transform a Maxwell CDC event into one or more upsert targets.
 * Returns an array of { schema, table, record } or null if skipped.
 */
function transform(event) {
  const { database, table, data } = event;

  // Phase 2 — skip HubSpot
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

  const result = fn(data);
  // Normalise to array (some transformers return multiple targets)
  return Array.isArray(result) ? result : [result];
}

module.exports = { transform };
