/**
 * Stub for callbox_hubspot_reports — Phase 2.
 * All tables are skipped with a warning.
 */

const logger = require('../logger');

function skip(table, data) {
  logger.warn('HubSpot sync skipped (Phase 2)', { database: 'callbox_hubspot_reports', table });
  return null;
}

module.exports = { skip };
