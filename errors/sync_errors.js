const pool = require('../db');
const logger = require('../logger');

/**
 * Log a failed event to crm.sync_errors for later replay.
 */
async function logSyncError(rawEvent, errorMessage, tableName) {
  try {
    const sql = `
      INSERT INTO crm.sync_errors (raw_event, error_message, table_name)
      VALUES ($1, $2, $3)
    `;
    await pool.query(sql, [JSON.stringify(rawEvent), errorMessage, tableName]);
    logger.info('Logged sync error to crm.sync_errors', { table_name: tableName });
  } catch (err) {
    // Last resort — can't even log the error to DB
    logger.error('Failed to log sync error to DB', {
      error: err.message,
      originalError: errorMessage,
      table_name: tableName,
    });
  }
}

module.exports = { logSyncError };
