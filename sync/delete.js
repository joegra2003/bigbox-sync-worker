const pool = require('../db');
const logger = require('../logger');

/**
 * Soft-delete: sets deleted_at = now() on the matching row.
 */
async function softDelete({ schema, table, source, sourceId }) {
  const sql = `
    UPDATE ${schema}.${table}
    SET deleted_at = now(), updated_at = now()
    WHERE source = $1 AND source_id = $2
  `;

  const result = await pool.query(sql, [source, sourceId]);

  if (result.rowCount === 0) {
    logger.warn('Soft-delete: no matching row found', { schema, table, source, sourceId });
  } else {
    logger.info('Soft-deleted', { schema, table, source_id: sourceId });
  }
}

module.exports = { softDelete };
