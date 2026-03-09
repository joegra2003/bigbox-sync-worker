const pool = require('../db');
const logger = require('../logger');

/**
 * Upsert a record into the target schema.table.
 * Uses ON CONFLICT (source, source_id) for idempotent writes.
 */
async function upsert({ schema, table, record }) {
  const columns = Object.keys(record);
  const values = Object.values(record);
  const placeholders = columns.map((_, i) => `$${i + 1}`);

  const setClauses = columns
    .filter((c) => c !== 'source' && c !== 'source_id')
    .map((c) => `${c} = EXCLUDED.${c}`);
  setClauses.push('updated_at = now()');

  const sql = `
    INSERT INTO ${schema}.${table} (${columns.join(', ')})
    VALUES (${placeholders.join(', ')})
    ON CONFLICT (source, source_id) DO UPDATE SET ${setClauses.join(', ')}
  `;

  logger.debug('Upsert SQL', { schema, table, sql: sql.trim() });

  await pool.query(sql, values);
  logger.info('Upserted', { schema, table, source_id: record.source_id });
}

module.exports = { upsert };
