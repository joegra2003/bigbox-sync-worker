const { Pool } = require('pg');
const config = require('./config');
const logger = require('./logger');

const pool = new Pool({
  connectionString: config.supabase.connectionString,
  max: 5,
});

pool.on('error', (err) => {
  logger.error('Unexpected pool error', { error: err.message });
});

module.exports = pool;
