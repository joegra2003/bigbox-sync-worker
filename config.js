require('dotenv').config();

module.exports = {
  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    key: process.env.REDIS_KEY || 'bigbox:cdc',
  },
  supabase: {
    connectionString: process.env.SUPABASE_DB_URL,
  },
  logLevel: process.env.LOG_LEVEL || 'info',
};
