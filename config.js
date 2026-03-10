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
  mysql: {
    host:     process.env.MYSQL_HOST     || '127.0.0.1',
    port:     parseInt(process.env.MYSQL_PORT, 10) || 3306,
    user:     process.env.MYSQL_USER     || 'maxwell',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'callbox_pipeline2',
  },
  logLevel: process.env.LOG_LEVEL || 'info',
};
