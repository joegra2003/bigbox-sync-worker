const mysql = require('mysql2/promise');
const config = require('./config');
const logger = require('./logger');

const pool = mysql.createPool({
  host:     config.mysql.host,
  port:     config.mysql.port,
  user:     config.mysql.user,
  password: config.mysql.password,
  database: config.mysql.database,
  waitForConnections: true,
  connectionLimit: 5,
  queueLimit: 0,
});

pool.on('connection', () => {
  logger.debug('MySQL pool: new connection');
});

module.exports = pool;
