const Redis = require('ioredis');
const config = require('./config');
const logger = require('./logger');

const redis = new Redis({
  host: config.redis.host,
  port: config.redis.port,
  maxRetriesPerRequest: null,
  retryStrategy(times) {
    const delay = Math.min(times * 500, 5000);
    logger.warn('Redis reconnecting', { attempt: times, delay });
    return delay;
  },
});

redis.on('connect', () => logger.info('Redis connected'));
redis.on('error', (err) => logger.error('Redis error', { error: err.message }));

module.exports = redis;
