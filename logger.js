const config = require('./config');

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const currentLevel = LEVELS[config.logLevel] ?? LEVELS.info;

function log(level, message, data = {}) {
  if (LEVELS[level] < currentLevel) return;
  const entry = {
    ts: new Date().toISOString(),
    level,
    msg: message,
    ...data,
  };
  process.stdout.write(JSON.stringify(entry) + '\n');
}

module.exports = {
  debug: (msg, data) => log('debug', msg, data),
  info: (msg, data) => log('info', msg, data),
  warn: (msg, data) => log('warn', msg, data),
  error: (msg, data) => log('error', msg, data),
};
