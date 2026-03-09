const http = require('node:http');
const config = require('./config');
const logger = require('./logger');
const { transform } = require('./transformers');
const { upsert } = require('./sync/upsert');
const { softDelete } = require('./sync/delete');
const { logSyncError } = require('./errors/sync_errors');

const PORT = process.env.PORT || 3001;

async function processEvent(event) {
  const { database, table, type, data } = event;
  const sourceId = data?.id ? String(data.id) : 'unknown';

  logger.info('Processing event', { database, table, type, source_id: sourceId });

  const targets = transform(event);
  if (!targets) return; // skipped (unknown table or Phase 2)

  for (const target of targets) {
    if (type === 'delete') {
      await softDelete({
        schema: target.schema,
        table: target.table,
        source: 'mysql',
        sourceId,
      });
    } else {
      await upsert(target);
    }
  }
}

const server = http.createServer(async (req, res) => {
  if (req.method !== 'POST') {
    res.writeHead(405);
    return res.end('Method Not Allowed');
  }

  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', async () => {
    let event;
    try {
      event = JSON.parse(body);
    } catch (err) {
      logger.warn('Invalid JSON received', { body });
      res.writeHead(400);
      return res.end('Bad Request');
    }

    try {
      await processEvent(event);
      res.writeHead(200);
      res.end('OK');
    } catch (err) {
      logger.error('Failed to process event', { error: err.message, event });
      await logSyncError({
        rawEvent: event,
        errorMessage: err.message,
        tableName: event?.table,
      }).catch(() => {});
      res.writeHead(500);
      res.end('Internal Server Error');
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  logger.info(`BigBox sync worker listening on port ${PORT}`);
});
