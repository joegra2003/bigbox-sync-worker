const http    = require('node:http');
const config  = require('./config');
const logger  = require('./logger');
const pg      = require('./db');
const mysql   = require('./mysql');
const { transform }    = require('./transformers');
const { upsert }       = require('./sync/upsert');
const { softDelete }   = require('./sync/delete');
const { logSyncError } = require('./errors/sync_errors');

const PORT = process.env.PORT || 3001;

async function processEvent(event) {
  const { database, table, type, data } = event;
  const sourceId = data?.id
    ?? data?.client_list_detail_id
    ?? data?.event_tm_ob_txn_id
    ?? 'unknown';

  logger.info('Processing event', { database, table, type, source_id: String(sourceId) });

  const targets = await transform(event, { pg, mysql });
  if (!targets) return; // skipped

  for (const target of targets) {
    if (type === 'delete') {
      await softDelete({
        schema:   target.schema,
        table:    target.table,
        source:   'pipeline2',
        sourceId: String(sourceId),
      });
      continue;
    }

    // Two-step write: outreach needs activity_id from prior activities upsert
    if (target.__depends_on_activity) {
      const activityRes = await pg.query(
        `SELECT id FROM crm.activities
         WHERE source = 'pipeline2' AND source_id = $1
         LIMIT 1`,
        [target.record.source_id]
      );
      if (activityRes.rows.length) {
        target.record.activity_id = activityRes.rows[0].id;
      }
      delete target.__depends_on_activity;
    }

    // Two-step write: campaign_lists junction needs list_id from prior lists upsert
    if (target.__depends_on_list) {
      // source_id on junction is `cl-{client_list_id}` — strip prefix to get list source_id
      const listSourceId = target.record.source_id.replace(/^cl-/, '');
      const listRes = await pg.query(
        `SELECT id FROM crm.lists
         WHERE source = 'pipeline2' AND source_id = $1
         LIMIT 1`,
        [listSourceId]
      );
      if (listRes.rows.length) {
        target.record.list_id = listRes.rows[0].id;
      } else {
        logger.warn('list not found for campaign_lists junction', { listSourceId });
        continue; // skip junction if list not found
      }
      delete target.__depends_on_list;
    }

    await upsert(target);
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

    process.stderr.write(
      `[DEBUG] db=${event.database} table=${event.table} type=${event.type} ` +
      `id=${event.data?.id ?? event.data?.client_list_detail_id ?? event.data?.event_tm_ob_txn_id}\n`
    );

    try {
      await processEvent(event);
      res.writeHead(200);
      res.end('OK');
    } catch (err) {
      logger.error('Failed to process event', { error: err.message, event });
      await logSyncError({
        rawEvent:     event,
        errorMessage: err.message,
        tableName:    event?.table,
      }).catch(() => {});
      res.writeHead(500);
      res.end('Internal Server Error');
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  logger.info(`BigBox sync worker listening on port ${PORT}`);
});
