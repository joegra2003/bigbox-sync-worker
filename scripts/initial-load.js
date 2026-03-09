/**
 * Direct initial load: reads callbox_pipeline2.clients from MySQL
 * and POSTs each row to the sync worker HTTP endpoint.
 *
 * Usage: node scripts/initial-load.js
 */

const http = require('http');
const mysql = require('mysql2/promise');

const SYNC_WORKER_URL = process.env.SYNC_WORKER_URL || 'http://127.0.0.1:3001';
const BATCH_SIZE = 100;

const MYSQL_CONFIG = {
  host: process.env.MYSQL_HOST || '127.0.0.1',
  port: parseInt(process.env.MYSQL_PORT || '3306'),
  user: process.env.MYSQL_USER || 'maxwell',
  password: process.env.MYSQL_PASSWORD || 'maxwell@CallB0x2026!!',
  database: 'callbox_pipeline2',
};

function postEvent(event) {
  return new Promise((resolve, reject) => {
    const data = Buffer.from(JSON.stringify(event));
    const req = http.request({
      hostname: '127.0.0.1',
      port: 3001,
      path: '/',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length,
      },
    }, (res) => {
      res.resume();
      if (res.statusCode === 200) resolve();
      else reject(new Error(`HTTP ${res.statusCode}`));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function main() {
  console.log('Connecting to MySQL...');
  const conn = await mysql.createConnection(MYSQL_CONFIG);

  const [rows] = await conn.query('SELECT COUNT(*) AS total FROM clients');
  const total = rows[0].total;
  console.log(`Total clients: ${total}`);

  let offset = 0;
  let processed = 0;
  let errors = 0;

  while (offset < total) {
    const [batch] = await conn.query('SELECT * FROM clients LIMIT ? OFFSET ?', [BATCH_SIZE, offset]);
    for (const row of batch) {
      const event = {
        database: 'callbox_pipeline2',
        table: 'clients',
        type: 'insert',
        data: row,
      };
      try {
        await postEvent(event);
        processed++;
      } catch (err) {
        errors++;
        console.error(`Error on id=${row.id}: ${err.message}`);
      }
    }
    offset += BATCH_SIZE;
    console.log(`Progress: ${Math.min(offset, total)}/${total} (errors: ${errors})`);
  }

  await conn.end();
  console.log(`Done. Processed: ${processed}, Errors: ${errors}`);
}

main().catch(console.error);
