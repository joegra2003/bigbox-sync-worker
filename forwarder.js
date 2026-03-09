/**
 * Reads Maxwell CDC events from stdin and forwards each line to the sync worker.
 */
const readline = require('readline');
const http = require('http');

const SYNC_WORKER_PORT = process.env.SYNC_WORKER_PORT || 3001;

const rl = readline.createInterface({ input: process.stdin });

rl.on('line', (line) => {
  const trimmed = line.trim();
  if (!trimmed || !trimmed.startsWith('{')) return;

  const data = Buffer.from(trimmed);
  const req = http.request({
    hostname: '127.0.0.1',
    port: SYNC_WORKER_PORT,
    path: '/',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': data.length,
    },
  }, (res) => {
    process.stderr.write(`forwarded: ${res.statusCode}\n`);
  });

  req.on('error', (e) => {
    process.stderr.write(`forward error: ${e.message}\n`);
  });

  req.write(data);
  req.end();
});
