// BASE_URL=https://... BURST=100 k6 run k6/logs.js
import { check, sleep } from 'k6';
import { THRESHOLDS, abort, completed, get, int, json, post, summary } from './lib.js';

export const options = { scenarios: { logs: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: '5m' } }, thresholds: THRESHOLDS() };

export default function () {
  const res = post('/api/logtest', { format: 'all', where: 'both', burst: int('BURST', 0) });
  const run = json(res);
  if (!check(res, { 'logtest accepted': r => r.status === 202 }) || !run?.marker) abort('logtest not accepted');
  console.log(`marker=${run.marker}`);
  console.log(run.command);
  const deadline = Date.now() + 240000;
  let status;
  do {
    sleep(2);
    const res = get(`/api/logtest/${run.marker}`);
    status = res.status === 200 ? json(res) : null;
    if (status?.web === 'failed' || status?.worker === 'failed') abort('logtest emission failed');
  } while (Date.now() < deadline && !(status?.web_emitted && status?.worker_emitted));
  if (!check(status, { 'web and worker emitted': s => s?.web_emitted && s?.worker_emitted })) abort('logtest worker timeout');
  completed.add(1);
}

export const handleSummary = summary('logs');
