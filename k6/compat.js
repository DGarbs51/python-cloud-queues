// Test 11: runtime compatibility probes on the web process and on a worker.
//   MIN_PROBES=40 ALLOWED_SKIPS=readline,dbm k6 run k6/compat.js
// Fails on any probe with status "fail", on fewer than MIN_PROBES probes per side, and on a skip
// unless the probe needs a newer Python than the one running or is listed in ALLOWED_SKIPS.
// Probes whose status differs between web and worker are logged and counted.
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import { THRESHOLDS, TREND_STATS, abort, completed, get, int, json, post, summary } from './lib.js';

const MIN_PROBES = int('MIN_PROBES', 1);
const ALLOWED_SKIPS = (__ENV.ALLOWED_SKIPS || '').split(',').filter(Boolean);
const mismatches = new Counter('web_worker_mismatches');

export const options = {
  scenarios: { compat: { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: '5m' } },
  thresholds: THRESHOLDS(),
  summaryTrendStats: TREND_STATS,
};

// "3.12" -> [3, 12]; compares major then minor.
function older(python, minPython) {
  const [a, b] = python.split('.').map(Number);
  const [c, d] = String(minPython).split('.').map(Number);
  return a < c || (a === c && b < d);
}

function assertProbes(side, probes, python) {
  const failed = probes.filter((p) => p.status === 'fail');
  const badSkips = probes.filter(
    (p) => p.status === 'skip' && !ALLOWED_SKIPS.includes(p.name) && !(p.min_python && older(python, p.min_python))
  );
  for (const p of failed.concat(badSkips)) console.warn(`${side} ${p.status}: ${p.name} (${p.group}): ${p.detail}`);
  check(probes, {
    [`${side}: at least ${MIN_PROBES} probes`]: (ps) => ps.length >= MIN_PROBES,
    [`${side}: no failed probe`]: () => failed.length === 0,
    [`${side}: no unexpected skip`]: () => badSkips.length === 0,
  });
}

export default function () {
  const env = json(get('/api/env', { tags: { name: 'GET /api/env' } }));
  if (!env || !env.python) abort('GET /api/env did not return JSON');
  const before = json(get('/api/compat', { tags: { name: 'GET /api/compat' } }));
  if (!before) abort('GET /api/compat did not return JSON');
  assertProbes('web', before.web || [], env.python);

  const res = post('/api/compat/worker', {}, { tags: { name: 'POST /api/compat/worker' } });
  check(res, { 'worker probes dispatched (202)': (r) => r.status === 202 });
  // Wait for a new result: worker_at changes once a worker has stored its probes.
  const end = Date.now() + int('WORKER_S', 180) * 1000;
  let after = before;
  while (Date.now() < end) {
    sleep(2);
    after = json(get('/api/compat', { tags: { name: 'GET /api/compat' } })) || after;
    if (after.worker && after.worker_at !== before.worker_at) break;
  }
  if (!check(after, { 'worker stored fresh probes': (a) => a.worker && a.worker_at !== before.worker_at })) {
    abort('no fresh worker probes');
  }
  assertProbes('worker', after.worker, env.python);

  const web = Object.fromEntries((after.web || []).map((p) => [p.name, p]));
  for (const p of after.worker) {
    if (web[p.name] && web[p.name].status !== p.status) {
      mismatches.add(1);
      console.warn(`web/worker differ: ${p.name}: web ${web[p.name].status} (${web[p.name].detail}), worker ${p.status} (${p.detail})`);
    }
  }
  completed.add(1);
}

export const handleSummary = summary('compat');
