// Shared helpers for the k6 suite. Run scripts from the repository root so summaries land in
// results/: BASE_URL=https://python-cloud-queues-3-12.laravel-demo.cloud k6 run k6/http.js
import http from 'k6/http';
import { check, fail, sleep } from 'k6';

export const BASE_URL = (__ENV.BASE_URL || 'http://127.0.0.1:8000').replace(/\/+$/, '');
const HOST = BASE_URL.replace(/^\w+:\/\//, '').split('/')[0];
// "3-12" for python-cloud-queues-3-12.laravel-demo.cloud; ENV_NAME overrides.
export const ENV_NAME =
  __ENV.ENV_NAME || (HOST.startsWith('python-cloud-queues-') ? HOST.split('.')[0].slice(20) : 'local');

export const TREND_STATS = ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'];
export const TERMINAL = ['done', 'failed', 'expired'];

export function int(name, fallback) {
  const value = __ENV[name];
  return value === undefined || value === '' ? fallback : parseInt(value, 10);
}

export function get(path, params = {}) {
  return http.get(BASE_URL + path, params);
}

// Every POST sends JSON: the app rejects other content types with 415.
export function post(path, body = {}, params = {}) {
  const headers = Object.assign({ 'Content-Type': 'application/json' }, params.headers);
  return http.post(BASE_URL + path, JSON.stringify(body), Object.assign({}, params, { headers }));
}

export function json(res) {
  try {
    return res.json();
  } catch (e) {
    return null;
  }
}

// Polls GET /api/load/<run> until the run is terminal or `deadline` seconds pass.
// onSample(run) sees every successful poll. Returns {run, timedOut}; run is the last record seen.
export function poll(run, deadline = 900, interval = 2, onSample = null) {
  const end = Date.now() + deadline * 1000;
  let last = null;
  while (Date.now() < end) {
    const res = get(`/api/load/${run}`, { tags: { name: 'GET /api/load/<run>' } });
    const body = res.status === 200 ? json(res) : null;
    if (body) {
      last = body;
      if (onSample) onSample(body);
      if (TERMINAL.includes(body.state)) return { run: body, timedOut: false };
    }
    sleep(interval);
  }
  return { run: last, timedOut: true };
}

// Starts a run and waits for it. Past `deadline` seconds the run is cancelled so it
// stops holding the environment's single load slot.
export function runLoad(body, deadline, interval = 2) {
  const res = post('/api/load', body, { tags: { name: 'POST /api/load' } });
  const accepted = json(res);
  if (res.status !== 202 || !accepted) fail(`POST /api/load ${res.status}: ${res.body}`);
  const started = Date.now();
  const result = poll(accepted.run, deadline, interval);
  if (result.timedOut) post(`/api/load/${accepted.run}/cancel`);
  return Object.assign(result, { id: accepted.run, seconds: (Date.now() - started) / 1000 });
}

export function checkRun(result, count) {
  const run = result.run || {};
  return check(run, {
    'run reached a terminal state before the deadline': () => !result.timedOut,
    'state is done': (r) => r.state === 'done',
    'dispatched == count': (r) => r.dispatched === count && !r.dispatch_error,
    'processed == count': (r) => r.processed === count,
    'failed == 0': (r) => r.failed === 0,
  });
}

// Use as `export const handleSummary = summary('queue');` to write results/<env>-<script>.json
// and print a compact summary (k6 skips its own once handleSummary is defined).
export function summary(script) {
  return (data) => {
    const file = `results/${ENV_NAME}-${script}.json`;
    return {
      [file]: JSON.stringify(data, null, 2),
      stdout: `\n${BASE_URL} (${ENV_NAME})\n${text(data)}\nsummary: ${file}\n`,
    };
  };
}

function text(data) {
  const lines = [];
  const checks = (group) => {
    for (const c of group.checks || []) lines.push(`  ${c.fails ? 'FAIL' : 'ok  '} ${c.name} (${c.passes}/${c.passes + c.fails})`);
    for (const g of group.groups || []) checks(g);
  };
  checks(data.root_group);
  for (const name of Object.keys(data.metrics).sort()) {
    const m = data.metrics[name];
    const v = m.values;
    const shown =
      m.type === 'trend' ? TREND_STATS.filter((s) => s in v).map((s) => `${s}=${round(v[s])}`).join(' ')
      : m.type === 'counter' ? `count=${round(v.count)} rate=${round(v.rate)}/s`
      : m.type === 'rate' ? `${round(v.rate * 100)}% (${v.passes}/${v.passes + v.fails})`
      : `value=${round(v.value)}`;
    const failed = Object.entries(m.thresholds || {}).filter(([, t]) => !t.ok).map(([expr]) => expr);
    lines.push(`  ${name}: ${shown}${failed.length ? `  THRESHOLD FAILED: ${failed.join(', ')}` : ''}`);
  }
  return lines.join('\n');
}

function round(n) {
  return Math.round(n * 100) / 100;
}
