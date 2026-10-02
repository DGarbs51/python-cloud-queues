// End to end under load: dashboard readers poll /, /api/stats and /api/load the whole time
// while one VU runs a queue load, then a single Run check, and asserts every verdict passes.
//   READ_RATE=10 DURATION=420 KIND=sync COUNT=500 MS=50 k6 run k6/e2e.js
// The check is never run concurrently: the driver waits out any check already running first.
import { check, fail, sleep } from 'k6';
import { TREND_STATS, checkRun, get, int, json, post, runLoad, summary } from './lib.js';

const DURATION = int('DURATION', 420);
const COUNT = int('COUNT', 500);
const CHECK_DEADLINE = 300; // telemetry gives up on a case after 240 s
const READS = ['/', '/api/stats', '/api/load'];

export const options = {
  scenarios: {
    readers: {
      executor: 'constant-arrival-rate',
      rate: int('READ_RATE', 10),
      timeUnit: '1s',
      duration: `${DURATION}s`,
      preAllocatedVUs: 20,
      maxVUs: 200,
      exec: 'read',
    },
    driver: { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: `${DURATION}s`, exec: 'drive' },
  },
  thresholds: {
    checks: ['rate==1'],
    'http_req_failed{scenario:readers}': ['rate<0.01'],
    'http_req_duration{scenario:readers}': [`p(95)<${int('P95_MS', 1000)}`],
    'dropped_iterations{scenario:readers}': ['count>=0'],
  },
  summaryTrendStats: TREND_STATS,
};

export function read() {
  const path = READS[Math.floor(Math.random() * READS.length)];
  const res = get(path, { timeout: '30s', tags: { name: `GET ${path}` } });
  check(res, { 'reader 200': (r) => r.status === 200 });
}

function checkState() {
  const snapshot = json(get('/api/stats', { tags: { name: 'GET /api/stats' } }));
  return snapshot ? snapshot.check : undefined;
}

export function drive() {
  const deadline = int('DRAIN_S', 600);
  const body = { kind: __ENV.KIND || 'sync', count: COUNT, ms: int('MS', 50) };
  checkRun(runLoad(body, deadline), COUNT);

  let end = Date.now() + CHECK_DEADLINE * 1000;
  while ((checkState() || {}).status === 'running' && Date.now() < end) sleep(5);
  const before = checkState();
  if (before && before.status === 'running') fail('another check is still running');

  const res = post('/api/check', {}, { timeout: '60s', tags: { name: 'POST /api/check' } });
  if (!check(res, { 'POST /api/check 200': (r) => r.status === 200 })) return;
  end = Date.now() + CHECK_DEADLINE * 1000;
  let verdict;
  do {
    sleep(5);
    verdict = checkState();
  } while (
    Date.now() < end &&
    (!verdict || verdict.status === 'running' || (before && verdict.started_at === before.started_at))
  );

  check(verdict, { 'check finished': (v) => v && v.status !== 'running' });
  for (const r of (verdict && verdict.results) || []) {
    check(r, { [`check ${r.case} passes`]: (x) => x.ok === true });
    if (r.ok !== true) console.warn(`check ${r.case}: ${r.detail}`);
  }
}

export const handleSummary = summary('e2e');
