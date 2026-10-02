// End to end under load: dashboard readers poll /, /api/stats and /api/load the whole time
// while one VU runs a queue load, then a single Run check, and asserts every verdict passes.
//   READ_RATE=10 KIND=sync COUNT=500 MS=50 k6 run k6/e2e.js
// The check is never run concurrently: the driver waits out any check already running first.
// Readers run for the driver's worst case (DRAIN_S plus two check waits); DURATION shortens it.
import { check, sleep } from 'k6';
import { THRESHOLDS, TREND_STATS, abort, checkRun, completed, get, int, json, post, runLoad, summary } from './lib.js';

const COUNT = int('COUNT', 500);
const DRAIN_S = int('DRAIN_S', 300);
const CHECK_WAIT_S = 300; // telemetry gives up on a case after 240 s
const DRIVER_S = DRAIN_S + 2 * CHECK_WAIT_S + 120; // the phases below, plus request and cancel slack
const DURATION = int('DURATION', DRIVER_S);
const READS = ['/', '/api/stats', '/api/load'];
const CASES = ['quick', 'async', 'delayed', 'flaky', 'failing', 'timeout', 'burst', 'workers'];

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
    driver: { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: `${DRIVER_S}s`, exec: 'drive' },
  },
  thresholds: Object.assign(THRESHOLDS(), {
    'http_req_failed{scenario:readers}': ['rate<0.01'],
    'http_req_duration{scenario:readers}': [`p(95)<${int('P95_MS', 1000)}`],
    'dropped_iterations{scenario:readers}': ['count>=0'],
  }),
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
  const body = { kind: __ENV.KIND || 'sync', count: COUNT, ms: int('MS', 50) };
  checkRun(runLoad(body, DRAIN_S), COUNT);

  let end = Date.now() + CHECK_WAIT_S * 1000;
  while ((checkState() || {}).status === 'running' && Date.now() < end) sleep(5);
  const before = checkState();
  if (before && before.status === 'running') abort('another check is still running');

  const res = post('/api/check', {}, { timeout: '60s', tags: { name: 'POST /api/check' } });
  if (!check(res, { 'POST /api/check 200': (r) => r.status === 200 })) abort(`POST /api/check ${res.status}`);
  // Our check is the first verdict with a different started_at from the one before it.
  const fresh = (v) => v && (!before || v.started_at !== before.started_at);
  end = Date.now() + CHECK_WAIT_S * 1000;
  let verdict;
  do {
    sleep(5);
    verdict = checkState();
  } while (Date.now() < end && !(fresh(verdict) && verdict.status !== 'running'));

  const results = fresh(verdict) ? verdict.results || [] : [];
  check(verdict, {
    'check is this run': fresh,
    'check status pass': (v) => fresh(v) && v.status === 'pass',
  });
  for (const name of CASES) {
    const r = results.find((x) => x.case === name);
    check(r, { [`check ${name} passes`]: (x) => x !== undefined && x.ok === true });
    if (!r || r.ok !== true) console.warn(`check ${name}: ${r ? r.detail : 'missing'}`);
  }
  completed.add(1);
}

export const handleSummary = summary('e2e');
