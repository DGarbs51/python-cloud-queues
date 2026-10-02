// Test 1: HTTP ingress knee. Drives each endpoint in turn with an arrival-rate executor, so
// a slow app shows up as latency, dropped iterations and 502/504/429 instead of fewer requests.
//   RATE=50 DURATION=120 k6 run k6/http.js              ramp 1 -> RATE req/s over DURATION seconds
//   EXECUTOR=constant RATE=20 DURATION=60 k6 run k6/http.js
//   ENDPOINTS=ping k6 run k6/http.js                     a subset: ping,static,stats
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { TREND_STATS, get, int, summary } from './lib.js';

const RATE = int('RATE', 50);
const DURATION = int('DURATION', 120);
const EXECUTOR = __ENV.EXECUTOR || 'ramping';
const P95_MS = int('P95_MS', 1000);
const PATHS = { ping: '/api/ping', static: '/', stats: '/api/stats' };
const ENDPOINTS = (__ENV.ENDPOINTS || 'ping,static,stats').split(',');

const statuses = { 429: new Counter('status_429'), 502: new Counter('status_502'), 504: new Counter('status_504') };
const otherErrors = new Counter('status_other_error'); // other >= 400 and connection errors (status 0)

const scenarios = {};
const thresholds = {};
ENDPOINTS.forEach((name, i) => {
  const executor =
    EXECUTOR === 'constant'
      ? { executor: 'constant-arrival-rate', rate: RATE, duration: `${DURATION}s` }
      : { executor: 'ramping-arrival-rate', startRate: 1, stages: [{ target: RATE, duration: `${DURATION}s` }] };
  // Endpoints run one after another, with a 10 s gap, so each knee is measured alone.
  scenarios[name] = Object.assign(executor, {
    timeUnit: '1s',
    preAllocatedVUs: Math.max(10, RATE),
    // Enough VUs to keep the rate while every request waits out the 20 s proxy timeout.
    maxVUs: int('MAX_VUS', RATE * 25),
    startTime: `${i * (DURATION + 10)}s`,
    exec: 'hit',
    env: { ENDPOINT: name },
  });
  thresholds[`http_req_duration{scenario:${name}}`] = [`p(95)<${P95_MS}`];
  thresholds[`http_req_failed{scenario:${name}}`] = ['rate<0.01'];
  // Always-true thresholds make k6 report these per endpoint in the summary.
  thresholds[`dropped_iterations{scenario:${name}}`] = ['count>=0'];
  for (const s of ['429', '502', '504', 'other_error']) thresholds[`status_${s}{scenario:${name}}`] = ['count>=0'];
});

export const options = {
  scenarios,
  thresholds,
  summaryTrendStats: TREND_STATS,
};

export function hit() {
  const name = __ENV.ENDPOINT;
  // 30 s is above the 20 s nginx proxy_read_timeout, so the proxy's 504 is what k6 records.
  const res = get(PATHS[name], { timeout: '30s', tags: { name } });
  if (statuses[res.status]) statuses[res.status].add(1);
  else if (res.status === 0 || res.status >= 400) otherErrors.add(1);
  check(res, { [`${name} 200`]: (r) => r.status === 200 });
}

export const handleSummary = summary('http');
