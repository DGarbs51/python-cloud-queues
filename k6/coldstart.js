// Test 6: hibernation cold start. Close every dashboard tab (it polls) and wait until the
// environment is asleep, then run once:
//   k6 run k6/coldstart.js
// Measures time to first byte of /api/ping, a Python route (nginx answers /healthz-* without
// waking the app), retrying until it answers 200, then dispatches one sync job and times it
// until the worker has processed it.
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';
import { THRESHOLDS, TREND_STATS, checkRun, completed, get, int, runLoad, summary } from './lib.js';

const firstByte = new Trend('first_byte_ms', true);
const firstOk = new Trend('first_200_ms', true);
const firstJob = new Trend('first_job_s');
const WAKE_S = int('WAKE_S', 300);
const DRAIN_S = int('DRAIN_S', 600);

export const options = {
  scenarios: { cold: { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: `${WAKE_S + DRAIN_S + 180}s` } },
  thresholds: THRESHOLDS(),
  summaryTrendStats: TREND_STATS,
};

export default function () {
  const started = Date.now();
  const deadline = started + WAKE_S * 1000;
  let res;
  let attempts = 0;
  do {
    if (attempts) sleep(1);
    attempts++;
    res = get('/api/ping', { timeout: '120s', tags: { name: 'GET /api/ping' } });
    if (attempts === 1) {
      const t = res.timings;
      firstByte.add(t.blocked + t.connecting + t.tls_handshaking + t.sending + t.waiting);
      console.log(`first request: status ${res.status}, ${Math.round(res.timings.duration)} ms`);
    }
  } while (res.status !== 200 && Date.now() < deadline);
  firstOk.add(Date.now() - started);
  check(res, {
    'ping answered 200': (r) => r.status === 200,
    'first request answered 200 (no 502/504 while waking)': () => attempts === 1,
  });

  const result = runLoad({ kind: 'sync', count: 1, ms: 1 }, DRAIN_S, 0.5);
  firstJob.add(result.seconds);
  checkRun(result, 1);
  console.log(`first job: ${result.seconds.toFixed(1)} s, run ${JSON.stringify(result.run)}`);
  completed.add(1);
}

export const handleSummary = summary('coldstart');
