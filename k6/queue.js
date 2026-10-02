// Tests 2-4 and 9: one load run, from POST /api/load to a terminal state.
//   KIND=async COUNT=1000 MS=100 k6 run k6/queue.js
// KIND: sync|async|cpu|mem|db_write|db_read|db_async|db_sync. COUNT, MS, MB (mem), ROWS (db kinds)
// and KEY (idempotency) are passed through when set. DRAIN_S is the deadline; past it the run
// is cancelled so it stops holding the environment's single load slot.
import { Counter, Trend } from 'k6/metrics';
import { THRESHOLDS, TREND_STATS, checkRun, completed, int, runLoad, summary } from './lib.js';

const COUNT = int('COUNT', 100);
const DRAIN_S = int('DRAIN_S', 900);

const drain = new Trend('drain_s');
const jobsPerS = new Trend('jobs_per_s');
const waitP95 = new Trend('wait_p95');
const runP95 = new Trend('run_p95');
const duplicates = new Counter('duplicates');

export const options = {
  scenarios: { run: { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: `${DRAIN_S + 120}s` } },
  thresholds: THRESHOLDS(),
  summaryTrendStats: TREND_STATS,
};

function loadBody(kind, count) {
  const body = { kind, count };
  for (const name of ['ms', 'mb', 'rows']) {
    const value = int(name.toUpperCase(), undefined);
    if (value !== undefined) body[name] = value;
  }
  if (__ENV.KEY) body.key = __ENV.KEY;
  return body;
}

export default function () {
  const result = runLoad(loadBody(__ENV.KIND || 'sync', COUNT), DRAIN_S);
  const run = result.run || {};
  drain.add(result.seconds);
  if (run.jobs_per_s != null) jobsPerS.add(run.jobs_per_s);
  if (run.wait_ms) waitP95.add(run.wait_ms.p95);
  if (run.run_ms) runP95.add(run.run_ms.p95);
  duplicates.add(run.duplicates || 0); // at-least-once delivery: reported, not failed
  checkRun(result, COUNT);
  console.log(`run ${result.id}: ${JSON.stringify(run)}`);
  completed.add(1);
}

export const handleSummary = summary('queue');
