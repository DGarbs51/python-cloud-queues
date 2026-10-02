// Tests 5 and 7: memory autoscaling and OOM. Every 5 s it samples /api/env (a few requests,
// to see which App replicas answer) and the load run, and logs the replicas seen over time.
//   SCENARIO=worker_scale k6 run k6/memory.js  COUNT x mem jobs of MB MiB for MS ms each; the 4
//                                              worker processes per replica hold ~4 x MB (default
//                                              40 x 350 MiB x 30 s: ~1.6 of 2 GiB for ~5 min)
//   SCENARIO=app_scale k6 run k6/memory.js     one /api/hold of MB (1500) MiB for HOLD_S (600) s
//                                              on the App, sampled for 2 more minutes
//   SCENARIO=oom k6 run k6/memory.js           COUNT (2) x 1800 MiB jobs: more than one 2 GiB
//                                              replica fits. Run it alone, with max replicas 1.
// EXPECT_SCALE=0 drops the "scaled out" check (for example on a pinned baseline).
import { check, sleep } from 'k6';
import http from 'k6/http';
import { Gauge } from 'k6/metrics';
import { BASE_URL, TERMINAL, THRESHOLDS, TREND_STATS, abort, completed, get, int, json, post, summary } from './lib.js';

const SCENARIO = __ENV.SCENARIO || 'worker_scale';
const DEFAULTS = {
  worker_scale: { count: 40, mb: 350, ms: 30000 },
  app_scale: { mb: 1500, seconds: 600 },
  oom: { count: 2, mb: 1800, ms: 30000 },
}[SCENARIO];
if (!DEFAULTS) throw new Error(`SCENARIO must be worker_scale, app_scale or oom, not ${SCENARIO}`);
const DEADLINE_S = int('DEADLINE_S', 1800);
const EXPECT_SCALE = __ENV.EXPECT_SCALE !== '0' && SCENARIO !== 'oom';

const appReplicas = new Gauge('app_replicas_seen');
const workerReplicas = new Gauge('worker_replicas_seen');
const maxCgroupMem = new Gauge('max_cgroup_mem_mb');
const oomEvents = new Gauge('oom_events');

export const options = {
  scenarios: { memory: { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: `${DEADLINE_S + 120}s` } },
  thresholds: THRESHOLDS(),
  summaryTrendStats: TREND_STATS,
};

// A few /api/env requests per sample: each lands on one App replica.
function sampleEnv() {
  const req = { method: 'GET', url: `${BASE_URL}/api/env`, params: { tags: { name: 'GET /api/env' } } };
  return http.batch([req, req, req, req, req]).map(json).filter(Boolean);
}

export default function () {
  const started = Date.now();
  let run = null;
  let holdEnds = 0;
  if (SCENARIO === 'app_scale') {
    const seconds = int('HOLD_S', DEFAULTS.seconds);
    const res = post('/api/hold', { mb: int('MB', DEFAULTS.mb), seconds }, { tags: { name: 'POST /api/hold' } });
    if (!check(res, { 'hold accepted (202)': (r) => r.status === 202 })) {
      abort(`POST /api/hold ${res.status}: ${String(res.body).slice(0, 200)}`);
    }
    holdEnds = started + (seconds + 120) * 1000;
  } else {
    const body = { kind: 'mem', count: int('COUNT', DEFAULTS.count), mb: int('MB', DEFAULTS.mb), ms: int('MS', DEFAULTS.ms) };
    const res = post('/api/load', body, { tags: { name: 'POST /api/load' } });
    run = (json(res) || {}).run;
    if (!check(res, { 'load accepted (202)': (r) => r.status === 202 && typeof run === 'string' && run !== '' })) {
      abort(`POST /api/load ${res.status}: ${String(res.body).slice(0, 200)}`);
    }
  }

  const apps = new Set();
  let maxWorkers = 0;
  let last = {};
  const end = started + DEADLINE_S * 1000;
  while (Date.now() < end) {
    const envs = sampleEnv();
    envs.forEach((e) => apps.add(e.host));
    if (run) {
      last = json(get(`/api/load/${run}`, { tags: { name: 'GET /api/load/<run>' } })) || last;
      maxWorkers = Math.max(maxWorkers, last.replicas || 0);
    }
    const t = Math.round((Date.now() - started) / 1000);
    const mem = envs.map((e) => `${e.host}=${Math.round((e.memory_current || 0) / 1048576)}MiB`);
    console.log(
      `t=${t}s app_replicas=${new Set(envs.map((e) => e.host)).size} [${[...new Set(mem)].join(' ')}]` +
        (run ? ` run=${last.state} processed=${last.processed}/${last.count} failed=${last.failed}` +
          ` worker_replicas=${last.replicas} workers=${Object.keys(last.workers || {}).length}` +
          ` max_cgroup_mem_mb=${last.max_cgroup_mem_mb} oom_events=${last.oom_events}` : '')
    );
    if (run ? TERMINAL.includes(last.state) : Date.now() > holdEnds) break;
    sleep(5);
  }

  if (run && !TERMINAL.includes(last.state)) post(`/api/load/${run}/cancel`);
  appReplicas.add(apps.size);
  console.log(`app replicas seen: ${[...apps].join(', ')}`);
  if (SCENARIO === 'app_scale') {
    if (EXPECT_SCALE) check(apps, { 'App scaled out (more than 1 replica seen)': (a) => a.size > 1 });
    completed.add(1);
    return;
  }
  workerReplicas.add(maxWorkers);
  maxCgroupMem.add(last.max_cgroup_mem_mb || 0);
  oomEvents.add(last.oom_events || 0);
  console.log(`run ${run}: ${JSON.stringify(last)}`);
  check(last, { 'run reached a terminal state': (r) => TERMINAL.includes(r.state) });
  if (SCENARIO === 'oom') {
    check(last, {
      'OOM kill observed (oom_events > 0)': (r) => r.oom_events > 0,
      'no job lost (processed + failed == count)': (r) => r.processed + r.failed === r.count,
    });
  } else {
    check(last, {
      'processed == count': (r) => r.processed === r.count,
      'failed == 0': (r) => r.failed === 0,
      'no OOM kill': (r) => !r.oom_events,
    });
    if (EXPECT_SCALE) check(maxWorkers, { 'Worker scaled out (more than 1 replica ran jobs)': (n) => n > 1 });
  }
  completed.add(1);
}

export const handleSummary = summary(`memory-${SCENARIO}`);
