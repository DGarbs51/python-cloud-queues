// In-flight concurrency vs Cloud's HTTP autoscaling trigger: GET /api/slow?seconds=1 held just below, then just
// above, 95 in flight per pod (floor(0.7 x 4096 / 30) on 4 GiB). By Little's law in flight = arrival rate x 1 s.
// Starts at 1 pod. HPA ignores ratios within 10% of the target, so the load steps are: 85/s (below target), 100/s
// (above 95 but inside the tolerance), 115/s (clearly above it); then "down" shows scale-in. Same shape for PASS=A and B.
//   below 120 s at 85/s | inside 120 s at 100/s | above 120 s at 115/s | down 360 s at 30/s   (steps > 1 min, scale-down > 300 s)
// Caps: maxVUs 250 x 3 load steps (115/s survives 2 s latency) and 60, wall-clock 900 s.
// VUh upper bound: (3 x 250 x 120 + 60 x 360 + 5 x 720 for the stats sampler)/3600 = 32.
import { CLOUD, SAFETY, begin, end, get, sampleStats, statsScenario } from './lib.js';

const WALL_CAP_S = 900;
const PATH = '/api/slow?seconds=1';
const step = (rate, duration, startTime, maxVUs) => ({
  executor: 'constant-arrival-rate', exec: 'slow', rate, timeUnit: '1s', duration, startTime,
  preAllocatedVUs: Math.min(maxVUs, rate + 20), maxVUs, gracefulStop: '30s',
});

// STEPS=below,inside,above,down (req/s) resizes the steps for a server whose real capacity is far below 95 per pod,
// e.g. sync gunicorn: STEPS=2,3,5,1 (3 workers) or STEPS=20,24,30,8 (3 workers x 8 threads). Same durations.
const [BELOW, INSIDE, ABOVE, DOWN] = (__ENV.STEPS || '85,100,115,30').split(',').map(Number);
// A saturated sync pod queues requests until the 60 s timeout; give k6 room to keep them in flight (cap 300).
const vus = (rate) => Math.min(300, Math.max(20, rate * 60));

export const scenarios = {
  below: step(BELOW, '120s', '0s', __ENV.STEPS ? vus(BELOW) : 250),
  inside: step(INSIDE, '120s', '120s', __ENV.STEPS ? vus(INSIDE) : 250),
  above: step(ABOVE, '120s', '240s', __ENV.STEPS ? vus(ABOVE) : 250),
  down: step(DOWN, '360s', '360s', __ENV.STEPS ? vus(DOWN) : 60),
  stats: statsScenario('720s'),
};
const SHAPE = `below ${BELOW}/s 120 s; inside ${INSIDE}/s 120 s; above ${ABOVE}/s 120 s; down ${DOWN}/s 360 s`;

export const options = { scenarios, thresholds: SAFETY, cloud: { name: `concurrency ${__ENV.ENV_NAME} pass ${__ENV.PASS}${__ENV.STEPS ? ` steps ${__ENV.STEPS}` : ''}`, distribution: CLOUD.distribution } };

export function setup() {
  return begin('concurrency.js', scenarios, SHAPE);
}

export function slow() {
  get(PATH, WALL_CAP_S, '/api/slow?seconds=1');
}

export function stats() {
  sampleStats(WALL_CAP_S);
}

export function teardown(run) {
  end(run);
}
