// L5 server modes (gunicorn/uvicorn): proxy behaviour for streaming, large bodies and websockets.
//   k6 run k6/stream.js                     all three; TESTS=sse,upload,ws picks a subset
//   STREAM_S=30 INTERVAL=1 UPLOAD_MB=100 WS_IDLE_S=25
// SSE: k6 buffers the body, so it cannot see when each event arrives and cannot detect proxy
// buffering. It checks the status, content type and event count (a short or 504 stream is the
// 20 s proxy cut) and records first-byte and receive times as aggregates only. Use k6/sse.py
// for per-event arrival times, first-event latency and buffering.
// websocket /ws/echo is ASGI only (uvicorn); the ws test fails under stdlib and gunicorn.
import { check } from 'k6';
import crypto from 'k6/crypto';
import http from 'k6/http';
import { Trend } from 'k6/metrics';
import { WebSocket } from 'k6/websockets';
import { BASE_URL, THRESHOLDS, TREND_STATS, completed, int, json, summary } from './lib.js';

const STREAM_S = int('STREAM_S', 30);
const INTERVAL = Number(__ENV.INTERVAL || 1);
const UPLOAD_MB = int('UPLOAD_MB', 100);
const WS_IDLE_S = int('WS_IDLE_S', 25);
const TESTS = (__ENV.TESTS || 'sse,upload,ws').split(',');

// Aggregates over the whole response; not per-event timings.
const firstByte = new Trend('sse_first_byte_ms', true);
const receiving = new Trend('sse_receiving_ms', true);
const uploadTime = new Trend('upload_ms', true);
const wsConnect = new Trend('ws_connect_ms', true);
const wsEcho = new Trend('ws_echo_ms', true);

const scenarios = {};
TESTS.forEach((name) => {
  scenarios[name] = { executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: '5m', exec: name };
});

export const options = { scenarios, thresholds: THRESHOLDS(TESTS.length), summaryTrendStats: TREND_STATS };

export function sse() {
  const res = http.get(`${BASE_URL}/api/stream?seconds=${STREAM_S}&interval=${INTERVAL}`, {
    timeout: `${STREAM_S + 60}s`,
    tags: { name: 'GET /api/stream' },
  });
  const events = (res.body || '').split('\n\n').filter((e) => e.startsWith('data:')).length;
  const expected = Math.floor(STREAM_S / INTERVAL);
  const t = res.timings;
  firstByte.add(t.waiting);
  receiving.add(t.receiving);
  console.log(`sse: status ${res.status}, ${events}/${expected} events, first byte ${Math.round(t.waiting)} ms, receiving ${Math.round(t.receiving)} ms`);
  check(res, {
    'sse 200': (r) => r.status === 200,
    'sse content-type text/event-stream': (r) => String(r.headers['Content-Type']).startsWith('text/event-stream'),
    'sse all events arrived (not cut at the proxy timeout)': () => events >= expected,
  });
  completed.add(1);
}

export function upload() {
  const body = new Uint8Array(UPLOAD_MB * 1048576).buffer;
  const res = http.post(`${BASE_URL}/api/upload`, body, {
    headers: { 'Content-Type': 'application/octet-stream' },
    timeout: '180s',
    tags: { name: 'POST /api/upload' },
  });
  uploadTime.add(res.timings.duration);
  const out = json(res) || {};
  console.log(`upload: status ${res.status}, ${Math.round(res.timings.duration)} ms, ${res.body && res.body.slice(0, 200)}`);
  check(out, {
    'upload 200': () => res.status === 200,
    'upload byte count matches': (o) => o.bytes === UPLOAD_MB * 1048576,
    'upload sha256 matches': (o) => o.sha256 === crypto.sha256(body, 'hex'),
  });
  completed.add(1);
}

export function ws() {
  const url = `${BASE_URL.replace(/^http/, 'ws')}/ws/echo`;
  const started = Date.now();
  const socket = new WebSocket(url);
  const got = [];
  let sentAt = 0;
  let closed = false;
  let timedOut = false;
  let timer;
  const send = (text) => {
    if (closed) return;
    sentAt = Date.now();
    socket.send(text);
  };
  socket.onopen = () => {
    wsConnect.add(Date.now() - started);
    send('hello');
  };
  socket.onmessage = (e) => {
    wsEcho.add(Date.now() - sentAt);
    got.push(e.data);
    // After the first echo, stay idle past the 20 s proxy timeout, then check the socket is alive.
    if (got.length === 1) setTimeout(() => send('after idle'), WS_IDLE_S * 1000);
    else if (got.length === 2) send('bye');
  };
  socket.onerror = (e) => console.warn(`ws error: ${e.error}`);
  socket.onclose = () => {
    closed = true;
    clearTimeout(timer);
    console.log(`ws: received ${JSON.stringify(got)} in ${Date.now() - started} ms`);
    check(got, {
      'ws echoes text': (g) => g[0] === 'hello',
      [`ws survives ${WS_IDLE_S} s idle`]: (g) => g[1] === 'after idle',
      'ws server closes on "bye"': () => got.length >= 2 && !timedOut,
    });
    completed.add(1);
  };
  timer = setTimeout(() => {
    timedOut = true;
    socket.close();
  }, (WS_IDLE_S + 30) * 1000);
}

export const handleSummary = summary('stream');
