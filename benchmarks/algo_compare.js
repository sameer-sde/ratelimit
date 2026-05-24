// algo_compare.js — same load, different algorithms. Compare throughput.
// Each algorithm runs for 15 seconds sequentially with 50 VUs.

import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    fixed: {
      executor: 'constant-vus', vus: 50, duration: '15s',
      exec: 'runFixed', startTime: '0s',
    },
    slog: {
      executor: 'constant-vus', vus: 50, duration: '15s',
      exec: 'runSlog', startTime: '20s',
    },
    swc: {
      executor: 'constant-vus', vus: 50, duration: '15s',
      exec: 'runSwc', startTime: '40s',
    },
    bucket: {
      executor: 'constant-vus', vus: 50, duration: '15s',
      exec: 'runBucket', startTime: '60s',
    },
  },
};

// 429 is a legitimate response for a rate limiter
http.setResponseCallback(http.expectedStatuses(200, 429));

const params = { headers: { 'Content-Type': 'application/json' } };
const baseURL = 'http://localhost:8080/check';

function send(payload, tag) {
  const res = http.post(baseURL, payload, {
    ...params,
    tags: { algorithm: tag },
  });
  check(res, { ok: (r) => r.status === 200 || r.status === 429 });
}

export function runFixed() {
  send(JSON.stringify({
    key: `algocmp_fixed_${Math.floor(Math.random()*200)}`,
    limit: 100, window: 60, algorithm: 'fixed',
  }), 'fixed');
}
export function runSlog() {
  send(JSON.stringify({
    key: `algocmp_slog_${Math.floor(Math.random()*200)}`,
    limit: 100, window: 60, algorithm: 'slog',
  }), 'slog');
}
export function runSwc() {
  send(JSON.stringify({
    key: `algocmp_swc_${Math.floor(Math.random()*200)}`,
    limit: 100, window: 60, algorithm: 'swc',
  }), 'swc');
}
export function runBucket() {
  send(JSON.stringify({
    key: `algocmp_bucket_${Math.floor(Math.random()*200)}`,
    capacity: 100, refill: 10, algorithm: 'bucket',
  }), 'bucket');
}
