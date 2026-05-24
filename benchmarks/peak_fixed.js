// peak_fixed.js — find peak RPS for the fixed-window algorithm.
//
// Strategy: ramp up to 200 concurrent virtual users (VUs) over 10 seconds,
// hold at 200 for 30 seconds, ramp down. Each VU fires requests as fast as
// it can. The plateau number is your sustainable throughput.
//
// 429 (rate-limited) is treated as a successful response — for a rate
// limiter, returning 429 when capacity is exceeded is correct behavior.

import http from 'k6/http';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 200 },  // ramp up
    { duration: '30s', target: 200 },  // sustain
    { duration: '5s',  target: 0   },  // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<50'],
    // Only count true errors (5xx, network failures) — not 429s
    'http_req_failed{expected_response:true}': ['rate<0.01'],
  },
};

// Tell k6 that BOTH 200 and 429 are expected/successful statuses.
http.setResponseCallback(http.expectedStatuses(200, 429));

const payload = JSON.stringify({
  key: 'k6_peak_fixed',
  limit: 1000000,
  window: 60,
  algorithm: 'fixed',
});

const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post('http://localhost:8080/check', payload, params);
  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
  });
}
