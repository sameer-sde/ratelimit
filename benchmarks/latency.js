// latency.js — fixed throughput, measure latency distribution.
// At 2000 req/s sustained, what does p50/p95/p99 look like?

import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    constant_rps: {
      executor: 'constant-arrival-rate',
      rate: 2000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
  thresholds: {
    http_req_duration: ['p(50)<10', 'p(95)<30', 'p(99)<100'],
    'http_req_failed{expected_response:true}': ['rate<0.01'],
  },
};

http.setResponseCallback(http.expectedStatuses(200, 429));

const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const userId = Math.floor(Math.random() * 1000);
  const payload = JSON.stringify({
    key: `user_${userId}`,
    limit: 100,
    window: 60,
    algorithm: 'fixed',
  });
  const res = http.post('http://localhost:8080/check', payload, params);
  check(res, {
    'status ok': (r) => r.status === 200 || r.status === 429,
  });
}
