// Small k6 load test against a running Compose stack (api-gateway :8080).
// Prerequisites: k6 installed (https://k6.io/docs/get-started/installation/)
// Usage:
//   k6 run scripts/load-accept.js
//   BASE_URL=http://localhost:8080 RATE=50 DURATION=1m k6 run scripts/load-accept.js

import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 20);
const DURATION = __ENV.DURATION || '30s';

export const options = {
  scenarios: {
    accept: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(10, RATE),
      maxVUs: Math.max(50, RATE * 3),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<1000'],
    checks: ['rate>0.95'],
  },
};

export function setup() {
  const accRes = http.post(
    `${BASE}/v1/accounts`,
    JSON.stringify({ name: `k6-load-${Date.now()}` }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (accRes.status !== 201) {
    throw new Error(`create account failed: ${accRes.status} ${accRes.body}`);
  }
  const { apiKey } = accRes.json();

  // Enough credit for RATE * duration seconds (plus headroom).
  const seconds = Math.ceil(parseDurationSeconds(DURATION));
  const amount = RATE * seconds + RATE * 10;
  const topRes = http.post(
    `${BASE}/v1/topups`,
    JSON.stringify({ amount }),
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
    },
  );
  if (topRes.status !== 200) {
    throw new Error(`topup failed: ${topRes.status} ${topRes.body}`);
  }

  return { apiKey };
}

export default function (data) {
  const idem = `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const res = http.post(
    `${BASE}/v1/messages`,
    JSON.stringify({
      to: '09121234567',
      text: `k6-${__VU}-${__ITER}`,
      priority: 'normal',
    }),
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${data.apiKey}`,
        'Idempotency-Key': idem,
      },
      tags: { name: 'POST /v1/messages' },
    },
  );
  check(res, {
    'accepted (202)': (r) => r.status === 202,
  });
}

function parseDurationSeconds(d) {
  const m = /^(\d+)(s|m|h)?$/.exec(String(d));
  if (!m) return 30;
  const n = Number(m[1]);
  switch (m[2] || 's') {
    case 'm':
      return n * 60;
    case 'h':
      return n * 3600;
    default:
      return n;
  }
}
