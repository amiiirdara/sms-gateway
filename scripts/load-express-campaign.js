// k6 load: mix of Express single sends + small campaigns.
// Prerequisites: Compose stack up, k6 installed.
// Usage:
//   k6 run scripts/load-express-campaign.js
//   $env:BASE_URL='http://[::1]:8080'; k6 run scripts/load-express-campaign.js

import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '20s';

export const options = {
  scenarios: {
    mixed: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(10, RATE),
      maxVUs: Math.max(40, RATE * 3),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<1500'],
    checks: ['rate>0.95'],
  },
};

export function setup() {
  const accRes = http.post(
    `${BASE}/v1/accounts`,
    JSON.stringify({ name: `k6-mixed-${Date.now()}` }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (accRes.status !== 201) {
    throw new Error(`create account failed: ${accRes.status} ${accRes.body}`);
  }
  const { apiKey } = accRes.json();
  const seconds = Math.ceil(parseDurationSeconds(DURATION));
  // Express (1) + campaign of 2 ≈ 3 credits/iter worst case; headroom ×2.
  const amount = RATE * seconds * 3 * 2 + 100;
  const topRes = http.post(`${BASE}/v1/topups`, JSON.stringify({ amount }), {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
  });
  if (topRes.status !== 200) {
    throw new Error(`topup failed: ${topRes.status} ${topRes.body}`);
  }
  return { apiKey };
}

export default function (data) {
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${data.apiKey}`,
    'Idempotency-Key': `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  };

  // ~70% express single, ~30% small campaign
  if (__ITER % 10 < 7) {
    const res = http.post(
      `${BASE}/v1/messages`,
      JSON.stringify({
        to: '09121234567',
        text: `otp-${__VU}-${__ITER}`,
        priority: 'express',
      }),
      { headers, tags: { name: 'POST /v1/messages express' } },
    );
    check(res, { 'express 202': (r) => r.status === 202 });
  } else {
    const res = http.post(
      `${BASE}/v1/campaigns`,
      JSON.stringify({
        text: `promo-${__VU}-${__ITER}`,
        recipients: ['09121111111', '09122222222'],
      }),
      { headers, tags: { name: 'POST /v1/campaigns' } },
    );
    check(res, { 'campaign 202': (r) => r.status === 202 });
  }
}

function parseDurationSeconds(d) {
  const m = /^(\d+)([smh])$/.exec(d);
  if (!m) return 30;
  const n = Number(m[1]);
  switch (m[2]) {
    case 's':
      return n;
    case 'm':
      return n * 60;
    case 'h':
      return n * 3600;
    default:
      return 30;
  }
}
