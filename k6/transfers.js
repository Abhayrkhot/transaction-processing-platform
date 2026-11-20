// Sustained transfer posting at a constant arrival rate (open model: k6 starts RATE
// requests per second no matter how slowly the server answers, so a slow server shows up
// as dropped_iterations and latency instead of a quietly lower request rate).
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { BASE, transferBody, summaryJson } from './lib.js';

const RUN = __ENV.RUN || `${Date.now()}`;

export const options = {
  discardResponseBodies: true,
  scenarios: {
    transfers: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 2500),
      timeUnit: '1s',
      duration: __ENV.DURATION || '10m',
      preAllocatedVUs: 400,
      maxVUs: 2000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    'http_req_duration{expected_response:true}': ['p(99)<500'],
    dropped_iterations: ['count<1'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)', 'max'],
};

export default function () {
  const key = `${RUN}-${exec.vu.idInTest}-${exec.vu.iterationInInstance}`;
  const res = http.post(`${BASE}/api/v1/transfers`, transferBody(), {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
  });
  check(res, { posted: (r) => r.status === 201 });
}

export function handleSummary(data) {
  return { ...summaryJson(data, __ENV.NAME || 'transfers'), stdout: JSON.stringify(data.metrics.http_reqs.values) + '\n' };
}
