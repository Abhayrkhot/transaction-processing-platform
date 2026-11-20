// Account reads (balance + recent activity) while transfers keep posting. Reads follow the
// same popularity skew as payments: busy merchant accounts are read most and also have the
// most entries, which is what makes the unindexed query slow.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { BASE, transferBody, zipfMerchant, randomCustomer, summaryJson } from './lib.js';

const RUN = __ENV.RUN || `${Date.now()}`;
const DURATION = __ENV.DURATION || '5m';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    writes: {
      executor: 'constant-arrival-rate', exec: 'write', rate: Number(__ENV.WRITE_RATE || 1000), timeUnit: '1s',
      duration: DURATION, preAllocatedVUs: 200, maxVUs: 1500,
    },
    reads: {
      executor: 'constant-arrival-rate', exec: 'read', rate: Number(__ENV.READ_RATE || 1000), timeUnit: '1s',
      duration: DURATION, preAllocatedVUs: 200, maxVUs: 1500,
    },
  },
  thresholds: {
    'http_req_duration{scenario:reads}': ['p(95)<10000'],
    'http_req_duration{scenario:writes}': ['p(95)<10000'],
    'http_req_failed{scenario:reads}': ['rate<0.001'],
    'http_req_failed{scenario:writes}': ['rate<0.001'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function write() {
  const key = `${RUN}-${exec.vu.idInTest}-${exec.vu.iterationInInstance}`;
  const res = http.post(`${BASE}/api/v1/transfers`, transferBody(), {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
  });
  check(res, { posted: (r) => r.status === 201 });
}

export function read() {
  const id = Math.random() < 0.6 ? zipfMerchant() : randomCustomer();
  const res = http.get(`${BASE}/api/v1/accounts/${id}`, { tags: { name: 'GET /accounts/{id}' } });
  check(res, { ok: (r) => r.status === 200 });
}

export function handleSummary(data) {
  return summaryJson(data, __ENV.NAME || 'account-queries');
}
