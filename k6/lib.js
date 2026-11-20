import { SharedArray } from 'k6/data';

export const BASE = __ENV.BASE_URL || 'http://localhost:8080';

const lines = (path) => open(path).split('\n').map((s) => s.trim()).filter((s) => s.length > 0);
export const customers = new SharedArray('customers', () => lines('/data/customers.txt'));
export const merchants = new SharedArray('merchants', () => lines('/data/merchants.txt'));

// Merchant popularity is Zipf-distributed (s = 0.9): a few merchants receive most payments,
// as in real card traffic. The same distribution drives which accounts get read.
const ZIPF_S = 0.9;
const cdf = (() => {
  const out = [];
  let total = 0;
  for (let k = 1; k <= merchants.length; k++) {
    total += 1 / Math.pow(k, ZIPF_S);
    out.push(total);
  }
  return out.map((v) => v / total);
})();

export function zipfMerchant() {
  const x = Math.random();
  let lo = 0;
  let hi = cdf.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cdf[mid] < x) lo = mid + 1; else hi = mid;
  }
  return merchants[lo];
}

export function randomCustomer() {
  return customers[Math.floor(Math.random() * customers.length)];
}

/** 60% card payments to merchants, 40% person-to-person. */
export function transferBody() {
  const from = randomCustomer();
  let to = Math.random() < 0.6 ? zipfMerchant() : randomCustomer();
  while (to === from) to = randomCustomer();
  const amount = 100 + Math.floor(Math.random() * 19900);
  return JSON.stringify({ fromAccountId: from, toAccountId: to, amount, currency: 'USD' });
}

export function summaryJson(data, name) {
  const pick = (metric) => (data.metrics[metric] ? data.metrics[metric].values : undefined);
  const out = {};
  for (const key of Object.keys(data.metrics)) {
    if (key.startsWith('http_req_duration') || key.startsWith('http_reqs') || key.startsWith('http_req_failed')
        || key.startsWith('iterations') || key.startsWith('dropped_iterations') || key.startsWith('checks')) {
      out[key] = pick(key);
    }
  }
  return { [`/results/${name}.json`]: JSON.stringify({ name, env: { RATE: __ENV.RATE, DURATION: __ENV.DURATION, READ_RATE: __ENV.READ_RATE, WRITE_RATE: __ENV.WRITE_RATE }, metrics: out }, null, 2) };
}
