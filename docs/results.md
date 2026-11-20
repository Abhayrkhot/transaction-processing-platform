# Results

All numbers come from the scripts in `scripts/` against the Docker Compose stack in this repo.
Raw k6 summaries, verifier output and logs are in [`results/raw`](../results/raw).

**Host.** One Linux machine (20 vCPU, 61 GB RAM, NVMe), shared with other workloads (other
projects' kind clusters and databases). Load average during runs was typically 10–25. Postgres runs
with `synchronous_commit=on` (every commit is fsynced) and `shared_buffers=2GB`. The ledger runs with
a 2 GB heap and a 40-connection pool. k6 runs on the same host, so the client competes for CPU too.

**Data set.** 10,000 customers and 1,000 merchants, then 3.0M transfers from the history run.
Merchant popularity is Zipf (s = 0.9) and 60% of payments go to merchants, so the busiest merchant
account holds ~180K entries. In total: 3.2M transactions and 6.3M entries (2.3 GB).

## 1. Transfer API throughput

`scripts/bench-throughput.sh` — k6 `constant-arrival-rate` (open model: k6 starts the configured
number of requests every second whatever the latency, so an overloaded server shows up as dropped
iterations instead of a quietly lower rate). Every request is a `POST /api/v1/transfers` with a unique
`Idempotency-Key`: 60% card payments to Zipf-popular merchants, 40% person-to-person. Each one is a
full ACID posting (idempotency row, two row locks, two entries, two balance updates, outbox row,
fsynced commit), and every posted transfer also flows through the outbox to the risk consumer.

| run | transfers | achieved rate | p50 | p95 | p99 | p99.9 | errors | dropped |
|---|---|---|---|---|---|---|---|---|
| final config (V3 index + cache), 15 min | 2,250,001 | 2,500.0 /s | 5.4 ms | 10.0 ms | 19.5 ms | 85.8 ms | 0 | 0 |
| history build (V2 schema), 20 min | 3,000,001 | 2,500.0 /s | 5.3 ms | 10.5 ms | 23.5 ms | 52.4 ms | 0 | 0 |

After the final run, `GET /api/v1/ledger/integrity` over the whole ledger: balances sum to 0, every
balance equals the sum of its entries, every posted transaction has two entries summing to 0
(`results/raw/throughput-final-2500rps-15m-integrity.json`). After every benchmark in this document the
ledger held 8.2M transactions and 16.4M entries (7.8 GB) and the check still passed.

The ceiling was not searched for; 2,500/s was the target. Seeding funds every customer from a single
clearing account, which serialises on that one row (~840 transfers/s); hot rows are the real limit of
this design (see Limitations in the README).


## 2. Account query latency (indexes + Redis)

`scripts/bench-account-queries.sh` — `GET /api/v1/accounts/{id}` (balance + 20 newest entries) while
transfers keep posting at a constant rate. Reads pick a merchant (60%, same Zipf skew as payments) or a
random customer (40%). Each phase sets the index set explicitly and restarts the ledger:

- **baseline** — entries indexed by `account_id` only (the V2 index set), cache off
- **indexed** — V3 `entries (account_id, id DESC) INCLUDE (transaction_id, amount, balance_after, posted_at)`, cache off
- **indexed-cached** — V3 index + Redis cache

**Same load for every phase: 200 reads/s + 1,000 writes/s, 5 min each.** 200 reads/s is the highest
rate the baseline sustains (sweep below); above it the baseline saturates and its "latency" is
queueing.

| phase | read p50 | read p95 | read p99 | write p95 | cache hit rate |
|---|---|---|---|---|---|
| baseline | 12.0 ms | **570 ms** | 881 ms | 79.9 ms | – |
| indexed | 0.93 ms | **2.14 ms** | 2.66 ms | 7.6 ms | – |
| indexed-cached | 1.26 ms | **3.34 ms** | 5.30 ms | 9.0 ms | 12% |

Baseline sweep (2 min each, 1,000 writes/s): 100 reads/s → p95 310 ms; 200 → 447 ms; 400 → collapse
(p95 6.1 s, 12K reads and 77K writes dropped).

At **1,000 reads/s + 1,000 writes/s** the baseline collapses (read p95 4.7 s, 366K of 600K iterations
dropped) while the indexed configuration holds the load at read p95 1.6 ms (2.8 ms with the cache).

**What the cache does and does not do.** With the index, Postgres answers this query in about a
millisecond, so the cache cannot make p95 much faster; on a miss it adds a Redis round trip and a
guarded write, so in write-heavy mixes (where postings keep invalidating hot accounts) p95 is about
1 ms worse. Its value is taking reads off the primary in read-heavy traffic:

| 3,000 reads/s + 300 writes/s, 3 min | read p50 | read p95 | read p99 | reads served by Postgres |
|---|---|---|---|---|
| indexed | 0.84 ms | 1.96 ms | 3.38 ms | 100% |
| indexed-cached | 0.57 ms | 2.34 ms | 5.32 ms | **18%** (82.3% hit rate) |

The cached read-heavy run dropped 1,787 of 540K read iterations (0.3%) versus 16 without the cache.
The Postgres counters captured per phase (`*-db.json`) are not used here because the risk consumer's
queries are mixed into them.


## 3. Kafka ingest: retries and idempotency

`scripts/bench-kafka-chaos.sh` — `bench kafka-load` produces **100,000 unique transfer requests**
between 10,000 customers, **25,000 duplicate deliveries** (re-sends of random earlier requests with the
same key and body, up to ~10 s later, as an upstream retry would) and **50 poison messages**
(malformed JSON, unknown account, negative amount): 125,050 messages at 2,000/s. While they are
consumed:

| time | fault |
|---|---|
| t+10 s | Postgres stopped for 15 s (connections terminated, new ones refused) |
| t+40 s | ledger container SIGKILLed, restarted 5 s later |

`bench kafka-verify` waits for consumer lag 0 and then checks the effects, not the logs:

| check | result |
|---|---|
| transactions for the run's 100,000 keys | **100,000**, all POSTED, 200,000 entries |
| duplicate postings | **0** |
| account balances vs. exactly-once expectation (all 10,000 accounts) | **10,000 / 10,000 exact** |
| poison messages in `ledger.transfer-requests.dlt` | **50 / 50** |
| valid messages dead-lettered | **0** |
| ledger integrity | ok |

From the service's counters (`results/raw/kafka-kafka-chaos-metrics-*.txt`, scraped before the kill
and at the end): during the outage **24 deliveries on the ingest topic failed** (no database connection)
and were retried with back-off until Postgres returned — none dead-lettered — plus 24 on the risk
consumer and failed outbox relay ticks that also recovered. **25,508 deliveries were absorbed as
idempotent replays**: the 25,000 injected duplicates plus ~500 records redelivered after the SIGKILL
(their offsets had not been committed). The whole backlog drained 48 s after the producer finished.

An earlier run (`kafka-final`) used the opposite order (SIGKILL, then the outage) and also verified
exactly-once balances, but its outage fell inside the 45 s window in which Kafka still assigned the
partitions to the killed consumers, so the listener retry path was not exercised; the script was
changed to the order above.


## 4. Risk rules and reconciliation on seeded anomalies

`scripts/bench-risk.sh` — `bench risk-stream` generates 14 days of activity for 2,000 customers and
200 merchants (Poisson arrivals at 1–6 transfers/day, log-normal amounts around a per-customer median
of $8–$120, favourite merchants, friends, rent, weekly payroll deposits and cash withdrawals through
the clearing account), about 114K transfers per seed. Into it go **600 labelled fraud cases** (120 of
each typology) and a settlement file for the clearing transfers with **200 labelled breaks**:

| typology | how it is generated |
|---|---|
| account takeover | one transfer of $3,000–$20,000 to a mule account or cashed out |
| card testing | 10–30 payments of $1–$20 over 1–10 minutes |
| structuring | 3–6 transfers of $9,000–$9,990 spread over 1–22 hours |
| round trip | $600–$8,000 sent and 95–100% of it returned within 1–55 minutes |
| duplicate payment | the same payment submitted again 3 s–4 min later |
| settlement breaks | 50 each: line missing, wrong amount, line duplicated, line with no ledger transaction |

The stream goes through the real pipeline (Kafka ingest → posting → outbox → risk consumer) with
event-time `occurredAt`; reconciliation runs over the settlement file through the API. Labels stay in
`ground_truth.csv`. A fraud case counts as identified when an alert **of its rule** fired on one of its
transactions; a settlement break when a break of the same type was reported for its reference. Victims
are drawn at random, so cases overlap with each other and with ordinary activity.

**Seed 1 was the only seed used for tuning.** Its three misses were investigated and led to two rule
fixes, each with a regression test that fails on the old rule:

- *Round-trip tolerance was relative to the leg being evaluated.* A return of 95.03% matched when the
  outbound leg was scored but not when the return leg was, and the outbound leg is scored before the
  return exists. The tolerance is now relative to the larger leg (2 misses).
- *Earlier fraud inflated the large-amount baseline.* An account hit by structuring the day before had
  a 30-day average of $3,438 (median $33), hiding a $19,722 drain. Transfers that already raised an
  alert are now excluded from the baseline (1 miss).

| seeds | fraud cases identified | settlement breaks identified | overall | alerts on ordinary transfers |
|---|---|---|---|---|
| seed 1 (tuning, before the fixes) | 597 / 600 (99.50%) | 200 / 200 | 797 / 800 (**99.63%**) | 64 of 110,421 (0.058%) |
| **seeds 2–5 (held out, after the fixes)** | **2,399 / 2,400 (99.96%)** | **800 / 800 (100%)** | **3,199 / 3,200 (99.97%)** | 255 of 441,831 (0.058%) |

The one held-out miss (seed 3, `STRUCTURING-6`): the same account was the victim of two overlapping
structuring cases within 24 hours, and the rule raises one structuring alert per account per window, so
case 110's alert covered both and case 6 got none of its own. All four of its transfers were still
flagged (`LARGE_AMOUNT`). No transfer in any risk run was rejected for insufficient funds, and no
reconciliation run reported a break that was not seeded.

Per-run detail, including the list of missed case ids: `results/raw/risk-risk-s*.json`.


## 5. Rerun on a fresh stack

The whole suite was run again the same evening (21:03–22:11) on a fresh database: same code
(`16477dc`), same scripts, same order, results under `results/raw/*rerun*` and
`results/raw/risk-risk-s*-17952*.json`. A 30-second load sample (`rerun-load.log`) starts at 21:24.

| measurement | first run | rerun |
|---|---|---|
| throughput, final config, 2,500/s × 15 min | p99 19.5 ms, 0 errors, 0 dropped | **p50 5.3, p95 9.0, p99 15.3, p99.9 42.7 ms, 0 errors, 0 dropped** |
| account query p95 @ 200 r/s + 1,000 w/s: baseline | 570 ms | 158 ms (p99 523 ms) |
| … composite index | 2.14 ms | 2.09 ms |
| … index + Redis | 3.34 ms | 2.69 ms |
| read-heavy 3,000 r/s + 300 w/s: p95 index / index + Redis | 1.96 / 2.34 ms | 1.87 / 2.37 ms |
| … cache hit rate | 82.3% | 82.3% |
| Kafka chaos: postings / duplicates / wrong balances | 100,000 / 0 / 0 | 100,000 / 0 / 0 |
| … poison dead-lettered / valid dead-lettered | 50 / 0 | 50 / 0 |
| … ingest deliveries retried during the outage | 24 | 24 |
| seeded anomalies, held-out seeds 2–5 | 3,199 / 3,200 | 3,199 / 3,200 (same miss) |
| seeded anomalies, seed 1 | 797 / 800 (before the rule fixes) | 800 / 800 |

Everything on the optimised path reproduced within noise. The streams are deterministic per seed, so
seeds 2–5 give identical results (alerts on ordinary transfers 63 / 57 / 74 / 61 both times); across
all five seeds after the rule fixes: **3,999 / 4,000 (99.975%)**. The baseline query is the noisy number:
its plan sorts every entry of a hot merchant account, so it moves with that account's depth, the
buffer cache and host load. The ~1,750 dropped reads in the cached read-heavy phase also recurred
(1,740 vs 1,787), so that is a property of the cache path, not noise.

**The 20-minute history run did not hold the rate the second time:** 2,938,509 transfers at
2,448.7/s, p50 7.0 ms but p95 917 ms and p99 1,087 ms, 61,493 iterations dropped, 0 errors. During it
another project's container was compiling C++ at ~590% CPU; when the run ended the load average was
26–30 on 20 cores with CPU pressure ("some") at ~25%, against 17–20 and no build during the first
run's history phase. The 15-minute run with the same code and configuration, after that build had
finished (load ~7–11), was the best of all runs (p99 15.3 ms). With every core busy, Postgres
backends that hold account row locks get descheduled and requests queue behind them; latency
degrades sharply but nothing is lost — 0 errors, and the ledger integrity check passed after the
15-minute run and again at the end (6.9M transactions, 6.7 GB). The history run itself has no
integrity file because `bench-throughput.sh` stopped at the failed k6 latency threshold; it now runs
the check regardless.
