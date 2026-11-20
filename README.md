# Transaction Processing Platform

A double-entry ledger service for money transfers: **Java 21, Spring Boot 4.1, PostgreSQL 18, Kafka 4.1, Redis 8.**

Transfers arrive over REST or Kafka and are posted as balanced debit/credit entries in a single
Postgres transaction. Idempotency keys make retries and redeliveries safe. Posted transactions are
published through a transactional outbox to a risk engine. A reconciliation job matches the ledger
against settlement files from the payment network.

## Results

Measured on one shared Linux host (20 vCPU, NVMe, other workloads running, load generator on the
same machine). Method, caveats and raw output: [docs/results.md](docs/results.md),
[results/raw](results/raw).

| claim | measured |
|---|---|
| ACID transfer API sustaining 2,000 req/s | **2,500 transfers/s for 15 min** (2.25M postings): p50 5.4 ms, p95 10.0 ms, p99 19.5 ms, 0 errors, 0 dropped; ledger invariants hold afterwards. A 20-min run (3.0M postings) gave the same rate at p99 23.5 ms. |
| Kafka retries + idempotency keys, no duplicate postings across 100K events | **125,050 messages** (100,000 unique, 25,000 duplicate deliveries, 50 poison) with Postgres stopped for 15 s and the service SIGKILLed mid-stream: **100,000 postings, 0 duplicates, all 10,000 balances exact**, 50/50 poison dead-lettered, 0 valid messages lost. |
| Indexes + Redis cut p95 account-query latency 85 → 32 ms under load | **570 ms → 2.1 ms** at 200 reads/s + 1,000 writes/s (index; 3.3 ms with the cache in front). At 1,000 + 1,000 the old schema collapses (p95 4.7 s) and the new one holds 1.6 ms. Redis serves 82% of reads in read-heavy traffic. |
| Reconciliation + risk rules identify 99.5% of seeded anomalies | **3,199 / 3,200 (99.97%)** on four held-out seeds: 2,399/2,400 fraud cases across five typologies and 800/800 settlement breaks, with alerts on 0.058% of ordinary transfers. |

The cache does not lower p95 in write-heavy traffic (it adds ~1 ms on misses); see
[docs/results.md](docs/results.md#2-account-query-latency-indexes--redis).

## Architecture

```
  REST  POST /transfers                     Kafka  ledger.transfer-requests
  (Idempotency-Key header)                  (upstream payment channels)
          │                                          │  retries: unbounded back-off for DB outages,
          │                                          │  invalid messages -> .dlt
          ▼                                          ▼
  ┌──────────────────────────── ledger-service ─────────────────────────────┐
  │ PostingService: ONE Postgres transaction                                 │
  │   idempotency row (unique key) · lock both accounts in id order ·       │
  │   2 entries (debit + credit) · 2 balance updates · outbox row            │
  └───────┬────────────────────────────────────────┬────────────────────────┘
          │ OutboxRelay (FOR UPDATE SKIP LOCKED,    │ after commit: version-guarded
          │ delete only after Kafka ack)            │ cache invalidation
          ▼                                         ▼
  Kafka  ledger.transactions.posted               Redis  account summaries
          │
          ▼
  RiskEngine (event-time rules, history read     Settlement CSV ─► ReconciliationService
  from the ledger) ─► risk_alerts                                 ─► reconciliation_breaks
```

One deployable service, one database. Everything that must be atomic with a balance change
(idempotency record, entries, outbox event) lives in the same Postgres transaction; Kafka and Redis
sit outside it and are made safe by idempotent consumers and a cache that is never trusted for
posting decisions.

## How it works

**Posting.** `PostingService.transfer` checks the idempotency key, locks both accounts with one
`SELECT … ORDER BY id FOR UPDATE` (Postgres locks rows as they leave the sort, so every transfer locks
in the same order and transfers cannot deadlock each other), checks funds, inserts the transaction
with `ON CONFLICT (idempotency_key) DO NOTHING`, writes both entries with `balance_after`, updates both
balances in one statement and appends the outbox event. Customer balances also have a
`CHECK (balance >= 0)` as a last line of defence. Insufficient funds is a recorded outcome
(`REJECTED`, HTTP 422), not an error, so a retry returns the same answer.

**Idempotency.** The same key with the same body returns the original result with
`Idempotent-Replayed: true`; the same key with a different body is refused with 409. Two concurrent
requests with one key serialize on the account locks and the unique index; the loser replays the
winner's result. The key is the only dedup mechanism, so it covers every source of duplicates:
client retries, upstream re-sends, Kafka redelivery after a crash or rebalance.

**Kafka ingest and retries.** Offsets are committed after processing (at-least-once). Retries are
blocking so a partition keeps its order:

| failure | handling |
|---|---|
| Postgres unreachable, deadlock, serialization failure | retry with exponential back-off (200 ms → 10 s cap), no attempt limit — dead-lettering during an outage would drain the topic into the DLT |
| invalid message, unknown account, currency mismatch | straight to `ledger.transfer-requests.dlt` with the exception in headers |
| anything else, including statement/transaction timeouts | 4 retries, then DLT, so one record that can never finish cannot stall its partition |

Retries and dead-letters are counted (`ledger_kafka_retries_total`, `ledger_kafka_dead_lettered_total`).

**Account query.** `GET /accounts/{id}` returns the balance and the newest 20 entries from one
`LATERAL` query (one snapshot). V3 adds `entries (account_id, id DESC) INCLUDE (…)`, built
`CONCURRENTLY`, so the newest entries of a busy merchant account are read directly instead of
fetching and sorting all of them. Summaries are cached in Redis. Plain delete-on-write has a race
(a reader that loaded the old row can write it back after the delete), so every cache write goes
through a Lua script that only accepts a newer version: a posting writes a tombstone at
`2·version−1`, which rejects older summaries and accepts the first reader that sees the new version.
Redis errors are treated as misses.

**Risk rules.** Evaluated per posted transaction, in event time (`occurred_at`), with history read
from the ledger (so the consumer is stateless and a replayed stream gives the same alerts). Alerts
are unique per (rule, transaction), which makes redelivery harmless.

| rule | fires when (defaults in `application.yml`) |
|---|---|
| `LARGE_AMOUNT` | ≥ $25,000; or ≥ $2,500 and either < 5 prior transfers in 30 days or ≥ 10× the 30-day average (transfers that already raised an alert are left out of the average) |
| `VELOCITY` | ≥ 8 outgoing transfers within 10 minutes (one alert per burst) |
| `STRUCTURING` | ≥ 3 transfers within 10% below the $10,000 reporting threshold within 24 h |
| `ROUND_TRIP` | ≥ $500 sent to an account that sends it back within 1 h, amounts within 5% of the larger one |
| `DUPLICATE_PAYMENT` | same payer, payee and amount within 5 minutes under a different key |

Pairwise rules search both directions in time and always flag the later transaction, so the result
does not depend on which event is processed first.

**Reconciliation.** `POST /api/v1/reconciliations?from&to` with a settlement CSV
(`reference,amount,currency,value_date`) matches lines against posted transactions that crossed a
clearing (`SYSTEM`) account and records `MISSING_IN_LEDGER`, `MISSING_IN_SETTLEMENT`,
`AMOUNT_MISMATCH` and `DUPLICATE_SETTLEMENT` breaks. `GET /api/v1/ledger/integrity` checks the
double-entry invariants in one `REPEATABLE READ` snapshot: balances sum to zero per currency, every
balance equals the sum of its entries, every posted transaction has exactly two entries summing to 0.

## API

| | |
|---|---|
| `POST /api/v1/accounts` | `{ownerId, currency, type?}` → 201 |
| `GET /api/v1/accounts/{id}` | balance + 20 newest entries (cached) |
| `GET /api/v1/accounts/{id}/entries?beforeId&limit` | statement, keyset-paginated |
| `POST /api/v1/transfers` | header `Idempotency-Key`; `{fromAccountId, toAccountId, amount, currency, reference?, occurredAt?}` → 201 posted / 422 rejected / 409 key reused |
| `GET /api/v1/transfers/{id}` | |
| `GET /api/v1/risk/alerts?accountId` | |
| `POST /api/v1/reconciliations?from&to` | `text/csv` settlement file → report with breaks |
| `GET /api/v1/ledger/integrity` | invariant check |
| `/actuator/health`, `/actuator/prometheus` | |

Amounts are integer minor units (cents). Errors are RFC 9457 problem details.

Kafka message on `ledger.transfer-requests` (key: source account id):

```json
{"idempotencyKey":"pay-42","fromAccountId":"…","toAccountId":"…","amount":1250,"currency":"USD","reference":"ext-ref","occurredAt":"2026-11-20T10:15:00Z"}
```

## Running it

```bash
docker compose up -d --build --wait
```

```bash
curl -s localhost:8080/api/v1/ledger/integrity
```

Tests (unit + integration against real Postgres, Kafka and Redis via Testcontainers; needs Docker):

```bash
mvn verify
```

## Reproducing the benchmarks

Build the load tool once with `mvn -q -f bench/pom.xml package`. Load is generated with k6 in
Docker (`scripts/k6.sh`).

```bash
docker compose up -d --build --wait
java -jar bench/target/bench.jar seed --customers 10000 --merchants 1000
RATE=2500 DURATION=20m NAME=history scripts/bench-throughput.sh   # 3M transfers of history
READ_RATE=200 scripts/bench-account-queries.sh                    # baseline -> index -> index + cache
RATE=2500 DURATION=15m scripts/bench-throughput.sh                # final configuration
scripts/bench-kafka-chaos.sh                                      # 125K messages, Postgres outage + SIGKILL
scripts/bench-risk.sh                                             # seeded anomalies, seeds 1-5
```

## Limitations

- A single clearing account is a hot row: every deposit locks it, which caps deposits at roughly
  the commit rate of one row. Real deployments shard clearing accounts.
- Several outbox relays can run safely (`SKIP LOCKED`), but then per-account event order across
  batches is not guaranteed; the risk rules use event time, so they do not depend on it.
- Dead-lettered messages are replayed with standard Kafka tooling; there is no replay endpoint.
- Authentication and rate limiting are expected at the gateway and are not part of this service.
