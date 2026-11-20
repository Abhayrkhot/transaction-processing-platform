-- Event-time lookups for the risk rules and reference lookups for reconciliation.
CREATE INDEX transactions_from_occurred_idx ON transactions (from_account_id, occurred_at);
CREATE INDEX transactions_to_occurred_idx ON transactions (to_account_id, occurred_at);
CREATE INDEX transactions_reference_idx ON transactions (reference) WHERE reference IS NOT NULL;
CREATE INDEX transactions_occurred_idx ON transactions (occurred_at);

CREATE TABLE risk_alerts (
    id                      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule                    text        NOT NULL,
    account_id              uuid        NOT NULL REFERENCES accounts (id),
    transaction_id          uuid        NOT NULL REFERENCES transactions (id),
    related_transaction_id  uuid        REFERENCES transactions (id),
    details                 text        NOT NULL,
    occurred_at             timestamptz NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    -- Redelivered events re-run the rules; this makes alerting idempotent.
    CONSTRAINT risk_alerts_rule_txn_uq UNIQUE (rule, transaction_id)
);
CREATE INDEX risk_alerts_account_idx ON risk_alerts (account_id, rule, occurred_at);

CREATE TABLE reconciliation_runs (
    id                uuid        PRIMARY KEY DEFAULT uuidv7(),
    window_start      timestamptz NOT NULL,
    window_end        timestamptz NOT NULL,
    settlement_lines  integer     NOT NULL,
    ledger_records    integer     NOT NULL,
    matched           integer     NOT NULL,
    breaks            integer     NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reconciliation_breaks (
    id                 bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id             uuid    NOT NULL REFERENCES reconciliation_runs (id),
    type               text    NOT NULL CHECK (type IN ('MISSING_IN_LEDGER', 'MISSING_IN_SETTLEMENT',
                                                        'AMOUNT_MISMATCH', 'DUPLICATE_SETTLEMENT')),
    reference          text    NOT NULL,
    transaction_id     uuid,
    ledger_amount      bigint,
    settlement_amount  bigint
);
CREATE INDEX reconciliation_breaks_run_idx ON reconciliation_breaks (run_id);
