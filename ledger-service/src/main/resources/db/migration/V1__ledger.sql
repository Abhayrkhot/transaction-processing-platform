-- Double-entry ledger. Amounts are signed minor units (cents); a transfer writes one
-- debit entry and one credit entry that sum to zero.

CREATE TABLE accounts (
    id          uuid        PRIMARY KEY DEFAULT uuidv7(),
    owner_id    text        NOT NULL,
    type        text        NOT NULL CHECK (type IN ('CUSTOMER', 'SYSTEM')),
    currency    char(3)     NOT NULL,
    balance     bigint      NOT NULL DEFAULT 0,
    status      text        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    version     bigint      NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    -- SYSTEM accounts (clearing, settlement) represent the outside world and may go negative.
    CONSTRAINT customer_balance_non_negative CHECK (type = 'SYSTEM' OR balance >= 0)
);

CREATE TABLE transactions (
    id               uuid        PRIMARY KEY DEFAULT uuidv7(),
    idempotency_key  text        NOT NULL,
    request_hash     text        NOT NULL,
    from_account_id  uuid        NOT NULL REFERENCES accounts (id),
    to_account_id    uuid        NOT NULL REFERENCES accounts (id),
    amount           bigint      NOT NULL CHECK (amount > 0),
    currency         char(3)     NOT NULL,
    status           text        NOT NULL CHECK (status IN ('POSTED', 'REJECTED')),
    reject_reason    text,
    reference        text,
    occurred_at      timestamptz NOT NULL,
    posted_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT transactions_idempotency_key_uq UNIQUE (idempotency_key),
    CONSTRAINT transactions_distinct_accounts CHECK (from_account_id <> to_account_id)
);

CREATE TABLE entries (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id  uuid        NOT NULL REFERENCES transactions (id),
    account_id      uuid        NOT NULL REFERENCES accounts (id),
    amount          bigint      NOT NULL CHECK (amount <> 0),
    balance_after   bigint      NOT NULL,
    posted_at       timestamptz NOT NULL DEFAULT now()
);

-- Foreign-key indexes only; query-specific indexes come in V3.
CREATE INDEX entries_account_id_idx ON entries (account_id);
CREATE INDEX entries_transaction_id_idx ON entries (transaction_id);

-- Transactional outbox: rows are written in the posting transaction and relayed to Kafka.
CREATE TABLE outbox (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic       text        NOT NULL,
    message_key text        NOT NULL,
    payload     text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
