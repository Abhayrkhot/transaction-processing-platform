-- The account query reads the newest entries of one account. With only the FK index,
-- Postgres fetches every entry of the account and sorts them; hot merchant accounts hold
-- hundreds of thousands of entries. This index returns the newest N rows directly, and the
-- INCLUDE columns let it answer without visiting the heap once pages are all-visible.
--
-- CONCURRENTLY keeps writes flowing while the index builds; Flyway runs this file outside
-- a transaction because every statement in it is non-transactional.
--
-- An interrupted CONCURRENTLY build leaves an INVALID index behind; drop any leftover so a
-- rerun builds it again instead of skipping it and then dropping the only usable index.
DROP INDEX CONCURRENTLY IF EXISTS entries_account_recent_idx;

CREATE INDEX CONCURRENTLY entries_account_recent_idx
    ON entries (account_id, id DESC)
    INCLUDE (transaction_id, amount, balance_after, posted_at);

-- Superseded by the composite index (same leading column).
DROP INDEX CONCURRENTLY IF EXISTS entries_account_id_idx;
