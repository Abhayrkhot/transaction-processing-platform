package io.github.abhayrkhot.ledger.transfer;

import static io.github.abhayrkhot.ledger.common.Jdbc.instant;
import static io.github.abhayrkhot.ledger.common.Jdbc.ts;
import static io.github.abhayrkhot.ledger.common.Jdbc.uuid;

import java.sql.Types;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class TransactionRepository {

    private static final String COLUMNS = "id, idempotency_key, request_hash, from_account_id, to_account_id, "
            + "amount, currency, status, reject_reason, reference, occurred_at, posted_at";

    static final RowMapper<Transaction> TRANSACTION = (rs, i) -> new Transaction(
            uuid(rs, "id"),
            rs.getString("idempotency_key"),
            rs.getString("request_hash"),
            uuid(rs, "from_account_id"),
            uuid(rs, "to_account_id"),
            rs.getLong("amount"),
            rs.getString("currency"),
            Transaction.Status.valueOf(rs.getString("status")),
            rs.getString("reject_reason"),
            rs.getString("reference"),
            instant(rs, "occurred_at"),
            instant(rs, "posted_at"));

    private final JdbcClient jdbc;

    TransactionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Transaction> findByIdempotencyKey(String key) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM transactions WHERE idempotency_key = :key")
                .param("key", key)
                .query(TRANSACTION)
                .optional();
    }

    public Optional<Transaction> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM transactions WHERE id = :id")
                .param("id", id)
                .query(TRANSACTION)
                .optional();
    }

    /** Returns empty when another transaction already committed the same idempotency key. */
    public Optional<Transaction> insert(
            TransferCommand cmd, String requestHash, Instant occurredAt, Transaction.Status status, String rejectReason) {
        return jdbc.sql("""
                        INSERT INTO transactions (idempotency_key, request_hash, from_account_id, to_account_id,
                                                  amount, currency, status, reject_reason, reference, occurred_at)
                        VALUES (:key, :hash, :from, :to, :amount, :currency, :status, :reason, :reference, :occurredAt)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        RETURNING\s""" + COLUMNS)
                .param("key", cmd.idempotencyKey())
                .param("hash", requestHash)
                .param("from", cmd.fromAccountId())
                .param("to", cmd.toAccountId())
                .param("amount", cmd.amount())
                .param("currency", cmd.currency())
                .param("status", status.name())
                .param("reason", rejectReason, Types.VARCHAR)
                .param("reference", cmd.reference(), Types.VARCHAR)
                .param("occurredAt", ts(occurredAt))
                .query(TRANSACTION)
                .optional();
    }

    /** Writes the debit and credit legs of a posted transaction in one round trip. */
    public void insertEntries(UUID transactionId, UUID from, long fromBalanceAfter, UUID to, long toBalanceAfter, long amount) {
        jdbc.sql("""
                        INSERT INTO entries (transaction_id, account_id, amount, balance_after)
                        VALUES (:txn, :from, :debit, :fromBalance), (:txn, :to, :credit, :toBalance)
                        """)
                .param("txn", transactionId)
                .param("from", from)
                .param("debit", -amount)
                .param("fromBalance", fromBalanceAfter)
                .param("to", to)
                .param("credit", amount)
                .param("toBalance", toBalanceAfter)
                .update();
    }
}
