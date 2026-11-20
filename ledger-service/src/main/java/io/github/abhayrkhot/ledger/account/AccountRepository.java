package io.github.abhayrkhot.ledger.account;

import static io.github.abhayrkhot.ledger.common.Jdbc.instant;
import static io.github.abhayrkhot.ledger.common.Jdbc.uuid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class AccountRepository {

    private static final String COLUMNS =
            "id, owner_id, type, currency, balance, status, version, created_at, updated_at";

    private static final RowMapper<Account> ACCOUNT = (rs, i) -> new Account(
            uuid(rs, "id"),
            rs.getString("owner_id"),
            Account.Type.valueOf(rs.getString("type")),
            rs.getString("currency"),
            rs.getLong("balance"),
            Account.Status.valueOf(rs.getString("status")),
            rs.getLong("version"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<AccountSummary.Entry> ENTRY = (rs, i) -> entry(rs);

    private final JdbcClient jdbc;

    AccountRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Account insert(String ownerId, Account.Type type, String currency) {
        return jdbc.sql("INSERT INTO accounts (owner_id, type, currency) VALUES (:owner, :type, :currency) RETURNING " + COLUMNS)
                .param("owner", ownerId)
                .param("type", type.name())
                .param("currency", currency)
                .query(ACCOUNT)
                .single();
    }

    public Optional<Account> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM accounts WHERE id = :id")
                .param("id", id)
                .query(ACCOUNT)
                .optional();
    }

    /**
     * Locks both accounts of a transfer. Postgres locks rows as they leave the sort, so every
     * transfer takes its two locks in id order and transfers cannot deadlock on each other.
     * NO KEY UPDATE is the lock the balance UPDATE takes anyway; unlike FOR UPDATE it does not
     * conflict with the KEY SHARE locks that foreign-key checks (entries, risk_alerts) take.
     */
    public List<Account> lockForTransfer(UUID a, UUID b) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM accounts WHERE id IN (:a, :b) ORDER BY id FOR NO KEY UPDATE")
                .param("a", a)
                .param("b", b)
                .query(ACCOUNT)
                .list();
    }

    /** Applies both legs of a posting in one statement; the caller holds both row locks. */
    public void applyTransfer(UUID from, UUID to, long amount) {
        int updated = jdbc.sql("""
                        UPDATE accounts
                           SET balance = balance + CASE WHEN id = :from THEN -:amount ELSE :amount END,
                               version = version + 1,
                               updated_at = now()
                         WHERE id IN (:from, :to)
                        """)
                .param("from", from)
                .param("to", to)
                .param("amount", amount)
                .update();
        if (updated != 2) {
            throw new IllegalStateException("expected to update 2 accounts, updated " + updated);
        }
    }

    /** Account row and newest entries from one statement, so they share one snapshot. */
    public Optional<AccountSummary> summary(UUID id, int recentEntries) {
        return jdbc.sql("""
                        SELECT a.id, a.owner_id, a.type, a.currency, a.balance, a.status, a.version, a.updated_at,
                               e.id AS entry_id, e.transaction_id, e.amount, e.balance_after, e.posted_at
                          FROM accounts a
                          LEFT JOIN LATERAL (
                                SELECT id, transaction_id, amount, balance_after, posted_at
                                  FROM entries
                                 WHERE account_id = a.id
                                 ORDER BY id DESC
                                 LIMIT :limit) e ON true
                         WHERE a.id = :id
                         ORDER BY e.id DESC
                        """)
                .param("id", id)
                .param("limit", recentEntries)
                .query((ResultSetExtractor<Optional<AccountSummary>>) AccountRepository::readSummary);
    }

    public List<AccountSummary.Entry> entries(UUID accountId, Long beforeId, int limit) {
        return jdbc.sql("""
                        SELECT id AS entry_id, transaction_id, amount, balance_after, posted_at
                          FROM entries
                         WHERE account_id = :account AND id < :before
                         ORDER BY id DESC
                         LIMIT :limit
                        """)
                .param("account", accountId)
                .param("before", beforeId == null ? Long.MAX_VALUE : beforeId)
                .param("limit", limit)
                .query(ENTRY)
                .list();
    }

    private static Optional<AccountSummary> readSummary(ResultSet rs) throws SQLException {
        if (!rs.next()) {
            return Optional.empty();
        }
        UUID id = uuid(rs, "id");
        String owner = rs.getString("owner_id");
        Account.Type type = Account.Type.valueOf(rs.getString("type"));
        String currency = rs.getString("currency");
        long balance = rs.getLong("balance");
        Account.Status status = Account.Status.valueOf(rs.getString("status"));
        long version = rs.getLong("version");
        var updatedAt = instant(rs, "updated_at");
        List<AccountSummary.Entry> entries = new ArrayList<>();
        do {
            rs.getLong("entry_id");
            if (!rs.wasNull()) {
                entries.add(entry(rs));
            }
        } while (rs.next());
        return Optional.of(new AccountSummary(id, owner, type, currency, balance, status, version, updatedAt, entries));
    }

    private static AccountSummary.Entry entry(ResultSet rs) throws SQLException {
        return new AccountSummary.Entry(
                rs.getLong("entry_id"),
                uuid(rs, "transaction_id"),
                rs.getLong("amount"),
                rs.getLong("balance_after"),
                instant(rs, "posted_at"));
    }
}
