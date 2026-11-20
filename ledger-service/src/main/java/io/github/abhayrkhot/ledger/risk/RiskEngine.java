package io.github.abhayrkhot.ledger.risk;

import static io.github.abhayrkhot.ledger.common.Jdbc.instant;
import static io.github.abhayrkhot.ledger.common.Jdbc.ts;
import static io.github.abhayrkhot.ledger.common.Jdbc.uuid;

import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.transfer.TransactionPostedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Rules over posted transactions. Windows are in event time ({@code occurred_at}), so a
 * replayed or delayed stream produces the same alerts as a live one. History comes from
 * the ledger itself rather than consumer memory, which keeps the consumer stateless: a
 * restart or rebalance loses nothing, and redelivered events hit the (rule, transaction)
 * unique key instead of raising a second alert.
 *
 * <p>Pairwise rules (round trip, duplicate payment) search both directions in time and
 * always flag the later transaction of the pair, so the result does not depend on which
 * of the two events is evaluated first.
 */
@Service
public class RiskEngine {

    public enum Rule { LARGE_AMOUNT, VELOCITY, STRUCTURING, ROUND_TRIP, DUPLICATE_PAYMENT }

    record Alert(Rule rule, UUID accountId, UUID transactionId, UUID relatedTransactionId, String details, Instant occurredAt) {}

    private record Match(UUID id, UUID fromAccountId, Instant occurredAt) {}

    private final JdbcClient jdbc;
    private final RiskProperties rules;
    private final MeterRegistry meters;

    RiskEngine(JdbcClient jdbc, RiskProperties rules, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.rules = rules;
        this.meters = meters;
    }

    @Transactional(timeout = 10)
    public List<Alert> evaluate(TransactionPostedEvent e) {
        if (e.fromAccountType() == Account.Type.SYSTEM) {
            return List.of(); // deposits from clearing accounts are reconciled, not scored
        }
        List<Alert> alerts = new ArrayList<>();
        largeAmount(e).ifPresent(alerts::add);
        velocity(e).ifPresent(alerts::add);
        structuring(e).ifPresent(alerts::add);
        if (e.toAccountType() == Account.Type.CUSTOMER) {
            roundTrip(e).ifPresent(alerts::add);
        }
        duplicatePayment(e).ifPresent(alerts::add);
        alerts.forEach(this::save);
        return alerts;
    }

    /** Far above what the account normally sends, or a large amount from an account with little history. */
    Optional<Alert> largeAmount(TransactionPostedEvent e) {
        var p = rules.largeAmount();
        if (e.amount() >= p.absoluteLimit()) {
            return alert(Rule.LARGE_AMOUNT, e, null, "amount " + e.amount() + " >= absolute limit " + p.absoluteLimit());
        }
        if (e.amount() < p.minAmount()) {
            return Optional.empty();
        }
        // Transfers that already raised an alert are not "normal" spend; leaving them in would let
        // an earlier attack (e.g. structuring) raise the average enough to hide the next one.
        var history = jdbc.sql("""
                        SELECT count(*) AS n, coalesce(avg(amount), 0) AS avg
                          FROM transactions t
                         WHERE t.from_account_id = :account AND t.status = 'POSTED'
                           AND t.occurred_at >= :since AND t.occurred_at < :at AND t.id <> :txn
                           AND NOT EXISTS (SELECT 1 FROM risk_alerts a
                                            WHERE a.account_id = :account AND a.transaction_id = t.id)
                        """)
                .param("account", e.fromAccountId())
                .param("since", ts(e.occurredAt().minus(p.lookback())))
                .param("at", ts(e.occurredAt()))
                .param("txn", e.transactionId())
                .query((rs, i) -> new long[] {rs.getLong("n"), Math.round(rs.getDouble("avg"))})
                .single();
        long n = history[0];
        long avg = history[1];
        if (n < p.minHistory()) {
            return alert(Rule.LARGE_AMOUNT, e, null, "amount " + e.amount() + " with only " + n + " prior transfers");
        }
        if (e.amount() >= p.multiplier() * avg) {
            return alert(Rule.LARGE_AMOUNT, e, null,
                    "amount " + e.amount() + " >= " + p.multiplier() + "x average " + avg + " over " + n + " transfers");
        }
        return Optional.empty();
    }

    /** Burst of outgoing transfers (card testing, account takeover). One alert per burst. */
    Optional<Alert> velocity(TransactionPostedEvent e) {
        var p = rules.velocity();
        Instant since = e.occurredAt().minus(p.window());
        long count = countOutgoing(e.fromAccountId(), since, e.occurredAt(), 0, Long.MAX_VALUE);
        if (count < p.maxTransfers() || alertedNear(Rule.VELOCITY, e.fromAccountId(), e.occurredAt(), p.window())) {
            return Optional.empty();
        }
        return alert(Rule.VELOCITY, e, null, count + " transfers within " + p.window());
    }

    /** Repeated amounts just under the reporting threshold. One alert per window. */
    Optional<Alert> structuring(TransactionPostedEvent e) {
        var p = rules.structuring();
        long floor = p.threshold() - p.threshold() * p.bandPercent() / 100;
        if (e.amount() < floor || e.amount() >= p.threshold()) {
            return Optional.empty();
        }
        Instant since = e.occurredAt().minus(p.window());
        long count = countOutgoing(e.fromAccountId(), since, e.occurredAt(), floor, p.threshold() - 1);
        if (count < p.minCount() || alertedNear(Rule.STRUCTURING, e.fromAccountId(), e.occurredAt(), p.window())) {
            return Optional.empty();
        }
        return alert(Rule.STRUCTURING, e, null, count + " transfers in [" + floor + ", " + p.threshold() + ") within " + p.window());
    }

    /** Money sent back to the payer shortly after, for about the same amount. */
    Optional<Alert> roundTrip(TransactionPostedEvent e) {
        var p = rules.roundTrip();
        if (e.amount() < p.minAmount()) {
            return Optional.empty();
        }
        // Tolerance is relative to the larger leg, so the match is the same whichever leg is
        // evaluated (and committed) first.
        return jdbc.sql("""
                        SELECT id, from_account_id, occurred_at
                          FROM transactions
                         WHERE from_account_id = :to AND to_account_id = :from AND status = 'POSTED'
                           AND occurred_at BETWEEN :start AND :end
                           AND abs(amount - :amount) * 100 <= greatest(amount, :amount) * :tolerance
                         ORDER BY abs(extract(epoch FROM occurred_at - :at)), id
                         LIMIT 1
                        """)
                .param("from", e.fromAccountId())
                .param("to", e.toAccountId())
                .param("start", ts(e.occurredAt().minus(p.window())))
                .param("end", ts(e.occurredAt().plus(p.window())))
                .param("at", ts(e.occurredAt()))
                .param("amount", e.amount())
                .param("tolerance", p.amountTolerancePercent())
                .query(RiskEngine::match)
                .optional()
                .flatMap(other -> pairAlert(Rule.ROUND_TRIP, e, other, "returned to payer within " + p.window()));
    }

    /** Same payer, payee and amount again within minutes under a different idempotency key. */
    Optional<Alert> duplicatePayment(TransactionPostedEvent e) {
        var p = rules.duplicatePayment();
        return jdbc.sql("""
                        SELECT id, from_account_id, occurred_at
                          FROM transactions
                         WHERE from_account_id = :from AND to_account_id = :to AND amount = :amount
                           AND status = 'POSTED' AND id <> :txn
                           AND occurred_at BETWEEN :start AND :end
                         ORDER BY abs(extract(epoch FROM occurred_at - :at)), id
                         LIMIT 1
                        """)
                .param("from", e.fromAccountId())
                .param("to", e.toAccountId())
                .param("amount", e.amount())
                .param("txn", e.transactionId())
                .param("start", ts(e.occurredAt().minus(p.window())))
                .param("end", ts(e.occurredAt().plus(p.window())))
                .param("at", ts(e.occurredAt()))
                .query(RiskEngine::match)
                .optional()
                .flatMap(other -> pairAlert(Rule.DUPLICATE_PAYMENT, e, other, "same payee and amount within " + p.window()));
    }

    private Optional<Alert> pairAlert(Rule rule, TransactionPostedEvent e, Match other, String details) {
        boolean thisIsLater = e.occurredAt().isAfter(other.occurredAt())
                || (e.occurredAt().equals(other.occurredAt()) && e.transactionId().compareTo(other.id()) > 0);
        if (thisIsLater) {
            return Optional.of(new Alert(rule, e.fromAccountId(), e.transactionId(), other.id(), details, e.occurredAt()));
        }
        return Optional.of(new Alert(rule, other.fromAccountId(), other.id(), e.transactionId(), details, other.occurredAt()));
    }

    private long countOutgoing(UUID account, Instant since, Instant until, long minAmount, long maxAmount) {
        return jdbc.sql("""
                        SELECT count(*) FROM transactions
                         WHERE from_account_id = :account AND status = 'POSTED'
                           AND occurred_at > :since AND occurred_at <= :until
                           AND amount BETWEEN :min AND :max
                        """)
                .param("account", account)
                .param("since", ts(since))
                .param("until", ts(until))
                .param("min", minAmount)
                .param("max", maxAmount)
                .query(Long.class)
                .single();
    }

    /** One alert per burst: looks both ways so out-of-order evaluation does not alert twice. */
    private boolean alertedNear(Rule rule, UUID account, Instant at, Duration window) {
        return jdbc.sql("""
                        SELECT EXISTS (SELECT 1 FROM risk_alerts
                                        WHERE account_id = :account AND rule = :rule
                                          AND occurred_at > :since AND occurred_at < :until)
                        """)
                .param("account", account)
                .param("rule", rule.name())
                .param("since", ts(at.minus(window)))
                .param("until", ts(at.plus(window)))
                .query(Boolean.class)
                .single();
    }

    private void save(Alert a) {
        int inserted = jdbc.sql("""
                        INSERT INTO risk_alerts (rule, account_id, transaction_id, related_transaction_id, details, occurred_at)
                        VALUES (:rule, :account, :txn, :related, :details, :at)
                        ON CONFLICT (rule, transaction_id) DO NOTHING
                        """)
                .param("rule", a.rule().name())
                .param("account", a.accountId())
                .param("txn", a.transactionId())
                .param("related", a.relatedTransactionId(), java.sql.Types.OTHER)
                .param("details", a.details())
                .param("at", ts(a.occurredAt()))
                .update();
        if (inserted == 1) {
            meters.counter("risk.alerts", "rule", a.rule().name()).increment();
        }
    }

    private static Optional<Alert> alert(Rule rule, TransactionPostedEvent e, UUID related, String details) {
        return Optional.of(new Alert(rule, e.fromAccountId(), e.transactionId(), related, details, e.occurredAt()));
    }

    private static Match match(java.sql.ResultSet rs, int i) throws java.sql.SQLException {
        return new Match(uuid(rs, "id"), uuid(rs, "from_account_id"), instant(rs, "occurred_at"));
    }
}
