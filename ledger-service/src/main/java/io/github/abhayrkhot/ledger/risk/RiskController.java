package io.github.abhayrkhot.ledger.risk;

import static io.github.abhayrkhot.ledger.common.Jdbc.instant;
import static io.github.abhayrkhot.ledger.common.Jdbc.uuid;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
class RiskController {

    record AlertView(long id, String rule, UUID accountId, UUID transactionId, UUID relatedTransactionId,
                     String details, Instant occurredAt, Instant createdAt) {}

    private final JdbcClient jdbc;

    RiskController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/api/v1/risk/alerts")
    List<AlertView> alerts(
            @RequestParam UUID accountId,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        return jdbc.sql("""
                        SELECT id, rule, account_id, transaction_id, related_transaction_id, details, occurred_at, created_at
                          FROM risk_alerts
                         WHERE account_id = :account
                         ORDER BY occurred_at DESC
                         LIMIT :limit
                        """)
                .param("account", accountId)
                .param("limit", limit)
                .query((rs, i) -> new AlertView(rs.getLong("id"), rs.getString("rule"), uuid(rs, "account_id"),
                        uuid(rs, "transaction_id"), uuid(rs, "related_transaction_id"), rs.getString("details"),
                        instant(rs, "occurred_at"), instant(rs, "created_at")))
                .list();
    }
}
