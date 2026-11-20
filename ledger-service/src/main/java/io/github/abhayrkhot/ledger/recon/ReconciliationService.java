package io.github.abhayrkhot.ledger.recon;

import static io.github.abhayrkhot.ledger.common.Jdbc.ts;
import static io.github.abhayrkhot.ledger.common.Jdbc.uuid;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Two checks:
 * <ul>
 *   <li>External: matches a settlement file against posted transactions that crossed a
 *       SYSTEM (clearing) account, by reference, and records every break.</li>
 *   <li>Internal: double-entry invariants over the whole ledger in one snapshot.</li>
 * </ul>
 */
@Service
public class ReconciliationService {

    public enum BreakType { MISSING_IN_LEDGER, MISSING_IN_SETTLEMENT, AMOUNT_MISMATCH, DUPLICATE_SETTLEMENT }

    public record Break(BreakType type, String reference, UUID transactionId, Long ledgerAmount, Long settlementAmount) {}

    public record Report(UUID runId, Instant windowStart, Instant windowEnd, int settlementLines, int ledgerRecords,
                         int matched, Map<BreakType, Integer> breakCounts, List<Break> breaks) {}

    public record Integrity(boolean ok, Map<String, Long> balanceByCurrency, long accountsNotMatchingEntries,
                            long unbalancedTransactions, long postedWithoutEntries) {
        static Integrity of(Map<String, Long> byCurrency, long accounts, long unbalanced, long withoutEntries) {
            boolean ok = byCurrency.values().stream().allMatch(v -> v == 0)
                    && accounts == 0 && unbalanced == 0 && withoutEntries == 0;
            return new Integrity(ok, byCurrency, accounts, unbalanced, withoutEntries);
        }
    }

    private record LedgerRecord(UUID id, String reference, long amount, String currency) {}

    private static final int LOOKUP_CHUNK = 1000;
    private static final Duration SETTLEMENT_TOLERANCE = Duration.ofDays(2);

    /** Posted transactions that crossed a clearing (SYSTEM) account, i.e. ones the network settles. */
    private static final String EXTERNAL_POSTED = """
            SELECT t.id, t.reference, t.amount, t.currency
              FROM transactions t
              JOIN accounts src ON src.id = t.from_account_id
              JOIN accounts dst ON dst.id = t.to_account_id
             WHERE t.status = 'POSTED' AND t.reference IS NOT NULL
               AND t.occurred_at >= :from AND t.occurred_at < :to
               AND (src.type = 'SYSTEM' OR dst.type = 'SYSTEM')""";

    private final JdbcClient jdbc;

    ReconciliationService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public Report reconcile(List<SettlementLine> lines, Instant from, Instant to) {
        List<LedgerRecord> window = externalTransactions(from, to);
        Map<String, LedgerRecord> ledger = new HashMap<>();
        List<LedgerRecord> sameReference = new ArrayList<>();
        for (LedgerRecord r : window) {
            if (ledger.putIfAbsent(r.reference(), r) != null) {
                sameReference.add(r); // e.g. one deposit posted twice under two keys: at most one can settle
            }
        }
        int ledgerRecords = window.size();

        Map<String, List<SettlementLine>> byReference = new LinkedHashMap<>();
        lines.forEach(l -> byReference.computeIfAbsent(l.reference(), k -> new ArrayList<>()).add(l));

        // Settlement lines may legitimately refer to transactions just outside the window.
        List<String> outside = byReference.keySet().stream().filter(r -> !ledger.containsKey(r)).toList();
        Map<String, LedgerRecord> outsideWindow = lookupNearWindow(outside, from, to);

        List<Break> breaks = new ArrayList<>();
        int matched = 0;
        for (var entry : byReference.entrySet()) {
            String reference = entry.getKey();
            List<SettlementLine> group = entry.getValue();
            SettlementLine line = group.getFirst();
            LedgerRecord record = ledger.remove(reference);
            if (record == null) {
                record = outsideWindow.get(reference);
            }
            for (SettlementLine extra : group.subList(1, group.size())) {
                breaks.add(new Break(BreakType.DUPLICATE_SETTLEMENT, reference,
                        record == null ? null : record.id(), record == null ? null : record.amount(), extra.amount()));
            }
            if (record == null) {
                breaks.add(new Break(BreakType.MISSING_IN_LEDGER, reference, null, null, line.amount()));
            } else if (record.amount() != line.amount() || !record.currency().equals(line.currency())) {
                breaks.add(new Break(BreakType.AMOUNT_MISMATCH, reference, record.id(), record.amount(), line.amount()));
            } else {
                matched++;
            }
        }
        List<LedgerRecord> unsettled = new ArrayList<>(ledger.values());
        unsettled.addAll(sameReference);
        for (LedgerRecord unmatched : unsettled) {
            breaks.add(new Break(BreakType.MISSING_IN_SETTLEMENT, unmatched.reference(), unmatched.id(), unmatched.amount(), null));
        }

        UUID runId = jdbc.sql("""
                        INSERT INTO reconciliation_runs (window_start, window_end, settlement_lines, ledger_records, matched, breaks)
                        VALUES (:from, :to, :lines, :records, :matched, :breaks)
                        RETURNING id
                        """)
                .param("from", ts(from))
                .param("to", ts(to))
                .param("lines", lines.size())
                .param("records", ledgerRecords)
                .param("matched", matched)
                .param("breaks", breaks.size())
                .query((rs, i) -> uuid(rs, "id"))
                .single();
        for (Break b : breaks) {
            jdbc.sql("""
                            INSERT INTO reconciliation_breaks (run_id, type, reference, transaction_id, ledger_amount, settlement_amount)
                            VALUES (:run, :type, :reference, :txn, :ledger, :settlement)
                            """)
                    .param("run", runId)
                    .param("type", b.type().name())
                    .param("reference", b.reference())
                    .param("txn", b.transactionId(), java.sql.Types.OTHER)
                    .param("ledger", b.ledgerAmount(), java.sql.Types.BIGINT)
                    .param("settlement", b.settlementAmount(), java.sql.Types.BIGINT)
                    .update();
        }

        Map<BreakType, Integer> counts = new LinkedHashMap<>();
        for (BreakType t : BreakType.values()) {
            counts.put(t, (int) breaks.stream().filter(b -> b.type() == t).count());
        }
        return new Report(runId, from, to, lines.size(), ledgerRecords, matched, counts, breaks);
    }

    /** Runs in one REPEATABLE READ snapshot so concurrent postings cannot produce false breaks. */
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Integrity integrity() {
        Map<String, Long> byCurrency = new LinkedHashMap<>();
        jdbc.sql("SELECT currency, sum(balance) AS total FROM accounts GROUP BY currency ORDER BY currency")
                .query(rs -> {
                    byCurrency.put(rs.getString("currency"), rs.getLong("total"));
                });
        long accountMismatches = jdbc.sql("""
                        SELECT count(*) FROM accounts a
                          LEFT JOIN (SELECT account_id, sum(amount) AS total FROM entries GROUP BY account_id) e
                                 ON e.account_id = a.id
                         WHERE a.balance <> coalesce(e.total, 0)
                        """).query(Long.class).single();
        long unbalanced = jdbc.sql("""
                        SELECT count(*) FROM (SELECT transaction_id FROM entries GROUP BY transaction_id
                                              HAVING sum(amount) <> 0 OR count(*) <> 2) x
                        """).query(Long.class).single();
        long withoutEntries = jdbc.sql("""
                        SELECT count(*) FROM transactions t
                         WHERE t.status = 'POSTED' AND NOT EXISTS (SELECT 1 FROM entries e WHERE e.transaction_id = t.id)
                        """).query(Long.class).single();
        return Integrity.of(byCurrency, accountMismatches, unbalanced, withoutEntries);
    }

    private List<LedgerRecord> externalTransactions(Instant from, Instant to) {
        return jdbc.sql(EXTERNAL_POSTED)
                .param("from", ts(from))
                .param("to", ts(to))
                .query((rs, i) -> new LedgerRecord(uuid(rs, "id"), rs.getString("reference"), rs.getLong("amount"), rs.getString("currency")))
                .list();
    }

    /**
     * Settlement can lag or lead the ledger by a little; a line may match an external
     * transaction up to {@link #SETTLEMENT_TOLERANCE} outside the window, never further, so an
     * old transaction settled again in a later file is still reported.
     */
    private Map<String, LedgerRecord> lookupNearWindow(List<String> references, Instant from, Instant to) {
        Map<String, LedgerRecord> found = new HashMap<>();
        for (int i = 0; i < references.size(); i += LOOKUP_CHUNK) {
            jdbc.sql(EXTERNAL_POSTED + " AND t.reference IN (:refs)")
                    .param("from", ts(from.minus(SETTLEMENT_TOLERANCE)))
                    .param("to", ts(to.plus(SETTLEMENT_TOLERANCE)))
                    .param("refs", references.subList(i, Math.min(i + LOOKUP_CHUNK, references.size())))
                    .query((rs, n) -> new LedgerRecord(uuid(rs, "id"), rs.getString("reference"), rs.getLong("amount"), rs.getString("currency")))
                    .list()
                    .forEach(r -> found.putIfAbsent(r.reference(), r));
        }
        return found;
    }
}
