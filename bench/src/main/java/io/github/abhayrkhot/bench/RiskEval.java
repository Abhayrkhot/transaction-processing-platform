package io.github.abhayrkhot.bench;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/**
 * Scores a risk-stream run. A seeded fraud case counts as identified when an alert of its
 * rule fired on one of its transactions; a seeded settlement break counts when the
 * reconciliation run reported a break of the same type for its reference. Alerts on
 * transactions that are not part of any seeded case are reported as false positives.
 */
final class RiskEval {

    record Case(String kind, String type, String id, List<String> references) {}

    static void run(Main.Args a) throws Exception {
        Path dir = Path.of(a.str("dir", null));
        Properties meta = new Properties();
        meta.load(new StringReader(Files.readString(dir.resolve("meta.properties"))));
        String run = meta.getProperty("run");
        long expected = Long.parseLong(meta.getProperty("transfers"));
        long deadline = System.currentTimeMillis() + a.lng("timeout", 900) * 1000;

        while (true) {
            long posted = KafkaVerify.count(a.jdbc(), "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ?", run + "-%");
            long ingestLag = Kafka.lag(a.bootstrap(), "ledger-ingest", Kafka.REQUESTS);
            long riskLag = Kafka.lag(a.bootstrap(), "risk-engine", Kafka.POSTED);
            long outbox = KafkaVerify.count(a.jdbc(), "SELECT count(*) FROM outbox WHERE topic <> ?", "");
            System.out.printf("  posted %,d / %,d, ingest lag %,d, outbox %,d, risk lag %,d%n", posted, expected, ingestLag, outbox, riskLag);
            if (posted >= expected && ingestLag == 0 && outbox == 0 && riskLag == 0) {
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("timed out waiting for the pipeline to drain");
            }
            Api.sleep(2_000);
        }

        List<Case> cases = new ArrayList<>();
        for (String line : Files.readAllLines(dir.resolve("ground_truth.csv"))) {
            String[] f = line.split(",", 4);
            cases.add(new Case(f[0], f[1], f[2], List.of(f[3].split(";"))));
        }

        // reference -> set of rules alerted on that transaction
        Map<String, Set<String>> alerts = new HashMap<>();
        long totalAlerts = 0;
        long rejected;
        try (Connection c = DriverManager.getConnection(a.jdbc());
             var ps = c.prepareStatement("""
                     SELECT t.reference, a.rule FROM risk_alerts a JOIN transactions t ON t.id = a.transaction_id
                      WHERE t.idempotency_key LIKE ?
                     """)) {
            ps.setString(1, run + "-%");
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    alerts.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2));
                    totalAlerts++;
                }
            }
        }
        rejected = KafkaVerify.count(a.jdbc(), "SELECT count(*) FROM transactions WHERE status = 'REJECTED' AND idempotency_key LIKE ?", run + "-%");

        JsonNode report = new Api(a.api()).postCsv("/api/v1/reconciliations?from=" + meta.getProperty("start") + "&to=" + meta.getProperty("end"),
                Files.readString(dir.resolve("settlement.csv")));
        Set<String> breaks = new HashSet<>();
        for (JsonNode b : report.get("breaks")) {
            breaks.add(b.get("type").asString() + "|" + b.get("reference").asString());
        }

        Map<String, int[]> byType = new LinkedHashMap<>(); // type -> {seeded, identified}
        Set<String> anomalousRefs = new HashSet<>();
        List<String> missed = new ArrayList<>();
        for (Case k : cases) {
            boolean found;
            if (k.kind().equals("RISK")) {
                anomalousRefs.addAll(k.references());
                found = k.references().stream().anyMatch(ref -> alerts.getOrDefault(ref, Set.of()).contains(k.type()));
            } else {
                found = breaks.contains(k.type() + "|" + k.references().getFirst());
            }
            int[] counts = byType.computeIfAbsent(k.kind() + ":" + k.type(), t -> new int[2]);
            counts[0]++;
            if (found) {
                counts[1]++;
            } else {
                missed.add(k.id().startsWith(k.type()) ? k.id() : k.type() + ":" + k.id());
            }
        }
        long alertsOnSeeded = 0;
        long falsePositiveAlerts = 0;
        for (var e : alerts.entrySet()) {
            if (anomalousRefs.contains(e.getKey())) {
                alertsOnSeeded += e.getValue().size();
            } else {
                falsePositiveAlerts += e.getValue().size();
            }
        }
        long seededBreaks = cases.stream().filter(k -> k.kind().equals("RECON")).count();
        long unexpectedBreaks = Math.max(0, breaks.size() - seededBreaks);

        int seeded = 0;
        int identified = 0;
        int riskSeeded = 0;
        int riskIdentified = 0;
        StringBuilder types = new StringBuilder();
        for (var e : byType.entrySet()) {
            seeded += e.getValue()[0];
            identified += e.getValue()[1];
            if (e.getKey().startsWith("RISK")) {
                riskSeeded += e.getValue()[0];
                riskIdentified += e.getValue()[1];
            }
            types.append(types.isEmpty() ? "" : ",\n").append("    \"").append(e.getKey()).append("\": {\"seeded\": ")
                    .append(e.getValue()[0]).append(", \"identified\": ").append(e.getValue()[1]).append("}");
        }
        String json = """
                {
                  "run": "%s",
                  "transfers": %d,
                  "rejectedTransfers": %d,
                  "byType": {
                %s
                  },
                  "riskRecall": %.4f,
                  "reconRecall": %.4f,
                  "seededAnomalies": %d,
                  "identified": %d,
                  "recall": %.4f,
                  "alerts": %d,
                  "alertsOnSeededTransactions": %d,
                  "alertsOnOrdinaryTransactions": %d,
                  "ordinaryTransactionFalsePositiveRate": %.5f,
                  "unexpectedReconciliationBreaks": %d,
                  "missed": %s
                }
                """.formatted(run, expected, rejected, types, riskIdentified / (double) riskSeeded,
                (identified - riskIdentified) / (double) (seeded - riskSeeded), seeded, identified, identified / (double) seeded,
                totalAlerts, alertsOnSeeded, falsePositiveAlerts,
                falsePositiveAlerts / (double) (expected - anomalousRefs.size()), unexpectedBreaks,
                Api.JSON.writeValueAsString(missed));
        System.out.println(json);
        Path out = Path.of(a.str("out", "results/raw"));
        Files.createDirectories(out);
        Files.writeString(out.resolve("risk-" + run + ".json"), json);
    }
}
