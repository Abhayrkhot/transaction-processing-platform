package io.github.abhayrkhot.bench;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Waits until the ingest consumer has drained the topic, then checks the run's effects:
 * every unique request posted once, no duplicate postings, every account balance equal to
 * the exactly-once expectation, and exactly the poison messages in the dead-letter topic.
 */
final class KafkaVerify {

    static void run(Main.Args a) throws Exception {
        Path data = Path.of(a.str("data", "bench-data"));
        String run = a.str("run", null);
        Properties meta = new Properties();
        meta.load(new StringReader(Files.readString(data.resolve("run-" + run + ".properties"))));
        long events = Long.parseLong(meta.getProperty("events"));
        long duplicates = Long.parseLong(meta.getProperty("duplicates"));
        long poison = Long.parseLong(meta.getProperty("poison"));
        long deadline = System.currentTimeMillis() + a.lng("timeout", 900) * 1000;

        long start = System.currentTimeMillis();
        while (true) {
            long lag = Kafka.lag(a.bootstrap(), "ledger-ingest", Kafka.REQUESTS);
            long posted = count(a.jdbc(), "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ?", run + "-e-%");
            System.out.printf("  ingest lag %,d, run transactions %,d / %,d%n", lag, posted, events);
            if (lag == 0 && posted >= events) {
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("timed out waiting for ingest to drain");
            }
            Api.sleep(2_000);
        }
        System.out.printf("drained after %.1fs of waiting%n", (System.currentTimeMillis() - start) / 1e3);

        String like = run + "-e-%";
        long transactions = count(a.jdbc(), "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ?", like);
        long postedCount = count(a.jdbc(), "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ? AND status = 'POSTED'", like);
        long legs = count(a.jdbc(), "SELECT count(*) FROM entries e JOIN transactions t ON t.id = e.transaction_id WHERE t.idempotency_key LIKE ?", like);
        long references = count(a.jdbc(), "SELECT count(DISTINCT reference) FROM transactions WHERE idempotency_key LIKE ?", like);

        Map<UUID, Long> expected = new HashMap<>();
        for (String line : Files.readAllLines(data.resolve("expected-" + run + ".csv"))) {
            String[] f = line.split(",");
            expected.put(UUID.fromString(f[0]), Long.parseLong(f[1]));
        }
        Map<UUID, Long> actual = KafkaLoad.balances(a.jdbc(), new ArrayList<>(expected.keySet()));
        long mismatched = expected.entrySet().stream().filter(e -> !e.getValue().equals(actual.get(e.getKey()))).count();

        AtomicLong dltPoison = new AtomicLong();
        AtomicLong dltOther = new AtomicLong();
        Kafka.readAll(a.bootstrap(), Kafka.REQUESTS + ".dlt", r -> {
            if (r.key() != null && r.key().startsWith("poison-" + run + "-")) {
                dltPoison.incrementAndGet();
            } else if (r.value() != null && r.value().contains("\"" + run + "-e-")) {
                dltOther.incrementAndGet();
            }
        });

        var integrity = new Api(a.api()).get("/api/v1/ledger/integrity");

        boolean ok = transactions == events && postedCount == events && legs == 2 * events && references == events
                && mismatched == 0 && dltPoison.get() == poison && dltOther.get() == 0 && integrity.get("ok").asBoolean();
        String report = """
                {
                  "run": "%s",
                  "uniqueRequests": %d,
                  "duplicateDeliveriesProduced": %d,
                  "poisonMessages": %d,
                  "transactionsForRun": %d,
                  "postedForRun": %d,
                  "entriesForRun": %d,
                  "duplicatePostings": %d,
                  "accountsChecked": %d,
                  "accountsWithWrongBalance": %d,
                  "deadLetteredPoison": %d,
                  "deadLetteredValid": %d,
                  "ledgerIntegrity": %s,
                  "ok": %s
                }
                """.formatted(run, events, duplicates, poison, transactions, postedCount, legs, transactions - references,
                expected.size(), mismatched, dltPoison.get(), dltOther.get(), integrity, ok);
        System.out.println(report);
        Path out = Path.of(a.str("out", "results/raw"));
        Files.createDirectories(out);
        Files.writeString(out.resolve("kafka-" + run + ".json"), report);
        if (!ok) {
            System.exit(1);
        }
    }

    static long count(String jdbc, String sql, String arg) throws Exception {
        try (Connection c = DriverManager.getConnection(jdbc); var ps = c.prepareStatement(sql)) {
            ps.setString(1, arg);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

}
