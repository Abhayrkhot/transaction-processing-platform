package io.github.abhayrkhot.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Produces {@code events} unique transfer requests, plus {@code duplicates} re-sends of
 * random earlier requests (same key and body, as an upstream retry would) and
 * {@code poison} invalid messages. Writes the balance every account must end with if each
 * unique request is applied exactly once.
 */
final class KafkaLoad {

    static void run(Main.Args a) throws Exception {
        Path data = Path.of(a.str("data", "bench-data"));
        String run = a.str("run", "k" + System.currentTimeMillis());
        int events = a.integer("events", 100_000);
        int duplicates = a.integer("duplicates", 25_000);
        int poison = a.integer("poison", 50);
        int rate = a.integer("rate", 2_500);
        Random random = new Random(a.lng("seed", 7));

        List<UUID> customers = Files.readAllLines(data.resolve("customers.txt")).stream().map(UUID::fromString).toList();
        Map<UUID, Long> expected = balances(a.jdbc(), customers);

        record Msg(double position, String key, String value) {}
        List<Msg> messages = new ArrayList<>(events + duplicates + poison);
        String[] bodies = new String[events];
        String[] keys = new String[events];
        for (int i = 0; i < events; i++) {
            UUID from = customers.get(random.nextInt(customers.size()));
            UUID to;
            do {
                to = customers.get(random.nextInt(customers.size()));
            } while (to.equals(from));
            long amount = 1 + random.nextInt(50_000);
            expected.merge(from, -amount, Long::sum);
            expected.merge(to, amount, Long::sum);
            keys[i] = from.toString();
            bodies[i] = message(run + "-e-" + i, from, to, amount, "load-" + run + "-" + i);
            messages.add(new Msg(i, keys[i], bodies[i]));
        }
        for (int d = 0; d < duplicates; d++) {
            int original = random.nextInt(events);
            // Re-sent later in the stream, up to ~10 s later at the default rate.
            double position = Math.min(events - 0.5, original + 1 + random.nextInt(25_000)) + random.nextDouble() * 0.4;
            messages.add(new Msg(position, keys[original], bodies[original]));
        }
        for (int p = 0; p < poison; p++) {
            String key = "poison-" + run + "-" + p;
            UUID from = customers.get(random.nextInt(customers.size()));
            String value = switch (p % 3) {
                case 0 -> "{\"idempotencyKey\": \"" + run + "-p-" + p + "\", truncated";
                case 1 -> message(run + "-p-" + p, from, UUID.randomUUID(), 100, null); // unknown account
                default -> message(run + "-p-" + p, from, customers.getFirst(), -5, null); // invalid amount
            };
            messages.add(new Msg(random.nextDouble() * events, key, value));
        }
        messages.sort((x, y) -> Double.compare(x.position(), y.position()));

        List<String> expectedLines = new ArrayList<>();
        expected.forEach((id, balance) -> expectedLines.add(id + "," + balance));
        Files.write(data.resolve("expected-" + run + ".csv"), expectedLines);
        Files.writeString(data.resolve("run-" + run + ".properties"),
                "events=" + events + "\nduplicates=" + duplicates + "\npoison=" + poison + "\n");

        System.out.printf("run %s: producing %,d messages (%,d unique, %,d duplicates, %d poison) at %,d/s%n",
                run, messages.size(), events, duplicates, poison, rate);
        long start = System.nanoTime();
        try (var producer = Kafka.producer(a.bootstrap())) {
            Kafka.sendPaced(producer, Kafka.REQUESTS, messages.stream().map(m -> new String[] {m.key(), m.value()}).toList(), rate);
        }
        System.out.printf("produced in %.1fs%n", (System.nanoTime() - start) / 1e9);
    }

    static String message(String key, UUID from, UUID to, long amount, String reference) {
        return "{\"idempotencyKey\":\"" + key + "\",\"fromAccountId\":\"" + from + "\",\"toAccountId\":\"" + to
                + "\",\"amount\":" + amount + ",\"currency\":\"USD\""
                + (reference == null ? "" : ",\"reference\":\"" + reference + "\"") + "}";
    }

    static Map<UUID, Long> balances(String jdbc, List<UUID> accounts) throws Exception {
        Map<UUID, Long> out = new HashMap<>();
        try (Connection c = DriverManager.getConnection(jdbc);
             var ps = c.prepareStatement("SELECT id, balance FROM accounts WHERE id = ANY (?)")) {
            ps.setArray(1, c.createArrayOf("uuid", accounts.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getObject(1, UUID.class), rs.getLong(2));
                }
            }
        }
        return out;
    }
}
