package io.github.abhayrkhot.bench;

import java.util.HashMap;
import java.util.Map;

/**
 * bench seed          create and fund accounts through the API
 * bench kafka-load    produce N unique transfer requests plus duplicates and poison messages
 * bench kafka-verify  wait for ingest to drain, then check exactly-once effects on balances
 * bench risk-stream   produce a labelled event-time stream with seeded fraud and settlement breaks
 * bench risk-eval     score alerts and reconciliation breaks against the labels
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: bench <seed|kafka-load|kafka-verify|risk-stream|risk-eval> [--key value ...]");
            System.exit(2);
        }
        Args a = Args.parse(args);
        switch (args[0]) {
            case "seed" -> Seed.run(a);
            case "kafka-load" -> KafkaLoad.run(a);
            case "kafka-verify" -> KafkaVerify.run(a);
            case "risk-stream" -> RiskStream.run(a);
            case "risk-eval" -> RiskEval.run(a);
            default -> {
                System.err.println("unknown command " + args[0]);
                System.exit(2);
            }
        }
    }

    record Args(Map<String, String> values) {
        static Args parse(String[] args) {
            Map<String, String> m = new HashMap<>();
            for (int i = 1; i < args.length; i += 2) {
                if (!args[i].startsWith("--") || i + 1 >= args.length) {
                    throw new IllegalArgumentException("expected --key value pairs, got " + args[i]);
                }
                m.put(args[i].substring(2), args[i + 1]);
            }
            return new Args(m);
        }

        String str(String key, String def) {
            return values.getOrDefault(key, def);
        }

        int integer(String key, int def) {
            return values.containsKey(key) ? Integer.parseInt(values.get(key)) : def;
        }

        long lng(String key, long def) {
            return values.containsKey(key) ? Long.parseLong(values.get(key)) : def;
        }

        String api() {
            return str("api", "http://localhost:8080");
        }

        String jdbc() {
            return str("jdbc", "jdbc:postgresql://localhost:5432/ledger?user=ledger&password=ledger");
        }

        String bootstrap() {
            return str("bootstrap", "localhost:19092");
        }
    }
}
