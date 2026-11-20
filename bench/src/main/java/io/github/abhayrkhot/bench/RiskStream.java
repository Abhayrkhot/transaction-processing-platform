package io.github.abhayrkhot.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Generates a labelled event-time stream: ordinary customer activity with seeded fraud
 * typologies, sent through the real pipeline (Kafka ingest -> ledger -> outbox -> risk), and
 * a settlement file for the external (clearing) transfers with seeded breaks. Labels go to
 * ground_truth.csv and are never visible to the service.
 *
 * <p>Typology parameters are drawn from fixed ranges chosen to describe each pattern, not
 * fitted to the rule thresholds; some cases land outside a rule (e.g. a drain from an
 * account whose normal spend is high) and are genuinely missed.
 */
final class RiskStream {

    record Transfer(Instant at, UUID from, UUID to, long amount, String reference) {}

    private final Random r;
    private final List<Transfer> transfers = new ArrayList<>();
    private final List<String> truth = new ArrayList<>();
    private final List<Transfer> external = new ArrayList<>();
    private final String run;
    private int sequence;

    private RiskStream(long seed, String run) {
        this.r = new Random(seed);
        this.run = run;
    }

    static void run(Main.Args a) throws Exception {
        long seed = a.lng("seed", 1);
        String run = a.str("run", "r" + seed + "-" + System.currentTimeMillis() % 100_000);
        int customers = a.integer("customers", 2_000);
        int merchants = a.integer("merchants", 200);
        int days = a.integer("days", 14);
        int perType = a.integer("per-type", 120);
        int breaksPerType = a.integer("breaks-per-type", 50);
        Path out = Path.of(a.str("out", "bench-data/risk-" + run));
        Files.createDirectories(out);
        Instant start = Instant.parse("2025-01-06T00:00:00Z").plus(Duration.ofDays(30L * seed));
        Instant end = start.plus(Duration.ofDays(days));

        Api api = new Api(a.api());
        UUID clearing = api.openAccount("clearing-" + run, "SYSTEM");
        UUID[] c = new UUID[customers];
        UUID[] m = new UUID[merchants];
        UUID[] mules = new UUID[30];
        Api.parallel(customers, 64, i -> c[i] = api.openAccount("rc-" + run + "-" + i, "CUSTOMER"));
        Api.parallel(merchants, 64, i -> m[i] = api.openAccount("rm-" + run + "-" + i, "CUSTOMER"));
        Api.parallel(mules.length, 16, i -> mules[i] = api.openAccount("mule-" + run + "-" + i, "CUSTOMER"));
        Instant opened = start.minus(Duration.ofHours(1));
        Api.parallel(customers, 64, i -> api.transfer("open-" + run + "-" + i, clearing, c[i], 20_000_000, "open-" + run + "-" + i, opened));

        RiskStream g = new RiskStream(seed, run);
        g.ordinaryActivity(c, m, clearing, start, end);
        g.anomalies(c, m, mules, clearing, start, end, perType);
        List<String> settlement = g.settlement(breaksPerType);

        g.transfers.sort((x, y) -> x.at().compareTo(y.at()));
        List<String[]> messages = new ArrayList<>(g.transfers.size());
        for (Transfer t : g.transfers) {
            messages.add(new String[] {t.from().toString(),
                    "{\"idempotencyKey\":\"" + run + "-" + t.reference() + "\",\"fromAccountId\":\"" + t.from()
                            + "\",\"toAccountId\":\"" + t.to() + "\",\"amount\":" + t.amount()
                            + ",\"currency\":\"USD\",\"reference\":\"" + t.reference() + "\",\"occurredAt\":\"" + t.at() + "\"}"});
        }
        Files.write(out.resolve("ground_truth.csv"), g.truth);
        Files.write(out.resolve("settlement.csv"), settlement);
        Files.writeString(out.resolve("meta.properties"), "run=" + run + "\nseed=" + seed + "\nstart=" + start + "\nend=" + end
                + "\ntransfers=" + g.transfers.size() + "\n");

        int rate = a.integer("rate", 3_000);
        System.out.printf("run %s: %,d transfers over %d days, %d labelled risk anomalies, %d settlement breaks; producing at %,d/s%n",
                run, g.transfers.size(), days, perType * 5, breaksPerType * 4, rate);
        try (var producer = Kafka.producer(a.bootstrap())) {
            Kafka.sendPaced(producer, Kafka.REQUESTS, messages, rate);
        }
        System.out.println("labels in " + out.toAbsolutePath());
    }

    private void ordinaryActivity(UUID[] c, UUID[] m, UUID clearing, Instant start, Instant end) {
        double days = Duration.between(start, end).toHours() / 24.0;
        for (UUID customer : c) {
            double perDay = 1 + 5 * r.nextDouble();
            double median = Math.exp(Math.log(8) + r.nextDouble() * (Math.log(120) - Math.log(8)));
            List<UUID> favourites = sample(m, 5 + r.nextInt(11));
            List<UUID> friends = sample(c, 3 + r.nextInt(6));
            friends.remove(customer);

            double t = expo(perDay);
            while (t < days) {
                boolean shop = r.nextDouble() < 0.85 || friends.isEmpty();
                UUID to = shop ? favourites.get(zipf(favourites.size())) : friends.get(r.nextInt(friends.size()));
                long amount = Math.max(100, Math.round(median * Math.exp(0.8 * r.nextGaussian()) * 100));
                add(start, t, customer, to, amount, "n");
                t += expo(perDay);
            }
            if (r.nextDouble() < 0.10) { // rent
                add(start, r.nextDouble() * days, customer, m[r.nextInt(m.length)], cents(900, 3_200), "rent");
            }
            for (double pay = r.nextDouble() * 7; pay < days; pay += 7) { // weekly payroll deposit
                external.add(add(start, pay, clearing, customer, cents(800, 4_000), "pay"));
            }
            for (double atm = expo(0.3); atm < days; atm += expo(0.3)) { // cash withdrawal
                external.add(add(start, atm, customer, clearing, 2_000L * (1 + r.nextInt(20)), "atm"));
            }
        }
    }

    private void anomalies(UUID[] c, UUID[] m, UUID[] mules, UUID clearing, Instant start, Instant end, int perType) {
        double days = Duration.between(start, end).toHours() / 24.0;
        for (int i = 0; i < perType; i++) {
            // Account takeover: one large transfer out to a mule, or cashed out externally.
            double t = 2 + r.nextDouble() * (days - 3);
            boolean cashOut = r.nextDouble() < 0.3;
            Transfer drain = add(start, t, pick(c), cashOut ? clearing : pick(mules), cents(3_000, 20_000), "anom-large-" + i);
            if (cashOut) {
                external.add(drain);
            }
            label("LARGE_AMOUNT", i, drain);

            // Card testing: a burst of small payments.
            UUID victim = pick(c);
            int k = 10 + r.nextInt(21);
            double span = (1 + 9 * r.nextDouble()) / 1440.0;
            t = 2 + r.nextDouble() * (days - 3);
            List<Transfer> burst = new ArrayList<>();
            for (int j = 0; j < k; j++) {
                burst.add(add(start, t + span * j / (k - 1), victim, pick(m), cents(1, 20), "anom-velocity-" + i + "-" + j));
            }
            label("VELOCITY", i, burst.toArray(Transfer[]::new));

            // Structuring: several payments just under $10,000 within a day.
            victim = pick(c);
            int n = 3 + r.nextInt(4);
            span = (1 + 21 * r.nextDouble()) / 24.0;
            t = 2 + r.nextDouble() * (days - 3);
            List<Transfer> legs = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                legs.add(add(start, t + span * j / (n - 1), victim, pick(mules), cents(9_000, 9_990), "anom-structuring-" + i + "-" + j));
            }
            label("STRUCTURING", i, legs.toArray(Transfer[]::new));

            // Round trip: money sent and returned almost in full within the hour.
            UUID a = pick(c);
            UUID b;
            do {
                b = pick(c);
            } while (b.equals(a));
            long amount = cents(600, 8_000);
            t = 2 + r.nextDouble() * (days - 3);
            Transfer out = add(start, t, a, b, amount, "anom-roundtrip-" + i + "-out");
            Transfer back = add(start, t + (1 + 54 * r.nextDouble()) / 1440.0, b, a,
                    Math.round(amount * (0.95 + 0.05 * r.nextDouble())), "anom-roundtrip-" + i + "-back");
            label("ROUND_TRIP", i, out, back);

            // Duplicate payment: the same charge submitted twice within minutes.
            victim = pick(c);
            UUID merchant = pick(m);
            amount = cents(5, 400);
            t = 2 + r.nextDouble() * (days - 3);
            Transfer first = add(start, t, victim, merchant, amount, "anom-duplicate-" + i + "-a");
            Transfer second = add(start, t + (3 + 237 * r.nextDouble()) / 86_400.0, victim, merchant, amount, "anom-duplicate-" + i + "-b");
            label("DUPLICATE_PAYMENT", i, first, second);
        }
    }

    /** Settlement lines for every external transfer, then four kinds of seeded breaks. */
    private List<String> settlement(int perType) {
        List<Transfer> pool = new ArrayList<>(external);
        Collections.shuffle(pool, r);
        Set<String> missing = new HashSet<>();
        Set<String> wrongAmount = new HashSet<>();
        Set<String> duplicated = new HashSet<>();
        for (int i = 0; i < perType; i++) {
            missing.add(pool.get(i).reference());
            wrongAmount.add(pool.get(perType + i).reference());
            duplicated.add(pool.get(2 * perType + i).reference());
        }
        List<String> lines = new ArrayList<>();
        lines.add("reference,amount,currency,value_date");
        for (Transfer t : external) {
            if (missing.contains(t.reference())) {
                truth.add("RECON,MISSING_IN_SETTLEMENT," + t.reference() + "," + t.reference());
                continue;
            }
            long amount = t.amount();
            if (wrongAmount.contains(t.reference())) {
                long delta = 1 + r.nextInt(5_000);
                amount += r.nextBoolean() ? delta : -Math.min(delta, amount - 1);
                truth.add("RECON,AMOUNT_MISMATCH," + t.reference() + "," + t.reference());
            }
            lines.add(t.reference() + "," + amount + ",USD," + t.at());
            if (duplicated.contains(t.reference())) {
                lines.add(t.reference() + "," + amount + ",USD," + t.at());
                truth.add("RECON,DUPLICATE_SETTLEMENT," + t.reference() + "," + t.reference());
            }
        }
        for (int i = 0; i < perType; i++) {
            String phantom = "phantom-" + run + "-" + i;
            lines.add(phantom + "," + cents(20, 2_000) + ",USD," + external.get(r.nextInt(external.size())).at());
            truth.add("RECON,MISSING_IN_LEDGER," + phantom + "," + phantom);
        }
        return lines;
    }

    private Transfer add(Instant start, double dayOffset, UUID from, UUID to, long amount, String kind) {
        Instant at = start.plusMillis(Math.round(dayOffset * 86_400_000));
        Transfer t = new Transfer(at, from, to, amount, kind + "-" + run + "-" + (sequence++));
        transfers.add(t);
        return t;
    }

    private void label(String type, int index, Transfer... legs) {
        StringBuilder refs = new StringBuilder();
        for (Transfer t : legs) {
            refs.append(refs.isEmpty() ? "" : ";").append(t.reference());
        }
        truth.add("RISK," + type + "," + type + "-" + index + "," + refs);
    }

    private long cents(int minDollars, int maxDollars) {
        return minDollars * 100L + (long) (r.nextDouble() * (maxDollars - minDollars) * 100);
    }

    private double expo(double perDay) {
        return -Math.log(1 - r.nextDouble()) / perDay;
    }

    private int zipf(int n) {
        double total = 0;
        for (int k = 1; k <= n; k++) {
            total += 1.0 / k;
        }
        double x = r.nextDouble() * total;
        for (int k = 1; k <= n; k++) {
            x -= 1.0 / k;
            if (x <= 0) {
                return k - 1;
            }
        }
        return n - 1;
    }

    private UUID pick(UUID[] ids) {
        return ids[r.nextInt(ids.length)];
    }

    private List<UUID> sample(UUID[] ids, int n) {
        Set<UUID> chosen = new HashSet<>();
        while (chosen.size() < Math.min(n, ids.length)) {
            chosen.add(pick(ids));
        }
        return new ArrayList<>(chosen);
    }
}
