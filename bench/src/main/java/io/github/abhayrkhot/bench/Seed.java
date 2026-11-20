package io.github.abhayrkhot.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

/** Creates a clearing account, customers and merchants, and funds every customer. */
final class Seed {

    static void run(Main.Args a) throws Exception {
        Api api = new Api(a.api());
        int customers = a.integer("customers", 10_000);
        int merchants = a.integer("merchants", 1_000);
        long funding = a.lng("funding", 100_000_000); // $1,000,000.00 each
        Path out = Path.of(a.str("out", "bench-data"));
        Files.createDirectories(out);
        String run = a.str("run", Long.toString(System.currentTimeMillis()));

        long start = System.nanoTime();
        UUID clearing = api.openAccount("clearing-" + run, "SYSTEM");
        UUID[] c = new UUID[customers];
        UUID[] m = new UUID[merchants];
        Api.parallel(customers, 64, i -> c[i] = api.openAccount("customer-" + run + "-" + i, "CUSTOMER"));
        Api.parallel(merchants, 64, i -> m[i] = api.openAccount("merchant-" + run + "-" + i, "CUSTOMER"));
        Instant fundedAt = Instant.now().minusSeconds(3_600);
        Api.parallel(customers, 64, i -> api.transfer("fund-" + run + "-" + i, clearing, c[i], funding, "fund-" + run + "-" + i, fundedAt));

        Files.write(out.resolve("customers.txt"), lines(c));
        Files.write(out.resolve("merchants.txt"), lines(m));
        Files.writeString(out.resolve("clearing.txt"), clearing + "\n");
        System.out.printf("seeded %d customers, %d merchants in %.1fs -> %s%n",
                customers, merchants, (System.nanoTime() - start) / 1e9, out.toAbsolutePath());
    }

    private static List<String> lines(UUID[] ids) {
        return IntStream.range(0, ids.length).mapToObj(i -> ids[i].toString()).toList();
    }
}
