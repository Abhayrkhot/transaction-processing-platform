package io.github.abhayrkhot.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.recon.ReconciliationService;
import io.github.abhayrkhot.ledger.recon.ReconciliationService.BreakType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import tools.jackson.databind.json.JsonMapper;

class ReconciliationIT extends IntegrationTest {

    @Value("${local.server.port}")
    int port;

    @Autowired
    JsonMapper json;

    @Test
    void findsEveryKindOfBreak() throws Exception {
        Instant day = Instant.parse("2031-03-04T00:00:00Z");
        Account clearing = clearing();
        Account customer = customer(0);
        String run = Long.toString(System.nanoTime());
        for (int i = 0; i < 5; i++) {
            transfer(clearing.id(), customer.id(), 10_000 + i, "dep-" + run + "-" + i, day.plus(Duration.ofHours(i)));
        }
        // dep-0, dep-1 match; dep-2 has the wrong amount; dep-3 is settled twice; dep-4 is missing; ghost is unknown
        String csv = String.join("\n",
                "reference,amount,currency,value_date",
                line("dep-" + run + "-0", 10_000, day),
                line("dep-" + run + "-1", 10_001, day),
                line("dep-" + run + "-2", 10_502, day),
                line("dep-" + run + "-3", 10_003, day),
                line("dep-" + run + "-3", 10_003, day),
                line("ghost-" + run, 777, day));

        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + port + "/api/v1/reconciliations?from=" + day + "&to=" + day.plus(1, ChronoUnit.DAYS)))
                .header("Content-Type", "text/csv")
                .POST(HttpRequest.BodyPublishers.ofString(csv)).build(), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        var report = json.readValue(response.body(), ReconciliationService.Report.class);
        assertThat(report.matched()).isEqualTo(3);
        assertThat(report.breakCounts()).containsEntry(BreakType.AMOUNT_MISMATCH, 1)
                .containsEntry(BreakType.DUPLICATE_SETTLEMENT, 1)
                .containsEntry(BreakType.MISSING_IN_SETTLEMENT, 1)
                .containsEntry(BreakType.MISSING_IN_LEDGER, 1);
        assertThat(report.breaks()).filteredOn(b -> b.type() == BreakType.MISSING_IN_SETTLEMENT)
                .extracting(ReconciliationService.Break::reference).containsExactly("dep-" + run + "-4");
    }

    @Test
    void rejectsMalformedFiles() throws Exception {
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + port + "/api/v1/reconciliations?from=2031-01-01T00:00:00Z&to=2031-01-02T00:00:00Z"))
                .header("Content-Type", "text/csv")
                .POST(HttpRequest.BodyPublishers.ofString("reference,amount,currency,value_date\nx,notanumber,USD,2031-01-01T00:00:00Z"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("line 2");
    }

    private static String line(String reference, long amount, Instant at) {
        return reference + "," + amount + ",USD," + at;
    }
}
