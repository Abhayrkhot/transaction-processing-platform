package io.github.abhayrkhot.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.abhayrkhot.ledger.account.Account;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.Test;

/** End to end: posting -> outbox -> Kafka -> risk consumer -> alerts. */
class RiskIT extends IntegrationTest {

    private final Instant t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(Duration.ofDays(3));

    @Test
    void largeAmountAgainstHistory() {
        Account a = customer(1_000_000);
        Account shop = customer(0);
        for (int i = 0; i < 6; i++) {
            transfer(a.id(), shop.id(), 2_000 + i, null, t0.plus(Duration.ofHours(i * 5)));
        }
        transfer(a.id(), shop.id(), 300_000, null, t0.plus(Duration.ofDays(2)));
        assertAlerts(a, "LARGE_AMOUNT");
    }

    @Test
    void velocityBurstAlertsOnce() {
        Account a = customer(1_000_000);
        Account shop = customer(0);
        for (int i = 0; i < 12; i++) {
            transfer(a.id(), shop.id(), 500 + i, null, t0.plus(Duration.ofSeconds(20L * i)));
        }
        assertAlerts(a, "VELOCITY");
    }

    @Test
    void structuringBelowThreshold() {
        Account a = customer(10_000_000);
        Account b = customer(0);
        Account c = customer(0);
        // Enough ordinary history (avg ~$1,500) that $9.5k is not "large" for this account.
        for (int i = 0; i < 6; i++) {
            transfer(a.id(), c.id(), 150_000 + 1_000 * i, null, t0.minus(Duration.ofDays(5 + i)));
        }
        transfer(a.id(), b.id(), 950_000, null, t0);
        transfer(a.id(), c.id(), 925_000, null, t0.plus(Duration.ofHours(3)));
        transfer(a.id(), b.id(), 990_000, null, t0.plus(Duration.ofHours(9)));
        assertAlerts(a, "STRUCTURING");
    }

    @Test
    void roundTripFlagsTheReturnLeg() {
        Account a = customer(1_000_000);
        Account b = customer(0);
        transfer(a.id(), b.id(), 100_000, null, t0);
        transfer(b.id(), a.id(), 98_000, null, t0.plus(Duration.ofMinutes(25)));
        assertAlerts(b, "ROUND_TRIP");
    }

    @Test
    void earlierFraudDoesNotRaiseTheBaselineForLargeAmounts() {
        Account a = customer(10_000_000);
        Account shop = customer(0);
        Account mule = customer(0);
        for (int i = 0; i < 8; i++) {
            transfer(a.id(), shop.id(), 3_000 + 100 * i, null, t0.minus(Duration.ofDays(10 - i)));
        }
        for (int i = 0; i < 4; i++) { // structuring the day before
            transfer(a.id(), mule.id(), 960_000 + i, null, t0.minus(Duration.ofHours(20 - 2 * i)));
        }
        var drain = transfer(a.id(), mule.id(), 1_500_000, null, t0);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(jdbc.sql(
                        "SELECT rule FROM risk_alerts WHERE transaction_id = :t")
                .param("t", drain.transaction().id()).query(String.class).list()).contains("LARGE_AMOUNT"));
    }

    @Test
    void duplicatePaymentFlagsTheSecondCopy() {
        Account a = customer(1_000_000);
        Account shop = customer(0);
        for (int i = 0; i < 6; i++) {
            transfer(a.id(), shop.id(), 1_000 + 37 * i, null, t0.minus(Duration.ofHours(10 + i)));
        }
        transfer(a.id(), shop.id(), 4_599, null, t0);
        transfer(a.id(), shop.id(), 4_599, null, t0.plus(Duration.ofSeconds(40)));
        assertAlerts(a, "DUPLICATE_PAYMENT");
    }

    @Test
    void ordinaryActivityRaisesNothing() {
        Account a = customer(1_000_000);
        Account shop = customer(0);
        Account friend = customer(0);
        for (int i = 0; i < 10; i++) {
            transfer(a.id(), i % 3 == 0 ? friend.id() : shop.id(), 1_500 + 311 * i, null, t0.plus(Duration.ofHours(7L * i)));
        }
        // The marker is posted last; once its alert exists, a's events have had time to be scored.
        Account marker = customer(10_000_000);
        transfer(marker.id(), shop.id(), 3_000_000, null, t0);
        assertAlerts(marker, "LARGE_AMOUNT");
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(rules(a)).isEmpty());
    }

    private void assertAlerts(Account account, String... expected) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(rules(account)).containsExactlyInAnyOrder(expected));
    }

    private List<String> rules(Account account) {
        return jdbc.sql("SELECT rule FROM risk_alerts WHERE account_id = :a").param("a", account.id()).query(String.class).list();
    }
}
