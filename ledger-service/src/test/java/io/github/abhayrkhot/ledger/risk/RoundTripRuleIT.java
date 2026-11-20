package io.github.abhayrkhot.ledger.risk;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abhayrkhot.ledger.IntegrationTest;
import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.transfer.Transaction;
import io.github.abhayrkhot.ledger.transfer.TransactionPostedEvent;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RoundTripRuleIT extends IntegrationTest {

    @Autowired
    RiskEngine engine;

    /**
     * The return leg is 4.9% below the outbound leg but 5.15% of itself. Evaluated on the
     * return leg (the outbound one was scored before the return existed), it must still match.
     */
    @Test
    void toleranceIsRelativeToTheLargerLeg() {
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(Duration.ofDays(3));
        Account a = customer(1_000_000);
        Account b = customer(0);
        Transaction out = transfer(a.id(), b.id(), 100_000, null, t0).transaction();
        Transaction back = transfer(b.id(), a.id(), 95_100, null, t0.plus(Duration.ofMinutes(40))).transaction();

        var alert = engine.roundTrip(new TransactionPostedEvent(back.id(), b.id(), Account.Type.CUSTOMER, a.id(),
                Account.Type.CUSTOMER, back.amount(), "USD", null, back.occurredAt(), back.postedAt()));

        assertThat(alert).hasValueSatisfying(x -> {
            assertThat(x.transactionId()).isEqualTo(back.id());
            assertThat(x.relatedTransactionId()).isEqualTo(out.id());
        });
    }
}
