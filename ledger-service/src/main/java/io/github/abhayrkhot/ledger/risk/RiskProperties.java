package io.github.abhayrkhot.ledger.risk;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Rule thresholds; amounts are minor units. */
@ConfigurationProperties("risk")
public record RiskProperties(
        LargeAmount largeAmount,
        Velocity velocity,
        Structuring structuring,
        RoundTrip roundTrip,
        DuplicatePayment duplicatePayment) {

    public record LargeAmount(long absoluteLimit, long minAmount, int minHistory, long multiplier, Duration lookback) {}

    public record Velocity(Duration window, int maxTransfers) {}

    public record Structuring(long threshold, int bandPercent, Duration window, int minCount) {}

    public record RoundTrip(Duration window, int amountTolerancePercent, long minAmount) {}

    public record DuplicatePayment(Duration window) {}
}
