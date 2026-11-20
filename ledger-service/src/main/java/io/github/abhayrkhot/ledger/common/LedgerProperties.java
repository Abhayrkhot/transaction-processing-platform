package io.github.abhayrkhot.ledger.common;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ledger")
public record LedgerProperties(AccountCache accountCache, int recentEntries, Kafka kafka, Outbox outbox) {

    public record AccountCache(boolean enabled, Duration ttl) {}

    public record Kafka(
            String transferRequestsTopic,
            String postedTopic,
            int partitions,
            int replicas,
            int ingestConcurrency,
            int riskConcurrency) {}

    public record Outbox(int batchSize, long pollIntervalMs) {}
}
