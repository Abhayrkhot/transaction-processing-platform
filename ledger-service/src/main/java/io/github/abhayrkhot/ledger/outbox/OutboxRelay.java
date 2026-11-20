package io.github.abhayrkhot.ledger.outbox;

import io.github.abhayrkhot.ledger.common.LedgerProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Relays committed outbox rows to Kafka. A row is deleted only after the broker
 * acknowledged it (acks=all), so delivery is at-least-once; consumers are idempotent.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final Counter published;

    OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                LedgerProperties properties, MeterRegistry meters) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.tx = tx;
        this.batchSize = properties.outbox().batchSize();
        this.published = meters.counter("ledger.outbox.published");
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.poll-interval-ms}")
    void relay() {
        try {
            int sent;
            do {
                sent = tx.execute(status -> publishBatch());
            } while (sent == batchSize);
        } catch (RuntimeException e) {
            // Rows stay in the outbox and are retried on the next tick.
            log.warn("outbox relay failed, will retry: {}", e.toString());
        }
    }

    private int publishBatch() {
        List<OutboxRepository.Message> batch = outbox.claim(batchSize);
        if (batch.isEmpty()) {
            return 0;
        }
        List<CompletableFuture<?>> acks = batch.stream()
                .<CompletableFuture<?>>map(m -> kafka.send(m.topic(), m.key(), m.payload()))
                .toList();
        try {
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka did not acknowledge outbox batch", e);
        }
        outbox.delete(batch.stream().map(OutboxRepository.Message::id).toList());
        published.increment(batch.size());
        return batch.size();
    }
}
