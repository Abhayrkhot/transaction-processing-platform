package io.github.abhayrkhot.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.abhayrkhot.ledger.account.Account;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaIngestIT extends IntegrationTest {

    static final String TOPIC = "ledger.transfer-requests";

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ConsumerFactory<String, String> consumers;

    @Test
    void duplicateDeliveriesPostOnceAndBadMessagesAreDeadLettered() throws Exception {
        Account a = customer(10_000);
        Account b = customer(0);
        String key = "kafka-" + UUID.randomUUID();
        String message = """
                {"idempotencyKey":"%s","fromAccountId":"%s","toAccountId":"%s","amount":400,"currency":"USD","reference":"r-1"}
                """.formatted(key, a.id(), b.id());
        String unknownAccount = """
                {"idempotencyKey":"%s","fromAccountId":"%s","toAccountId":"%s","amount":400,"currency":"USD"}
                """.formatted("kafka-" + UUID.randomUUID(), a.id(), UUID.randomUUID());
        String marker = UUID.randomUUID().toString();

        try (Consumer<String, String> dlt = consumers.createConsumer("dlt-test-" + marker, null, null, earliest())) {
            dlt.subscribe(List.of(TOPIC + ".dlt"));

            for (int i = 0; i < 3; i++) {
                kafka.send(TOPIC, a.id().toString(), message).get();
            }
            kafka.send(TOPIC, a.id().toString(), "{not json " + marker).get();
            kafka.send(TOPIC, a.id().toString(), unknownAccount).get();

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(balance(b.id())).isEqualTo(400);
                assertThat(jdbc.sql("SELECT count(*) FROM transactions WHERE idempotency_key = :k")
                        .param("k", key).query(Long.class).single()).isEqualTo(1);
            });

            List<String> deadLettered = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                dlt.poll(Duration.ofMillis(500)).forEach(r -> deadLettered.add(r.value()));
                assertThat(deadLettered).anyMatch(v -> v.contains(marker));
                assertThat(deadLettered).anyMatch(v -> v.equals(unknownAccount));
            });
        }
        assertThat(balance(a.id())).isEqualTo(9_600);
    }

    private static Properties earliest() {
        var p = new Properties();
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }
}
