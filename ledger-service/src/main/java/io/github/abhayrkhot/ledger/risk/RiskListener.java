package io.github.abhayrkhot.ledger.risk;

import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.transfer.TransactionPostedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
class RiskListener {

    private final RiskEngine engine;
    private final JsonMapper json;

    RiskListener(RiskEngine engine, JsonMapper json) {
        this.engine = engine;
        this.json = json;
    }

    @KafkaListener(
            id = "risk-engine",
            groupId = "risk-engine",
            topics = "${ledger.kafka.posted-topic}",
            concurrency = "${ledger.kafka.risk-concurrency}")
    void onPosted(ConsumerRecord<String, String> record) {
        TransactionPostedEvent event;
        try {
            event = json.readValue(record.value(), TransactionPostedEvent.class);
        } catch (JacksonException | IllegalArgumentException e) {
            throw LedgerException.badRequest("MALFORMED_MESSAGE", "unreadable posted event: " + e.getMessage());
        }
        engine.evaluate(event);
    }
}
