package io.github.abhayrkhot.ledger.ingest;

import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.transfer.PostingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes transfer requests. Offsets are committed after processing, so a crash or
 * rebalance redelivers records; the idempotency key turns each redelivery into a replay
 * instead of a second posting.
 */
@Component
class TransferRequestListener {

    private final PostingService posting;
    private final JsonMapper json;

    TransferRequestListener(PostingService posting, JsonMapper json) {
        this.posting = posting;
        this.json = json;
    }

    @KafkaListener(
            id = "transfer-ingest",
            groupId = "ledger-ingest",
            topics = "${ledger.kafka.transfer-requests-topic}",
            concurrency = "${ledger.kafka.ingest-concurrency}")
    void onTransferRequest(ConsumerRecord<String, String> record) {
        TransferRequestMessage message;
        try {
            message = json.readValue(record.value(), TransferRequestMessage.class);
        } catch (JacksonException | IllegalArgumentException e) {
            throw LedgerException.badRequest("MALFORMED_MESSAGE", "unreadable transfer request: " + e.getMessage());
        }
        posting.transfer(message.toCommand(), PostingService.Channel.KAFKA);
    }
}
