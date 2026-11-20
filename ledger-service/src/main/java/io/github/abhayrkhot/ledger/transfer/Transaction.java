package io.github.abhayrkhot.ledger.transfer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.UUID;

public record Transaction(
        UUID id,
        String idempotencyKey,
        @JsonIgnore String requestHash,
        UUID fromAccountId,
        UUID toAccountId,
        long amount,
        String currency,
        Status status,
        String rejectReason,
        String reference,
        Instant occurredAt,
        Instant postedAt) {

    public enum Status { POSTED, REJECTED }
}
