package io.github.abhayrkhot.ledger.transfer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** A validated transfer request, from the REST API or the Kafka ingest topic. */
public record TransferCommand(
        String idempotencyKey,
        UUID fromAccountId,
        UUID toAccountId,
        long amount,
        String currency,
        String reference,
        Instant occurredAt) {

    /**
     * Fingerprint of everything except the key. A retry must carry the same fingerprint;
     * the same key with a different body is a client bug and is refused.
     */
    public String requestHash() {
        String canonical = String.join("|",
                fromAccountId.toString(),
                toAccountId.toString(),
                Long.toString(amount),
                currency,
                Objects.toString(reference, ""),
                occurredAt == null ? "" : occurredAt.toString());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
