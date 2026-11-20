package io.github.abhayrkhot.ledger.ingest;

import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.transfer.TransferCommand;
import java.time.Instant;
import java.util.UUID;

/** Payload of the transfer-requests topic, produced by upstream payment channels. */
record TransferRequestMessage(
        String idempotencyKey,
        UUID fromAccountId,
        UUID toAccountId,
        Long amount,
        String currency,
        String reference,
        Instant occurredAt) {

    TransferCommand toCommand() {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 128) {
            throw invalid("idempotencyKey is required (max 128 chars)");
        }
        if (fromAccountId == null || toAccountId == null) {
            throw invalid("fromAccountId and toAccountId are required");
        }
        if (amount == null || amount <= 0) {
            throw invalid("amount must be positive");
        }
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw invalid("currency must be an ISO-4217 code");
        }
        if (reference != null && reference.length() > 128) {
            throw invalid("reference is limited to 128 chars");
        }
        return new TransferCommand(idempotencyKey, fromAccountId, toAccountId, amount, currency, reference, occurredAt);
    }

    private static LedgerException invalid(String message) {
        return LedgerException.badRequest("INVALID_MESSAGE", message);
    }
}
