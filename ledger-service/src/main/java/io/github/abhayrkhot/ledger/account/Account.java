package io.github.abhayrkhot.ledger.account;

import java.time.Instant;
import java.util.UUID;

public record Account(
        UUID id,
        String ownerId,
        Type type,
        String currency,
        long balance,
        Status status,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    public enum Type { CUSTOMER, SYSTEM }

    public enum Status { ACTIVE, FROZEN, CLOSED }

    public boolean isSystem() {
        return type == Type.SYSTEM;
    }
}
