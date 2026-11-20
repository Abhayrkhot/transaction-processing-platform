package io.github.abhayrkhot.ledger.account;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Read model served by GET /accounts/{id}: the balance plus the newest entries. */
public record AccountSummary(
        UUID id,
        String ownerId,
        Account.Type type,
        String currency,
        long balance,
        Account.Status status,
        long version,
        Instant updatedAt,
        List<Entry> recentEntries) {

    public record Entry(long id, UUID transactionId, long amount, long balanceAfter, Instant postedAt) {}
}
